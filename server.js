'use strict';

const express  = require('express');
const cors     = require('cors');
const https    = require('https');
const path     = require('path');
const fs       = require('fs');
const { spawn } = require('child_process');
const Database = require('better-sqlite3');

const app  = express();
const PORT = process.env.PORT || 3000;
const FMP  = process.env.FMP_KEY || 'OJfv9bPVEMrnwPX7noNpJLZCFLLFTmlu';

const DATA_DIR = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH = path.join(DATA_DIR, 'trades.db');

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

// Global safety net — log crashes but keep the process alive
process.on('uncaughtException',  err => console.error('UNCAUGHT:', err.message));
process.on('unhandledRejection', err => console.error('UNHANDLED:', err?.message || err));

// Per-request timeout: kill any request that takes > 25s so it returns a proper error
// rather than hanging until the platform kills the whole dyno
app.use((req, res, next) => {
  res.setTimeout(25000, () => {
    if (!res.headersSent) res.status(503).json({ error: 'Request timeout' });
  });
  next();
});

// ─── DB ───────────────────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
    trade_date TEXT NOT NULL, filing_date TEXT,
    type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER, accession TEXT,
    UNIQUE(accession, insider, trade_date, type, qty)
  );
  CREATE INDEX IF NOT EXISTS idx_ticker     ON trades(ticker);
  CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date DESC);
  CREATE INDEX IF NOT EXISTS idx_filing_date ON trades(filing_date DESC);
  CREATE INDEX IF NOT EXISTS idx_insider    ON trades(insider);
  CREATE TABLE IF NOT EXISTS sync_log (
    quarter TEXT PRIMARY KEY, synced_at TEXT DEFAULT (datetime('now')), rows INTEGER
  );
`);

// ─── SYNC via child process ────────────────────────────────────
let syncRunning = false;
const syncLog   = [];

function slog(msg) {
  const line = `[${new Date().toISOString().slice(11,19)}] ${msg}`;
  console.log(line);
  syncLog.push(line);
  if (syncLog.length > 300) syncLog.shift();
}

function runSync(numQ = 4) {
  if (syncRunning) { slog('sync already running'); return; }
  syncRunning = true;
  slog(`=== spawning sync-worker (${numQ} quarters) ===`);

  const worker = spawn(
    process.execPath,
    ['--max-old-space-size=400', path.join(__dirname, 'sync-worker.js'), String(numQ)],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );

  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog(l)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('ERR: '+l)));
  worker.on('exit', code => {
    syncRunning = false;
    slog(`=== worker exited (code ${code}) ===`);
  });
}

// ─── HTTP helper ──────────────────────────────────────────────
function get(url, ms=30000) {
  return new Promise((resolve,reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: ms,
    }, res => {
      if ([301,302,303].includes(res.statusCode) && res.headers.location)
        return get(res.headers.location, ms).then(resolve).catch(reject);
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ─── DAILY INGESTION (recent Form 4s from EDGAR daily index) ────
let dailyRunning = false;

function runDaily(daysBack = 3) {
  if (dailyRunning) return;
  dailyRunning = true;
  slog(`=== spawning daily-worker (RSS poll mode, ${daysBack} days backfill) ===`);

  const worker = spawn(
    process.execPath,
    ['--max-old-space-size=200', path.join(__dirname, 'daily-worker.js'), String(daysBack), 'poll'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );

  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[daily] ' + l)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[daily] ERR: ' + l)));
  worker.on('exit', code => {
    dailyRunning = false;
    slog(`=== daily-worker exited (code ${code}) — restarting in 30s ===`);
    setTimeout(() => runDaily(2), 30 * 1000);
  });
}

// ─── PRICE CACHE ──────────────────────────────────────────────
const pc = new Map();
function getPC(k)      { const c = pc.get(k); return c && Date.now()<c.e ? c.v : null; }
function setPC(k,v,ms) { pc.set(k, { v, e: Date.now()+ms }); }

// ─── API ROUTES ───────────────────────────────────────────────

// SCREENER
app.get('/api/screener', (req, res) => {
  try {
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    if (n === 0 && syncRunning)
      return res.json({ building: true, message: 'Loading SEC data (~3 min)...', trades: [] });

    const days    = Math.min(Math.max(parseInt(req.query.days || '30'), 1), 1095); // cap at 3 years
    const limit   = Math.min(parseInt(req.query.limit || '1000'), 2000);           // cap rows returned

    let rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades
      WHERE filing_date >= date('now', '-' || ? || ' days')
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT ?
    `).all(days, limit);

    if (!rows.length && n > 0) {
      rows = db.prepare(`
        SELECT ticker, company, insider, title,
               trade_date AS trade, filing_date AS filing,
               type, qty, price, value, owned
        FROM trades
        ORDER BY trade_date DESC, filing_date DESC
        LIMIT 500
      `).all();
    }

    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// TICKER
app.get('/api/ticker', (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  try {
    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades WHERE ticker = ?
      ORDER BY trade_date DESC LIMIT 5000
    `).all(sym);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// INSIDER
app.get('/api/insider', (req, res) => {
  const name  = (req.query.name  || '').trim();
  const exact = req.query.exact === '1';
  if (!name) return res.status(400).json({ error: 'name required' });
  if (name.length < 2) return res.status(400).json({ error: 'name too short' });
  try {
    const pattern = exact ? name : `%${name}%`;
    // Exact match: full history up to 2000 rows
    // Fuzzy match: cap at 500 rows to avoid massive payloads on broad queries
    const limit = exact ? 2000 : 500;
    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades WHERE UPPER(insider) LIKE UPPER(?)
      ORDER BY trade_date DESC LIMIT ?
    `).all(pattern, limit);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PRICE — FMP with Yahoo Finance fallback
// FIRST BUY IN YEARS — scans entire DB history, no date filter
// Uses window functions to find insiders whose most recent buy on a ticker
// came after a long gap since their previous buy on that same ticker.
app.get('/api/firstbuys', (req, res) => {
  try {
    const minGapDays  = parseInt(req.query.mingap  || '180');  // default 6 months
    const lookbackDays = parseInt(req.query.lookback || '90'); // how recent must the new buy be
    const limit        = parseInt(req.query.limit   || '100');

    // Step 1: get all open-market buys, ordered per insider+ticker
    // Step 2: self-join to get each buy paired with the previous buy
    //         for the same insider on the same ticker
    // Step 3: filter to pairs where:
    //   - the newer buy is within lookbackDays
    //   - the gap between the two buys is >= minGapDays
    const rows = db.prepare(`
      WITH buys AS (
        SELECT ticker, company, insider, title,
               trade_date, filing_date, price, qty, value, owned,
               ROW_NUMBER() OVER (
                 PARTITION BY insider, ticker
                 ORDER BY trade_date DESC, filing_date DESC
               ) AS rn
        FROM trades
        WHERE type = 'P'
          AND price > 0
          AND insider IS NOT NULL
          AND ticker  IS NOT NULL
      ),
      latest AS (SELECT * FROM buys WHERE rn = 1),
      prev   AS (SELECT * FROM buys WHERE rn = 2)
      SELECT
        l.ticker,
        l.company,
        l.insider,
        l.title,
        l.trade_date    AS latest_trade,
        l.filing_date   AS latest_filing,
        l.price         AS latest_price,
        l.qty           AS latest_qty,
        l.value         AS latest_value,
        l.owned         AS latest_owned,
        p.trade_date    AS prev_trade,
        p.owned         AS prev_owned,
        CAST(
          julianday(l.trade_date) - julianday(p.trade_date)
        AS INTEGER)     AS gap_days
      FROM latest l
      JOIN prev p ON l.insider = p.insider AND l.ticker = p.ticker
      WHERE l.trade_date >= date('now', '-' || ? || ' days')
        AND (julianday(l.trade_date) - julianday(p.trade_date)) >= ?
      ORDER BY gap_days DESC
      LIMIT ?
    `).all(lookbackDays, minGapDays, limit);

    res.json(rows);
  } catch(e) {
    slog('firstbuys error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// OPPORTUNITY RANKER — per-ticker aggregates for frontend scoring
app.get('/api/ranker', (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days || '30'), 90);
    const rows = db.prepare(`
      SELECT
        ticker,
        MAX(company) AS company,
        COUNT(CASE WHEN type='P' THEN 1 END)                            AS buy_count,
        COUNT(DISTINCT CASE WHEN type='P' THEN insider END)             AS buyer_count,
        SUM(CASE WHEN type='P' THEN COALESCE(value,0) ELSE 0 END)       AS total_buy_val,
        MAX(CASE WHEN type='P' THEN trade_date ELSE NULL END)           AS latest_buy_date,
        COUNT(CASE WHEN type IN ('S','S-') THEN 1 END)                  AS sell_count,
        COUNT(DISTINCT CASE WHEN type IN ('S','S-') THEN insider END)   AS seller_count,
        SUM(CASE WHEN type IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS total_sell_val,
        MAX(CASE WHEN type IN ('S','S-') THEN trade_date ELSE NULL END) AS latest_sell_date,
        MAX(CASE WHEN type='P' AND (
          UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%' OR
          UPPER(title) LIKE '%PRESIDENT%' OR UPPER(title) LIKE '%CHAIRMAN%' OR
          UPPER(title) LIKE '%FOUNDER%' OR UPPER(title) LIKE '%COO%'
        ) THEN 1 ELSE 0 END)                                            AS has_exec_buyer,
        CAST(julianday('now') - julianday(
          MAX(CASE WHEN type='P' THEN trade_date ELSE NULL END)
        ) AS INTEGER)                                                    AS days_since_buy,
        MAX(CASE WHEN type='P' AND owned>qty AND qty>0
          THEN CAST(qty*100.0/(owned-qty) AS INTEGER) ELSE 0 END)       AS max_stake_pct
      FROM trades
      WHERE filing_date >= date('now', '-' || ? || ' days')
      GROUP BY ticker
      HAVING buy_count > 0
      ORDER BY total_buy_val DESC
      LIMIT 200
    `).all(days);
    res.json(rows);
  } catch(e) {
    slog('ranker error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// LEADERBOARD — top insiders by open-market buy activity (enough history to score)
app.get('/api/leaderboard', (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '40'), 80);
    const rows = db.prepare(`
      SELECT
        insider,
        MAX(title)                                                          AS title,
        COUNT(CASE WHEN type='P' THEN 1 END)                               AS buy_count,
        COUNT(DISTINCT CASE WHEN type='P' THEN ticker END)                 AS ticker_count,
        SUM(CASE WHEN type='P' THEN COALESCE(value,0) ELSE 0 END)          AS total_buy_val,
        MAX(CASE WHEN type='P' THEN trade_date ELSE NULL END)               AS latest_buy,
        MIN(CASE WHEN type='P' THEN trade_date ELSE NULL END)               AS earliest_buy,
        GROUP_CONCAT(DISTINCT CASE WHEN type='P' THEN ticker END)          AS tickers_csv
      FROM trades
      WHERE insider IS NOT NULL
        AND type = 'P'
        AND price > 0
      GROUP BY insider
      HAVING buy_count >= 3
      ORDER BY buy_count DESC, total_buy_val DESC
      LIMIT ?
    `).all(limit);
    res.json(rows);
  } catch(e) {
    slog('leaderboard error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// SCOREBOARD — pre-computes per-insider buy stats + unique tickers needed for pricing
// Frontend only needs to fetch price data (deduplicated across insiders), not individual trade histories
// SCOREBOARD — fully server-side: fetches prices, scores every insider, returns results
// Browser makes ONE request and receives ranked leaderboards ready to render
let _scoreboardCache = null;
let _scoreboardCacheTime = 0;
const SCOREBOARD_TTL = 10 * 60 * 1000; // 10 minutes

app.get('/api/scoreboard', async (req, res) => {
  // Return cached result if fresh
  if (_scoreboardCache && Date.now() - _scoreboardCacheTime < SCOREBOARD_TTL) {
    return res.json(_scoreboardCache);
  }
  try {
    const minBuys = parseInt(req.query.minbuys || '4');
    const limit   = Math.min(parseInt(req.query.limit || '30'), 60);

    // Pull qualifying insiders and their packed buy history from DB
    const rows = db.prepare(`
      SELECT
        insider,
        MAX(title)                                                            AS title,
        COUNT(*)                                                              AS buy_count,
        GROUP_CONCAT(ticker || '|' || trade_date || '|' || price, ';;')      AS trade_data,
        GROUP_CONCAT(DISTINCT ticker)                                         AS tickers_csv,
        CAST(julianday(MAX(trade_date)) - julianday(MIN(trade_date)) AS INTEGER) AS span_days
      FROM trades
      WHERE type = 'P'
        AND price > 0
        AND insider IS NOT NULL
        AND ticker  IS NOT NULL
        AND trade_date <= date('now', '-95 days')
      GROUP BY insider
      HAVING buy_count >= ? AND span_days >= 90
      ORDER BY buy_count DESC, span_days DESC
      LIMIT ?
    `).all(minBuys, limit);

    if (!rows.length) return res.json({ accuracy: [], timing: [] });

    // Deduplicate tickers across all insiders
    const allTickers = [...new Set(
      rows.flatMap(r => (r.tickers_csv || '').split(',').filter(Boolean))
    )];

    // ── Fetch all price data using shared fetchPriceBars helper (all sources + cache) ──
    const priceEntries = await Promise.all(allTickers.map(async sym => [sym, await fetchPriceBars(sym)]));
    const priceCache   = Object.fromEntries(priceEntries.filter(([, v]) => v));

    // ── Score every insider ──
    const accuracyResults = [];
    const timingResults   = [];

    rows.forEach(leader => {
      try {
        const rawTrades = (leader.trade_data || '').split(';;').map(s => {
          const [ticker, trade_date, price] = s.split('|');
          return { ticker, trade: trade_date, price: parseFloat(price) };
        }).filter(t => t.ticker && t.price > 0 && t.trade);

        if (rawTrades.length < 4) return;

        // Forward returns per trade
        const scored = rawTrades.map(t => {
          const bars = priceCache[t.ticker] || [];
          if (!bars.length) return null;
          const buyDate = t.trade.slice(0, 10);
          const fwd = days => {
            const fd = new Date(buyDate + 'T12:00:00Z'); fd.setUTCDate(fd.getUTCDate() + days);
            const fs = fd.toISOString().slice(0, 10);
            const bar = bars.find(b => b.time >= fs);
            return bar ? +((bar.close - t.price) / t.price * 100).toFixed(2) : null;
          };
          return { ticker: t.ticker, tradeDate: buyDate, buyPrice: t.price,
            ret30: fwd(30), ret90: fwd(90), ret180: fwd(180) };
        }).filter(Boolean);

        const completed = scored.filter(s => s.ret90 !== null);
        if (completed.length < 3) return;

        // Accuracy score
        const rets90    = completed.map(s => s.ret90);
        const avgRet90  = +(rets90.reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
        const rets30    = completed.filter(s => s.ret30 !== null).map(s => s.ret30);
        const avgRet30  = rets30.length ? +(rets30.reduce((a, b) => a + b, 0) / rets30.length).toFixed(1) : null;
        const winRate   = Math.round(rets90.filter(r => r > 0).length / rets90.length * 100);
        const avgMag    = +(rets90.map(Math.abs).reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
        const sorted    = [...rets90].sort((a, b) => a - b);
        const median    = sorted[Math.floor(sorted.length / 2)];
        const consist   = Math.round(Math.min(100, Math.max(0, (median / Math.max(avgMag, 1) + 1) * 50)));
        const accScore  = Math.round(Math.min(100, Math.max(0,
          winRate * 0.40 + Math.min(35, Math.max(0, avgRet90 / 20 * 35))
          + consist * 0.15 + Math.min(10, completed.length * 1.2)
        )));
        const tier      = accScore >= 75 ? 'ELITE' : accScore >= 55 ? 'STRONG' : accScore >= 35 ? 'AVERAGE' : 'WEAK';
        const tickers3  = [...new Set(rawTrades.map(t => t.ticker))].slice(0, 3).join(', ');

        accuracyResults.push({ name: leader.insider, title: leader.title || '',
          accuracyScore: accScore, tier, winRate, avgRet90, avgRet30,
          tradeCount: completed.length, tickers: tickers3 });

        // Timing alpha — buy-near-low + sell-near-high + fwd returns
        let nearLowCount = 0, nearHighSellCount = 0, totalSells = 0;
        let ret30sum = 0, ret90sum = 0, ret180sum = 0, retN = 0;

        scored.forEach(s => {
          const bars = priceCache[s.ticker] || [];
          if (!bars.length || !s.buyPrice) return;
          const yr1Start = new Date(s.tradeDate + 'T12:00:00Z');
          yr1Start.setUTCFullYear(yr1Start.getUTCFullYear() - 1);
          const yr1Bars  = bars.filter(b => b.time >= yr1Start.toISOString().slice(0, 10) && b.time <= s.tradeDate);
          if (yr1Bars.length < 20) return;
          const yr1Lo = Math.min(...yr1Bars.map(b => b.low || b.close));
          const pctAboveLow = (s.buyPrice - yr1Lo) / yr1Lo * 100;
          if (pctAboveLow <= 20) nearLowCount++;
          if (s.ret30 !== null)  { ret30sum  += s.ret30;  }
          if (s.ret90 !== null)  { ret90sum  += s.ret90;  retN++; }
          if (s.ret180 !== null) { ret180sum += s.ret180; }
        });

        const n          = scored.length;
        const nearLowPct = n > 0 ? Math.round(nearLowCount / n * 100) : 0;
        const avgFwd90   = retN > 0 ? +(ret90sum / retN).toFixed(1) : null;
        const avgFwd180  = retN > 0 ? +(ret180sum / retN).toFixed(1) : null;
        const avgPos     = scored.reduce((acc, s) => {
          const bars = priceCache[s.ticker] || [];
          if (!bars.length || !s.buyPrice) return acc;
          const yr1Start = new Date(s.tradeDate + 'T12:00:00Z');
          yr1Start.setUTCFullYear(yr1Start.getUTCFullYear() - 1);
          const yr1Bars  = bars.filter(b => b.time >= yr1Start.toISOString().slice(0, 10) && b.time <= s.tradeDate);
          if (yr1Bars.length < 20) return acc;
          const yr1Lo = Math.min(...yr1Bars.map(b => b.low  || b.close));
          const yr1Hi = Math.max(...yr1Bars.map(b => b.high || b.close));
          const rng = yr1Hi - yr1Lo;
          return rng > 0.01 ? { sum: acc.sum + (s.buyPrice - yr1Lo) / rng, n: acc.n + 1 } : acc;
        }, { sum: 0, n: 0 });
        const avgPosVal = avgPos.n > 0 ? avgPos.sum / avgPos.n : 0.5;

        const compLow  = Math.round(Math.min(25, (nearLowPct / 100) * 50));
        const compPos  = Math.round(Math.min(25, Math.max(0, (0.7 - avgPosVal) / 0.4 * 25)));
        const compFwd  = avgFwd90 !== null ? Math.round(Math.min(25, Math.max(0, avgFwd90 / 10 * 25))) : 12;
        const compSell = 12; // server doesn't have sell data in this query
        const timingAlpha = Math.min(100, Math.max(0, compLow + compPos + compFwd + compSell));

        let verdict, verdictColor;
        if (timingAlpha >= 80 && nearLowPct >= 50) {
          verdict = 'Buys near bottoms'; verdictColor = 'buy';
        } else if (timingAlpha >= 80) {
          verdict = 'Elite forward returns'; verdictColor = 'buy';
        } else if (timingAlpha >= 60) {
          verdict = 'Above-average timing'; verdictColor = 'accent';
        } else if (timingAlpha >= 40) {
          verdict = 'Mixed timing signals'; verdictColor = 'option';
        } else {
          verdict = 'Tends to buy high'; verdictColor = 'sell';
        }

        if (timingAlpha >= 35) {
          timingResults.push({ name: leader.insider, title: leader.title || '',
            timingAlpha, nearLowPct, nearHighSellPct: null,
            avgRet90: avgFwd90, avgRet180: avgFwd180,
            verdict, verdictColor, tradeCount: completed.length, tickers: tickers3 });
        }
      } catch(e) { slog('score err ' + leader.insider + ': ' + e.message); }
    });

    accuracyResults.sort((a, b) => b.accuracyScore - a.accuracyScore);
    timingResults.sort((a, b) => b.timingAlpha - a.timingAlpha);

    const result = { accuracy: accuracyResults, timing: timingResults };
    _scoreboardCache = result;
    _scoreboardCacheTime = Date.now();
    res.json(result);
  } catch(e) {
    slog('scoreboard error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});




// PRICE — FMP with Yahoo Finance fallback
// ─── PRICE FETCH HELPER — shared by /api/price and /api/scoreboard ───────────
// ─── PRICE FETCH — FMP primary, Yahoo fallback, no Stooq (blocks) ─────────────
const FAILED_SYM    = new Map();  // sym → last-fail timestamp (5-min cooldown)
const PRICE_INFLIGHT = new Map(); // dedup concurrent requests for same symbol

async function fetchPriceBars(sym) {
  const cached = getPC(sym);
  if (cached) return cached;

  const lastFail = FAILED_SYM.get(sym);
  if (lastFail && Date.now() - lastFail < 5 * 60 * 1000) return null;

  // Dedup: if a fetch is already in-flight for this symbol, reuse it
  if (PRICE_INFLIGHT.has(sym)) return PRICE_INFLIGHT.get(sym);
  const p = _doFetch(sym).finally(() => PRICE_INFLIGHT.delete(sym));
  PRICE_INFLIGHT.set(sym, p);
  return p;
}

async function _doFetch(sym) {
  function norm(bars) {
    return (bars || []).filter(d => d.close > 0 && d.time && /^\d{4}-\d{2}-\d{2}$/.test(d.time));
  }

  // ── Source 1: FMP (best coverage, fast) ──────────────────────
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${sym}&apikey=${FMP}`, 12000
    );
    if (status === 200) {
      const data = JSON.parse(body.toString());
      const raw  = Array.isArray(data) ? data : (data?.historical || []);
      if (raw.length >= 5) {
        const bars = norm(raw.slice().reverse().map(d => ({
          time: d.date, open: +(+d.open).toFixed(2), high: +(+d.high).toFixed(2),
          low: +(+d.low).toFixed(2), close: +(+d.close).toFixed(2), volume: d.volume||0
        })));
        if (bars.length >= 5) { setPC(sym, bars, 60*60*1000); return bars; }
      }
    }
  } catch(e) { slog(`FMP ${sym}: ${e.message}`); }

  // ── Source 2: Yahoo Finance query1 ───────────────────────────
  try {
    const now = Math.floor(Date.now()/1000), from = now - 6*365*86400;
    const { status, body } = await get(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?period1=${from}&period2=${now}&interval=1d&events=history`, 12000
    );
    if (status === 200) {
      const json = JSON.parse(body.toString()), r = json?.chart?.result?.[0];
      const times = r?.timestamp || [], q = r?.indicators?.quote?.[0] || {};
      if (times.length >= 5 && q.close?.length) {
        const bars = norm(times.map((t,i) => ({
          time:   new Date(t*1000).toISOString().slice(0,10),
          open:   +(+(q.open?.[i]  ||0)).toFixed(2), high: +(+(q.high?.[i] ||0)).toFixed(2),
          low:    +(+(q.low?.[i]   ||0)).toFixed(2), close:+(+(q.close?.[i]||0)).toFixed(2),
          volume: q.volume?.[i]||0
        })));
        if (bars.length >= 5) { setPC(sym, bars, 60*60*1000); return bars; }
      }
    }
  } catch(e) { slog(`Yahoo q1 ${sym}: ${e.message}`); }

  // ── Source 3: Yahoo Finance query2 (different CDN) ───────────
  try {
    const now = Math.floor(Date.now()/1000), from = now - 6*365*86400;
    const { status, body } = await get(
      `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?period1=${from}&period2=${now}&interval=1d`, 12000
    );
    if (status === 200) {
      const json = JSON.parse(body.toString()), r = json?.chart?.result?.[0];
      const times = r?.timestamp || [], q = r?.indicators?.quote?.[0] || {};
      if (times.length >= 5 && q.close?.length) {
        const bars = norm(times.map((t,i) => ({
          time:   new Date(t*1000).toISOString().slice(0,10),
          open:   +(+(q.open?.[i]  ||0)).toFixed(2), high: +(+(q.high?.[i] ||0)).toFixed(2),
          low:    +(+(q.low?.[i]   ||0)).toFixed(2), close:+(+(q.close?.[i]||0)).toFixed(2),
          volume: q.volume?.[i]||0
        })));
        if (bars.length >= 5) { setPC(sym, bars, 60*60*1000); return bars; }
      }
    }
  } catch(e) { slog(`Yahoo q2 ${sym}: ${e.message}`); }

  FAILED_SYM.set(sym, Date.now());
  return null;
}

app.get('/api/price', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  const bars = await fetchPriceBars(sym);
  if (bars) return res.json(bars);
  res.status(404).json({ error: `No price data for ${sym}` });
});



// STATUS
app.get('/api/status', (req, res) => {
  const n      = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const latest = db.prepare('SELECT MAX(trade_date) AS d FROM trades').get().d;
  const synced = db.prepare('SELECT * FROM sync_log ORDER BY synced_at').all();
  res.json({ running: syncRunning, trades: n, latestTrade: latest, quarters: synced, log: syncLog.slice(-40) });
});

app.get('/api/daily-status', (req, res) => {
  try {
    const logs    = db.prepare('SELECT * FROM daily_log ORDER BY date DESC LIMIT 14').all();
    const recent  = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE filing_date >= date('now','-7 days')").get().n;
    const maxFile = db.prepare('SELECT MAX(filing_date) AS d FROM trades').get().d;
    res.json({ running: dailyRunning, recentDays: logs, tradesLast7d: recent, maxFilingDate: maxFile });
  } catch(e) { res.json({ running: dailyRunning, error: e.message }); }
});

app.get('/api/debug-form4', async (req, res) => {
  const https = require('https');
  const steps = [];

  function debugGet(url) {
    return new Promise((resolve) => {
      steps.push({ step: 'GET', url: url.slice(0,120) });
      const req = https.get(url, {
        headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
        timeout: 15000,
      }, r => {
        const chunks = [];
        r.on('data', c => chunks.push(c));
        r.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf8').slice(0, 300);
          steps.push({ status: r.statusCode, preview: body });
          resolve({ status: r.statusCode, body: Buffer.concat(chunks).toString('utf8') });
        });
      });
      req.on('error', e => { steps.push({ error: e.message }); resolve({ status: 0, body: '' }); });
      req.on('timeout', () => { req.destroy(); steps.push({ error: 'timeout' }); resolve({ status: 0, body: '' }); });
    });
  }

  try {
    const eftsUrl = 'https://efts.sec.gov/LATEST/search-index?forms=4&from=0&size=1';
    const { status: es, body: eb } = await debugGet(eftsUrl);
    let accession = '', filerCik = '', filingDate = '';
    if (es === 200) {
      const data = JSON.parse(eb);
      const hit  = data.hits?.hits?.[0];
      if (hit) {
        accession  = (hit._id || '').replace(/\//g, '-');
        filingDate = hit._source?.file_date || '';
        steps.push({ parsed: { accession, filingDate, raw_id: hit._id, source_keys: Object.keys(hit._source || {}) } });
      }
    }

    if (accession) {
      const acc = accession.replace(/-/g, '');
      filerCik = parseInt(acc.slice(0,10), 10).toString();
      steps.push({ derived: { filerCik, acc, accDash: accession } });

      const idxUrl = `https://www.sec.gov/Archives/edgar/data/${filerCik}/${acc}/${accession}-index.json`;
      const { status: is, body: ib } = await debugGet(idxUrl);
      if (is === 200) {
        const idx = JSON.parse(ib);
        steps.push({ indexDocs: (idx.documents || []).map(d => ({ type: d.type, document: d.document })) });
      }
    }
  } catch(e) {
    steps.push({ fatalError: e.message });
  }

  res.json({ steps });
});

app.get('/api/run-daily', (req, res) => {
  const days = parseInt(req.query.days || '10');
  if (req.query.force === '1') {
    db.prepare("DELETE FROM daily_log WHERE date >= date('now', ? || ' days')").run(`-${days}`);
    slog(`Cleared daily_log for last ${days} days`);
  }
  res.json({ started: !dailyRunning });
  if (!dailyRunning) runDaily(days);
});

let backfillRunning = false;
app.get('/api/backfill', (req, res) => {
  const days = Math.min(parseInt(req.query.days || '60'), 365);
  if (backfillRunning) return res.json({ started: false, reason: 'backfill already running' });
  backfillRunning = true;
  slog(`=== spawning one-shot backfill (${days} days) ===`);
  const worker = spawn(
    process.execPath,
    ['--max-old-space-size=400', path.join(__dirname, 'daily-worker.js'), String(days), 'backfill'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog(`[backfill] ${l}`)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog(`[backfill] ERR: ${l}`)));
  worker.on('exit', code => {
    backfillRunning = false;
    slog(`=== backfill worker exited (code ${code}) ===`);
  });
  res.json({ started: true, days });
});

app.get('/api/sync', (req, res) => {
  const numQ = parseInt(req.query.quarters || '4');
  res.json({ started: !syncRunning });
  if (!syncRunning) runSync(numQ);
});

app.get('/api/diag', async (req, res) => {
  const n      = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const latest = db.prepare('SELECT MAX(trade_date) AS d FROM trades').get().d;
  const oldest = db.prepare('SELECT MIN(trade_date) AS d FROM trades').get().d;
  const synced = db.prepare('SELECT * FROM sync_log').all();
  const byFiling = db.prepare(`SELECT filing_date AS d, COUNT(*) AS n FROM trades WHERE filing_date >= date('now','-14 days') GROUP BY filing_date ORDER BY filing_date DESC`).all();
  const byTrade  = db.prepare(`SELECT trade_date  AS d, COUNT(*) AS n FROM trades WHERE filing_date >= date('now','-14 days') GROUP BY trade_date  ORDER BY trade_date  DESC LIMIT 20`).all();
  let price = {};
  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`);
    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    price = { ok: status===200 && raw.length>0, bars: raw.length, latest: raw[0]?.date };
  } catch(e) { price = { ok: false, error: e.message }; }
  res.json({ db: { trades: n, latest, oldest, synced }, byFiling, byTrade, price, sync: { running: syncRunning, log: syncLog.slice(-10) } });
});

app.get('/api/health', (req, res) =>
  res.json({ ok: true, trades: db.prepare('SELECT COUNT(*) AS n FROM trades').get().n }));

app.get('/api/cleanup-dates', (req, res) => {
  const r = db.prepare(`
    DELETE FROM trades WHERE trade_date < '2000-01-01' OR trade_date > date('now') OR filing_date < '2000-01-01' OR filing_date > date('now')
  `).run();
  slog(`Date cleanup: removed ${r.changes} bad rows`);
  const { n } = db.prepare('SELECT COUNT(*) AS n FROM trades').get();
  res.json({ removed: r.changes, remaining: n });
});

// ─── SPA CATCH-ALL ────────────────────────────────────────────
// Must be last — serves index.html for all non-API, non-static routes
// so that /stock/AAPL, /insider, /insider/Name etc. work on direct load/refresh
app.get('*', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── STARTUP ──────────────────────────────────────────────────

// Add source column if not present (migration for existing DBs)
try {
  db.exec(`ALTER TABLE trades ADD COLUMN source TEXT DEFAULT 'sec'`);
  db.exec(`UPDATE trades SET source='sec' WHERE source IS NULL`);
} catch(e) {} // column already exists — fine

const existing  = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
const syncedQ   = db.prepare('SELECT COUNT(*) AS n FROM sync_log').get().n;
console.log(`DB: ${existing} trades, ${syncedQ} quarters synced`);

const TARGET_QUARTERS = 12;
if (syncedQ < TARGET_QUARTERS) {
  const needed = TARGET_QUARTERS - syncedQ;
  console.log(`Only ${syncedQ} quarters in DB — syncing ${needed} more to reach ${TARGET_QUARTERS}...`);
  runSync(TARGET_QUARTERS);
} else {
  const now = new Date();
  const yr  = now.getFullYear();
  const q   = Math.ceil((now.getMonth() + 1) / 3);
  let tq = q - 1; let ty = yr;
  if (tq < 1) { tq = 4; ty--; }
  const key = `${ty}Q${tq}`;
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(key);
  slog(`Re-syncing ${key} for latest filings...`);
  runSync(1);
}

setInterval(() => {
  const now = new Date();
  const yr  = now.getFullYear();
  const q   = Math.ceil((now.getMonth() + 1) / 3);
  let tq = q - 1; let ty = yr;
  if (tq < 1) { tq = 4; ty--; }
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${ty}Q${tq}`);
  runSync(1);
}, 24 * 60 * 60 * 1000);

setTimeout(() => runDaily(3), 5000);

// ─── PRICE CACHE WARMER ───────────────────────────────────────
// Pre-fetch prices for all active tickers so scoreboard/stock views are instant
async function warmPriceCache() {
  try {
    const tickers = db.prepare(`
      SELECT DISTINCT ticker FROM trades
      WHERE type='P' AND price>0 AND ticker IS NOT NULL
        AND trade_date >= date('now','-3 years')
      ORDER BY trade_date DESC
    `).all().map(r => r.ticker).slice(0, 120); // top 120 most recently active

    slog(`Warming price cache for ${tickers.length} tickers...`);
    // Batch of 15 at a time — fast without hammering sources
    for (let i = 0; i < tickers.length; i += 15) {
      const batch = tickers.slice(i, i + 15);
      await Promise.allSettled(batch.map(sym => fetchPriceBars(sym)));
      await new Promise(r => setTimeout(r, 200)); // small gap between batches
    }
    slog('Price cache warm-up complete');
  } catch(e) { slog('Price cache warmer error: ' + e.message); }
}

// Start warming 60s after boot — after congress sync has a head start
setTimeout(async () => {
  await warmPriceCache();
  // Pre-compute scoreboard while cache is hot
  try {
    slog('Pre-computing scoreboard...');
    const r = await fetch(`http://localhost:${PORT}/api/scoreboard?minbuys=4&limit=30`);
    if (r.ok) slog('Scoreboard pre-computed and cached');
  } catch(e) {}
}, 60000);
// Re-warm every 2 hours to keep cache fresh
setInterval(() => warmPriceCache(), 2 * 60 * 60 * 1000);


// Sources: Capitol Trades public API + House/Senate STOCK Act disclosures via Quiver
let congressSyncRunning = false;

async function syncCongressTrades() {
  if (congressSyncRunning) return;
  congressSyncRunning = true;
  slog('=== Starting congressional trade sync ===');

  const insert = db.prepare(`
    INSERT OR IGNORE INTO trades
      (ticker, company, insider, title, trade_date, filing_date, type, qty, price, value, owned, accession, source)
    VALUES
      (@ticker, @company, @insider, @title, @trade_date, @filing_date, @type, @qty, @price, @value, @owned, @accession, @source)
  `);

  let total = 0;

  function nt(str) {
    const s = (str||'').toLowerCase();
    if (s.includes('sale')||s.includes('sell')||s==='s') return 'S';
    if (s.includes('purchase')||s.includes('buy')||s==='p') return 'P';
    return 'X';
  }

  function mid(rangeStr) {
    // Handles "$1,001 - $15,000" or "$50,000" or "over $1,000,000"
    const n = (rangeStr||'').replace(/[^0-9\-]/g,'');
    const parts = n.split('-').map(Number).filter(x=>x>0);
    if (parts.length>=2) return Math.round((parts[0]+parts[1])/2);
    if (parts.length===1) return parts[0];
    return 0;
  }

  function batchInsert(rows) {
    if (!rows.length) return 0;
    let n = 0;
    const tx = db.transaction(items => {
      for (const row of items) {
        try { insert.run(row); n++; } catch(e) {}
      }
    });
    tx(rows);
    return n;
  }

  // ── Source A: House Stock Watcher (S3, covers all House PTR filings) ────────
  // File is ~8-12MB — stream and parse in chunks
  try {
    slog('Fetching House Stock Watcher data...');
    const { status, body } = await get(
      'https://house-stock-watcher-data.s3-us-east-2.amazonaws.com/data/all_transactions.json',
      45000  // larger file needs more time
    );
    slog(`House S3 status: ${status}, body size: ${body.length}`);
    if (status === 200 && body.length > 100) {
      const trades = JSON.parse(body.toString('utf8'));
      if (Array.isArray(trades)) {
        const rows = [];
        for (const t of trades) {
          const ticker = (t.ticker||'').toUpperCase().replace(/[^A-Z0-9.]/g,'').trim();
          if (!ticker || ticker==='--' || ticker.length>10) continue;
          const td = (t.transaction_date||t.disclosure_date||'').slice(0,10);
          const fd = (t.disclosure_date||td).slice(0,10);
          if (!td||td<'2012-01-01') continue;
          const name = (t.representative||'').replace(/^(Hon\.?|Dr\.?|Mr\.?|Ms\.?|Mrs\.?)\s*/i,'').trim();
          if (!name) continue;
          rows.push({
            ticker, company: (t.asset_description||ticker).slice(0,200),
            insider: name, title: 'Representative',
            trade_date: td, filing_date: fd,
            type: nt(t.type), qty:0, price:0, value: mid(t.amount),
            owned:0, accession: `hsw-${name.replace(/\W+/g,'-').toLowerCase()}-${ticker}-${td}`,
            source:'congress'
          });
        }
        const n = batchInsert(rows);
        total += n;
        slog(`House Stock Watcher: processed ${rows.length} records, inserted ${n}`);
      }
    }
  } catch(e) { slog(`House Stock Watcher ERROR: ${e.message}`); }

  // ── Source B: Senate Stock Watcher (S3, covers all Senate PTR filings) ──────
  try {
    slog('Fetching Senate Stock Watcher data...');
    const { status, body } = await get(
      'https://senate-stock-watcher-data.s3-us-east-2.amazonaws.com/aggregate/all_transactions.json',
      45000
    );
    slog(`Senate S3 status: ${status}, body size: ${body.length}`);
    if (status === 200 && body.length > 100) {
      const trades = JSON.parse(body.toString('utf8'));
      if (Array.isArray(trades)) {
        const rows = [];
        for (const t of trades) {
          const ticker = (t.ticker||'').toUpperCase().replace(/[^A-Z0-9.]/g,'').trim();
          if (!ticker || ticker==='--' || ticker.length>10) continue;
          const td = (t.transaction_date||t.disclosure_date||'').slice(0,10);
          const fd = (t.disclosure_date||td).slice(0,10);
          if (!td||td<'2012-01-01') continue;
          // Senate watcher uses first_name/last_name or senator field
          const rawName = t.senator || ((t.first_name||'')+' '+(t.last_name||'')).trim();
          const name = rawName.replace(/^(Hon\.?|Sen\.?|Dr\.?)\s*/i,'').trim();
          if (!name) continue;
          rows.push({
            ticker, company: (t.asset_description||ticker).slice(0,200),
            insider: name, title: 'Senator',
            trade_date: td, filing_date: fd,
            type: nt(t.type), qty:0, price:0, value: mid(t.amount),
            owned:0, accession: `ssw-${name.replace(/\W+/g,'-').toLowerCase()}-${ticker}-${td}`,
            source:'congress'
          });
        }
        const n = batchInsert(rows);
        total += n;
        slog(`Senate Stock Watcher: processed ${rows.length} records, inserted ${n}`);
      }
    }
  } catch(e) { slog(`Senate Stock Watcher ERROR: ${e.message}`); }

  slog(`=== Congressional sync complete: ${total} total rows inserted ===`);
  congressSyncRunning = false;
}
// Manual trigger endpoint
app.post('/api/sync-congress', async (req, res) => {
  if (congressSyncRunning) return res.json({ status: 'running', message: 'Sync already in progress' });
  syncCongressTrades();
  res.json({ status: 'started', message: 'Congressional trade sync started' });
});

app.get('/api/congress-status', (req, res) => {
  try {
    const total   = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE source='congress'").get().n;
    const latest  = db.prepare("SELECT MAX(trade_date) AS d FROM trades WHERE source='congress'").get().d;
    const members = db.prepare("SELECT COUNT(DISTINCT insider) AS n FROM trades WHERE source='congress'").get().n;
    res.json({ total, latest, members, syncing: congressSyncRunning });
  } catch(e) { res.status(500).json({ error: e.message }); }
});



// Expose congress trade filter to screener
app.get('/api/congress', (req, res) => {
  try {
    const days  = Math.min(parseInt(req.query.days || '90'), 730);
    const limit = Math.min(parseInt(req.query.limit || '500'), 2000);
    const rows  = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned, source
      FROM trades
      WHERE source = 'congress'
        AND filing_date >= date('now', '-' || ? || ' days')
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT ?
    `).all(days, limit);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Auto-sync congressional trades every 6 hours
// Congress fires at 10s — before price warmer (60s) to avoid DB contention
setTimeout(() => syncCongressTrades(), 10000);
setInterval(() => syncCongressTrades(), 6 * 60 * 60 * 1000);


app.listen(PORT, () => console.log(`Server on port ${PORT}`));
