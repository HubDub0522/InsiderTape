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

// Each table/index created separately so existing DBs get new tables too
db.exec(`CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
  trade_date TEXT NOT NULL, filing_date TEXT,
  type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER, accession TEXT,
  UNIQUE(accession, insider, trade_date, type, qty)
)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_ticker      ON trades(ticker)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_date  ON trades(trade_date DESC)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_filing_date ON trades(filing_date DESC)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_insider     ON trades(insider)`);

// ─── Clean up bad/invalid trade records ──────────────────────
{
  const r = db.prepare(`
    DELETE FROM trades
    WHERE ticker IN ('N/A','NA','NONE','NULL','--','-','.')
       OR ticker NOT GLOB '[A-Z]*'
       OR LENGTH(ticker) < 1 OR LENGTH(ticker) > 10
       OR COALESCE(company,'') IN ('N/A','NA','None','NULL','--','-')
  `).run();
  if (r.changes > 0) console.log(`Cleaned up ${r.changes} invalid trade records`);
}

db.exec(`CREATE TABLE IF NOT EXISTS sync_log (
  quarter TEXT PRIMARY KEY, synced_at TEXT DEFAULT (datetime('now')), rows INTEGER
)`);

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
const http = require('http');

function get(url, ms=30000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('http://') ? http : https;
    const req = mod.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; InsiderTape/1.0)' },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        // Drain the redirect response body before following
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return get(next, ms).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}


// ─── DAILY INGESTION (recent Form 4s from EDGAR daily index) ────
let dailyRunning = false;

function runDaily(daysBack = 3) {
  if (dailyRunning) return;

  // Smart scheduling: only poll during market hours (Mon–Fri, 7am–8pm ET)
  // Outside those hours, sleep until the next morning open
  const now = new Date();
  // Convert to US Eastern time
  const etOffset = -5; // EST (UTC-5); DST handled approximately
  const etHour = (now.getUTCHours() + 24 + etOffset) % 24;
  const etDay  = new Date(now.getTime() + etOffset * 3600000).getUTCDay(); // 0=Sun,6=Sat
  const isWeekday = etDay >= 1 && etDay <= 5;
  const isMarketHours = etHour >= 7 && etHour < 20; // 7am–8pm ET (4 hrs after 4pm close)

  if (!isWeekday || !isMarketHours) {
    // Sleep until 7:05am ET next weekday
    const msUntilOpen = msUntilNextOpen(now, etOffset);
    slog(`=== daily-worker sleeping ${Math.round(msUntilOpen/60000)}min until market hours ===`);
    setTimeout(() => runDaily(2), msUntilOpen);
    return;
  }

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
    slog(`=== daily-worker exited (code ${code}) — restarting in 2min ===`);
    setTimeout(() => runDaily(2), 2 * 60 * 1000); // 2 min between runs during market hours
  });
}

function msUntilNextOpen(now, etOffset) {
  // Find next weekday 7:05am ET
  const target = new Date(now);
  target.setUTCHours(7 - etOffset, 5, 0, 0); // 7:05am ET in UTC
  if (target <= now) target.setUTCDate(target.getUTCDate() + 1);
  // Skip weekends
  while (true) {
    const day = new Date(target.getTime() + etOffset * 3600000).getUTCDay();
    if (day >= 1 && day <= 5) break;
    target.setUTCDate(target.getUTCDate() + 1);
  }
  return Math.max(target - now, 60000); // at least 1 min
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

    const days  = Math.min(Math.max(parseInt(req.query.days || '30'), 1), 1095);
    const limit = Math.min(parseInt(req.query.limit || '1000'), 2000);

        let rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned,
             COALESCE(MAX(source), 'sec') AS source
      FROM trades
      WHERE filing_date >= date('now', '-' || ? || ' days')
        AND type IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT ?
    `).all(days, limit);

    if (!rows.length && n > 0) {
      rows = db.prepare(`
        SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
               trade_date AS trade, MAX(filing_date) AS filing,
               type, MAX(qty) AS qty, MAX(price) AS price,
               MAX(value) AS value, MAX(owned) AS owned,
               COALESCE(MAX(source), 'sec') AS source
        FROM trades
        WHERE (source IS NULL OR source = 'sec')
        AND type IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
        GROUP BY ticker, insider, trade_date, type
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
    // Sort by transaction date, filter derivatives, deduplicate amendments
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned
      FROM trades
      WHERE ticker = ?
        AND type IN ('P','S','S-')
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT 5000
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
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned
      FROM trades WHERE UPPER(insider) LIKE UPPER(?)
      GROUP BY ticker, insider, trade_date, type
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
  if (res.headersSent) return;
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






// ─── EVENTS: Earnings + catalysts per symbol ─────────────────────────────────
// Returns earnings dates (historical + upcoming) from FMP.
// Also returns recent 8-K filing dates from SEC EDGAR EFTS as catalyst proxy.
// Frontend uses this to detect insider buys in the window before known events.
const EVENT_CACHE = new Map(); // sym → { expires, data }

app.get('/api/events', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });

  const cached = EVENT_CACHE.get(sym);
  if (cached && Date.now() < cached.expires) return res.json(cached.data);

  const events = [];

  // ── FMP: historical + upcoming earnings ────────────────────────
  try {
    const url = `https://financialmodelingprep.com/stable/historical/earning-calendar/${sym}?apikey=${FMP}`;
    const { status, body } = await get(url, 15000);
    if (status === 200) {
      const data = JSON.parse(body.toString());
      const arr  = Array.isArray(data) ? data : (data?.historical || []);
      arr.forEach(e => {
        const d = (e.date || e.reportDate || '').slice(0, 10);
        if (!d) return;
        events.push({
          date: d,
          type: 'EARNINGS',
          label: `Earnings${e.eps !== undefined ? ` (EPS: ${e.eps ?? 'est'})` : ''}`,
          source: 'FMP',
        });
      });
    }
  } catch(e) { /* silent */ }

  // ── SEC EFTS: recent 8-K filings = material events ─────────────
  // 8-K items that signal material events: 1.01 (agreements), 2.02 (results),
  // 5.02 (officer changes), 8.01 (other material events)
  try {
    const url = `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(sym)}%22&forms=8-K&dateRange=custom&startDate=${new Date(Date.now()-730*86400000).toISOString().slice(0,10)}&endDate=${new Date().toISOString().slice(0,10)}&hits.hits.total.value=1&hits.hits._source.period_of_report=1`;
    const { status, body } = await get(url, 10000);
    if (status === 200) {
      const json = JSON.parse(body.toString());
      const hits = json?.hits?.hits || [];
      hits.forEach(h => {
        const d = (h._source?.period_of_report || h._source?.file_date || '').slice(0, 10);
        const items = (h._source?.items || '').toString();
        if (!d) return;
        // Classify item type
        let type = 'MATERIAL_EVENT', label = '8-K Filing';
        if (items.includes('2.02') || items.includes('Results')) { type = 'EARNINGS'; label = '8-K: Earnings Results'; }
        else if (items.includes('1.01')) { label = '8-K: Material Agreement'; }
        else if (items.includes('8.01')) { label = '8-K: Other Material Event'; }
        else if (items.includes('1.05') || items.includes('FDA') || items.includes('regulatory')) { type = 'REGULATORY'; label = '8-K: Regulatory Update'; }
        events.push({ date: d, type, label, source: 'SEC_8K' });
      });
    }
  } catch(e) { /* silent */ }

  // ── SEC EFTS: SC 13G/13D (activist/large holder — often precede announcements)
  // Skip — too noisy

  // Deduplicate by date+type, sort descending
  const seen = new Set();
  const deduped = events.filter(e => {
    const k = `${e.date}::${e.type}`;
    if (seen.has(k)) return false;
    seen.add(k); return true;
  }).sort((a, b) => b.date.localeCompare(a.date));

  EVENT_CACHE.set(sym, { expires: Date.now() + 6 * 60 * 60 * 1000, data: deduped });
  res.json(deduped);
});

// STOCK LISTS — for stock view landing page
// Returns multiple ranked lists in one call: most active, recent buys, recent sells, cluster buys
app.get('/api/stock-lists', (req, res) => {
  try {
    // All queries use a dedup subquery to prevent double-counting trades that appear
    // in both the quarterly bulk sync and the daily worker (different accession formats)
    const DEDUP = (filter, days, dateField='filing_date') => `
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date, type, MAX(COALESCE(value,0)) AS value, MAX(COALESCE(qty,0)) AS qty
      FROM trades
      WHERE ${filter} AND ${dateField} >= date('now','-${days} days')
        AND (source IS NULL OR source='sec')
        AND type IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*'
        AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
      GROUP BY ticker, insider, trade_date, type
    `;

    const hotBuys = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        COUNT(CASE WHEN type='P' THEN 1 END) AS buys,
        COUNT(DISTINCT CASE WHEN type='P' THEN insider END) AS buyers,
        SUM(CASE WHEN type='P' THEN value ELSE 0 END) AS buy_val,
        MAX(CASE WHEN type='P' THEN trade_date END) AS latest,
        MAX(CASE WHEN type='P' AND (UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%'
          OR UPPER(title) LIKE '%PRESIDENT%' OR UPPER(title) LIKE '%CHAIRMAN%') THEN 1 ELSE 0 END) AS exec_buy
      FROM (${DEDUP("type='P'", 30)})
      GROUP BY ticker HAVING buys > 0
      ORDER BY buy_val DESC LIMIT 20
    `).all();

    const clusterBuys = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        COUNT(DISTINCT insider) AS buyer_count,
        COUNT(*) AS trade_count,
        SUM(value) AS total_val,
        MAX(trade_date) AS latest
      FROM (${DEDUP("type='P'", 14)})
      GROUP BY ticker HAVING buyer_count >= 2
      ORDER BY buyer_count DESC, total_val DESC LIMIT 15
    `).all();

    const freshBuys = db.prepare(`
      SELECT d.ticker, MAX(d.company) AS company,
        d.insider, MAX(d.title) AS title,
        MAX(d.trade_date) AS latest, MAX(d.value) AS val
      FROM (${DEDUP("type='P'", 14)}) d
      WHERE NOT EXISTS (
        SELECT 1 FROM trades t2
        WHERE t2.ticker=d.ticker AND t2.insider=d.insider AND t2.type='P'
          AND t2.trade_date < d.trade_date
          AND t2.trade_date >= date(d.trade_date,'-730 days')
      )
      GROUP BY d.ticker, d.insider
      ORDER BY val DESC LIMIT 15
    `).all();

    const heavySells = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        COUNT(DISTINCT insider) AS seller_count,
        SUM(value) AS sell_val,
        MAX(trade_date) AS latest
      FROM (${DEDUP("type IN ('S','S-')", 14)})
      GROUP BY ticker
      ORDER BY sell_val DESC LIMIT 15
    `).all();

    const mostActive = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        COUNT(*) AS total_trades,
        COUNT(DISTINCT insider) AS insiders,
        SUM(CASE WHEN type='P' THEN 1 ELSE 0 END) AS buys,
        SUM(CASE WHEN type IN ('S','S-') THEN 1 ELSE 0 END) AS sells,
        MAX(trade_date) AS latest
      FROM (${DEDUP("1=1", 7)})
      GROUP BY ticker
      ORDER BY total_trades DESC, insiders DESC LIMIT 20
    `).all();

    res.json({ hotBuys, clusterBuys, freshBuys, heavySells, mostActive });
  } catch(e) { res.status(500).json({ error: e.message }); }
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

// Start warming 60s after boot — 60s after boot
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
app.listen(PORT, () => console.log(`Server on port ${PORT}`));
