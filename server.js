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

// Per-request timeout: kill any request that takes > 55s so it returns a proper error
// rather than hanging until the platform kills the whole dyno.
// Scoreboard needs ~10-15s for parallel price fetches; 55s gives comfortable headroom.
app.use((req, res, next) => {
  const limit = req.path === '/api/scoreboard' ? 55000
              : (req.path === '/api/drift' || req.path === '/api/proximity') ? 45000
              : 25000;
  res.setTimeout(limit, () => {
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
db.exec(`CREATE INDEX IF NOT EXISTS idx_ticker_date_price ON trades(ticker, trade_date, price)`);

// ─── Clean up bad/invalid trade records ──────────────────────
{
  // Remove invalid tickers
  const r1 = db.prepare(`
    DELETE FROM trades
    WHERE ticker IN ('N/A','NA','NONE','NULL','--','-','.')
       OR ticker NOT GLOB '[A-Z]*'
       OR LENGTH(ticker) < 1 OR LENGTH(ticker) > 10
       OR COALESCE(company,'') IN ('N/A','NA','None','NULL','--','-')
  `).run();
  if (r1.changes > 0) console.log(`Cleaned up ${r1.changes} invalid ticker records`);

  // Remove non-open-market transaction codes.
  // Only keep P (open-market buy), S (open-market sell), S- (sale under 10b5-1 plan).
  // Conversions (C), exercises (M), awards (A), gifts (G), tax withholding (F),
  // disposals (D), inheritance (W), trust transfers (Z), etc. are not market trades
  // and produce fabricated values (qty * exercise_price = billions of fake dollars).
  const r2 = db.prepare(`
    DELETE FROM trades
    WHERE TRIM(type) NOT IN ('P', 'S', 'S-')
  `).run();
  if (r2.changes > 0) console.log(`Removed ${r2.changes} non-market transaction records (conversions, exercises, awards, etc.)`);

  // Remove records with implausible values that slipped through
  const r3 = db.prepare(`
    DELETE FROM trades
    WHERE value > 2000000000
       OR price > 1500000
       OR qty   > 50000000
  `).run();
  if (r3.changes > 0) console.log(`Removed ${r3.changes} records with implausible values (likely derivative artifacts)`);
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
    const limit = Math.min(parseInt(req.query.limit || '5000'), 10000);

        let rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned,
             COALESCE(MAX(source), 'sec') AS source
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT ?
    `).all(days, limit);

    if (!rows.length && n > 0) {
      // Smart fallback: use the most recent available window relative to MAX(trade_date)
      // This handles the case where the daily worker hasn't inserted recent trades
      const maxDate = db.prepare('SELECT MAX(trade_date) AS d FROM trades WHERE trade_date IS NOT NULL').get().d;
      if (maxDate) {
        rows = db.prepare(`
          SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
                 trade_date AS trade, MAX(filing_date) AS filing,
                 TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
                 MAX(value) AS value, MAX(owned) AS owned,
                 COALESCE(MAX(source), 'sec') AS source
          FROM trades
          WHERE trade_date >= date(?, '-' || ? || ' days')
            AND TRIM(type) IN ('P','S','S-')
            AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
            AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
            AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
          GROUP BY ticker, insider, trade_date, type
          ORDER BY trade_date DESC, filing_date DESC
          LIMIT ?
        `).all(maxDate, days, limit);
      }
    }

    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// HISTORY — like screener but uses trade_date (not filing_date) for the window
// Used by analysis tools that need historical data: drift, regime shift, first-buy
app.get('/api/history', (req, res) => {
  try {
    const days  = Math.min(Math.max(parseInt(req.query.days || '1095'), 1), 1825);
    const limit = Math.min(parseInt(req.query.limit || '10000'), 25000);

    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
        AND insider IS NOT NULL
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC
      LIMIT ?
    `).all(days, limit);

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
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned
      FROM trades
      WHERE ticker = ?
        AND TRIM(type) IN ('P','S','S-')
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
    const limit = exact ? 2000 : 500;
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned
      FROM trades
      WHERE UPPER(insider) LIKE UPPER(?)
        AND TRIM(type) IN ('P','S','S-')
        AND COALESCE(value, 0) <= 2000000000
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
      WITH deduped AS (
        -- Deduplicate: one row per insider+ticker+trade_date (pick latest filing)
        SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
               trade_date, MAX(filing_date) AS filing_date,
               MAX(price) AS price, MAX(qty) AS qty,
               MAX(value) AS value, MAX(owned) AS owned
        FROM trades
        WHERE TRIM(type) = 'P'
          AND insider IS NOT NULL
          AND ticker  IS NOT NULL
        GROUP BY insider, ticker, trade_date
      ),
      buys AS (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY insider, ticker
                 ORDER BY trade_date DESC
               ) AS rn
        FROM deduped
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
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END)                            AS buy_count,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END)             AS buyer_count,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END)       AS total_buy_val,
        MAX(CASE WHEN TRIM(type)='P' THEN trade_date ELSE NULL END)           AS latest_buy_date,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END)                  AS sell_count,
        COUNT(DISTINCT CASE WHEN TRIM(type) IN ('S','S-') THEN insider END)   AS seller_count,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS total_sell_val,
        MAX(CASE WHEN TRIM(type) IN ('S','S-') THEN trade_date ELSE NULL END) AS latest_sell_date,
        MAX(CASE WHEN TRIM(type)='P' AND (
          UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%' OR
          UPPER(title) LIKE '%PRESIDENT%' OR UPPER(title) LIKE '%CHAIRMAN%' OR
          UPPER(title) LIKE '%FOUNDER%' OR UPPER(title) LIKE '%COO%'
        ) THEN 1 ELSE 0 END)                                            AS has_exec_buyer,
        CAST(julianday('now') - julianday(
          MAX(CASE WHEN TRIM(type)='P' THEN trade_date ELSE NULL END)
        ) AS INTEGER)                                                    AS days_since_buy,
        MAX(CASE WHEN TRIM(type)='P' AND owned>qty AND qty>0
          THEN CAST(qty*100.0/(owned-qty) AS INTEGER) ELSE 0 END)       AS max_stake_pct
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days')
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
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END)                               AS buy_count,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN ticker END)                 AS ticker_count,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END)          AS total_buy_val,
        MAX(CASE WHEN TRIM(type)='P' THEN trade_date ELSE NULL END)               AS latest_buy,
        MIN(CASE WHEN TRIM(type)='P' THEN trade_date ELSE NULL END)               AS earliest_buy,
        GROUP_CONCAT(DISTINCT CASE WHEN TRIM(type)='P' THEN ticker END)          AS tickers_csv
      FROM trades
      WHERE insider IS NOT NULL
        AND TRIM(type) = 'P'
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
const SCOREBOARD_TTL = 6 * 60 * 60 * 1000; // 6 hours

app.get('/api/scoreboard', (req, res) => {
  // Return cached result if fresh
  if (_scoreboardCache && Date.now() - _scoreboardCacheTime < SCOREBOARD_TTL) {
    return res.json(_scoreboardCache);
  }
  try {
    const minBuys = parseInt(req.query.minbuys || '4');
    const limit   = Math.min(parseInt(req.query.limit || '30'), 60);

    // Pull insiders with enough buy history
    const rows = db.prepare(`
      SELECT
        insider,
        MAX(title) AS title,
        COUNT(*) AS buy_count,
        GROUP_CONCAT(ticker || '|' || trade_date || '|' || COALESCE(price,0) || '|' || COALESCE(value,0), ';;') AS trade_data,
        GROUP_CONCAT(DISTINCT ticker) AS tickers_csv,
        CAST(julianday(MAX(trade_date)) - julianday(MIN(trade_date)) AS INTEGER) AS span_days
      FROM trades
      WHERE TRIM(type) = 'P'
        AND insider IS NOT NULL AND ticker IS NOT NULL
        AND price > 0
        AND trade_date <= date('now', '-95 days')
      GROUP BY insider
      HAVING buy_count >= ? AND span_days >= 90
      ORDER BY buy_count DESC, span_days DESC
      LIMIT ?
    `).all(minBuys, limit);

    if (!rows.length) {
      const empty = { accuracy: [], timing: [] };
      _scoreboardCache = empty; _scoreboardCacheTime = Date.now();
      return res.json(empty);
    }

    // Build a DB-only price series per ticker using indexed per-ticker queries.
    // Capped at 60 tickers to avoid blocking the SQLite event loop.
    // Each query uses the idx_ticker_date_price covering index — fast range scan.
    const allTickers = [...new Set(rows.flatMap(r => (r.tickers_csv||'').split(',').filter(Boolean)))].slice(0, 60);
    const priceCache = {};
    const priceStmt = db.prepare(`
      SELECT trade_date, AVG(price) AS price
      FROM trades
      WHERE ticker = ? AND price > 0
      GROUP BY trade_date
      ORDER BY trade_date
    `);
    allTickers.forEach(sym => {
      const warmBars = getPC(sym);
      if (warmBars && warmBars.length > 10) {
        priceCache[sym] = warmBars; // prefer full OHLC from warm cache
      } else {
        const rows2 = priceStmt.all(sym);
        if (rows2.length) {
          priceCache[sym] = rows2.map(r => ({ time: r.trade_date, close: r.price, high: r.price, low: r.price }));
        }
      }
    });

    // Score every insider
    const accuracyResults = [], timingResults = [];
    rows.forEach(leader => {
      try {
        const rawTrades = (leader.trade_data||'').split(';;').map(s => {
          const [ticker, trade_date, priceStr, valueStr] = s.split('|');
          return { ticker, trade: trade_date, price: parseFloat(priceStr)||0, value: parseFloat(valueStr)||0 };
        }).filter(t => t.ticker && t.trade && parseFloat(t.price)>0);
        if (rawTrades.length < 4) return;

        const scored = rawTrades.map(t => {
          const bars = priceCache[t.ticker] || [];
          if (!bars.length) return null;
          const buyDate = t.trade.slice(0, 10);
          const buyPrice = t.price;
          if (!buyPrice || buyPrice <= 0) return null;
          const fwd = days => {
            const fd = new Date(buyDate + 'T12:00:00Z'); fd.setUTCDate(fd.getUTCDate() + days);
            const fs = fd.toISOString().slice(0, 10);
            // Find nearest price observation within ±45 days
            let best = null, bestDiff = Infinity;
            bars.forEach(b => {
              if (b.time <= buyDate) return;
              const diff = Math.abs(new Date(b.time+'T00:00:00Z') - new Date(fs+'T00:00:00Z')) / 86400000;
              if (diff < bestDiff && diff <= 45) { best = b; bestDiff = diff; }
            });
            return best ? +((best.close - buyPrice) / buyPrice * 100).toFixed(2) : null;
          };
          return { ticker: t.ticker, tradeDate: buyDate, buyPrice, ret30: fwd(30), ret90: fwd(90), ret180: fwd(180) };
        }).filter(Boolean);

        const completed = scored.filter(s => s.ret90 !== null);
        if (completed.length < 3) return;

        const rets90   = completed.map(s => s.ret90);
        const avgRet90 = +(rets90.reduce((a,b)=>a+b,0)/rets90.length).toFixed(1);
        const rets30   = completed.filter(s=>s.ret30!==null).map(s=>s.ret30);
        const avgRet30 = rets30.length ? +(rets30.reduce((a,b)=>a+b,0)/rets30.length).toFixed(1) : null;
        const winRate  = Math.round(rets90.filter(r=>r>0).length/rets90.length*100);
        const avgMag   = +(rets90.map(Math.abs).reduce((a,b)=>a+b,0)/rets90.length).toFixed(1);
        const sorted   = [...rets90].sort((a,b)=>a-b);
        const median   = sorted[Math.floor(sorted.length/2)];
        const consist  = Math.round(Math.min(100,Math.max(0,(median/Math.max(avgMag,1)+1)*50)));
        const accScore = Math.round(Math.min(100,Math.max(0,winRate*0.40+Math.min(35,Math.max(0,avgRet90/20*35))+consist*0.15+Math.min(10,completed.length*1.2))));
        const tier = accScore>=75?'ELITE':accScore>=55?'STRONG':accScore>=35?'AVERAGE':'WEAK';
        const tickers3 = [...new Set(rawTrades.map(t=>t.ticker))].slice(0,3).join(', ');
        accuracyResults.push({ name:leader.insider, title:leader.title||'', accuracyScore:accScore, tier, winRate, avgRet90, avgRet30, tradeCount:completed.length, tickers:tickers3 });

        // Timing alpha
        const yr1Bars = (sym) => {
          const b = priceCache[sym] || [];
          const cutoff = new Date(); cutoff.setFullYear(cutoff.getFullYear()-1);
          return b.filter(x => new Date(x.time+'T00:00:00Z') >= cutoff);
        };
        let nearLowCount = 0;
        let ret90sum = 0, retN = 0, ret180sum = 0;
        const tickers = [...new Set(rawTrades.map(t=>t.ticker))].slice(0,3);
        completed.forEach(s => {
          const bars1 = yr1Bars(s.ticker);
          if (bars1.length >= 10) {
            const lo = Math.min(...bars1.map(b=>b.low||b.close));
            if ((s.buyPrice - lo) / lo * 100 <= 20) nearLowCount++;
          }
          if (s.ret90 !== null) { ret90sum += s.ret90; retN++; }
          if (s.ret180 !== null) ret180sum += s.ret180;
        });
        const nearLowPct = completed.length > 0 ? Math.round(nearLowCount/completed.length*100) : 0;
        const avgFwd90  = retN > 0 ? +(ret90sum/retN).toFixed(1) : null;
        const avgFwd180 = retN > 0 ? +(ret180sum/retN).toFixed(1) : null;
        const timingAlpha = Math.min(100,Math.max(0,Math.round(
          Math.min(25,(nearLowPct/100)*50) +
          Math.min(25,Math.max(0,(avgFwd90||0)/10*25)) + 25
        )));
        let verdict, verdictColor;
        if (timingAlpha>=75&&nearLowPct>=50){verdict='Buys near bottoms';verdictColor='buy';}
        else if (timingAlpha>=75){verdict='Elite forward returns';verdictColor='buy';}
        else if (timingAlpha>=55){verdict='Above-average timing';verdictColor='accent';}
        else if (timingAlpha>=35){verdict='Mixed timing signals';verdictColor='option';}
        else{verdict='Tends to buy high';verdictColor='sell';}
        if (timingAlpha>=30) timingResults.push({ name:leader.insider, title:leader.title||'', timingAlpha, nearLowPct, avgRet90:avgFwd90, avgRet180:avgFwd180, verdict, verdictColor, tradeCount:completed.length, tickers:tickers3 });
      } catch(e) { /* skip insider on error */ }
    });

    accuracyResults.sort((a,b)=>b.accuracyScore-a.accuracyScore);
    timingResults.sort((a,b)=>b.timingAlpha-a.timingAlpha);
    const result = { accuracy: accuracyResults, timing: timingResults };
    _scoreboardCache = result;
    _scoreboardCacheTime = Date.now();
    res.json(result);
  } catch(e) {
    slog('scoreboard error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});



// PRICE — warm in-memory cache + external fallback
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

  // ── Source 1: Stooq (reliable, no auth, global coverage) ─────
  try {
    const { status, body } = await get(
      `https://stooq.com/q/d/l/?s=${sym.toLowerCase()}.us&i=d`, 8000
    );
    if (status === 200) {
      const text = body.toString();
      const lines = text.trim().split('\n');
      // CSV: Date,Open,High,Low,Close,Volume
      if (lines.length > 2 && lines[0].toLowerCase().includes('date')) {
        const bars = norm(lines.slice(1).map(line => {
          const [date, open, high, low, close, volume] = line.split(',');
          return { time: (date||'').trim(), open: +(+open).toFixed(2), high: +(+high).toFixed(2), low: +(+low).toFixed(2), close: +(+close).toFixed(2), volume: +(volume||0) };
        }).filter(b => b.time && b.close > 0));
        if (bars.length >= 5) { setPC(sym, bars, 60*60*1000); return bars; }
      }
    }
  } catch(e) { /* try next */ }

  // ── Source 2: FMP ─────────────────────────────────────────────
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${sym}&apikey=${FMP}`, 8000
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

  // ── Source 3: Yahoo Finance query1 ───────────────────────────
  try {
    const now = Math.floor(Date.now()/1000), from = now - 6*365*86400;
    const { status, body } = await get(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?period1=${from}&period2=${now}&interval=1d&events=history`, 8000
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

  // ── Source 4: Yahoo Finance query2 (different CDN) ───────────
  try {
    const now = Math.floor(Date.now()/1000), from = now - 6*365*86400;
    const { status, body } = await get(
      `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?period1=${from}&period2=${now}&interval=1d`, 8000
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

// PRICES-BULK — returns price bars for multiple tickers in one request.
// Serves from warm in-memory cache; fetches cold misses in parallel (capped at 20 tickers).
// Used by Post-Buy Drift Analyzer to avoid 150 individual round-trips.
app.get('/api/prices-bulk', (req, res) => {
  const raw = (req.query.symbols || '').toUpperCase().trim();
  if (!raw) return res.status(400).json({ error: 'symbols required' });
  const syms = [...new Set(raw.split(',').map(s => s.trim()).filter(Boolean))].slice(0, 50);

  // Respond IMMEDIATELY with whatever is in warm cache — never block on external fetches.
  // Fire cold-miss fetches in background so they'll be warm on the next request.
  const result = {};
  const coldMisses = [];
  syms.forEach(s => {
    const bars = getPC(s);
    if (bars) result[s] = bars;
    else coldMisses.push(s);
  });
  res.json(result); // instant response

  // Warm up cold misses in background (don't await)
  if (coldMisses.length) {
    Promise.allSettled(coldMisses.map(s => fetchPriceBars(s))).catch(() => {});
  }
});

// POST-BUY DRIFT — computed entirely from the DB's own trade price records.
// No external price API needed. Uses price observations already in the DB
// (every trade has a price + date) to build a sparse price series per ticker,
// then measures D+30, D+90, D+180 returns relative to each buy price.
let _driftServerCache = null;
let _driftServerCacheTime = 0;
const DRIFT_TTL = 60 * 60 * 1000; // 1 hour

app.get('/api/drift', (req, res) => {
  if (_driftServerCache && Date.now() - _driftServerCacheTime < DRIFT_TTL) {
    return res.json(_driftServerCache);
  }
  try {
    // Fast query: only buy trades (type='P') with a price, last 4 years
    // Two-pass: first get all buys per ticker, then get all price observations
    const buyRows = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        trade_date, AVG(price) AS price, SUM(COALESCE(value,0)) AS val
      FROM trades
      WHERE TRIM(type) = 'P' AND price > 0 AND ticker IS NOT NULL
        AND trade_date >= date('now','-4 years')
      GROUP BY ticker, trade_date
      ORDER BY ticker, trade_date
    `).all();

    // Get all price observations (any type) per ticker for forward price lookups
    const priceRows = db.prepare(`
      SELECT ticker, trade_date, AVG(price) AS price
      FROM trades
      WHERE price > 0 AND ticker IS NOT NULL
        AND trade_date >= date('now','-4 years')
      GROUP BY ticker, trade_date
      ORDER BY ticker, trade_date
    `).all();

    // Build price series per ticker
    const priceSeriesByTicker = {};
    priceRows.forEach(r => {
      if (!priceSeriesByTicker[r.ticker]) priceSeriesByTicker[r.ticker] = [];
      priceSeriesByTicker[r.ticker].push({ date: r.trade_date, price: r.price });
    });

    // Group buys by ticker
    const buysByTicker = {};
    const companyByTicker = {};
    buyRows.forEach(r => {
      if (!buysByTicker[r.ticker]) { buysByTicker[r.ticker] = []; companyByTicker[r.ticker] = r.company; }
      buysByTicker[r.ticker].push({ date: r.trade_date, price: r.price, val: r.val });
    });

    const results = [];
    const cutoff = new Date(Date.now() - 180 * 86400000);

    for (const [ticker, buys] of Object.entries(buysByTicker)) {
      if (buys.length < 3) continue;

      const priceSeries = priceSeriesByTicker[ticker] || [];
      const TARGETS = [30, 90, 180];
      const retsByTarget = { 30: [], 90: [], 180: [] };
      let measured = 0;

      buys.forEach(buy => {
        const buyDt = new Date(buy.date + 'T00:00:00Z');
        if (buyDt > cutoff) return; // need 180 days of forward data

        let gotAny = false;
        TARGETS.forEach(days => {
          const targetDt = new Date(buyDt.getTime() + days * 86400000);
          let best = null, bestDiff = Infinity;
          priceSeries.forEach(obs => {
            if (obs.date <= buy.date) return;
            const diff = Math.abs(new Date(obs.date + 'T00:00:00Z') - targetDt) / 86400000;
            if (diff < bestDiff && diff <= 45) { best = obs; bestDiff = diff; }
          });
          if (best && best.price > 0) {
            retsByTarget[days].push((best.price - buy.price) / buy.price * 100);
            gotAny = true;
          }
        });
        if (gotAny) measured++;
      });

      if (measured < 2 || !retsByTarget[90].length) continue;

      const avg = {};
      TARGETS.forEach(d => {
        avg[d] = retsByTarget[d].length ? retsByTarget[d].reduce((a, b) => a + b, 0) / retsByTarget[d].length : null;
      });

      const rets90 = retsByTarget[90];
      const winRate = Math.round(rets90.filter(r => r > 0).length / rets90.length * 100);
      const wAvg = ((avg[30] || 0) * 0.25 + (avg[90] || 0) * 0.45 + (avg[180] || 0) * 0.30);

      let score = Math.max(0, Math.min(70, 35 + wAvg * 2.5));
      if (winRate >= 75) score += 15; else if (winRate >= 60) score += 8; else if (winRate < 40) score -= 8;
      const accelerates = avg[30] !== null && avg[90] !== null && avg[180] !== null && avg[90] > avg[30] && avg[180] > avg[90];
      if (accelerates) score += 10;
      else if (avg[180] !== null && avg[90] !== null && avg[180] > avg[90]) score += 5;
      if (measured >= 8) score += 5; else if (measured >= 5) score += 3; else if (measured >= 3) score += 1;
      score = Math.min(100, Math.max(0, Math.round(score)));
      if (score < 15) continue;

      const vals = buys.map(b => b.val).filter(v => v > 0).sort((a, b) => a - b);
      results.push({
        ticker, company: companyByTicker[ticker] || ticker,
        buyCount: measured,
        avg: { 1: avg[30], 5: avg[90], 20: avg[180], 60: avg[180] },
        winRate, accelerates,
        weightedAvg: +wAvg.toFixed(2), score,
        medianBuyVal: vals.length ? vals[Math.floor(vals.length / 2)] : 0,
        lastBuy: buys[buys.length - 1].date,
        d60sample: rets90.length,
      });
    }

    results.sort((a, b) => b.score - a.score);
    _driftServerCache = results.slice(0, 50);
    _driftServerCacheTime = Date.now();
    res.json(_driftServerCache);
  } catch(e) {
    slog('drift error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// EVENT PROXIMITY — computed fully server-side.
// Uses DB-predicted earnings dates (no external API needed) and recent buys.
let _proximityServerCache = null;
let _proximityServerCacheTime = 0;
const PROXIMITY_TTL = 30 * 60 * 1000;
const PROXIMITY_WINDOW = 120; // days after buy — covers full quarterly cycle

app.get('/api/proximity', async (req, res) => {
  if (_proximityServerCache && Date.now() - _proximityServerCacheTime < PROXIMITY_TTL) {
    return res.json(_proximityServerCache);
  }
  try {
    // Candidate buys: last 30 days
    const buys = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date, COALESCE(AVG(price),0) AS price, SUM(COALESCE(value,0)) AS value,
             MAX(filing_date) AS filing_date
      FROM trades
      WHERE TRIM(type) = 'P'
        AND ticker IS NOT NULL
        AND insider IS NOT NULL
        AND trade_date >= date('now', '-30 days')
      GROUP BY ticker, insider, trade_date
      ORDER BY trade_date DESC
    `).all();

    if (!buys.length) return res.json([]);

    // For each unique ticker, compute predicted next earnings dates using historical
    // trade patterns from the DB (quarterly cadence ~91 days)
    const tickers = [...new Set(buys.map(b => b.ticker))];
    const today = new Date().toISOString().slice(0, 10);

    // Get historical buy cadence per ticker to predict earnings windows
    const tickerHistory = {};
    tickers.forEach(tk => {
      const hist = db.prepare(`
        SELECT trade_date FROM trades
        WHERE ticker = ? AND TRIM(type) = 'P' AND trade_date < date('now', '-30 days')
        ORDER BY trade_date DESC LIMIT 8
      `).all(tk);
      tickerHistory[tk] = hist.map(r => r.trade_date);
    });

    const results = [];

    buys.forEach(buy => {
      const buyDate = buy.trade_date;
      if (!buyDate) return;

      // Build predicted upcoming earnings for this ticker
      const hist = tickerHistory[buy.ticker] || [];
      const predicted = [];

      // Strategy 1: extrapolate from most recent historical buy clusters (quarterly ~91 days)
      if (hist.length >= 2) {
        // Find average gap between buy clusters — proxy for earnings cycle
        const gaps = [];
        for (let i = 0; i < Math.min(hist.length - 1, 4); i++) {
          const d1 = new Date(hist[i] + 'T00:00:00Z');
          const d2 = new Date(hist[i+1] + 'T00:00:00Z');
          const gap = Math.abs((d1 - d2) / 86400000);
          if (gap >= 30 && gap <= 150) gaps.push(gap);
        }
        const avgGap = gaps.length ? gaps.reduce((a, b) => a + b, 0) / gaps.length : 91;
        const mostRecent = new Date(hist[0] + 'T00:00:00Z');
        // Project next 4 quarters forward from most recent
        for (let q = 1; q <= 4; q++) {
          const nextDt = new Date(mostRecent);
          nextDt.setUTCDate(nextDt.getUTCDate() + Math.round(avgGap * q));
          const nextStr = nextDt.toISOString().slice(0, 10);
          if (nextStr > buyDate && nextStr <= new Date(Date.now() + 180*86400000).toISOString().slice(0,10)) {
            predicted.push({ date: nextStr, type: 'EARNINGS', label: 'Predicted Earnings (est.)', source: 'DB_PREDICT' });
          }
        }
      } else {
        // Fallback: project quarterly from the buy date itself
        for (let q = 1; q <= 4; q++) {
          const nextDt = new Date(buyDate + 'T00:00:00Z');
          nextDt.setUTCDate(nextDt.getUTCDate() + q * 91);
          predicted.push({ date: nextDt.toISOString().slice(0, 10), type: 'EARNINGS', label: 'Predicted Earnings (est.)', source: 'DB_PREDICT' });
        }
      }

      // Also add events from warm EVENT_CACHE if available
      const cachedEvts = EVENT_CACHE.get(buy.ticker);
      if (cachedEvts) {
        const future = cachedEvts.data.filter(e => e.date > buyDate);
        predicted.push(...future);
      }

      // Deduplicate and sort
      const seen = new Set();
      const upcoming = predicted.filter(e => {
        if (seen.has(e.date)) return false;
        seen.add(e.date); return true;
      }).sort((a, b) => a.date.localeCompare(b.date));

      if (!upcoming.length) return;

      // Window: look up to 120 days out for events (full quarter + buffer)
      const windowEnd = new Date(buyDate + 'T00:00:00Z');
      windowEnd.setUTCDate(windowEnd.getUTCDate() + 120);
      const windowEndStr = windowEnd.toISOString().slice(0, 10);
      const inWindow = upcoming.filter(e => e.date > buyDate && e.date <= windowEndStr);
      if (!inWindow.length) return;

      const nextEvent = inWindow[0];
      const buyDt = new Date(buyDate + 'T00:00:00Z');
      const evtDt = new Date(nextEvent.date + 'T00:00:00Z');
      const daysTo = Math.max(0, Math.round((evtDt - buyDt) / 86400000));

      // Score: closer = higher urgency (scale over 120-day window)
      let score = Math.round(Math.max(0, (1 - daysTo / 120)) * 60);
      const buyValue = buy.value || 0;
      if (buyValue > 1000000) score += 20;
      else if (buyValue > 500000) score += 12;
      else if (buyValue > 100000) score += 6;
      if (hist.length >= 4) score += 10; // active insider
      score = Math.min(100, score);

      const proximityColor = daysTo <= 14 ? 'var(--sell)' : daysTo <= 30 ? 'var(--option)' : daysTo <= 60 ? 'var(--accent)' : 'var(--muted)';
      const isAbnormal = score >= 55 || daysTo <= 21;

      results.push({
        ticker: buy.ticker, company: buy.company || buy.ticker,
        insider: buy.insider, title: buy.title || '',
        buyDate, buyVal: buyValue, buyValue, daysTo, score, isAbnormal, proximityColor,
        repeatPattern: hist.length >= 3, // insider has repeated buying pattern
        nextEvent, allUpcoming: inWindow.slice(0, 4),
      });
    });

    // Deduplicate by ticker+insider, keep highest score
    const deduped = Object.values(
      results.reduce((acc, r) => {
        const k = `${r.ticker}::${r.insider}`;
        if (!acc[k] || r.score > acc[k].score) acc[k] = r;
        return acc;
      }, {})
    ).sort((a, b) => b.score - a.score);

    _proximityServerCache = deduped;
    _proximityServerCacheTime = Date.now();
    res.json(deduped);
  } catch(e) {
    slog('proximity error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
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
  const today  = new Date().toISOString().slice(0, 10);

  // ── Source 1: FMP historical + upcoming earnings ──────────────
  let fmpGotData = false;
  for (const url of [
    `https://financialmodelingprep.com/api/v3/historical/earning_calendar/${sym}?apikey=${FMP}`,
    `https://financialmodelingprep.com/stable/historical-earning-calendar?symbol=${sym}&apikey=${FMP}`,
    `https://financialmodelingprep.com/api/v3/earning_calendar?symbol=${sym}&apikey=${FMP}`,
  ]) {
    if (fmpGotData) break;
    try {
      const { status, body } = await get(url, 6000);
      if (status === 200) {
        const data = JSON.parse(body.toString());
        const arr  = Array.isArray(data) ? data : (data?.historical || data?.earningCalendar || []);
        if (arr.length > 0) {
          fmpGotData = true;
          arr.forEach(e => {
            const d = (e.date || e.reportDate || '').slice(0, 10);
            if (!d) return;
            events.push({ date: d, type: 'EARNINGS', label: `Earnings${e.eps !== undefined ? ` (EPS: ${e.eps ?? 'est'})` : ''}`, source: 'FMP' });
          });
        }
      }
    } catch(e) { /* try next */ }
  }

  // ── Source 2: FMP upcoming date-range calendar ────────────────
  if (!fmpGotData) {
    try {
      const future = new Date(Date.now() + 90*86400000).toISOString().slice(0,10);
      const url = `https://financialmodelingprep.com/api/v3/earning_calendar?symbol=${sym}&from=${today}&to=${future}&apikey=${FMP}`;
      const { status, body } = await get(url, 6000);
      if (status === 200) {
        const arr = JSON.parse(body.toString());
        if (Array.isArray(arr)) {
          arr.forEach(e => {
            const d = (e.date || '').slice(0, 10);
            if (d) events.push({ date: d, type: 'EARNINGS', label: 'Upcoming Earnings', source: 'FMP' });
          });
          if (arr.length) fmpGotData = true;
        }
      }
    } catch(e) { /* silent */ }
  }

  // ── Source 3: Nasdaq free earnings calendar (no key required) ──
  if (!fmpGotData) {
    try {
      const url = `https://api.nasdaq.com/api/calendar/earnings?date=${today}&symbol=${sym}`;
      const { status, body } = await get(url, 6000);
      if (status === 200) {
        const json = JSON.parse(body.toString());
        const rows = json?.data?.rows || [];
        rows.forEach(r => {
          const d = (r.priceEarningsDate || r.reportDate || r.date || '').slice(0, 10);
          if (d) events.push({ date: d, type: 'EARNINGS', label: 'Upcoming Earnings', source: 'NASDAQ' });
        });
        if (rows.length) fmpGotData = true;
      }
    } catch(e) { /* silent */ }
  }

  // ── Source 4: DB-based earnings prediction from historical filing patterns ──
  // If no external earnings data found, predict next earnings from insider filing history.
  // Public companies typically report earnings quarterly every ~91 days.
  if (!fmpGotData) {
    try {
      // Find the most recent earnings-proximate filing dates from 8-K filings in the DB
      // Use the last known trade dates for this ticker to infer the earnings cycle
      const rows = db.prepare(`
        SELECT MAX(filing_date) AS last_filing, MAX(trade_date) AS last_trade
        FROM trades WHERE ticker = ? AND insider IS NOT NULL
      `).get(sym);

      if (rows && (rows.last_filing || rows.last_trade)) {
        // Predict quarterly earnings: ~91 days after last known activity cycle
        const base = rows.last_filing || rows.last_trade;
        const baseDt = new Date(base + 'T12:00:00Z');
        for (let q = 1; q <= 4; q++) {
          const predDt = new Date(baseDt);
          predDt.setUTCDate(predDt.getUTCDate() + q * 91);
          const predStr = predDt.toISOString().slice(0, 10);
          if (predStr > today) {
            events.push({ date: predStr, type: 'EARNINGS', label: 'Predicted Earnings (est.)', source: 'DB_PREDICT' });
          }
        }
      }
    } catch(e) { /* silent */ }
  }

  // ── Source 5: SEC EFTS — recent 8-K filings as material events ─
  try {
    const startDate = new Date(Date.now() - 730*86400000).toISOString().slice(0,10);
    const url = `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(sym)}%22&forms=8-K&dateRange=custom&startDate=${startDate}&endDate=${today}&hits.hits.total.value=1&hits.hits._source.period_of_report=1`;
    const { status, body } = await get(url, 5000);
    if (status === 200) {
      const json = JSON.parse(body.toString());
      const hits = json?.hits?.hits || [];
      hits.forEach(h => {
        const d = (h._source?.period_of_report || h._source?.file_date || '').slice(0, 10);
        const items = (h._source?.items || '').toString();
        if (!d) return;
        let type = 'MATERIAL_EVENT', label = '8-K Filing';
        if (items.includes('2.02') || items.includes('Results')) { type = 'EARNINGS'; label = '8-K: Earnings Results'; }
        else if (items.includes('1.01')) { label = '8-K: Material Agreement'; }
        else if (items.includes('8.01')) { label = '8-K: Other Material Event'; }
        else if (items.includes('1.05') || items.includes('FDA') || items.includes('regulatory')) { type = 'REGULATORY'; label = '8-K: Regulatory Update'; }
        events.push({ date: d, type, label, source: 'SEC_8K' });
      });
    }
  } catch(e) { /* silent */ }

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
    const DEDUP = (filter, days, dateField='trade_date') => `
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date, type, MAX(COALESCE(value,0)) AS value, MAX(COALESCE(qty,0)) AS qty
      FROM trades
      WHERE ${filter} AND ${dateField} >= date('now','-${days} days')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*'
        AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
      GROUP BY ticker, insider, trade_date, type
    `;

    const hotBuys = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS buyers,
        SUM(CASE WHEN TRIM(type)='P' THEN value ELSE 0 END) AS buy_val,
        MAX(CASE WHEN TRIM(type)='P' THEN trade_date END) AS latest,
        MAX(CASE WHEN TRIM(type)='P' AND (UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%'
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
        SUM(CASE WHEN TRIM(type)='P' THEN 1 ELSE 0 END) AS buys,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 ELSE 0 END) AS sells,
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
      WHERE TRIM(type)='P' AND price>0 AND ticker IS NOT NULL
        AND trade_date >= date('now','-3 years')
      ORDER BY trade_date DESC
    `).all().map(r => r.ticker).slice(0, 180); // top 180 most recently active (covers drift tickers)

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

// Pre-compute and cache the scoreboard after price cache is warm.
// Runs the scoring logic directly — no localhost HTTP round-trip.
async function preComputeScoreboard() {
  if (_scoreboardCache) return;
  try {
    slog('Pre-computing scoreboard...');
    const minBuys = 4, limit = 30;
    const rows = db.prepare(`
      SELECT insider, MAX(title) AS title, COUNT(*) AS buy_count,
        GROUP_CONCAT(ticker || '|' || trade_date || '|' || COALESCE(price,0) || '|' || COALESCE(value,0), ';;') AS trade_data,
        GROUP_CONCAT(DISTINCT ticker) AS tickers_csv,
        CAST(julianday(MAX(trade_date)) - julianday(MIN(trade_date)) AS INTEGER) AS span_days
      FROM trades
      WHERE TRIM(type) = 'P' AND insider IS NOT NULL AND ticker IS NOT NULL
        AND trade_date <= date('now', '-95 days')
      GROUP BY insider HAVING buy_count >= ? AND span_days >= 90
      ORDER BY buy_count DESC, span_days DESC LIMIT ?
    `).all(minBuys, limit);
    if (!rows.length) { slog('Scoreboard pre-compute: no qualifying rows'); return; }

    const allTickers = [...new Set(rows.flatMap(r => (r.tickers_csv||'').split(',').filter(Boolean)))].slice(0,40);
    const priceEntries = await Promise.allSettled(allTickers.map(async sym => [sym, await fetchPriceBars(sym)]));
    const priceCache = Object.fromEntries(priceEntries.filter(r=>r.status==='fulfilled'&&r.value[1]).map(r=>r.value));

    const accuracyResults = [], timingResults = [];
    rows.forEach(leader => {
      try {
        const rawTrades = (leader.trade_data||'').split(';;').map(s => {
          const [ticker,trade_date,priceStr,valueStr] = s.split('|');
          return { ticker, trade: trade_date, price: parseFloat(priceStr)||0, value: parseFloat(valueStr)||0 };
        }).filter(t => t.ticker && t.trade);
        if (rawTrades.length < 4) return;
        const scored = rawTrades.map(t => {
          const bars = priceCache[t.ticker]||[]; if (!bars.length) return null;
          const buyDate = t.trade.slice(0,10);
          const entryBar = bars.find(b=>b.time>=buyDate);
          const buyPrice = t.price>0 ? t.price : (entryBar?.close||0);
          if (!buyPrice||buyPrice<=0) return null;
          const fwd = d => { const fd=new Date(buyDate+'T12:00:00Z'); fd.setUTCDate(fd.getUTCDate()+d); const b=bars.find(x=>x.time>=fd.toISOString().slice(0,10)); return b?+((b.close-buyPrice)/buyPrice*100).toFixed(2):null; };
          return { ticker:t.ticker, tradeDate:buyDate, buyPrice, ret30:fwd(30), ret90:fwd(90), ret180:fwd(180) };
        }).filter(Boolean);
        const completed = scored.filter(s=>s.ret90!==null); if (completed.length<3) return;
        const rets90=completed.map(s=>s.ret90), avgRet90=+(rets90.reduce((a,b)=>a+b,0)/rets90.length).toFixed(1);
        const rets30=completed.filter(s=>s.ret30!==null).map(s=>s.ret30);
        const avgRet30=rets30.length?+(rets30.reduce((a,b)=>a+b,0)/rets30.length).toFixed(1):null;
        const winRate=Math.round(rets90.filter(r=>r>0).length/rets90.length*100);
        const avgMag=+(rets90.map(Math.abs).reduce((a,b)=>a+b,0)/rets90.length).toFixed(1);
        const sorted=[...rets90].sort((a,b)=>a-b), median=sorted[Math.floor(sorted.length/2)];
        const consist=Math.round(Math.min(100,Math.max(0,(median/Math.max(avgMag,1)+1)*50)));
        const accScore=Math.round(Math.min(100,Math.max(0,winRate*0.40+Math.min(35,Math.max(0,avgRet90/20*35))+consist*0.15+Math.min(10,completed.length*1.2))));
        const tier=accScore>=75?'ELITE':accScore>=55?'STRONG':accScore>=35?'AVERAGE':'WEAK';
        const tickers3=[...new Set(rawTrades.map(t=>t.ticker))].slice(0,3).join(', ');
        accuracyResults.push({name:leader.insider,title:leader.title||'',accuracyScore:accScore,tier,winRate,avgRet90,avgRet30,tradeCount:completed.length,tickers:tickers3});

        let nearLowCount=0,ret90sum=0,ret180sum=0,retN=0;
        scored.forEach(s => {
          const bars=priceCache[s.ticker]||[]; if(!bars.length||!s.buyPrice) return;
          const yr1Start=new Date(s.tradeDate+'T12:00:00Z'); yr1Start.setUTCFullYear(yr1Start.getUTCFullYear()-1);
          const yr1Bars=bars.filter(b=>b.time>=yr1Start.toISOString().slice(0,10)&&b.time<=s.tradeDate);
          if(yr1Bars.length<20) return;
          const yr1Lo=Math.min(...yr1Bars.map(b=>b.low||b.close));
          if((s.buyPrice-yr1Lo)/yr1Lo*100<=20) nearLowCount++;
          if(s.ret90!==null){ret90sum+=s.ret90;retN++;} if(s.ret180!==null) ret180sum+=s.ret180;
        });
        const n=scored.length, nearLowPct=n>0?Math.round(nearLowCount/n*100):0;
        const avgFwd90=retN>0?+(ret90sum/retN).toFixed(1):null, avgFwd180=retN>0?+(ret180sum/retN).toFixed(1):null;
        const avgPos=scored.reduce((acc,s)=>{
          const bars=priceCache[s.ticker]||[]; if(!bars.length||!s.buyPrice) return acc;
          const yr1Start=new Date(s.tradeDate+'T12:00:00Z'); yr1Start.setUTCFullYear(yr1Start.getUTCFullYear()-1);
          const yr1Bars=bars.filter(b=>b.time>=yr1Start.toISOString().slice(0,10)&&b.time<=s.tradeDate);
          if(yr1Bars.length<20) return acc;
          const yr1Lo=Math.min(...yr1Bars.map(b=>b.low||b.close)),yr1Hi=Math.max(...yr1Bars.map(b=>b.high||b.close)),rng=yr1Hi-yr1Lo;
          return rng>0.01?{sum:acc.sum+(s.buyPrice-yr1Lo)/rng,n:acc.n+1}:acc;
        },{sum:0,n:0});
        const avgPosVal=avgPos.n>0?avgPos.sum/avgPos.n:0.5;
        const timingAlpha=Math.min(100,Math.max(0,Math.round(Math.min(25,(nearLowPct/100)*50)+Math.min(25,Math.max(0,(0.7-avgPosVal)/0.4*25))+(avgFwd90!==null?Math.min(25,Math.max(0,avgFwd90/10*25)):12)+12)));
        let verdict,verdictColor;
        if(timingAlpha>=80&&nearLowPct>=50){verdict='Buys near bottoms';verdictColor='buy';}
        else if(timingAlpha>=80){verdict='Elite forward returns';verdictColor='buy';}
        else if(timingAlpha>=60){verdict='Above-average timing';verdictColor='accent';}
        else if(timingAlpha>=40){verdict='Mixed timing signals';verdictColor='option';}
        else{verdict='Tends to buy high';verdictColor='sell';}
        if(timingAlpha>=35) timingResults.push({name:leader.insider,title:leader.title||'',timingAlpha,nearLowPct,nearHighSellPct:null,avgRet90:avgFwd90,avgRet180:avgFwd180,verdict,verdictColor,tradeCount:completed.length,tickers:tickers3});
      } catch(e) { /* skip */ }
    });
    accuracyResults.sort((a,b)=>b.accuracyScore-a.accuracyScore);
    timingResults.sort((a,b)=>b.timingAlpha-a.timingAlpha);
    _scoreboardCache = { accuracy:accuracyResults, timing:timingResults };
    _scoreboardCacheTime = Date.now();
    slog(`Scoreboard pre-computed: ${accuracyResults.length} accuracy, ${timingResults.length} timing`);
  } catch(e) { slog('preComputeScoreboard error: ' + e.message); }
}

// Warm price cache 60s after boot so first requests are fast
setTimeout(async () => {
  await warmPriceCache();
  // Pre-compute scoreboard immediately after price cache is warm
  await preComputeScoreboard();
}, 60000);
// Re-warm every 2 hours to keep cache fresh
setInterval(() => warmPriceCache(), 2 * 60 * 60 * 1000);

// Pre-compute drift at startup (pure DB query, no external calls needed)
// Run after 10s to let the DB settle after initial sync
setTimeout(() => {
  try {
    slog('Pre-computing drift...');
    // Trigger the drift endpoint logic inline
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        trade_date, TRIM(type) AS type,
        AVG(price) AS price, SUM(COALESCE(value,0)) AS val
      FROM trades
      WHERE price > 0 AND ticker IS NOT NULL AND trade_date >= date('now','-4 years')
      GROUP BY ticker, trade_date, TRIM(type)
      ORDER BY ticker, trade_date
    `).all();
    const seriesByTicker = {}, companyByTicker = {};
    rows.forEach(r => {
      if (!seriesByTicker[r.ticker]) { seriesByTicker[r.ticker] = []; companyByTicker[r.ticker] = r.company; }
      seriesByTicker[r.ticker].push({ date: r.trade_date, price: r.price, type: r.type, val: r.val });
    });
    const results = [];
    for (const [ticker, series] of Object.entries(seriesByTicker)) {
      const buys = series.filter(s => s.type === 'P');
      if (buys.length < 3) continue;
      const retsByTarget = { 30: [], 90: [], 180: [] };
      let measured = 0;
      buys.forEach(buy => {
        const buyDt = new Date(buy.date + 'T00:00:00Z');
        if (buyDt > new Date(Date.now() - 180 * 86400000)) return;
        let gotAny = false;
        [30, 90, 180].forEach(days => {
          const targetDt = new Date(buyDt.getTime() + days * 86400000);
          let best = null, bestDiff = Infinity;
          series.forEach(obs => {
            if (obs.date <= buy.date) return;
            const diff = Math.abs(new Date(obs.date + 'T00:00:00Z') - targetDt) / 86400000;
            if (diff < bestDiff && diff <= 45) { best = obs; bestDiff = diff; }
          });
          if (best && best.price > 0) { retsByTarget[days].push((best.price - buy.price) / buy.price * 100); gotAny = true; }
        });
        if (gotAny) measured++;
      });
      if (measured < 2 || !retsByTarget[90].length) continue;
      const avg = {};
      [30, 90, 180].forEach(d => { avg[d] = retsByTarget[d].length ? retsByTarget[d].reduce((a,b)=>a+b,0)/retsByTarget[d].length : null; });
      const rets90 = retsByTarget[90];
      const winRate = Math.round(rets90.filter(r=>r>0).length/rets90.length*100);
      const wAvg = ((avg[30]||0)*0.25 + (avg[90]||0)*0.45 + (avg[180]||0)*0.30);
      let score = Math.max(0, Math.min(70, 35 + wAvg*2.5));
      if (winRate>=75) score+=15; else if (winRate>=60) score+=8; else if (winRate<40) score-=8;
      const accelerates = avg[30]!==null&&avg[90]!==null&&avg[180]!==null&&avg[90]>avg[30]&&avg[180]>avg[90];
      if (accelerates) score+=10; else if (avg[180]!==null&&avg[90]!==null&&avg[180]>avg[90]) score+=5;
      if (measured>=8) score+=5; else if (measured>=5) score+=3; else if (measured>=3) score+=1;
      score = Math.min(100, Math.max(0, Math.round(score)));
      if (score < 15) continue;
      const vals = buys.map(b=>b.val).filter(v=>v>0).sort((a,b)=>a-b);
      results.push({ ticker, company: companyByTicker[ticker]||ticker, buyCount: measured,
        avg: { 1: avg[30], 5: avg[90], 20: avg[180], 60: avg[180] },
        winRate, accelerates, weightedAvg: +wAvg.toFixed(2), score,
        medianBuyVal: vals.length ? vals[Math.floor(vals.length/2)] : 0,
        lastBuy: buys[buys.length-1].date, d60sample: rets90.length });
    }
    results.sort((a,b)=>b.score-a.score);
    _driftServerCache = results.slice(0, 50);
    _driftServerCacheTime = Date.now();
    slog(`Drift pre-computed: ${_driftServerCache.length} tickers`);
  } catch(e) { slog('Drift pre-compute error: ' + e.message); }
}, 10000);

// Pre-compute proximity at startup (pure DB query)
setTimeout(() => {
  try {
    slog('Pre-computing proximity...');
    const buys = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date, COALESCE(AVG(price),0) AS price, SUM(COALESCE(value,0)) AS value,
             MAX(filing_date) AS filing_date
      FROM trades
      WHERE TRIM(type) = 'P' AND ticker IS NOT NULL AND insider IS NOT NULL
        AND trade_date >= date('now', '-30 days')
      GROUP BY ticker, insider, trade_date
      ORDER BY trade_date DESC
    `).all();
    if (!buys.length) { slog('Proximity: no recent buys'); return; }

    const tickers = [...new Set(buys.map(b => b.ticker))];
    const tickerHistory = {};
    tickers.forEach(tk => {
      const hist = db.prepare(`
        SELECT trade_date FROM trades
        WHERE ticker = ? AND TRIM(type) = 'P' AND trade_date < date('now', '-30 days')
        ORDER BY trade_date DESC LIMIT 8
      `).all(tk);
      tickerHistory[tk] = hist.map(r => r.trade_date);
    });

    const results = [];
    buys.forEach(buy => {
      const buyDate = buy.trade_date; if (!buyDate) return;
      const hist = tickerHistory[buy.ticker] || [];
      const predicted = [];
      if (hist.length >= 2) {
        const gaps = [];
        for (let i = 0; i < Math.min(hist.length-1,4); i++) {
          const gap = Math.abs((new Date(hist[i]+'T00:00:00Z') - new Date(hist[i+1]+'T00:00:00Z')) / 86400000);
          if (gap >= 30 && gap <= 150) gaps.push(gap);
        }
        const avgGap = gaps.length ? gaps.reduce((a,b)=>a+b,0)/gaps.length : 91;
        const mostRecent = new Date(hist[0]+'T00:00:00Z');
        for (let q = 1; q <= 6; q++) {
          const nd = new Date(mostRecent); nd.setUTCDate(nd.getUTCDate() + Math.round(avgGap*q));
          const ns = nd.toISOString().slice(0,10);
          if (ns > buyDate) predicted.push({ date: ns, type: 'EARNINGS', label: 'Predicted Earnings (est.)', source: 'DB_PREDICT' });
        }
      } else {
        for (let q = 1; q <= 4; q++) {
          const nd = new Date(buyDate+'T00:00:00Z'); nd.setUTCDate(nd.getUTCDate() + q*91);
          predicted.push({ date: nd.toISOString().slice(0,10), type: 'EARNINGS', label: 'Predicted Earnings (est.)', source: 'DB_PREDICT' });
        }
      }
      const cachedEvts = EVENT_CACHE.get(buy.ticker);
      if (cachedEvts) predicted.push(...cachedEvts.data.filter(e => e.date > buyDate));
      const seen = new Set();
      const upcoming = predicted.filter(e => { if (seen.has(e.date)) return false; seen.add(e.date); return true; }).sort((a,b) => a.date.localeCompare(b.date));
      if (!upcoming.length) return;
      const windowEnd = new Date(buyDate+'T00:00:00Z'); windowEnd.setUTCDate(windowEnd.getUTCDate()+120);
      const inWindow = upcoming.filter(e => e.date > buyDate && e.date <= windowEnd.toISOString().slice(0,10));
      if (!inWindow.length) return;
      const nextEvent = inWindow[0];
      const daysTo = Math.max(0, Math.round((new Date(nextEvent.date+'T00:00:00Z') - new Date(buyDate+'T00:00:00Z')) / 86400000));
      let score = Math.round(Math.max(0,(1-daysTo/120))*60);
      const bv = buy.value||0;
      if (bv>1000000) score+=20; else if (bv>500000) score+=12; else if (bv>100000) score+=6;
      if (hist.length>=4) score+=10;
      score = Math.min(100,score);
      const proximityColor = daysTo<=14?'var(--sell)':daysTo<=30?'var(--option)':daysTo<=60?'var(--accent)':'var(--muted)';
      results.push({ ticker:buy.ticker, company:buy.company||buy.ticker, insider:buy.insider, title:buy.title||'',
        buyDate, buyVal:bv, buyValue:bv, daysTo, score, isAbnormal:score>=55||daysTo<=21, proximityColor,
        repeatPattern:hist.length>=3, nextEvent, allUpcoming:inWindow.slice(0,4) });
    });
    const deduped = Object.values(results.reduce((acc,r)=>{
      const k=`${r.ticker}::${r.insider}`; if (!acc[k]||r.score>acc[k].score) acc[k]=r; return acc;
    },{})).sort((a,b)=>b.score-a.score);
    _proximityServerCache = deduped;
    _proximityServerCacheTime = Date.now();
    slog(`Proximity pre-computed: ${deduped.length} results`);
  } catch(e) { slog('Proximity pre-compute error: ' + e.message); }
}, 12000);



// ─── DEBUG ENDPOINT — shows DB stats for diagnosing data issues ───────────
app.get('/api/debug', (req, res) => {
  try {
    const total = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    const byType = db.prepare("SELECT COALESCE(TRIM(type),'NULL') AS type, COUNT(*) AS n FROM trades GROUP BY TRIM(type) ORDER BY n DESC LIMIT 20").all();
    const dates = db.prepare("SELECT MAX(trade_date) AS td_latest, MIN(trade_date) AS td_earliest, MAX(filing_date) AS fd_latest, MIN(filing_date) AS fd_earliest FROM trades WHERE trade_date IS NOT NULL").get();
    const p30_trade  = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-30 days')").get().n;
    const p30_filing = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND filing_date >= date('now','-30 days')").get().n;
    const p3yr_trade = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-1095 days')").get().n;
    const ranker30   = db.prepare("SELECT COUNT(DISTINCT ticker) AS n FROM trades WHERE filing_date >= date('now','-30 days')").get().n;
    const history1095 = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date >= date('now','-1095 days')").get().n;
    res.json({ total, byType, dates, p30_trade, p30_filing, p3yr_trade, ranker30_tickers: ranker30, history1095_buys: history1095 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Sources: Capitol Trades public API + House/Senate STOCK Act disclosures via Quiver
app.listen(PORT, () => console.log(`Server on port ${PORT}`));
