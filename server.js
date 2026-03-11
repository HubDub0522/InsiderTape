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
let db;
try {
  db = new Database(DB_PATH);
} catch (e) {
  console.error('FATAL: cannot open database at', DB_PATH, '-', e.message);
  process.exit(1);
}

try { db.pragma('journal_mode = WAL'); } catch(e) { console.warn('WAL pragma failed (read-only disk?):', e.message); }

// Each table/index created separately so existing DBs get new tables too
// All wrapped in try/catch — on Render the persistent disk can be briefly
// unavailable during a new deploy; a write failure here must NOT crash the server.
try {
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
} catch(e) { console.warn('Schema init warning:', e.message); }

// ─── Clean up bad/invalid trade records ──────────────────────
// These are idempotent maintenance tasks — safe to skip if the disk is busy.
try {
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
} catch(e) { console.warn('Startup cleanup skipped (disk busy?):', e.message); }

try {
  db.exec(`CREATE TABLE IF NOT EXISTS sync_log (
    quarter TEXT PRIMARY KEY, synced_at TEXT DEFAULT (datetime('now')), rows INTEGER
  )`);
} catch(e) { console.warn('sync_log table init warning:', e.message); }

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
    // Scale limit by window so every range returns a meaningfully different dataset.
    // 7d → 1000, 30d → 2000, 90d → 5000, 365d → 15000
    const defaultLimit = days <= 7 ? 1000 : days <= 30 ? 2000 : days <= 90 ? 5000 : 15000;
    const limit = Math.min(parseInt(req.query.limit || String(defaultLimit)), 20000);

        let rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned,
             MAX(accession) AS accession
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
                 MAX(accession) AS accession
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
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
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
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
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
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
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
// SEARCH AUTOCOMPLETE — ticker prefix + company name + insider name
app.get('/api/search', (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q || q.length < 1) return res.json({ tickers: [], insiders: [] });
  try {
    const upper = q.toUpperCase();
    // Ticker prefix match
    const tickerRows = db.prepare(`
      SELECT ticker, MAX(company) AS company, COUNT(*) AS n
      FROM trades
      WHERE ticker GLOB ? AND LENGTH(ticker) BETWEEN 1 AND 6
      GROUP BY ticker ORDER BY n DESC LIMIT 8
    `).all(upper + '*');
    // Company name match (case-insensitive contains)
    const companyRows = db.prepare(`
      SELECT ticker, MAX(company) AS company, COUNT(*) AS n
      FROM trades
      WHERE UPPER(company) LIKE ? AND LENGTH(ticker) BETWEEN 1 AND 6
        AND ticker GLOB '[A-Z]*'
      GROUP BY ticker ORDER BY n DESC LIMIT 4
    `).all('%' + upper + '%');
    // Insider name match
    const insiderRows = db.prepare(`
      SELECT insider, MAX(title) AS title, COUNT(*) AS n
      FROM trades
      WHERE UPPER(insider) LIKE ? AND insider IS NOT NULL
      GROUP BY insider ORDER BY n DESC LIMIT 6
    `).all('%' + q.toUpperCase() + '%');

    // Merge ticker results (prefix first, then company matches not already in prefix)
    const seenTickers = new Set(tickerRows.map(r => r.ticker));
    const allTickers = [...tickerRows];
    companyRows.forEach(r => { if (!seenTickers.has(r.ticker)) allTickers.push(r); });

    res.json({
      tickers: allTickers.slice(0, 8).map(r => ({ ticker: r.ticker, company: r.company || r.ticker })),
      insiders: insiderRows.slice(0, 6).map(r => ({ name: r.insider, title: r.title || '' })),
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

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
               MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
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

// ── SECTORS ─────────────────────────────────────────────────────────────────
// Static sector/industry map. Zero external calls — instant lookups.
// Covers S&P 500 + Russell 1000 + high-insider-activity names (~750 tickers).
const TICKER_SECTOR_MAP = {
  'AAPL':['Technology','Consumer Electronics'],'MSFT':['Technology','Software—Infrastructure'],
  'NVDA':['Technology','Semiconductors'],'AVGO':['Technology','Semiconductors'],
  'ORCL':['Technology','Software—Infrastructure'],'CSCO':['Technology','Communication Equipment'],
  'ADBE':['Technology','Software—Application'],'CRM':['Technology','Software—Application'],
  'AMD':['Technology','Semiconductors'],'QCOM':['Technology','Semiconductors'],
  'TXN':['Technology','Semiconductors'],'INTC':['Technology','Semiconductors'],
  'IBM':['Technology','Information Technology Services'],'NOW':['Technology','Software—Application'],
  'INTU':['Technology','Software—Application'],'AMAT':['Technology','Semiconductor Equipment & Materials'],
  'MU':['Technology','Semiconductors'],'LRCX':['Technology','Semiconductor Equipment & Materials'],
  'KLAC':['Technology','Semiconductor Equipment & Materials'],'ADI':['Technology','Semiconductors'],
  'MRVL':['Technology','Semiconductors'],'PANW':['Technology','Software—Infrastructure'],
  'SNPS':['Technology','Software—Application'],'CDNS':['Technology','Software—Application'],
  'FTNT':['Technology','Software—Infrastructure'],'HPQ':['Technology','Computer Hardware'],
  'HPE':['Technology','Computer Hardware'],'DELL':['Technology','Computer Hardware'],
  'STX':['Technology','Computer Hardware'],'WDC':['Technology','Computer Hardware'],
  'NTAP':['Technology','Computer Hardware'],'GLW':['Technology','Electronic Components'],
  'TEL':['Technology','Electronic Components'],'KEYS':['Technology','Scientific & Technical Instruments'],
  'ANSS':['Technology','Software—Application'],'PTC':['Technology','Software—Application'],
  'CTSH':['Technology','Information Technology Services'],'ACN':['Technology','Information Technology Services'],
  'IT':['Technology','Information Technology Services'],'GPN':['Technology','Software—Infrastructure'],
  'FISV':['Technology','Software—Infrastructure'],'FIS':['Technology','Software—Infrastructure'],
  'FI':['Technology','Software—Infrastructure'],'PYPL':['Technology','Software—Infrastructure'],
  'ADSK':['Technology','Software—Application'],'WDAY':['Technology','Software—Application'],
  'ZS':['Technology','Software—Infrastructure'],'CRWD':['Technology','Software—Infrastructure'],
  'OKTA':['Technology','Software—Infrastructure'],'DDOG':['Technology','Software—Application'],
  'NET':['Technology','Software—Infrastructure'],'MDB':['Technology','Software—Application'],
  'SNOW':['Technology','Software—Application'],'HUBS':['Technology','Software—Application'],
  'GTLB':['Technology','Software—Application'],'APP':['Technology','Software—Application'],
  'RBLX':['Technology','Software—Application'],'FFIV':['Technology','Software—Infrastructure'],
  'JNPR':['Technology','Communication Equipment'],'MSCI':['Technology','Information Technology Services'],
  'BR':['Technology','Information Technology Services'],'NXPI':['Technology','Semiconductors'],
  'ON':['Technology','Semiconductors'],'MPWR':['Technology','Semiconductors'],
  'SWKS':['Technology','Semiconductors'],'MCHP':['Technology','Semiconductors'],
  'SMCI':['Technology','Computer Hardware'],'PSTG':['Technology','Computer Hardware'],
  'TWLO':['Technology','Software—Application'],'ZM':['Technology','Software—Application'],
  'DOCU':['Technology','Software—Application'],'BOX':['Technology','Software—Infrastructure'],
  'DBX':['Technology','Software—Infrastructure'],'PCOR':['Technology','Software—Application'],
  'SPSC':['Technology','Software—Application'],'BRZE':['Technology','Software—Application'],
  'BILL':['Technology','Software—Application'],'DT':['Technology','Software—Application'],
  'AI':['Technology','Software—Application'],'NCNO':['Technology','Software—Application'],
  'TOST':['Technology','Software—Application'],'AMBA':['Technology','Semiconductors'],
  'SLAB':['Technology','Semiconductors'],'ONTO':['Technology','Semiconductor Equipment & Materials'],
  'IPGP':['Technology','Electronic Components'],'VIAV':['Technology','Communication Equipment'],
  'CIEN':['Technology','Communication Equipment'],'CALX':['Technology','Communication Equipment'],
  'EPAM':['Technology','Information Technology Services'],'GDDY':['Technology','Software—Infrastructure'],
  'AKAM':['Technology','Software—Infrastructure'],'CDW':['Technology','Information Technology Services'],
  'NTDOY':['Technology','Electronic Gaming & Multimedia'],'ACLS':['Technology','Semiconductor Equipment & Materials'],

  'JPM':['Financial Services','Banks—Diversified'],'BAC':['Financial Services','Banks—Diversified'],
  'WFC':['Financial Services','Banks—Diversified'],'GS':['Financial Services','Capital Markets'],
  'MS':['Financial Services','Capital Markets'],'BLK':['Financial Services','Asset Management'],
  'C':['Financial Services','Banks—Diversified'],'AXP':['Financial Services','Credit Services'],
  'V':['Financial Services','Credit Services'],'MA':['Financial Services','Credit Services'],
  'COF':['Financial Services','Credit Services'],'DFS':['Financial Services','Credit Services'],
  'SYF':['Financial Services','Credit Services'],'USB':['Financial Services','Banks—Regional'],
  'PNC':['Financial Services','Banks—Regional'],'TFC':['Financial Services','Banks—Regional'],
  'MTB':['Financial Services','Banks—Regional'],'KEY':['Financial Services','Banks—Regional'],
  'RF':['Financial Services','Banks—Regional'],'CFG':['Financial Services','Banks—Regional'],
  'HBAN':['Financial Services','Banks—Regional'],'FITB':['Financial Services','Banks—Regional'],
  'WBS':['Financial Services','Banks—Regional'],'BOKF':['Financial Services','Banks—Regional'],
  'SNV':['Financial Services','Banks—Regional'],'FHN':['Financial Services','Banks—Regional'],
  'WAL':['Financial Services','Banks—Regional'],'COLB':['Financial Services','Banks—Regional'],
  'EWBC':['Financial Services','Banks—Regional'],'FFIN':['Financial Services','Banks—Regional'],
  'CVBF':['Financial Services','Banks—Regional'],'FULT':['Financial Services','Banks—Regional'],
  'INDB':['Financial Services','Banks—Regional'],'NTRS':['Financial Services','Asset Management'],
  'STT':['Financial Services','Asset Management'],'BK':['Financial Services','Asset Management'],
  'SCHW':['Financial Services','Capital Markets'],'RJF':['Financial Services','Capital Markets'],
  'LPLA':['Financial Services','Capital Markets'],'PFG':['Financial Services','Insurance—Diversified'],
  'LNC':['Financial Services','Insurance—Life'],'UNM':['Financial Services','Insurance—Life'],
  'GL':['Financial Services','Insurance—Life'],'MET':['Financial Services','Insurance—Life'],
  'PRU':['Financial Services','Insurance—Life'],'AIG':['Financial Services','Insurance—Diversified'],
  'CB':['Financial Services','Insurance—Property & Casualty'],'PGR':['Financial Services','Insurance—Property & Casualty'],
  'ALL':['Financial Services','Insurance—Property & Casualty'],'HIG':['Financial Services','Insurance—Property & Casualty'],
  'TRV':['Financial Services','Insurance—Property & Casualty'],'CNA':['Financial Services','Insurance—Property & Casualty'],
  'WRB':['Financial Services','Insurance—Property & Casualty'],'CINF':['Financial Services','Insurance—Property & Casualty'],
  'MKL':['Financial Services','Insurance—Property & Casualty'],'ERIE':['Financial Services','Insurance—Property & Casualty'],
  'ICE':['Financial Services','Financial Data & Stock Exchanges'],'CME':['Financial Services','Financial Data & Stock Exchanges'],
  'CBOE':['Financial Services','Financial Data & Stock Exchanges'],'MCO':['Financial Services','Financial Data & Stock Exchanges'],
  'SPGI':['Financial Services','Financial Data & Stock Exchanges'],'FDS':['Financial Services','Financial Data & Stock Exchanges'],
  'NDAQ':['Financial Services','Financial Data & Stock Exchanges'],'ALLY':['Financial Services','Credit Services'],
  'CACC':['Financial Services','Credit Services'],'OMF':['Financial Services','Credit Services'],
  'ENVA':['Financial Services','Credit Services'],'SLM':['Financial Services','Credit Services'],
  'NAVI':['Financial Services','Credit Services'],'UWMC':['Financial Services','Mortgage Finance'],
  'AMG':['Financial Services','Asset Management'],'IVZ':['Financial Services','Asset Management'],
  'AMP':['Financial Services','Asset Management'],'BEN':['Financial Services','Asset Management'],
  'TROW':['Financial Services','Asset Management'],'WTW':['Financial Services','Insurance Brokers'],
  'AON':['Financial Services','Insurance Brokers'],'MMC':['Financial Services','Insurance Brokers'],
  'AJG':['Financial Services','Insurance Brokers'],'RYAN':['Financial Services','Insurance Brokers'],
  'BRP':['Financial Services','Insurance Brokers'],'NMIH':['Financial Services','Insurance—Property & Casualty'],
  'ESNT':['Financial Services','Insurance—Property & Casualty'],'RDN':['Financial Services','Insurance—Property & Casualty'],
  'MTG':['Financial Services','Insurance—Property & Casualty'],'ORI':['Financial Services','Insurance—Diversified'],
  'HOOD':['Financial Services','Capital Markets'],'COIN':['Financial Services','Capital Markets'],
  'MSTR':['Financial Services','Capital Markets'],'WEX':['Financial Services','Credit Services'],
  'ARCC':['Financial Services','Asset Management'],'MAIN':['Financial Services','Asset Management'],
  'HTGC':['Financial Services','Asset Management'],'CSWC':['Financial Services','Asset Management'],
  'OBDC':['Financial Services','Asset Management'],'BXSL':['Financial Services','Asset Management'],
  'OPBK':['Financial Services','Banks—Regional'],'FFBC':['Financial Services','Banks—Regional'],
  'HOMB':['Financial Services','Banks—Regional'],'TOWN':['Financial Services','Banks—Regional'],
  'SMBC':['Financial Services','Banks—Regional'],'HOPE':['Financial Services','Banks—Regional'],
  'BANF':['Financial Services','Banks—Regional'],'SFNC':['Financial Services','Banks—Regional'],
  'HTLF':['Financial Services','Banks—Regional'],'WSFS':['Financial Services','Banks—Regional'],
  'HAFC':['Financial Services','Banks—Regional'],'NBTB':['Financial Services','Banks—Regional'],
  'NWBI':['Financial Services','Banks—Regional'],'KMPR':['Financial Services','Insurance—Life'],
  'FG':['Financial Services','Insurance—Life'],'SIGI':['Financial Services','Insurance—Property & Casualty'],
  'ROOT':['Financial Services','Insurance—Property & Casualty'],'PRAA':['Financial Services','Credit Services'],
  'ECPG':['Financial Services','Credit Services'],'ATLC':['Financial Services','Credit Services'],
  'PMTS':['Financial Services','Credit Services'],'PSFE':['Financial Services','Credit Services'],

  'LLY':['Healthcare','Drug Manufacturers—General'],'JNJ':['Healthcare','Drug Manufacturers—General'],
  'ABBV':['Healthcare','Drug Manufacturers—General'],'MRK':['Healthcare','Drug Manufacturers—General'],
  'PFE':['Healthcare','Drug Manufacturers—General'],'BMY':['Healthcare','Drug Manufacturers—General'],
  'AMGN':['Healthcare','Drug Manufacturers—General'],'GILD':['Healthcare','Drug Manufacturers—General'],
  'BIIB':['Healthcare','Drug Manufacturers—General'],'REGN':['Healthcare','Drug Manufacturers—General'],
  'VRTX':['Healthcare','Drug Manufacturers—General'],'MRNA':['Healthcare','Drug Manufacturers—General'],
  'BNTX':['Healthcare','Drug Manufacturers—General'],'ALNY':['Healthcare','Biotechnology'],
  'IDXX':['Healthcare','Diagnostics & Research'],'ILMN':['Healthcare','Diagnostics & Research'],
  'TMO':['Healthcare','Diagnostics & Research'],'DHR':['Healthcare','Diagnostics & Research'],
  'A':['Healthcare','Diagnostics & Research'],'BIO':['Healthcare','Diagnostics & Research'],
  'HOLX':['Healthcare','Medical Devices'],'MDT':['Healthcare','Medical Devices'],
  'ABT':['Healthcare','Medical Devices'],'SYK':['Healthcare','Medical Devices'],
  'BSX':['Healthcare','Medical Devices'],'EW':['Healthcare','Medical Devices'],
  'ZBH':['Healthcare','Medical Devices'],'ISRG':['Healthcare','Medical Devices'],
  'DXCM':['Healthcare','Medical Devices'],'PODD':['Healthcare','Medical Devices'],
  'INSP':['Healthcare','Medical Devices'],'SWAV':['Healthcare','Medical Devices'],
  'NVCR':['Healthcare','Medical Devices'],'RMD':['Healthcare','Medical Devices'],
  'ALGN':['Healthcare','Medical Devices'],'BDX':['Healthcare','Medical Devices'],
  'BAX':['Healthcare','Medical Devices'],'XRAY':['Healthcare','Medical Devices'],
  'GKOS':['Healthcare','Medical Devices'],'LMAT':['Healthcare','Medical Devices'],
  'IRTC':['Healthcare','Medical Devices'],'NARI':['Healthcare','Medical Devices'],
  'TMDX':['Healthcare','Medical Devices'],'VCEL':['Healthcare','Medical Devices'],
  'CNMD':['Healthcare','Medical Devices'],'MMSI':['Healthcare','Medical Devices'],
  'ATRC':['Healthcare','Medical Devices'],'AMED':['Healthcare','Medical Care Facilities'],
  'HCA':['Healthcare','Medical Care Facilities'],'THC':['Healthcare','Medical Care Facilities'],
  'CYH':['Healthcare','Medical Care Facilities'],'UHS':['Healthcare','Medical Care Facilities'],
  'ENSG':['Healthcare','Medical Care Facilities'],'PGNY':['Healthcare','Medical Care Facilities'],
  'ACHC':['Healthcare','Medical Care Facilities'],'CERT':['Healthcare','Medical Care Facilities'],
  'UNH':['Healthcare','Healthcare Plans'],'CVS':['Healthcare','Healthcare Plans'],
  'CI':['Healthcare','Healthcare Plans'],'HUM':['Healthcare','Healthcare Plans'],
  'MOH':['Healthcare','Healthcare Plans'],'CNC':['Healthcare','Healthcare Plans'],
  'ELV':['Healthcare','Healthcare Plans'],'MCK':['Healthcare','Medical Distribution'],
  'CAH':['Healthcare','Medical Distribution'],'ABC':['Healthcare','Medical Distribution'],
  'PDCO':['Healthcare','Medical Distribution'],'AMPH':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'PRGO':['Healthcare','Drug Manufacturers—Specialty & Generic'],'TEVA':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'VTRS':['Healthcare','Drug Manufacturers—Specialty & Generic'],'JAZZ':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'SUPN':['Healthcare','Drug Manufacturers—Specialty & Generic'],'HIMS':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'IOVA':['Healthcare','Biotechnology'],'NTLA':['Healthcare','Biotechnology'],
  'BEAM':['Healthcare','Biotechnology'],'CRSP':['Healthcare','Biotechnology'],
  'EDIT':['Healthcare','Biotechnology'],'RCKT':['Healthcare','Biotechnology'],
  'BLUE':['Healthcare','Biotechnology'],'ACAD':['Healthcare','Biotechnology'],
  'AXSM':['Healthcare','Biotechnology'],'APLS':['Healthcare','Biotechnology'],
  'IMVT':['Healthcare','Biotechnology'],'PRAX':['Healthcare','Biotechnology'],
  'KRYS':['Healthcare','Biotechnology'],'TGTX':['Healthcare','Biotechnology'],
  'RXRX':['Healthcare','Biotechnology'],'RVMD':['Healthcare','Biotechnology'],
  'IONS':['Healthcare','Biotechnology'],'SRPT':['Healthcare','Biotechnology'],
  'BMRN':['Healthcare','Biotechnology'],'PTCT':['Healthcare','Biotechnology'],
  'RARE':['Healthcare','Biotechnology'],'FOLD':['Healthcare','Biotechnology'],
  'KYMR':['Healthcare','Biotechnology'],'RCUS':['Healthcare','Biotechnology'],
  'IMCR':['Healthcare','Biotechnology'],'KRTX':['Healthcare','Biotechnology'],
  'NUVL':['Healthcare','Biotechnology'],'VRNA':['Healthcare','Biotechnology'],
  'ARCT':['Healthcare','Biotechnology'],'SAVA':['Healthcare','Biotechnology'],
  'NVAX':['Healthcare','Drug Manufacturers—General'],'NTRA':['Healthcare','Diagnostics & Research'],
  'EXAS':['Healthcare','Diagnostics & Research'],'PACB':['Healthcare','Diagnostics & Research'],
  'NKTR':['Healthcare','Drug Manufacturers—Specialty & Generic'],'MNKD':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'ACCD':['Healthcare','Healthcare Plans'],'CLOV':['Healthcare','Healthcare Plans'],
  'OSCR':['Healthcare','Healthcare Plans'],'OPRX':['Healthcare','Healthcare Plans'],

  'AMZN':['Consumer Cyclical','Internet Retail'],'TSLA':['Consumer Cyclical','Auto Manufacturers'],
  'HD':['Consumer Cyclical','Home Improvement Retail'],'LOW':['Consumer Cyclical','Home Improvement Retail'],
  'MCD':['Consumer Cyclical','Restaurants'],'SBUX':['Consumer Cyclical','Restaurants'],
  'NKE':['Consumer Cyclical','Footwear & Accessories'],'TGT':['Consumer Cyclical','Discount Stores'],
  'BKNG':['Consumer Cyclical','Travel Services'],'ABNB':['Consumer Cyclical','Travel Services'],
  'EXPE':['Consumer Cyclical','Travel Services'],'MAR':['Consumer Cyclical','Lodging'],
  'HLT':['Consumer Cyclical','Lodging'],'H':['Consumer Cyclical','Lodging'],
  'MGM':['Consumer Cyclical','Resorts & Casinos'],'WYNN':['Consumer Cyclical','Resorts & Casinos'],
  'LVS':['Consumer Cyclical','Resorts & Casinos'],'CZR':['Consumer Cyclical','Resorts & Casinos'],
  'DKNG':['Consumer Cyclical','Gambling'],'F':['Consumer Cyclical','Auto Manufacturers'],
  'GM':['Consumer Cyclical','Auto Manufacturers'],'RIVN':['Consumer Cyclical','Auto Manufacturers'],
  'LCID':['Consumer Cyclical','Auto Manufacturers'],'AZO':['Consumer Cyclical','Auto Parts'],
  'ORLY':['Consumer Cyclical','Auto Parts'],'AAP':['Consumer Cyclical','Auto Parts'],
  'AN':['Consumer Cyclical','Auto & Truck Dealerships'],'KMX':['Consumer Cyclical','Auto & Truck Dealerships'],
  'PAG':['Consumer Cyclical','Auto & Truck Dealerships'],'LAD':['Consumer Cyclical','Auto & Truck Dealerships'],
  'ABG':['Consumer Cyclical','Auto & Truck Dealerships'],'GPI':['Consumer Cyclical','Auto & Truck Dealerships'],
  'SAH':['Consumer Cyclical','Auto & Truck Dealerships'],'CVNA':['Consumer Cyclical','Auto & Truck Dealerships'],
  'EBAY':['Consumer Cyclical','Internet Retail'],'ETSY':['Consumer Cyclical','Internet Retail'],
  'W':['Consumer Cyclical','Internet Retail'],'BBY':['Consumer Cyclical','Specialty Retail'],
  'FIVE':['Consumer Cyclical','Specialty Retail'],'DLTR':['Consumer Cyclical','Discount Stores'],
  'DG':['Consumer Cyclical','Discount Stores'],'ULTA':['Consumer Cyclical','Specialty Retail'],
  'RL':['Consumer Cyclical','Apparel Retail'],'PVH':['Consumer Cyclical','Apparel Manufacturing'],
  'HBI':['Consumer Cyclical','Apparel Manufacturing'],'UAA':['Consumer Cyclical','Athletic & Outdoor Clothing'],
  'VFC':['Consumer Cyclical','Apparel Manufacturing'],'TPR':['Consumer Cyclical','Luxury Goods'],
  'CPRI':['Consumer Cyclical','Luxury Goods'],'TJX':['Consumer Cyclical','Apparel Retail'],
  'ROST':['Consumer Cyclical','Apparel Retail'],'GPS':['Consumer Cyclical','Apparel Retail'],
  'M':['Consumer Cyclical','Department Stores'],'JWN':['Consumer Cyclical','Department Stores'],
  'KSS':['Consumer Cyclical','Department Stores'],'DKS':['Consumer Cyclical','Specialty Retail'],
  'BOOT':['Consumer Cyclical','Specialty Retail'],'YUM':['Consumer Cyclical','Restaurants'],
  'QSR':['Consumer Cyclical','Restaurants'],'DPZ':['Consumer Cyclical','Restaurants'],
  'CMG':['Consumer Cyclical','Restaurants'],'TXRH':['Consumer Cyclical','Restaurants'],
  'SHAK':['Consumer Cyclical','Restaurants'],'WEN':['Consumer Cyclical','Restaurants'],
  'JACK':['Consumer Cyclical','Restaurants'],'CAKE':['Consumer Cyclical','Restaurants'],
  'CCL':['Consumer Cyclical','Travel Services'],'RCL':['Consumer Cyclical','Travel Services'],
  'NCLH':['Consumer Cyclical','Travel Services'],'LUV':['Consumer Cyclical','Airlines'],
  'DAL':['Consumer Cyclical','Airlines'],'UAL':['Consumer Cyclical','Airlines'],
  'AAL':['Consumer Cyclical','Airlines'],'ALK':['Consumer Cyclical','Airlines'],
  'JBLU':['Consumer Cyclical','Airlines'],'PLNT':['Consumer Cyclical','Leisure'],
  'PENN':['Consumer Cyclical','Resorts & Casinos'],'RRR':['Consumer Cyclical','Resorts & Casinos'],
  'CHDN':['Consumer Cyclical','Gambling'],'AEO':['Consumer Cyclical','Apparel Retail'],
  'ANF':['Consumer Cyclical','Apparel Retail'],'URBN':['Consumer Cyclical','Apparel Retail'],
  'VSCO':['Consumer Cyclical','Apparel Retail'],'BURL':['Consumer Cyclical','Apparel Retail'],
  'CROX':['Consumer Cyclical','Footwear & Accessories'],'DECK':['Consumer Cyclical','Footwear & Accessories'],
  'SKX':['Consumer Cyclical','Footwear & Accessories'],'ONON':['Consumer Cyclical','Footwear & Accessories'],
  'POOL':['Consumer Cyclical','Specialty Retail'],'OLLI':['Consumer Cyclical','Discount Stores'],
  'CHH':['Consumer Cyclical','Lodging'],'WH':['Consumer Cyclical','Lodging'],
  'IHG':['Consumer Cyclical','Lodging'],

  'WMT':['Consumer Defensive','Discount Stores'],'COST':['Consumer Defensive','Discount Stores'],
  'KR':['Consumer Defensive','Grocery Stores'],'SFM':['Consumer Defensive','Grocery Stores'],
  'ACI':['Consumer Defensive','Grocery Stores'],'GO':['Consumer Defensive','Grocery Stores'],
  'PG':['Consumer Defensive','Household & Personal Products'],'CL':['Consumer Defensive','Household & Personal Products'],
  'CHD':['Consumer Defensive','Household & Personal Products'],'EL':['Consumer Defensive','Household & Personal Products'],
  'KMB':['Consumer Defensive','Household & Personal Products'],'CLX':['Consumer Defensive','Household & Personal Products'],
  'ELF':['Consumer Defensive','Household & Personal Products'],'COTY':['Consumer Defensive','Household & Personal Products'],
  'KO':['Consumer Defensive','Beverages—Non-Alcoholic'],'PEP':['Consumer Defensive','Beverages—Non-Alcoholic'],
  'MNST':['Consumer Defensive','Beverages—Non-Alcoholic'],'CELH':['Consumer Defensive','Beverages—Non-Alcoholic'],
  'FIZZ':['Consumer Defensive','Beverages—Non-Alcoholic'],'TAP':['Consumer Defensive','Beverages—Brewers'],
  'SAM':['Consumer Defensive','Beverages—Brewers'],'STZ':['Consumer Defensive','Beverages—Wineries & Distilleries'],
  'MO':['Consumer Defensive','Tobacco'],'PM':['Consumer Defensive','Tobacco'],'BTI':['Consumer Defensive','Tobacco'],
  'MKC':['Consumer Defensive','Packaged Foods'],'GIS':['Consumer Defensive','Packaged Foods'],
  'CPB':['Consumer Defensive','Packaged Foods'],'HRL':['Consumer Defensive','Packaged Foods'],
  'SJM':['Consumer Defensive','Packaged Foods'],'MDLZ':['Consumer Defensive','Confectioners'],
  'HSY':['Consumer Defensive','Confectioners'],'POST':['Consumer Defensive','Packaged Foods'],
  'TSN':['Consumer Defensive','Packaged Foods'],'CAG':['Consumer Defensive','Packaged Foods'],
  'PPC':['Consumer Defensive','Packaged Foods'],'USFD':['Consumer Defensive','Food Distribution'],
  'SYY':['Consumer Defensive','Food Distribution'],'PFGC':['Consumer Defensive','Food Distribution'],
  'HAIN':['Consumer Defensive','Packaged Foods'],'SMPL':['Consumer Defensive','Packaged Foods'],
  'FRPT':['Consumer Defensive','Packaged Foods'],'WBA':['Consumer Defensive','Pharmaceutical Retailers'],

  'CAT':['Industrials','Farm & Heavy Construction Machinery'],'DE':['Industrials','Farm & Heavy Construction Machinery'],
  'HON':['Industrials','Conglomerates'],'MMM':['Industrials','Conglomerates'],
  'GE':['Industrials','Specialty Industrial Machinery'],'GEV':['Industrials','Specialty Industrial Machinery'],
  'EMR':['Industrials','Specialty Industrial Machinery'],'ETN':['Industrials','Specialty Industrial Machinery'],
  'PH':['Industrials','Specialty Industrial Machinery'],'ROK':['Industrials','Specialty Industrial Machinery'],
  'IR':['Industrials','Specialty Industrial Machinery'],'AME':['Industrials','Specialty Industrial Machinery'],
  'ROP':['Industrials','Specialty Industrial Machinery'],'ITW':['Industrials','Specialty Industrial Machinery'],
  'DOV':['Industrials','Specialty Industrial Machinery'],'IEX':['Industrials','Specialty Industrial Machinery'],
  'GNRC':['Industrials','Specialty Industrial Machinery'],'AOS':['Industrials','Specialty Industrial Machinery'],
  'NDSN':['Industrials','Specialty Industrial Machinery'],'CSWI':['Industrials','Specialty Industrial Machinery'],
  'XYL':['Industrials','Specialty Industrial Machinery'],'MAS':['Industrials','Specialty Industrial Machinery'],
  'FTV':['Industrials','Specialty Industrial Machinery'],'AAON':['Industrials','Building Products & Equipment'],
  'AWI':['Industrials','Building Products & Equipment'],'MHK':['Industrials','Building Products & Equipment'],
  'OC':['Industrials','Building Products & Equipment'],'CARR':['Industrials','Building Products & Equipment'],
  'TT':['Industrials','Building Products & Equipment'],'JCI':['Industrials','Building Products & Equipment'],
  'WMS':['Industrials','Building Products & Equipment'],'TREX':['Industrials','Building Products & Equipment'],
  'AZEK':['Industrials','Building Products & Equipment'],'BLDR':['Industrials','Building Products & Equipment'],
  'IBP':['Industrials','Building Products & Equipment'],'NVR':['Industrials','Residential Construction'],
  'PHM':['Industrials','Residential Construction'],'DHI':['Industrials','Residential Construction'],
  'LEN':['Industrials','Residential Construction'],'TOL':['Industrials','Residential Construction'],
  'MDC':['Industrials','Residential Construction'],'TMHC':['Industrials','Residential Construction'],
  'MHO':['Industrials','Residential Construction'],'GRBK':['Industrials','Residential Construction'],
  'LGIH':['Industrials','Residential Construction'],'SKY':['Industrials','Residential Construction'],
  'CVCO':['Industrials','Residential Construction'],'BA':['Industrials','Aerospace & Defense'],
  'RTX':['Industrials','Aerospace & Defense'],'LMT':['Industrials','Aerospace & Defense'],
  'NOC':['Industrials','Aerospace & Defense'],'GD':['Industrials','Aerospace & Defense'],
  'HII':['Industrials','Aerospace & Defense'],'TDG':['Industrials','Aerospace & Defense'],
  'HEI':['Industrials','Aerospace & Defense'],'LDOS':['Industrials','Aerospace & Defense'],
  'SAIC':['Industrials','Aerospace & Defense'],'BAH':['Industrials','Aerospace & Defense'],
  'CACI':['Industrials','Aerospace & Defense'],'MANT':['Industrials','Aerospace & Defense'],
  'AXON':['Industrials','Aerospace & Defense'],'CW':['Industrials','Aerospace & Defense'],
  'KTOS':['Industrials','Aerospace & Defense'],'DRS':['Industrials','Aerospace & Defense'],
  'TDY':['Industrials','Aerospace & Defense'],'MRCY':['Industrials','Aerospace & Defense'],
  'UPS':['Industrials','Integrated Freight & Logistics'],'FDX':['Industrials','Integrated Freight & Logistics'],
  'XPO':['Industrials','Trucking'],'ODFL':['Industrials','Trucking'],'SAIA':['Industrials','Trucking'],
  'ARCB':['Industrials','Trucking'],'JBHT':['Industrials','Trucking'],'KNX':['Industrials','Trucking'],
  'WERN':['Industrials','Trucking'],'CHRW':['Industrials','Integrated Freight & Logistics'],
  'EXPD':['Industrials','Integrated Freight & Logistics'],'GXO':['Industrials','Integrated Freight & Logistics'],
  'NSC':['Industrials','Railroads'],'CSX':['Industrials','Railroads'],'UNP':['Industrials','Railroads'],
  'CP':['Industrials','Railroads'],'CNI':['Industrials','Railroads'],'WAB':['Industrials','Railroads'],
  'URI':['Industrials','Rental & Leasing Services'],'AL':['Industrials','Rental & Leasing Services'],
  'R':['Industrials','Rental & Leasing Services'],'WCC':['Industrials','Industrial Distribution'],
  'MSM':['Industrials','Industrial Distribution'],'GWW':['Industrials','Industrial Distribution'],
  'FAST':['Industrials','Industrial Distribution'],'SITE':['Industrials','Industrial Distribution'],
  'BECN':['Industrials','Industrial Distribution'],'VMC':['Industrials','Building Materials'],
  'MLM':['Industrials','Building Materials'],'SUM':['Industrials','Building Materials'],
  'EXP':['Industrials','Building Materials'],'FELE':['Industrials','Specialty Industrial Machinery'],

  'XOM':['Energy','Oil & Gas Integrated'],'CVX':['Energy','Oil & Gas Integrated'],
  'COP':['Energy','Oil & Gas E&P'],'EOG':['Energy','Oil & Gas E&P'],
  'DVN':['Energy','Oil & Gas E&P'],'FANG':['Energy','Oil & Gas E&P'],
  'MPC':['Energy','Oil & Gas Refining & Marketing'],'VLO':['Energy','Oil & Gas Refining & Marketing'],
  'PSX':['Energy','Oil & Gas Refining & Marketing'],'HES':['Energy','Oil & Gas E&P'],
  'OXY':['Energy','Oil & Gas E&P'],'APA':['Energy','Oil & Gas E&P'],
  'MRO':['Energy','Oil & Gas E&P'],'OVV':['Energy','Oil & Gas E&P'],
  'SM':['Energy','Oil & Gas E&P'],'CIVI':['Energy','Oil & Gas E&P'],
  'MTDR':['Energy','Oil & Gas E&P'],'CPE':['Energy','Oil & Gas E&P'],
  'MGY':['Energy','Oil & Gas E&P'],'CRGY':['Energy','Oil & Gas E&P'],
  'NOG':['Energy','Oil & Gas E&P'],'VTLE':['Energy','Oil & Gas E&P'],
  'PR':['Energy','Oil & Gas E&P'],'TALO':['Energy','Oil & Gas E&P'],
  'HAL':['Energy','Oil & Gas Equipment & Services'],'SLB':['Energy','Oil & Gas Equipment & Services'],
  'BKR':['Energy','Oil & Gas Equipment & Services'],'OII':['Energy','Oil & Gas Equipment & Services'],
  'CHX':['Energy','Oil & Gas Equipment & Services'],'LBRT':['Energy','Oil & Gas Equipment & Services'],
  'KMI':['Energy','Oil & Gas Midstream'],'WMB':['Energy','Oil & Gas Midstream'],
  'OKE':['Energy','Oil & Gas Midstream'],'TRGP':['Energy','Oil & Gas Midstream'],
  'ET':['Energy','Oil & Gas Midstream'],'MPLX':['Energy','Oil & Gas Midstream'],
  'LNG':['Energy','Oil & Gas Midstream'],'CTRA':['Energy','Oil & Gas E&P'],
  'AR':['Energy','Oil & Gas E&P'],'RRC':['Energy','Oil & Gas E&P'],
  'EQT':['Energy','Oil & Gas E&P'],'CHK':['Energy','Oil & Gas E&P'],
  'FSLR':['Energy','Solar'],'ENPH':['Energy','Solar'],'ARRY':['Energy','Solar'],
  'NOVA':['Energy','Solar'],'MAXN':['Energy','Solar'],'SHLS':['Energy','Solar'],
  'BE':['Energy','Electrical Equipment & Parts'],'PLUG':['Energy','Electrical Equipment & Parts'],

  'META':['Communication Services','Internet Content & Information'],
  'GOOGL':['Communication Services','Internet Content & Information'],
  'GOOG':['Communication Services','Internet Content & Information'],
  'NFLX':['Communication Services','Entertainment'],'DIS':['Communication Services','Entertainment'],
  'CMCSA':['Communication Services','Telecom Services'],'CHTR':['Communication Services','Telecom Services'],
  'T':['Communication Services','Telecom Services'],'VZ':['Communication Services','Telecom Services'],
  'TMUS':['Communication Services','Telecom Services'],'LUMN':['Communication Services','Telecom Services'],
  'DISH':['Communication Services','Telecom Services'],'SIRI':['Communication Services','Entertainment'],
  'LYV':['Communication Services','Entertainment'],'WMG':['Communication Services','Entertainment'],
  'PARA':['Communication Services','Entertainment'],'WBD':['Communication Services','Entertainment'],
  'FOX':['Communication Services','Entertainment'],'FOXA':['Communication Services','Entertainment'],
  'NYT':['Communication Services','Publishing'],'NWSA':['Communication Services','Publishing'],
  'IAC':['Communication Services','Internet Content & Information'],
  'PINS':['Communication Services','Internet Content & Information'],
  'SNAP':['Communication Services','Internet Content & Information'],
  'RDDT':['Communication Services','Internet Content & Information'],
  'MTCH':['Communication Services','Internet Content & Information'],
  'TTD':['Communication Services','Advertising Agencies'],'DV':['Communication Services','Advertising Agencies'],
  'MGNI':['Communication Services','Advertising Agencies'],'YELP':['Communication Services','Internet Content & Information'],
  'Z':['Communication Services','Internet Content & Information'],'ZG':['Communication Services','Internet Content & Information'],
  'TRIP':['Communication Services','Internet Content & Information'],
  'IDT':['Communication Services','Telecom Services'],

  'AMT':['Real Estate','REIT—Specialty'],'PLD':['Real Estate','REIT—Industrial'],
  'EQIX':['Real Estate','REIT—Specialty'],'CCI':['Real Estate','REIT—Specialty'],
  'SBAC':['Real Estate','REIT—Specialty'],'SPG':['Real Estate','REIT—Retail'],
  'O':['Real Estate','REIT—Retail'],'WPC':['Real Estate','REIT—Diversified'],
  'VICI':['Real Estate','REIT—Diversified'],'GLPI':['Real Estate','REIT—Diversified'],
  'EQR':['Real Estate','REIT—Residential'],'AVB':['Real Estate','REIT—Residential'],
  'ESS':['Real Estate','REIT—Residential'],'UDR':['Real Estate','REIT—Residential'],
  'CPT':['Real Estate','REIT—Residential'],'MAA':['Real Estate','REIT—Residential'],
  'NNN':['Real Estate','REIT—Retail'],'EPRT':['Real Estate','REIT—Retail'],
  'ADC':['Real Estate','REIT—Retail'],'KIM':['Real Estate','REIT—Retail'],
  'REG':['Real Estate','REIT—Retail'],'FRT':['Real Estate','REIT—Retail'],
  'BXP':['Real Estate','REIT—Office'],'VNO':['Real Estate','REIT—Office'],
  'SLG':['Real Estate','REIT—Office'],'HIW':['Real Estate','REIT—Office'],
  'ARE':['Real Estate','REIT—Office'],'EXR':['Real Estate','REIT—Industrial'],
  'PSA':['Real Estate','REIT—Industrial'],'CUBE':['Real Estate','REIT—Industrial'],
  'STAG':['Real Estate','REIT—Industrial'],'REXR':['Real Estate','REIT—Industrial'],
  'EGP':['Real Estate','REIT—Industrial'],'FR':['Real Estate','REIT—Industrial'],
  'TRNO':['Real Estate','REIT—Industrial'],'COLD':['Real Estate','REIT—Industrial'],
  'DRH':['Real Estate','REIT—Hotel & Motel'],'PK':['Real Estate','REIT—Hotel & Motel'],
  'HST':['Real Estate','REIT—Hotel & Motel'],'RHP':['Real Estate','REIT—Hotel & Motel'],
  'WELL':['Real Estate','REIT—Healthcare Facilities'],'VTR':['Real Estate','REIT—Healthcare Facilities'],
  'PEAK':['Real Estate','REIT—Healthcare Facilities'],'OHI':['Real Estate','REIT—Healthcare Facilities'],
  'NHI':['Real Estate','REIT—Healthcare Facilities'],'CTRE':['Real Estate','REIT—Healthcare Facilities'],
  'MPW':['Real Estate','REIT—Healthcare Facilities'],'HR':['Real Estate','REIT—Healthcare Facilities'],
  'INVH':['Real Estate','REIT—Residential'],'AMH':['Real Estate','REIT—Residential'],
  'AIRC':['Real Estate','REIT—Residential'],'IRM':['Real Estate','REIT—Specialty'],
  'DLR':['Real Estate','REIT—Specialty'],'WY':['Real Estate','REIT—Specialty'],
  'RYN':['Real Estate','REIT—Specialty'],'IIPR':['Real Estate','REIT—Industrial'],
  'NTST':['Real Estate','REIT—Retail'],'PINE':['Real Estate','REIT—Retail'],
  'GNL':['Real Estate','REIT—Diversified'],'PDM':['Real Estate','REIT—Office'],
  'SBRA':['Real Estate','REIT—Healthcare Facilities'],'CHCT':['Real Estate','REIT—Healthcare Facilities'],
  'DHC':['Real Estate','REIT—Healthcare Facilities'],'GMRE':['Real Estate','REIT—Healthcare Facilities'],

  'NEE':['Utilities','Utilities—Regulated Electric'],'DUK':['Utilities','Utilities—Regulated Electric'],
  'SO':['Utilities','Utilities—Regulated Electric'],'D':['Utilities','Utilities—Regulated Electric'],
  'AEP':['Utilities','Utilities—Regulated Electric'],'EXC':['Utilities','Utilities—Regulated Electric'],
  'XEL':['Utilities','Utilities—Regulated Electric'],'ED':['Utilities','Utilities—Regulated Electric'],
  'WEC':['Utilities','Utilities—Regulated Electric'],'ETR':['Utilities','Utilities—Regulated Electric'],
  'PPL':['Utilities','Utilities—Regulated Electric'],'CNP':['Utilities','Utilities—Regulated Electric'],
  'AES':['Utilities','Utilities—Diversified'],'ES':['Utilities','Utilities—Regulated Electric'],
  'FE':['Utilities','Utilities—Regulated Electric'],'NRG':['Utilities','Utilities—Independent Power Producers'],
  'VST':['Utilities','Utilities—Independent Power Producers'],'CEG':['Utilities','Utilities—Independent Power Producers'],
  'PCG':['Utilities','Utilities—Regulated Electric'],'SRE':['Utilities','Utilities—Diversified'],
  'CMS':['Utilities','Utilities—Regulated Electric'],'LNT':['Utilities','Utilities—Regulated Electric'],
  'EVRG':['Utilities','Utilities—Regulated Electric'],'OGE':['Utilities','Utilities—Regulated Electric'],
  'POR':['Utilities','Utilities—Regulated Electric'],'OTTR':['Utilities','Utilities—Regulated Electric'],
  'MGEE':['Utilities','Utilities—Regulated Electric'],'IDA':['Utilities','Utilities—Regulated Electric'],
  'AWK':['Utilities','Utilities—Regulated Water'],'NI':['Utilities','Utilities—Regulated Gas'],
  'ATO':['Utilities','Utilities—Regulated Gas'],'NWN':['Utilities','Utilities—Regulated Gas'],
  'CWEN':['Utilities','Utilities—Renewable'],'BEP':['Utilities','Utilities—Renewable'],

  'LIN':['Basic Materials','Specialty Chemicals'],'APD':['Basic Materials','Specialty Chemicals'],
  'SHW':['Basic Materials','Specialty Chemicals'],'ECL':['Basic Materials','Specialty Chemicals'],
  'PPG':['Basic Materials','Specialty Chemicals'],'DD':['Basic Materials','Specialty Chemicals'],
  'DOW':['Basic Materials','Specialty Chemicals'],'EMN':['Basic Materials','Specialty Chemicals'],
  'CE':['Basic Materials','Specialty Chemicals'],'RPM':['Basic Materials','Specialty Chemicals'],
  'HUN':['Basic Materials','Specialty Chemicals'],'OLN':['Basic Materials','Specialty Chemicals'],
  'WLK':['Basic Materials','Specialty Chemicals'],'AXTA':['Basic Materials','Specialty Chemicals'],
  'FMC':['Basic Materials','Agricultural Inputs'],'MOS':['Basic Materials','Agricultural Inputs'],
  'CF':['Basic Materials','Agricultural Inputs'],'NTR':['Basic Materials','Agricultural Inputs'],
  'CTVA':['Basic Materials','Agricultural Inputs'],'NEM':['Basic Materials','Gold'],
  'GOLD':['Basic Materials','Gold'],'AEM':['Basic Materials','Gold'],'KGC':['Basic Materials','Gold'],
  'PAAS':['Basic Materials','Silver'],'FCX':['Basic Materials','Copper'],'SCCO':['Basic Materials','Copper'],
  'AA':['Basic Materials','Aluminum'],'CENX':['Basic Materials','Aluminum'],
  'X':['Basic Materials','Steel'],'NUE':['Basic Materials','Steel'],'STLD':['Basic Materials','Steel'],
  'CLF':['Basic Materials','Steel'],'CMC':['Basic Materials','Steel'],'RS':['Basic Materials','Steel'],
  'ATI':['Basic Materials','Steel'],'ZEUS':['Basic Materials','Steel'],
  'IP':['Basic Materials','Paper & Paper Products'],'GPK':['Basic Materials','Packaging & Containers'],
  'PKG':['Basic Materials','Packaging & Containers'],'SEE':['Basic Materials','Packaging & Containers'],
  'SON':['Basic Materials','Packaging & Containers'],'BERY':['Basic Materials','Packaging & Containers'],
  'AMCR':['Basic Materials','Packaging & Containers'],'WRK':['Basic Materials','Packaging & Containers'],
};

function getTickerSector(ticker) {
  return TICKER_SECTOR_MAP[ticker] || null;
}

// Per-window cache: { 7: {result, time}, 30: {result, time}, 90: {result, time} }
const _sectorResultCache = {};
const SECTOR_RESULT_TTL = 30 * 60 * 1000;

function buildSectorResult(days) {
  const rows = db.prepare(`
    SELECT
      ticker, MAX(company) AS company,
      SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END)           AS buy_val,
      SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
      COUNT(CASE WHEN TRIM(type)='P' THEN 1 END)                                 AS buy_count,
      COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END)                       AS sell_count,
      COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END)                  AS buyer_count,
      MAX(CASE WHEN TRIM(type)='P' THEN trade_date END)                          AS latest_buy
    FROM trades
    WHERE trade_date >= date('now', '-' || ? || ' days')
      AND TRIM(type) IN ('P','S','S-')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
    GROUP BY ticker
    HAVING buy_count > 0 OR sell_count > 0
    ORDER BY (buy_val + sell_val) DESC
    LIMIT 500
  `).all(days);

  const sectorMap = {};
  let mapped = 0, skipped = 0;
  rows.forEach(r => {
    const info = getTickerSector(r.ticker);
    if (!info) { skipped++; return; }
    mapped++;
    const [sector, industry] = info;
    if (!sectorMap[sector]) {
      sectorMap[sector] = {
        sector, buy_val: 0, sell_val: 0, buy_count: 0, sell_count: 0,
        buyer_count: 0, ticker_count: 0, tickers: [], industries: {}
      };
    }
    const s = sectorMap[sector];
    s.buy_val     += r.buy_val  || 0;
    s.sell_val    += r.sell_val || 0;
    s.buy_count   += r.buy_count  || 0;
    s.sell_count  += r.sell_count || 0;
    s.buyer_count += r.buyer_count || 0;
    s.ticker_count++;
    if (r.buy_val > 0) s.tickers.push({ ticker: r.ticker, company: r.company || r.ticker, buy_val: r.buy_val, buyer_count: r.buyer_count, latest_buy: r.latest_buy });
    if (industry) s.industries[industry] = (s.industries[industry] || 0) + (r.buy_val || 0);
  });

  slog(`Sectors(${days}d): ${rows.length} tickers, ${mapped} mapped, ${skipped} no-sector`);

  const sectors = Object.values(sectorMap)
    .filter(s => s.buy_val > 0 || s.sell_val > 0)
    .map(s => ({
      ...s,
      tickers: s.tickers.sort((a, b) => b.buy_val - a.buy_val).slice(0, 10),
      top_industry: Object.entries(s.industries).sort((a, b) => b[1] - a[1])[0]?.[0] || '',
      sentiment: s.buy_val + s.sell_val > 0
        ? Math.round(s.buy_val / (s.buy_val + s.sell_val) * 100)
        : 0,
    }))
    .sort((a, b) => b.buy_val - a.buy_val);

  return { sectors, days, total_buy_val: sectors.reduce((s, x) => s + x.buy_val, 0) };
}

app.get('/api/sectors', (req, res) => {
  try {
    const days = [7, 30, 90].includes(parseInt(req.query.days)) ? parseInt(req.query.days) : 30;
    const now  = Date.now();
    const entry = _sectorResultCache[days];
    if (entry && now - entry.time < SECTOR_RESULT_TTL) {
      return res.json(entry.result);
    }
    const result = buildSectorResult(days);
    _sectorResultCache[days] = { result, time: now };
    res.json(result);
  } catch(e) {
    slog('sectors error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── DEBUG ENDPOINT — shows DB stats for diagnosing data issues ───────────
app.get('/api/debug', (req, res) => {
  try {
    const total = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    const byType = db.prepare("SELECT COALESCE(TRIM(type),'NULL') AS type, COUNT(*) AS n FROM trades GROUP BY TRIM(type) ORDER BY n DESC LIMIT 20").all();
    const dates = db.prepare("SELECT MAX(trade_date) AS td_latest, MIN(trade_date) AS td_earliest, MAX(filing_date) AS fd_latest, MIN(filing_date) AS fd_earliest FROM trades WHERE trade_date IS NOT NULL").get();
    const p7_trade   = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-7 days')").get().n;
    const p30_trade  = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-30 days')").get().n;
    const p30_filing = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND filing_date >= date('now','-30 days')").get().n;
    const p3yr_trade = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-1095 days')").get().n;
    const ranker30   = db.prepare("SELECT COUNT(DISTINCT ticker) AS n FROM trades WHERE filing_date >= date('now','-30 days')").get().n;
    const history1095 = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date >= date('now','-1095 days')").get().n;

    // Test the exact screener query to confirm it works
    let screenerTest = null, screenerError = null;
    try {
      const rows = db.prepare(`
        SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
               trade_date AS trade, MAX(filing_date) AS filing,
               TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
               MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
        FROM trades
        WHERE trade_date >= date('now', '-7 days')
          AND TRIM(type) IN ('P','S','S-')
          AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        GROUP BY ticker, insider, trade_date, type
        ORDER BY trade_date DESC LIMIT 5
      `).all();
      screenerTest = { ok: true, rows: rows.length };
    } catch(e) { screenerError = e.message; }

    res.json({
      db_path: DB_PATH,
      total_rows: total,
      by_type: byType,
      dates,
      buys_last_7d_by_trade_date: p7_trade,
      buys_last_30d_by_trade_date: p30_trade,
      buys_last_30d_by_filing_date: p30_filing,
      buys_3yr: p3yr_trade,
      tickers_filed_30d: ranker30,
      history_3yr_buys: history1095,
      screener_query_test: screenerTest || { ok: false, error: screenerError },
      server_version: '2026-03-11',
      sync_running: syncRunning,
      daily_running: dailyRunning,
    });
  } catch(e) { res.status(500).json({ error: e.message, db_path: DB_PATH }); }
});

// ── PRICE FETCH HELPER ──────────────────────────────────────────────────────
// In-memory LRU-style price cache: ticker → { bars, fetchedAt }
// bars = [{time:'YYYY-MM-DD', open, high, low, close, volume}, ...]
const _priceCache = {};
const PRICE_TTL   = 12 * 60 * 60 * 1000; // 12h

function getPC(sym) {
  const e = _priceCache[sym];
  if (!e || Date.now() - e.fetchedAt > PRICE_TTL) return null;
  return e.bars;
}
function setPC(sym, bars) {
  _priceCache[sym] = { bars, fetchedAt: Date.now() };
}

// Fetch 2 years of daily OHLCV bars. Try Stooq → FMP → Yahoo in order.
async function fetchPriceBars(sym) {
  const cached = getPC(sym);
  if (cached) return cached;

  // --- Stooq ---
  try {
    const url = `https://stooq.com/q/d/l/?s=${sym.toLowerCase()}.us&i=d`;
    const { status, body } = await get(url, 8000);
    if (status === 200) {
      const text = body.toString();
      const lines = text.trim().split('\n').slice(1);
      if (lines.length > 30) {
        const bars = lines.map(l => {
          const [date, open, high, low, close, volume] = l.split(',');
          return { time: date, open: +open, high: +high, low: +low, close: +close, volume: +volume||0 };
        }).filter(b => b.time && b.close > 0);
        if (bars.length > 30) { setPC(sym, bars); return bars; }
      }
    }
  } catch(_) {}

  // --- FMP ---
  try {
    if (FMP) {
      const url = `https://financialmodelingprep.com/api/v3/historical-price-full/${sym}?serietype=line&timeseries=504&apikey=${FMP}`;
      const { status, body } = await get(url, 8000);
      if (status === 200) {
        const data = JSON.parse(body.toString());
        const hist = (data.historical || []).map(d => ({
          time: d.date, open: d.open||d.close, high: d.high||d.close,
          low: d.low||d.close, close: d.close, volume: d.volume||0
        })).filter(b => b.close > 0).reverse();
        if (hist.length > 30) { setPC(sym, hist); return hist; }
      }
    }
  } catch(_) {}

  // --- Yahoo Finance ---
  try {
    const end   = Math.floor(Date.now() / 1000);
    const start = end - 2 * 365 * 86400;
    const url   = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1d&period1=${start}&period2=${end}`;
    const { status, body } = await get(url, 8000);
    if (status === 200) {
      const data = JSON.parse(body.toString());
      const result = data?.chart?.result?.[0];
      const ts     = result?.timestamp || [];
      const q      = result?.indicators?.quote?.[0] || {};
      const bars   = ts.map((t, i) => ({
        time: new Date(t * 1000).toISOString().slice(0, 10),
        open: q.open?.[i]||0, high: q.high?.[i]||0, low: q.low?.[i]||0,
        close: q.close?.[i]||0, volume: q.volume?.[i]||0
      })).filter(b => b.close > 0);
      if (bars.length > 30) { setPC(sym, bars); return bars; }
    }
  } catch(_) {}

  return null;
}

// Warm price cache for the most-active tickers at startup
async function warmPriceCache() {
  try {
    const rows = db.prepare(`
      SELECT ticker, COUNT(*) AS n FROM trades
      WHERE TRIM(type)='P' AND trade_date >= date('now','-365 days')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      GROUP BY ticker ORDER BY n DESC LIMIT 180
    `).all();
    slog(`Warming price cache for ${rows.length} tickers...`);
    // Batch in groups of 10, 300ms delay between batches
    for (let i = 0; i < rows.length; i += 10) {
      const batch = rows.slice(i, i + 10).map(r => r.ticker);
      await Promise.allSettled(batch.map(sym => fetchPriceBars(sym)));
      if (i + 10 < rows.length) await new Promise(r => setTimeout(r, 300));
    }
    slog('Price cache warm-up complete');
  } catch(e) { slog('warmPriceCache error: ' + e.message); }
}

app.get('/api/price', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  const bars = await fetchPriceBars(sym);
  if (res.headersSent) return;
  bars ? res.json(bars) : res.status(404).json({ error: `No price data for ${sym}` });
});

// Returns {TICKER: bars[], ...} for up to 20 tickers in one call
app.get('/api/prices-bulk', async (req, res) => {
  const raw  = (req.query.symbols || '').toUpperCase().trim();
  if (!raw) return res.status(400).json({ error: 'symbols required' });
  const syms = [...new Set(raw.split(',').map(s => s.trim()).filter(Boolean))].slice(0, 20);
  const cold = syms.filter(s => !getPC(s));
  if (cold.length) await Promise.allSettled(cold.map(s => fetchPriceBars(s)));
  const result = {};
  syms.forEach(s => { const b = getPC(s); if (b) result[s] = b; });
  res.json(result);
});

// ── POST-BUY DRIFT ───────────────────────────────────────────────────────────
// Measures average D+30 / D+90 / D+180 returns after every insider buy.
// Uses the price already stored in each trade row (SEC-reported price) plus
// the warm price-bar cache for forward price lookup — no extra API calls.
let _driftServerCache     = null;
let _driftServerCacheTime = 0;
const DRIFT_TTL = 6 * 60 * 60 * 1000;

async function preComputeDrift() {
  try {
    slog('Pre-computing drift...');
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        GROUP_CONCAT(trade_date || '|' || COALESCE(price,0), ';;') AS trade_series,
        COUNT(*) AS buy_count,
        MAX(trade_date) AS last_buy
      FROM trades
      WHERE TRIM(type)='P'
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND trade_date <= date('now','-95 days')
        AND price > 0
      GROUP BY ticker
      HAVING buy_count >= 2
      ORDER BY buy_count DESC
      LIMIT 80
    `).all();

    if (!rows.length) { slog('Drift pre-compute: no rows'); return; }

    // Fetch price bars for all tickers
    await Promise.allSettled(rows.map(r => fetchPriceBars(r.ticker)));

    const results = [];
    rows.forEach(r => {
      try {
        const bars = getPC(r.ticker);
        if (!bars || bars.length < 30) return;

        const trades = (r.trade_series || '').split(';;').map(s => {
          const [dt, prStr] = s.split('|');
          return { date: dt?.slice(0, 10), price: parseFloat(prStr) || 0 };
        }).filter(t => t.date && t.price > 0);

        if (trades.length < 2) return;

        const fwd = (buyDate, buyPrice, days) => {
          const fd = new Date(buyDate + 'T12:00:00Z');
          fd.setUTCDate(fd.getUTCDate() + days);
          const fs = fd.toISOString().slice(0, 10);
          const bar = bars.find(b => b.time >= fs);
          return bar ? +((bar.close - buyPrice) / buyPrice * 100).toFixed(2) : null;
        };

        const scored = trades.map(t => ({
          ret1:   fwd(t.date, t.price, 30),   // ~D+30 calendar = ~D+1 trading name
          ret5:   fwd(t.date, t.price, 90),   // ~D+90
          ret20:  fwd(t.date, t.price, 180),  // ~D+180
          ret60:  fwd(t.date, t.price, 365),  // ~D+365
        }));

        const avg = (cp) => {
          const vals = scored.map(s => s[cp]).filter(v => v !== null);
          return vals.length ? +(vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(1) : null;
        };

        const avgObj = { 1: avg('ret1'), 5: avg('ret5'), 20: avg('ret20'), 60: avg('ret60') };
        const wins = scored.filter(s => s.ret5 !== null && s.ret5 > 0).length;
        const total = scored.filter(s => s.ret5 !== null).length;
        const winRate = total > 0 ? Math.round(wins / total * 100) : 0;

        // Weighted drift score
        const weights = { 1: 0.10, 5: 0.20, 20: 0.30, 60: 0.40 };
        let wscore = 0, wsum = 0;
        Object.entries(weights).forEach(([cp, w]) => {
          const v = avgObj[parseInt(cp)];
          if (v !== null) { wscore += v * w; wsum += w; }
        });
        const weightedAvg = wsum > 0 ? +(wscore / wsum).toFixed(1) : 0;

        // Score 0-100: base from weighted avg + win rate bonus + acceleration bonus
        const accelerates = avgObj[5] !== null && avgObj[20] !== null && avgObj[20] > avgObj[5];
        const base  = Math.min(60, Math.max(0, weightedAvg * 3 + 30));
        const wrBonus = Math.min(25, winRate * 0.25);
        const accBonus = accelerates ? 15 : 0;
        const score = Math.min(100, Math.round(base + wrBonus + accBonus));

        if (score < 10) return;

        results.push({
          ticker: r.ticker, company: r.company || r.ticker,
          avg: avgObj, score, winRate, weightedAvg,
          buyCount: trades.length, lastBuy: r.last_buy,
          accelerates
        });
      } catch(e) {}
    });

    results.sort((a, b) => b.score - a.score);
    _driftServerCache     = results;
    _driftServerCacheTime = Date.now();
    slog(`Drift pre-computed: ${results.length} tickers`);
  } catch(e) { slog('preComputeDrift error: ' + e.message); }
}

app.get('/api/drift', async (req, res) => {
  if (_driftServerCache && Date.now() - _driftServerCacheTime < DRIFT_TTL) {
    return res.json(_driftServerCache);
  }
  await preComputeDrift();
  res.json(_driftServerCache || []);
});

// ── INSIDER EVENT PROXIMITY ──────────────────────────────────────────────────
// Finds insider buys within 120 days before a known upcoming event (earnings,
// 8-K, regulatory) and scores them by proximity.
let _proximityServerCache     = null;
let _proximityServerCacheTime = 0;
const PROXIMITY_TTL = 3 * 60 * 60 * 1000; // 3h

// Predict next quarterly earnings date based on trailing pattern
function predictNextEarnings(ticker, today) {
  // Pull last 4 earnings-like 8-K filing dates (form type ARS or 10-Q/10-K adjacent filings)
  // Fallback: estimate based on fiscal quarter pattern (every ~91 days)
  // Simple model: look at the last known filing date and add 91 days
  try {
    // Form 4 filing dates are NOT earnings dates — they're SEC filing deadlines.
    // Best we can do without an earnings calendar: estimate next quarter based on
    // the latest trade_date seen (not filing_date), which at least correlates with
    // fiscal activity. Label honestly as an estimate.
    const rows = db.prepare(`
      SELECT MAX(trade_date) AS latest_trade FROM trades
      WHERE ticker = ? AND trade_date IS NOT NULL AND TRIM(type) = 'P'
    `).get(ticker);
    if (!rows?.latest_trade) return null;
    const latest = new Date(rows.latest_trade + 'T12:00:00Z');
    // Find the next ~quarter boundary after the latest known buy
    latest.setUTCDate(latest.getUTCDate() + 91);
    const predicted = latest.toISOString().slice(0, 10);
    return predicted > today ? { date: predicted, type: 'QUARTERLY', label: 'Est. Next Quarter', predicted: true } : null;
  } catch(_) { return null; }
}

async function preComputeProximity() {
  try {
    slog('Pre-computing proximity...');
    const today = new Date().toISOString().slice(0, 10);
    const since = new Date(Date.now() - 90 * 86400000).toISOString().slice(0, 10);
    const future = new Date(Date.now() + 120 * 86400000).toISOString().slice(0, 10);

    const rows = db.prepare(`
      SELECT
        t.ticker, t.company, t.insider, t.title,
        t.trade_date AS buyDate,
        t.value AS buyVal,
        t.filing_date
      FROM trades t
      WHERE TRIM(t.type) = 'P'
        AND t.trade_date >= ?
        AND t.trade_date <= ?
        AND t.ticker GLOB '[A-Z]*' AND LENGTH(t.ticker) BETWEEN 1 AND 6
        AND t.value > 0
      ORDER BY t.trade_date DESC
      LIMIT 400
    `).all(since, today);

    const results = [];
    const seenPairs = new Set(); // deduplicate ticker+insider

    rows.forEach(row => {
      try {
        const pairKey = `${row.ticker}|${row.insider}`;
        if (seenPairs.has(pairKey)) return;
        seenPairs.add(pairKey);

        // Predict next event
        const event = predictNextEarnings(row.ticker, row.buyDate);
        if (!event) return;

        const buyDt  = new Date(row.buyDate + 'T12:00:00Z');
        const evtDt  = new Date(event.date + 'T12:00:00Z');
        const daysTo = Math.round((evtDt - buyDt) / 86400000);

        if (daysTo < 0 || daysTo > 120) return;

        // Score
        let score = 0;
        if (daysTo <= 7)       score += 40;
        else if (daysTo <= 14) score += 28;
        else if (daysTo <= 21) score += 18;
        else if (daysTo <= 30) score += 10;
        else                   score += 5;

        if (event.type === 'QUARTERLY')      score += 10;
        else if (event.type === 'REGULATORY') score += 8;
        else                                  score += 5;

        const titleL = (row.title || '').toLowerCase();
        const isCsuite = /\b(ceo|cfo|coo|cto|president|chairman|chief)\b/.test(titleL);
        const isDir    = /\bdirector\b/.test(titleL);
        if (isCsuite) score += 8;
        else if (isDir) score += 4;

        if (row.buyVal >= 5000000) score += 12;
        else if (row.buyVal >= 1000000) score += 8;
        else if (row.buyVal >= 500000) score += 5;

        // Check repeat pattern: did this insider buy before prior predicted events?
        const priorBuys = db.prepare(`
          SELECT COUNT(*) AS n FROM trades
          WHERE insider = ? AND ticker = ? AND TRIM(type) = 'P'
            AND trade_date < ? AND trade_date >= date(?,' -365 days')
        `).get(row.insider, row.ticker, row.buyDate, row.buyDate);
        const repeatPattern = (priorBuys?.n || 0) >= 2;
        if (repeatPattern) score += 12;

        score = Math.min(100, score);

        const isAbnormal = (isCsuite || isDir) && daysTo <= 14 && event.type === 'QUARTERLY';
        const proximityColor = daysTo <= 7 ? 'var(--sell)' : daysTo <= 14 ? 'var(--option)' : daysTo <= 30 ? 'var(--accent)' : 'var(--muted)';

        results.push({
          ticker: row.ticker,
          company: row.company || row.ticker,
          insider: row.insider || '—',
          title: row.title || '—',
          buyDate: row.buyDate,
          buyVal: row.buyVal || 0,
          buyValue: row.buyVal || 0,
          nextEvent: event,
          allUpcoming: [event],
          daysTo,
          score,
          isAbnormal,
          repeatPattern,
          proximityColor,
        });
      } catch(_) {}
    });

    results.sort((a, b) => b.score - a.score);
    _proximityServerCache     = results;
    _proximityServerCacheTime = Date.now();
    slog(`Proximity pre-computed: ${results.length} results`);
  } catch(e) { slog('preComputeProximity error: ' + e.message); }
}

app.get('/api/proximity', async (req, res) => {
  if (_proximityServerCache && Date.now() - _proximityServerCacheTime < PROXIMITY_TTL) {
    return res.json(_proximityServerCache);
  }
  await preComputeProximity();
  res.json(_proximityServerCache || []);
});

// ── INSIDER SCOREBOARD ───────────────────────────────────────────────────────
// Scores every insider with 4+ completed buy trades against actual forward
// price returns at D+30 / D+90 / D+180. Uses the warm price cache.
let _scoreboardCache     = null;
let _scoreboardCacheTime = 0;
const SCOREBOARD_TTL = 6 * 60 * 60 * 1000;

async function preComputeScoreboard() {
  if (_scoreboardCache) return;
  try {
    slog('Pre-computing scoreboard...');
    const minBuys = 3, limit = 300;
    const rows = db.prepare(`
      SELECT
        insider, MAX(title) AS title, COUNT(*) AS buy_count,
        GROUP_CONCAT(ticker || '|' || trade_date || '|' || COALESCE(price,0) || '|' || COALESCE(value,0), ';;') AS trade_data,
        GROUP_CONCAT(DISTINCT ticker) AS tickers_csv,
        CAST(julianday(MAX(trade_date)) - julianday(MIN(trade_date)) AS INTEGER) AS span_days
      FROM trades
      WHERE TRIM(type) = 'P'
        AND insider IS NOT NULL AND ticker IS NOT NULL AND price > 0
        AND trade_date <= date('now', '-95 days')
      GROUP BY insider
      HAVING buy_count >= ? AND span_days >= 60
      ORDER BY buy_count DESC, span_days DESC LIMIT ?
    `).all(minBuys, limit);

    if (!rows.length) { slog('Scoreboard pre-compute: no qualifying rows'); return; }

    const allTickers = [...new Set(
      rows.flatMap(r => (r.tickers_csv || '').split(',').filter(Boolean))
    )].slice(0, 120);

    const priceEntries = await Promise.allSettled(
      allTickers.map(async sym => [sym, await fetchPriceBars(sym)])
    );
    const priceCache = Object.fromEntries(
      priceEntries.filter(r => r.status === 'fulfilled' && r.value[1]).map(r => r.value)
    );

    const accuracyResults = [], timingResults = [];

    rows.forEach(leader => {
      try {
        const rawTrades = (leader.trade_data || '').split(';;').map(s => {
          const [ticker, trade_date, priceStr, valueStr] = s.split('|');
          return { ticker, trade: trade_date, price: parseFloat(priceStr) || 0, value: parseFloat(valueStr) || 0 };
        }).filter(t => t.ticker && t.trade);

        if (rawTrades.length < 4) return;

        const scored = rawTrades.map(t => {
          const bars = priceCache[t.ticker] || [];
          if (!bars.length) return null;
          const buyDate  = t.trade.slice(0, 10);
          const entryBar = bars.find(b => b.time >= buyDate);
          const buyPrice = t.price > 0 ? t.price : (entryBar?.close || 0);
          if (!buyPrice || buyPrice <= 0) return null;
          const fwd = days => {
            const fd = new Date(buyDate + 'T12:00:00Z');
            fd.setUTCDate(fd.getUTCDate() + days);
            const fs  = fd.toISOString().slice(0, 10);
            const bar = bars.find(b => b.time >= fs);
            return bar ? +((bar.close - buyPrice) / buyPrice * 100).toFixed(2) : null;
          };
          return { ticker: t.ticker, tradeDate: buyDate, buyPrice,
            ret30: fwd(30), ret90: fwd(90), ret180: fwd(180) };
        }).filter(Boolean);

        const completed = scored.filter(s => s.ret90 !== null);
        if (completed.length < 3) return;

        const rets90   = completed.map(s => s.ret90);
        const avgRet90 = +(rets90.reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
        const rets30   = completed.filter(s => s.ret30 !== null).map(s => s.ret30);
        const avgRet30 = rets30.length ? +(rets30.reduce((a, b) => a + b, 0) / rets30.length).toFixed(1) : null;
        const winRate  = Math.round(rets90.filter(r => r > 0).length / rets90.length * 100);
        const avgMag   = +(rets90.map(Math.abs).reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
        const sortedR  = [...rets90].sort((a, b) => a - b);
        const median   = sortedR[Math.floor(sortedR.length / 2)];
        const consist  = Math.round(Math.min(100, Math.max(0, (median / Math.max(avgMag, 1) + 1) * 50)));
        const accScore = Math.round(Math.min(100, Math.max(0,
          winRate * 0.40 + Math.min(35, Math.max(0, avgRet90 / 20 * 35))
          + consist * 0.15 + Math.min(10, completed.length * 1.2)
        )));
        const tier     = accScore >= 75 ? 'ELITE' : accScore >= 55 ? 'STRONG' : accScore >= 35 ? 'AVERAGE' : 'WEAK';
        const tickers3 = [...new Set(rawTrades.map(t => t.ticker))].slice(0, 3).join(', ');

        if (accScore >= 35) {  // only include insiders with a meaningful accuracy score
          accuracyResults.push({ name: leader.insider, title: leader.title || '',
            accuracyScore: accScore, tier, winRate, avgRet90, avgRet30,
            tradeCount: completed.length, tickers: tickers3 });
        }

        // Timing alpha — measures quality of entry price relative to 12M range
        let nearLowCount = 0, ret90sum = 0, ret180sum = 0, retN = 0;
        scored.forEach(s => {
          const bars = priceCache[s.ticker] || [];
          if (!bars.length || !s.buyPrice) return;
          const yr1Start = new Date(s.tradeDate + 'T12:00:00Z');
          yr1Start.setUTCFullYear(yr1Start.getUTCFullYear() - 1);
          const yr1Bars = bars.filter(b =>
            b.time >= yr1Start.toISOString().slice(0, 10) && b.time <= s.tradeDate
          );
          if (yr1Bars.length < 20) return;
          const yr1Lo = Math.min(...yr1Bars.map(b => b.low || b.close));
          if ((s.buyPrice - yr1Lo) / yr1Lo * 100 <= 20) nearLowCount++;
          if (s.ret90  !== null) { ret90sum  += s.ret90;  retN++; }
          if (s.ret180 !== null)   ret180sum += s.ret180;
        });

        const n         = scored.length;
        const nearLowPct = n > 0 ? Math.round(nearLowCount / n * 100) : 0;
        const avgFwd90  = retN > 0 ? +(ret90sum  / retN).toFixed(1) : null;
        const avgFwd180 = retN > 0 ? +(ret180sum / retN).toFixed(1) : null;

        // Compute avg position in annual range
        const avgPos = scored.reduce((acc, s) => {
          const bars = priceCache[s.ticker] || [];
          if (!bars.length || !s.buyPrice) return acc;
          const yr1Start = new Date(s.tradeDate + 'T12:00:00Z');
          yr1Start.setUTCFullYear(yr1Start.getUTCFullYear() - 1);
          const yr1Bars = bars.filter(b =>
            b.time >= yr1Start.toISOString().slice(0, 10) && b.time <= s.tradeDate
          );
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
        const compSell = 12;
        const timingAlpha = Math.min(100, Math.max(0, compLow + compPos + compFwd + compSell));

        let verdict;
        if      (timingAlpha >= 80 && nearLowPct >= 50) verdict = 'Buys near bottoms';
        else if (timingAlpha >= 80)                     verdict = 'Elite forward returns';
        else if (timingAlpha >= 60)                     verdict = 'Above-average timing';
        else if (timingAlpha >= 40)                     verdict = 'Mixed timing signals';
        else                                            verdict = 'Tends to buy high';

        if (timingAlpha >= 35) {
          timingResults.push({ name: leader.insider, title: leader.title || '',
            timingAlpha, nearLowPct, avgRet90: avgFwd90, avgRet180: avgFwd180,
            verdict, tradeCount: completed.length, tickers: tickers3 });
        }
      } catch(e) { slog('scoreboard insider err: ' + e.message); }
    });

    accuracyResults.sort((a, b) => b.accuracyScore - a.accuracyScore);
    timingResults.sort((a, b) => b.timingAlpha - a.timingAlpha);

    const result = { accuracy: accuracyResults, timing: timingResults };
    _scoreboardCache     = result;
    _scoreboardCacheTime = Date.now();
    slog(`Scoreboard pre-computed: ${accuracyResults.length} accuracy, ${timingResults.length} timing`);
  } catch(e) { slog('preComputeScoreboard error: ' + e.message); }
}

app.get('/api/scoreboard', async (req, res) => {
  if (_scoreboardCache && Date.now() - _scoreboardCacheTime < SCOREBOARD_TTL) {
    return res.json(_scoreboardCache);
  }
  await preComputeScoreboard();
  res.json(_scoreboardCache || { accuracy: [], timing: [] });
});

// ── STARTUP PRECOMPUTES ──────────────────────────────────────────────────────
// Start daily ingestion immediately on boot (handles market-hours check internally)
runDaily(3);

// Stagger: price warm (60s) → drift + proximity (120s) → scoreboard (130s)
setTimeout(() => {
  warmPriceCache().then(() => {
    setTimeout(() => preComputeDrift().catch(e => slog('drift startup err: ' + e.message)), 10000);
    setTimeout(() => preComputeProximity().catch(e => slog('proximity startup err: ' + e.message)), 12000);
    setTimeout(() => preComputeScoreboard().catch(e => slog('scoreboard startup err: ' + e.message)), 20000);
  }).catch(e => slog('warmPriceCache startup err: ' + e.message));
}, 60000);

// Refresh every 12h
setInterval(() => {
  _scoreboardCache = null;
  warmPriceCache()
    .then(() => preComputeDrift())
    .then(() => preComputeProximity())
    .then(() => preComputeScoreboard())
    .catch(e => slog('refresh err: ' + e.message));
}, 12 * 60 * 60 * 1000);

// ── ADMIN TRIGGER ROUTES (no auth needed — internal use only) ───────────────
// POST /api/admin/sync — trigger a full historical sync (4 quarters)
app.get('/api/admin/sync', (req, res) => {
  runSync(parseInt(req.query.q || '4'));
  res.json({ ok: true, message: 'Sync triggered' });
});
// POST /api/admin/daily — trigger daily ingestion now regardless of market hours
app.get('/api/admin/daily', (req, res) => {
  if (dailyRunning) return res.json({ ok: false, message: 'Daily worker already running' });
  dailyRunning = true;
  const worker = require('child_process').spawn(
    process.execPath,
    ['--max-old-space-size=200', require('path').join(__dirname, 'daily-worker.js'), '7', 'poll'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[manual-daily] ' + l)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[manual-daily] ERR: ' + l)));
  worker.on('exit', code => { dailyRunning = false; slog(`manual-daily exited (${code})`); });
  res.json({ ok: true, message: 'Daily ingestion triggered (7-day backfill)' });
});

// SPA catch-all: serve index.html for any non-API route so that client-side
// routing (history.pushState) works correctly on hard refresh or direct URL navigation.
app.get('*', (req, res) => {
  if (req.path.startsWith('/api/')) return res.status(404).json({ error: 'Not found' });
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => console.log(`Server on port ${PORT}`));
