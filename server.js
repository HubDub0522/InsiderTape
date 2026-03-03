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
app.get('/', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

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
    // Always restart — this worker is meant to run continuously
    setTimeout(() => runDaily(2), 30 * 1000);
  });
}

// ─── PRICE CACHE ──────────────────────────────────────────────
const pc = new Map();
function getPC(k)      { const c = pc.get(k); return c && Date.now()<c.e ? c.v : null; }
function setPC(k,v,ms) { pc.set(k, { v, e: Date.now()+ms }); }

// ─── ROUTES ───────────────────────────────────────────────────

// SCREENER — most recent 500 trades, no date filter
// (SEC quarterly data has a publication lag; just show what we have newest-first)
app.get('/api/screener', (req, res) => {
  try {
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    if (n === 0 && syncRunning)
      return res.json({ building: true, message: 'Loading SEC data (~3 min)...', trades: [] });
    const days = Math.min(Math.max(parseInt(req.query.days || '30'), 1), 3650);
    let rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades
      WHERE filing_date >= date('now', '-' || ? || ' days')
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT 1000
    `).all(days);

    // If the date window returns nothing (worker hasn't run yet / DB is stale),
    // fall back to the most recent 500 trades regardless of date
    if (!rows.length && n > 0) {
      rows = db.prepare(`
        SELECT ticker, company, insider, title,
               trade_date AS trade, filing_date AS filing,
               type, qty, price, value, owned
        FROM trades
        ORDER BY filing_date DESC, trade_date DESC
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
      ORDER BY trade_date DESC LIMIT 1000
    `).all(sym);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// INSIDER
app.get('/api/insider', (req, res) => {
  const name  = (req.query.name  || '').trim();
  const exact = req.query.exact === '1';
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    const pattern = exact ? name : `%${name}%`;
    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades WHERE UPPER(insider) LIKE UPPER(?)
      ORDER BY trade_date DESC LIMIT 2000
    `).all(pattern);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PRICE — FMP with Yahoo Finance fallback
app.get('/api/price', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });

  const cached = getPC(sym);
  if (cached) return res.json(cached);

  // Try FMP first
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${sym}&apikey=${FMP}`
    );
    if (status === 200) {
      const data = JSON.parse(body.toString());
      const raw  = Array.isArray(data) ? data : (data?.historical || []);
      if (raw.length) {
        const bars = raw.slice().reverse()
          .map(d => ({ time: d.date, open: +(+d.open).toFixed(2), high: +(+d.high).toFixed(2), low: +(+d.low).toFixed(2), close: +(+d.close).toFixed(2), volume: d.volume||0 }))
          .filter(d => d.close > 0 && d.time);
        if (bars.length) {
          setPC(sym, bars, 60*60*1000);
          return res.json(bars);
        }
      }
    }
  } catch(e) { slog(`FMP price error for ${sym}: ${e.message}`); }

  // Fallback: Yahoo Finance
  try {
    const now   = Math.floor(Date.now() / 1000);
    const from  = now - 5 * 365 * 86400; // 5 years back
    const yUrl  = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?period1=${from}&period2=${now}&interval=1d&events=history`;
    const { status, body } = await get(yUrl, 20000);
    if (status === 200) {
      const json   = JSON.parse(body.toString());
      const result = json?.chart?.result?.[0];
      const times  = result?.timestamp || [];
      const q      = result?.indicators?.quote?.[0] || {};
      if (times.length && q.close?.length) {
        const bars = times.map((t, i) => ({
          time:   new Date(t * 1000).toISOString().slice(0, 10),
          open:   +(+(q.open?.[i]  || 0)).toFixed(2),
          high:   +(+(q.high?.[i]  || 0)).toFixed(2),
          low:    +(+(q.low?.[i]   || 0)).toFixed(2),
          close:  +(+(q.close?.[i] || 0)).toFixed(2),
          volume: q.volume?.[i] || 0,
        })).filter(d => d.close > 0 && d.time);
        if (bars.length) {
          setPC(sym, bars, 60*60*1000);
          return res.json(bars);
        }
      }
    }
  } catch(e) { slog(`Yahoo price error for ${sym}: ${e.message}`); }

  res.status(404).json({ error: `No price data available for ${sym}` });
});

// STATUS
app.get('/api/status', (req, res) => {
  const n      = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const latest = db.prepare('SELECT MAX(trade_date) AS d FROM trades').get().d;
  const synced = db.prepare('SELECT * FROM sync_log ORDER BY synced_at').all();
  res.json({ running: syncRunning, trades: n, latestTrade: latest, quarters: synced, log: syncLog.slice(-40) });
});

// FORCE SYNC
app.get('/api/daily-status', (req, res) => {
  try {
    const logs    = db.prepare('SELECT * FROM daily_log ORDER BY date DESC LIMIT 14').all();
    const recent  = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE filing_date >= date('now','-7 days')").get().n;
    const maxFile = db.prepare('SELECT MAX(filing_date) AS d FROM trades').get().d;
    res.json({ running: dailyRunning, recentDays: logs, tradesLast7d: recent, maxFilingDate: maxFile });
  } catch(e) { res.json({ running: dailyRunning, error: e.message }); }
});

// DEBUG: fetch one Form 4 live to see exactly what's failing
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
    // Step 1: Get one Form 4 from EFTS
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

      // Step 2: Try index.json
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
  // Delete daily_log entries for the range so worker re-fetches
  if (req.query.force === '1') {
    db.prepare("DELETE FROM daily_log WHERE date >= date('now', ? || ' days')").run(`-${days}`);
    slog(`Cleared daily_log for last ${days} days`);
  }
  res.json({ started: !dailyRunning });
  if (!dailyRunning) runDaily(days);
});

// One-shot backfill — runs alongside the continuous poll worker
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

// DIAG
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
    DELETE FROM trades
    WHERE trade_date  < '2000-01-01' OR trade_date  > '2030-12-31'
       OR filing_date < '2000-01-01' OR filing_date > '2030-12-31'
  `).run();
  slog(`Date cleanup: removed ${r.changes} bad rows`);
  const { n } = db.prepare('SELECT COUNT(*) AS n FROM trades').get();
  res.json({ removed: r.changes, remaining: n });
});

// ─── STARTUP ──────────────────────────────────────────────────
const existing  = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
const syncedQ   = db.prepare('SELECT COUNT(*) AS n FROM sync_log').get().n;
console.log(`DB: ${existing} trades, ${syncedQ} quarters synced`);

// Always ensure we have at least 12 quarters of history
// If we have fewer, queue up enough to reach 12
const TARGET_QUARTERS = 12;
if (syncedQ < TARGET_QUARTERS) {
  const needed = TARGET_QUARTERS - syncedQ;
  console.log(`Only ${syncedQ} quarters in DB — syncing ${needed} more to reach ${TARGET_QUARTERS}...`);
  runSync(TARGET_QUARTERS);
} else {
  // DB is populated — just re-sync the most recent quarter for new filings
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

// Daily re-sync at midnight
setInterval(() => {
  const now = new Date();
  const yr  = now.getFullYear();
  const q   = Math.ceil((now.getMonth() + 1) / 3);
  let tq = q - 1; let ty = yr;
  if (tq < 1) { tq = 4; ty--; }
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${ty}Q${tq}`);
  runSync(1);
}, 24 * 60 * 60 * 1000);

// Start daily-worker in continuous RSS poll mode.
// It runs indefinitely: polls EDGAR RSS every 2 min + daily EFTS backfill.
// We only need to spawn it once — it manages its own loop.
setTimeout(() => runDaily(3), 5000); // 5s after start

app.listen(PORT, () => console.log(`Server on port ${PORT}`));
