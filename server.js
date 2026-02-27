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

// ─── DB (read-only queries from server) ───────────────────────
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
  CREATE INDEX IF NOT EXISTS idx_insider    ON trades(insider);
  CREATE TABLE IF NOT EXISTS sync_log (
    quarter TEXT PRIMARY KEY, synced_at TEXT DEFAULT (datetime('now')), rows INTEGER
  );
`);

// ─── SYNC via child process ────────────────────────────────────
let workerProc  = null;
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

  // Give worker 400MB — main server keeps remaining RAM for Express + SQLite reads
  workerProc = spawn(
    process.execPath,
    ['--max-old-space-size=400', path.join(__dirname, 'sync-worker.js'), String(numQ)],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );

  workerProc.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog(l)));
  workerProc.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('ERR: ' + l)));

  workerProc.on('exit', code => {
    syncRunning = false;
    workerProc  = null;
    slog(`=== worker exited (code ${code}) ===`);
    if (code !== 0) slog('Sync failed — check logs above');
  });
}

// ─── PRICE CACHE ──────────────────────────────────────────────
const pc = new Map();
function getPC(k)      { const c = pc.get(k); return c && Date.now()<c.e ? c.v : null; }
function setPC(k,v,ms) { pc.set(k,{v,e:Date.now()+ms}); }

function get(url, ms=30000) {
  return new Promise((resolve,reject) => {
    const req = https.get(url,{
      headers:{'User-Agent':'InsiderTape/1.0 admin@insidertape.com'},
      timeout:ms,
    }, res => {
      if([301,302,303].includes(res.statusCode)&&res.headers.location)
        return get(res.headers.location,ms).then(resolve).catch(reject);
      const chunks=[];
      res.on('data',c=>chunks.push(c));
      res.on('end',()=>resolve({status:res.statusCode,body:Buffer.concat(chunks)}));
    });
    req.on('error',reject);
    req.on('timeout',()=>{req.destroy();reject(new Error('Timeout'));});
  });
}

// ─── ROUTES ───────────────────────────────────────────────────
app.get('/api/screener', (req, res) => {
  try {
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    if (n === 0 && syncRunning)
      return res.json({ building: true, message: 'Loading SEC data (~3 min on first run)...', trades: [] });
    const rows = db.prepare(`
      SELECT ticker,company,insider,title,
             trade_date AS trade, filing_date AS filing,
             type,qty,price,value,owned
      FROM trades
      WHERE trade_date >= date('now','-90 days')
      ORDER BY filing_date DESC, trade_date DESC LIMIT 500
    `).all();
    res.json(rows);
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/ticker', (req, res) => {
  const sym = (req.query.symbol||'').toUpperCase().trim();
  if (!sym) return res.status(400).json({error:'symbol required'});
  try {
    const rows = db.prepare(`
      SELECT ticker,company,insider,title,
             trade_date AS trade, filing_date AS filing,
             type,qty,price,value,owned
      FROM trades WHERE ticker=? ORDER BY trade_date DESC LIMIT 1000
    `).all(sym);
    res.json(rows);
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/insider', (req, res) => {
  const name = (req.query.name||'').trim();
  if (!name) return res.status(400).json({error:'name required'});
  try {
    const rows = db.prepare(`
      SELECT ticker,company,insider,title,
             trade_date AS trade, filing_date AS filing,
             type,qty,price,value,owned
      FROM trades WHERE UPPER(insider) LIKE UPPER(?) ORDER BY trade_date DESC LIMIT 500
    `).all(`%${name}%`);
    res.json(rows);
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/price', async (req, res) => {
  const sym = (req.query.symbol||'').toUpperCase().trim();
  if (!sym) return res.status(400).json({error:'symbol required'});
  const cached = getPC(sym);
  if (cached) return res.json(cached);
  try {
    const {status,body} = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${sym}&apikey=${FMP}`
    );
    if (status!==200) return res.status(502).json({error:`FMP HTTP ${status}`});
    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data)?data:(data?.historical||[]);
    if (!raw.length) return res.status(404).json({error:`No price data for ${sym}`});
    const bars = raw.slice().reverse()
      .map(d=>({time:d.date,open:+(+d.open).toFixed(2),high:+(+d.high).toFixed(2),low:+(+d.low).toFixed(2),close:+(+d.close).toFixed(2),volume:d.volume||0}))
      .filter(d=>d.close>0&&d.time);
    setPC(sym,bars,60*60*1000);
    res.json(bars);
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/status', (req, res) => {
  const n      = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const synced = db.prepare('SELECT * FROM sync_log ORDER BY synced_at').all();
  res.json({ running: syncRunning, trades: n, quarters: synced, log: syncLog.slice(-40) });
});

app.get('/api/sync', (req, res) => {
  const numQ = parseInt(req.query.quarters||'4');
  res.json({ started: !syncRunning });
  if (!syncRunning) runSync(numQ);
});

app.get('/api/diag', async (req, res) => {
  const n      = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const latest = db.prepare('SELECT MAX(trade_date) AS d FROM trades').get().d;
  const synced = db.prepare('SELECT * FROM sync_log').all();
  let price={};
  try {
    const {status,body} = await get(`https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`);
    const data=JSON.parse(body.toString());
    const raw=Array.isArray(data)?data:(data?.historical||[]);
    price={ok:status===200&&raw.length>0,bars:raw.length};
  } catch(e){price={ok:false,error:e.message};}
  res.json({db:{trades:n,latest,synced},price,sync:{running:syncRunning,log:syncLog.slice(-10)}});
});

app.get('/api/health', (req, res) =>
  res.json({ok:true,trades:db.prepare('SELECT COUNT(*) AS n FROM trades').get().n}));

// ─── STARTUP ──────────────────────────────────────────────────
const existing = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
console.log(`DB: ${existing} trades`);

if (existing === 0) {
  console.log('Empty — starting sync worker...');
  runSync(4);
} else {
  // Re-sync only current quarter
  const now = new Date();
  const yr  = now.getFullYear();
  const q   = Math.ceil((now.getMonth()+1)/3);
  const prev = q > 1 ? { year: yr, q: q-1 } : { year: yr-1, q: 4 };
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${prev.year}Q${prev.q}`);
  slog(`Re-syncing ${prev.year}Q${prev.q} for new filings...`);
  runSync(1);
}

setInterval(() => {
  const now = new Date();
  const yr  = now.getFullYear();
  const q   = Math.ceil((now.getMonth()+1)/3);
  const prev = q > 1 ? { year: yr, q: q-1 } : { year: yr-1, q: 4 };
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${prev.year}Q${prev.q}`);
  runSync(1);
}, 24*60*60*1000);

app.listen(PORT, () => console.log(`Server on port ${PORT}`));
