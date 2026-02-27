'use strict';

const express  = require('express');
const cors     = require('cors');
const https    = require('https');
const zlib     = require('zlib');
const path     = require('path');
const fs       = require('fs');
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

// ─── DATABASE ─────────────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('cache_size = -8000');  // 8MB cache only
db.pragma('temp_store = memory');

db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    company     TEXT,
    insider     TEXT,
    title       TEXT,
    trade_date  TEXT NOT NULL,
    filing_date TEXT,
    type        TEXT,
    qty         INTEGER,
    price       REAL,
    value       INTEGER,
    owned       INTEGER,
    accession   TEXT,
    UNIQUE(accession, insider, trade_date, type, qty)
  );
  CREATE INDEX IF NOT EXISTS idx_ticker     ON trades(ticker);
  CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date DESC);
  CREATE INDEX IF NOT EXISTS idx_insider    ON trades(insider);
  CREATE TABLE IF NOT EXISTS sync_log (
    quarter   TEXT PRIMARY KEY,
    synced_at TEXT DEFAULT (datetime('now')),
    rows      INTEGER
  );
`);

const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO trades
    (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);

// ─── HTTP ─────────────────────────────────────────────────────
function get(url, ms = 90000) {
  return new Promise((resolve, reject) => {
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

// ─── SYNC ─────────────────────────────────────────────────────
const syncState = { running: false, log: [] };
function slog(msg) {
  const line = `[${new Date().toISOString().slice(11,19)}] ${msg}`;
  console.log(line);
  syncState.log.push(line);
  if (syncState.log.length > 200) syncState.log.shift();
}

function parseDate(s) {
  if (!s) return null;
  const mon = {JAN:'01',FEB:'02',MAR:'03',APR:'04',MAY:'05',JUN:'06',
               JUL:'07',AUG:'08',SEP:'09',OCT:'10',NOV:'11',DEC:'12'};
  const m = s.match(/^(\d{2})-([A-Z]{3})-(\d{4})$/i);
  if (m) return `${m[3]}-${mon[m[2].toUpperCase()]||'01'}-${m[1]}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.slice(0, 10);
  return null;
}

// Extract a single named file from ZIP buffer, return lines array
// Frees the compressed buffer after inflation — never holds >1 file at once
function extractOne(zipBuf, targetName) {
  let pos = 0;
  while (pos < zipBuf.length - 4) {
    if (zipBuf[pos] !== 0x50 || zipBuf[pos+1] !== 0x4B ||
        zipBuf[pos+2] !== 0x03 || zipBuf[pos+3] !== 0x04) { pos++; continue; }

    const compression = zipBuf.readUInt16LE(pos + 8);
    const compSize    = zipBuf.readUInt32LE(pos + 18);
    const fnLen       = zipBuf.readUInt16LE(pos + 26);
    const exLen       = zipBuf.readUInt16LE(pos + 28);
    const fname       = zipBuf.slice(pos + 30, pos + 30 + fnLen).toString();
    const dataStart   = pos + 30 + fnLen + exLen;
    const base        = fname.split('/').pop().toUpperCase();

    if (base.startsWith(targetName.toUpperCase())) {
      const compressed = zipBuf.slice(dataStart, dataStart + compSize);
      const raw = compression === 8
        ? zlib.inflateRawSync(compressed)
        : compressed;
      const lines = raw.toString('utf8').split('\n');
      slog(`  ${base}: ${lines.length} lines`);
      return lines;
    }
    pos = dataStart + compSize;
  }
  return null;
}

// Parse TSV lines into header→value objects, yielding one at a time
function* tsvRows(lines) {
  if (!lines?.length) return;
  const hdrs = lines[0].split('\t').map(h => h.trim().toUpperCase());
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split('\t');
    const row  = {};
    hdrs.forEach((h, j) => { row[h] = (cols[j] || '').trim(); });
    yield row;
  }
}

async function syncQuarter(year, q, force = false) {
  const key = `${year}Q${q}`;
  if (!force && db.prepare('SELECT 1 FROM sync_log WHERE quarter=?').get(key)) {
    slog(`${key}: already synced, skipping`);
    return;
  }

  const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${year}q${q}_form345.zip`;
  slog(`${key}: downloading...`);

  const { status, body: zipBuf } = await get(url, 180000);
  if (status !== 200) throw new Error(`HTTP ${status}`);
  slog(`${key}: ${(zipBuf.length/1024/1024).toFixed(1)}MB`);

  // ── Step 1: build subMap from SUBMISSION only ─────────────
  slog(`${key}: loading submissions...`);
  const subLines = extractOne(zipBuf, 'SUBMISSION');
  const subMap   = {};
  for (const s of tsvRows(subLines)) {
    const acc = s.ACCESSION_NUMBER || '';
    if (!acc) continue;
    subMap[acc] = {
      ticker:  (s.ISSUERTRADINGSYMBOL || '').toUpperCase().trim(),
      company: (s.ISSUERNAME          || '').trim(),
      filed:    parseDate(s.FILEDATE || s.PERIOD_OF_REPORT || ''),
      period:   parseDate(s.PERIOD_OF_REPORT || s.FILEDATE || ''),
    };
  }
  // Free subLines
  subLines.length = 0;
  slog(`${key}: ${Object.keys(subMap).length} submissions loaded`);

  // ── Step 2: build ownerMap from REPORTINGOWNER only ───────
  slog(`${key}: loading owners...`);
  const ownerLines = extractOne(zipBuf, 'REPORTINGOWNER');
  const ownerMap   = {};
  for (const o of tsvRows(ownerLines)) {
    const acc = o.ACCESSION_NUMBER || '';
    if (!acc || ownerMap[acc]) continue;
    ownerMap[acc] = {
      name:  (o.RPTOWNERNAME || '').trim(),
      title: (o.OFFICERTITLE || o.RPTOWNERRELATIONSHIP || '').trim(),
    };
  }
  ownerLines.length = 0;
  slog(`${key}: ${Object.keys(ownerMap).length} owners loaded`);

  // ── Step 3: insert ND transactions in chunks of 1000 ──────
  let inserted = 0;
  const doInsert = db.transaction(batch => {
    let n = 0;
    for (const r of batch) { insertStmt.run(r); n++; }
    return n;
  });

  function processTransLines(lines, isDeriv) {
    let batch = [];
    let count = 0;
    for (const t of tsvRows(lines)) {
      const acc = t.ACCESSION_NUMBER || '';
      const sub = subMap[acc];
      if (!sub?.ticker) continue;
      const date  = parseDate(t.TRANS_DATE || '') || sub.period || sub.filed;
      if (!date) continue;
      const qty   = Math.round(Math.abs(parseFloat(t.TRANS_SHARES || t.UNDERLYING_SHARES || '0') || 0));
      const price = Math.abs(parseFloat(t.TRANS_PRICEPERSHARE || t.EXERCISE_PRICE || '0') || 0);
      batch.push([
        sub.ticker, sub.company,
        ownerMap[acc]?.name  || '',
        ownerMap[acc]?.title || '',
        date, sub.filed || date,
        (t.TRANS_CODE || '?').trim(),
        qty, +price.toFixed(4),
        Math.round(qty * price),
        Math.round(Math.abs(parseFloat(t.SHRSOWNFOLLOWINGTRANS || '0') || 0)),
        acc,
      ]);
      if (batch.length >= 1000) {
        count += doInsert(batch);
        batch = [];
      }
    }
    if (batch.length) count += doInsert(batch);
    return count;
  }

  slog(`${key}: processing ND transactions...`);
  const ndLines = extractOne(zipBuf, 'NONDERIV_TRANS');
  inserted += processTransLines(ndLines, false);
  ndLines.length = 0;

  slog(`${key}: processing derivative transactions...`);
  const dLines = extractOne(zipBuf, 'DERIV_TRANS');
  inserted += processTransLines(dLines, true);
  dLines.length = 0;

  db.prepare('INSERT OR REPLACE INTO sync_log (quarter, rows) VALUES (?,?)').run(key, inserted);
  slog(`${key}: done — ${inserted} rows inserted`);
}

function getQuarters(n) {
  const out = [];
  let yr = new Date().getFullYear();
  let q  = Math.ceil((new Date().getMonth() + 1) / 3);
  while (out.length < n) {
    if (--q < 1) { q = 4; yr--; }
    out.push({ year: yr, q });
  }
  return out;
}

async function runSync(numQ = 4, force = false) {
  if (syncState.running) { slog('already running'); return; }
  syncState.running = true;
  try {
    slog(`=== sync start (${numQ} quarters) ===`);
    for (const { year, q } of getQuarters(numQ))
      await syncQuarter(year, q, force);
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    slog(`=== sync done — ${n.toLocaleString()} total trades ===`);
  } catch(e) {
    slog(`=== sync ERROR: ${e.message} ===`);
  } finally {
    syncState.running = false;
  }
}

// ─── PRICE CACHE ──────────────────────────────────────────────
const pc = new Map();
function getPC(k)      { const c = pc.get(k); return c && Date.now() < c.e ? c.v : null; }
function setPC(k,v,ms) { pc.set(k, { v, e: Date.now()+ms }); }

// ─── ROUTES ───────────────────────────────────────────────────
app.get('/api/screener', (req, res) => {
  try {
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    if (n === 0 && syncState.running)
      return res.json({ building: true, message: 'Loading SEC data, check back in ~3 min...', trades: [] });

    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades
      WHERE trade_date >= date('now','-90 days')
      ORDER BY filing_date DESC, trade_date DESC
      LIMIT 500
    `).all();
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/ticker', (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  try {
    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades WHERE ticker=? ORDER BY trade_date DESC LIMIT 1000
    `).all(sym);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/insider', (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades WHERE UPPER(insider) LIKE UPPER(?) ORDER BY trade_date DESC LIMIT 500
    `).all(`%${name}%`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/price', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  const cached = getPC(sym);
  if (cached) return res.json(cached);
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${sym}&apikey=${FMP}`
    );
    if (status !== 200) return res.status(502).json({ error: `FMP HTTP ${status}` });
    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    if (!raw.length) return res.status(404).json({ error: `No price data for ${sym}` });
    const bars = raw.slice().reverse()
      .map(d => ({ time: d.date, open: +(+d.open).toFixed(2), high: +(+d.high).toFixed(2), low: +(+d.low).toFixed(2), close: +(+d.close).toFixed(2), volume: d.volume||0 }))
      .filter(d => d.close > 0 && d.time);
    setPC(sym, bars, 60*60*1000);
    res.json(bars);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status', (req, res) => {
  const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const synced = db.prepare('SELECT * FROM sync_log ORDER BY synced_at').all();
  res.json({ running: syncState.running, trades: n, quarters: synced, log: syncState.log.slice(-40) });
});

app.get('/api/sync',  (req, res) => {
  res.json({ started: !syncState.running });
  if (!syncState.running) runSync(parseInt(req.query.quarters||'4'), req.query.force==='1');
});

app.get('/api/diag', async (req, res) => {
  const n      = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const latest = db.prepare('SELECT MAX(trade_date) AS d FROM trades').get().d;
  const synced = db.prepare('SELECT * FROM sync_log').all();
  let price = {};
  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`);
    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data) ? data : (data?.historical||[]);
    price = { ok: status===200 && raw.length>0, bars: raw.length };
  } catch(e) { price = { ok:false, error: e.message }; }
  res.json({ db: { trades: n, latest, synced }, price, sync: { running: syncState.running, log: syncState.log.slice(-10) } });
});

app.get('/api/health', (req, res) =>
  res.json({ ok: true, trades: db.prepare('SELECT COUNT(*) AS n FROM trades').get().n }));

// ─── START ────────────────────────────────────────────────────
const existing = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
console.log(`DB: ${existing} existing trades at ${DB_PATH}`);

if (existing === 0) {
  console.log('Empty DB — starting sync...');
  runSync(4);
} else {
  // Re-sync only current quarter for new filings
  const { year, q } = getQuarters(1)[0];
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${year}Q${q}`);
  runSync(1);
}

// Daily re-sync at midnight
setInterval(() => {
  const { year, q } = getQuarters(1)[0];
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${year}Q${q}`);
  runSync(1);
}, 24 * 60 * 60 * 1000);

app.listen(PORT, () => console.log(`Server on port ${PORT}`));
