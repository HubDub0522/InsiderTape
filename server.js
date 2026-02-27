'use strict';

// ─────────────────────────────────────────────────────────────
//  INSIDERTAPE — Server (SQLite edition)
//
//  SQLite database stored at /var/data/trades.db
//  On Render: attach a Persistent Disk, mount path = /var/data
//
//  Trade data: SEC Insider Transactions Data Sets (quarterly ZIPs)
//  Price data: FMP EOD historical
// ─────────────────────────────────────────────────────────────

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

// Use /var/data if it exists (Render persistent disk), else local ./data
const DATA_DIR = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH = path.join(DATA_DIR, 'trades.db');

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── DATABASE SETUP ───────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT    NOT NULL,
    company     TEXT,
    insider     TEXT,
    title       TEXT,
    trade_date  TEXT    NOT NULL,
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

console.log(`SQLite at ${DB_PATH}`);

// ─── HTTP HELPER ──────────────────────────────────────────────
function get(url, timeoutMs = 90000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: timeoutMs,
    }, res => {
      if ([301,302,303].includes(res.statusCode) && res.headers.location)
        return get(res.headers.location, timeoutMs).then(resolve).catch(reject);
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout: ' + url.slice(0,60))); });
  });
}

// ─── ZIP EXTRACTOR ────────────────────────────────────────────
function extractZip(buf, wanted) {
  const out = {};
  let pos = 0;
  while (pos < buf.length - 4) {
    if (buf[pos] !== 0x50 || buf[pos+1] !== 0x4B ||
        buf[pos+2] !== 0x03 || buf[pos+3] !== 0x04) { pos++; continue; }
    const compression = buf.readUInt16LE(pos + 8);
    const compSize    = buf.readUInt32LE(pos + 18);
    const fnLen       = buf.readUInt16LE(pos + 26);
    const exLen       = buf.readUInt16LE(pos + 28);
    const fname       = buf.slice(pos + 30, pos + 30 + fnLen).toString();
    const dataStart   = pos + 30 + fnLen + exLen;
    const base        = fname.split('/').pop().toUpperCase();
    if (wanted.some(w => base.startsWith(w))) {
      try {
        const raw = compression === 8
          ? zlib.inflateRawSync(buf.slice(dataStart, dataStart + compSize))
          : buf.slice(dataStart, dataStart + compSize);
        out[base] = raw.toString('utf8').split('\n');
        syncLog(`  Extracted ${base} (${out[base].length} lines)`);
      } catch(e) { syncLog(`  Failed to extract ${base}: ${e.message}`); }
    }
    pos = dataStart + compSize;
  }
  return out;
}

// ─── TSV PARSER ───────────────────────────────────────────────
function parseTSV(lines) {
  if (!lines?.length) return [];
  const hdrs = lines[0].split('\t').map(h => h.trim().toUpperCase());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split('\t');
    const row  = {};
    hdrs.forEach((h, j) => { row[h] = (cols[j] || '').trim(); });
    rows.push(row);
  }
  return rows;
}

function parseDate(s) {
  if (!s) return null;
  // DD-MON-YYYY
  const mon = {JAN:'01',FEB:'02',MAR:'03',APR:'04',MAY:'05',JUN:'06',
                JUL:'07',AUG:'08',SEP:'09',OCT:'10',NOV:'11',DEC:'12'};
  const m = s.match(/^(\d{2})-([A-Z]{3})-(\d{4})$/i);
  if (m) return `${m[3]}-${mon[m[2].toUpperCase()]||'01'}-${m[1]}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.slice(0,10);
  return null;
}

// ─── SYNC STATE ───────────────────────────────────────────────
const syncState = { running: false, log: [] };

function syncLog(msg) {
  const line = `[${new Date().toISOString().slice(11,19)}] ${msg}`;
  console.log(line);
  syncState.log.push(line);
  if (syncState.log.length > 300) syncState.log.splice(0, 100);
}

// ─── SYNC ONE QUARTER ─────────────────────────────────────────
async function syncQuarter(year, q, force = false) {
  const key = `${year}Q${q}`;
  if (!force) {
    const row = db.prepare('SELECT 1 FROM sync_log WHERE quarter=?').get(key);
    if (row) { syncLog(`${key}: already done, skipping`); return 0; }
  }

  const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${year}q${q}_form345.zip`;
  syncLog(`${key}: downloading ${url}`);

  const { status, body } = await get(url, 180000);
  if (status !== 200) throw new Error(`HTTP ${status} downloading ${key}`);
  syncLog(`${key}: ${(body.length/1024/1024).toFixed(1)}MB received`);

  const files = extractZip(body, ['SUBMISSION', 'REPORTINGOWNER', 'NONDERIV_TRANS', 'DERIV_TRANS']);

  const subs   = parseTSV(files['SUBMISSION.TSV']     || files['SUBMISSION.TXT']     || []);
  const owners = parseTSV(files['REPORTINGOWNER.TSV'] || files['REPORTINGOWNER.TXT'] || []);
  const ndT    = parseTSV(files['NONDERIV_TRANS.TSV'] || files['NONDERIV_TRANS.TXT'] || []);
  const dT     = parseTSV(files['DERIV_TRANS.TSV']    || files['DERIV_TRANS.TXT']    || []);

  syncLog(`${key}: ${subs.length} subs | ${owners.length} owners | ${ndT.length} ND | ${dT.length} D`);

  // Build maps
  const subMap = {};
  for (const s of subs) {
    const acc = s.ACCESSION_NUMBER || '';
    if (!acc) continue;
    subMap[acc] = {
      ticker:  (s.ISSUERTRADINGSYMBOL || '').toUpperCase().trim(),
      company: (s.ISSUERNAME || '').trim(),
      filed:    parseDate(s.FILEDATE || s.PERIOD_OF_REPORT || ''),
      period:   parseDate(s.PERIOD_OF_REPORT || s.FILEDATE || ''),
    };
  }

  const ownerMap = {};
  for (const o of owners) {
    const acc = o.ACCESSION_NUMBER || '';
    if (!acc || ownerMap[acc]) continue;
    ownerMap[acc] = {
      name:  (o.RPTOWNERNAME || '').trim(),
      title: (o.OFFICERTITLE || o.RPTOWNERRELATIONSHIP || '').trim(),
    };
  }

  // Insert in one transaction — much faster
  const insert = db.prepare(`
    INSERT OR IGNORE INTO trades
      (ticker, company, insider, title, trade_date, filing_date, type, qty, price, value, owned, accession)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const insertMany = db.transaction(rows => {
    let count = 0;
    for (const r of rows) {
      const info = insert.run(r);
      count += info.changes;
    }
    return count;
  });

  const rows = [];
  function addRow(t) {
    const acc = t.ACCESSION_NUMBER || '';
    const sub = subMap[acc];
    if (!sub?.ticker) return;
    const date  = parseDate(t.TRANS_DATE || '') || sub.period || sub.filed;
    if (!date) return;
    const qty   = Math.round(Math.abs(parseFloat(t.TRANS_SHARES || t.UNDERLYING_SHARES || '0') || 0));
    const price = Math.abs(parseFloat(t.TRANS_PRICEPERSHARE || t.EXERCISE_PRICE || '0') || 0);
    const code  = (t.TRANS_CODE || '?').trim();
    const owned = Math.round(Math.abs(parseFloat(t.SHRSOWNFOLLOWINGTRANS || '0') || 0));
    rows.push([
      sub.ticker, sub.company,
      ownerMap[acc]?.name  || '',
      ownerMap[acc]?.title || '',
      date, sub.filed || date,
      code, qty, +price.toFixed(4),
      Math.round(qty * price),
      owned, acc,
    ]);
  }

  for (const t of ndT) addRow(t);
  for (const t of dT)  addRow(t);

  const inserted = insertMany(rows);
  db.prepare('INSERT OR REPLACE INTO sync_log (quarter, rows) VALUES (?,?)').run(key, inserted);
  syncLog(`${key}: inserted ${inserted} new rows`);
  return inserted;
}

// ─── FULL SYNC ────────────────────────────────────────────────
function getQuarters(n) {
  const out = [];
  let year = new Date().getFullYear();
  let q    = Math.ceil((new Date().getMonth() + 1) / 3);
  while (out.length < n) {
    q--; if (q < 1) { q = 4; year--; }
    out.push({ year, q });
  }
  return out;
}

async function runSync(numQuarters = 4, force = false) {
  if (syncState.running) return;
  syncState.running = true;
  try {
    syncLog(`=== Sync started (${numQuarters} quarters) ===`);
    for (const { year, q } of getQuarters(numQuarters)) {
      await syncQuarter(year, q, force);
    }
    const total = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    syncLog(`=== Sync complete — ${total.toLocaleString()} trades in DB ===`);
  } catch(e) {
    syncLog(`=== Sync ERROR: ${e.message} ===`);
  } finally {
    syncState.running = false;
  }
}

// ─── PRICE CACHE ──────────────────────────────────────────────
const priceCache = new Map();
function getPC(k)      { const c = priceCache.get(k); return c && Date.now() < c.e ? c.v : null; }
function setPC(k,v,ms) { priceCache.set(k, { v, e: Date.now()+ms }); }

// ─── ROUTES ───────────────────────────────────────────────────

app.get('/api/screener', (req, res) => {
  try {
    const count = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    if (syncState.running && count === 0)
      return res.json({ building: true, message: 'Loading SEC data, please wait...', trades: [] });

    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades
      WHERE trade_date >= date('now', '-90 days')
      ORDER BY filing_date DESC, trade_date DESC
      LIMIT 500
    `).all();
    res.json(rows);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/ticker', (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  try {
    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades
      WHERE ticker = ?
      ORDER BY trade_date DESC
      LIMIT 1000
    `).all(symbol);
    res.json(rows);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/insider', (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    const rows = db.prepare(`
      SELECT ticker, company, insider, title,
             trade_date AS trade, filing_date AS filing,
             type, qty, price, value, owned
      FROM trades
      WHERE UPPER(insider) LIKE UPPER(?)
      ORDER BY trade_date DESC
      LIMIT 500
    `).all(`%${name}%`);
    res.json(rows);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/price', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  const cached = getPC(symbol);
  if (cached) return res.json(cached);
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${symbol}&apikey=${FMP}`
    );
    if (status !== 200) return res.status(502).json({ error: `FMP HTTP ${status}` });
    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    if (!raw.length) return res.status(404).json({ error: `No price data for ${symbol}` });
    const bars = raw.slice().reverse()
      .map(d => ({ time: d.date, open: +(+d.open).toFixed(2), high: +(+d.high).toFixed(2), low: +(+d.low).toFixed(2), close: +(+d.close).toFixed(2), volume: d.volume||0 }))
      .filter(d => d.close > 0 && d.time);
    setPC(symbol, bars, 60*60*1000);
    res.json(bars);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/status', (req, res) => {
  const count = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const synced = db.prepare('SELECT * FROM sync_log ORDER BY synced_at').all();
  res.json({ running: syncState.running, trades: count, quarters: synced, log: syncState.log.slice(-30) });
});

app.get('/api/sync', (req, res) => {
  const force = req.query.force === '1';
  const quarters = parseInt(req.query.quarters || '4');
  res.json({ started: !syncState.running, force });
  if (!syncState.running) runSync(quarters, force);
});

app.get('/api/diag', async (req, res) => {
  const count = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const latest = db.prepare('SELECT MAX(trade_date) AS d FROM trades').get().d;
  const synced = db.prepare('SELECT * FROM sync_log').all();
  let price = {};
  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`);
    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    price = { ok: status===200 && raw.length>0, bars: raw.length };
  } catch(e) { price = { ok:false, error: e.message }; }
  res.json({ db: { trades: count, latest, synced }, price, sync: { running: syncState.running, log: syncState.log.slice(-10) } });
});

app.get('/api/health', (req, res) =>
  res.json({ ok: true, trades: db.prepare('SELECT COUNT(*) AS n FROM trades').get().n }));

// ─── STARTUP ──────────────────────────────────────────────────
const count = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
console.log(`DB has ${count} trades`);

if (count === 0) {
  console.log('Empty DB — starting initial sync (last 4 quarters)...');
  runSync(4);
} else {
  // Re-sync current quarter only for new filings
  console.log('DB populated — syncing current quarter for new filings...');
  const q = getQuarters(1)[0];
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${q.year}Q${q.q}`);
  runSync(1);
}

// Daily re-sync of current quarter
setInterval(() => {
  const q = getQuarters(1)[0];
  db.prepare('DELETE FROM sync_log WHERE quarter=?').run(`${q.year}Q${q.q}`);
  runSync(1);
}, 24 * 60 * 60 * 1000);

app.listen(PORT, () => console.log(`InsiderTape on port ${PORT}`));
