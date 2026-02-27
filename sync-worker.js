'use strict';

// sync-worker.js — runs as a child process, downloads + parses SEC ZIPs
// Called by server.js via: node --max-old-space-size=400 sync-worker.js
// Writes to the same SQLite DB, then exits cleanly.

const https    = require('https');
const zlib     = require('zlib');
const fs       = require('fs');
const path     = require('path');
const Database = require('better-sqlite3');

const DATA_DIR = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH = path.join(DATA_DIR, 'trades.db');

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('cache_size = -4000');

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

const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO trades
    (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`); }

function get(url, ms = 180000) {
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

function parseDate(s) {
  if (!s) return null;
  const mon = {JAN:'01',FEB:'02',MAR:'03',APR:'04',MAY:'05',JUN:'06',
               JUL:'07',AUG:'08',SEP:'09',OCT:'10',NOV:'11',DEC:'12'};
  const m = s.match(/^(\d{2})-([A-Z]{3})-(\d{4})$/i);
  if (m) return `${m[3]}-${mon[m[2].toUpperCase()]||'01'}-${m[1]}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.slice(0,10);
  return null;
}

// Extract ONE file from ZIP, return lines[]. Scans sequentially, never holds >1 file.
function extractOne(zipBuf, targetPrefix) {
  let pos = 0;
  while (pos < zipBuf.length - 4) {
    if (zipBuf[pos]!==0x50||zipBuf[pos+1]!==0x4B||zipBuf[pos+2]!==0x03||zipBuf[pos+3]!==0x04) { pos++; continue; }
    const compression = zipBuf.readUInt16LE(pos+8);
    const compSize    = zipBuf.readUInt32LE(pos+18);
    const fnLen       = zipBuf.readUInt16LE(pos+26);
    const exLen       = zipBuf.readUInt16LE(pos+28);
    const fname       = zipBuf.slice(pos+30, pos+30+fnLen).toString();
    const dataStart   = pos+30+fnLen+exLen;
    const base        = fname.split('/').pop().toUpperCase();
    if (base.startsWith(targetPrefix.toUpperCase())) {
      const slice = zipBuf.slice(dataStart, dataStart+compSize);
      const raw   = compression===8 ? zlib.inflateRawSync(slice) : slice;
      const lines = raw.toString('utf8').split('\n');
      log(`  ${base}: ${lines.length} lines`);
      return lines;
    }
    pos = dataStart + compSize;
  }
  return null;
}

function* tsvRows(lines) {
  if (!lines?.length) return;
  const hdrs = lines[0].split('\t').map(h => h.trim().toUpperCase());
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split('\t');
    const row  = {};
    hdrs.forEach((h,j) => { row[h] = (cols[j]||'').trim(); });
    yield row;
  }
}

const doInsert = db.transaction(batch => {
  for (const r of batch) insertStmt.run(r);
});

function processTransactions(zipBuf, prefix, subMap, ownerMap) {
  const lines = extractOne(zipBuf, prefix);
  if (!lines) { log(`  ${prefix}: not found`); return 0; }
  let batch = [], count = 0;
  for (const t of tsvRows(lines)) {
    const acc = t.ACCESSION_NUMBER || '';
    const sub = subMap[acc];
    if (!sub?.ticker) continue;
    const date  = parseDate(t.TRANS_DATE||'') || sub.period || sub.filed;
    if (!date) continue;
    const qty   = Math.round(Math.abs(parseFloat(t.TRANS_SHARES||t.UNDERLYING_SHARES||'0')||0));
    const price = Math.abs(parseFloat(t.TRANS_PRICEPERSHARE||t.EXERCISE_PRICE||'0')||0);
    batch.push([
      sub.ticker, sub.company,
      ownerMap[acc]?.name||'', ownerMap[acc]?.title||'',
      date, sub.filed||date,
      (t.TRANS_CODE||'?').trim(),
      qty, +price.toFixed(4), Math.round(qty*price),
      Math.round(Math.abs(parseFloat(t.SHRSOWNFOLLOWINGTRANS||'0')||0)),
      acc,
    ]);
    if (batch.length >= 500) { doInsert(batch); count += batch.length; batch = []; }
  }
  if (batch.length) { doInsert(batch); count += batch.length; }
  lines.length = 0; // free memory
  return count;
}

async function syncQuarter(year, q) {
  const key = `${year}Q${q}`;
  const already = db.prepare('SELECT 1 FROM sync_log WHERE quarter=?').get(key);
  if (already) { log(`${key}: already synced`); return; }

  const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${year}q${q}_form345.zip`;
  log(`${key}: downloading...`);
  const { status, body: zipBuf } = await get(url);
  if (status !== 200) { log(`${key}: HTTP ${status}, skipping`); return; }
  log(`${key}: ${(zipBuf.length/1024/1024).toFixed(1)}MB`);

  // Step 1: submissions (small, keep in memory as map)
  const subLines = extractOne(zipBuf, 'SUBMISSION');
  const subMap   = {};
  for (const s of tsvRows(subLines)) {
    const acc = s.ACCESSION_NUMBER||'';
    if (!acc) continue;
    subMap[acc] = {
      ticker:  (s.ISSUERTRADINGSYMBOL||'').toUpperCase().trim(),
      company: (s.ISSUERNAME||'').trim(),
      filed:    parseDate(s.FILEDATE||s.PERIOD_OF_REPORT||''),
      period:   parseDate(s.PERIOD_OF_REPORT||s.FILEDATE||''),
    };
  }
  subLines.length = 0;
  log(`${key}: ${Object.keys(subMap).length} submissions`);

  // Step 2: owners
  const ownerLines = extractOne(zipBuf, 'REPORTINGOWNER');
  const ownerMap   = {};
  for (const o of tsvRows(ownerLines)) {
    const acc = o.ACCESSION_NUMBER||'';
    if (!acc||ownerMap[acc]) continue;
    ownerMap[acc] = {
      name:  (o.RPTOWNERNAME||'').trim(),
      title: (o.OFFICERTITLE||o.RPTOWNERRELATIONSHIP||'').trim(),
    };
  }
  ownerLines.length = 0;
  log(`${key}: ${Object.keys(ownerMap).length} owners`);

  // Step 3: transactions — process and insert, then free
  const ndCount = processTransactions(zipBuf, 'NONDERIV_TRANS', subMap, ownerMap);
  log(`${key}: ${ndCount} ND rows inserted`);

  const dCount  = processTransactions(zipBuf, 'DERIV_TRANS', subMap, ownerMap);
  log(`${key}: ${dCount} D rows inserted`);

  db.prepare('INSERT OR REPLACE INTO sync_log (quarter,rows) VALUES (?,?)').run(key, ndCount+dCount);
  log(`${key}: complete`);
}

function getQuarters(n) {
  const out = [];
  let yr = new Date().getFullYear();
  let q  = Math.ceil((new Date().getMonth()+1)/3);
  while (out.length < n) {
    if (--q < 1) { q=4; yr--; }
    out.push({ year: yr, q });
  }
  return out;
}

(async () => {
  const numQ = parseInt(process.argv[2] || '4');
  log(`=== sync-worker start (${numQ} quarters) ===`);
  for (const { year, q } of getQuarters(numQ)) {
    await syncQuarter(year, q);
  }
  const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  log(`=== sync-worker done — ${n.toLocaleString()} trades ===`);
  db.close();
  process.exit(0);
})();
