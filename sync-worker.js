'use strict';

// sync-worker.js v2 — Turso edition
// Downloads SEC structured insider-trade ZIP files (quarterly) and bulk-inserts
// records into Turso. Runs as a one-shot GitHub Actions job.
// Usage: node sync-worker.js [numQuarters]

const https  = require('https');
const zlib   = require('zlib');
const { createClient } = require('@libsql/client');

const TURSO_URL   = process.env.TURSO_DATABASE_URL;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;
if (!TURSO_URL) { console.error('TURSO_DATABASE_URL not set'); process.exit(1); }

const client = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN || undefined });

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11, 19)}] ${msg}\n`); }

async function dbQuery(sql, args = []) {
  const r = await client.execute({ sql, args });
  return r.rows.map(row => Object.fromEntries(r.columns.map((c, i) => [c, row[i] ?? null])));
}
async function dbRun(sql, args = []) {
  const r = await client.execute({ sql, args });
  return r.rowsAffected;
}
async function dbBatch(stmts) {
  return client.batch(stmts, 'write');
}

async function initSchema() {
  const stmts = [
    `CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
      trade_date TEXT NOT NULL, filing_date TEXT,
      type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER,
      accession TEXT, footnote TEXT,
      UNIQUE(accession, insider, trade_date, type, qty)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_ticker     ON trades(ticker)`,
    `CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_insider    ON trades(insider)`,
    `CREATE TABLE IF NOT EXISTS sync_log (quarter TEXT PRIMARY KEY, synced_at TEXT DEFAULT (datetime('now')), rows INTEGER)`,
  ];
  for (const sql of stmts) try { await client.execute(sql); } catch(_) {}
}

function get(url, ms = 180000, _hops = 0) {
  if (_hops > 5) return Promise.reject(new Error('Too many redirects'));
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'User-Agent': 'InsiderTape/2.0 admin@insidertape.com' }, timeout: ms }, res => {
      if ([301, 302, 303].includes(res.statusCode) && res.headers.location) {
        res.resume();
        return get(res.headers.location, ms, _hops + 1).then(resolve).catch(reject);
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

function parseDate(s) {
  if (!s) return null;
  const mon = { JAN: '01', FEB: '02', MAR: '03', APR: '04', MAY: '05', JUN: '06', JUL: '07', AUG: '08', SEP: '09', OCT: '10', NOV: '11', DEC: '12' };
  let result = null;
  const m = s.match(/^(\d{2})-([A-Z]{3})-(\d{4})$/i);
  if (m) result = `${m[3]}-${mon[m[2].toUpperCase()] || '01'}-${m[1]}`;
  else if (/^\d{4}-\d{2}-\d{2}$/.test(s)) result = s.slice(0, 10);
  if (!result) return null;
  const yr = parseInt(result.slice(0, 4));
  if (yr < 2000 || yr > 2027) return null;
  return result;
}

function extractOne(zipBuf, targetPrefix) {
  let pos = 0;
  while (pos < zipBuf.length - 4) {
    if (zipBuf[pos] !== 0x50 || zipBuf[pos+1] !== 0x4B || zipBuf[pos+2] !== 0x03 || zipBuf[pos+3] !== 0x04) { pos++; continue; }
    const compression = zipBuf.readUInt16LE(pos + 8);
    const compSize    = zipBuf.readUInt32LE(pos + 18);
    const fnLen       = zipBuf.readUInt16LE(pos + 26);
    const exLen       = zipBuf.readUInt16LE(pos + 28);
    const fname       = zipBuf.slice(pos + 30, pos + 30 + fnLen).toString();
    const dataStart   = pos + 30 + fnLen + exLen;
    const base        = fname.split('/').pop().toUpperCase();
    if (base.startsWith(targetPrefix.toUpperCase())) {
      const slice = zipBuf.slice(dataStart, dataStart + compSize);
      const raw   = compression === 8 ? zlib.inflateRawSync(slice) : slice;
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
    const cols = lines[i].split('\t'), row = {};
    hdrs.forEach((h, j) => { row[h] = (cols[j] || '').trim(); });
    yield row;
  }
}

const INSERT_SQL = `INSERT OR IGNORE INTO trades (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession,footnote) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`;

async function insertBatch(rows) {
  if (!rows.length) return 0;
  let inserted = 0;
  const CHUNK = 100;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const stmts = rows.slice(i, i + CHUNK).map(r => ({ sql: INSERT_SQL, args: r }));
    const results = await dbBatch(stmts);
    for (const r of results) inserted += r.rowsAffected || 0;
    if (i % 1000 === 0 && i > 0) { log(`    ...${i} rows processed`); }
  }
  return inserted;
}

async function processTransactions(zipBuf, prefix, subMap, ownerMap) {
  const lines = extractOne(zipBuf, prefix);
  if (!lines) { log(`  ${prefix}: not found`); return 0; }
  if (prefix.toUpperCase().startsWith('DERIV_TRANS')) { log(`  ${prefix}: skipping derivatives`); return 0; }

  const batch = [];
  for (const t of tsvRows(lines)) {
    const acc = t.ACCESSION_NUMBER || '';
    const sub = subMap[acc];
    if (!sub?.ticker) continue;
    const date = parseDate(t.TRANS_DATE || '') || sub.period || sub.filed;
    if (!date) continue;
    const code = (t.TRANS_CODE || '').trim();
    if (!['P', 'S', 'S-'].includes(code)) continue;
    const qty   = Math.round(Math.abs(parseFloat(t.TRANS_SHARES || '0') || 0));
    const price = Math.abs(parseFloat(t.TRANS_PRICEPERSHARE || '0') || 0);
    if (qty > 50_000_000 || price > 1_500_000) continue;
    const value = Math.round(qty * price);
    if (value > 2_000_000_000) continue;
    batch.push([
      sub.ticker, sub.company,
      ownerMap[acc]?.name || '', ownerMap[acc]?.title || '',
      date, sub.filed || date,
      code, qty, +price.toFixed(4), value,
      Math.round(Math.abs(parseFloat(t.SHRSOWNFOLLOWINGTRANS || '0') || 0)),
      acc, null,
    ]);
  }
  lines.length = 0;

  const inserted = await insertBatch(batch);
  log(`  ${prefix}: ${inserted} rows inserted`);
  return inserted;
}

async function syncQuarter(year, q) {
  const key = `${year}Q${q}`;
  const already = await dbQuery('SELECT 1 AS n FROM sync_log WHERE quarter = ?', [key]);
  if (already.length) { log(`${key}: already synced`); return; }

  const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${year}q${q}_form345.zip`;
  log(`${key}: downloading ${url}`);
  const { status, body: zipBuf } = await get(url);
  if (status !== 200) { log(`${key}: HTTP ${status}, skipping`); return; }
  log(`${key}: ${(zipBuf.length / 1024 / 1024).toFixed(1)}MB downloaded`);

  // Build submission map
  const subLines = extractOne(zipBuf, 'SUBMISSION');
  const subMap = {};
  for (const s of tsvRows(subLines)) {
    const acc = s.ACCESSION_NUMBER || '';
    if (!acc) continue;
    subMap[acc] = {
      ticker:  (s.ISSUERTRADINGSYMBOL || '').toUpperCase().trim(),
      company: (s.ISSUERNAME || '').trim(),
      filed:   parseDate(s.FILEDATE || s.PERIOD_OF_REPORT || ''),
      period:  parseDate(s.PERIOD_OF_REPORT || s.FILEDATE || ''),
    };
  }
  subLines.length = 0;
  log(`${key}: ${Object.keys(subMap).length} submissions`);

  // Build owner map
  const ownerLines = extractOne(zipBuf, 'REPORTINGOWNER');
  const ownerMap = {};
  for (const o of tsvRows(ownerLines)) {
    const acc = o.ACCESSION_NUMBER || '';
    if (!acc || ownerMap[acc]) continue;
    ownerMap[acc] = { name: (o.RPTOWNERNAME || '').trim(), title: (o.OFFICERTITLE || o.RPTOWNERRELATIONSHIP || '').trim() };
  }
  ownerLines.length = 0;
  log(`${key}: ${Object.keys(ownerMap).length} owners`);

  // Process non-derivative transactions
  const ndCount = await processTransactions(zipBuf, 'NONDERIV_TRANS', subMap, ownerMap);
  // Derivative transactions are skipped (misleading values)
  await processTransactions(zipBuf, 'DERIV_TRANS', subMap, ownerMap);

  await dbRun('INSERT OR REPLACE INTO sync_log (quarter, rows) VALUES (?, ?)', [key, ndCount]);
  log(`${key}: complete (${ndCount} rows)`);
}

function getQuarters(n) {
  const out = [];
  let yr = new Date().getFullYear(), q = Math.ceil((new Date().getMonth() + 1) / 3);
  while (out.length < n) {
    if (--q < 1) { q = 4; yr--; }
    out.push({ year: yr, q });
  }
  return out;
}

(async () => {
  const numQ = parseInt(process.argv[2] || '4');
  log(`=== sync-worker v2 (Turso) start — ${numQ} quarters ===`);
  await initSchema();
  for (const { year, q } of getQuarters(numQ)) {
    await syncQuarter(year, q);
  }
  const count = await dbQuery('SELECT COUNT(*) AS n FROM trades');
  log(`=== sync-worker done — ${(count[0]?.n || 0).toLocaleString()} trades ===`);
  process.exit(0);
})();
