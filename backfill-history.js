#!/usr/bin/env node
/**
 * InsiderTape — Historical Backfill
 * ===================================
 * One-time script. Downloads SEC EDGAR Form 4 bulk ZIPs quarter by quarter.
 * Uses IDENTICAL URL + parse logic as sync-worker.js — no column name drift.
 * Safe to re-run: already-completed quarters are skipped via backfill_log.
 *
 * Usage:
 *   node backfill-history.js              # 2004–present
 *   node backfill-history.js 2015         # 2015–present
 *   node backfill-history.js 2015 2020    # specific range only
 *
 * Run AFTER market close. Full 2004–present run takes ~4-8 hours.
 */
'use strict';

const https    = require('https');
const zlib     = require('zlib');
const fs       = require('fs');
const path     = require('path');
const Database = require('better-sqlite3');

const DATA_DIR   = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
const DB_PATH    = process.env.DB_PATH || path.join(DATA_DIR, 'trades.db');
const START_YEAR = parseInt(process.argv[2]) || 2004;
const END_YEAR   = parseInt(process.argv[3]) || new Date().getFullYear();
const DELAY_MS   = 2500;

fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('cache_size = -32000');

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
  CREATE TABLE IF NOT EXISTS backfill_log (
    quarter TEXT PRIMARY KEY,
    completed_at TEXT DEFAULT (datetime('now')),
    rows_inserted INTEGER DEFAULT 0
  );
`);

const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO trades
    (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);
const doInsert = db.transaction(batch => { for (const r of batch) insertStmt.run(r); });

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function get(url, ms=180000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        return get(res.headers.location, ms).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function parseDate(s) {
  if (!s) return null;
  const mon = {JAN:'01',FEB:'02',MAR:'03',APR:'04',MAY:'05',JUN:'06',
               JUL:'07',AUG:'08',SEP:'09',OCT:'10',NOV:'11',DEC:'12'};
  const m = s.match(/^(\d{2})-([A-Z]{3})-(\d{4})$/i);
  let result = null;
  if (m) result = `${m[3]}-${mon[m[2].toUpperCase()]||'01'}-${m[1]}`;
  else if (/^\d{4}-\d{2}-\d{2}$/.test(s)) result = s.slice(0,10);
  if (!result) return null;
  const yr = parseInt(result.slice(0,4));
  if (yr < 2000 || yr > 2030) return null;
  return result;
}

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
      try {
        const raw   = compression===8 ? zlib.inflateRawSync(slice) : slice;
        const lines = raw.toString('utf8').split('\n');
        log(`  ${base}: ${lines.length} lines`);
        return lines;
      } catch(e) { log(`  extractOne(${targetPrefix}): ${e.message}`); return null; }
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
    const row = {};
    hdrs.forEach((h,j) => { row[h] = (cols[j]||'').trim(); });
    yield row;
  }
}

function processTransactions(zipBuf, prefix, subMap, ownerMap) {
  const lines = extractOne(zipBuf, prefix);
  if (!lines) { log(`  ${prefix}: not found`); return 0; }
  if (prefix.toUpperCase().startsWith('DERIV_TRANS')) {
    log(`  ${prefix}: skipping derivative transactions`);
    return 0;
  }
  let batch = [], count = 0;
  for (const t of tsvRows(lines)) {
    const acc = t.ACCESSION_NUMBER || '';
    const sub = subMap[acc];
    if (!sub?.ticker) continue;
    const date = parseDate(t.TRANS_DATE||'') || sub.period || sub.filed;
    if (!date) continue;
    const code = (t.TRANS_CODE||'').trim();
    if (!['P','S','S-'].includes(code)) continue;
    const qty   = Math.round(Math.abs(parseFloat(t.TRANS_SHARES||'0')||0));
    const price = Math.abs(parseFloat(t.TRANS_PRICEPERSHARE||'0')||0);
    if (qty > 50_000_000) continue;
    if (price > 1_500_000) continue;
    const value = Math.round(qty * price);
    if (value > 2_000_000_000) continue;
    batch.push([
      sub.ticker, sub.company,
      ownerMap[acc]?.name||'', ownerMap[acc]?.title||'',
      date, sub.filed||date,
      code, qty, +price.toFixed(4), value,
      Math.round(Math.abs(parseFloat(t.SHRSOWNFOLLOWINGTRANS||'0')||0)),
      acc,
    ]);
    if (batch.length >= 500) { doInsert(batch); count += batch.length; batch = []; }
  }
  if (batch.length) { doInsert(batch); count += batch.length; }
  lines.length = 0;
  return count;
}

async function processQuarter(year, qtr) {
  const label = `${year}Q${qtr}`;
  if (db.prepare('SELECT 1 FROM backfill_log WHERE quarter=?').get(label)) {
    log(`${label}: already done — skipping`); return 0;
  }
  if (db.prepare('SELECT 1 FROM sync_log WHERE quarter=?').get(label)) {
    log(`${label}: in sync_log — marking done`);
    db.prepare('INSERT OR IGNORE INTO backfill_log (quarter,rows_inserted) VALUES (?,0)').run(label);
    return 0;
  }

  const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${year}q${qtr}_form345.zip`;
  log(`${label}: ${url}`);

  let zipBuf;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const { status, body } = await get(url);
      if (status === 404) { log(`${label}: 404 — not published yet`); return 0; }
      if (status !== 200) {
        log(`${label}: HTTP ${status} attempt ${attempt}`);
        if (attempt < 3) { await sleep(attempt * 8000); continue; }
        return 0;
      }
      zipBuf = body;
      log(`${label}: ${(body.length/1024/1024).toFixed(1)} MB`);
      break;
    } catch(e) {
      log(`${label}: error attempt ${attempt}: ${e.message}`);
      if (attempt < 3) await sleep(attempt * 8000);
    }
  }
  if (!zipBuf) return 0;

  // Submissions
  const subLines = extractOne(zipBuf, 'SUBMISSION');
  if (!subLines) { log(`${label}: SUBMISSION.TSV missing — skip`); return 0; }
  const subMap = {};
  for (const s of tsvRows(subLines)) {
    const acc = s.ACCESSION_NUMBER||'';
    if (!acc) continue;
    const ticker = (s.ISSUERTRADINGSYMBOL||'').toUpperCase().replace(/[^A-Z0-9.]/g,'').trim();
    if (!ticker || ticker.length > 10) continue;
    subMap[acc] = {
      ticker,
      company: (s.ISSUERNAME||'').slice(0,120),
      filed:   parseDate(s.FILEDATE||s.PERIOD_OF_REPORT||''),
      period:  parseDate(s.PERIOD_OF_REPORT||s.FILEDATE||''),
    };
  }
  subLines.length = 0;
  log(`${label}: ${Object.keys(subMap).length} submissions`);

  // Owners
  const ownerLines = extractOne(zipBuf, 'REPORTINGOWNER');
  if (!ownerLines) { log(`${label}: REPORTINGOWNER.TSV missing — skip`); return 0; }
  const ownerMap = {};
  for (const o of tsvRows(ownerLines)) {
    const acc = o.ACCESSION_NUMBER||'';
    if (!acc || ownerMap[acc]) continue;
    ownerMap[acc] = {
      name:  (o.RPTOWNERNAME||'').trim(),
      title: (o.OFFICERTITLE||o.RPTOWNERRELATIONSHIP||'').trim().slice(0,80),
    };
  }
  ownerLines.length = 0;
  log(`${label}: ${Object.keys(ownerMap).length} owners`);

  const ndCount = processTransactions(zipBuf, 'NONDERIV_TRANS', subMap, ownerMap);
  log(`${label}: ${ndCount} rows inserted`);

  db.prepare('INSERT OR REPLACE INTO backfill_log (quarter,rows_inserted) VALUES (?,?)').run(label, ndCount);
  log(`${label}: ✓ done`);
  return ndCount;
}

function getQuarters(startYear, endYear) {
  const now = new Date();
  const curY = now.getFullYear(), curQ = Math.ceil((now.getMonth()+1)/3);
  const out = [];
  for (let y = startYear; y <= endYear; y++)
    for (let q = 1; q <= 4; q++) {
      if (y === curY && q > curQ) break;
      out.push({ year: y, qtr: q });
    }
  return out;
}

async function main() {
  const quarters = getQuarters(START_YEAR, END_YEAR);
  const existing = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  const done     = db.prepare('SELECT COUNT(*) AS n FROM backfill_log').get().n;

  log(`=== InsiderTape Historical Backfill ===`);
  log(`Range: ${START_YEAR}–${END_YEAR} · ${quarters.length} quarters to check`);
  log(`DB currently: ${existing.toLocaleString()} trades · ${done} quarters already logged`);
  log(`DB path: ${DB_PATH}`);
  log('');

  let grandTotal = 0, processed = 0, skipped = 0;
  for (const { year, qtr } of quarters) {
    try {
      const n = await processQuarter(year, qtr);
      if (n > 0) { grandTotal += n; processed++; }
      else skipped++;
    } catch(e) {
      log(`${year}Q${qtr}: ERROR — ${e.message}`);
      skipped++;
    }
    if ((processed + skipped) % 8 === 0 && processed + skipped > 0) {
      const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
      log(`--- ${processed} done, ${skipped} skipped · DB: ${n.toLocaleString()} ---`);
    }
    await sleep(DELAY_MS);
  }

  const final = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  log('');
  log(`=== Done ===`);
  log(`Quarters processed: ${processed} · Skipped: ${skipped}`);
  log(`New rows: ${grandTotal.toLocaleString()} · Total DB: ${final.toLocaleString()}`);
  db.close();
}

main().catch(e => { log(`FATAL: ${e.message}\n${e.stack}`); process.exit(1); });
