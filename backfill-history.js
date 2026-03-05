#!/usr/bin/env node
/**
 * InsiderTape — Historical Backfill Script
 * =========================================
 * One-time script. Downloads all SEC EDGAR Form 4 bulk ZIPs from 2004 to present.
 * Uses the IDENTICAL URL + parse logic as sync-worker.js.
 * Inserts into the same trades table. Safe to re-run (skips completed quarters).
 *
 * Usage:
 *   node backfill-history.js              # 2004–present (~85 quarters)
 *   node backfill-history.js 2015         # 2015–present
 *   node backfill-history.js 2015 2020    # specific range only
 */
'use strict';
const https    = require('https');
const http     = require('http');
const path     = require('path');
const fs       = require('fs');
const zlib     = require('zlib');
const Database = require('better-sqlite3');

// ── Config ─────────────────────────────────────────────────────────────────
const DATA_DIR   = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
const DB_PATH    = process.env.DB_PATH || path.join(DATA_DIR, 'trades.db');
const START_YEAR = parseInt(process.argv[2]) || 2004;
const END_YEAR   = parseInt(process.argv[3]) || new Date().getFullYear();
const DELAY_MS   = 2000; // polite delay between quarters

// ── DB ─────────────────────────────────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous  = NORMAL');
db.pragma('cache_size   = -32000');

// Ensure tracking table exists
db.exec(`CREATE TABLE IF NOT EXISTS backfill_log (
  quarter TEXT PRIMARY KEY,
  completed_at TEXT DEFAULT (datetime('now')),
  rows_inserted INTEGER DEFAULT 0
)`);

// Also ensure sync_log exists for the skip-if-already-synced check
try { db.prepare('SELECT 1 FROM sync_log LIMIT 1').get(); }
catch(e) { db.exec(`CREATE TABLE IF NOT EXISTS sync_log (quarter TEXT PRIMARY KEY, synced_at TEXT, rows INTEGER)`); }

const upsert = db.prepare(`
  INSERT OR IGNORE INTO trades
    (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES
    (@ticker,@company,@insider,@title,@trade_date,@filing_date,@type,@qty,@price,@value,@owned,@accession)
`);
const batchInsert = db.transaction(rows => {
  let n = 0;
  for (const r of rows) { try { upsert.run(r); n++; } catch(e) {} }
  return n;
});

// ── Helpers ────────────────────────────────────────────────────────────────
function log(msg) { console.log(`[${new Date().toISOString().slice(11,19)}] ${msg}`); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function get(url, ms = 120000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 historical-backfill (admin@insidertape.com)' },
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

// ── ZIP extractor — identical logic to sync-worker.js ──────────────────────
// Scans local file headers sequentially, returns lines[] for first file matching prefix
function extractOne(zipBuf, targetPrefix) {
  let pos = 0;
  while (pos < zipBuf.length - 4) {
    if (zipBuf[pos]!==0x50||zipBuf[pos+1]!==0x4B||zipBuf[pos+2]!==0x03||zipBuf[pos+3]!==0x04) {
      pos++; continue;
    }
    const compression = zipBuf.readUInt16LE(pos + 8);
    const compSize    = zipBuf.readUInt32LE(pos + 18);
    const fnLen       = zipBuf.readUInt16LE(pos + 26);
    const exLen       = zipBuf.readUInt16LE(pos + 28);
    const fname       = zipBuf.slice(pos + 30, pos + 30 + fnLen).toString();
    const dataStart   = pos + 30 + fnLen + exLen;

    if (fname.toUpperCase().startsWith(targetPrefix.toUpperCase())) {
      const slice = zipBuf.slice(dataStart, dataStart + compSize);
      try {
        const raw = compression === 0 ? slice : zlib.inflateRawSync(slice);
        return raw.toString('utf8').split('\n');
      } catch(e) {
        log(`extractOne(${targetPrefix}): decompress failed — ${e.message}`);
        return [];
      }
    }
    pos = dataStart + compSize;
  }
  return [];
}

// ── TSV row parser ─────────────────────────────────────────────────────────
function parseLines(lines) {
  if (!lines.length) return [];
  const headers = lines[0].split('\t').map(h => h.trim().toLowerCase());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split('\t');
    if (cols.length < 2) continue;
    const row = {};
    headers.forEach((h, j) => { row[h] = (cols[j] || '').trim(); });
    rows.push(row);
  }
  return rows;
}

// ── Process one quarter ────────────────────────────────────────────────────
async function processQuarter(year, qtr) {
  const label = `${year}Q${qtr}`;

  if (db.prepare('SELECT 1 FROM backfill_log WHERE quarter=?').get(label)) {
    log(`${label}: already done — skipping`);
    return 0;
  }
  // Skip if sync_log already has it (the live worker synced it)
  if (db.prepare('SELECT 1 FROM sync_log WHERE quarter=?').get(label)) {
    log(`${label}: in sync_log — marking done, skipping`);
    db.prepare('INSERT OR IGNORE INTO backfill_log (quarter,rows_inserted) VALUES (?,0)').run(label);
    return 0;
  }

  // EXACT URL from sync-worker.js
  const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${year}q${qtr}_form345.zip`;
  log(`${label}: downloading...`);

  let zipBuf;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const { status, body } = await get(url, 120000);
      if (status === 404) { log(`${label}: 404 — not yet available`); return 0; }
      if (status !== 200) {
        log(`${label}: HTTP ${status} (attempt ${attempt})`);
        if (attempt < 3) { await sleep(attempt * 6000); continue; }
        return 0;
      }
      zipBuf = body;
      log(`${label}: ${(body.length/1024/1024).toFixed(1)}MB`);
      break;
    } catch(e) {
      log(`${label}: fetch error (attempt ${attempt}): ${e.message}`);
      if (attempt < 3) await sleep(attempt * 6000);
    }
  }
  if (!zipBuf) return 0;

  // Extract files — same calls as sync-worker.js
  const subLines   = extractOne(zipBuf, 'SUBMISSION');
  const ownerLines = extractOne(zipBuf, 'REPORTINGOWNER');
  log(`${label}:   SUBMISSION.TSV: ${subLines.length} lines`);
  log(`${label}:   REPORTINGOWNER.TSV: ${ownerLines.length} lines`);

  if (subLines.length < 2 || ownerLines.length < 2) {
    log(`${label}: missing required files — skipping`);
    return 0;
  }

  // Build submission map: accession → { company, filing_date }
  const subRows = parseLines(subLines);
  const subMap  = {};
  for (const s of subRows) {
    const ft  = (s.form_type || s.formtype || '').replace(/\s/g,'');
    if (ft !== '4' && ft !== '4/A') continue;
    const acc = (s.accession_number || s.accessionnumber || '').replace(/-/g,'');
    if (!acc) continue;
    subMap[acc] = {
      company:     (s.company_name || s.companyname || '').slice(0,120),
      filing_date: (s.date_filed   || s.datefiled   || '').slice(0,10),
    };
  }

  // Build owner map: accession → { insider, title }
  const ownerRows = parseLines(ownerLines);
  const ownerMap  = {};
  for (const o of ownerRows) {
    const acc  = (o.accession_number || o.accessionnumber || '').replace(/-/g,'');
    const name = (o.reporting_owner_name || o.reportingownername || '').trim();
    const title= (o.reporting_owner_title || o.reportingownertitle || o.officer_title || o.officertitle || '').trim().slice(0,80);
    if (acc && name) ownerMap[acc] = { insider: name, title };
  }

  let totalInserted = 0;
  const BATCH = 2000;
  let batch = [];
  function flush() { if (batch.length) { totalInserted += batchInsert(batch); batch = []; } }

  // ── Non-derivative transactions ──────────────────────────────────────────
  const ndLines = extractOne(zipBuf, 'NONDERIV_TRANS');
  log(`${label}:   NONDERIV_TRANS.TSV: ${ndLines.length} lines`);
  const ndRows  = parseLines(ndLines);

  for (const r of ndRows) {
    const acc = (r.accession_number || r.accessionnumber || '').replace(/-/g,'');
    const sub = subMap[acc];
    const own = ownerMap[acc];
    if (!sub || !own) continue;

    const ticker = (r.issuer_trading_symbol || r.issuertradingsymbol || '').toUpperCase().replace(/[^A-Z0-9.]/g,'').trim();
    if (!ticker || ticker.length > 10) continue;

    const tradeDate = (r.transaction_date || r.transactiondate || '').slice(0,10);
    if (!tradeDate || tradeDate < '2000-01-01') continue;

    const code = (r.transaction_code || r.transactioncode || '').toUpperCase().trim();
    const type = code==='P' ? 'P' : code==='S' ? 'S' : code==='S-' ? 'S-' : null;
    if (!type) continue;

    const qty   = parseFloat(r.transaction_shares        || r.transactionshares        || '0') || 0;
    const price = parseFloat(r.transaction_pricepershare || r.transactionpricepershare || '0') || 0;
    const owned = parseFloat(r.shares_owned_following_transaction || r.sharesownedfollowing || '0') || 0;
    const accDash = acc.length===18 ? `${acc.slice(0,10)}-${acc.slice(10,12)}-${acc.slice(12)}` : acc;

    batch.push({ ticker, company: sub.company, insider: own.insider, title: own.title,
      trade_date: tradeDate, filing_date: sub.filing_date, type,
      qty: Math.round(qty), price, value: Math.round(qty*price), owned: Math.round(owned),
      accession: accDash });
    if (batch.length >= BATCH) flush();
  }
  flush();

  // ── Derivative transactions ──────────────────────────────────────────────
  const dLines = extractOne(zipBuf, 'DERIV_TRANS');
  log(`${label}:   DERIV_TRANS.TSV: ${dLines.length} lines`);
  const dRows  = parseLines(dLines);

  for (const r of dRows) {
    const acc = (r.accession_number || r.accessionnumber || '').replace(/-/g,'');
    const sub = subMap[acc];
    const own = ownerMap[acc];
    if (!sub || !own) continue;

    const ticker = (r.issuer_trading_symbol || r.issuertradingsymbol || '').toUpperCase().replace(/[^A-Z0-9.]/g,'').trim();
    if (!ticker || ticker.length > 10) continue;

    const tradeDate = (r.transaction_date || r.transactiondate || '').slice(0,10);
    if (!tradeDate || tradeDate < '2000-01-01') continue;

    const code = (r.transaction_code || r.transactioncode || '').toUpperCase().trim();
    const type = code==='A'?'O-A':code==='M'?'O-M':code==='D'?'O-D':null;
    if (!type) continue;

    const qty   = parseFloat(r.transaction_shares || r.number_of_derivative_securities_acquired || '0') || 0;
    const price = parseFloat(r.exercise_price     || r.transactionpricepershare || '0') || 0;
    const accDash = acc.length===18 ? `${acc.slice(0,10)}-${acc.slice(10,12)}-${acc.slice(12)}` : acc;

    batch.push({ ticker, company: sub.company, insider: own.insider, title: own.title,
      trade_date: tradeDate, filing_date: sub.filing_date, type,
      qty: Math.round(qty), price, value: Math.round(qty*price), owned: 0,
      accession: accDash });
    if (batch.length >= BATCH) flush();
  }
  flush();

  db.prepare('INSERT OR REPLACE INTO backfill_log (quarter,rows_inserted) VALUES (?,?)').run(label, totalInserted);
  log(`${label}: ✓ ${totalInserted.toLocaleString()} rows inserted`);
  return totalInserted;
}

// ── Build quarter list ─────────────────────────────────────────────────────
function getQuarters(startYear, endYear) {
  const now = new Date();
  const curY = now.getFullYear(), curQ = Math.ceil((now.getMonth()+1)/3);
  const quarters = [];
  for (let y = startYear; y <= endYear; y++)
    for (let q = 1; q <= 4; q++) {
      if (y === curY && q > curQ) break;
      quarters.push({ year: y, qtr: q });
    }
  return quarters;
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  const quarters = getQuarters(START_YEAR, END_YEAR);
  const existing = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;

  log(`=== InsiderTape Historical Backfill ===`);
  log(`Range: ${START_YEAR}–${END_YEAR} (${quarters.length} quarters)`);
  log(`DB currently: ${existing.toLocaleString()} trades`);
  log(`DB path: ${DB_PATH}`);
  log('');

  let grandTotal = 0, processed = 0, skipped = 0;

  for (const { year, qtr } of quarters) {
    try {
      const n = await processQuarter(year, qtr);
      if (n > 0) { grandTotal += n; processed++; }
      else skipped++;
    } catch(e) { log(`${year}Q${qtr}: ERROR — ${e.message}`); skipped++; }

    if ((processed + skipped) % 8 === 0 && (processed + skipped) > 0) {
      const total = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
      log(`--- Progress: ${processed} done, ${skipped} skipped, DB: ${total.toLocaleString()} rows ---`);
    }
    await sleep(DELAY_MS);
  }

  const finalTotal = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  log('');
  log(`=== Done: ${processed} quarters processed, ${grandTotal.toLocaleString()} new rows ===`);
  log(`Total DB rows: ${finalTotal.toLocaleString()}`);
  db.close();
}

main().catch(e => { log(`FATAL: ${e.message}`); process.exit(1); });
