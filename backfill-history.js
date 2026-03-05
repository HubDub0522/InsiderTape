#!/usr/bin/env node
/**
 * InsiderTape — Historical Backfill Script
 * =========================================
 * One-time script. Downloads ALL SEC Form 4 insider trading data from 2004 to present.
 * Inserts into the same `trades` table used by the live site.
 * Safe to run multiple times — uses INSERT OR IGNORE, skips duplicates.
 * 
 * Usage:
 *   node backfill-history.js              # 2004–present (~80+ quarters)
 *   node backfill-history.js 2015         # 2015–present
 *   node backfill-history.js 2015 2020    # 2015–2020 only
 *
 * Estimated time: ~20-40 minutes for full history on Render free tier.
 * Estimated DB size: ~5-8M additional rows (currently have ~1M for last 3 years).
 *
 * Data source: SEC EDGAR DERA Insider Trading bulk files
 *   https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=4
 *   Bulk files: https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{n}/
 *   TSV format: SUBMISSION.tsv + REPORTINGOWNER.tsv + NONDERIV_TRANS.tsv + DERIV_TRANS.tsv
 *
 * This script is IDENTICAL in logic to sync-worker.js but iterates all quarters
 * instead of just the most recent ones. No changes to server.js required.
 */

'use strict';
const https  = require('https');
const http   = require('http');
const path   = require('path');
const fs     = require('fs');
const zlib   = require('zlib');

// ── Config ────────────────────────────────────────────────────────
// Match server.js: uses /var/data on Render (persistent disk), local data/ otherwise
const DATA_DIR = require('fs').existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
const DB_PATH  = process.env.DB_PATH || path.join(DATA_DIR, 'trades.db');
const START_YEAR = parseInt(process.argv[2]) || 2004;
const END_YEAR   = parseInt(process.argv[3]) || new Date().getFullYear();
const DELAY_MS   = 1200; // polite delay between SEC requests (SEC asks for ≤10 req/s)
const MAX_RETRIES = 3;

// ── Database ──────────────────────────────────────────────────────
const Database = require('better-sqlite3');
if (!fs.existsSync(path.dirname(DB_PATH))) {
  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
}
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('cache_size = -32000'); // 32MB cache for faster bulk inserts

// Ensure table exists (same schema as server.js)
db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
    trade_date TEXT NOT NULL, filing_date TEXT,
    type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER,
    accession TEXT,
    UNIQUE(accession, insider, trade_date, type, qty)
  );
  CREATE INDEX IF NOT EXISTS idx_ticker      ON trades(ticker);
  CREATE INDEX IF NOT EXISTS idx_trade_date  ON trades(trade_date DESC);
  CREATE INDEX IF NOT EXISTS idx_filing_date ON trades(filing_date DESC);
  CREATE INDEX IF NOT EXISTS idx_insider     ON trades(insider);
`);

// Track which quarters have been fully backfilled
db.exec(`
  CREATE TABLE IF NOT EXISTS backfill_log (
    quarter TEXT PRIMARY KEY,
    completed_at TEXT DEFAULT (datetime('now')),
    rows_inserted INTEGER
  )
`);

const upsert = db.prepare(`
  INSERT OR IGNORE INTO trades
    (ticker, company, insider, title, trade_date, filing_date,
     type, qty, price, value, owned, accession)
  VALUES
    (@ticker, @company, @insider, @title, @trade_date, @filing_date,
     @type, @qty, @price, @value, @owned, @accession)
`);

const insertBatch = db.transaction(rows => {
  let n = 0;
  for (const r of rows) {
    try { upsert.run(r); n++; } catch(e) { /* duplicate — skip */ }
  }
  return n;
});

// ── HTTP helper ───────────────────────────────────────────────────
function get(url, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 (historical-backfill; contact@insidertape.com)' },
      timeout: timeoutMs,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

async function getWithRetry(url, retries = MAX_RETRIES) {
  for (let i = 0; i < retries; i++) {
    try {
      const r = await get(url);
      if (r.status === 200) return r.body;
      if (r.status === 404) return null; // quarter doesn't exist yet
      if (r.status === 429 || r.status >= 500) {
        const wait = (i + 1) * 5000;
        log(`  HTTP ${r.status} — waiting ${wait/1000}s before retry ${i+1}/${retries}`);
        await sleep(wait);
        continue;
      }
      return null;
    } catch(e) {
      if (i === retries - 1) throw e;
      await sleep((i + 1) * 3000);
    }
  }
  return null;
}

// ── Logging ───────────────────────────────────────────────────────
function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── TSV Parser ────────────────────────────────────────────────────
function parseTsv(text) {
  const lines = text.split('\n');
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

// ── Quarter processor ─────────────────────────────────────────────
async function processQuarter(year, qtr) {
  const label = `${year}Q${qtr}`;

  // Skip if already done
  const done = db.prepare('SELECT 1 FROM backfill_log WHERE quarter=?').get(label);
  if (done) {
    log(`${label}: already complete — skipping`);
    return 0;
  }

  const base = `https://www.sec.gov/Archives/edgar/full-index/${year}/QTR${qtr}`;
  log(`${label}: downloading index files from ${base}/`);

  // Download all four TSV files
  const [submBuf, ownerBuf, ndBuf, dBuf] = await Promise.all([
    getWithRetry(`${base}/SUBMISSION.tsv`),
    getWithRetry(`${base}/REPORTINGOWNER.tsv`),
    getWithRetry(`${base}/NONDERIV_TRANS.tsv`),
    getWithRetry(`${base}/DERIV_TRANS.tsv`),
  ]);

  if (!submBuf || !ownerBuf || !ndBuf) {
    log(`${label}: missing required files — skipping`);
    return 0;
  }

  // Parse submissions (filing metadata: accession → company info + filing date)
  const submissions = parseTsv(submBuf.toString('utf8'));
  const form4 = submissions.filter(r =>
    (r.form_type || r.formtype || '').replace(/ /g, '') === '4'
  );
  log(`${label}: ${form4.length} Form 4 submissions`);

  // Build lookup: accession → { company, filing_date, cik }
  const submMap = {};
  for (const s of form4) {
    const acc = (s.accession_number || s.accessionnumber || '').replace(/-/g, '');
    if (!acc) continue;
    submMap[acc] = {
      company:      (s.company_name || s.companyname || '').slice(0, 120),
      filing_date:  (s.date_filed || s.datefiled || '').slice(0, 10),
      cik:          s.cik || '',
    };
  }

  // Parse reporting owners (insider names + titles)
  const owners = parseTsv(ownerBuf.toString('utf8'));
  const ownerMap = {}; // accession → { insider, title }
  for (const o of owners) {
    const acc = (o.accession_number || o.accessionnumber || '').replace(/-/g, '');
    if (!acc) continue;
    const name = (o.reporting_owner_name || o.reportingownername || '').trim();
    const title = (
      o.reporting_owner_title || o.reportingownertitle ||
      o.officer_title || o.officertitle || ''
    ).trim().slice(0, 80);
    if (name) ownerMap[acc] = { insider: name, title };
  }

  let totalInserted = 0;
  const BATCH = 2000;

  // ── Non-derivative transactions ──────────────────────────────────
  const ndRows = parseTsv(ndBuf.toString('utf8'));
  log(`${label}: ${ndRows.length} non-derivative transaction rows`);

  let batch = [];
  for (const r of ndRows) {
    const acc = (r.accession_number || r.accessionnumber || '').replace(/-/g, '');
    const sub = submMap[acc];
    const own = ownerMap[acc];
    if (!sub || !own) continue;

    const ticker = (r.security_title === 'Common Stock' || !r.security_title
      ? (r.issuer_trading_symbol || r.issuertradingsymbol || '')
      : ''
    ).toUpperCase().replace(/[^A-Z0-9.]/g, '').trim();

    // Fall back: try to extract ticker from company name for older filings
    const ticker2 = (r.issuer_trading_symbol || r.issuertradingsymbol || '')
      .toUpperCase().replace(/[^A-Z0-9.]/g, '').trim();
    const finalTicker = ticker || ticker2;
    if (!finalTicker || finalTicker.length > 10) continue;

    const tradeDate   = (r.transaction_date || r.transactiondate || '').slice(0, 10);
    const filingDate  = sub.filing_date;
    if (!tradeDate || tradeDate < '2000-01-01') continue;

    const typeCode    = (r.transaction_code || r.transactioncode || '').toUpperCase();
    // P = open-market purchase, S = open-market sale, S- = sale (10b5-1)
    // Filter to open-market only (exclude gifts, grants, conversions etc.)
    const tradeType = typeCode === 'P' ? 'P'
                    : typeCode === 'S' ? 'S'
                    : typeCode === 'S-' ? 'S-'
                    : null;
    if (!tradeType) continue; // skip grants, gifts, conversions

    const qty   = parseFloat(r.transaction_shares || r.transactionshares || '0') || 0;
    const price = parseFloat(r.transaction_pricepershare || r.transactionpricepershare || '0') || 0;
    const value = Math.round(qty * price) || 0;
    const owned = parseFloat(r.shares_owned_following_transaction || r.sharesownedfollowing || '0') || 0;

    const accDash = acc.replace(/(\d{10})(\d{8})(\d{4})/, '$1-$2-$3');

    batch.push({
      ticker:       finalTicker,
      company:      sub.company,
      insider:      own.insider,
      title:        own.title,
      trade_date:   tradeDate,
      filing_date:  filingDate,
      type:         tradeType,
      qty:          Math.round(qty),
      price,
      value,
      owned:        Math.round(owned),
      accession:    accDash,
    });

    if (batch.length >= BATCH) {
      totalInserted += insertBatch(batch);
      batch = [];
    }
  }
  if (batch.length) { totalInserted += insertBatch(batch); batch = []; }

  // ── Derivative transactions (options, warrants) ──────────────────
  if (dBuf) {
    const dRows = parseTsv(dBuf.toString('utf8'));
    for (const r of dRows) {
      const acc = (r.accession_number || r.accessionnumber || '').replace(/-/g, '');
      const sub = submMap[acc];
      const own = ownerMap[acc];
      if (!sub || !own) continue;

      const ticker = (r.issuer_trading_symbol || r.issuertradingsymbol || '')
        .toUpperCase().replace(/[^A-Z0-9.]/g, '').trim();
      if (!ticker || ticker.length > 10) continue;

      const tradeDate  = (r.transaction_date || r.transactiondate || '').slice(0, 10);
      if (!tradeDate || tradeDate < '2000-01-01') continue;

      const typeCode = (r.transaction_code || r.transactioncode || '').toUpperCase();
      // For derivatives: A = grant/award, M = exercise, D = disposition
      const tradeType = typeCode === 'A' ? 'O-A'
                      : typeCode === 'M' ? 'O-M'
                      : typeCode === 'D' ? 'O-D'
                      : null;
      if (!tradeType) continue;

      const qty   = parseFloat(r.transaction_shares || r.number_of_derivative_securities_acquired || '0') || 0;
      const price = parseFloat(r.exercise_price || r.transactionpricepershare || '0') || 0;
      const value = Math.round(qty * price) || 0;
      const accDash = acc.replace(/(\d{10})(\d{8})(\d{4})/, '$1-$2-$3');

      batch.push({
        ticker, company: sub.company, insider: own.insider, title: own.title,
        trade_date: tradeDate, filing_date: sub.filing_date,
        type: tradeType, qty: Math.round(qty), price, value, owned: 0,
        accession: accDash,
      });

      if (batch.length >= BATCH) {
        totalInserted += insertBatch(batch);
        batch = [];
      }
    }
    if (batch.length) { totalInserted += insertBatch(batch); batch = []; }
  }

  // Mark quarter complete
  db.prepare(`
    INSERT OR REPLACE INTO backfill_log (quarter, rows_inserted)
    VALUES (?, ?)
  `).run(label, totalInserted);

  log(`${label}: complete — ${totalInserted.toLocaleString()} rows inserted`);
  return totalInserted;
}

// ── Build quarter list ─────────────────────────────────────────────
function getQuarters(startYear, endYear) {
  const quarters = [];
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentQtr  = Math.ceil((now.getMonth() + 1) / 3);

  for (let year = startYear; year <= endYear; year++) {
    for (let qtr = 1; qtr <= 4; qtr++) {
      // Don't request future quarters
      if (year === currentYear && qtr > currentQtr) break;
      // SEC EDGAR quarterly files available from 2004 Q1
      if (year < 2004) continue;
      quarters.push({ year, qtr });
    }
  }
  return quarters;
}

// ── Main ──────────────────────────────────────────────────────────
async function main() {
  const quarters = getQuarters(START_YEAR, END_YEAR);
  const existing = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;

  log(`=== InsiderTape Historical Backfill ===`);
  log(`Range: ${START_YEAR}–${END_YEAR} (${quarters.length} quarters)`);
  log(`Current DB: ${existing.toLocaleString()} trades`);
  log(`DB path: ${DB_PATH}`);
  log(`Started: ${new Date().toISOString()}`);
  log(`Polite delay: ${DELAY_MS}ms between quarters`);
  log('');

  let grandTotal = 0;
  let completed  = 0;
  let skipped    = 0;

  for (const { year, qtr } of quarters) {
    try {
      const n = await processQuarter(year, qtr);
      if (n === 0) {
        skipped++;
      } else {
        grandTotal += n;
        completed++;
      }
    } catch(e) {
      log(`${year}Q${qtr}: ERROR — ${e.message}`);
    }

    // Progress summary every 10 quarters
    if ((completed + skipped) % 10 === 0) {
      const total = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
      log(`--- Progress: ${completed} done, ${skipped} skipped, DB now ${total.toLocaleString()} rows ---`);
    }

    await sleep(DELAY_MS);
  }

  const finalTotal = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  log('');
  log(`=== Backfill Complete ===`);
  log(`Quarters processed: ${completed}`);
  log(`Quarters skipped (already done): ${skipped}`);
  log(`New rows inserted: ${grandTotal.toLocaleString()}`);
  log(`Total DB rows: ${finalTotal.toLocaleString()}`);
  log(`Finished: ${new Date().toISOString()}`);

  db.close();
}

main().catch(e => {
  log(`FATAL: ${e.message}`);
  console.error(e);
  process.exit(1);
});
