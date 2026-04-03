'use strict';
// congress-worker.js — fetches congressional trades from FMP API
// Requires FMP_API_KEY env var (Starter plan or above)

const https = require('https');
const path  = require('path');

const DB_PATH   = process.env.DB_PATH;
if (!DB_PATH) { console.error('[congress] DB_PATH env var not set — aborting'); process.exit(0); }
const FMP_KEY   = process.env.FMP_API_KEY;
const DAYS_BACK = parseInt(process.env.CONGRESS_DAYS_BACK || '90');

if (!FMP_KEY) { console.error('[congress] FMP_API_KEY not set — skipping'); process.exit(0); }

const Database = require('better-sqlite3');
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 10000');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Ensure gov_trades table ───────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS gov_trades (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    chamber          TEXT NOT NULL,
    member           TEXT NOT NULL,
    ticker           TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    transaction_date TEXT,
    disclosure_date  TEXT,
    amount_range     TEXT,
    owner            TEXT,
    asset_description TEXT,
    filing_url       TEXT,
    doc_id           TEXT,
    created_at       TEXT DEFAULT (date('now')),
    UNIQUE(chamber, member, ticker, transaction_type, transaction_date, amount_range)
  )
`);

const insertGov = db.prepare(`
  INSERT OR IGNORE INTO gov_trades
    (chamber, member, ticker, transaction_type, transaction_date, disclosure_date,
     amount_range, owner, asset_description, filing_url, doc_id)
  VALUES
    (@chamber, @member, @ticker, @transaction_type, @transaction_date, @disclosure_date,
     @amount_range, @owner, @asset_description, @filing_url, @doc_id)
`);

// ── HTTP helper ───────────────────────────────────────────────────────────────
function fmpGet(endpoint) {
  return new Promise((resolve, reject) => {
    const url = 'https://financialmodelingprep.com/stable/' + endpoint + '&apikey=' + FMP_KEY;
    https.get(url, { headers: { 'User-Agent': 'InsiderTape/1.0' }, timeout: 15000 }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
        catch(e) { reject(new Error('JSON parse: ' + e.message)); }
      });
      res.on('error', reject);
    }).on('error', reject).on('timeout', function() { this.destroy(); reject(new Error('timeout')); });
  });
}

// ── Normalize transaction type ────────────────────────────────────────────────
function normType(raw) {
  const t = (raw || '').toLowerCase();
  if (t.includes('purchase') || t.includes('exchange')) return 'P';
  if (t.includes('sale')) return 'S';
  return null;
}

function cutoffDate() {
  const d = new Date();
  d.setDate(d.getDate() - DAYS_BACK);
  return d.toISOString().split('T')[0];
}

// ── Insert FMP trades array ───────────────────────────────────────────────────
function processTrades(trades, chamber) {
  if (!Array.isArray(trades) || !trades.length) return 0;
  const cutoff = cutoffDate();
  let inserted = 0;
  const rows = [];

  for (const t of trades) {
    if (t.transactionDate && t.transactionDate < cutoff) continue;
    const txType = normType(t.type);
    if (!txType) continue;
    const ticker = (t.symbol || '').trim().toUpperCase();
    if (!ticker || !/^[A-Z]{1,6}$/.test(ticker)) continue;
    const member = ((t.firstName || '') + ' ' + (t.lastName || '')).trim();
    if (!member) continue;
    const urlMatch = (t.link || '').match(/\/(\d+)\.pdf$/);
    rows.push({
      chamber,
      member,
      ticker,
      transaction_type:  txType,
      transaction_date:  t.transactionDate  || null,
      disclosure_date:   t.disclosureDate   || null,
      amount_range:      t.amount           || null,
      owner:             t.owner            || 'Self',
      asset_description: (t.assetDescription || '').slice(0, 200),
      filing_url:        t.link             || null,
      doc_id:            urlMatch ? urlMatch[1] : null,
    });
  }

  const insertMany = db.transaction(rs => {
    for (const r of rs) { if (insertGov.run(r).changes) inserted++; }
  });
  if (rows.length) insertMany(rows);
  return inserted;
}

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  console.log('[congress] FMP sync starting — days back:', DAYS_BACK);
  console.log('[congress] DB_PATH:', DB_PATH);
  console.log('[congress] DB file exists:', require('fs').existsSync(DB_PATH));
  // List all tables in the DB
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all();
  console.log('[congress] Tables in DB:', tables.map(t => t.name).join(', ') || '(none)');

  // Wait up to 2 minutes for trades table to exist (DB initializes after server starts)
  for (let i = 0; i < 60; i++) {
    const exists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'").get();
    if (exists) break;
    console.log('[congress] Waiting for trades table... (' + (i * 5) + 's)');
    await sleep(5000);
  }
  const tradesExists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'").get();
  if (!tradesExists) {
    console.error('[congress] trades table never appeared after 5min — aborting');
    db.close();
    process.exit(0);
  }

  // Get tickers from our SEC data — use a wider window so tickers like SMPL
  // that have no recent Form 4 activity still get their congressional trades fetched
  const tickers = db.prepare(`
    SELECT DISTINCT ticker FROM trades
    WHERE ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
    ORDER BY ticker
  `).all().map(r => r.ticker);

  console.log('[congress] Tickers to check:', tickers.length);

  let totalH = 0, totalS = 0, errors = 0;
  // Rate limit: 300 calls/min on Starter plan. Each ticker = 2 calls.
  // Cap at 240 calls/min (120 tickers/min) to leave headroom for other endpoints.
  // 120 tickers/min = 1 ticker per 500ms.
  // Hard-cap: track calls in the current minute, pause if approaching 240.
  let callsThisMinute = 0;
  let minuteStart = Date.now();

  for (let i = 0; i < tickers.length; i++) {
    const ticker = tickers[i];

    // Hard rate-cap: if we've made 240 calls this minute, wait out the remainder
    if (callsThisMinute >= 240) {
      const elapsed = Date.now() - minuteStart;
      const waitMs = Math.max(0, 60000 - elapsed + 500);
      if (waitMs > 0) {
        console.log('[congress] Rate cap reached — pausing ' + Math.round(waitMs/1000) + 's');
        await sleep(waitMs);
      }
      callsThisMinute = 0;
      minuteStart = Date.now();
    }

    try {
      const h = await fmpGet('house-trades?symbol='  + ticker);
      callsThisMinute++;
      totalH += processTrades(h, 'H');
      const s = await fmpGet('senate-trades?symbol=' + ticker);
      callsThisMinute++;
      totalS += processTrades(s, 'S');
      await sleep(500); // 2 calls per 500ms = 240 calls/min max
      if ((i + 1) % 100 === 0) {
        console.log('[congress] ' + (i+1) + '/' + tickers.length + ' | H+' + totalH + ' S+' + totalS + ' | calls/min: ' + callsThisMinute);
      }
    } catch(e) {
      errors++;
      if (errors <= 5) console.warn('[congress] ' + ticker + ': ' + e.message);
    }
  }

  console.log('[congress] Done — house:' + totalH + ' senate:' + totalS + ' errors:' + errors);
  db.close();
})().catch(e => {
  console.error('[congress] Fatal:', e.message);
  try { db.close(); } catch(_) {}
  process.exit(1);
});
