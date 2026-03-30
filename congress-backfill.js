// One-time 5-year backfill for congressional trades
// Run with: node congress-backfill.js
// Uses member-level FMP endpoints to get ALL trades per member, not per-ticker

'use strict';
const https   = require('https');
const path    = require('path');
const Database = require('better-sqlite3');

const FMP_KEY  = process.env.FMP_API_KEY;
const DB_PATH  = process.env.DB_PATH || path.join(__dirname, 'trades.db');
const DELAY_MS = 300; // ~200 req/min — safe for FMP Starter (300/min limit)

if (!FMP_KEY) { console.error('FMP_API_KEY not set'); process.exit(1); }

const db = new Database(DB_PATH);

// Ensure table exists
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

const insert = db.prepare(`
  INSERT OR IGNORE INTO gov_trades
    (chamber, member, ticker, transaction_type, transaction_date, disclosure_date,
     amount_range, owner, asset_description, filing_url, doc_id)
  VALUES
    (@chamber, @member, @ticker, @transaction_type, @transaction_date, @disclosure_date,
     @amount_range, @owner, @asset_description, @filing_url, @doc_id)
`);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function normType(raw) {
  const t = (raw || '').toLowerCase();
  if (t.includes('purchase') || t.includes('exchange')) return 'P';
  if (t.includes('sale')) return 'S';
  return null;
}

function fmpGet(endpoint) {
  return new Promise((resolve, reject) => {
    const url = 'https://financialmodelingprep.com/stable/' + endpoint + '&apikey=' + FMP_KEY;
    https.get(url, { headers: { 'User-Agent': 'InsiderTape/1.0' }, timeout: 20000 }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { resolve([]); }
      });
    }).on('error', reject).on('timeout', () => reject(new Error('timeout')));
  });
}

function processBatch(trades, chamber) {
  let n = 0;
  const cutoff = '2020-01-01'; // 5-year lookback
  for (const t of (trades || [])) {
    if (!t.transactionDate || t.transactionDate < cutoff) continue;
    const txType = normType(t.type);
    if (!txType) continue;
    const ticker = (t.symbol || t.ticker || '').trim().toUpperCase();
    if (!ticker || ticker === '--' || ticker.length > 6) continue;
    try {
      insert.run({
        chamber,
        member:           (t.representative || t.senator || t.firstName + ' ' + t.lastName || '').trim(),
        ticker,
        transaction_type: txType,
        transaction_date: t.transactionDate,
        disclosure_date:  t.disclosureDate || null,
        amount_range:     t.amount || null,
        owner:            t.owner || null,
        asset_description: t.assetDescription || null,
        filing_url:       t.link || null,
        doc_id:           t.disclosureYear ? String(t.disclosureYear) : null,
      });
      n++;
    } catch(e) {}
  }
  return n;
}

(async () => {
  console.log('Starting 5-year congressional backfill...');
  console.log('DB:', DB_PATH);

  // Step 1: Fetch all House trades (no symbol filter = all members)
  console.log('\n[HOUSE] Fetching all house trades...');
  let houseTotal = 0, hPage = 0;
  while (true) {
    const data = await fmpGet('house-trades?page=' + hPage);
    const batch = Array.isArray(data) ? data : (data.data || data.trades || []);
    if (!batch.length) { console.log('[HOUSE] No more pages at page', hPage); break; }
    const n = processBatch(batch, 'H');
    houseTotal += n;
    console.log('[HOUSE] Page', hPage, '— batch:', batch.length, '| inserted:', n, '| total:', houseTotal);
    if (batch.length < 50) break; // last page
    hPage++;
    await sleep(DELAY_MS);
  }

  // Step 2: Fetch all Senate trades
  console.log('\n[SENATE] Fetching all senate trades...');
  let senateTotal = 0, sPage = 0;
  while (true) {
    const data = await fmpGet('senate-trades?page=' + sPage);
    const batch = Array.isArray(data) ? data : (data.data || data.trades || []);
    if (!batch.length) { console.log('[SENATE] No more pages at page', sPage); break; }
    const n = processBatch(batch, 'S');
    senateTotal += n;
    console.log('[SENATE] Page', sPage, '— batch:', batch.length, '| inserted:', n, '| total:', senateTotal);
    if (batch.length < 50) break;
    sPage++;
    await sleep(DELAY_MS);
  }

  const final = db.prepare('SELECT COUNT(*) as n FROM gov_trades').get();
  console.log('\n✅ Backfill complete!');
  console.log('   House inserted:', houseTotal);
  console.log('   Senate inserted:', senateTotal);
  console.log('   Total in DB:', final.n);
  db.close();
})();
