'use strict';
// congress-worker.js
// Polls the free S3-backed community STOCK Act feeds and inserts
// Senate/House congressional trades into the gov_trades table.
//
// Sources (no auth, updated daily by community scrapers):
//   Senate: senate-stock-watcher-data.s3-us-west-2.amazonaws.com
//   House:  house-stock-watcher-data.s3-us-west-2.amazonaws.com

const https    = require('https');
const http     = require('http');
const path     = require('path');
const fs       = require('fs');
const Database = require('better-sqlite3');

// ── DB path (server passes DB_PATH via env) ─────────────────────
const DATA_DIR = (() => {
  const envPath = process.env.DB_PATH;
  if (envPath) return path.dirname(envPath);
  for (const d of ['/var/data', path.join(__dirname, 'data')]) {
    try { fs.mkdirSync(d, { recursive: true }); const p = path.join(d, '.probe'); fs.writeFileSync(p, '1'); fs.unlinkSync(p); return d; }
    catch { /* try next */ }
  }
  return path.join(__dirname, 'data');
})();
const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, 'trades.db');

console.log(`[congress] DB path: ${DB_PATH}`);
let db;
try {
  db = new Database(DB_PATH);
} catch(e) {
  console.error(`[congress] Cannot open DB: ${e.message}`);
  process.exit(1);
}
db.pragma('journal_mode = WAL');

// ── Schema ───────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS gov_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    chamber           TEXT NOT NULL,        -- 'S' senate | 'H' house
    member            TEXT NOT NULL,
    ticker            TEXT,                 -- may be '--' if unknown
    asset_description TEXT,
    transaction_type  TEXT,                 -- 'P' purchase | 'S' sale | 'E' exchange
    transaction_date  TEXT,
    disclosure_date   TEXT,
    amount_range      TEXT,                 -- e.g. "$1,001 - $15,000"
    owner             TEXT,                 -- Self | Spouse | Joint | Dependent
    filing_url        TEXT,
    UNIQUE(chamber, member, ticker, transaction_date, transaction_type, amount_range)
  )
`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_ticker   ON gov_trades(ticker)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_member   ON gov_trades(member)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_td       ON gov_trades(transaction_date DESC)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_chamber  ON gov_trades(chamber)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_dd       ON gov_trades(disclosure_date DESC)`);

// ── HTTP helper ─────────────────────────────────────────────────
function get(url, ms = 60000, _hops = 0) {
  if (_hops > 5) return Promise.reject(new Error('Too many redirects'));
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('http://') ? http : https;
    const req = mod.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; InsiderTape/1.0)' },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return get(next, ms, _hops + 1).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout fetching ${url}`)); });
  });
}

// ── Normalise transaction type ───────────────────────────────────
function normType(raw = '') {
  const t = raw.trim().toLowerCase();
  if (t.includes('purchase') || t === 'buy' || t === 'p') return 'P';
  if (t.includes('sale') || t.includes('sell') || t === 's') return 'S';
  if (t.includes('exchange') || t === 'e') return 'E';
  return raw.slice(0, 20);
}

// ── Normalise date strings ────────────────────────────────────────
function normDate(raw = '') {
  if (!raw || raw === '--') return null;
  // Handles MM/DD/YYYY → YYYY-MM-DD, already YYYY-MM-DD passthrough
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  return null;
}

// ── Normalise ticker ─────────────────────────────────────────────
function normTicker(raw = '') {
  const t = (raw || '').trim().toUpperCase();
  if (!t || t === '--' || t === 'N/A' || t === 'NA') return '--';
  // Remove exchange prefixes like NYSE: or NASDAQ:
  return t.replace(/^[A-Z]+:\s*/,'').slice(0, 10);
}

const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO gov_trades
    (chamber, member, ticker, asset_description, transaction_type,
     transaction_date, disclosure_date, amount_range, owner, filing_url)
  VALUES
    (@chamber, @member, @ticker, @asset_description, @transaction_type,
     @transaction_date, @disclosure_date, @amount_range, @owner, @filing_url)
`);

const insertMany = db.transaction(rows => {
  let inserted = 0;
  for (const r of rows) {
    const info = insertStmt.run(r);
    inserted += info.changes;
  }
  return inserted;
});

// ── Senate ───────────────────────────────────────────────────────
async function fetchSenate() {
  const url = 'https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json';
  console.log('[congress] Fetching Senate data…');
  const { status, body } = await get(url, 90000);
  if (status !== 200) throw new Error(`Senate S3 returned HTTP ${status}`);

  let raw;
  try { raw = JSON.parse(body.toString('utf8')); }
  catch(e) { throw new Error('Senate JSON parse failed: ' + e.message); }

  // Format: array of { first_name, last_name, office, ptr_link, date_recieved, transactions[] }
  const rows = [];
  for (const filing of raw) {
    const member = `${filing.first_name || ''} ${filing.last_name || ''}`.trim();
    const disclosureDate = normDate(filing.date_recieved || filing.date_received || '');
    const filingUrl = filing.ptr_link || '';

    for (const tx of (filing.transactions || [])) {
      if (!tx || tx.asset_type?.toLowerCase().includes('non-public')) continue;
      rows.push({
        chamber:          'S',
        member,
        ticker:           normTicker(tx.ticker),
        asset_description:(tx.asset_description || '').slice(0, 200),
        transaction_type: normType(tx.type || ''),
        transaction_date: normDate(tx.transaction_date || ''),
        disclosure_date:  disclosureDate,
        amount_range:     (tx.amount || '').slice(0, 50),
        owner:            (tx.owner || '').slice(0, 30),
        filing_url:       filingUrl,
      });
    }
  }
  console.log(`[congress] Senate: parsed ${rows.length} transactions`);
  return rows;
}

// ── House ────────────────────────────────────────────────────────
async function fetchHouse() {
  const url = 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json';
  console.log('[congress] Fetching House data…');
  const { status, body } = await get(url, 90000);
  if (status !== 200) throw new Error(`House S3 returned HTTP ${status}`);

  let raw;
  try { raw = JSON.parse(body.toString('utf8')); }
  catch(e) { throw new Error('House JSON parse failed: ' + e.message); }

  // Format: flat array of { representative, ticker, asset_description, transaction_date,
  //   disclosure_date, type, amount, owner, district, link, ... }
  const rows = [];
  for (const tx of raw) {
    if (!tx) continue;
    rows.push({
      chamber:          'H',
      member:           (tx.representative || '').slice(0, 100),
      ticker:           normTicker(tx.ticker),
      asset_description:(tx.asset_description || '').slice(0, 200),
      transaction_type: normType(tx.type || ''),
      transaction_date: normDate(tx.transaction_date || ''),
      disclosure_date:  normDate(tx.disclosure_date || ''),
      amount_range:     (tx.amount || '').slice(0, 50),
      owner:            (tx.owner || '').slice(0, 30),
      filing_url:       (tx.link || '').slice(0, 300),
    });
  }
  console.log(`[congress] House: parsed ${rows.length} transactions`);
  return rows;
}

// ── Main ─────────────────────────────────────────────────────────
(async () => {
  try {
    const [senateRows, houseRows] = await Promise.all([fetchSenate(), fetchHouse()]);
    const allRows = [...senateRows, ...houseRows];
    console.log(`[congress] Total rows to insert: ${allRows.length}`);
    const inserted = insertMany(allRows);
    console.log(`[congress] Inserted ${inserted} new rows (${allRows.length - inserted} already existed)`);
    process.exit(0);
  } catch(e) {
    console.error('[congress] FATAL:', e.message);
    process.exit(1);
  }
})();
