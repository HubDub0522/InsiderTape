// One-time 5-year backfill for congressional trades
// Fetches per-member using FMP's member list endpoints
// Run: DB_PATH=/var/data/trades.db FMP_API_KEY=your_key node congress-backfill.js

'use strict';
const https    = require('https');
const path     = require('path');
const Database = require('better-sqlite3');

const FMP_KEY = process.env.FMP_API_KEY;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'trades.db');
const DELAY_MS = 350; // stay under FMP Starter 300 req/min

if (!FMP_KEY) { console.error('FMP_API_KEY not set'); process.exit(1); }

const db = new Database(DB_PATH);

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

function fmpGet(path) {
  return new Promise((resolve, reject) => {
    const url = 'https://financialmodelingprep.com/stable/' + path + '&apikey=' + FMP_KEY;
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

// First, probe to see what the endpoint actually returns so we know its shape
async function probe() {
  console.log('Probing FMP endpoints...');
  const h = await fmpGet('house-trades?');
  console.log('house-trades response type:', typeof h, Array.isArray(h) ? 'array len=' + h.length : JSON.stringify(h).slice(0, 200));
  const s = await fmpGet('senate-trades?');
  console.log('senate-trades response type:', typeof s, Array.isArray(s) ? 'array len=' + s.length : JSON.stringify(s).slice(0, 200));
  // Try with a known member
  const p = await fmpGet('senate-trades?name=Nancy+Pelosi');
  console.log('senate-trades?name=Pelosi:', typeof p, Array.isArray(p) ? 'len=' + p.length : JSON.stringify(p).slice(0, 300));
  const p2 = await fmpGet('house-trades?name=Nancy+Pelosi');
  console.log('house-trades?name=Pelosi:', typeof p2, Array.isArray(p2) ? 'len=' + p2.length : JSON.stringify(p2).slice(0, 300));
  // Try first item shape
  if (Array.isArray(h) && h.length) console.log('First house trade keys:', Object.keys(h[0]));
  if (Array.isArray(p) && p.length) console.log('First senate trade keys:', Object.keys(p[0]));
  if (Array.isArray(p2) && p2.length) console.log('First house trade (Pelosi) keys:', Object.keys(p2[0]));
}

// Get unique members already in our DB to fetch per-member
function getKnownMembers() {
  return db.prepare('SELECT DISTINCT member, chamber FROM gov_trades ORDER BY member').all();
}

function processTrades(trades, chamber) {
  if (!Array.isArray(trades) || !trades.length) return 0;
  let n = 0;
  const cutoff = '2020-01-01';
  for (const t of trades) {
    const txDate = t.transactionDate || t.transaction_date || t.date || '';
    if (!txDate || txDate < cutoff) continue;
    const txType = normType(t.type || t.transactionType || t.transaction_type || '');
    if (!txType) continue;
    const ticker = (t.symbol || t.ticker || t.asset || '').trim().toUpperCase();
    if (!ticker || ticker === '--' || ticker.length > 6 || /[^A-Z.]/.test(ticker)) continue;
    const member = (t.representative || t.senator || t.name ||
      ((t.firstName||'') + ' ' + (t.lastName||'')).trim() || '').trim();
    if (!member) continue;
    try {
      insert.run({
        chamber,
        member,
        ticker,
        transaction_type: txType,
        transaction_date: txDate,
        disclosure_date:  t.disclosureDate || t.disclosure_date || null,
        amount_range:     t.amount || t.amount_range || null,
        owner:            t.owner || null,
        asset_description: t.assetDescription || t.asset_description || null,
        filing_url:       t.link || t.filing_url || null,
        doc_id:           t.disclosureYear ? String(t.disclosureYear) : null,
      });
      n++;
    } catch(e) {}
  }
  return n;
}

(async () => {
  console.log('5-year congressional backfill — DB:', DB_PATH);

  // Step 1: probe to understand endpoint shape
  await probe();
  await sleep(500);

  // Step 2: fetch per known member (already in DB from prior syncs)
  const members = getKnownMembers();
  console.log('\nKnown members in DB:', members.length);

  let total = 0;
  for (let i = 0; i < members.length; i++) {
    const { member, chamber } = members[i];
    const encoded = encodeURIComponent(member);
    try {
      let trades;
      if (chamber === 'H') {
        trades = await fmpGet('house-trades?name=' + encoded);
      } else {
        trades = await fmpGet('senate-trades?name=' + encoded);
      }
      const n = processTrades(trades, chamber);
      total += n;
      if (n > 0) console.log('[' + chamber + '] ' + member + ': +' + n);
      await sleep(DELAY_MS);
    } catch(e) {
      console.warn('Error for', member, e.message);
    }
    if ((i + 1) % 20 === 0) {
      console.log('Progress:', (i+1) + '/' + members.length, '| inserted so far:', total);
    }
  }

  const final = db.prepare('SELECT COUNT(*) as n FROM gov_trades').get();
  const byYear = db.prepare("SELECT substr(transaction_date,1,4) as yr, COUNT(*) as n FROM gov_trades GROUP BY yr ORDER BY yr DESC").all();
  console.log('\n✅ Done! Total in DB:', final.n, '| New this run:', total);
  console.log('By year:', byYear.map(r => r.yr + ':' + r.n).join(', '));
  db.close();
})();
