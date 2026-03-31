// 5-year congressional backfill — loops all tickers in trades DB
// Run: DB_PATH=/var/data/trades.db FMP_API_KEY=your_key node congress-backfill.js
'use strict';
const https    = require('https');
const path     = require('path');
const Database = require('better-sqlite3');

const FMP_KEY  = process.env.FMP_API_KEY;
const DB_PATH  = process.env.DB_PATH || path.join(__dirname, 'trades.db');
const DELAY_MS = 350; // ~170 req/min — safe under 300/min Starter limit
const CUTOFF   = '2020-01-01';

if (!FMP_KEY) { console.error('FMP_API_KEY not set'); process.exit(1); }

const db = new Database(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS gov_trades (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    chamber          TEXT NOT NULL, member TEXT NOT NULL,
    ticker           TEXT NOT NULL, transaction_type TEXT NOT NULL,
    transaction_date TEXT, disclosure_date TEXT, amount_range TEXT,
    owner TEXT, asset_description TEXT, filing_url TEXT, doc_id TEXT,
    created_at TEXT DEFAULT (date('now')),
    UNIQUE(chamber, member, ticker, transaction_type, transaction_date, amount_range)
  )
`);

const insert = db.prepare(`
  INSERT OR IGNORE INTO gov_trades
    (chamber, member, ticker, transaction_type, transaction_date, disclosure_date,
     amount_range, owner, asset_description, filing_url, doc_id)
  VALUES (@chamber,@member,@ticker,@transaction_type,@transaction_date,@disclosure_date,
          @amount_range,@owner,@asset_description,@filing_url,@doc_id)
`);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function normType(raw) {
  const t = (raw||'').toLowerCase();
  if (t.includes('purchase')||t.includes('exchange')) return 'P';
  if (t.includes('sale')) return 'S';
  return null;
}
function fmpGet(endpoint) {
  return new Promise((resolve, reject) => {
    const url = 'https://financialmodelingprep.com/stable/' + endpoint + '&apikey=' + FMP_KEY;
    https.get(url, { headers:{'User-Agent':'InsiderTape/1.0'}, timeout:20000 }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
    }).on('error', reject).on('timeout', () => reject(new Error('timeout')));
  });
}

function processTrades(trades, chamber) {
  if (!Array.isArray(trades)||!trades.length) return 0;
  let n = 0;
  for (const t of trades) {
    if (!t.transactionDate || t.transactionDate < CUTOFF) continue;
    const txType = normType(t.type);
    if (!txType) continue;
    const ticker = (t.symbol||'').trim().toUpperCase();
    if (!ticker||ticker==='--'||ticker.length>6) continue;
    const member = ((t.firstName||'')+' '+(t.lastName||'')).trim();
    if (!member) continue;
    try {
      insert.run({
        chamber, member, ticker,
        transaction_type: txType,
        transaction_date: t.transactionDate,
        disclosure_date:  t.disclosureDate||null,
        amount_range:     t.amount||null,
        owner:            t.owner||null,
        asset_description: t.assetDescription||null,
        filing_url:       t.link||null,
        doc_id:           t.disclosureDate ? t.disclosureDate.slice(0,4) : null,
      });
      n++;
    } catch(e) {}
  }
  return n;
}

(async () => {
  // Get ALL tickers ever seen in our DB (not just recent ones)
  const tickers = db.prepare(`
    SELECT DISTINCT ticker FROM trades
    WHERE ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
    ORDER BY ticker
  `).all().map(r => r.ticker);

  console.log('Backfilling', tickers.length, 'tickers from', CUTOFF, '→ now');
  console.log('Estimated time:', Math.round(tickers.length * DELAY_MS * 2 / 60000), 'minutes');

  let total = 0, errors = 0;
  const startTime = Date.now();

  for (let i = 0; i < tickers.length; i++) {
    const ticker = tickers[i];
    try {
      const [h, s] = await Promise.all([
        fmpGet('house-trades?symbol=' + ticker),
        fmpGet('senate-trades?symbol=' + ticker),
      ]);
      const n = processTrades(h, 'H') + processTrades(s, 'S');
      total += n;
      if (n > 0) process.stdout.write('.');
      await sleep(DELAY_MS);
    } catch(e) {
      errors++;
      await sleep(1000);
    }

    if ((i+1) % 50 === 0) {
      const elapsed = ((Date.now()-startTime)/60000).toFixed(1);
      const eta = ((tickers.length-(i+1)) * DELAY_MS * 2 / 60000).toFixed(0);
      console.log('\n['+elapsed+'min] '+( i+1)+'/'+tickers.length+' tickers | +'+total+' inserted | ~'+eta+'min left');
    }
  }

  const final = db.prepare('SELECT COUNT(*) as n FROM gov_trades').get();
  const byYear = db.prepare("SELECT substr(transaction_date,1,4) yr, COUNT(*) n FROM gov_trades GROUP BY yr ORDER BY yr DESC").all();
  console.log('\n✅ Done! New rows:', total, '| Total in DB:', final.n, '| Errors:', errors);
  console.log('By year:', byYear.map(r=>r.yr+':'+r.n).join(', '));
  db.close();
})();
