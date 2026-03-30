'use strict';
// congress-worker.js
// Pulls congressional trades directly from government sources:
//   House: disclosures-clerk.house.gov XML index → individual PTR HTML/PDFs
//   Senate: efts.senate.gov full-text search JSON → individual PTR HTML/PDFs
//
// Lightweight: 1 index fetch + N per-filing fetches (usually 5-20/day)
// No third-party APIs, no cost, no auth required.

const https    = require('https');
const http     = require('http');
const path     = require('path');
const fs       = require('fs');
const Database = require('better-sqlite3');

const DATA_DIR = (() => {
  const envPath = process.env.DB_PATH;
  if (envPath) return path.dirname(envPath);
  for (const d of ['/var/data', path.join(__dirname, 'data')]) {
    try { fs.mkdirSync(d, { recursive: true }); const p = path.join(d, '.probe'); fs.writeFileSync(p, '1'); fs.unlinkSync(p); return d; }
    catch(_) {}
  }
  return path.join(__dirname, 'data');
})();
const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, 'trades.db');

console.log(`[congress] DB: ${DB_PATH}`);
let db;
try { db = new Database(DB_PATH); }
catch(e) { console.error(`[congress] Cannot open DB: ${e.message}`); process.exit(1); }
db.pragma('journal_mode = WAL');

// ── Schema ────────────────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS gov_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    chamber           TEXT NOT NULL,
    member            TEXT NOT NULL,
    ticker            TEXT,
    asset_description TEXT,
    transaction_type  TEXT,
    transaction_date  TEXT,
    disclosure_date   TEXT,
    amount_range      TEXT,
    owner             TEXT,
    filing_url        TEXT,
    doc_id            TEXT,
    UNIQUE(chamber, doc_id, ticker, transaction_date, transaction_type)
  )
`);
// Migrate: add doc_id column if old table exists without it
try {
  const cols = db.prepare("PRAGMA table_info(gov_trades)").all().map(c => c.name);
  if (!cols.includes('doc_id')) {
    db.exec("ALTER TABLE gov_trades ADD COLUMN doc_id TEXT");
    console.log('[congress] Migrated: added doc_id column');
  }
} catch(e) { console.warn('[congress] Migration check failed:', e.message); }

db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_ticker  ON gov_trades(ticker)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_member  ON gov_trades(member)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_td      ON gov_trades(transaction_date DESC)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_chamber ON gov_trades(chamber)`);
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_docid ON gov_trades(doc_id)`); } catch(_) {}

// Track which doc IDs we've already processed
const seenDocs = new Set(
  db.prepare("SELECT DISTINCT doc_id FROM gov_trades WHERE doc_id IS NOT NULL").all().map(r => r.doc_id)
);
console.log(`[congress] Already processed ${seenDocs.size} filing IDs`);

// ── HTTP helper ───────────────────────────────────────────────────────────────
function get(url, ms = 30000, hops = 0) {
  if (hops > 5) return Promise.reject(new Error('Too many redirects'));
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; InsiderTape/1.0; +https://insidertape.com)',
        'Accept': 'text/html,application/xhtml+xml,application/xml,*/*',
      },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return get(next, ms, hops + 1).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Normalise helpers ─────────────────────────────────────────────────────────
function normDate(raw = '') {
  if (!raw || raw === '--') return null;
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0,10);
  return null;
}

function normType(raw = '') {
  const t = raw.trim().toLowerCase();
  if (t.includes('purchase') || t === 'buy' || t === 'p') return 'P';
  if (t.includes('sale') || t.includes('sell') || t === 's') return 'S';
  if (t.includes('exchange') || t === 'e') return 'E';
  return raw.slice(0,10);
}

async function fetchCapitolTrades() {
  // Use Capitol Trades BFF (backend-for-frontend) JSON API
  // Returns structured JSON — no HTML parsing needed
  // Covers both House and Senate, tickers already parsed
  let totalInserted = 0;

  for (let page = 1; page <= 5; page++) {
    const url = `https://bff.capitoltrades.com/trades?pageSize=100&page=${page}`;
    try {
      await sleep(800);
      const { status, body } = await get(url, 30000);
      if (status !== 200) { console.warn(`[congress] BFF page ${page}: HTTP ${status}`); break; }

      let json;
      try { json = JSON.parse(body); } catch(e) { console.warn('[congress] BFF JSON parse failed'); break; }

      const trades = json.data || json.trades || json.items || [];
      if (!trades.length) { console.log(`[congress] BFF page ${page}: no trades`); break; }

      const rows = trades.map(t => {
        // Extract ticker — may be in asset.assetTicker, issuer.issuerTicker, ticker field
        const ticker = (t.issuer?.issuerTicker || t.asset?.assetTicker || t.ticker || '--')
          .replace(/:.*/, '').trim().toUpperCase();
        if (!ticker || ticker === '--' || ticker === 'N/A') return null;

        const chamber = (t.politician?.chamber || t.chamber || '').toLowerCase() === 'senate' ? 'S' : 'H';
        const member  = t.politician?.fullName || t.politician?.lastName || t.politicianName || 'Unknown';
        const txType  = (t.txType || t.type || '').toLowerCase().includes('buy') ? 'P' : 'S';
        const txDate  = (t.txDate || t.tradeDate || '').slice(0,10) || null;
        const discDate = (t.pubDate || t.disclosureDate || t.publishedDate || '').slice(0,10) || null;
        const doc_id  = String(t.id || t.tradeId || t.txId || '');
        if (!doc_id || seenDocs.has(doc_id)) return null;

        // Amount range
        const SIZE_MAP = {'1K-15K':'$1,001 - $15,000','15K-50K':'$15,001 - $50,000',
          '50K-100K':'$50,001 - $100,000','100K-250K':'$100,001 - $250,000',
          '250K-500K':'$250,001 - $500,000','500K-1M':'$500,001 - $1,000,000',
          '1M-5M':'$1,000,001 - $5,000,000','5M-25M':'$5,000,001 - $25,000,000'};
        const sizeRaw = (t.txSize || t.size || '').replace(/[–—]/g,'-');
        const amtRange = SIZE_MAP[sizeRaw] || sizeRaw || null;

        return {
          chamber, member, ticker,
          asset_description: (t.issuer?.issuerName || t.assetName || ticker).slice(0,200),
          transaction_type: txType,
          transaction_date: txDate,
          disclosure_date: discDate,
          amount_range: amtRange ? amtRange.slice(0,50) : null,
          owner: (t.owner || 'Self').slice(0,20),
          filing_url: `https://www.capitoltrades.com/trades/${doc_id}`,
          doc_id,
        };
      }).filter(Boolean);

      if (!rows.length) {
        console.log(`[congress] BFF page ${page}: all trades already seen — stopping`);
        break;
      }

      const inserted = insertMany(rows);
      totalInserted += inserted;
      rows.forEach(r => seenDocs.add(r.doc_id));
      console.log(`[congress] BFF page ${page}: ${inserted} new (${trades.length} total)`);

      // Stop if we've hit all-seen trades for 2 consecutive pages
      if (inserted === 0 && page > 1) break;

    } catch(e) {
      console.warn(`[congress] BFF page ${page} error: ${e.message}`);
      break;
    }
  }

  return totalInserted;
}

function parseCapitolTradesHTML(html) {
  const rows = [];
  const text = html.replace(/\r/g, '');

  // Extract each trade row — Capitol Trades renders a table with consistent structure
  // Trade detail links look like: /trades/20003795785 (House) or /trades/10000064795 (Senate)
  const rowRe = /<tr[^>]*class="[^"]*q-tr[^"]*"[^>]*>([\/s\/S]*?)<\/tr>/g;
  let m;

  while ((m = rowRe.exec(text)) !== null) {
    const row = m[1];

    // Trade ID from the detail link
    const idMatch = row.match(/\/trades\/(\d+)/);
    if (!idMatch) continue;
    const doc_id = idMatch[1];

    // Chamber from politician badge: "Senate" or "House"
    const chamberMatch = row.match(/\b(Senate|House)\b/);
    const chamber = chamberMatch ? (chamberMatch[1] === 'Senate' ? 'S' : 'H') : 'H';

    // Member name — first link text in politician cell
    const memberMatch = row.match(/politicians\/[A-Z0-9]+[^>]*>([^<]+)</);
    const member = memberMatch ? memberMatch[1].trim() : 'Unknown';

    // Ticker — appears as "TICK:US" or "N/A"
    const tickerMatch = row.match(/([A-Z]{1,5}):US/);
    const ticker = tickerMatch ? tickerMatch[1] : '--';
    if (ticker === '--') continue; // skip non-stock trades (bonds, etc.)

    // Transaction type: "buy" or "sell"
    const typeMatch = row.match(/class="[^"]*q-badge[^"]*[^"]*"[^>]*>\s*(buy|sell)\s*</i);
    const txType = typeMatch ? (typeMatch[1].toLowerCase() === 'buy' ? 'P' : 'S') : null;
    if (!txType) continue;

    // Trade date: "23 Mar\n  2026" → "2026-03-23"
    const dateMatch = row.match(/(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})/g);
    const MONTHS = {Jan:'01',Feb:'02',Mar:'03',Apr:'04',May:'05',Jun:'06',Jul:'07',Aug:'08',Sep:'09',Oct:'10',Nov:'11',Dec:'12'};
    let txDate = null, discDate = null;
    if (dateMatch && dateMatch.length >= 2) {
      // First date = published/disclosure, second = traded
      const parseDMY = s => { const p = s.match(/(\d{1,2})\s+(\w{3})\s+(\d{4})/); return p ? `${p[3]}-${MONTHS[p[2]]}-${p[1].padStart(2,'0')}` : null; };
      discDate = parseDMY(dateMatch[0]);
      txDate   = parseDMY(dateMatch[1]);
    }

    // Size range: "1K–15K" → "$1,001 - $15,000"
    const sizeMap = {'1K–15K':'$1,001 - $15,000','15K–50K':'$15,001 - $50,000','50K–100K':'$50,001 - $100,000',
      '100K–250K':'$100,001 - $250,000','250K–500K':'$250,001 - $500,000','500K–1M':'$500,001 - $1,000,000',
      '1M–5M':'$1,000,001 - $5,000,000','5M–25M':'$5,000,001 - $25,000,000'};
    const sizeMatch = row.match(/(\d+K[–-]\d+[KM]|\d+M[–-]\d+M)/);
    const amtRange = sizeMatch ? (sizeMap[sizeMatch[1]] || sizeMatch[1]) : null;

    // Owner
    const ownerMatch = row.match(/\b(Self|Spouse|Joint|Dependent|Undisclosed)\b/i);
    const owner = ownerMatch ? ownerMatch[1] : 'Self';

    rows.push({
      chamber, member, ticker,
      asset_description: ticker,
      transaction_type: txType,
      transaction_date: txDate,
      disclosure_date: discDate,
      amount_range: amtRange,
      owner,
      filing_url: `https://www.capitoltrades.com/trades/${doc_id}`,
      doc_id,
    });
  }

  return rows;
}

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  try {
    const total = await fetchCapitolTrades();
    console.log(`[congress] Done. Capitol Trades total new: ${total}`);
    db.close();
    process.exit(0);
  } catch(e) {
    console.error('[congress] FATAL:', e.message);
    process.exit(1);
  }
})();
