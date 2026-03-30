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

// ── Parse a House PTR HTML/PDF ────────────────────────────────────────────────
// The "PDF" is actually rendered HTML with consistent structure.
// Tickers appear as: "Company Name (TICK) [ST]" or "Company Name (TICK)"
// Transaction type: "P" or "S" or "E" in a table cell
// Dates: MM/DD/YYYY format
// Amount: "$1,001 - $15,000" ranges
function parseHousePTR(html, member, docId, filingUrl) {
  const rows = [];

  // Extract all ticker mentions: text like "(AAPL)" or "(AAPL) [ST]"
  // Each ticker block in the PDF has: asset name, transaction type, date, amount
  // The HTML uses table rows — extract each transaction row

  // Strategy: find all table rows that contain a ticker symbol
  // Tickers are in parens: /\(([A-Z]{1,5})\)(?:\s*\[ST\])?/
  // Each row also has: type (P/S/E/sale/purchase), date MM/DD/YYYY, amount range

  // Split into lines for easier parsing
  const lines = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').split(/(?=\$[\d,]+)/);
  const fullText = html.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&#\d+;/g, ' ').replace(/\s+/g, ' ');

  // Extract disclosure date from "Digitally Signed: ... , MM/DD/YYYY"
  const signedMatch = fullText.match(/Digitally Signed:[^,]+,\s*(\d{1,2}\/\d{1,2}\/\d{4})/);
  const disclosureDate = signedMatch ? normDate(signedMatch[1]) : null;

  // Find all transaction blocks
  // Pattern: asset name with ticker → transaction type → dates → amount range
  // Regex to find ticker symbol
  const tickerRe = /([^(]{3,80})\(([A-Z]{1,5})\)(?:\s*\[(?:ST|OP|DC|CS|MF|ET|PS|AS)\])?\s*/g;

  let match;
  while ((match = tickerRe.exec(fullText)) !== null) {
    const assetDesc = match[1].trim().replace(/\s+/g, ' ').slice(0, 150);
    const ticker    = match[2];

    // Skip obvious non-tickers
    if (['EST', 'LLC', 'INC', 'ETF', 'THE', 'FOR', 'AND', 'ARE', 'NOT'].includes(ticker)) continue;
    if (ticker.length < 1 || ticker.length > 5) continue;

    // Look ahead in the text after this ticker for type + date + amount
    const after = fullText.slice(match.index + match[0].length, match.index + match[0].length + 400);

    // Transaction type: look for P, S, Purchase, Sale, Exchange
    let txType = null;
    const typeMatch = after.match(/\b(Purchase|Sale|Exchange|P|S|E)\b/i);
    if (typeMatch) txType = normType(typeMatch[1]);
    if (!txType) continue; // can't determine type, skip

    // Transaction date: MM/DD/YYYY
    const dateMatch = after.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
    const txDate = dateMatch ? normDate(dateMatch[1]) : null;

    // Amount range: $X - $Y or $X,XXX - $X,XXX
    const amtMatch = after.match(/(\$[\d,]+\s*-\s*\$[\d,]+|\$[\d,]+\+?)/);
    const amtRange = amtMatch ? amtMatch[1].replace(/\s+/g, ' ') : null;

    // Owner (Self/Spouse/Joint/Dependent)
    const ownerMatch = after.match(/\b(Self|Spouse|Joint|Dependent|SP|JT|DC)\b/i);
    const owner = ownerMatch ? ownerMatch[1] : 'Self';

    rows.push({
      chamber:          'H',
      member,
      ticker,
      asset_description: assetDesc.slice(0, 200),
      transaction_type:  txType,
      transaction_date:  txDate,
      disclosure_date:   disclosureDate,
      amount_range:      amtRange ? amtRange.slice(0, 50) : null,
      owner:             owner.slice(0, 20),
      filing_url:        filingUrl,
      doc_id:            docId,
    });
  }

  return rows;
}

// ── Parse a Senate PTR ────────────────────────────────────────────────────────
function parseSenatePTR(html, member, docId, filingUrl) {
  const rows = [];
  const fullText = html.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ');

  const signedMatch = fullText.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
  const disclosureDate = signedMatch ? normDate(signedMatch[1]) : null;

  const tickerRe = /([^(]{3,80})\(([A-Z]{1,5})\)(?:\s*\[(?:ST|OP|DC|CS|MF|ET|PS|AS)\])?\s*/g;
  let match;
  while ((match = tickerRe.exec(fullText)) !== null) {
    const assetDesc = match[1].trim().slice(0, 150);
    const ticker    = match[2];
    if (['EST','LLC','INC','ETF','THE','FOR','AND','ARE','NOT','USA','SEC'].includes(ticker)) continue;
    if (ticker.length < 1 || ticker.length > 5) continue;

    const after = fullText.slice(match.index + match[0].length, match.index + match[0].length + 400);
    const typeMatch = after.match(/\b(Purchase|Sale|Exchange|P|S|E)\b/i);
    const txType = typeMatch ? normType(typeMatch[1]) : null;
    if (!txType) continue;

    const dateMatch = after.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
    const txDate = dateMatch ? normDate(dateMatch[1]) : null;
    const amtMatch = after.match(/(\$[\d,]+\s*-\s*\$[\d,]+|\$[\d,]+\+?)/);
    const amtRange = amtMatch ? amtMatch[1].replace(/\s+/g, ' ') : null;
    const ownerMatch = after.match(/\b(Self|Spouse|Joint|Dependent|SP|JT|DC)\b/i);

    rows.push({
      chamber: 'S', member, ticker,
      asset_description: assetDesc.slice(0, 200),
      transaction_type: txType,
      transaction_date: txDate,
      disclosure_date:  disclosureDate,
      amount_range:     amtRange ? amtRange.slice(0, 50) : null,
      owner:            ownerMatch ? ownerMatch[1].slice(0,20) : 'Self',
      filing_url:       filingUrl,
      doc_id:           docId,
    });
  }
  return rows;
}

// ── Insert helper ─────────────────────────────────────────────────────────────
const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO gov_trades
    (chamber, member, ticker, asset_description, transaction_type,
     transaction_date, disclosure_date, amount_range, owner, filing_url, doc_id)
  VALUES
    (@chamber, @member, @ticker, @asset_description, @transaction_type,
     @transaction_date, @disclosure_date, @amount_range, @owner, @filing_url, @doc_id)
`);
const insertMany = db.transaction(rows => {
  let n = 0;
  for (const r of rows) { const info = insertStmt.run(r); n += info.changes; }
  return n;
});

// ── HOUSE: fetch XML index, find new PTR filings ──────────────────────────────
async function fetchHouse() {
  const year = new Date().getFullYear();
  // House publishes a ZIP file containing an XML index of all filings
  const zipUrl = `https://disclosures-clerk.house.gov/public_disc/financial-disclosure-pdfs/${year}FD.zip`;
  console.log(`[congress] House: fetching ZIP index for ${year}...`);

  let xmlBody;
  try {
    const { status, body } = await get(zipUrl, 60000);
    if (status !== 200) throw new Error(`ZIP index returned HTTP ${status}`);
    // Parse ZIP using Node built-ins — no external library needed
    // ZIP local file header: signature 0x04034b50, then fixed fields, then filename, then data
    const buf = Buffer.isBuffer(body) ? body : Buffer.from(body, 'binary');
    const zlib = require('zlib');
    let extracted = false;
    let offset = 0;
    while (offset < buf.length - 4) {
      if (buf.readUInt32LE(offset) !== 0x04034b50) { offset++; continue; }
      const compression = buf.readUInt16LE(offset + 8);
      const compSize    = buf.readUInt32LE(offset + 18);
      const uncompSize  = buf.readUInt32LE(offset + 22);
      const fnameLen    = buf.readUInt16LE(offset + 26);
      const extraLen    = buf.readUInt16LE(offset + 28);
      const fname       = buf.slice(offset + 30, offset + 30 + fnameLen).toString('utf8');
      const dataStart   = offset + 30 + fnameLen + extraLen;
      const dataEnd     = dataStart + compSize;
      if (fname.endsWith('.xml') || fname.endsWith('.XML')) {
        const compressed = buf.slice(dataStart, dataEnd);
        if (compression === 0) {
          xmlBody = compressed.toString('utf8'); // stored uncompressed
        } else if (compression === 8) {
          xmlBody = zlib.inflateRawSync(compressed).toString('utf8');
        }
        extracted = true;
        break;
      }
      offset = dataEnd;
    }
    if (!extracted) throw new Error('No XML found in ZIP');
  } catch(e) {
    console.warn(`[congress] House fetch failed: ${e.message}`);
    return 0;
  }

  // Parse XML: <Member><Prefix>Hon.</Prefix><Last>Pelosi</Last><First>Nancy</First>...
  // <FilingDate>01/17/2025</FilingDate><DocID>20026590</DocID><FilingType>PTR</FilingType>
  const filingRe = /<Row>([\s\S]*?)<\/Row>/g;
  const newFilings = [];
  let xmlMatch;

  while ((xmlMatch = filingRe.exec(xmlBody)) !== null) {
    const row = xmlMatch[1];
    const get = tag => { const m = row.match(new RegExp(`<${tag}>([^<]*)</${tag}>`)); return m ? m[1].trim() : ''; };
    const filingType = get('FilingType');
    if (filingType !== 'PTR') continue; // only Periodic Transaction Reports

    const docId = get('DocID');
    if (!docId || seenDocs.has(docId)) continue;

    const last  = get('Last');
    const first = get('First');
    const member = `${first} ${last}`.trim() || get('OfficerName') || 'Unknown';
    const filingDate = get('FilingDate');

    newFilings.push({ docId, member, filingDate });
  }

  console.log(`[congress] House: ${newFilings.length} new PTR filings to process`);

  let totalInserted = 0;
  for (const filing of newFilings) {
    const pdfUrl = `https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/${year}/${filing.docId}.pdf`;
    try {
      await sleep(500); // polite delay
      const { status, body } = await get(pdfUrl, 20000);
      if (status !== 200) { console.warn(`[congress] House PDF ${filing.docId}: HTTP ${status}`); continue; }

      const rows = parseHousePTR(body, filing.member, filing.docId, pdfUrl);
      if (rows.length) {
        const inserted = insertMany(rows);
        totalInserted += inserted;
        if (inserted > 0) console.log(`[congress] House ${filing.member} (${filing.docId}): ${inserted} trades inserted`);
      }
      seenDocs.add(filing.docId);
    } catch(e) {
      console.warn(`[congress] House ${filing.docId} error: ${e.message}`);
    }
  }

  return totalInserted;
}

// ── SENATE: use efts.senate.gov search for recent PTRs ───────────────────────
async function fetchSenate() {
  // The Senate eFD search returns JSON of recent PTR filings
  const today = new Date().toISOString().slice(0,10);
  const cutoff = new Date(Date.now() - 60 * 86400000).toISOString().slice(0,10); // last 60 days
  // Senate eFD search endpoint — returns JSON list of PTR filings
  const searchUrl = `https://efts.senate.gov/LATEST/search-index?q=%22Periodic+Transaction+Report%22&dateRange=custom&fromDate=${cutoff}&toDate=${today}&resultsPerPage=100`;

  console.log(`[congress] Senate: fetching recent PTRs...`);
  let data;
  try {
    const { status, body } = await get(searchUrl, 30000);
    if (status !== 200) throw new Error(`Senate search returned HTTP ${status}`);
    data = JSON.parse(body);
  } catch(e) {
    console.warn(`[congress] Senate search failed: ${e.message}`);
    return 0;
  }

  const hits = data.hits?.hits || [];
  console.log(`[congress] Senate: ${hits.length} recent PTR docs found`);

  let totalInserted = 0;
  for (const hit of hits) {
    const src = hit._source || {};
    const docId = hit._id || src.docId || '';
    if (!docId || seenDocs.has(docId)) continue;

    const member = `${src.first_name||''} ${src.last_name||''}`.trim() || 'Unknown';
    // Senate PTR PDFs are at efts.senate.gov
    const pdfUrl = src.url || src.link || `https://efts.senate.gov/LATEST/search-index?id=${docId}`;

    try {
      await sleep(500);
      const { status, body } = await get(pdfUrl, 20000);
      if (status !== 200) { console.warn(`[congress] Senate doc ${docId}: HTTP ${status}`); continue; }

      const rows = parseSenatePTR(body, member, docId, pdfUrl);
      if (rows.length) {
        const inserted = insertMany(rows);
        totalInserted += inserted;
        if (inserted > 0) console.log(`[congress] Senate ${member} (${docId}): ${inserted} trades inserted`);
      }
      seenDocs.add(docId);
    } catch(e) {
      console.warn(`[congress] Senate ${docId} error: ${e.message}`);
    }
  }

  return totalInserted;
}

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  try {
    const [houseN, senateN] = await Promise.allSettled([
      fetchHouse(),
      fetchSenate(),
    ]).then(results => results.map(r => r.status === 'fulfilled' ? r.value : 0));

    const total = (houseN || 0) + (senateN || 0);
    console.log(`[congress] Done. House: ${houseN} | Senate: ${senateN} | Total new: ${total}`);
    db.close();
    process.exit(0);
  } catch(e) {
    console.error('[congress] FATAL:', e.message);
    process.exit(1);
  }
})();
