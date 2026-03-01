'use strict';

// daily-worker.js
// Fetches recent Form 4s using EDGAR full-text search, then retrieves
// each filing via the correct Archives path using the accession number.

const https    = require('https');
const fs       = require('fs');
const path     = require('path');
const Database = require('better-sqlite3');

const DATA_DIR = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH = path.join(DATA_DIR, 'trades.db');

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
    trade_date TEXT NOT NULL, filing_date TEXT,
    type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER, accession TEXT,
    UNIQUE(accession, insider, trade_date, type, qty)
  );
  CREATE INDEX IF NOT EXISTS idx_ticker      ON trades(ticker);
  CREATE INDEX IF NOT EXISTS idx_trade_date  ON trades(trade_date DESC);
  CREATE INDEX IF NOT EXISTS idx_filing_date ON trades(filing_date DESC);
  CREATE INDEX IF NOT EXISTS idx_insider     ON trades(insider);
  CREATE TABLE IF NOT EXISTS daily_log (
    date TEXT PRIMARY KEY,
    synced_at TEXT DEFAULT (datetime('now')),
    filings INTEGER,
    trades INTEGER
  );
`);

const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO trades
    (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);
const doInsert = db.transaction(rows => {
  let n = 0;
  for (const r of rows) { const i = insertStmt.run(r); n += i.changes; }
  return n;
});

function log(msg) {
  process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`);
}

// Rate-limited GET — ~8 req/sec max
let lastReq = 0;
function get(url, ms = 20000) {
  return new Promise((resolve, reject) => {
    const wait = Math.max(0, 125 - (Date.now() - lastReq));
    setTimeout(() => {
      lastReq = Date.now();
      const req = https.get(url, {
        headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
        timeout: ms,
      }, res => {
        if ([301,302,303].includes(res.statusCode) && res.headers.location)
          return get(res.headers.location, ms).then(resolve).catch(reject);
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Timeout: ' + url.slice(0,60))); });
    }, wait);
  });
}

function parseDate(s) {
  if (!s) return null;
  const d = s.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null;
  const yr = parseInt(d.slice(0, 4));
  return (yr >= 2000 && yr <= 2027) ? d : null;
}

function xmlGet(xml, tag) {
  let m = xml.match(new RegExp('<' + tag + '[^>]*>\\s*<value>\\s*([^<]+?)\\s*</value>', 'is'));
  if (m?.[1]?.trim()) return m[1].trim();
  m = xml.match(new RegExp('<' + tag + '[^>]*>\\s*([^<\\s][^<]*?)\\s*</' + tag + '>', 'i'));
  return m?.[1]?.trim() || '';
}

function parseForm4(xml, filingDate, accession) {
  const ticker  = xmlGet(xml, 'issuerTradingSymbol').toUpperCase().trim();
  const company = xmlGet(xml, 'issuerName').trim();
  const insider = xmlGet(xml, 'rptOwnerName').trim();
  const title   = (xmlGet(xml, 'officerTitle') || xmlGet(xml, 'rptOwnerRelationship') || '').trim();
  const period  = parseDate(xmlGet(xml, 'periodOfReport'));
  if (!ticker) return [];

  const rows = [];
  function parseBlock(block) {
    const code  = xmlGet(block, 'transactionCode') || '?';
    const date  = parseDate(xmlGet(block, 'transactionDate')) || period || filingDate;
    if (!date) return;
    const qty   = Math.round(Math.abs(parseFloat(xmlGet(block, 'transactionShares') || xmlGet(block, 'underlyingSecurityShares') || '0') || 0));
    const price = Math.abs(parseFloat(xmlGet(block, 'transactionPricePerShare') || xmlGet(block, 'exercisePrice') || '0') || 0);
    const owned = Math.round(Math.abs(parseFloat(xmlGet(block, 'sharesOwnedFollowingTransaction') || '0') || 0));
    rows.push([ticker, company, insider, title, date, filingDate, code, qty, +price.toFixed(4), Math.round(qty * price), owned, accession]);
  }

  let m;
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/gi;
  const dRe  = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/gi;
  while ((m = ndRe.exec(xml))) parseBlock(m[1]);
  while ((m = dRe.exec(xml)))  parseBlock(m[1]);
  return rows;
}

// Fetch Form 4 XML using the EDGAR filing index
// accession format: 0001234567-26-000001
async function fetchForm4(accession, filingDate) {
  // The issuer CIK is embedded in the accession number (first 10 digits)
  const accNoDash = accession.replace(/-/g, '');
  const filerCik  = accNoDash.slice(0, 10).replace(/^0+/, '');

  // First try: use the filing index JSON to find the XML filename
  const idxUrl = `https://www.sec.gov/Archives/edgar/data/${filerCik}/${accNoDash}/${accession}-index.json`;
  try {
    const { status, body } = await get(idxUrl);
    if (status === 200) {
      const idx    = JSON.parse(body);
      const xmlDoc = (idx.documents || []).find(d =>
        (d.type === '4' || d.type === '4/A') && d.document?.match(/\.xml$/i)
      );
      if (xmlDoc) {
        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${filerCik}/${accNoDash}/${xmlDoc.document}`;
        const { status: xs, body: xml } = await get(xmlUrl);
        if (xs === 200 && xml.includes('ownershipDocument'))
          return parseForm4(xml, filingDate, accession);
      }
    }
  } catch(e) {}

  // Second try: common XML filename patterns
  for (const name of [`${accession}.xml`, 'form4.xml', 'wf-form4.xml', 'xslF345X03/' + accession + '.xml']) {
    try {
      const { status, body } = await get(
        `https://www.sec.gov/Archives/edgar/data/${filerCik}/${accNoDash}/${name}`
      );
      if (status === 200 && body.includes('ownershipDocument'))
        return parseForm4(body, filingDate, accession);
    } catch(e) {}
  }

  return [];
}

// Search EFTS for Form 4 filings in a date range, paginated
async function searchFilings(startDate, endDate) {
  const filings = [];
  for (let from = 0; from < 2000; from += 100) {
    const url = `https://efts.sec.gov/LATEST/search-index?forms=4,4%2FA&dateRange=custom&startdt=${startDate}&enddt=${endDate}&from=${from}&size=100`;
    try {
      const { status, body } = await get(url, 30000);
      if (status !== 200) { log(`EFTS ${status} at offset ${from}`); break; }
      const data = JSON.parse(body);
      const hits = data.hits?.hits || [];
      if (!hits.length) break;
      for (const h of hits) {
        // _id is accession number with slashes e.g. "0001234567-26-000001"
        const acc = (h._id || '').replace(/\//g, '-');
        const fd  = parseDate(h._source?.file_date) || endDate;
        if (acc) filings.push({ accession: acc, filingDate: fd });
      }
      log(`  EFTS offset ${from}: ${hits.length} hits (total so far: ${filings.length})`);
      if (hits.length < 100) break;
    } catch(e) { log(`EFTS error: ${e.message}`); break; }
  }
  return filings;
}

async function run(daysBack) {
  log(`=== daily-worker start (${daysBack} days back) ===`);

  // Build list of business days to process
  const dates = [];
  const today = new Date();
  for (let i = 0; i <= daysBack + 4; i++) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    if (d.getDay() >= 1 && d.getDay() <= 5) {
      dates.push(d.toISOString().slice(0, 10));
    }
    if (dates.length >= daysBack) break;
  }

  const done = new Set(db.prepare('SELECT date FROM daily_log').all().map(r => r.date));
  const todo = dates.filter(d => !done.has(d));

  if (!todo.length) { log('All dates already synced'); return; }

  const startDate = todo[todo.length - 1];
  const endDate   = todo[0];
  log(`Processing ${todo.length} days: ${startDate} → ${endDate}`);

  // Search EFTS
  const filings = await searchFilings(startDate, endDate);
  log(`Found ${filings.length} Form 4 filings total`);

  if (!filings.length) {
    const mark = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
    db.transaction(() => todo.forEach(d => mark.run(d, 0, 0)))();
    return;
  }

  // Process filings in batches of 6 concurrent
  let totalInserted = 0;
  let failures = 0;
  const BATCH = 6;

  for (let i = 0; i < filings.length; i += BATCH) {
    const batch   = filings.slice(i, i + BATCH);
    const results = await Promise.allSettled(
      batch.map(f => fetchForm4(f.accession, f.filingDate))
    );
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value.length) {
        totalInserted += doInsert(r.value);
      } else if (r.status === 'rejected') {
        failures++;
      }
    }
    if ((i + BATCH) % 60 === 0)
      log(`  ${i + BATCH}/${filings.length} processed — ${totalInserted} inserted, ${failures} failures`);
  }

  log(`Done: ${totalInserted} trades from ${filings.length} filings (${failures} failures)`);

  // Mark all processed dates as done
  const mark = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
  db.transaction(() => todo.forEach(d => mark.run(d, filings.length, totalInserted)))();
}

run(parseInt(process.argv[2] || '10'))
  .then(() => {
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    log(`=== daily-worker done — ${n.toLocaleString()} total trades ===`);
    db.close();
    process.exit(0);
  })
  .catch(e => {
    log(`FATAL: ${e.message}`);
    db.close();
    process.exit(1);
  });
