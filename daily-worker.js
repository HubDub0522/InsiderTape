'use strict';

// daily-worker.js - uses EDGAR EFTS search + XML fetching for recent Form 4s

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

let lastReq = 0;
function get(url, ms = 20000) {
  return new Promise((resolve, reject) => {
    const delay = Math.max(0, 120 - (Date.now() - lastReq));
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
      req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    }, delay);
  });
}

function parseDate(s) {
  if (!s) return null;
  const d = s.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null;
  const yr = parseInt(d.slice(0,4));
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
    rows.push([ticker, company, insider, title, date, filingDate, code, qty, +price.toFixed(4), Math.round(qty*price), owned, accession]);
  }

  let m;
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/gi;
  const dRe  = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/gi;
  while ((m = ndRe.exec(xml))) parseBlock(m[1]);
  while ((m = dRe.exec(xml)))  parseBlock(m[1]);
  return rows;
}

async function fetchForm4(cik, accession, filingDate) {
  const accNoDash = accession.replace(/-/g, '');
  
  // Get index JSON to find actual XML filename
  try {
    const idxUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accNoDash}/${accession}-index.json`;
    const { status, body } = await get(idxUrl);
    if (status === 200) {
      const idx = JSON.parse(body);
      const xmlDoc = (idx.documents || []).find(d =>
        (d.type === '4' || d.type === '4/A') && d.document?.match(/\.xml$/i)
      );
      if (xmlDoc) {
        const { status: xs, body: xml } = await get(
          `https://www.sec.gov/Archives/edgar/data/${cik}/${accNoDash}/${xmlDoc.document}`
        );
        if (xs === 200 && xml.includes('ownershipDocument'))
          return parseForm4(xml, filingDate, accession);
      }
    }
  } catch(e) {}

  // Fallback patterns
  for (const name of [`${accession}.xml`, 'form4.xml', 'wf-form4.xml']) {
    try {
      const { status, body } = await get(
        `https://www.sec.gov/Archives/edgar/data/${cik}/${accNoDash}/${name}`
      );
      if (status === 200 && body.includes('ownershipDocument'))
        return parseForm4(body, filingDate, accession);
    } catch(e) {}
  }
  return [];
}

async function run(daysBack) {
  log(`=== daily-worker start (${daysBack} days) ===`);

  // Get date range
  const dates = [];
  const today = new Date();
  for (let i = 0; i <= daysBack + 2; i++) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    if (d.getDay() >= 1 && d.getDay() <= 5) dates.push(d.toISOString().slice(0,10));
    if (dates.length >= daysBack) break;
  }

  const done = new Set(db.prepare('SELECT date FROM daily_log').all().map(r => r.date));
  const todo = dates.filter(d => !done.has(d));
  if (!todo.length) { log('All dates already synced'); return; }

  const startDate = todo[todo.length - 1];
  const endDate   = todo[0];
  log(`Date range: ${startDate} → ${endDate} (${todo.length} days)`);

  // EFTS search - paginate up to 500 results
  let allFilings = [];
  for (let from = 0; from < 500; from += 100) {
    const url = `https://efts.sec.gov/LATEST/search-index?forms=4,4%2FA&dateRange=custom&startdt=${startDate}&enddt=${endDate}&from=${from}&size=100`;
    try {
      const { status, body } = await get(url, 30000);
      if (status !== 200) { log(`EFTS ${status}: ${body.slice(0,80)}`); break; }
      const data = JSON.parse(body);
      const hits = data.hits?.hits || [];
      if (!hits.length) break;
      for (const h of hits) {
        const acc = (h._id || '').replace(/\//g, '-');
        const cik = String(h._source?.entity_id || h._source?.ciks?.[0] || '').replace(/^0+/, '');
        const fd  = h._source?.file_date || endDate;
        if (acc && cik) allFilings.push({ accession: acc, cik, filingDate: fd });
      }
      if (hits.length < 100) break;
    } catch(e) { log(`EFTS error: ${e.message}`); break; }
  }

  log(`Found ${allFilings.length} Form 4 filings to process`);
  if (!allFilings.length) {
    const mark = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
    db.transaction(() => todo.forEach(d => mark.run(d, 0, 0)))();
    return;
  }

  // Process in batches of 5
  let totalInserted = 0;
  const BATCH = 5;
  for (let i = 0; i < allFilings.length; i += BATCH) {
    const batch = allFilings.slice(i, i + BATCH);
    const results = await Promise.allSettled(batch.map(f => fetchForm4(f.cik, f.accession, f.filingDate)));
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value.length) totalInserted += doInsert(r.value);
    }
    if ((i + BATCH) % 50 === 0) log(`  ${i + BATCH}/${allFilings.length} processed, ${totalInserted} trades`);
  }

  log(`Inserted ${totalInserted} trades from ${allFilings.length} filings`);

  const mark = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
  db.transaction(() => todo.forEach(d => mark.run(d, allFilings.length, totalInserted)))();
}

run(parseInt(process.argv[2] || '10'))
  .then(() => {
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    log(`=== done — ${n.toLocaleString()} total trades ===`);
    db.close();
    process.exit(0);
  })
  .catch(e => {
    log(`FATAL: ${e.message}`);
    db.close();
    process.exit(1);
  });
