'use strict';

// daily-worker.js
// Fetches Form 4 filings from the SEC EDGAR daily index for recent days
// and parses + inserts them into the same SQLite DB as the quarterly data.
//
// EDGAR daily index URL:
//   https://www.sec.gov/Archives/edgar/daily-index/2026/QTR1/form.idx
// Each line contains: company name | form type | CIK | date | filename
//
// Run: node daily-worker.js [days_back]
// Default: fetches last 10 days of filings

const https    = require('https');
const zlib     = require('zlib');
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
  for (const r of rows) { insertStmt.run(r); n++; }
  return n;
});

function log(msg) {
  process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`);
}

function get(url, ms = 30000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'InsiderTape/1.0 admin@insidertape.com',
        'Accept-Encoding': 'gzip, deflate',
      },
      timeout: ms,
    }, res => {
      if ([301,302,303].includes(res.statusCode) && res.headers.location)
        return get(res.headers.location, ms).then(resolve).catch(reject);

      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        const enc = res.headers['content-encoding'];
        if (enc === 'gzip') {
          zlib.gunzip(buf, (e, d) => e ? reject(e) : resolve({ status: res.statusCode, body: d.toString('utf8') }));
        } else if (enc === 'deflate') {
          zlib.inflate(buf, (e, d) => e ? reject(e) : resolve({ status: res.statusCode, body: d.toString('utf8') }));
        } else {
          resolve({ status: res.statusCode, body: buf.toString('utf8') });
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout: ' + url.slice(0,60))); });
  });
}

// Throttled get — SEC allows 10 req/sec, we stay well under
const queue = [];
let active = 0;
const MAX_CONCURRENT = 4;

function throttledGet(url, ms) {
  return new Promise((resolve, reject) => {
    queue.push({ url, ms, resolve, reject });
    drain();
  });
}

function drain() {
  while (active < MAX_CONCURRENT && queue.length > 0) {
    const { url, ms, resolve, reject } = queue.shift();
    active++;
    // Small random delay to avoid burst
    setTimeout(() => {
      get(url, ms)
        .then(r => { active--; resolve(r); drain(); })
        .catch(e => { active--; reject(e);  drain(); });
    }, Math.random() * 200);
  }
}

function parseDate(s) {
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const yr = parseInt(s.slice(0,4));
    return (yr >= 2000 && yr <= 2027) ? s : null;
  }
  return null;
}

function xmlGet(xml, tag) {
  let m = xml.match(new RegExp('<' + tag + '[^>]*>\\s*<value>\\s*([^<]+?)\\s*</value>', 'is'));
  if (m?.[1]?.trim()) return m[1].trim();
  m = xml.match(new RegExp('<' + tag + '[^>]*>\\s*([^<\\s][^<]*?)\\s*</' + tag + '>', 'i'));
  return m?.[1]?.trim() || '';
}

function parseForm4(xml, filingDate) {
  const ticker  = xmlGet(xml, 'issuerTradingSymbol').toUpperCase().trim();
  const company = xmlGet(xml, 'issuerName').trim();
  const insider = xmlGet(xml, 'rptOwnerName').trim();
  const title   = xmlGet(xml, 'officerTitle').trim();
  const period  = parseDate(xmlGet(xml, 'periodOfReport').slice(0,10));
  if (!ticker) return [];

  const trades = [];
  function parseBlock(block, isDeriv) {
    const code  = xmlGet(block, 'transactionCode') || (isDeriv ? 'A' : 'P');
    const date  = parseDate((xmlGet(block, 'transactionDate') || '').slice(0,10)) || period || filingDate;
    if (!date) return;
    const qty   = Math.round(Math.abs(parseFloat(xmlGet(block, 'transactionShares') || xmlGet(block, 'underlyingSecurityShares') || '0') || 0));
    const price = Math.abs(parseFloat(xmlGet(block, 'transactionPricePerShare') || xmlGet(block, 'exercisePrice') || '0') || 0);
    const owned = Math.round(Math.abs(parseFloat(xmlGet(block, 'sharesOwnedFollowingTransaction') || '0') || 0));
    trades.push([ticker, company, insider, title, date, filingDate, code, qty, +price.toFixed(4), Math.round(qty*price), owned, '']);
  }

  let m;
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
  const dRe  = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/g;
  while ((m = ndRe.exec(xml))) parseBlock(m[1], false);
  while ((m = dRe.exec(xml)))  parseBlock(m[1], true);
  return trades;
}

// Get quarter string for a date: 2026-02-15 → QTR1
function dateToQtr(dateStr) {
  const m = parseInt(dateStr.slice(5,7));
  return 'QTR' + Math.ceil(m/3);
}

// Get list of business days to fetch (skip weekends)
function getBusinessDays(daysBack) {
  const days = [];
  const d = new Date();
  while (days.length < daysBack) {
    d.setDate(d.getDate() - 1);
    const dow = d.getDay();
    if (dow !== 0 && dow !== 6) {
      days.push(d.toISOString().slice(0,10));
    }
    if (days.length >= daysBack * 2) break; // safety
  }
  return days;
}

async function fetchDayIndex(dateStr) {
  // Check if already done
  const already = db.prepare('SELECT 1 FROM daily_log WHERE date=?').get(dateStr);
  if (already) { log(`${dateStr}: already ingested`); return; }

  const yr  = dateStr.slice(0,4);
  const qtr = dateToQtr(dateStr);
  const compact = dateStr.replace(/-/g,''); // 20260215

  // EDGAR daily index — form.idx lists all filings for that date
  const idxUrl = `https://www.sec.gov/Archives/edgar/daily-index/${yr}/${qtr}/form${compact}.idx`;
  log(`${dateStr}: fetching index...`);

  let idxBody;
  try {
    const { status, body } = await throttledGet(idxUrl, 20000);
    if (status === 404) { log(`${dateStr}: no index (holiday/weekend)`); return; }
    if (status !== 200) { log(`${dateStr}: index HTTP ${status}`); return; }
    idxBody = body;
  } catch(e) {
    log(`${dateStr}: index fetch failed — ${e.message}`);
    return;
  }

  // Parse form.idx — fixed-width format, Form 4 lines
  // Format: Company Name           Form  CIK         Date       Filename
  const lines = idxBody.split('\n');
  const form4Lines = lines.filter(l => {
    const formField = l.slice(62, 74).trim();
    return formField === '4' || formField === '4/A';
  });

  log(`${dateStr}: ${form4Lines.length} Form 4 filings found`);
  if (!form4Lines.length) {
    db.prepare('INSERT OR IGNORE INTO daily_log (date,filings,trades) VALUES (?,?,?)').run(dateStr, 0, 0);
    return;
  }

  // Extract filing paths
  const filings = form4Lines.map(l => ({
    company: l.slice(0, 62).trim(),
    date:    l.slice(74, 86).trim(),
    path:    l.slice(86).trim(),
  })).filter(f => f.path);

  // Fetch XMLs in batches
  let totalTrades = 0;
  const BATCH = 8;

  for (let i = 0; i < filings.length; i += BATCH) {
    const batch = filings.slice(i, i + BATCH);
    const results = await Promise.allSettled(batch.map(async f => {
      // path is like: edgar/data/1234567/0001234567-26-000001.txt
      // We need the XML inside the filing
      const indexUrl = `https://www.sec.gov/Archives/${f.path.replace('.txt', '-index.htm')}`;
      try {
        const { status, body } = await throttledGet(indexUrl, 15000);
        if (status !== 200) return 0;

        // Find XML file in index
        const xmlMatch = body.match(/href="(\/Archives\/[^"]+\.xml)"/i);
        if (!xmlMatch) return 0;

        const { status: xs, body: xml } = await throttledGet(`https://www.sec.gov${xmlMatch[1]}`, 15000);
        if (xs !== 200 || !xml.includes('<ownershipDocument>')) return 0;

        const rows = parseForm4(xml, f.date || dateStr);
        if (rows.length) {
          // Set accession from path
          const acc = f.path.match(/(\d{18}|\d{10}-\d{2}-\d{6})/)?.[1] || '';
          rows.forEach(r => { r[11] = acc; });
          doInsert(rows);
          return rows.length;
        }
        return 0;
      } catch(e) { return 0; }
    }));

    totalTrades += results.reduce((s, r) => s + (r.status === 'fulfilled' ? r.value : 0), 0);

    if (i % 40 === 0 && i > 0)
      log(`${dateStr}: processed ${i}/${filings.length}, ${totalTrades} trades so far`);
  }

  db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)').run(dateStr, filings.length, totalTrades);
  log(`${dateStr}: done — ${totalTrades} trades from ${filings.length} filings`);
}

(async () => {
  const daysBack = parseInt(process.argv[2] || '10');
  log(`=== daily-worker start (${daysBack} days back) ===`);

  // Always include today if it's a weekday
  const today = new Date();
  const dow   = today.getDay();
  const dates = [];
  if (dow >= 1 && dow <= 5) dates.push(today.toISOString().slice(0,10));
  dates.push(...getBusinessDays(daysBack));

  // Remove duplicates
  const unique = [...new Set(dates)];
  log(`Fetching ${unique.length} days: ${unique.join(', ')}`);

  for (const date of unique) {
    await fetchDayIndex(date);
  }

  const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
  log(`=== daily-worker done — ${n.toLocaleString()} total trades in DB ===`);
  db.close();
  process.exit(0);
})();
