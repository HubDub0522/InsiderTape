'use strict';

// daily-worker.js
// Strategy: EFTS search gives us accession numbers.
// We resolve each accession to a full filing URL via the EDGAR submissions API,
// which correctly maps accession → issuer CIK → XML path.

const https    = require('https');
const fs       = require('fs');
const path     = require('path');
const Database = require('better-sqlite3');

const DATA_DIR = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH  = path.join(DATA_DIR, 'trades.db');

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
  INSERT OR IGNORE INTO trades (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);
const doInsert = db.transaction(rows => {
  let n = 0;
  for (const r of rows) n += insertStmt.run(r).changes;
  return n;
});

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`); }

// Rate-limited GET — stay under SEC's 10 req/sec limit
const reqTimes = [];
async function get(url, ms = 20000) {
  // Sliding window: no more than 8 requests in any 1-second window
  const now = Date.now();
  while (reqTimes.length && reqTimes[0] < now - 1000) reqTimes.shift();
  if (reqTimes.length >= 8) {
    await new Promise(r => setTimeout(r, 1000 - (now - reqTimes[0]) + 10));
  }
  reqTimes.push(Date.now());

  return new Promise((resolve, reject) => {
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

// Given an accession number, find the XML using the filing index
// The accession number encodes the FILER CIK (first 10 digits) — but we need ISSUER CIK.
// Solution: use EDGAR's /submissions/ API with the accession to find issuer CIK.
// Actually simpler: use the filing index at data.sec.gov which doesn't need CIK in the URL.
async function fetchForm4ByAccession(accession, filingDate, xmlFile, ciks) {
  const acc     = accession.replace(/-/g, '');
  const accDash = accession;

  // CIK candidates: EFTS provides ciks array, plus filer CIK from accession prefix
  const filerCik = parseInt(acc.slice(0, 10), 10).toString();
  const allCiks  = [...new Set([...(ciks || []).map(k => parseInt(k,10).toString()), filerCik])];

  // Best path: EFTS _id includes the filename after colon e.g. "0000021344-26-000038:form4.xml"
  // We already extracted xmlFile — try it directly with each candidate CIK
  if (xmlFile) {
    for (const cik of allCiks) {
      try {
        const url = `https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlFile}`;
        const { status, body } = await get(url);
        if (status === 200 && body.includes('ownershipDocument'))
          return { rows: parseForm4(body, filingDate, accession), method: 'direct-xmlfile' };
      } catch(e) {}
    }
  }


  // Use the -index.json which always exists and has the document list
  try {
    const idxUrl = `https://www.sec.gov/Archives/edgar/data/${filerCik}/${acc}/${accDash}-index.json`;
    const { status, body } = await get(idxUrl);
    if (status === 200) {
      const idx = JSON.parse(body);
      // Find the Form 4 XML document
      const xmlDoc = (idx.documents || []).find(d =>
        d.document?.match(/\.xml$/i) &&
        (d.type === '4' || d.type === '4/A' || !d.type)
      );
      if (xmlDoc) {
        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${filerCik}/${acc}/${xmlDoc.document}`;
        const { status: xs, body: xml } = await get(xmlUrl);
        if (xs === 200 && xml.includes('ownershipDocument'))
          return { rows: parseForm4(xml, filingDate, accession), method: 'index-json' };
      }
      // If no XML type found, try any .xml file
      const anyXml = (idx.documents || []).find(d => d.document?.match(/\.xml$/i));
      if (anyXml) {
        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${filerCik}/${acc}/${anyXml.document}`;
        const { status: xs, body: xml } = await get(xmlUrl);
        if (xs === 200 && xml.includes('ownershipDocument'))
          return { rows: parseForm4(xml, filingDate, accession), method: 'index-json-anyxml' };
      }
    }
    // If index not found under filer CIK, the filing might be under a different CIK
    // Try using the EDGAR full text search to get the actual filing URL
    if (status === 404) {
      // Use EDGAR submissions search to find CIK by accession
      const searchUrl = `https://efts.sec.gov/LATEST/search-index?q=%22${accDash}%22&forms=4,4%2FA&from=0&size=1`;
      const { status: ss, body: sb } = await get(searchUrl);
      if (ss === 200) {
        const data = JSON.parse(sb);
        const hit  = data.hits?.hits?.[0];
        if (hit) {
          const ciks = hit._source?.ciks || [];
          for (const cik of ciks) {
            const cikInt = parseInt(cik, 10).toString();
            const u = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${acc}/${accDash}-index.json`;
            const { status: cs, body: cb } = await get(u);
            if (cs === 200) {
              const idx2   = JSON.parse(cb);
              const xmlDoc = (idx2.documents || []).find(d => d.document?.match(/\.xml$/i));
              if (xmlDoc) {
                const { status: xs, body: xml } = await get(
                  `https://www.sec.gov/Archives/edgar/data/${cikInt}/${acc}/${xmlDoc.document}`
                );
                if (xs === 200 && xml.includes('ownershipDocument'))
                  return { rows: parseForm4(xml, filingDate, accession), method: 'cik-search' };
              }
            }
          }
        }
      }
    }
  } catch(e) {
    return { rows: [], error: e.message };
  }

  // Strategy 3: Try common XML filename patterns directly
  for (const name of [`${accDash}.xml`, 'form4.xml', 'wf-form4.xml']) {
    try {
      const { status, body } = await get(
        `https://www.sec.gov/Archives/edgar/data/${filerCik}/${acc}/${name}`
      );
      if (status === 200 && body.includes('ownershipDocument'))
        return { rows: parseForm4(body, filingDate, accession), method: 'fallback-' + name };
    } catch(e) {}
  }

  return { rows: [], error: 'not-found' };
}

// Search EFTS for Form 4 filings in date range
async function searchFilings(startDate, endDate) {
  const filings = [];
  for (let from = 0; from < 2000; from += 100) {
    const url = `https://efts.sec.gov/LATEST/search-index?forms=4,4%2FA&dateRange=custom&startdt=${startDate}&enddt=${endDate}&from=${from}&size=100`;
    try {
      const { status, body } = await get(url, 30000);
      if (status !== 200) { log(`EFTS HTTP ${status}`); break; }
      const data = JSON.parse(body);
      const hits = data.hits?.hits || [];
      if (!hits.length) break;

      for (const h of hits) {
        // _id format: "0000021344-26-000038:form4.xml"
        // accession = part before colon, xmlFile = part after colon
        const raw     = h._id || '';
        const colonAt = raw.indexOf(':');
        const accDash = colonAt >= 0 ? raw.slice(0, colonAt) : raw.replace(/\//g, '-');
        const xmlFile = colonAt >= 0 ? raw.slice(colonAt + 1) : null;
        const fd      = parseDate(h._source?.file_date) || endDate;
        // CIK is in the source
        const ciks    = h._source?.ciks || [];
        if (accDash.match(/^\d{10}-\d{2}-\d{6}$/)) {
          filings.push({ accession: accDash, xmlFile, ciks, filingDate: fd });
        }
      }
      if (hits.length < 100) break;
    } catch(e) { log(`EFTS error: ${e.message}`); break; }
  }
  return filings;
}

async function run(daysBack) {
  log(`=== daily-worker start (${daysBack} days back) ===`);

  // Build business day list
  const dates = [];
  const today = new Date();
  for (let i = 0; i <= daysBack + 5; i++) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    if (d.getDay() >= 1 && d.getDay() <= 5) dates.push(d.toISOString().slice(0, 10));
    if (dates.length >= daysBack) break;
  }

  const done = new Set(db.prepare('SELECT date FROM daily_log WHERE trades > 0').all().map(r => r.date));
  const todo = dates.filter(d => !done.has(d));
  if (!todo.length) { log('All dates already have trades'); return; }

  const startDate = todo[todo.length - 1];
  const endDate   = todo[0];
  log(`Fetching: ${startDate} → ${endDate} (${todo.length} days)`);

  // Search
  const filings = await searchFilings(startDate, endDate);
  log(`Found ${filings.length} filings — sample: ${JSON.stringify(filings.slice(0,3))}`);

  if (!filings.length) {
    const mark = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
    db.transaction(() => todo.forEach(d => mark.run(d, 0, 0)))();
    return;
  }

  // Process concurrently with limit of 4
  let inserted = 0, failures = 0, notFound = 0;
  const BATCH = 4;
  for (let i = 0; i < filings.length; i += BATCH) {
    const batch = filings.slice(i, i + BATCH);
    const results = await Promise.allSettled(batch.map(f => fetchForm4ByAccession(f.accession, f.filingDate, f.xmlFile, f.ciks)));
    for (const r of results) {
      if (r.status === 'fulfilled') {
        if (r.value.rows?.length) inserted += doInsert(r.value.rows);
        else if (r.value.error === 'not-found') notFound++;
        else if (r.value.error) failures++;
      } else failures++;
    }
    if ((i + BATCH) % 40 === 0)
      log(`  ${i+BATCH}/${filings.length} — inserted:${inserted} notFound:${notFound} err:${failures}`);
  }

  log(`DONE — inserted:${inserted} notFound:${notFound} failures:${failures}`);

  const mark = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
  db.transaction(() => todo.forEach(d => mark.run(d, filings.length, inserted)))();
}

run(parseInt(process.argv[2] || '10'))
  .then(() => {
    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    log(`=== total trades in DB: ${n.toLocaleString()} ===`);
    db.close();
    process.exit(0);
  })
  .catch(e => {
    log(`FATAL: ${e.message}\n${e.stack}`);
    db.close();
    process.exit(1);
  });
