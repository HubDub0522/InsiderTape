'use strict';

// daily-worker.js
// Polls EDGAR's live RSS feed for Form 4 filings — same source OpenInsider uses.
// Much lower latency than EFTS search (~2-5 min vs ~60 min).
// Also runs a daily backfill via EFTS to catch anything the RSS missed.

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
  CREATE TABLE IF NOT EXISTS seen_accessions (
    accession TEXT PRIMARY KEY,
    seen_at TEXT DEFAULT (datetime('now'))
  );
`);

const insertTrade = db.prepare(`
  INSERT OR IGNORE INTO trades (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);
const doInsert = db.transaction(rows => {
  let n = 0;
  for (const r of rows) n += insertTrade.run(r).changes;
  return n;
});

const markSeen   = db.prepare(`INSERT OR IGNORE INTO seen_accessions (accession) VALUES (?)`);
const hasSeen    = db.prepare(`SELECT 1 FROM seen_accessions WHERE accession = ?`);
const cleanSeen  = db.prepare(`DELETE FROM seen_accessions WHERE seen_at < datetime('now', '-3 days')`);

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`); }

// ── Rate-limited GET — stay under SEC's 10 req/sec ──────────────
const reqTimes = [];
async function get(url, ms = 20000) {
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
  return (yr >= 2000 && yr <= 2030) ? d : null;
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

// ── Fetch and parse a Form 4 XML given accession + candidate CIKs ──
async function fetchForm4(accession, filingDate, xmlFile, ciks) {
  const acc     = accession.replace(/-/g, '');
  const accDash = accession;
  const filerCik = parseInt(acc.slice(0, 10), 10).toString();
  const allCiks  = [...new Set([...(ciks || []).map(k => parseInt(k,10).toString()), filerCik])];

  // Best path: direct xmlFile URL with each CIK
  if (xmlFile) {
    for (const cik of allCiks) {
      try {
        const url = `https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlFile}`;
        const { status, body } = await get(url);
        if (status === 200 && body.includes('ownershipDocument'))
          return parseForm4(body, filingDate, accession);
      } catch(e) {}
    }
  }

  // Try index.json with each candidate CIK
  for (const cik of allCiks) {
    try {
      const idxUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${accDash}-index.json`;
      const { status, body } = await get(idxUrl);
      if (status === 200) {
        const idx = JSON.parse(body);
        const xmlDoc = (idx.documents || []).find(d =>
          d.document?.match(/\.xml$/i) && (d.type === '4' || d.type === '4/A' || !d.type)
        ) || (idx.documents || []).find(d => d.document?.match(/\.xml$/i));
        if (xmlDoc) {
          const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlDoc.document}`;
          const { status: xs, body: xml } = await get(xmlUrl);
          if (xs === 200 && xml.includes('ownershipDocument'))
            return parseForm4(xml, filingDate, accession);
        }
      }
    } catch(e) {}
  }

  // Fallback: common filename patterns
  for (const cik of allCiks) {
    for (const name of [`${accDash}.xml`, 'form4.xml', 'wf-form4.xml']) {
      try {
        const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${name}`);
        if (status === 200 && body.includes('ownershipDocument'))
          return parseForm4(body, filingDate, accession);
      } catch(e) {}
    }
  }
  return [];
}

// ── EDGAR RSS feed — reflects filings within ~2-5 minutes ──────────
// Returns array of { accession, xmlFile, ciks, filingDate }
async function pollRSS() {
  // Fetch up to 40 most recent Form 4 + 4/A filings
  const url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&output=atom';
  const { status, body } = await get(url, 30000);
  if (status !== 200) throw new Error(`RSS HTTP ${status}`);

  const filings = [];
  // Each <entry> contains the filing index URL we need
  const entryRe = /<entry>([\s\S]*?)<\/entry>/gi;
  let m;
  while ((m = entryRe.exec(body))) {
    const entry = m[1];

    // Filing date
    const dateMatch = entry.match(/<updated>(\d{4}-\d{2}-\d{2})/);
    const filingDate = dateMatch ? dateMatch[1] : new Date().toISOString().slice(0,10);

    // Index URL looks like: https://www.sec.gov/Archives/edgar/data/1234567/000123456726000001/0001234567-26-000001-index.htm
    const linkMatch = entry.match(/https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/(\d+)\/([\d]+)\//);
    if (!linkMatch) continue;

    const cik      = linkMatch[1];
    const accRaw   = linkMatch[2]; // 18-digit no-dash accession
    // Convert 18-digit to dashed format
    const accDash  = `${accRaw.slice(0,10)}-${accRaw.slice(10,12)}-${accRaw.slice(12)}`;

    filings.push({ accession: accDash, xmlFile: null, ciks: [cik], filingDate });
  }
  return filings;
}

// ── EFTS search — for backfill, covers anything RSS missed ────────
async function searchEFTS(startDate, endDate) {
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
        const raw     = h._id || '';
        const colonAt = raw.indexOf(':');
        const accDash = colonAt >= 0 ? raw.slice(0, colonAt) : raw.replace(/\//g, '-');
        const xmlFile = colonAt >= 0 ? raw.slice(colonAt + 1) : null;
        const fd      = parseDate(h._source?.file_date) || endDate;
        const ciks    = h._source?.ciks || [];
        if (accDash.match(/^\d{10}-\d{2}-\d{6}$/))
          filings.push({ accession: accDash, xmlFile, ciks, filingDate: fd });
      }
      if (hits.length < 100) break;
    } catch(e) { log(`EFTS error: ${e.message}`); break; }
  }
  return filings;
}

// ── Process a batch of filings, skipping already-seen accessions ──
async function processBatch(filings, label) {
  const unseen = filings.filter(f => !hasSeen.get(f.accession));
  if (!unseen.length) return 0;
  log(`${label}: ${unseen.length} new filings to process`);

  let inserted = 0;
  const CONCURRENCY = 4;
  for (let i = 0; i < unseen.length; i += CONCURRENCY) {
    const batch = unseen.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map(f => fetchForm4(f.accession, f.filingDate, f.xmlFile, f.ciks))
    );
    for (let j = 0; j < results.length; j++) {
      const f = batch[j];
      markSeen.run(f.accession);
      if (results[j].status === 'fulfilled' && results[j].value?.length) {
        inserted += doInsert(results[j].value);
      }
    }
  }
  return inserted;
}

// ── RSS poll: run every 2 minutes ─────────────────────────────────
async function runRSSPoll() {
  try {
    const filings = await pollRSS();
    const inserted = await processBatch(filings, 'RSS');
    if (inserted > 0) log(`RSS poll: inserted ${inserted} new trades`);
  } catch(e) {
    log(`RSS poll error: ${e.message}`);
  }
}

// ── Daily EFTS backfill: run once a day to catch any RSS misses ───
async function runBackfill(daysBack) {
  const today = new Date().toISOString().slice(0, 10);
  const start = new Date();
  start.setDate(start.getDate() - daysBack);
  const startDate = start.toISOString().slice(0, 10);

  log(`Backfill: ${startDate} → ${today}`);
  try {
    const filings  = await searchEFTS(startDate, today);
    const inserted = await processBatch(filings, 'Backfill');
    log(`Backfill done: ${inserted} new trades from ${filings.length} filings`);

    // Update daily_log
    const mark = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
    mark.run(today, filings.length, inserted);
  } catch(e) {
    log(`Backfill error: ${e.message}`);
  }
}

// ── MAIN ─────────────────────────────────────────────────────────
const daysBack = parseInt(process.argv[2] || '3');
const mode     = process.argv[3] || 'poll'; // 'poll' = continuous RSS, 'backfill' = one-shot EFTS

async function main() {
  log(`=== daily-worker start (mode=${mode}, daysBack=${daysBack}) ===`);
  cleanSeen.run(); // prune old seen_accessions

  if (mode === 'backfill') {
    // One-shot backfill via EFTS, then exit
    await runBackfill(daysBack);
    db.close();
    process.exit(0);
  }

  // Continuous RSS polling mode
  // 1. Do an initial backfill first to catch up
  await runBackfill(daysBack);

  // 2. Then poll RSS every 2 minutes indefinitely
  log('Starting RSS poll loop (every 2 min)...');
  await runRSSPoll();
  setInterval(runRSSPoll, 2 * 60 * 1000);

  // 3. Do a full backfill once a day at midnight to catch any gaps
  setInterval(async () => {
    const h = new Date().getUTCHours();
    if (h === 0) await runBackfill(2);
  }, 60 * 60 * 1000); // check every hour
}

main().catch(e => {
  log(`FATAL: ${e.message}\n${e.stack}`);
  db.close();
  process.exit(1);
});
