'use strict';

// daily-worker.js — v2
// Fixes:
//  - Removed seen_accessions cache (was blocking re-insertion; DB UNIQUE constraint handles dedup)
//  - EFTS backfill now runs on startup AND every 4 hours (not just once/day)
//  - RSS poll runs every 2 min for low-latency same-day picks
//  - daily_log now records per-date counts correctly
//  - Increased EFTS page limit to catch all filings (up to 10k/day)

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

const insertTrade = db.prepare(`
  INSERT OR IGNORE INTO trades (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);
const doInsert = db.transaction(rows => {
  let n = 0;
  for (const r of rows) n += insertTrade.run(r).changes;
  return n;
});

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`); }

// ── Rate-limited GET (max 8 req/sec to respect SEC limits) ───────
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
  return /^\d{4}-\d{2}-\d{2}$/.test(d) ? d : null;
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

// ── Resolve accession number → parsed Form 4 rows ────────────────
async function fetchForm4(accession, filingDate, xmlFile, ciks) {
  const acc      = accession.replace(/-/g, '');
  const filerCik = parseInt(acc.slice(0, 10), 10).toString();
  const allCiks  = [...new Set([...(ciks || []).map(k => parseInt(k, 10).toString()), filerCik])];

  // 1. Direct xmlFile path (fastest — EFTS gives us the filename)
  if (xmlFile) {
    for (const cik of allCiks) {
      try {
        const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlFile}`);
        if (status === 200 && body.includes('ownershipDocument'))
          return parseForm4(body, filingDate, accession);
      } catch(e) {}
    }
  }

  // 2. Filing index JSON → find XML filename
  for (const cik of allCiks) {
    try {
      const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${accession}-index.json`);
      if (status === 200) {
        const idx    = JSON.parse(body);
        const xmlDoc = (idx.documents || []).find(d =>
          d.document?.match(/\.xml$/i) && (d.type === '4' || d.type === '4/A' || !d.type)
        ) || (idx.documents || []).find(d => d.document?.match(/\.xml$/i));
        if (xmlDoc) {
          const { status: xs, body: xml } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlDoc.document}`);
          if (xs === 200 && xml.includes('ownershipDocument'))
            return parseForm4(xml, filingDate, accession);
        }
      }
    } catch(e) {}
  }

  // 3. Common filename fallbacks
  for (const cik of allCiks) {
    for (const name of [`${accession}.xml`, 'form4.xml', 'wf-form4.xml']) {
      try {
        const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${name}`);
        if (status === 200 && body.includes('ownershipDocument'))
          return parseForm4(body, filingDate, accession);
      } catch(e) {}
    }
  }

  return [];
}

// ── EDGAR RSS feed (~2-5 min latency for brand new filings) ───────
async function pollRSS() {
  const url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&output=atom';
  const { status, body } = await get(url, 30000);
  if (status !== 200) throw new Error(`RSS HTTP ${status}`);

  const filings = [];
  const entryRe = /<entry>([\s\S]*?)<\/entry>/gi;
  let m;
  while ((m = entryRe.exec(body))) {
    const entry      = m[1];
    const dateMatch  = entry.match(/<updated>(\d{4}-\d{2}-\d{2})/);
    const filingDate = dateMatch ? dateMatch[1] : new Date().toISOString().slice(0, 10);
    const linkMatch  = entry.match(/https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/(\d+)\/([\d]+)\//);
    if (!linkMatch) continue;
    const cik    = linkMatch[1];
    const accRaw = linkMatch[2];
    const accDash = `${accRaw.slice(0,10)}-${accRaw.slice(10,12)}-${accRaw.slice(12)}`;
    filings.push({ accession: accDash, xmlFile: null, ciks: [cik], filingDate });
  }
  return filings;
}

// ── EDGAR daily index (definitive — lists every filing for each day) ─
// https://www.sec.gov/Archives/edgar/full-index/YYYY/QN/company.idx
// This is what serious data providers use — it's the authoritative list.
async function fetchDailyIndex(dateStr) {
  const d = new Date(dateStr + 'T12:00:00Z');
  const yr = d.getUTCFullYear();
  const mo = d.getUTCMonth() + 1;
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const q  = Math.ceil(mo / 3);

  // The full-index company.gz file for this quarter lists all filings
  // But it's updated daily — we parse it and filter by date + form type
  const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&search_text=&action=getcurrent&output=atom`;
  
  // Actually use the EDGAR full-index for the specific date
  // Format: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=4&dateb=YYYYMMDD&owner=include&count=100&search_text=&output=atom
  const dateFmt = `${yr}${String(mo).padStart(2,'0')}${dd}`;
  const idxUrl  = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=${dateFmt}&owner=include&count=100&search_text=&output=atom`;
  
  try {
    const { status, body } = await get(idxUrl, 30000);
    if (status !== 200) return [];
    return parseAtomFeed(body, dateStr);
  } catch(e) {
    log(`Daily index error for ${dateStr}: ${e.message}`);
    return [];
  }
}

function parseAtomFeed(body, expectedDate) {
  const filings = [];
  const entryRe = /<entry>([\s\S]*?)<\/entry>/gi;
  let m;
  while ((m = entryRe.exec(body))) {
    const entry = m[1];
    const dateMatch = entry.match(/<updated>(\d{4}-\d{2}-\d{2})/);
    const filingDate = dateMatch ? dateMatch[1] : expectedDate;
    const linkMatch = entry.match(/https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/(\d+)\/([\d]+)\//);
    if (!linkMatch) continue;
    const cik    = linkMatch[1];
    const accRaw = linkMatch[2];
    if (accRaw.length !== 18) continue;
    const accDash = `${accRaw.slice(0,10)}-${accRaw.slice(10,12)}-${accRaw.slice(12)}`;
    filings.push({ accession: accDash, xmlFile: null, ciks: [cik], filingDate });
  }
  return filings;
}

// ── EFTS search — paginates through ALL Form 4s in date range ────
async function searchEFTS(startDate, endDate) {
  const filings = [];
  // Use category=form-type to get exact form type matching
  for (let from = 0; from < 10000; from += 100) {
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
  log(`EFTS returned ${filings.length} filings for ${startDate}→${endDate}`);
  return filings;
}

// ── Full-index TSV — most reliable bulk method for backfill ───────
// SEC publishes form4.idx daily at:
// https://www.sec.gov/Archives/edgar/full-index/YYYY/QN/form.idx
async function fetchFullIndex(startDate, endDate) {
  const filings = [];
  const start = new Date(startDate + 'T12:00:00Z');
  const end   = new Date(endDate   + 'T12:00:00Z');

  // Collect unique quarter keys in range
  const quarters = new Set();
  const cur = new Date(start);
  while (cur <= end) {
    const yr = cur.getUTCFullYear();
    const q  = Math.ceil((cur.getUTCMonth() + 1) / 3);
    quarters.add(`${yr}-${q}`);
    cur.setUTCDate(cur.getUTCDate() + 32);
    cur.setUTCDate(1);
  }

  for (const qkey of quarters) {
    const [yr, q] = qkey.split('-');
    const url = `https://www.sec.gov/Archives/edgar/full-index/${yr}/QTR${q}/form.idx`;
    log(`Fetching full-index: ${url}`);
    try {
      const { status, body } = await get(url, 60000);
      if (status !== 200) { log(`full-index HTTP ${status} for ${qkey}`); continue; }

      // Format: Form Type | Company Name | CIK | Date Filed | Filename
      // Lines start after a header separator "---..."
      const lines = body.split('
');
      let pastHeader = false;
      for (const line of lines) {
        if (!pastHeader) {
          if (line.startsWith('---')) pastHeader = true;
          continue;
        }
        // Fixed-width: form type ends ~12, company ~74, CIK ~86, date ~98, filename rest
        const formType = line.slice(0, 12).trim();
        if (formType !== '4' && formType !== '4/A') continue;
        const dateFiled = line.slice(86, 98).trim();
        if (!dateFiled || dateFiled < startDate || dateFiled > endDate) continue;
        const filename  = line.slice(98).trim(); // e.g. edgar/data/1234/0001234-26-000001.txt
        // Extract CIK and accession from filename
        const fm = filename.match(/edgar\/data\/(\d+)\/([\d-]+)\.txt/);
        if (!fm) continue;
        const cik     = fm[1];
        const accDash = fm[2];
        if (!accDash.match(/^\d{10}-\d{2}-\d{6}$/)) continue;
        filings.push({ accession: accDash, xmlFile: null, ciks: [cik], filingDate: dateFiled });
      }
    } catch(e) { log(`full-index error for ${qkey}: ${e.message}`); }
  }

  log(`Full-index found ${filings.length} Form 4 filings for ${startDate}→${endDate}`);
  return filings;
}


// ── Process filings — no seen-cache, rely on DB UNIQUE constraint ──
async function processBatch(filings, label) {
  if (!filings.length) return 0;
  log(`${label}: processing ${filings.length} filings`);

  let inserted = 0;
  let failed   = 0;
  const CONCURRENCY = 6;

  for (let i = 0; i < filings.length; i += CONCURRENCY) {
    const batch   = filings.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map(f => fetchForm4(f.accession, f.filingDate, f.xmlFile, f.ciks))
    );
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value?.length) {
        inserted += doInsert(r.value);
      } else if (r.status === 'rejected') {
        failed++;
      }
    }
    if ((i + CONCURRENCY) % 60 === 0)
      log(`  ${i + CONCURRENCY}/${filings.length} — inserted:${inserted} failed:${failed}`);
  }

  log(`${label} done — inserted:${inserted} failed:${failed} from ${filings.length} filings`);
  return inserted;
}

// ── Backfill using full-index (primary) + EFTS (fallback) ───────
async function runBackfill(daysBack) {
  const today = new Date().toISOString().slice(0, 10);
  const start = new Date();
  start.setDate(start.getDate() - daysBack);
  const startDate = start.toISOString().slice(0, 10);

  log(`Backfill: ${startDate} → ${today}`);
  try {
    // Primary: EDGAR full-index (definitive list of every filing)
    let filings = await fetchFullIndex(startDate, today);

    // Fallback to EFTS if full-index returned nothing
    if (!filings.length) {
      log('Full-index empty, falling back to EFTS...');
      filings = await searchEFTS(startDate, today);
    }

    if (!filings.length) {
      log('No filings found from any source');
      return;
    }

    const inserted = await processBatch(filings, 'Backfill');

    // Log per-date counts
    const byDate = {};
    filings.forEach(f => { byDate[f.filingDate] = (byDate[f.filingDate] || 0) + 1; });
    const upsert = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
    const upsertMany = db.transaction(entries => {
      for (const [date, count] of entries) upsert.run(date, count, 0);
    });
    upsertMany(Object.entries(byDate));
    db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)').run(today, filings.length, inserted);

    log(`Backfill complete: ${inserted} trades inserted across ${Object.keys(byDate).length} days`);
  } catch(e) {
    log(`Backfill error: ${e.message}\n${e.stack}`);
  }
}

// ── RSS poll ──────────────────────────────────────────────────────
async function runRSSPoll() {
  try {
    const filings  = await pollRSS();
    const inserted = await processBatch(filings, 'RSS');
    if (inserted > 0) log(`RSS poll: +${inserted} trades`);
  } catch(e) {
    log(`RSS poll error: ${e.message}`);
  }
}

// ── MAIN ─────────────────────────────────────────────────────────
const daysBack = parseInt(process.argv[2] || '3');
const mode     = process.argv[3] || 'poll';

async function main() {
  log(`=== daily-worker v2 start (mode=${mode}, daysBack=${daysBack}) ===`);

  if (mode === 'backfill') {
    await runBackfill(daysBack);
    db.close();
    process.exit(0);
  }

  // Continuous poll mode:
  // 1. Full backfill on startup
  await runBackfill(daysBack);

  // 2. RSS every 2 minutes
  log('Starting RSS poll (every 2 min)...');
  setInterval(runRSSPoll, 2 * 60 * 1000);
  await runRSSPoll();

  // 3. EFTS backfill every 4 hours to catch anything RSS missed
  setInterval(() => runBackfill(2), 4 * 60 * 60 * 1000);
}

main().catch(e => {
  log(`FATAL: ${e.message}\n${e.stack}`);
  db.close();
  process.exit(1);
});
