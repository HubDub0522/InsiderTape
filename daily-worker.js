'use strict';

// daily-worker.js — v9 (paginated RSS for recent data)
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
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null;
  const yr = parseInt(d.slice(0, 4), 10);
  const today = new Date().toISOString().slice(0, 10);
  if (d > today || yr < 2000) return null;
  return d;
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
    const code  = (xmlGet(block, 'transactionCode') || '').trim();
    // Only store open-market buys (P) and sales (S, S-)
    // All other codes (C=conversion, M=exercise, A=award, G=gift, F=tax withholding, etc.)
    // are not open-market transactions and produce fabricated values when qty*price is computed
    if (!['P', 'S', 'S-'].includes(code)) return;

    const date  = parseDate(xmlGet(block, 'transactionDate')) || period || filingDate;
    if (!date) return;

    const qty   = Math.round(Math.abs(parseFloat(xmlGet(block, 'transactionShares') || '0') || 0));
    const price = Math.abs(parseFloat(xmlGet(block, 'transactionPricePerShare') || '0') || 0);
    const owned = Math.round(Math.abs(parseFloat(xmlGet(block, 'sharesOwnedFollowingTransaction') || '0') || 0));

    // Sanity checks: reject implausible values that indicate parsing errors or derivative noise
    if (qty > 50_000_000) return;       // >50M shares in one trade = likely a conversion/derivative artifact
    if (price > 1_500_000) return;      // >$1.5M/share = above even Berkshire A, likely bad data
    const value = Math.round(qty * price);
    if (value > 2_000_000_000) return;  // >$2B single trade = implausible, cap any edge cases

    rows.push([ticker, company, insider, title, date, filingDate, code, qty, +price.toFixed(4), value, owned, accession]);
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

  // Helper: try fetching XML at a given URL
  async function tryXml(url) {
    try {
      const { status, body } = await get(url);
      if (status === 200 && body.includes('ownershipDocument')) return body;
    } catch(e) {}
    return null;
  }

  // 1. Direct xmlFile path (fastest — EFTS gives us the filename)
  if (xmlFile) {
    for (const cik of allCiks) {
      const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlFile}`);
      if (xml) return parseForm4(xml, filingDate, accession);
    }
  }

  // 2. Use the SGML submission index (.txt) — always accessible, lists all docs
  // URL: https://www.sec.gov/Archives/edgar/data/{CIK}/{ACC}/{ACC}.txt (header file)
  // Better: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum=...
  // Best: fetch the index page directly
  for (const cik of allCiks) {
    try {
      const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${accession}-index.htm`);
      if (status === 200) {
        // Extract XML filename from index HTML
        const xmlMatch = body.match(/href="([^"]+\.xml)"/i);
        if (xmlMatch) {
          const xmlName = xmlMatch[1].split('/').pop();
          const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlName}`);
          if (xml) return parseForm4(xml, filingDate, accession);
        }
      }
    } catch(e) {}

    // Also try the JSON index
    try {
      const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${accession}-index.json`);
      if (status === 200) {
        const idx    = JSON.parse(body);
        const xmlDoc = (idx.documents || []).find(d =>
          d.document?.match(/\.xml$/i) && (d.type === '4' || d.type === '4/A' || !d.type)
        ) || (idx.documents || []).find(d => d.document?.match(/\.xml$/i));
        if (xmlDoc) {
          const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlDoc.document}`);
          if (xml) return parseForm4(xml, filingDate, accession);
        }
      }
    } catch(e) {}
  }

  // 3. Common filename patterns
  for (const cik of allCiks) {
    for (const name of [`${accession}.xml`, 'form4.xml', 'wf-form4.xml', 'xslF345X03/primary_doc.xml']) {
      const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${name}`);
      if (xml) return parseForm4(xml, filingDate, accession);
    }
  }

  return [];
}

// ── EDGAR browse-edgar paginated fetch — gets ALL filings since cutoff ──
// fetchRecentFilings: get all Form 4s filed since sinceDate
// Combines atom feed (low latency) + per-day EFTS (fills gaps)
async function fetchRecentFilings(sinceDate) {
  const seen    = new Set();
  let   filings = [];

  // 1. Atom feed — paginated, low latency
  try {
    const atomFilings = await fetchViaAtom(sinceDate);
    atomFilings.forEach(f => { if (!seen.has(f.accession)) { seen.add(f.accession); filings.push(f); } });
    log(`Atom feed: ${atomFilings.length} filings since ${sinceDate}`);
  } catch(e) {
    log(`Atom feed error: ${e.message}`);
  }

  // 2. EFTS per-day supplement — catches anything atom missed (slight lag but complete)
  const today = new Date().toISOString().slice(0, 10);
  const start = new Date(sinceDate + 'T12:00:00Z');
  const end   = new Date(today     + 'T12:00:00Z');
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    const dateStr = d.toISOString().slice(0, 10);
    try {
      const dayFilings = await searchEFTS(dateStr, dateStr);
      let added = 0;
      dayFilings.forEach(f => { if (!seen.has(f.accession)) { seen.add(f.accession); filings.push(f); added++; } });
      if (added > 0) log(`EFTS ${dateStr}: +${added} filings not in atom feed`);
    } catch(e) {}
  }

  log(`fetchRecentFilings total: ${filings.length} since ${sinceDate}`);
  return filings;
}

async function fetchViaAtom(sinceDate) {
  const filings = [];
  const seen    = new Set();

  for (let start = 0; start < 4000; start += 40) {
    const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&start=${start}&output=atom`;
    const r = await get(url, 30000);
    if (r.status !== 200) {
      log(`Atom HTTP ${r.status} at start=${start}`);
      break;
    }

    const entries = r.body.split('<entry>').slice(1);
    if (start === 0) log(`Atom page 0: ${entries.length} entries, body length ${r.body.length}`);
    if (!entries.length) break;

    let oldestOnPage = '';
    for (const entry of entries) {
      const dateMatch  = entry.match(/<updated>(\d{4}-\d{2}-\d{2})/);
      const filingDate = dateMatch ? dateMatch[1] : new Date().toISOString().slice(0,10);
      const linkMatch  = entry.match(/https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/(\d+)\/([\d]{18})\//);
      if (!linkMatch) continue;
      const cik    = linkMatch[1];
      const accRaw = linkMatch[2];
      const accDash = `${accRaw.slice(0,10)}-${accRaw.slice(10,12)}-${accRaw.slice(12)}`;
      if (!seen.has(accDash)) {
        seen.add(accDash);
        filings.push({ accession: accDash, xmlFile: null, ciks: [cik], filingDate });
      }
      if (!oldestOnPage || filingDate < oldestOnPage) oldestOnPage = filingDate;
    }

    if (oldestOnPage && oldestOnPage < sinceDate) break;
    if (entries.length < 40) break;
  }

  return filings.filter(f => f.filingDate >= sinceDate);
}





async function pollRSS() {
  // For the live 2-min poll, grab everything from today (fast, usually <40 new)
  const today = new Date().toISOString().slice(0, 10);
  return fetchRecentFilings(today);
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

// ── EDGAR full-index — authoritative list of every filing by quarter ─
// URL: https://www.sec.gov/Archives/edgar/full-index/YYYY/QTRN/form.idx
// Updated daily. Lists every Form 4/4A with CIK, date, and accession path.
async function fetchFullIndex(startDate, endDate) {
  const filings = [];
  const start = new Date(startDate + 'T12:00:00Z');
  const end   = new Date(endDate   + 'T12:00:00Z');

  // Collect unique quarter keys spanning the date range
  const quarters = new Set();
  const cur = new Date(start);
  while (cur <= end) {
    const yr = cur.getUTCFullYear();
    const q  = Math.ceil((cur.getUTCMonth() + 1) / 3);
    quarters.add(`${yr}|${q}`);
    cur.setUTCMonth(cur.getUTCMonth() + 3);
  }

  for (const qkey of quarters) {
    const [yr, q] = qkey.split('|');
    const url = `https://www.sec.gov/Archives/edgar/full-index/${yr}/QTR${q}/form.idx`;
    log(`Fetching full-index: ${url}`);
    try {
      const { status, body } = await get(url, 60000);
      if (status !== 200) { log(`full-index HTTP ${status} for ${yr}Q${q}`); continue; }

      // form.idx fixed-width format:
      //   Form Type  Company Name        CIK         Date Filed  Filename
      //   ---------- ------------------- ----------- ----------- ---------------------------------
      //   4          ACME CORP           0001234567  2026-02-28  edgar/data/1234567/0001234567-26-000001.txt
      //
      // Strategy: skip header lines, match form type at start, extract date+filename by regex.
      const lines = body.split('\n');
      let pastHeader = false;
      let scanned = 0;
      let maxDateSeen = '';
      let debugPrinted = 0;

      for (const line of lines) {
        // The separator line is all dashes and spaces
        if (!pastHeader) {
          if (/^-{5}/.test(line.trim())) pastHeader = true;
          continue;
        }
        if (line.length < 30) continue;

        // Debug: print first 3 lines after header so we can see actual format
        if (debugPrinted < 3) {
          log(`  form.idx sample line: "${line.slice(0, 120)}"`);
          debugPrinted++;
        }

        const formType = line.slice(0, 12).trim();
        if (formType !== '4' && formType !== '4/A') continue;
        scanned++;

        // Date filed — ISO format YYYY-MM-DD
        let dateFiled = '';
        const isoMatch = line.match(/(\d{4}-\d{2}-\d{2})/);
        const mdyMatch = line.match(/(\d{2})\/(\d{2})\/(\d{4})/);
        if (isoMatch) {
          dateFiled = isoMatch[1];
        } else if (mdyMatch) {
          dateFiled = `${mdyMatch[3]}-${mdyMatch[1]}-${mdyMatch[2]}`;
        } else continue;

        if (dateFiled > maxDateSeen) maxDateSeen = dateFiled;
        if (dateFiled < startDate || dateFiled > endDate) continue;

        // Accession path — always edgar/data/CIK/XXXXXXXXXX-XX-XXXXXX.txt
        const fm = line.match(/edgar\/data\/(\d+)\/([\d-]+)\.txt/i);
        if (!fm) continue;
        const cik   = fm[1];
        const parts = fm[2].split('-');
        if (parts.length !== 3) continue;
        const accDash = `${parts[0].padStart(10,'0')}-${parts[1]}-${parts[2]}`;
        filings.push({ accession: accDash, xmlFile: null, ciks: [cik], filingDate: dateFiled });
      }
      log(`  form.idx ${yr}Q${q}: scanned ${scanned} Form-4 lines, most recent date: ${maxDateSeen}, ${filings.length} total in range`);
    } catch(e) { log(`full-index error ${yr}Q${q}: ${e.message}`); }
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

// ── Backfill: form.idx for older data, paginated RSS for last 5 days ──
async function runBackfill(daysBack) {
  const today = new Date().toISOString().slice(0, 10);
  const start = new Date();
  start.setDate(start.getDate() - daysBack);
  const startDate = start.toISOString().slice(0, 10);

  // Split: form.idx covers up to 5 days ago (reliable), RSS covers last 5 days
  const recentCutoff = new Date();
  recentCutoff.setDate(recentCutoff.getDate() - 5);
  const recentCutoffStr = recentCutoff.toISOString().slice(0, 10);

  log(`Backfill: ${startDate} → ${today} (idx: ${startDate}→${recentCutoffStr}, rss: ${recentCutoffStr}→${today})`);

  try {
    let allFilings = [];

    // 1. form.idx for older portion (more than 5 days ago)
    if (startDate < recentCutoffStr) {
      const idxFilings = await fetchFullIndex(startDate, recentCutoffStr);
      log(`form.idx returned ${idxFilings.length} filings for older range`);
      allFilings = allFilings.concat(idxFilings);
    }

    // 2. Paginated RSS for last 5 days (bypasses form.idx lag)
    const rssFilings = await fetchRecentFilings(recentCutoffStr);
    log(`Paginated RSS returned ${rssFilings.length} filings for recent range`);
    allFilings = allFilings.concat(rssFilings);

    if (!allFilings.length) {
      log('No filings found from any source');
      return;
    }

    const inserted = await processBatch(allFilings, 'Backfill');

    const byDate = {};
    allFilings.forEach(f => { byDate[f.filingDate] = (byDate[f.filingDate] || 0) + 1; });
    const upsert = db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)');
    const upsertMany = db.transaction(entries => {
      for (const [date, count] of entries) upsert.run(date, count, 0);
    });
    upsertMany(Object.entries(byDate));
    db.prepare('INSERT OR REPLACE INTO daily_log (date,filings,trades) VALUES (?,?,?)').run(today, allFilings.length, inserted);

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
  log(`=== daily-worker v9 start (mode=${mode}, daysBack=${daysBack}) ===`);

  // Clean up any rows with implausible trade_date or filing_date values
  const cleaned = db.prepare(`
    DELETE FROM trades
    WHERE trade_date  < '2000-01-01' OR trade_date  > '2030-12-31'
       OR filing_date < '2000-01-01' OR filing_date > '2030-12-31'
  `).run();
  if (cleaned.changes > 0) log(`Cleaned up ${cleaned.changes} rows with bad dates`);

  // Purge non-open-market codes and implausible values from any previously ingested data
  const c2 = db.prepare(`DELETE FROM trades WHERE TRIM(type) NOT IN ('P','S','S-')`).run();
  if (c2.changes > 0) log(`Removed ${c2.changes} non-market transaction records`);
  const c3 = db.prepare(`DELETE FROM trades WHERE value > 2000000000 OR price > 1500000 OR qty > 50000000`).run();
  if (c3.changes > 0) log(`Removed ${c3.changes} records with implausible values`);

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
