'use strict';

// daily-worker.js v10 — Turso edition
// Designed to run as a one-shot GitHub Actions job (backfill mode).
// Replaces better-sqlite3 with @libsql/client (async, remote Turso DB).
// Usage: node daily-worker.js [daysBack] [mode]
//   daysBack — how many calendar days to backfill (default: 3)
//   mode     — 'backfill' (default) or 'recent' (same, alias)

const https  = require('https');
const { createClient } = require('@libsql/client');

const TURSO_URL   = process.env.TURSO_DATABASE_URL;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;
if (!TURSO_URL) { console.error('TURSO_DATABASE_URL not set'); process.exit(1); }

const client = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN || undefined });

// ─── DB helpers ───────────────────────────────────────────────────────────────
async function dbQuery(sql, args = []) {
  const r = await client.execute({ sql, args });
  return r.rows.map(row => Object.fromEntries(r.columns.map((c, i) => [c, row[i] ?? null])));
}
async function dbRun(sql, args = []) {
  const r = await client.execute({ sql, args });
  return r.rowsAffected;
}
async function dbBatch(stmts) {
  return client.batch(stmts, 'write');
}

// ─── Schema ───────────────────────────────────────────────────────────────────
async function initSchema() {
  const stmts = [
    `CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
      trade_date TEXT NOT NULL, filing_date TEXT,
      type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER,
      accession TEXT, footnote TEXT,
      UNIQUE(accession, insider, trade_date, type, qty)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_ticker      ON trades(ticker)`,
    `CREATE INDEX IF NOT EXISTS idx_trade_date  ON trades(trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_filing_date ON trades(filing_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_insider     ON trades(insider)`,
    `CREATE TABLE IF NOT EXISTS daily_log (date TEXT PRIMARY KEY, synced_at TEXT DEFAULT (datetime('now')), filings INTEGER, trades INTEGER)`,
    `CREATE TABLE IF NOT EXISTS seen_filings (accession TEXT PRIMARY KEY, seen_at TEXT DEFAULT (datetime('now')))`,
  ];
  for (const sql of stmts) {
    try { await client.execute(sql); } catch(e) { /* already exists */ }
  }
}

// ─── Logging ──────────────────────────────────────────────────────────────────
function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11, 19)}] ${msg}\n`); }

// ─── Rate-limited HTTPS GET (SEC rate limit: max 8 req/sec) ──────────────────
const reqTimes = [];
async function get(url, ms = 20000, _hops = 0) {
  if (_hops > 5) throw new Error('Too many redirects');
  const now = Date.now();
  while (reqTimes.length && reqTimes[0] < now - 1000) reqTimes.shift();
  if (reqTimes.length >= 8) await new Promise(r => setTimeout(r, 1000 - (now - reqTimes[0]) + 10));
  reqTimes.push(Date.now());
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'User-Agent': 'InsiderTape/2.0 admin@insidertape.com' }, timeout: ms }, res => {
      if ([301, 302, 303].includes(res.statusCode) && res.headers.location) {
        res.resume();
        return get(res.headers.location, ms, _hops + 1).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ─── Form 4 XML parsing ───────────────────────────────────────────────────────
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
  const ticker  = xmlGet(xml, 'issuerTradingSymbol').toUpperCase().trim().replace(/^[A-Z]+:/, '');
  const company = xmlGet(xml, 'issuerName').trim();
  const insider = xmlGet(xml, 'rptOwnerName').trim();
  const title   = (xmlGet(xml, 'officerTitle') || xmlGet(xml, 'rptOwnerRelationship') || '').trim();
  const period  = parseDate(xmlGet(xml, 'periodOfReport'));
  const INVALID = new Set(['NONE', 'NULL', 'N/A', 'NA', '--', '-', '.', '0', 'FALSE', 'TRUE']);
  if (!ticker || INVALID.has(ticker) || !/^[A-Z]/.test(ticker) || ticker.length > 10) return [];

  const rows = [];
  function parseBlock(block) {
    const code = (xmlGet(block, 'transactionCode') || '').trim();
    if (!['P', 'S', 'S-'].includes(code)) return;
    const date  = parseDate(xmlGet(block, 'transactionDate')) || period || filingDate;
    if (!date) return;
    const qty   = Math.round(Math.abs(parseFloat(xmlGet(block, 'transactionShares') || '0') || 0));
    const price = Math.abs(parseFloat(xmlGet(block, 'transactionPricePerShare') || '0') || 0);
    const owned = Math.round(Math.abs(parseFloat(xmlGet(block, 'sharesOwnedFollowingTransaction') || '0') || 0));
    if (qty > 500_000_000 || price > 1_500_000) return;
    const value = Math.round(qty * price);
    if (value > 5_000_000_000) return;
    if (price > 0 && price < 0.05 && qty > 1_000_000) return;
    if (value > 0 && value < 500) return;

    const fnTexts = [];
    const fnRe = /<footnote[^>]*id="([^"]*)"[^>]*>([\s\S]*?)<\/footnote>/gi;
    let fnMatch;
    while ((fnMatch = fnRe.exec(block + xml)) !== null) fnTexts.push(fnMatch[2].replace(/<[^>]+>/g, '').trim());
    const footnote = fnTexts.join(' ').replace(/\s+/g, ' ').trim().slice(0, 500);

    if (code === 'P') {
      const fn = footnote.toLowerCase();
      const isDRIP = (fn.includes('pursuant to') || fn.includes('through the') || fn.includes('under the') || fn.includes('under a') || fn.includes('automatic') || fn.includes('prior election'))
        && (fn.includes('dividend reinvest') || fn.includes('drip') || fn.includes('reinvestment plan') || fn.includes('stock purchase plan') || fn.includes('espp') || fn.includes('compensation plan') || fn.includes('deferred compensation'));
      const isOffering = fn.includes('public offering') || fn.includes('underwritten offering') || fn.includes('private placement') || fn.includes('subscription agreement') || fn.includes('securities purchase agreement') || fn.includes('placement agent') || fn.includes('direct offering');
      if (isDRIP || isOffering) return;
    }

    rows.push([ticker, company, insider, title, date, filingDate, code, qty, +price.toFixed(4), value, owned, accession, footnote || null]);
  }

  let m;
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/gi;
  while ((m = ndRe.exec(xml))) parseBlock(m[1]);
  return rows;
}

// ─── Fetch Form 4 XML from EDGAR ──────────────────────────────────────────────
async function fetchForm4(accession, filingDate, xmlFile, ciks) {
  const acc      = accession.replace(/-/g, '');
  const filerCik = parseInt(acc.slice(0, 10), 10).toString();
  const allCiks  = [...new Set([...(ciks || []).map(k => parseInt(k, 10).toString()), filerCik])];

  async function tryXml(url) {
    try { const { status, body } = await get(url); if (status === 200 && body.includes('ownershipDocument')) return body; } catch(_) {}
    return null;
  }

  if (xmlFile) {
    for (const cik of allCiks) {
      const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlFile}`);
      if (xml) return parseForm4(xml, filingDate, accession);
    }
  }

  for (const cik of allCiks) {
    try {
      const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${accession}-index.htm`);
      if (status === 200) {
        const xmlMatch = body.match(/href="([^"]+\.xml)"/i);
        if (xmlMatch) {
          const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${xmlMatch[1].split('/').pop()}`);
          if (xml) return parseForm4(xml, filingDate, accession);
        }
      }
    } catch(_) {}

    try {
      const { status, body } = await get(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${accession}-index.json`);
      if (status === 200) {
        const idx = JSON.parse(body);
        const doc = (idx.documents || []).find(d => d.document?.match(/\.xml$/i) && (d.type === '4' || d.type === '4/A' || !d.type)) || (idx.documents || []).find(d => d.document?.match(/\.xml$/i));
        if (doc) { const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${doc.document}`); if (xml) return parseForm4(xml, filingDate, accession); }
      }
    } catch(_) {}
  }

  for (const cik of allCiks) {
    for (const name of [`${accession}.xml`, 'form4.xml', 'wf-form4.xml']) {
      const xml = await tryXml(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${name}`);
      if (xml) return parseForm4(xml, filingDate, accession);
    }
  }

  return [];
}

// ─── EDGAR filing discovery ────────────────────────────────────────────────────
async function searchEFTS(startDate, endDate) {
  const filings = [];
  for (let from = 0; from < 10000; from += 100) {
    const url = `https://efts.sec.gov/LATEST/search-index?forms=4,4%2FA&dateRange=custom&startdt=${startDate}&enddt=${endDate}&from=${from}&size=100`;
    try {
      const { status, body } = await get(url, 30000);
      if (status !== 200) { log(`EFTS HTTP ${status}`); break; }
      const data = JSON.parse(body);
      const hits = data.hits?.hits || [];
      if (!hits.length) break;
      for (const h of hits) {
        const raw = h._id || '';
        const colonAt = raw.indexOf(':');
        const accDash = colonAt >= 0 ? raw.slice(0, colonAt) : raw.replace(/\//g, '-');
        const xmlFile = colonAt >= 0 ? raw.slice(colonAt + 1) : null;
        const fd = parseDate(h._source?.file_date) || endDate;
        const ciks = h._source?.ciks || [];
        const formType = (h._source?.form_type || '4').trim();
        if (accDash.match(/^\d{10}-\d{2}-\d{6}$/)) filings.push({ accession: accDash, xmlFile, ciks, filingDate: fd, formType });
      }
      if (hits.length < 100) break;
    } catch(e) { log(`EFTS error: ${e.message}`); break; }
  }
  log(`EFTS: ${filings.length} filings for ${startDate}→${endDate}`);
  return filings;
}

async function fetchViaAtom(sinceDate) {
  const filings = [], seen = new Set();
  for (let start = 0; start < 4000; start += 40) {
    const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&start=${start}&output=atom`;
    const r = await get(url, 30000);
    if (r.status !== 200) break;
    const entries = r.body.split('<entry>').slice(1);
    if (!entries.length) break;
    let oldestOnPage = '';
    for (const entry of entries) {
      const dateMatch  = entry.match(/<updated>(\d{4}-\d{2}-\d{2})/);
      const filingDate = dateMatch ? dateMatch[1] : new Date().toISOString().slice(0, 10);
      const linkMatch  = entry.match(/https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/(\d+)\/([\d]{18})\//);
      if (!linkMatch) continue;
      const cik    = linkMatch[1];
      const accRaw = linkMatch[2];
      const accDash = `${accRaw.slice(0, 10)}-${accRaw.slice(10, 12)}-${accRaw.slice(12)}`;
      if (!seen.has(accDash)) {
        seen.add(accDash);
        const isAmend = entry.includes('4/A');
        filings.push({ accession: accDash, xmlFile: null, ciks: [cik], filingDate, formType: isAmend ? '4/A' : '4' });
      }
      if (!oldestOnPage || filingDate < oldestOnPage) oldestOnPage = filingDate;
    }
    if (oldestOnPage && oldestOnPage < sinceDate) break;
    if (entries.length < 40) break;
  }
  return filings.filter(f => f.filingDate >= sinceDate);
}

async function fetchFullIndex(startDate, endDate) {
  const filings = [];
  const quarters = new Set();
  const cur = new Date(startDate + 'T12:00:00Z');
  while (cur <= new Date(endDate + 'T12:00:00Z')) {
    const yr = cur.getUTCFullYear(), q = Math.ceil((cur.getUTCMonth() + 1) / 3);
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
      const lines = body.split('\n');
      let pastHeader = false;
      for (const line of lines) {
        if (!pastHeader) { if (/^-{5}/.test(line.trim())) pastHeader = true; continue; }
        if (line.length < 30) continue;
        const formType = line.slice(0, 12).trim();
        if (formType !== '4' && formType !== '4/A') continue;
        let dateFiled = '';
        const isoM = line.match(/(\d{4}-\d{2}-\d{2})/);
        const mdyM = line.match(/(\d{2})\/(\d{2})\/(\d{4})/);
        if (isoM) dateFiled = isoM[1];
        else if (mdyM) dateFiled = `${mdyM[3]}-${mdyM[1]}-${mdyM[2]}`;
        else continue;
        if (dateFiled < startDate || dateFiled > endDate) continue;
        const fm = line.match(/edgar\/data\/(\d+)\/([\d-]+)\.txt/i);
        if (!fm) continue;
        const parts = fm[2].split('-');
        if (parts.length !== 3) continue;
        filings.push({ accession: `${parts[0].padStart(10, '0')}-${parts[1]}-${parts[2]}`, xmlFile: null, ciks: [fm[1]], filingDate: dateFiled, formType });
      }
      log(`  form.idx ${yr}Q${q}: ${filings.length} total so far in range`);
    } catch(e) { log(`full-index error ${yr}Q${q}: ${e.message}`); }
  }
  log(`Full-index: ${filings.length} Form 4 filings for ${startDate}→${endDate}`);
  return filings;
}

async function fetchRecentFilings(sinceDate) {
  const seen = new Set(), filings = [];
  try {
    const atom = await fetchViaAtom(sinceDate);
    atom.forEach(f => { if (!seen.has(f.accession)) { seen.add(f.accession); filings.push(f); } });
    log(`Atom feed: ${atom.length} since ${sinceDate}`);
  } catch(e) { log(`Atom error: ${e.message}`); }

  const today = new Date().toISOString().slice(0, 10);
  for (let d = new Date(sinceDate + 'T12:00:00Z'); d.toISOString().slice(0, 10) <= today; d.setUTCDate(d.getUTCDate() + 1)) {
    const ds = d.toISOString().slice(0, 10);
    try {
      const day = await searchEFTS(ds, ds);
      let added = 0;
      day.forEach(f => { if (!seen.has(f.accession)) { seen.add(f.accession); filings.push(f); added++; } });
      if (added > 0) log(`EFTS ${ds}: +${added}`);
    } catch(_) {}
  }
  log(`fetchRecentFilings: ${filings.length} since ${sinceDate}`);
  return filings;
}

// ─── Batch insert helpers (Turso) ─────────────────────────────────────────────
const INSERT_SQL = `INSERT OR IGNORE INTO trades (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession,footnote) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`;

async function doInsertBatch(rows) {
  if (!rows.length) return 0;
  let inserted = 0;
  const CHUNK = 50;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const stmts = rows.slice(i, i + CHUNK).map(r => ({ sql: INSERT_SQL, args: r }));
    const results = await dbBatch(stmts);
    for (const r of results) inserted += r.rowsAffected || 0;
  }
  return inserted;
}

async function doInsertAmendmentBatch(rows) {
  if (!rows.length) return 0;
  const keys = new Set(rows.map(r => r[0] + '|' + r[2] + '|' + r[4]));
  const stmts = [];
  for (const key of keys) {
    const [ticker, insider, trade_date] = key.split('|');
    stmts.push({ sql: `DELETE FROM trades WHERE ticker=? AND insider=? AND trade_date=? AND TRIM(type) IN ('P','S','S-')`, args: [ticker, insider, trade_date] });
  }
  for (const r of rows) stmts.push({ sql: INSERT_SQL, args: r });
  const results = await dbBatch(stmts);
  let inserted = 0;
  for (const r of results.slice(keys.size)) inserted += r.rowsAffected || 0;
  return inserted;
}

async function markSeenBatch(accessions) {
  const CHUNK = 100;
  for (let i = 0; i < accessions.length; i += CHUNK) {
    const stmts = accessions.slice(i, i + CHUNK).map(acc => ({ sql: 'INSERT OR IGNORE INTO seen_filings (accession) VALUES (?)', args: [acc] }));
    await dbBatch(stmts).catch(() => {});
  }
}

// ─── Process filings (batch-aware) ────────────────────────────────────────────
async function processBatch(filings, label) {
  if (!filings.length) return 0;

  // Batch check: which accessions are already processed?
  const alreadySeen = new Set();
  const BATCH_SIZE = 500;
  for (let i = 0; i < filings.length; i += BATCH_SIZE) {
    const chunk = filings.slice(i, i + BATCH_SIZE).map(f => f.accession);
    const placeholders = chunk.map(() => '?').join(',');
    const [tradeRows, seenRows] = await Promise.all([
      dbQuery(`SELECT DISTINCT accession FROM trades WHERE accession IN (${placeholders})`, chunk),
      dbQuery(`SELECT accession FROM seen_filings WHERE accession IN (${placeholders})`, chunk),
    ]);
    tradeRows.forEach(r => alreadySeen.add(r.accession));
    seenRows.forEach(r => alreadySeen.add(r.accession));
  }

  const newFilings = filings.filter(f => !alreadySeen.has(f.accession));
  log(`${label}: ${filings.length} total, ${filings.length - newFilings.length} already processed, ${newFilings.length} new`);
  if (!newFilings.length) return 0;

  let inserted = 0;
  const CONCURRENCY = 8;

  for (let i = 0; i < newFilings.length; i += CONCURRENCY) {
    const chunk = newFilings.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      chunk.map(f => fetchForm4(f.accession, f.filingDate, f.xmlFile, f.ciks))
    );
    const insertRows = [], amendRows = [];
    for (let j = 0; j < results.length; j++) {
      const r = results[j];
      if (r.status === 'fulfilled' && r.value?.length) {
        if (chunk[j].formType === '4/A') amendRows.push(...r.value);
        else insertRows.push(...r.value);
      }
    }
    if (insertRows.length) inserted += await doInsertBatch(insertRows);
    if (amendRows.length)  inserted += await doInsertAmendmentBatch(amendRows);
    if ((i + CONCURRENCY) % 60 === 0) log(`  ${i + CONCURRENCY}/${newFilings.length} done, inserted:${inserted}`);
    await new Promise(r => setTimeout(r, 25)); // brief yield
  }

  await markSeenBatch(newFilings.map(f => f.accession));
  log(`${label}: done — ${inserted} trades from ${newFilings.length} new filings`);
  return inserted;
}

// ─── Backfill ─────────────────────────────────────────────────────────────────
async function runBackfill(daysBack) {
  const today     = new Date().toISOString().slice(0, 10);
  const startDate = new Date(Date.now() - daysBack * 86400000).toISOString().slice(0, 10);
  const recentCutoff = new Date(Date.now() - 5 * 86400000).toISOString().slice(0, 10);

  log(`Backfill: ${startDate} → ${today}`);

  let allFilings = [];
  if (startDate < recentCutoff) {
    const idxFilings = await fetchFullIndex(startDate, recentCutoff);
    allFilings = allFilings.concat(idxFilings);
  }
  const rssFilings = await fetchRecentFilings(recentCutoff);
  allFilings = allFilings.concat(rssFilings);

  if (!allFilings.length) { log('No filings found'); return; }
  const inserted = await processBatch(allFilings, 'Backfill');

  // Update daily_log
  const byDate = {};
  allFilings.forEach(f => { byDate[f.filingDate] = (byDate[f.filingDate] || 0) + 1; });
  const logStmts = Object.entries(byDate).map(([date, count]) => ({
    sql: 'INSERT OR REPLACE INTO daily_log (date, filings, trades) VALUES (?, ?, ?)',
    args: [date, count, 0],
  }));
  logStmts.push({ sql: 'INSERT OR REPLACE INTO daily_log (date, filings, trades) VALUES (?, ?, ?)', args: [today, allFilings.length, inserted] });
  await dbBatch(logStmts).catch(() => {});

  // Prune seen_filings older than 45 days
  await dbRun("DELETE FROM seen_filings WHERE seen_at < datetime('now','-45 days')").catch(() => {});

  log(`Backfill complete: ${inserted} trades across ${Object.keys(byDate).length} days`);
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
const daysBack = parseInt(process.argv[2] || '3');

async function main() {
  log(`=== daily-worker v10 (Turso) start, daysBack=${daysBack} ===`);
  await initSchema();

  // Light startup cleanup
  await dbRun(`DELETE FROM trades WHERE trade_date < '2000-01-01' OR trade_date > '2030-12-31'`).catch(() => {});
  await dbRun(`DELETE FROM trades WHERE TRIM(type) NOT IN ('P','S','S-')`).catch(() => {});
  await dbRun(`DELETE FROM trades WHERE value > 5000000000 OR price > 1500000 OR qty > 500000000`).catch(() => {});

  await runBackfill(daysBack);
  log('=== daily-worker done ===');
}

main().catch(e => { log(`FATAL: ${e.message}\n${e.stack}`); process.exit(1); });
