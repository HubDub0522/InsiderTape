'use strict';
// ── 13F Holdings Worker ───────────────────────────────────────────────────────
// Processes SEC 13F-HR quarterly filings from EDGAR.
// Only stores CHANGES (new positions, additions, reductions, exits) — not flat
// holdings. This keeps the DB small while enabling chart plotting and analysis.
//
// Flow:
//   1. Stream EDGAR quarterly form.idx to find all 13F-HR filings
//   2. For each filing fetch the XML holdings table (infotable.xml)
//   3. Compare to prior quarter's holdings for same filer
//   4. Insert only changed rows into f13_changes
//   5. Resolve CUSIPs → tickers via OpenFIGI API (batched, cached)
//
// Run modes:
//   node f13-worker.js          — process latest 2 quarters
//   node f13-worker.js full     — process last 8 quarters (2 years)
//   node f13-worker.js Q 2025 4 — process specific quarter

const https   = require('https');
const http    = require('http');
const path    = require('path');
const Database = require('better-sqlite3');

const DATA_DIR  = process.env.DATA_DIR || '/var/data';
const DB_PATH   = process.env.DB_PATH  || path.join(DATA_DIR, 'trades.db');
const OPENFIGI_KEY = process.env.OPENFIGI_KEY || ''; // optional — bumps rate limit

const WORKER_VERSION = 1;
const MAX_FILERS_PER_QUARTER = 2000; // cap to avoid runaway processing
const BATCH_SIZE = 10; // OpenFIGI batch size (max 100 per request)

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  const prefix = process.send ? '[f13] ' : '';
  const line = `[${ts}] ${prefix}${msg}`;
  console.log(line);
  if (process.send) process.send({ type: 'log', line });
}

// ── DB setup ─────────────────────────────────────────────────────────────────
log(`DB path: ${DB_PATH}`);
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 15000');
db.pragma('synchronous = NORMAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS f13_changes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker        TEXT    NOT NULL DEFAULT '',
    cusip         TEXT    NOT NULL DEFAULT '',
    filer_cik     TEXT    NOT NULL,
    filer_name    TEXT    NOT NULL DEFAULT '',
    quarter       TEXT    NOT NULL,  -- e.g. 2025Q4
    filed_date    TEXT    NOT NULL,
    shares        INTEGER,           -- shares held this quarter
    shares_delta  INTEGER,           -- positive=added, negative=reduced
    value_usd     INTEGER,           -- market value in dollars
    pct_change    REAL,              -- % change vs prior quarter
    is_new        INTEGER DEFAULT 0, -- 1 = new position this quarter
    is_exit       INTEGER DEFAULT 0  -- 1 = fully exited this quarter
  );
  CREATE INDEX IF NOT EXISTS idx_f13_ticker   ON f13_changes(ticker);
  CREATE INDEX IF NOT EXISTS idx_f13_quarter  ON f13_changes(quarter);
  CREATE INDEX IF NOT EXISTS idx_f13_filer    ON f13_changes(filer_cik);
  CREATE INDEX IF NOT EXISTS idx_f13_date     ON f13_changes(filed_date);
`);

// Quarter log — tracks which quarters have been processed
db.exec(`
  CREATE TABLE IF NOT EXISTS f13_quarter_log (
    quarter   TEXT PRIMARY KEY,
    filers    INTEGER DEFAULT 0,
    changes   INTEGER DEFAULT 0,
    processed_at TEXT
  );
`);

// CUSIP cache — avoids re-hitting OpenFIGI for known mappings
db.exec(`
  CREATE TABLE IF NOT EXISTS f13_cusip_cache (
    cusip  TEXT PRIMARY KEY,
    ticker TEXT NOT NULL DEFAULT '',  -- empty = unresolvable
    cached_at TEXT
  );
`);

// ── HTTP helpers ──────────────────────────────────────────────────────────────
const _reqTimes = [];
async function get(url, ms = 30000, _hops = 0) {
  if (_hops > 5) throw new Error('Too many redirects');
  const now = Date.now();
  while (_reqTimes.length && _reqTimes[0] < now - 1000) _reqTimes.shift();
  if (_reqTimes.length >= 8) {
    await new Promise(r => setTimeout(r, 1000 - (now - _reqTimes[0]) + 10));
  }
  _reqTimes.push(Date.now());

  const lib = url.startsWith('https') ? https : http;
  return new Promise((resolve, reject) => {
    const req = lib.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return get(next, ms, _hops + 1).then(resolve).catch(reject);
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

// Streaming line processor — never loads full file into memory
function getLines(url, ms, onLine) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return getLines(next, ms, onLine).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return resolve({ status: res.statusCode, lineCount: 0 });
      }
      let buf = '', lineCount = 0;
      res.on('data', chunk => {
        buf += chunk.toString('utf8');
        let idx;
        while ((idx = buf.indexOf('\n')) >= 0) {
          onLine(buf.slice(0, idx));
          buf = buf.slice(idx + 1);
          lineCount++;
        }
      });
      res.on('end', () => {
        if (buf.length) { onLine(buf); lineCount++; }
        resolve({ status: 200, lineCount });
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// POST request for OpenFIGI
function post(url, bodyObj, ms = 15000) {
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(bodyObj);
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(bodyStr),
    };
    if (OPENFIGI_KEY) headers['X-OPENFIGI-APIKEY'] = OPENFIGI_KEY;
    const urlObj = new URL(url);
    const req = https.request({
      hostname: urlObj.hostname,
      path: urlObj.pathname,
      method: 'POST',
      headers,
      timeout: ms,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(bodyStr);
    req.end();
  });
}

// ── CUSIP → Ticker resolution via OpenFIGI ────────────────────────────────────
// Rate limit: 25 req/min without key, 250/min with key
const _openFigiTimes = [];
async function rateLimitOpenFigi() {
  const limit = OPENFIGI_KEY ? 250 : 25;
  const window = 60000;
  const now = Date.now();
  while (_openFigiTimes.length && _openFigiTimes[0] < now - window) _openFigiTimes.shift();
  if (_openFigiTimes.length >= limit) {
    const wait = window - (now - _openFigiTimes[0]) + 100;
    await new Promise(r => setTimeout(r, wait));
  }
  _openFigiTimes.push(Date.now());
}

// In-memory cache for this run (supplements DB cache)
const _cusipMemCache = {};

async function resolveCusips(cusips) {
  // Filter to ones not already in memory cache or DB
  const toResolve = [];
  const result = {};

  for (const cusip of cusips) {
    if (_cusipMemCache[cusip] !== undefined) {
      result[cusip] = _cusipMemCache[cusip];
      continue;
    }
    const row = db.prepare('SELECT ticker FROM f13_cusip_cache WHERE cusip=?').get(cusip);
    if (row) {
      _cusipMemCache[cusip] = row.ticker;
      result[cusip] = row.ticker;
      continue;
    }
    toResolve.push(cusip);
  }

  if (!toResolve.length) return result;

  // Batch into groups of BATCH_SIZE (OpenFIGI max per request is 100)
  const upsert = db.prepare(`
    INSERT OR REPLACE INTO f13_cusip_cache (cusip, ticker, cached_at)
    VALUES (?, ?, date('now'))
  `);

  for (let i = 0; i < toResolve.length; i += BATCH_SIZE) {
    const batch = toResolve.slice(i, i + BATCH_SIZE);
    await rateLimitOpenFigi();

    try {
      const payload = batch.map(cusip => ({ idType: 'ID_CUSIP', idValue: cusip, exchCode: 'US' }));
      const { status, body } = await post('https://api.openfigi.com/v3/mapping', payload, 15000);

      if (status === 200) {
        const data = JSON.parse(body);
        data.forEach((item, idx) => {
          const cusip = batch[idx];
          // OpenFIGI returns array of matches — prefer common stock (shareClassFIGI)
          const match = item.data?.find(d => d.securityType === 'Common Stock' && d.ticker)
                     || item.data?.find(d => d.ticker);
          const ticker = match?.ticker?.toUpperCase().trim() || '';
          // Validate ticker format
          const validTicker = ticker && /^[A-Z]{1,7}(-[A-Z]{1,2})?$/.test(ticker) ? ticker : '';
          _cusipMemCache[cusip] = validTicker;
          result[cusip] = validTicker;
          upsert.run(cusip, validTicker);
        });
      } else if (status === 429) {
        // Rate limited — wait 60s and retry this batch
        log(`OpenFIGI rate limited — waiting 60s...`);
        await new Promise(r => setTimeout(r, 61000));
        i -= BATCH_SIZE; // retry
      } else {
        // On error, mark all as empty to avoid re-querying this run
        batch.forEach(cusip => { _cusipMemCache[cusip] = ''; result[cusip] = ''; });
      }
    } catch(e) {
      batch.forEach(cusip => { _cusipMemCache[cusip] = ''; result[cusip] = ''; });
    }
  }

  return result;
}

// ── XML parsing helpers ───────────────────────────────────────────────────────
// 13F XML infotable has rows like:
// <infoTable>
//   <nameOfIssuer>APPLE INC</nameOfIssuer>
//   <titleOfClass>COM</titleOfClass>
//   <cusip>037833100</cusip>
//   <value>1234567</value>  ← in thousands of dollars
//   <shrsOrPrnAmt><sshPrnamt>12345678</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
//   <putCall></putCall>
//   <investmentDiscretion>SOLE</investmentDiscretion>
//   <votingAuthority>...</votingAuthority>
// </infoTable>

function parseXmlValue(xml, tag) {
  const m = xml.match(new RegExp(`<${tag}[^>]*>([^<]*)</${tag}>`, 'i'));
  return m ? m[1].trim() : '';
}

function parseHoldings(xmlBody) {
  const holdings = [];
  // Split on infoTable tags
  const tableRe = /<infoTable>([\s\S]*?)<\/infoTable>/gi;
  let m;
  while ((m = tableRe.exec(xmlBody)) !== null) {
    const block = m[1];
    const cusip  = parseXmlValue(block, 'cusip').replace(/[^A-Z0-9]/gi, '').toUpperCase();
    const sharesStr = parseXmlValue(block, 'sshPrnamt');
    const valueStr  = parseXmlValue(block, 'value');
    const type      = parseXmlValue(block, 'sshPrnamtType').toUpperCase();
    const putCall   = parseXmlValue(block, 'putCall').toUpperCase();

    // Skip options/puts/calls — only want equity positions
    if (putCall === 'PUT' || putCall === 'CALL') continue;
    // Skip if not shares (some are principal amounts for bonds)
    if (type && type !== 'SH') continue;
    if (!cusip || cusip.length < 6) continue;

    const shares = parseInt(sharesStr, 10) || 0;
    const value  = (parseInt(valueStr, 10) || 0) * 1000; // convert from thousands

    if (shares > 0) holdings.push({ cusip, shares, value });
  }
  return holdings;
}

// ── Find XML holdings URL from filing index page ──────────────────────────────
async function getInfoTableUrl(accession, cik) {
  const acc = accession.replace(/-/g, '');
  const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${accession}-index.htm`;
  try {
    const { status, body } = await get(indexUrl, 15000);
    if (status !== 200) return null;
    // Look for infotable.xml or primary_doc.xml
    const m = body.match(/href="([^"]*(?:infotable|information_table|13finfotable)[^"]*\.xml)"/i)
           || body.match(/href="([^"]*\.xml)"/i);
    if (!m) return null;
    const xmlPath = m[1].startsWith('http') ? m[1] : `https://www.sec.gov${m[1].startsWith('/') ? '' : `/Archives/edgar/data/${cik}/${acc}/`}${m[1]}`;
    return xmlPath;
  } catch(e) { return null; }
}

// ── Quarter processing ────────────────────────────────────────────────────────
async function processQuarter(year, q) {
  const key = `${year}Q${q}`;

  const already = db.prepare('SELECT filers FROM f13_quarter_log WHERE quarter=?').get(key);
  if (already) {
    log(`${key}: already processed (${already.filers} filers), skipping`);
    return 0;
  }

  log(`${key}: streaming EDGAR form.idx for 13F-HR filings...`);

  // Try form.idx first, fall back to company.idx
  const formIdxUrl = `https://www.sec.gov/Archives/edgar/full-index/${year}/QTR${q}/form.idx`;

  const filings = []; // { cik, accession, filedDate, filerName }
  let lineCount = 0, pastHeader = false, isCompanyIdx = false;
  let firstLines = [];

  async function streamIndex(url, isCompIdx) {
    const found = [];
    await getLines(url, 120000, line => {
      lineCount++;
      if (firstLines.length < 6) firstLines.push(line);
      if (!pastHeader) {
        if (/^-{5,}/.test(line.trim())) pastHeader = true;
        return;
      }
      if (line.length < 40) return;

      const formType = isCompIdx ? line.slice(62, 74).trim() : line.slice(0, 12).trim();
      if (formType !== '13F-HR' && formType !== '13F-HR/A') return;

      const isoM = line.match(/(\d{4}-\d{2}-\d{2})/);
      if (!isoM) return;
      const filedDate = isoM[1];
      if (filedDate < '2000-01-01') return;

      const fnM = line.match(/edgar\/data\/(\d+)\/([\d-]{20,})\.txt/i)
               || line.match(/edgar\/data\/(\d+)\/(\d{10}-\d{2}-\d{6})/);
      if (!fnM) return;

      const cik   = fnM[1];
      const parts = fnM[2].replace(/\.txt$/i,'').split('-');
      if (parts.length !== 3) return;
      const accDash = `${parts[0].padStart(10,'0')}-${parts[1]}-${parts[2]}`;

      let filerName = '';
      if (isCompIdx) {
        filerName = line.slice(0, 62).trim();
      } else {
        const cikPadded = cik.padStart(10,'0');
        let cikPos = line.indexOf(cikPadded);
        if (cikPos < 0) cikPos = line.indexOf(' ' + cik + ' ');
        const raw = cikPos > 12 ? line.slice(12, cikPos) : '';
        filerName = raw.replace(/\s{2,}/g,' ').trim();
      }

      found.push({ cik, accession: accDash, filedDate, filerName });
    });
    return found;
  }

  let found = await streamIndex(formIdxUrl, false);

  // Check if form.idx was a readme
  const firstMeaningful = firstLines.find(l => l.trim().length > 10) || '';
  const isReadme = /^(Description|Last Data Received|Comments)/i.test(firstMeaningful.trim());
  if (isReadme || found.length === 0) {
    log(`${key}: form.idx is readme — trying company.idx...`);
    pastHeader = false; firstLines = []; lineCount = 0;
    const url2 = `https://www.sec.gov/Archives/edgar/full-index/${year}/QTR${q}/company.idx`;
    found = await streamIndex(url2, true);
    isCompanyIdx = true;
  }

  log(`${key}: found ${found.length} 13F-HR filings`);
  if (!found.length) {
    db.prepare('INSERT OR REPLACE INTO f13_quarter_log (quarter,filers,changes,processed_at) VALUES (?,0,0,datetime("now"))').run(key);
    return 0;
  }

  // Cap to avoid runaway — take most recent filings first (amendments filed last)
  // Deduplicate by filer CIK — keep the latest filing (amendment if any)
  const byCik = {};
  for (const f of found) {
    if (!byCik[f.cik] || f.filedDate > byCik[f.cik].filedDate) byCik[f.cik] = f;
  }
  const unique = Object.values(byCik).sort((a,b) => b.filedDate.localeCompare(a.filedDate));
  const toProcess = unique.slice(0, MAX_FILERS_PER_QUARTER);

  log(`${key}: processing ${toProcess.length} unique filers (${unique.length - toProcess.length} capped)`);

  // Load prior quarter's holdings for diffing
  const priorKey = getPriorQuarter(year, q);
  const priorHoldings = {}; // { filerCik: { cusip: shares } }
  if (priorKey) {
    const priorRows = db.prepare(`
      SELECT filer_cik, cusip, shares FROM f13_changes WHERE quarter=?
    `).all(priorKey);
    for (const row of priorRows) {
      if (!priorHoldings[row.filer_cik]) priorHoldings[row.filer_cik] = {};
      if (row.is_exit) priorHoldings[row.filer_cik][row.cusip] = 0;
      else priorHoldings[row.filer_cik][row.cusip] = row.shares || 0;
    }
    log(`${key}: loaded prior quarter ${priorKey} (${priorRows.length} rows)`);
  }

  // Process each filer
  const insertChange = db.prepare(`
    INSERT OR REPLACE INTO f13_changes
      (ticker, cusip, filer_cik, filer_name, quarter, filed_date,
       shares, shares_delta, value_usd, pct_change, is_new, is_exit)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  let totalChanges = 0, totalFilers = 0, cusipBatch = [];
  const pendingRows = []; // accumulate before CUSIP resolution

  for (let i = 0; i < toProcess.length; i++) {
    const f = toProcess[i];

    try {
      // Get XML URL from filing index
      const xmlUrl = await getInfoTableUrl(f.accession, f.cik);
      if (!xmlUrl) continue;

      // Fetch and parse XML holdings
      const { status, body } = await get(xmlUrl, 30000);
      if (status !== 200) continue;

      const holdings = parseHoldings(body);
      if (!holdings.length) continue;

      // Diff against prior quarter
      const prior = priorHoldings[f.cik] || {};
      const currentCusips = new Set(holdings.map(h => h.cusip));

      for (const h of holdings) {
        const prevShares = prior[h.cusip];
        const isNew = prevShares === undefined;
        const delta = isNew ? h.shares : (h.shares - prevShares);

        // Skip if unchanged (same share count)
        if (!isNew && delta === 0) continue;
        // Skip tiny changes (< 1% change and < $100k)
        if (!isNew && Math.abs(delta) / Math.max(prevShares, 1) < 0.01 && Math.abs(h.value - (prior[h.cusip] || 0)) < 100000) continue;

        const pctChange = isNew ? null : prevShares > 0 ? +((delta / prevShares) * 100).toFixed(1) : null;

        pendingRows.push({
          cusip:     h.cusip,
          filerCik:  f.cik,
          filerName: f.filerName,
          quarter:   key,
          filedDate: f.filedDate,
          shares:    h.shares,
          delta,
          value:     h.value,
          pctChange,
          isNew:     isNew ? 1 : 0,
          isExit:    0,
        });
        cusipBatch.push(h.cusip);
      }

      // Track full exits — positions in prior but not in current
      for (const [cusip, prevShares] of Object.entries(prior)) {
        if (!currentCusips.has(cusip) && prevShares > 0) {
          pendingRows.push({
            cusip, filerCik: f.cik, filerName: f.filerName,
            quarter: key, filedDate: f.filedDate,
            shares: 0, delta: -prevShares, value: 0,
            pctChange: -100, isNew: 0, isExit: 1,
          });
          cusipBatch.push(cusip);
        }
      }

      totalFilers++;
    } catch(e) {
      // Skip individual filer errors silently
    }

    // Every 50 filers, resolve CUSIPs and flush to DB
    if (totalFilers > 0 && totalFilers % 50 === 0) {
      log(`${key}: processed ${i+1}/${toProcess.length} filers, ${pendingRows.length} pending changes...`);
      const uniqueCusips = [...new Set(cusipBatch)];
      const tickerMap = await resolveCusips(uniqueCusips);
      const insertMany = db.transaction(rows => {
        for (const r of rows) {
          const ticker = tickerMap[r.cusip] || '';
          insertChange.run(ticker, r.cusip, r.filerCik, r.filerName, r.quarter,
            r.filedDate, r.shares, r.delta, r.value, r.pctChange, r.isNew, r.isExit);
          if (ticker) totalChanges++;
        }
      });
      insertMany(pendingRows);
      pendingRows.length = 0;
      cusipBatch = [];
    }
  }

  // Final flush
  if (pendingRows.length > 0) {
    const uniqueCusips = [...new Set(cusipBatch)];
    const tickerMap = await resolveCusips(uniqueCusips);
    const insertMany = db.transaction(rows => {
      for (const r of rows) {
        const ticker = tickerMap[r.cusip] || '';
        insertChange.run(ticker, r.cusip, r.filerCik, r.filerName, r.quarter,
          r.filedDate, r.shares, r.delta, r.value, r.pctChange, r.isNew, r.isExit);
        if (ticker) totalChanges++;
      }
    });
    insertMany(pendingRows);
  }

  db.prepare('INSERT OR REPLACE INTO f13_quarter_log (quarter,filers,changes,processed_at) VALUES (?,?,?,datetime("now"))').run(key, totalFilers, totalChanges);
  log(`${key}: done — ${totalFilers} filers, ${totalChanges} changes with tickers`);
  return totalChanges;
}

function getPriorQuarter(year, q) {
  if (q === 1) return `${year-1}Q4`;
  return `${year}Q${q-1}`;
}

function getCurrentQuarters(n) {
  const now = new Date();
  const results = [];
  let year = now.getUTCFullYear();
  let q = Math.ceil((now.getUTCMonth() + 1) / 3);
  // 13Fs are filed 45 days after quarter end — back up one quarter to get latest available
  q--;
  if (q < 1) { q = 4; year--; }
  for (let i = 0; i < n; i++) {
    results.push({ year, q });
    q--;
    if (q < 1) { q = 4; year--; }
  }
  return results;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const args = process.argv.slice(2);
  let quarters;

  if (args[0] === 'Q' && args[1] && args[2]) {
    quarters = [{ year: parseInt(args[1]), q: parseInt(args[2]) }];
    log(`Manual mode: processing ${args[1]}Q${args[2]}`);
  } else if (args[0] === 'full') {
    quarters = getCurrentQuarters(8);
    log(`Full mode: processing last 8 quarters`);
  } else {
    quarters = getCurrentQuarters(2);
    log(`Default mode: processing last 2 quarters`);
  }

  log(`=== 13F worker v${WORKER_VERSION} start ===`);
  log(`OpenFIGI key: ${OPENFIGI_KEY ? 'configured' : 'none (25 req/min)'}`);

  let total = 0;
  for (const { year, q } of quarters) {
    total += await processQuarter(year, q);
    // Brief pause between quarters to be polite to EDGAR
    if (quarters.length > 1) await new Promise(r => setTimeout(r, 2000));
  }

  log(`=== 13F worker complete — ${total} total changes inserted ===`);
  process.exit(0);
}

main().catch(e => {
  log(`Fatal error: ${e.message}`);
  console.error(e);
  process.exit(1);
});
