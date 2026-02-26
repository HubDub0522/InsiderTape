// ─────────────────────────────────────────────────────────────
//  INSIDERTAPE — Backend Server
//  Source: SEC EDGAR (primary source for ALL Form 4 filings)
//
//  SEC EDGAR API rules:
//  1. User-Agent MUST be: "Company/App contact@email.com"
//  2. Max 10 requests/second to EDGAR endpoints
//  3. data.sec.gov for structured JSON (submissions, facts)
//  4. efts.sec.gov for full-text search
//  5. www.sec.gov/Archives for actual filing documents
// ─────────────────────────────────────────────────────────────

'use strict';

const express = require('express');
const cors    = require('cors');
const https   = require('https');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─────────────────────────────────────────────────────────────
//  RATE LIMITER — SEC allows max 10 req/sec
// ─────────────────────────────────────────────────────────────
const _queue = [];
let _active  = 0;
const MAX_CONCURRENT = 5;

function rateLimit(fn) {
  return new Promise((resolve, reject) => {
    _queue.push({ fn, resolve, reject });
    processQueue();
  });
}

function processQueue() {
  while (_active < MAX_CONCURRENT && _queue.length > 0) {
    const { fn, resolve, reject } = _queue.shift();
    _active++;
    fn()
      .then(v => { _active--; resolve(v); processQueue(); })
      .catch(e => { _active--; reject(e);  processQueue(); });
  }
}

// ─────────────────────────────────────────────────────────────
//  HTTP FETCH — correct SEC User-Agent
// ─────────────────────────────────────────────────────────────
function get(url) {
  return rateLimit(() => new Promise((resolve, reject) => {
    const opts = {
      headers: {
        'User-Agent':      'InsiderTape/1.0 admin@insidertape.com',
        'Accept':          'application/json, text/plain, */*',
        'Accept-Encoding': 'identity',
        'Host':            new URL(url).hostname,
      },
      timeout: 20000,
    };
    const req = https.get(url, opts, res => {
      if ([301, 302, 303].includes(res.statusCode) && res.headers.location) {
        return get(res.headers.location).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({
        status: res.statusCode,
        body:   Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout: ' + url)); });
  }));
}

// ─────────────────────────────────────────────────────────────
//  CACHE
// ─────────────────────────────────────────────────────────────
const _cache = new Map();
function fromCache(key) {
  const c = _cache.get(key);
  return c && Date.now() < c.exp ? c.val : null;
}
function toCache(key, val, ms) {
  _cache.set(key, { val, exp: Date.now() + ms });
}

// ─────────────────────────────────────────────────────────────
//  TICKER → CIK  (SEC company_tickers.json — official mapping)
// ─────────────────────────────────────────────────────────────
let _tickerMap = null;

async function tickerToCIK(symbol) {
  if (!_tickerMap) {
    console.log('Loading SEC ticker map...');
    const { status, body } = await get('https://www.sec.gov/files/company_tickers.json');
    if (status !== 200) throw new Error(`SEC ticker map: HTTP ${status}`);
    const raw = JSON.parse(body);
    _tickerMap = {};
    for (const v of Object.values(raw)) {
      _tickerMap[v.ticker.toUpperCase()] = {
        cik:  String(v.cik_str).padStart(10, '0'),
        name: v.title,
      };
    }
    console.log(`Ticker map: ${Object.keys(_tickerMap).length} entries`);
  }
  return _tickerMap[symbol.toUpperCase()] || null;
}

// ─────────────────────────────────────────────────────────────
//  PARSE FORM 4 XML
//  Handles all SEC XML nesting patterns:
//    <tag><value>123</value></tag>
//    <tag>123</tag>
// ─────────────────────────────────────────────────────────────
function xmlGet(xml, tag) {
  const patterns = [
    new RegExp(`<${tag}[^>]*>\\s*<value>\\s*([^<]+?)\\s*<\\/value>`, 'is'),
    new RegExp(`<${tag}[^>]*>\\s*([^<\\s][^<]*?)\\s*<\\/${tag}>`, 'i'),
  ];
  for (const re of patterns) {
    const m = xml.match(re);
    if (m?.[1]?.trim()) return m[1].trim();
  }
  return '';
}

function parseForm4(xml, fallbackTicker) {
  const trades  = [];
  const insider = xmlGet(xml, 'rptOwnerName');
  const title   = xmlGet(xml, 'officerTitle');
  const ticker  = xmlGet(xml, 'issuerTradingSymbol') || fallbackTicker || '';
  const company = xmlGet(xml, 'issuerName');
  const filed   = xmlGet(xml, 'periodOfReport');

  if (!insider || !ticker) return trades;

  function parseBlock(block, isDeriv) {
    const code   = xmlGet(block, 'transactionCode') || (isDeriv ? 'A' : '');
    const date   = xmlGet(block, 'transactionDate');
    if (!code || !date) return;

    const sharesStr = xmlGet(block, 'transactionShares') ||
                      (isDeriv ? xmlGet(block, 'underlyingSecurityShares') : '') || '0';
    const priceStr  = xmlGet(block, 'transactionPricePerShare') ||
                      (isDeriv ? xmlGet(block, 'exercisePrice') : '') || '0';
    const ownedStr  = xmlGet(block, 'sharesOwnedFollowingTransaction') || '0';

    const shares = Math.abs(parseFloat(sharesStr) || 0);
    const price  = Math.abs(parseFloat(priceStr)  || 0);
    const owned  = Math.abs(parseFloat(ownedStr)  || 0);

    trades.push({
      ticker,
      company,
      insider,
      title,
      trade:  date,
      filing: filed || date,
      type:   code,
      qty:    Math.round(shares),
      price:  +price.toFixed(2),
      value:  Math.round(shares * price),
      owned:  Math.round(owned),
    });
  }

  let m;
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
  const dRe  = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/g;
  while ((m = ndRe.exec(xml)) !== null) parseBlock(m[1], false);
  while ((m = dRe.exec(xml))  !== null) parseBlock(m[1], true);

  return trades;
}

// ─────────────────────────────────────────────────────────────
//  FETCH + PARSE ONE FORM 4 FILING
//  accession: "0001234567-24-000001" or "000123456724000001"
//  cik: numeric string (with or without leading zeros)
// ─────────────────────────────────────────────────────────────
async function fetchFiling(accession, cik, fallbackTicker) {
  // Normalise accession to no-dash 18-digit format
  const clean = accession.replace(/-/g, '').padStart(18, '0');

  // The filer CIK is embedded in the accession number (first 10 digits)
  // e.g. "0001059235-26-000002" -> filer CIK = 1059235
  // But for company submissions, the issuer CIK is what we have.
  // Try both: the passed CIK and the one embedded in the accession number.
  const filerCIKFromAcc = parseInt(clean.slice(0, 10), 10);
  const passedCIK       = cik ? parseInt(cik, 10) : null;

  // Always try the CIK embedded in accession first (it IS the filer's CIK)
  // Only add passedCIK as fallback if it differs
  const cikCandidates = [...new Set([filerCIKFromAcc, passedCIK])].filter(Boolean);

  for (const cikInt of cikCandidates) {
    try {
      const base = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${clean}/`;
      const { status: idxStatus, body: idxBody } = await get(base + 'index.json');
      if (idxStatus !== 200) continue;

      const items   = JSON.parse(idxBody)?.directory?.item || [];
      const xmlFile = items.find(f =>
        typeof f.name === 'string' &&
        f.name.endsWith('.xml') &&
        !f.name.toLowerCase().includes('xsl') &&
        !f.name.toLowerCase().includes('style') &&
        !f.name.endsWith('_htm.xml')
      );
      if (!xmlFile) continue;

      const { status: xmlStatus, body: xml } = await get(base + xmlFile.name);
      if (xmlStatus !== 200) continue;

      const trades = parseForm4(xml, fallbackTicker);
      return trades; // return even if empty — filing was valid
    } catch(e) { continue; }
  }

  return [];
}

// ─────────────────────────────────────────────────────────────
//  GET ALL FORM 4s FOR A COMPANY
//  Uses data.sec.gov/submissions — returns structured JSON
//  with full filing history (not just recent 40)
// ─────────────────────────────────────────────────────────────
async function getAllForm4s(cik, symbol) {
  const paddedCIK = String(cik).padStart(10, '0');
  const url       = `https://data.sec.gov/submissions/CIK${paddedCIK}.json`;

  const { status, body } = await get(url);
  if (status !== 200) throw new Error(`Submissions API: HTTP ${status} for CIK ${cik}`);

  const data    = JSON.parse(body);
  const recent  = data.filings?.recent || {};
  const forms   = recent.form             || [];
  const accNos  = recent.accessionNumber  || [];
  const dates   = recent.filingDate       || [];

  // Collect Form 4 + 4/A accession numbers from recent batch
  const form4s = [];
  for (let i = 0; i < forms.length; i++) {
    if (forms[i] === '4' || forms[i] === '4/A') {
      form4s.push({ acc: accNos[i], date: dates[i] });
    }
  }

  // Also fetch older filing batches (SEC paginates after ~1000 filings)
  if (data.filings?.files?.length) {
    for (const file of data.filings.files) {
      try {
        const { status: fs, body: fb } =
          await get(`https://data.sec.gov/submissions/${file.name}`);
        if (fs !== 200) continue;
        const fd = JSON.parse(fb);
        const ff = fd.form || [], fa = fd.accessionNumber || [], fd2 = fd.filingDate || [];
        for (let i = 0; i < ff.length; i++) {
          if (ff[i] === '4' || ff[i] === '4/A') {
            form4s.push({ acc: fa[i], date: fd2[i] });
          }
        }
      } catch(e) { /* skip bad batch */ }
    }
  }

  console.log(`${symbol} (CIK ${cik}): ${form4s.length} Form 4 filings total`);

  // Sort newest first, take up to 100 most recent
  form4s.sort((a, b) => (b.date || '').localeCompare(a.date || ''));
  const toFetch = form4s.slice(0, 100);

  // Process in batches of 10 to avoid overwhelming the rate limiter
  const allTrades = [];
  const BATCH_SIZE = 10;
  for (let i = 0; i < toFetch.length; i += BATCH_SIZE) {
    const batch = toFetch.slice(i, i + BATCH_SIZE);
    await Promise.allSettled(batch.map(async ({ acc }) => {
      try {
        const trades = await fetchFiling(acc, null, symbol);
        allTrades.push(...trades);
      } catch(e) { /* skip */ }
    }));
    console.log(`  ${symbol} batch ${Math.floor(i/BATCH_SIZE)+1}/${Math.ceil(toFetch.length/BATCH_SIZE)}: ${allTrades.length} trades so far`);
  }

  console.log(`${symbol}: done — ${allTrades.length} total raw trades`);
  return allTrades;
}

// ─────────────────────────────────────────────────────────────
//  DEDUPLICATE trades
// ─────────────────────────────────────────────────────────────
function dedup(trades) {
  const seen = new Set();
  return trades.filter(t => {
    const k = `${t.insider}|${t.ticker}|${t.trade}|${t.type}|${t.qty}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

// ─────────────────────────────────────────────────────────────
//  ROUTE 1: GET /api/screener
//  Latest Form 4 filings across all tickers (last 5 days)
//  Uses EDGAR EFTS full-text search (confirmed working)
// ─────────────────────────────────────────────────────────────
app.get('/api/screener', async (req, res) => {
  try {
    const cached = fromCache('screener');
    if (cached) return res.json(cached);

    const start = new Date(Date.now() - 5 * 86400000).toISOString().split('T')[0];
    const end   = new Date().toISOString().split('T')[0];
    const url   = `https://efts.sec.gov/LATEST/search-index?forms=4&dateRange=custom&startdt=${start}&enddt=${end}`;

    const { status, body } = await get(url);
    if (status !== 200) throw new Error(`EFTS: HTTP ${status}`);

    const hits = JSON.parse(body)?.hits?.hits || [];
    console.log(`Screener: ${hits.length} filings from EFTS`);

    const allTrades = [];
    const hitsToUse = hits.slice(0, 60);
    const BATCH = 10;
    for (let i = 0; i < hitsToUse.length; i += BATCH) {
      const batch = hitsToUse.slice(i, i + BATCH);
      await Promise.allSettled(batch.map(async hit => {
        try {
          const src = hit._source || {};
          // EFTS _id: "0001234567:26:000004:filename.xml" — take first 3 colon-parts as accession
          const idParts = (hit._id || '').split(':');
          const acc = idParts.slice(0, 3).join('-'); // "0001234567-26-000004"
          if (!acc || acc === '--') return;
          const trades = await fetchFiling(acc, null, '');
          allTrades.push(...trades);
        } catch(e) { /* skip */ }
      }));
    }

    const result = dedup(allTrades)
      .filter(t => t.ticker && t.trade)
      .sort((a, b) => b.trade.localeCompare(a.trade));

    console.log(`Screener: ${result.length} trades`);
    toCache('screener', result, 15 * 60 * 1000);
    res.json(result);
  } catch(e) {
    console.error('Screener error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 2: GET /api/ticker?symbol=AAPL
//  All insider trades for a company — full history
// ─────────────────────────────────────────────────────────────
app.get('/api/ticker', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  try {
    const cacheKey = 'ticker:' + symbol;
    const cached   = fromCache(cacheKey);
    if (cached) return res.json(cached);

    // Resolve ticker → CIK
    const co = await tickerToCIK(symbol);
    if (!co) return res.status(404).json({ error: `${symbol} not found in SEC database` });

    console.log(`Ticker ${symbol}: CIK ${co.cik}, name: ${co.name}`);

    const allTrades = await getAllForm4s(co.cik, symbol);
    const result    = dedup(allTrades)
      .filter(t => t.trade)
      .sort((a, b) => b.trade.localeCompare(a.trade));

    console.log(`Ticker ${symbol}: ${result.length} trades after dedup`);
    toCache(cacheKey, result, 30 * 60 * 1000);
    res.json(result);
  } catch(e) {
    console.error(`Ticker ${symbol} error:`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 3: GET /api/insider?name=Tim+Cook
//  All trades by a specific person across all their companies
// ─────────────────────────────────────────────────────────────
app.get('/api/insider', async (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });

  try {
    const cacheKey = 'insider:' + name.toLowerCase();
    const cached   = fromCache(cacheKey);
    if (cached) return res.json(cached);

    const start = new Date(Date.now() - 2 * 365 * 86400000).toISOString().split('T')[0];
    const end   = new Date().toISOString().split('T')[0];
    const url   = `https://efts.sec.gov/LATEST/search-index?q=${
      encodeURIComponent('"' + name + '"')
    }&forms=4&dateRange=custom&startdt=${start}&enddt=${end}`;

    const { status, body } = await get(url);
    if (status !== 200) throw new Error(`EFTS insider search: HTTP ${status}`);

    const hits = JSON.parse(body)?.hits?.hits || [];
    console.log(`Insider "${name}": ${hits.length} filings from EFTS`);

    const lastName  = name.split(' ').pop().toLowerCase();
    const allTrades = [];

    await Promise.allSettled(hits.slice(0, 50).map(async hit => {
      try {
        const src = hit._source || {};
        const acc = (hit._id || '').replace(/:/g, '-');
        const cik = String(src.entity_id || (src.ciks || [])[0] || '').replace(/\D/g, '');
        if (!acc || !cik) return;
        const trades = await fetchFiling(acc, null, '');
        // Filter to only trades from this person
        allTrades.push(...trades.filter(t =>
          t.insider.toLowerCase().includes(lastName)
        ));
      } catch(e) { /* skip */ }
    }));

    const result = dedup(allTrades)
      .filter(t => t.trade)
      .sort((a, b) => b.trade.localeCompare(a.trade));

    console.log(`Insider "${name}": ${result.length} trades`);
    toCache(cacheKey, result, 60 * 60 * 1000);
    res.json(result);
  } catch(e) {
    console.error(`Insider "${name}" error:`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  DIAGNOSTIC — test every component individually
// ─────────────────────────────────────────────────────────────
app.get('/api/diag', async (req, res) => {
  const out = {};

  // 1. Ticker map
  try {
    const co = await tickerToCIK('AAPL');
    out.ticker_map = { ok: true, aapl_cik: co?.cik, entries: Object.keys(_tickerMap || {}).length };
  } catch(e) { out.ticker_map = { ok: false, error: e.message }; }

  // 2. EFTS search
  try {
    const start = new Date(Date.now() - 3 * 86400000).toISOString().split('T')[0];
    const { status, body } = await get(`https://efts.sec.gov/LATEST/search-index?forms=4&dateRange=custom&startdt=${start}&enddt=${new Date().toISOString().split('T')[0]}`);
    const hits = JSON.parse(body)?.hits?.hits || [];
    out.efts_search = { ok: status === 200, status, hits: hits.length, sample_id: hits[0]?._id };
  } catch(e) { out.efts_search = { ok: false, error: e.message }; }

  // 3. Submissions API for AAPL
  try {
    const co  = await tickerToCIK('AAPL');
    const { status, body } = await get(`https://data.sec.gov/submissions/CIK${co.cik}.json`);
    const d   = JSON.parse(body);
    const f4s = (d.filings?.recent?.form || []).filter(f => f === '4' || f === '4/A').length;
    out.submissions_api = { ok: status === 200, status, name: d.name, form4s_in_recent: f4s };
  } catch(e) { out.submissions_api = { ok: false, error: e.message }; }

  // 4. Parse one real AAPL filing
  try {
    const co  = await tickerToCIK('AAPL');
    const { body } = await get(`https://data.sec.gov/submissions/CIK${co.cik}.json`);
    const d   = JSON.parse(body);
    const forms  = d.filings?.recent?.form || [];
    const accNos = d.filings?.recent?.accessionNumber || [];
    const idx4   = forms.findIndex(f => f === '4');
    if (idx4 >= 0) {
      const trades = await fetchFiling(accNos[idx4], co.cik, 'AAPL');
      out.xml_parse = { ok: true, acc: accNos[idx4], trades: trades.length, sample: trades[0] };
    } else {
      out.xml_parse = { ok: false, error: 'No Form 4 found in recent AAPL filings' };
    }
  } catch(e) { out.xml_parse = { ok: false, error: e.message }; }

  res.json(out);
});


// ─────────────────────────────────────────────────────────────
//  TEST — step by step trace for one ticker
// ─────────────────────────────────────────────────────────────
app.get('/api/test', async (req, res) => {
  const symbol = (req.query.s || 'AAPL').toUpperCase();
  const out = { symbol, steps: [] };

  try {
    // Step 1: CIK lookup
    const co = await tickerToCIK(symbol);
    out.steps.push({ step: '1_cik', ok: !!co, cik: co?.cik, name: co?.name });
    if (!co) return res.json(out);

    // Step 2: Submissions fetch
    const paddedCIK = String(co.cik).padStart(10, '0');
    const { status, body } = await get(`https://data.sec.gov/submissions/CIK${paddedCIK}.json`);
    const data = JSON.parse(body);
    const forms  = data.filings?.recent?.form || [];
    const accNos = data.filings?.recent?.accessionNumber || [];
    const dates  = data.filings?.recent?.filingDate || [];
    const form4s = [];
    for (let i = 0; i < forms.length; i++) {
      if (forms[i] === '4' || forms[i] === '4/A') form4s.push({ acc: accNos[i], date: dates[i] });
    }
    out.steps.push({ step: '2_submissions', ok: status === 200, status, total_filings: forms.length, form4s: form4s.length, sample_acc: form4s[0]?.acc });
    if (!form4s.length) return res.json(out);

    // Step 3: Try fetching first filing — show exact URL tried
    const { acc } = form4s[0];
    const clean = acc.replace(/-/g, '').padStart(18, '0');
    const filerCIK = parseInt(clean.slice(0, 10), 10);
    const companyCIK = parseInt(co.cik, 10);
    out.steps.push({ step: '3_acc_parse', acc, clean, filerCIK, companyCIK });

    // Step 4: Try index.json with filer CIK
    const url1 = `https://www.sec.gov/Archives/edgar/data/${filerCIK}/${clean}/index.json`;
    const url2 = `https://www.sec.gov/Archives/edgar/data/${companyCIK}/${clean}/index.json`;
    const r1 = await get(url1).catch(e => ({ status: 0, body: e.message }));
    const r2 = await get(url2).catch(e => ({ status: 0, body: e.message }));
    out.steps.push({
      step: '4_index_fetch',
      filerCIK_url: url1, filerCIK_status: r1.status,
      companyCIK_url: url2, companyCIK_status: r2.status,
      filerCIK_items: r1.status === 200 ? (JSON.parse(r1.body)?.directory?.item?.length || 0) : 'N/A',
    });

    // Step 5: If index worked, try getting the XML
    if (r1.status === 200) {
      const items = JSON.parse(r1.body)?.directory?.item || [];
      const xmlFile = items.find(f => typeof f.name === 'string' && f.name.endsWith('.xml') && !f.name.includes('xsl') && !f.name.endsWith('_htm.xml'));
      out.steps.push({ step: '5_xml_file', found: !!xmlFile, name: xmlFile?.name, all_files: items.map(f => f.name) });
      if (xmlFile) {
        const { status: xs, body: xml } = await get(`https://www.sec.gov/Archives/edgar/data/${filerCIK}/${clean}/${xmlFile.name}`);
        const trades = parseForm4(xml, symbol);
        out.steps.push({ step: '6_parse', xml_status: xs, xml_length: xml.length, trades: trades.length, sample: trades[0] });
      }
    }

  } catch(e) {
    out.error = e.message;
  }

  res.json(out);
});
// ─────────────────────────────────────────────────────────────
//  ROUTE: GET /api/price?symbol=AAPL
//  Financial Modeling Prep — free tier, no server blocking
//  Returns [{time, open, high, low, close, volume}] oldest→newest
// ─────────────────────────────────────────────────────────────
const FMP_KEY = 'OJfv9bPVEMrnwPX7noNpJLZCFLLFTmlu';

app.get('/api/price', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  try {
    const cacheKey = 'price:' + symbol;
    const cached   = fromCache(cacheKey);
    if (cached) return res.json(cached);

    // FMP daily historical — returns 5 years by default, newest first
    const url = `https://financialmodelingprep.com/api/v3/historical-price-full/${encodeURIComponent(symbol)}?apikey=${FMP_KEY}`;
    const { status, body } = await get(url);

    if (status !== 200) return res.status(502).json({ error: `FMP returned ${status}` });

    const data = JSON.parse(body);
    if (!data?.historical?.length) return res.status(404).json({ error: `No price data for ${symbol}` });

    // FMP returns newest-first — reverse to oldest-first for charting
    const bars = data.historical
      .slice()
      .reverse()
      .map(d => ({
        time:   d.date,          // YYYY-MM-DD string — perfect for Lightweight Charts
        open:   +d.open.toFixed(2),
        high:   +d.high.toFixed(2),
        low:    +d.low.toFixed(2),
        close:  +d.close.toFixed(2),
        volume: d.volume || 0,
      }))
      .filter(d => d.close > 0);

    console.log(`Price ${symbol}: ${bars.length} bars from FMP`);
    toCache(cacheKey, bars, 60 * 60 * 1000); // cache 1 hour
    res.json(bars);
  } catch(e) {
    console.error(`Price ${symbol} error:`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  HEALTH
// ─────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) =>
  res.json({ status: 'ok', version: 'final', time: new Date().toISOString() }));

app.listen(PORT, () =>
  console.log(`InsiderTape running on port ${PORT}`));
