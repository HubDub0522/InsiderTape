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
  // Normalise accession: remove dashes, ensure 18 digits
  const clean  = accession.replace(/-/g, '').replace(/^0+/, '').padStart(18, '0');
  const cikInt = parseInt(cik, 10);
  const base   = `https://www.sec.gov/Archives/edgar/data/${cikInt}/${clean}/`;

  // Get filing index to find the XML document
  const { status: idxStatus, body: idxBody } = await get(base + 'index.json');
  if (idxStatus !== 200) return [];

  const items   = JSON.parse(idxBody)?.directory?.item || [];
  // Find primary Form 4 XML (not stylesheet, not _htm.xml)
  const xmlFile = items.find(f =>
    typeof f.name === 'string' &&
    f.name.endsWith('.xml') &&
    !f.name.toLowerCase().includes('xsl') &&
    !f.name.toLowerCase().includes('style') &&
    !f.name.endsWith('_htm.xml')
  );
  if (!xmlFile) return [];

  const { status: xmlStatus, body: xml } = await get(base + xmlFile.name);
  if (xmlStatus !== 200) return [];

  return parseForm4(xml, fallbackTicker);
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

  // Fetch all in parallel (rate limiter handles concurrency)
  const allTrades = [];
  await Promise.allSettled(toFetch.map(async ({ acc }) => {
    try {
      const trades = await fetchFiling(acc, cik, symbol);
      allTrades.push(...trades);
    } catch(e) { /* skip failed filings */ }
  }));

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
    await Promise.allSettled(hits.slice(0, 60).map(async hit => {
      try {
        const src = hit._source || {};
        // _id format: "0001234567:24:000001" or similar
        const acc = (hit._id || '').replace(/:/g, '-');
        const cik = String(src.entity_id || (src.ciks || [])[0] || '').replace(/\D/g, '');
        if (!acc || !cik) return;
        const trades = await fetchFiling(acc, cik, '');
        allTrades.push(...trades);
      } catch(e) { /* skip */ }
    }));

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
        const trades = await fetchFiling(acc, cik, '');
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
//  HEALTH
// ─────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) =>
  res.json({ status: 'ok', version: 'final', time: new Date().toISOString() }));

app.listen(PORT, () =>
  console.log(`InsiderTape running on port ${PORT}`));
