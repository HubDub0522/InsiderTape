// ─────────────────────────────────────────────────────────────
//  INSIDERTAPE — Backend Server v2
//  Data: SEC EDGAR data.sec.gov JSON API (no XML parsing)
//  Price: Stooq CSV → Yahoo Finance fallback
// ─────────────────────────────────────────────────────────────

const express = require('express');
const cors    = require('cors');
const https   = require('https');
const http    = require('http');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─────────────────────────────────────────────────────────────
//  FETCH HELPER
// ─────────────────────────────────────────────────────────────
function fetchURL(url, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const opts = {
      headers: {
        'User-Agent': 'InsiderTape/2.0 contact@insidertape.com',
        'Accept':     'application/json, text/plain, text/csv, */*',
        ...extraHeaders,
      },
      timeout: 20000,
    };
    const req = lib.get(url, opts, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        return fetchURL(res.headers.location, extraHeaders).then(resolve).catch(reject);
      }
      let data = '';
      res.setEncoding('utf8');
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ─────────────────────────────────────────────────────────────
//  CACHE
// ─────────────────────────────────────────────────────────────
const cache = {};
const getCache = k => { const c = cache[k]; return c && Date.now() < c.exp ? c.val : null; };
const setCache = (k, v, ms) => { cache[k] = { val: v, exp: Date.now() + ms }; };

// ─────────────────────────────────────────────────────────────
//  TICKER → CIK  (uses SEC company tickers JSON)
// ─────────────────────────────────────────────────────────────
let tickerMap = null;
async function getTickerMap() {
  if (tickerMap) return tickerMap;
  const cached = getCache('tickerMap');
  if (cached) { tickerMap = cached; return tickerMap; }
  try {
    const { body } = await fetchURL('https://www.sec.gov/files/company_tickers.json');
    const data = JSON.parse(body);
    tickerMap = {};
    for (const v of Object.values(data)) {
      tickerMap[v.ticker.toUpperCase()] = {
        cik:  String(v.cik_str).padStart(10, '0'),
        name: v.title,
      };
    }
    setCache('tickerMap', tickerMap, 24 * 60 * 60 * 1000); // 24h
    return tickerMap;
  } catch(e) {
    console.error('tickerMap error:', e.message);
    return {};
  }
}

// ─────────────────────────────────────────────────────────────
//  PARSE FORM 4 XML  (only called for actual filing XML)
// ─────────────────────────────────────────────────────────────
function getXmlVal(block, tag) {
  const m = block.match(new RegExp(`<${tag}[^>]*>\\s*(?:<value>)?\\s*([\\d.\\-]+)\\s*(?:</value>)?\\s*</${tag}>`, 'i'))
         || block.match(new RegExp(`<${tag}[^>]*>([^<]+)<`, 'i'));
  return m ? m[1].trim() : '';
}

function parseForm4XML(xml, fallbackTicker) {
  const trades = [];
  const insiderName  = (xml.match(/<rptOwnerName>([^<]+)<\/rptOwnerName>/)  || [])[1]?.trim() || '';
  const insiderTitle = (xml.match(/<officerTitle>([^<]+)<\/officerTitle>/)  || [])[1]?.trim() || '';
  const ticker       = (xml.match(/<issuerTradingSymbol>([^<]+)<\/issuerTradingSymbol>/) || [])[1]?.trim() || fallbackTicker || '';
  const company      = (xml.match(/<issuerName>([^<]+)<\/issuerName>/)      || [])[1]?.trim() || '';
  const filingDate   = (xml.match(/<periodOfReport>([^<]+)<\/periodOfReport>/) || [])[1]?.trim() || '';

  if (!ticker) return trades;

  const parseBlock = (block, isDeriv) => {
    const code  = getXmlVal(block, 'transactionCode') || (isDeriv ? 'A' : '');
    const date  = getXmlVal(block, 'transactionDate');
    const sharesRaw = getXmlVal(block, 'transactionShares') || (isDeriv ? getXmlVal(block, 'underlyingSecurityShares') : '');
    const priceRaw  = getXmlVal(block, 'transactionPricePerShare') || (isDeriv ? getXmlVal(block, 'exercisePrice') : '');
    const ownedRaw  = getXmlVal(block, 'sharesOwnedFollowingTransaction');
    if (!date || !code) return null;
    const shares = parseFloat(sharesRaw) || 0;
    const price  = parseFloat(priceRaw)  || 0;
    return {
      ticker, company,
      insider: insiderName,
      title:   insiderTitle,
      trade:   date,
      filing:  filingDate || date,
      type:    code,
      qty:     Math.round(Math.abs(shares)),
      price:   +price.toFixed(2),
      value:   Math.round(Math.abs(shares * price)),
      owned:   Math.round(parseFloat(ownedRaw) || 0),
    };
  };

  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
  const dRe  = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/g;
  let m;
  while ((m = ndRe.exec(xml)) !== null) { const t = parseBlock(m[1], false); if (t) trades.push(t); }
  while ((m = dRe.exec(xml))  !== null) { const t = parseBlock(m[1], true);  if (t) trades.push(t); }
  return trades;
}

// ─────────────────────────────────────────────────────────────
//  FETCH ALL FORM 4 FILINGS FOR A CIK  (using submissions API)
//  data.sec.gov/submissions/CIK########.json returns structured
//  JSON list of every filing — no XML scraping needed for the list
// ─────────────────────────────────────────────────────────────
async function fetchAllForm4sForCIK(cik, symbol) {
  const url  = `https://data.sec.gov/submissions/CIK${cik}.json`;
  const { body } = await fetchURL(url);
  const data = JSON.parse(body);

  const recent = data.filings?.recent || {};
  const forms  = recent.form        || [];
  const accNos = recent.accessionNumber || [];
  const dates  = recent.filingDate  || [];

  // Collect all Form 4 accession numbers
  const form4s = [];
  for (let i = 0; i < forms.length; i++) {
    if (forms[i] === '4' || forms[i] === '4/A') {
      form4s.push({ accNo: accNos[i], date: dates[i] });
    }
  }

  // Also check older filings files if they exist
  if (data.filings?.files?.length) {
    for (const file of data.filings.files) {
      try {
        const { body: fb } = await fetchURL(`https://data.sec.gov/submissions/${file.name}`);
        const fd = JSON.parse(fb);
        const ff = fd.form || [], fa = fd.accessionNumber || [], fdt = fd.filingDate || [];
        for (let i = 0; i < ff.length; i++) {
          if (ff[i] === '4' || ff[i] === '4/A') {
            form4s.push({ accNo: fa[i], date: fdt[i] });
          }
        }
      } catch(e) {}
    }
  }

  console.log(`CIK ${cik} (${symbol}): found ${form4s.length} Form 4 filings`);

  // Fetch and parse each filing XML in parallel (limit 60 most recent)
  const allTrades = [];
  await Promise.allSettled(form4s.slice(0, 60).map(async ({ accNo, date }) => {
    try {
      const clean   = accNo.replace(/-/g, '');
      const baseUrl = `https://www.sec.gov/Archives/edgar/data/${parseInt(cik)}/${clean}/`;

      // Get filing index to find the XML file
      const { body: idxBody } = await fetchURL(baseUrl + 'index.json');
      const idx   = JSON.parse(idxBody);
      const files = idx?.directory?.item || [];
      const xmlF  = files.find(f =>
        typeof f.name === 'string' &&
        f.name.endsWith('.xml') &&
        !f.name.toLowerCase().includes('xsl') &&
        !f.name.toLowerCase().includes('style')
      );
      if (!xmlF) return;

      const { body: xml } = await fetchURL(baseUrl + xmlF.name);
      const trades = parseForm4XML(xml, symbol);
      allTrades.push(...trades);
    } catch(e) {}
  }));

  return allTrades;
}

// ─────────────────────────────────────────────────────────────
//  ROUTE 1: GET /api/screener
//  Latest Form 4 filings across all companies (last 7 days)
// ─────────────────────────────────────────────────────────────
app.get('/api/screener', async (req, res) => {
  try {
    const cached = getCache('screener');
    if (cached) return res.json(cached);

    const start = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
    const end   = new Date().toISOString().split('T')[0];
    const url   = `https://efts.sec.gov/LATEST/search-index?forms=4&dateRange=custom&startdt=${start}&enddt=${end}`;

    const { body } = await fetchURL(url);
    const data     = JSON.parse(body);
    const hits     = data?.hits?.hits || [];
    console.log(`Screener: ${hits.length} filings from EDGAR search`);

    const allTrades = [];
    await Promise.allSettled(hits.slice(0, 40).map(async (hit) => {
      try {
        const src       = hit._source || {};
        const accession = (hit._id || '').replace(/:/g, '');
        const cik       = src.entity_id || src.ciks?.[0] || '';
        if (!accession || !cik) return;

        const clean   = accession.padStart(18, '0');
        const baseUrl = `https://www.sec.gov/Archives/edgar/data/${parseInt(cik)}/${clean}/`;
        const { body: idxBody } = await fetchURL(baseUrl + 'index.json');
        const idx   = JSON.parse(idxBody);
        const files = idx?.directory?.item || [];
        const xmlF  = files.find(f =>
          typeof f.name === 'string' && f.name.endsWith('.xml') &&
          !f.name.toLowerCase().includes('xsl')
        );
        if (!xmlF) return;
        const { body: xml } = await fetchURL(baseUrl + xmlF.name);
        const trades = parseForm4XML(xml, '');
        allTrades.push(...trades);
      } catch(e) {}
    }));

    const result = allTrades
      .filter(t => t.ticker && t.trade)
      .sort((a, b) => new Date(b.trade) - new Date(a.trade));

    console.log(`Screener: ${result.length} trades parsed`);
    setCache('screener', result, 15 * 60 * 1000);
    res.json(result);
  } catch(e) {
    console.error('/api/screener error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 2: GET /api/ticker?symbol=AAPL
//  All insider trades for a company using submissions API
// ─────────────────────────────────────────────────────────────
app.get('/api/ticker', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  try {
    const cacheKey = 'ticker_' + symbol;
    const cached   = getCache(cacheKey);
    if (cached) return res.json(cached);

    const map = await getTickerMap();
    const co  = map[symbol];
    if (!co) return res.status(404).json({ error: `Ticker ${symbol} not found in SEC database` });

    const trades = await fetchAllForm4sForCIK(co.cik, symbol);
    const result = trades
      .filter(t => t.trade)
      .sort((a, b) => new Date(b.trade) - new Date(a.trade));

    // Deduplicate
    const seen = new Set();
    const deduped = result.filter(t => {
      const k = `${t.insider}${t.trade}${t.type}${t.qty}`;
      if (seen.has(k)) return false;
      seen.add(k); return true;
    });

    console.log(`Ticker ${symbol}: ${deduped.length} trades`);
    setCache(cacheKey, deduped, 30 * 60 * 1000);
    res.json(deduped);
  } catch(e) {
    console.error('/api/ticker error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 3: GET /api/insider?name=Tim+Cook
//  Search EDGAR full-text for a person's filings
// ─────────────────────────────────────────────────────────────
app.get('/api/insider', async (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });

  try {
    const cacheKey = 'insider_' + name.toLowerCase();
    const cached   = getCache(cacheKey);
    if (cached) return res.json(cached);

    const start = new Date(Date.now() - 2 * 365 * 86400000).toISOString().split('T')[0];
    const url   = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent('"' + name + '"')}&forms=4&dateRange=custom&startdt=${start}&enddt=${new Date().toISOString().split('T')[0]}`;

    const { body } = await fetchURL(url);
    const data  = JSON.parse(body);
    const hits  = data?.hits?.hits || [];
    console.log(`Insider "${name}": ${hits.length} filings found`);

    const allTrades = [];
    await Promise.allSettled(hits.slice(0, 30).map(async (hit) => {
      try {
        const src       = hit._source || {};
        const accession = (hit._id || '').replace(/:/g, '');
        const cik       = src.entity_id || src.ciks?.[0] || '';
        if (!accession || !cik) return;

        const clean   = accession.padStart(18, '0');
        const baseUrl = `https://www.sec.gov/Archives/edgar/data/${parseInt(cik)}/${clean}/`;
        const { body: idxBody } = await fetchURL(baseUrl + 'index.json');
        const idx   = JSON.parse(idxBody);
        const files = idx?.directory?.item || [];
        const xmlF  = files.find(f =>
          typeof f.name === 'string' && f.name.endsWith('.xml') &&
          !f.name.toLowerCase().includes('xsl')
        );
        if (!xmlF) return;
        const { body: xml } = await fetchURL(baseUrl + xmlF.name);
        const trades = parseForm4XML(xml, '');
        // Only keep trades from this person
        allTrades.push(...trades.filter(t =>
          t.insider.toLowerCase().includes(name.toLowerCase().split(' ').pop())
        ));
      } catch(e) {}
    }));

    const seen = new Set();
    const result = allTrades
      .filter(t => {
        const k = `${t.ticker}${t.trade}${t.type}${t.qty}`;
        if (seen.has(k)) return false;
        seen.add(k); return t.trade;
      })
      .sort((a, b) => new Date(b.trade) - new Date(a.trade));

    console.log(`Insider "${name}": ${result.length} trades`);
    setCache(cacheKey, result, 60 * 60 * 1000);
    res.json(result);
  } catch(e) {
    console.error('/api/insider error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 4: GET /api/price?symbol=AAPL
//  Stooq CSV first, Yahoo Finance fallback
// ─────────────────────────────────────────────────────────────
function parseStooqCSV(csv) {
  const lines = csv.trim().split('\n');
  if (lines.length < 2) return [];
  const bars = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].trim().split(',');
    if (cols.length < 5) continue;
    const [date, open, high, low, close] = cols;
    if (!date || date === 'Date') continue;
    const ts = Math.floor(new Date(date + 'T12:00:00Z').getTime() / 1000);
    const o = parseFloat(open), h = parseFloat(high),
          l = parseFloat(low),  c = parseFloat(close);
    if (!ts || !c || c <= 0) continue;
    bars.push({ time: ts, open: +o.toFixed(4), high: +h.toFixed(4),
                low: +l.toFixed(4), close: +c.toFixed(4) });
  }
  return bars.reverse();
}

app.get('/api/price', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  try {
    const cacheKey = 'price_' + symbol;
    const cached   = getCache(cacheKey);
    if (cached) return res.json(cached);

    let bars = [];

    // Try 1: Stooq
    try {
      const { body: csv } = await fetchURL(
        `https://stooq.com/q/d/l/?s=${symbol.toLowerCase()}.us&i=d`
      );
      if (csv && !csv.includes('No data') && csv.includes(',')) {
        bars = parseStooqCSV(csv);
      }
    } catch(e) {}

    // Try 2: Yahoo Finance
    if (bars.length < 10) {
      const to = Math.floor(Date.now() / 1000);
      const from = to - 2 * 365 * 86400;
      for (const host of ['query1', 'query2']) {
        try {
          const { body } = await fetchURL(
            `https://${host}.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&period1=${from}&period2=${to}&includePrePost=false`
          );
          const d = JSON.parse(body);
          const r = d?.chart?.result?.[0];
          if (!r?.timestamp?.length) continue;
          const q = r.indicators?.quote?.[0] || {};
          bars = r.timestamp
            .map((t, i) => ({
              time: t,
              open:  +((q.open?.[i]  || 0).toFixed(4)),
              high:  +((q.high?.[i]  || 0).toFixed(4)),
              low:   +((q.low?.[i]   || 0).toFixed(4)),
              close: +((q.close?.[i] || 0).toFixed(4)),
            }))
            .filter(d => d.close > 0);
          if (bars.length > 10) break;
        } catch(e) {}
      }
    }

    if (bars.length < 5) return res.status(404).json({ error: `No price data for ${symbol}` });

    console.log(`Price ${symbol}: ${bars.length} bars`);
    setCache(cacheKey, bars, 60 * 60 * 1000);
    res.json(bars);
  } catch(e) {
    console.error('/api/price error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  HEALTH CHECK
// ─────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

app.listen(PORT, () => console.log(`InsiderTape server running on port ${PORT}`));
