// ─────────────────────────────────────────────────────────────
//  INSIDERTAPE — Backend Server
//  Data source: SEC EDGAR (official US government API, free)
// ─────────────────────────────────────────────────────────────

const express = require('express');
const cors    = require('cors');
const https   = require('https');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─────────────────────────────────────────────────────────────
//  HELPER: fetch a URL server-side
// ─────────────────────────────────────────────────────────────
function fetchURL(url) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'InsiderTape/1.0 contact@insidertape.com',
        'Accept':     'application/json, text/html, text/plain, */*',
        'Accept-Encoding': 'identity',
      },
      timeout: 15000,
    };
    https.get(url, options, (res) => {
      // Follow redirects
      if (res.statusCode === 301 || res.statusCode === 302) {
        return fetchURL(res.headers.location).then(resolve).catch(reject);
      }
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    }).on('error', reject).on('timeout', () => reject(new Error('Request timed out')));
  });
}

// ─────────────────────────────────────────────────────────────
//  CACHE
// ─────────────────────────────────────────────────────────────
const cache = {};
function getCache(key)          { const c = cache[key]; return c && Date.now() < c.exp ? c.val : null; }
function setCache(key, val, ms) { cache[key] = { val, exp: Date.now() + ms }; }

// ─────────────────────────────────────────────────────────────
//  PARSE FORM 4 XML
// ─────────────────────────────────────────────────────────────
function parseForm4(xml, fallbackTicker) {
  const trades = [];

  const insiderName  = (xml.match(/<rptOwnerName>([^<]+)<\/rptOwnerName>/)  || [])[1]?.trim() || '';
  const insiderTitle = (xml.match(/<officerTitle>([^<]+)<\/officerTitle>/)  || [])[1]?.trim() || '';
  const ticker       = (xml.match(/<issuerTradingSymbol>([^<]+)<\/issuerTradingSymbol>/) || [])[1]?.trim() || fallbackTicker || '';
  const company      = (xml.match(/<issuerName>([^<]+)<\/issuerName>/)      || [])[1]?.trim() || '';
  const filingDate   = (xml.match(/<periodOfReport>([^<]+)<\/periodOfReport>/) || [])[1]?.trim() || '';

  if (!insiderName || !ticker) return trades;

  // Robust value extractor — handles all SEC XML nesting patterns:
  //   <tag><value>123</value></tag>
  //   <tag>123</tag>
  //   <tag footnotesId="F1"><value>123</value></tag>
  const getVal = (block, tag) => {
    const patterns = [
      new RegExp(`<${tag}[^>]*>\s*<value>\s*([^<]+?)\s*<\/value>`, 'is'),
      new RegExp(`<${tag}[^>]*>\s*([\d.\-]+)\s*<\/`, 'i'),
    ];
    for (const re of patterns) {
      const m = block.match(re);
      if (m && m[1].trim()) return m[1].trim();
    }
    return '';
  };

  // Non-derivative transactions (actual share purchases/sales)
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
  let m;
  while ((m = ndRe.exec(xml)) !== null) {
    const block     = m[1];
    const transCode = getVal(block, 'transactionCode');
    const date      = getVal(block, 'transactionDate');
    const sharesRaw = getVal(block, 'transactionShares');
    const priceRaw  = getVal(block, 'transactionPricePerShare');
    const ownedRaw  = getVal(block, 'sharesOwnedFollowingTransaction');

    if (!date || !transCode) continue;

    const shares = parseFloat(sharesRaw) || 0;
    const price  = parseFloat(priceRaw)  || 0;
    const owned  = parseFloat(ownedRaw)  || 0;

    trades.push({
      ticker, company, insider: insiderName, title: insiderTitle,
      trade: date, filing: filingDate || date,
      type:  transCode,
      qty:   Math.round(Math.abs(shares)),
      price: +price.toFixed(2),
      value: Math.round(Math.abs(shares * price)),
      owned: Math.round(owned),
    });
  }

  // Derivative transactions (options, warrants, convertibles)
  const dRe = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/g;
  while ((m = dRe.exec(xml)) !== null) {
    const block     = m[1];
    const transCode = getVal(block, 'transactionCode') || 'A';
    const date      = getVal(block, 'transactionDate');
    const sharesRaw = getVal(block, 'transactionShares')
                   || getVal(block, 'underlyingSecurityShares');
    const priceRaw  = getVal(block, 'exercisePrice')
                   || getVal(block, 'transactionPricePerShare');

    if (!date) continue;

    const shares = parseFloat(sharesRaw) || 0;
    const price  = parseFloat(priceRaw)  || 0;

    trades.push({
      ticker, company, insider: insiderName, title: insiderTitle,
      trade: date, filing: filingDate || date,
      type:  transCode,
      qty:   Math.round(Math.abs(shares)),
      price: +price.toFixed(2),
      value: Math.round(Math.abs(shares * price)),
      owned: 0,
    });
  }

  return trades;
}

// ─────────────────────────────────────────────────────────────
//  FETCH AND PARSE A SINGLE FILING INDEX → trades
// ─────────────────────────────────────────────────────────────
async function fetchFiling(accessionRaw, cik) {
  try {
    // Normalize accession number: 0001234567-24-000001
    const accession = accessionRaw.replace(/[^0-9]/g, '');
    const formatted = `${accession.slice(0,10)}-${accession.slice(10,12)}-${accession.slice(12)}`;
    const paddedCik = String(cik).padStart(10, '0');
    const baseUrl   = `https://www.sec.gov/Archives/edgar/data/${cik}/${accession}/`;
    const indexUrl  = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${paddedCik}&type=4&dateb=&owner=include&count=1&search_text=`;

    // Fetch the filing index JSON
    const indexJson = await fetchURL(`${baseUrl}index.json`);
    const index     = JSON.parse(indexJson);
    const files     = index?.directory?.item || [];

    // Find the primary XML (Form 4 data file, not the stylesheet)
    const xmlFile = files.find(f =>
      typeof f.name === 'string' &&
      f.name.endsWith('.xml') &&
      !f.name.includes('xsl') &&
      !f.name.includes('stylesheet')
    );
    if (!xmlFile) return [];

    const xml = await fetchURL(baseUrl + xmlFile.name);
    return parseForm4(xml);
  } catch(e) {
    return [];
  }
}

// ─────────────────────────────────────────────────────────────
//  ROUTE 1: GET /api/screener
//  Uses EDGAR full-text search to find recent Form 4 filings
// ─────────────────────────────────────────────────────────────
app.get('/api/screener', async (req, res) => {
  try {
    const cached = getCache('screener');
    if (cached) return res.json(cached);

    const days  = 7;
    const start = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
    const end   = new Date().toISOString().split('T')[0];

    // EDGAR full-text search API — returns recent Form 4 filings as JSON
    const searchUrl = `https://efts.sec.gov/LATEST/search-index?q=%22form+4%22&forms=4&dateRange=custom&startdt=${start}&enddt=${end}&hits.hits._source=period_of_report,display_names,file_date,entity_id,file_num&hits.hits.total.value=true`;

    // Use the simpler EDGAR search endpoint
    const url  = `https://efts.sec.gov/LATEST/search-index?forms=4&dateRange=custom&startdt=${start}&enddt=${end}`;
    const raw  = await fetchURL(url);
    const data = JSON.parse(raw);

    const hits = data?.hits?.hits || [];
    console.log(`Screener: found ${hits.length} filings from EDGAR`);

    const allTrades = [];
    // Process up to 30 filings in parallel
    await Promise.allSettled(hits.slice(0, 30).map(async (hit) => {
      try {
        const src        = hit._source || {};
        const accession  = (hit._id || '').replace(/:/g, '');
        const cik        = src.entity_id || src.ciks?.[0] || '';
        if (!accession || !cik) return;

        const trades = await fetchFiling(accession, cik);
        allTrades.push(...trades);
      } catch(e) {}
    }));

    const result = allTrades
      .filter(t => t.ticker && t.trade)
      .sort((a, b) => new Date(b.trade) - new Date(a.trade));

    console.log(`Screener: parsed ${result.length} trades`);
    setCache('screener', result, 15 * 60 * 1000);
    res.json(result);

  } catch(e) {
    console.error('/api/screener error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 2: GET /api/ticker?symbol=AAPL
// ─────────────────────────────────────────────────────────────
app.get('/api/ticker', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  try {
    const cached = getCache('ticker_' + symbol);
    if (cached) return res.json(cached);

    // Step 1: Get the company CIK from the ticker
    const cikUrl  = `https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=${encodeURIComponent(symbol)}&type=4&dateb=&owner=include&count=40&search_text=&action=getcompany&output=atom`;
    const feedXml = await fetchURL(cikUrl);

    // Pull filing links from the Atom feed
    const filingLinks = [];
    const entryRe = /<entry>([\s\S]*?)<\/entry>/g;
    let m;
    while ((m = entryRe.exec(feedXml)) !== null && filingLinks.length < 40) {
      const href = (m[1].match(/<link[^>]+href="([^"]+)"/) || [])[1] || '';
      if (href.includes('/Archives/')) filingLinks.push(href);
    }

    console.log(`Ticker ${symbol}: found ${filingLinks.length} filing links`);

    const allTrades = [];
    await Promise.allSettled(filingLinks.map(async (link) => {
      try {
        // Extract CIK and accession from the URL
        const urlMatch = link.match(/\/data\/(\d+)\/(\d+)\//);
        if (!urlMatch) return;
        const cik       = urlMatch[1];
        const accession = urlMatch[2];
        const trades    = await fetchFiling(accession, cik);
        allTrades.push(...trades);
      } catch(e) {}
    }));

    const result = allTrades
      .filter(t => t.trade)
      .sort((a, b) => new Date(b.trade) - new Date(a.trade));

    console.log(`Ticker ${symbol}: parsed ${result.length} trades`);
    setCache('ticker_' + symbol, result, 30 * 60 * 1000);
    res.json(result);

  } catch(e) {
    console.error('/api/ticker error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 3: GET /api/insider?name=Tim+Cook
// ─────────────────────────────────────────────────────────────
app.get('/api/insider', async (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });

  try {
    const cacheKey = 'insider_' + name.toLowerCase();
    const cached   = getCache(cacheKey);
    if (cached) return res.json(cached);

    // Search EDGAR for Form 4 filings mentioning this person's name
    const url  = `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(name)}%22&forms=4&dateRange=custom&startdt=${
      new Date(Date.now() - 2 * 365 * 86400000).toISOString().split('T')[0]}&enddt=${
      new Date().toISOString().split('T')[0]}`;
    const raw  = await fetchURL(url);
    const data = JSON.parse(raw);
    const hits = data?.hits?.hits || [];

    console.log(`Insider "${name}": found ${hits.length} filings`);

    const allTrades = [];
    await Promise.allSettled(hits.slice(0, 25).map(async (hit) => {
      try {
        const src       = hit._source || {};
        const accession = (hit._id || '').replace(/:/g, '');
        const cik       = src.entity_id || src.ciks?.[0] || '';
        if (!accession || !cik) return;
        const trades = await fetchFiling(accession, cik);
        // Only include trades by this person
        allTrades.push(...trades.filter(t =>
          t.insider.toLowerCase().includes(name.toLowerCase().split(' ')[0])
        ));
      } catch(e) {}
    }));

    const seen   = new Set();
    const result = allTrades
      .filter(t => {
        const key = `${t.ticker}${t.trade}${t.type}${t.qty}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return !!t.trade;
      })
      .sort((a, b) => new Date(b.trade) - new Date(a.trade));

    console.log(`Insider "${name}": parsed ${result.length} trades`);
    setCache(cacheKey, result, 60 * 60 * 1000);
    res.json(result);

  } catch(e) {
    console.error('/api/insider error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 4: GET /api/price?symbol=AAPL
//  Fetches 2 years of daily OHLC. Tries Stooq first (no auth
//  needed, very reliable), then Yahoo Finance as fallback.
//  Returns array of {time,open,high,low,close}
// ─────────────────────────────────────────────────────────────

// Parse Stooq CSV response into bar array
function parseStooqCSV(csv) {
  const lines = csv.trim().split('\n');
  if (lines.length < 2) return [];
  const bars = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].trim().split(',');
    if (cols.length < 5) continue;
    // Stooq format: Date,Open,High,Low,Close,Volume
    const [date, open, high, low, close] = cols;
    if (!date || date === 'Date') continue;
    const ts = Math.floor(new Date(date + 'T12:00:00Z').getTime() / 1000);
    const o = parseFloat(open), h = parseFloat(high),
          l = parseFloat(low),  c = parseFloat(close);
    if (!ts || !c || c <= 0) continue;
    bars.push({ time: ts, open: +o.toFixed(4), high: +h.toFixed(4),
                low:  +l.toFixed(4), close: +c.toFixed(4) });
  }
  // Stooq returns newest first — reverse to oldest first
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

    // ── Try 1: Stooq (free, no auth, very stable) ──────────
    try {
      const stooqUrl = `https://stooq.com/q/d/l/?s=${encodeURIComponent(symbol.toLowerCase())}.us&i=d`;
      const csv      = await fetchURL(stooqUrl);
      if (csv && !csv.includes('No data') && csv.includes(',')) {
        bars = parseStooqCSV(csv);
        if (bars.length > 10) console.log(`Price ${symbol}: ${bars.length} bars from Stooq`);
      }
    } catch(e) { console.log(`Stooq failed for ${symbol}:`, e.message); }

    // ── Try 2: Yahoo Finance v8 ─────────────────────────────
    if (bars.length < 10) {
      const to   = Math.floor(Date.now() / 1000);
      const from = to - 2 * 365 * 86400;
      const yhUrls = [
        `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&period1=${from}&period2=${to}&events=history&includePrePost=false`,
        `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&period1=${from}&period2=${to}&events=history&includePrePost=false`,
      ];
      for (const url of yhUrls) {
        try {
          const raw    = await fetchURL(url);
          const data   = JSON.parse(raw);
          const result = data?.chart?.result?.[0];
          if (!result?.timestamp?.length) continue;
          const timestamps = result.timestamp;
          const quote      = result.indicators?.quote?.[0] || {};
          bars = timestamps
            .map((t, i) => ({
              time:  t,
              open:  +((quote.open?.[i]  || 0).toFixed(4)),
              high:  +((quote.high?.[i]  || 0).toFixed(4)),
              low:   +((quote.low?.[i]   || 0).toFixed(4)),
              close: +((quote.close?.[i] || 0).toFixed(4)),
            }))
            .filter(d => d.open > 0 && d.close > 0);
          if (bars.length > 10) {
            console.log(`Price ${symbol}: ${bars.length} bars from Yahoo`);
            break;
          }
        } catch(e) { continue; }
      }
    }

    if (bars.length < 5) return res.status(404).json({ error: `No price data found for ${symbol}` });

    setCache(cacheKey, bars, 60 * 60 * 1000);
    res.json(bars);

  } catch(e) {
    console.error('/api/price error:', e.message);
    res.status(500).json({ error: e.message });
  }
});


// ─────────────────────────────────────────────────────────────
//  DEBUG: GET /api/debug?symbol=AAPL
//  Returns the first raw Form 4 XML for a symbol — dev use only
// ─────────────────────────────────────────────────────────────
app.get('/api/debug', async (req, res) => {
  const symbol = (req.query.symbol || 'AAPL').toUpperCase().trim();
  try {
    const cikUrl  = `https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=${encodeURIComponent(symbol)}&type=4&dateb=&owner=include&count=5&search_text=&action=getcompany&output=atom`;
    const feedXml = await fetchURL(cikUrl);
    const linkMatch = feedXml.match(/<link[^>]+href="([^"]+\/Archives\/[^"]+)"/);
    if (!linkMatch) return res.json({ error: 'No filing links found', feedXml: feedXml.slice(0, 2000) });

    const link      = linkMatch[1];
    const urlMatch  = link.match(/\/data\/(\d+)\/(\d+)\//);
    if (!urlMatch) return res.json({ error: 'Could not parse filing URL', link });

    const cik       = urlMatch[1];
    const accession = urlMatch[2];
    const baseUrl   = `https://www.sec.gov/Archives/edgar/data/${cik}/${accession}/`;
    const indexJson = await fetchURL(baseUrl + 'index.json');
    const index     = JSON.parse(indexJson);
    const files     = index?.directory?.item || [];
    const xmlFile   = files.find(f => typeof f.name === 'string' && f.name.endsWith('.xml') && !f.name.includes('xsl'));

    if (!xmlFile) return res.json({ error: 'No XML file found', files });

    const xml    = await fetchURL(baseUrl + xmlFile.name);
    const parsed = parseForm4(xml, symbol);

    res.json({
      symbol, cik, accession,
      xmlFile: xmlFile.name,
      parsedTrades: parsed,
      rawXmlSnippet: xml.slice(0, 3000),
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  HEALTH CHECK
// ─────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// ─────────────────────────────────────────────────────────────
//  START
// ─────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`InsiderTape server running on port ${PORT}`);
});
