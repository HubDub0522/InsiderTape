// ─────────────────────────────────────────────────────────────
//  INSIDERTAPE — Backend Server
//  Built with Node.js + Express
//  Data source: SEC EDGAR (official US government API, free, no key)
// ─────────────────────────────────────────────────────────────

const express = require('express');
const cors    = require('cors');
const https   = require('https');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Serve the frontend HTML file from the same folder
app.use(express.static(path.join(__dirname, 'public')));

// ─────────────────────────────────────────────────────────────
//  HELPER: fetch a URL server-side (no CORS issues)
// ─────────────────────────────────────────────────────────────
function fetchURL(url) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'InsiderTape/1.0 contact@insidertape.com',
        'Accept': 'application/json, text/html, text/plain',
      },
      timeout: 10000,
    };
    https.get(url, options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    }).on('error', reject).on('timeout', () => reject(new Error('Timeout')));
  });
}

// ─────────────────────────────────────────────────────────────
//  SIMPLE IN-MEMORY CACHE (avoids hammering SEC)
// ─────────────────────────────────────────────────────────────
const cache = {};
function getCache(key)         { const c = cache[key]; return c && Date.now() < c.exp ? c.val : null; }
function setCache(key, val, ms) { cache[key] = { val, exp: Date.now() + ms }; }

// ─────────────────────────────────────────────────────────────
//  PARSE SEC EDGAR FORM 4 — converts raw XML/JSON to trade objects
// ─────────────────────────────────────────────────────────────
function parseForm4(xml, ticker, companyName) {
  const trades = [];
  try {
    // Extract non-derivative transactions (actual stock buys/sells)
    const nonDerivRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
    let match;
    while ((match = nonDerivRe.exec(xml)) !== null) {
      const block = match[1];
      const get = (tag) => {
        const m = block.match(new RegExp(`<${tag}[^>]*>\\s*<value>([^<]*)<\\/value>`, 'i'))
                 || block.match(new RegExp(`<${tag}[^>]*>([^<]*)<\\/`, 'i'));
        return m ? m[1].trim() : '';
      };

      const transCode = get('transactionCode');
      const date      = get('transactionDate');
      const shares    = parseFloat(get('transactionShares')) || 0;
      const price     = parseFloat(get('transactionPricePerShare')) || 0;
      const sharesAfter = parseFloat(get('sharesOwnedFollowingTransaction')) || 0;

      if (!date || !shares) continue;

      trades.push({
        ticker,
        company:  companyName,
        trade:    date,
        filing:   date,
        type:     transCode,  // P=buy, S=sell, etc.
        qty:      Math.abs(shares),
        price,
        value:    Math.abs(shares * price),
        owned:    sharesAfter,
        insider:  '',   // filled in by caller
        title:    '',
      });
    }

    // Also grab derivative transactions (options)
    const derivRe = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/g;
    while ((match = derivRe.exec(xml)) !== null) {
      const block = match[1];
      const get = (tag) => {
        const m = block.match(new RegExp(`<${tag}[^>]*>\\s*<value>([^<]*)<\\/value>`, 'i'))
                 || block.match(new RegExp(`<${tag}[^>]*>([^<]*)<\\/`, 'i'));
        return m ? m[1].trim() : '';
      };
      const transCode = get('transactionCode');
      const date      = get('transactionDate');
      const shares    = parseFloat(get('transactionShares')) || 0;
      const price     = parseFloat(get('exercisePrice') || get('transactionPricePerShare')) || 0;
      if (!date || !shares) continue;

      trades.push({
        ticker, company: companyName,
        trade: date, filing: date,
        type: transCode || 'A',
        qty: Math.abs(shares), price,
        value: Math.abs(shares * price),
        owned: 0, insider: '', title: '',
      });
    }
  } catch(e) {
    console.error('Form4 parse error:', e.message);
  }
  return trades;
}

// ─────────────────────────────────────────────────────────────
//  ROUTE 1: GET /api/screener
//  Latest insider transactions across all companies
//  Source: SEC EDGAR recent filings RSS feed
// ─────────────────────────────────────────────────────────────
app.get('/api/screener', async (req, res) => {
  try {
    const cacheKey = 'screener';
    const cached = getCache(cacheKey);
    if (cached) return res.json(cached);

    // SEC EDGAR full-text search for recent Form 4 filings
    const url = 'https://efts.sec.gov/LATEST/search-index?q=%22form+4%22&dateRange=custom&startdt=' +
      new Date(Date.now() - 7*86400000).toISOString().split('T')[0] +
      '&enddt=' + new Date().toISOString().split('T')[0] +
      '&forms=4&hits.hits.total.value=true&hits.hits._source.period_of_report=true';

    // Use the EDGAR company search to get recent Form 4s
    const feedUrl = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&search_text=&output=atom';
    const feedXml = await fetchURL(feedUrl);

    const trades = [];
    const entryRe = /<entry>([\s\S]*?)<\/entry>/g;
    let m;
    const filingUrls = [];

    while ((m = entryRe.exec(feedXml)) !== null && filingUrls.length < 40) {
      const entry  = m[1];
      const link   = (entry.match(/<link[^>]*href="([^"]*)"/) || [])[1] || '';
      const title  = (entry.match(/<title>([^<]*)<\/title>/) || [])[1] || '';
      if (link && link.includes('Archives')) filingUrls.push({ link, title });
    }

    // Fetch a sample of filings in parallel (limit to 20 for speed)
    const sample = filingUrls.slice(0, 20);
    await Promise.allSettled(sample.map(async ({ link, title }) => {
      try {
        // Convert index URL to the actual .xml filing
        const indexUrl = link.endsWith('/') ? link + 'index.json' : link + '/index.json';
        const indexData = JSON.parse(await fetchURL(indexUrl));
        const files = indexData?.filings?.files || indexData?.directory?.item || [];

        // Find the Form 4 XML file
        const xmlFile = files.find(f => (f.name||f).match(/\.xml$/i) && !(f.name||f).match(/xsl/i));
        if (!xmlFile) return;

        const baseUrl = 'https://www.sec.gov' + (indexData.filings ? 
          link.replace('https://www.sec.gov','').replace(/\/?$/, '/') :
          link.replace('https://www.sec.gov','').replace(/index\.json.*/, ''));
        
        const xmlUrl  = baseUrl + (xmlFile.name || xmlFile);
        const xml     = await fetchURL(xmlUrl);

        // Extract insider name and title from XML
        const insiderName  = (xml.match(/<rptOwnerName>([^<]*)<\/rptOwnerName>/) || [])[1]?.trim() || 'Unknown';
        const insiderTitle = (xml.match(/<officerTitle>([^<]*)<\/officerTitle>/) || [])[1]?.trim() || '';
        const ticker       = (xml.match(/<issuerTradingSymbol>([^<]*)<\/issuerTradingSymbol>/) || [])[1]?.trim() || '';
        const company      = (xml.match(/<issuerName>([^<]*)<\/issuerName>/) || [])[1]?.trim() || '';
        const filingDate   = (xml.match(/<periodOfReport>([^<]*)<\/periodOfReport>/) || [])[1]?.trim() || '';

        if (!ticker) return;

        const parsed = parseForm4(xml, ticker, company);
        parsed.forEach(t => {
          t.insider = insiderName;
          t.title   = insiderTitle;
          t.filing  = filingDate || t.filing;
          trades.push(t);
        });
      } catch(e) { /* skip bad filings */ }
    }));

    const result = trades.filter(t => t.ticker && t.trade);
    setCache(cacheKey, result, 15 * 60 * 1000); // cache 15 min
    res.json(result);

  } catch(e) {
    console.error('/api/screener error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 2: GET /api/ticker?symbol=AAPL
//  All insider trades for a specific stock
//  Source: SEC EDGAR company search → Form 4 filings
// ─────────────────────────────────────────────────────────────
app.get('/api/ticker', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  try {
    const cacheKey = 'ticker_' + symbol;
    const cached = getCache(cacheKey);
    if (cached) return res.json(cached);

    // Step 1: Look up CIK for this ticker
    const searchUrl = `https://efts.sec.gov/LATEST/search-index?q=%22${symbol}%22&forms=4&dateRange=custom&startdt=${
      new Date(Date.now() - 365*86400000).toISOString().split('T')[0]}&enddt=${
      new Date().toISOString().split('T')[0]}`;

    // Use the company search to get CIK
    const companyUrl = `https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=${encodeURIComponent(symbol)}&type=4&dateb=&owner=include&count=40&search_text=&action=getcompany&output=atom`;
    const companyXml = await fetchURL(companyUrl);

    const trades = [];
    const entryRe = /<entry>([\s\S]*?)<\/entry>/g;
    const filingUrls = [];
    let m;

    while ((m = entryRe.exec(companyXml)) !== null && filingUrls.length < 30) {
      const entry = m[1];
      const link  = (entry.match(/<link[^>]*href="([^"]*)"/) || [])[1] || '';
      if (link && link.includes('Archives')) filingUrls.push(link);
    }

    await Promise.allSettled(filingUrls.map(async (link) => {
      try {
        const indexUrl = (link.endsWith('/') ? link : link + '/') + 'index.json';
        const indexData = JSON.parse(await fetchURL(indexUrl));
        const files = indexData?.directory?.item || [];

        const xmlFile = files.find(f => (f.name||'').match(/\.xml$/i) && !(f.name||'').match(/xsl/i));
        if (!xmlFile) return;

        const base   = link.replace('https://www.sec.gov', '').replace(/\/?$/, '/');
        const xmlUrl = 'https://www.sec.gov' + base + xmlFile.name;
        const xml    = await fetchURL(xmlUrl);

        const insiderName  = (xml.match(/<rptOwnerName>([^<]*)<\/rptOwnerName>/) || [])[1]?.trim() || 'Unknown';
        const insiderTitle = (xml.match(/<officerTitle>([^<]*)<\/officerTitle>/) || [])[1]?.trim() || '';
        const ticker       = (xml.match(/<issuerTradingSymbol>([^<]*)<\/issuerTradingSymbol>/) || [])[1]?.trim() || symbol;
        const company      = (xml.match(/<issuerName>([^<]*)<\/issuerName>/) || [])[1]?.trim() || '';
        const filingDate   = (xml.match(/<periodOfReport>([^<]*)<\/periodOfReport>/) || [])[1]?.trim() || '';

        const parsed = parseForm4(xml, ticker, company);
        parsed.forEach(t => {
          t.insider = insiderName;
          t.title   = insiderTitle;
          t.filing  = filingDate || t.filing;
          trades.push(t);
        });
      } catch(e) { /* skip */ }
    }));

    const result = trades.filter(t => t.trade).sort((a,b) => new Date(b.trade) - new Date(a.trade));
    setCache(cacheKey, result, 30 * 60 * 1000); // cache 30 min
    res.json(result);

  } catch(e) {
    console.error('/api/ticker error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
//  ROUTE 3: GET /api/insider?name=Tim+Cook
//  All trades filed by a specific person
// ─────────────────────────────────────────────────────────────
app.get('/api/insider', async (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });

  try {
    const cacheKey = 'insider_' + name.toLowerCase();
    const cached = getCache(cacheKey);
    if (cached) return res.json(cached);

    const searchUrl = `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(name)}%22&forms=4&dateRange=custom&startdt=${
      new Date(Date.now() - 2*365*86400000).toISOString().split('T')[0]}&enddt=${
      new Date().toISOString().split('T')[0]}&hits.hits._source=period_of_report,display_names,file_date`;

    const searchData = JSON.parse(await fetchURL(searchUrl));
    const hits = searchData?.hits?.hits || [];

    const trades = [];
    await Promise.allSettled(hits.slice(0, 25).map(async (hit) => {
      try {
        const accNo   = hit._id.replace(/:/g, '-');
        const baseUrl = `https://www.sec.gov/Archives/edgar/data/${hit._source?.entity_id || ''}/${accNo.replace(/-/g,'')}/`;
        const indexUrl = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum=&State=0&SIC=&dateb=&owner=include&count=1&search_text=&action=getcompany`;

        // Fetch filing index directly
        const xmlSearchUrl = `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(name)}%22&forms=4&dateRange=custom&startdt=2020-01-01&enddt=${new Date().toISOString().split('T')[0]}`;
        // Use EDGAR full text search API instead
        const ftUrl = `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(name)}%22&forms=4`;
        const ftData = JSON.parse(await fetchURL(ftUrl));
        const ftHits = ftData?.hits?.hits || [];

        for (const fh of ftHits.slice(0, 20)) {
          const src = fh._source || {};
          trades.push({
            ticker:  (src.period_of_report || '').substring(0,4),
            company: (src.display_names || [''])[0] || '',
            trade:   src.period_of_report || src.file_date || '',
            filing:  src.file_date || '',
            insider: name,
            title:   '',
            type:    'P',
            qty:     0, price: 0, value: 0, owned: 0,
          });
        }
      } catch(e) {}
    }));

    // De-duplicate and filter
    const seen = new Set();
    const result = trades.filter(t => {
      const k = t.ticker + t.trade;
      if (seen.has(k)) return false;
      seen.add(k);
      return t.trade;
    });

    setCache(cacheKey, result, 60 * 60 * 1000); // cache 1 hr
    res.json(result);

  } catch(e) {
    console.error('/api/insider error:', e.message);
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
