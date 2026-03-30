// Probe FMP congressional endpoints to find working parameters
'use strict';
const https = require('https');
const FMP_KEY = process.env.FMP_API_KEY;

function fmpGet(path) {
  return new Promise((resolve, reject) => {
    const url = 'https://financialmodelingprep.com/stable/' + path + (path.includes('?') ? '&' : '?') + 'apikey=' + FMP_KEY;
    console.log('GET', url.replace(FMP_KEY, 'KEY'));
    https.get(url, { headers: { 'User-Agent': 'InsiderTape/1.0' }, timeout: 20000 }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { 
          const parsed = JSON.parse(data);
          resolve(parsed);
        } catch(e) { 
          console.log('Raw response:', data.slice(0, 500));
          resolve(null); 
        }
      });
    }).on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

(async () => {
  // Try every plausible endpoint variant
  const tests = [
    'house-trades?symbol=AAPL',
    'house-trades?ticker=AAPL', 
    'house-trades?representative=Nancy+Pelosi',
    'house-trades?representative=Pelosi',
    'senate-trades?senator=John+Boozman',
    'senate-trades?name=John+Boozman',
    'house-disclosure',
    'senate-disclosure',
    'congressional-trading',
    'congressional-trading?symbol=AAPL',
    'house-trades-latest',
    'senate-trades-latest',
  ];
  
  for (const t of tests) {
    const r = await fmpGet(t);
    if (r !== null) {
      if (Array.isArray(r)) {
        console.log('  => array len=' + r.length + (r.length ? ' keys=' + Object.keys(r[0]).join(',') : ''));
      } else {
        console.log('  => object:', JSON.stringify(r).slice(0, 200));
      }
    }
    await sleep(300);
  }
})();
