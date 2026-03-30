#!/usr/bin/env node
'use strict';
// congress-action.js
// Runs in GitHub Actions (non-datacenter IPs get HTML from house.gov)
// Fetches PTR filings, parses them, POSTs results to Render via /api/gov-import

const https = require('https');
const http  = require('http');
const zlib  = require('zlib');

const RENDER_URL   = process.env.RENDER_URL;   // e.g. https://insidertape.com
const IMPORT_SECRET = process.env.IMPORT_SECRET;
const START_YEAR   = parseInt(process.env.CONGRESS_START_YEAR || String(new Date().getFullYear()));

if (!RENDER_URL || !IMPORT_SECRET) {
  console.error('Missing RENDER_URL or IMPORT_SECRET env vars');
  process.exit(1);
}

// ── HTTP ──────────────────────────────────────────────────────────────────────
function get(url, ms = 30000, hops = 0) {
  if (hops > 5) return Promise.reject(new Error('Too many redirects'));
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Upgrade-Insecure-Requests': '1',
      },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        return get(loc.startsWith('http') ? loc : new URL(loc, url).href, ms, hops+1).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function post(url, data, token) {
  const body = JSON.stringify(data);
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname, path: u.pathname, method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'X-Import-Token': token,
      },
      timeout: 30000,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('POST timeout')); });
    req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Helpers ───────────────────────────────────────────────────────────────────
function normDate(raw = '') {
  if (!raw) return null;
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0,10);
  return null;
}
function normType(raw = '') {
  const t = raw.trim().toLowerCase();
  if (t.includes('purchase') || t === 'p') return 'P';
  if (t.includes('sale') || t.includes('sell') || t === 's') return 'S';
  return null;
}

// ── Parse PTR HTML ────────────────────────────────────────────────────────────
function parseHousePTR(rawText, member, docId, filingUrl) {
  const rows = [];
  const text = rawText.replace(/<[^>]+>/g,' ').replace(/&amp;/g,'&').replace(/\s+/g,' ');

  const signedMatch = text.match(/Digitally Signed[^,]*,\s*(\d{1,2}\/\d{1,2}\/\d{4})/);
  const disclosureDate = signedMatch ? normDate(signedMatch[1]) : null;

  const tickerRe = /([^(]{3,80})\(([A-Z]{1,5})\)(?:\s*\[(?:ST|OP|DC|CS|MF|ET|PS|AS)\])?/g;
  const SKIP = new Set(['EST','LLC','INC','ETF','THE','FOR','AND','ARE','NOT','USA','SEC','ACT','GPO','PDF']);
  let m;
  while ((m = tickerRe.exec(text)) !== null) {
    const ticker = m[2];
    if (SKIP.has(ticker)) continue;
    const after = text.slice(m.index + m[0].length, m.index + m[0].length + 400);
    const typeM  = after.match(/\b(Purchase|Sale|Exchange|P|S)\b/i);
    const txType = typeM ? normType(typeM[1]) : null;
    if (!txType) continue;
    const afterType = after.slice(after.indexOf(typeM[0]) + typeM[0].length);
    const dateM    = afterType.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
    const amtM     = after.match(/(\$[\d,]+\s*[-–]\s*\$[\d,]+|\$[\d,]+\+?)/);
    const ownerM   = after.match(/\b(Self|Spouse|Joint|Dependent)\b/i);
    rows.push({
      chamber: 'H', member, ticker,
      asset_description: m[1].trim().slice(0,200),
      transaction_type: txType,
      transaction_date: dateM ? normDate(dateM[1]) : null,
      disclosure_date: disclosureDate,
      amount_range: amtM ? amtM[1].replace(/\s+/g,' ').slice(0,50) : null,
      owner: ownerM ? ownerM[1].slice(0,20) : 'Self',
      filing_url: filingUrl,
      doc_id: docId,
    });
  }
  return rows;
}

// ── Fetch year ────────────────────────────────────────────────────────────────
async function fetchYear(year) {
  const zipUrl = `https://disclosures-clerk.house.gov/public_disc/financial-pdfs/${year}FD.ZIP`;
  console.log(`Fetching ${year} ZIP...`);

  let xmlBody;
  try {
    const { status, body } = await get(zipUrl, 90000);
    if (status !== 200) throw new Error(`ZIP HTTP ${status}`);
    let offset = 0, found = false;
    while (offset < body.length - 4) {
      if (body.readUInt32LE(offset) !== 0x04034b50) { offset++; continue; }
      const compression = body.readUInt16LE(offset + 8);
      const compSize    = body.readUInt32LE(offset + 18);
      const fnameLen    = body.readUInt16LE(offset + 26);
      const extraLen    = body.readUInt16LE(offset + 28);
      const fname       = body.slice(offset + 30, offset + 30 + fnameLen).toString();
      const dataStart   = offset + 30 + fnameLen + extraLen;
      if (fname.toLowerCase().endsWith('.xml')) {
        const comp = body.slice(dataStart, dataStart + compSize);
        xmlBody = compression === 8 ? zlib.inflateRawSync(comp).toString('utf8') : comp.toString('utf8');
        found = true;
        break;
      }
      offset = dataStart + compSize;
    }
    if (!found) throw new Error('No XML in ZIP');
  } catch(e) {
    console.error(`Year ${year} ZIP failed: ${e.message}`);
    return [];
  }

  // Parse XML for PTR filings
  const getTag = (xml, tag) => { const m = xml.match(new RegExp(`<${tag}>([^<]*)</${tag}>`,'i')); return m ? m[1].trim() : ''; };
  const rowRe  = /<(?:Row|Member)>([\s\S]*?)<\/(?:Row|Member)>/gi;
  const PTR_TYPES = new Set(['ptr','p','periodic']);
  const filings = [];
  let rm;
  while ((rm = rowRe.exec(xmlBody)) !== null) {
    const row = rm[1];
    const ft  = getTag(row, 'FilingType').toLowerCase();
    if (ft && !PTR_TYPES.has(ft)) continue;
    const docId = getTag(row, 'DocID');
    if (!docId) continue;
    filings.push({
      docId,
      member: `${getTag(row,'First')} ${getTag(row,'Last')}`.trim() || 'Unknown',
      year,
    });
  }
  console.log(`Year ${year}: ${filings.length} PTR filings`);
  return filings;
}

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  const currentYear = new Date().getFullYear();
  let allRows = [];
  let totalPosted = 0;

  for (let y = START_YEAR; y <= currentYear; y++) {
    const filings = await fetchYear(y);

    for (const f of filings) {
      const pdfUrl = `https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/${f.year}/${f.docId}.pdf`;
      try {
        await sleep(300);
        const { status, body } = await get(pdfUrl, 20000);
        if (status !== 200) continue;

        const bodyStr = body.toString('utf8');
        // If we got binary PDF, skip (shouldn't happen on GitHub Actions IPs)
        if (bodyStr.startsWith('%PDF')) {
          console.warn(`${f.docId}: got binary PDF — GitHub Actions IP may be blocked`);
          continue;
        }

        const rows = parseHousePTR(bodyStr, f.member, f.docId, pdfUrl);
        if (rows.length) {
          allRows.push(...rows);
          console.log(`${f.member} (${f.docId}): ${rows.length} trades`);
        }

        // Batch POST every 500 rows to avoid memory/timeout issues
        if (allRows.length >= 500) {
          const result = await post(`${RENDER_URL}/api/gov-import`, allRows, IMPORT_SECRET);
          console.log(`Posted ${allRows.length} rows → ${result.body}`);
          totalPosted += allRows.length;
          allRows = [];
        }
      } catch(e) {
        console.warn(`${f.docId} error: ${e.message}`);
      }
    }
  }

  // Post remaining rows
  if (allRows.length) {
    const result = await post(`${RENDER_URL}/api/gov-import`, allRows, IMPORT_SECRET);
    console.log(`Posted final ${allRows.length} rows → ${result.body}`);
    totalPosted += allRows.length;
  }

  console.log(`Done. Total rows posted: ${totalPosted}`);
})().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
