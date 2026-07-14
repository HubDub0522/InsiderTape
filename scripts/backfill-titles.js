// One-time backfill: populate the empty `title` column on existing trades from the
// SEC quarterly REPORTINGOWNER files (RPTOWNER_TITLE, else RPTOWNER_RELATIONSHIP).
// Only UPDATEs existing rows keyed by accession - never inserts, so it will not
// re-add trades that cleanup steps removed. Run via workflow_dispatch.
const https = require('https');
const zlib  = require('zlib');
const { createClient } = require('@libsql/client');

const client = createClient({ url: process.env.TURSO_DATABASE_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const log = m => process.stdout.write(`[${new Date().toISOString().slice(11, 19)}] ${m}\n`);

function get(url, redirects = 0) {
  return new Promise((res, rej) => {
    https.get(url, { headers: { 'User-Agent': 'InsiderTape/2.0 admin@insidertape.com' }, timeout: 120000 }, r => {
      if (r.statusCode >= 300 && r.statusCode < 400 && r.headers.location && redirects < 5) { r.resume(); return get(r.headers.location, redirects + 1).then(res, rej); }
      const c = []; r.on('data', d => c.push(d)); r.on('end', () => res({ status: r.statusCode, buf: Buffer.concat(c) }));
    }).on('error', rej).on('timeout', function () { this.destroy(new Error('timeout')); });
  });
}

function extractOne(zipBuf, targetPrefix) {
  let pos = 0;
  while (pos < zipBuf.length - 4) {
    if (zipBuf[pos] !== 0x50 || zipBuf[pos + 1] !== 0x4B || zipBuf[pos + 2] !== 0x03 || zipBuf[pos + 3] !== 0x04) { pos++; continue; }
    const compression = zipBuf.readUInt16LE(pos + 8);
    const compSize    = zipBuf.readUInt32LE(pos + 18);
    const fnLen       = zipBuf.readUInt16LE(pos + 26);
    const exLen       = zipBuf.readUInt16LE(pos + 28);
    const fname       = zipBuf.slice(pos + 30, pos + 30 + fnLen).toString();
    const dataStart   = pos + 30 + fnLen + exLen;
    const base        = fname.split('/').pop().toUpperCase();
    if (base.startsWith(targetPrefix.toUpperCase())) {
      const slice = zipBuf.slice(dataStart, dataStart + compSize);
      const raw   = compression === 8 ? zlib.inflateRawSync(slice) : slice;
      return raw.toString('utf8').split('\n');
    }
    pos = dataStart + compSize;
  }
  return null;
}

function* tsvRows(lines) {
  if (!lines?.length) return;
  const hdrs = lines[0].split('\t').map(h => h.trim().toUpperCase());
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split('\t'), row = {};
    hdrs.forEach((h, j) => { row[h] = (cols[j] || '').trim(); });
    yield row;
  }
}

async function main() {
  log('=== backfill-titles start ===');
  // Accession index makes the per-accession UPDATE a seek instead of a full scan.
  try { await client.execute('CREATE INDEX IF NOT EXISTS idx_accession ON trades(accession)'); log('idx_accession ensured'); } catch (e) { log('idx_accession error: ' + e.message); }

  const now = new Date();
  let y = now.getUTCFullYear(), q = Math.floor(now.getUTCMonth() / 3) + 1;
  const quarters = [];
  for (let i = 0; i < 22; i++) { quarters.push(`${y}q${q}`); q--; if (q < 1) { q = 4; y--; } } // ~5.5 years

  let grandTotal = 0;
  for (const key of quarters) {
    const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${key}_form345.zip`;
    let buf;
    try { const r = await get(url); if (r.status !== 200) { log(`${key}: skip (HTTP ${r.status})`); continue; } buf = r.buf; }
    catch (e) { log(`${key}: fetch error ${e.message}`); continue; }
    const lines = extractOne(buf, 'REPORTINGOWNER');
    if (!lines) { log(`${key}: no REPORTINGOWNER`); continue; }
    const map = {};
    for (const o of tsvRows(lines)) {
      const acc = o.ACCESSION_NUMBER; if (!acc) continue;
      const t = (o.RPTOWNER_TITLE || o.RPTOWNER_RELATIONSHIP || '').trim();
      if (t && !map[acc]) map[acc] = t;
    }
    const entries = Object.entries(map);
    let updated = 0;
    for (let i = 0; i < entries.length; i += 200) {
      const chunk = entries.slice(i, i + 200);
      const stmts = chunk.map(([acc, t]) => ({ sql: "UPDATE trades SET title = ? WHERE accession = ? AND (title IS NULL OR title = '')", args: [t, acc] }));
      try { const res = await client.batch(stmts, 'write'); updated += res.reduce((s, r) => s + (r.rowsAffected || 0), 0); } catch (e) { log(`${key}: batch error ${e.message}`); }
    }
    grandTotal += updated;
    log(`${key}: ${entries.length} owners, updated ${updated} trades`);
  }

  // Drop the temporary index to keep storage down (accession lookups aren't needed elsewhere).
  try { await client.execute('DROP INDEX IF EXISTS idx_accession'); log('idx_accession dropped'); } catch (e) { log('drop idx_accession error: ' + e.message); }
  log(`=== backfill-titles done: ${grandTotal} rows updated ===`);
}

main().catch(e => { log('FATAL: ' + e.message); process.exit(1); });
