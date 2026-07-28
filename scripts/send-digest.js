'use strict';

// Weekly "Biggest Insider Buys" email digest.
//
// Runs in GitHub Actions (see .github/workflows/weekly-digest.yml), NOT on
// Vercel, so it adds zero Fast Origin Transfer. Sends through Resend to every
// active newsletter subscriber, with a one-click unsubscribe per recipient.
//
// Env: TURSO_DATABASE_URL, TURSO_AUTH_TOKEN, RESEND_KEY (required to send).
//      FROM_EMAIL, SITE_URL (optional; sensible defaults below).
//      TEST_EMAIL  -> send ONLY to that address (verify before going live).
//      DRY_RUN=1   -> compute + log, but don't send or write last_sent_at.

const { query, run, exec } = require('../lib/db');

const RESEND_KEY = process.env.RESEND_KEY || '';
const SITE_URL   = (process.env.SITE_URL || 'https://www.insidertape.com').replace(/\/$/, '');
const FROM_EMAIL = (() => {
  let v = (process.env.FROM_EMAIL || '').trim().replace(/^["']|["']$/g, '').trim();
  const valid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) || /^.+<[^\s@]+@[^\s@]+\.[^\s@]+>$/.test(v);
  return valid ? v : 'InsiderTape <noreply@insidertape.com>';
})();
const REPLY_TO   = (process.env.REPLY_TO || '').trim();
const TEST_EMAIL = (process.env.TEST_EMAIL || '').trim().toLowerCase();
const DRY_RUN    = process.env.DRY_RUN === '1' || process.argv.includes('--dry');

const sleep = ms => new Promise(r => setTimeout(r, ms));
function fmtV(n) {
  n = Number(n) || 0;
  if (n >= 1e9) return '$' + (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return '$' + Math.round(n / 1e3) + 'K';
  return '$' + Math.round(n);
}
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

async function ensureNewsletter() {
  await exec(`CREATE TABLE IF NOT EXISTS newsletter_subscribers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active',
    unsub_token TEXT NOT NULL,
    source TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_sent_at TEXT
  )`);
}

// Same data as the /biggest-insider-buys page: open-market (code P) purchases
// filed in the last 7 days, grouped by ticker, ranked by dollar value.
async function getBiggestBuys() {
  return query(`
    SELECT ticker, MAX(company) AS company,
           COUNT(DISTINCT insider) AS insiders, COUNT(*) AS trades,
           SUM(COALESCE(value,0)) AS buy_val
    FROM trades
    WHERE TRIM(type)='P' AND trade_date >= date('now','-7 days')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      AND COALESCE(value,0) >= 10000
    GROUP BY ticker HAVING buy_val > 0
      AND NOT (COUNT(DISTINCT insider) >= 2
               AND COUNT(DISTINCT CASE WHEN price>0 THEN price END) <= 1
               AND COUNT(DISTINCT trade_date) <= 1)
    ORDER BY buy_val DESC LIMIT 12`);
}

function buildEmail(rows, unsubToken) {
  const unsubUrl = `${SITE_URL}/api/unsubscribe?token=${encodeURIComponent(unsubToken)}`;
  const totalVal = rows.reduce((s, r) => s + (Number(r.buy_val) || 0), 0);
  const bodyRows = rows.map((r, i) => {
    const co = esc((r.company || r.ticker).slice(0, 34));
    return `<tr>
      <td style="padding:11px 12px;border-bottom:1px solid #e3e7ec;color:#8a95a3;font-weight:700;font-size:13px;font-family:Arial,sans-serif">${i + 1}</td>
      <td style="padding:11px 12px;border-bottom:1px solid #e3e7ec;font-family:Arial,sans-serif">
        <a href="${SITE_URL}/insider-trading/${encodeURIComponent(r.ticker)}" style="color:#0a6f88;font-weight:700;font-size:15px;text-decoration:none">${esc(r.ticker)}</a>
        <div style="color:#6e7a8a;font-size:12px;margin-top:1px">${co}</div>
      </td>
      <td style="padding:11px 12px;border-bottom:1px solid #e3e7ec;text-align:right;color:#3a4555;font-size:13px;font-family:Arial,sans-serif">${r.insiders}</td>
      <td style="padding:11px 12px;border-bottom:1px solid #e3e7ec;text-align:right;color:#12905f;font-weight:700;font-size:14px;font-family:Arial,sans-serif">${fmtV(r.buy_val)}</td>
    </tr>`;
  }).join('');

  return `<!doctype html><html><body style="margin:0;padding:0;background:#f0f2f5">
  <div style="max-width:560px;margin:0 auto;padding:24px 16px">
    <div style="text-align:center;margin-bottom:6px">
      <span style="font-family:Arial,sans-serif;font-size:18px;font-weight:800;letter-spacing:3px;color:#1a2030">INSIDER<span style="color:#0a6f88">TAPE</span></span>
    </div>
    <div style="text-align:center;font-family:Arial,sans-serif;font-size:11px;letter-spacing:1px;color:#8a95a3;text-transform:uppercase;margin-bottom:22px">Weekly Insider Digest</div>

    <div style="background:#fff;border:1px solid #d0d4db;border-radius:12px;padding:22px 20px">
      <div style="font-family:Arial,sans-serif;font-size:19px;font-weight:800;color:#1a2030;margin-bottom:6px">The biggest insider buys this week</div>
      <div style="font-family:Arial,sans-serif;font-size:13px;color:#6e7a8a;line-height:1.6;margin-bottom:18px">
        The largest open-market purchases insiders filed with the SEC over the past 7 days, ranked by dollar value. Grants, option exercises, and plan sales stripped out - just shares insiders chose to buy with their own money. ${fmtV(totalVal)} across ${rows.length} companies.
      </div>
      <table style="width:100%;border-collapse:collapse;background:#fff">
        <thead><tr>
          <th style="padding:8px 12px;border-bottom:2px solid #d0d4db;text-align:left;font-family:Arial,sans-serif;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:#8a95a3">#</th>
          <th style="padding:8px 12px;border-bottom:2px solid #d0d4db;text-align:left;font-family:Arial,sans-serif;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:#8a95a3">Company</th>
          <th style="padding:8px 12px;border-bottom:2px solid #d0d4db;text-align:right;font-family:Arial,sans-serif;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:#8a95a3">Insiders</th>
          <th style="padding:8px 12px;border-bottom:2px solid #d0d4db;text-align:right;font-family:Arial,sans-serif;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:#8a95a3">Bought</th>
        </tr></thead>
        <tbody>${bodyRows}</tbody>
      </table>
      <div style="text-align:center;margin-top:22px">
        <a href="${SITE_URL}/biggest-insider-buys" style="display:inline-block;background:#0a6f88;color:#fff;font-family:Arial,sans-serif;font-size:13px;font-weight:700;text-decoration:none;padding:12px 26px;border-radius:8px">See the full ranking &rarr;</a>
      </div>
    </div>

    <div style="background:#fff;border:1px solid #d0d4db;border-radius:12px;padding:18px 20px;margin-top:14px;text-align:center">
      <div style="font-family:Arial,sans-serif;font-size:14px;font-weight:700;color:#1a2030;margin-bottom:4px">Want the moment they file, not a week later?</div>
      <div style="font-family:Arial,sans-serif;font-size:12px;color:#6e7a8a;line-height:1.6;margin-bottom:14px">InsiderTape flags cluster buys, CFO conviction, and first buys in years in real time. Start a free 7-day trial.</div>
      <a href="${SITE_URL}/premium" style="display:inline-block;background:#12905f;color:#fff;font-family:Arial,sans-serif;font-size:12px;font-weight:700;text-decoration:none;padding:10px 22px;border-radius:8px">Start free trial &rarr;</a>
    </div>

    <div style="font-family:Arial,sans-serif;font-size:12px;color:#6e7a8a;line-height:1.6;margin-top:16px;padding:0 4px">
      P.S. Just hit reply and tell me which names you want more of - I read every one. And if this landed in your Promotions tab, drag it to Primary so you don't miss next Monday.
    </div>
    <div style="text-align:center;font-family:Arial,sans-serif;font-size:11px;color:#8a95a3;line-height:1.7;margin-top:18px">
      Data from SEC Form 4 filings via <a href="${SITE_URL}" style="color:#0a6f88;text-decoration:none">insidertape.com</a>. Not financial advice.<br>
      <a href="${unsubUrl}" style="color:#8a95a3;text-decoration:underline">Unsubscribe</a>
    </div>
  </div>
</body></html>`;
}

// Plain-text alternative. A multipart (text + html) email looks less like bulk
// marketing to Gmail and lands in Primary far more often than html-only.
function buildText(rows, unsubToken) {
  const unsubUrl = `${SITE_URL}/api/unsubscribe?token=${encodeURIComponent(unsubToken)}`;
  const lines = rows.map((r, i) => `${i + 1}. $${r.ticker} - ${(r.company || '').slice(0, 34)} - ${r.insiders} insider${r.insiders == 1 ? '' : 's'} - ${fmtV(r.buy_val)} bought`);
  return [
    'INSIDERTAPE - Weekly Insider Digest',
    '',
    'The biggest open-market insider buys filed with the SEC this week, ranked by dollar value. Grants, option exercises, and coordinated plan buys stripped out:',
    '',
    ...lines,
    '',
    `Full ranking: ${SITE_URL}/biggest-insider-buys`,
    `Track any of these in real time (free 7-day trial): ${SITE_URL}/premium`,
    '',
    "P.S. Just hit reply and tell me which names you want more of - I read every one. And if this landed in your Promotions tab, drag it to Primary so you don't miss next Monday.",
    '',
    'Data from SEC Form 4 filings via insidertape.com. Not financial advice.',
    `Unsubscribe: ${unsubUrl}`,
  ].join('\n');
}

async function main() {
  if (!RESEND_KEY && !DRY_RUN) { console.error('RESEND_KEY not set; aborting.'); process.exit(1); }
  await ensureNewsletter();

  const rows = await getBiggestBuys();
  if (!rows.length) { console.log('No qualifying buys this week; skipping send.'); return; }

  let subs;
  if (TEST_EMAIL) {
    // Test mode: send one email to the given address using a throwaway unsub token.
    subs = [{ email: TEST_EMAIL, unsub_token: 'test-token', last_sent_at: null }];
  } else {
    subs = await query(`
      SELECT email, unsub_token, last_sent_at FROM newsletter_subscribers
      WHERE status='active'
        AND email NOT LIKE '%@example.com'
        AND email NOT LIKE 'claude-test-%'
        AND (last_sent_at IS NULL OR last_sent_at < datetime('now','-3 days'))`);
  }
  if (!subs.length) { console.log('No active subscribers to send to.'); return; }

  const subject = rows.length >= 2
    ? `This week's biggest insider buys: $${rows[0].ticker}, $${rows[1].ticker} + more`
    : `This week's biggest insider buys`;

  console.log(`${DRY_RUN ? '[DRY RUN] ' : ''}Recipients: ${subs.length} | Buys: ${rows.length} | Subject: ${subject}`);
  if (DRY_RUN) {
    console.log('Top rows:', rows.slice(0, 5).map(r => `${r.ticker} ${fmtV(r.buy_val)} (${r.insiders} ins)`).join(' | '));
    return;
  }

  const { Resend } = require('resend');
  const resend = new Resend(RESEND_KEY);
  let sent = 0, failed = 0;
  for (const s of subs) {
    try {
      const unsubUrl = `${SITE_URL}/api/unsubscribe?token=${encodeURIComponent(s.unsub_token)}`;
      const opts = {
        from: FROM_EMAIL, to: s.email, subject,
        html: buildEmail(rows, s.unsub_token),
        text: buildText(rows, s.unsub_token),
        headers: { 'List-Unsubscribe': `<${unsubUrl}>`, 'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click' },
      };
      if (REPLY_TO) opts.replyTo = REPLY_TO;
      await resend.emails.send(opts);
      if (!TEST_EMAIL) await run(`UPDATE newsletter_subscribers SET last_sent_at=datetime('now') WHERE email=?`, [s.email]);
      sent++;
      await sleep(180); // stay under Resend rate limits
    } catch (e) {
      failed++;
      console.error('send failed:', s.email, e && e.message);
    }
  }
  console.log(`Digest complete. Sent: ${sent}, failed: ${failed}.`);
}

main().catch(e => { console.error(e); process.exit(1); });
