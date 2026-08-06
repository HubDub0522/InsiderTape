'use strict';

// Trial lifecycle emails: a short 2-touch sequence during the 7-day free trial
// to lift trial -> paid conversion (and cut early churn).
//
//   1. WELCOME  - sent ~day 1, right after a trial starts. Orients them so they
//                 actually use the product (the #1 driver of converting).
//   2. ENDING   - sent ~2 days before the trial ends. Reinforces the value they
//                 keep, honest "cancel anytime" link, no dark patterns.
//
// Runs in GitHub Actions (see .github/workflows/trial-emails.yml), NOT on Vercel,
// so it adds zero Fast Origin Transfer. STRIPE is the source of truth for who is
// trialing and when the trial ends (the DB `status` is written 'active' on the
// success redirect even during the trial, so it can't be trusted for this).
// De-dup is stored in each subscription's Stripe metadata (it_welcome / it_ending)
// so an email is never sent twice, with no extra DB schema.
//
// Env: STRIPE_SECRET, RESEND_KEY (both required to send).
//      FROM_EMAIL, REPLY_TO, SITE_URL (optional; sensible defaults below).
//      TEST_EMAIL -> send one sample of EACH email to that address, touch nothing.
//      DRY_RUN=1  -> list who WOULD get what, but don't send or write metadata.

const STRIPE_SECRET = process.env.STRIPE_SECRET || '';
const RESEND_KEY    = process.env.RESEND_KEY || '';
const SITE_URL      = (process.env.SITE_URL || 'https://www.insidertape.com').replace(/\/$/, '');
const FROM_EMAIL    = (() => {
  let v = (process.env.FROM_EMAIL || '').trim().replace(/^["']|["']$/g, '').trim();
  const valid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) || /^.+<[^\s@]+@[^\s@]+\.[^\s@]+>$/.test(v);
  return valid ? v : 'InsiderTape <noreply@insidertape.com>';
})();
const REPLY_TO   = (process.env.REPLY_TO || '').trim();
const TEST_EMAIL = (process.env.TEST_EMAIL || '').trim().toLowerCase();
const DRY_RUN    = process.env.DRY_RUN === '1' || process.argv.includes('--dry');

const HOUR = 3600 * 1000;
const sleep = ms => new Promise(r => setTimeout(r, ms));
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

// ─── Email bodies ─────────────────────────────────────────────────────────────
function shell(inner) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;background:#f0f2f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#1a2030;line-height:1.6">
  <div style="max-width:560px;margin:0 auto;padding:28px 20px">
    <div style="font-size:18px;font-weight:800;letter-spacing:2px;color:#1a2030;margin-bottom:18px">INSIDER<span style="color:#0a6f88">TAPE</span></div>
    <div style="background:#fff;border:1px solid #d0d4db;border-radius:12px;padding:26px 24px">
      ${inner}
    </div>
    <div style="font-size:11px;color:#6e7a8a;text-align:center;margin-top:18px;line-height:1.6">
      InsiderTape &middot; Insider data from SEC EDGAR (Form 4) &middot; Not financial advice<br>
      Manage your subscription any time from your <a href="${SITE_URL}/account" style="color:#0a6f88">account</a>.
    </div>
  </div>
</body></html>`;
}

function buildWelcome() {
  return shell(`
    <h1 style="font-size:21px;font-weight:800;margin:0 0 12px">You're in. Your trial is live.</h1>
    <p style="margin:0 0 16px;font-size:15px;color:#3a4555">Everything's unlocked for the next 7 days. Here's how to get value in the first five minutes:</p>
    <ol style="margin:0 0 20px;padding-left:20px;font-size:15px;color:#3a4555">
      <li style="margin-bottom:10px"><strong>Pull up any ticker.</strong> Every insider buy and sell is plotted right on the price chart, green where they buy, red where they sell, so the CFO buying the dip and the exec selling the top jump right out.</li>
      <li style="margin-bottom:10px"><strong>See where the money's going.</strong> The <a href="${SITE_URL}/biggest-insider-buys" style="color:#0a6f88">biggest insider buys this week</a> and the <a href="${SITE_URL}/biggest-insider-buyers" style="color:#0a6f88">biggest buyers of the year</a>, ranked.</li>
      <li><strong>Watch for the real signals.</strong> Cluster buys and a CFO stepping in are the setups that backtested best over five years. We flag them the moment a Form 4 files.</li>
    </ol>
    <div style="text-align:center;margin:22px 0 6px">
      <a href="${SITE_URL}/" style="display:inline-block;background:#0a6f88;color:#fff;padding:12px 30px;border-radius:8px;font-size:14px;font-weight:700;text-decoration:none">Open InsiderTape &rarr;</a>
    </div>
    <p style="margin:18px 0 0;font-size:13px;color:#6e7a8a">Cancel any time before day 7 and you won't be charged. Questions? Just hit reply, a real person reads it.</p>
  `);
}

function buildEnding() {
  return shell(`
    <h1 style="font-size:21px;font-weight:800;margin:0 0 12px">Your trial ends in 2 days</h1>
    <p style="margin:0 0 16px;font-size:15px;color:#3a4555">In two days your free trial rolls into full access. Here's what you keep tracking:</p>
    <ul style="margin:0 0 18px;padding-left:20px;font-size:15px;color:#3a4555">
      <li style="margin-bottom:8px">Every SEC Form 4, plotted on the chart the moment it files</li>
      <li style="margin-bottom:8px">Cluster-buy and CFO-conviction alerts, the signals that backtested best</li>
      <li style="margin-bottom:8px">First-buy-in-years and buying-at-the-lows flags</li>
      <li>The full studies behind all of it</li>
    </ul>
    <p style="margin:0 0 18px;font-size:14px;color:#3a4555;background:#f0f7f4;border-left:3px solid #12905f;padding:12px 14px;border-radius:6px">The edge is early: insider buy signals are strongest in the first 30&ndash;90 days after a purchase. Staying on means you see them as they happen, not weeks later.</p>
    <div style="text-align:center;margin:20px 0 6px">
      <a href="${SITE_URL}/" style="display:inline-block;background:#0a6f88;color:#fff;padding:12px 30px;border-radius:8px;font-size:14px;font-weight:700;text-decoration:none">Keep my access &rarr;</a>
    </div>
    <p style="margin:18px 0 0;font-size:13px;color:#6e7a8a">Not for you? No hard feelings, <a href="${SITE_URL}/account" style="color:#0a6f88">cancel in one click</a> before the trial ends and you won't be charged a cent.</p>
  `);
}

// ─── Send ─────────────────────────────────────────────────────────────────────
async function sendEmail(resend, to, subject, html) {
  const opts = { from: FROM_EMAIL, to, subject, html };
  if (REPLY_TO) opts.reply_to = REPLY_TO;
  await resend.emails.send(opts);
}

async function main() {
  if (!STRIPE_SECRET) { console.error('STRIPE_SECRET not set; aborting.'); process.exit(1); }
  if (!RESEND_KEY && !DRY_RUN && !TEST_EMAIL) { console.error('RESEND_KEY not set; aborting.'); process.exit(1); }

  const stripe = require('stripe')(STRIPE_SECRET);
  const { Resend } = require('resend');
  const resend = (RESEND_KEY) ? new Resend(RESEND_KEY) : null;

  // TEST_EMAIL: send one of each so you can eyeball them, and stop.
  if (TEST_EMAIL) {
    if (!resend) { console.error('RESEND_KEY needed for TEST_EMAIL send.'); process.exit(1); }
    console.log('TEST_EMAIL mode -> sending welcome + ending to', TEST_EMAIL);
    await sendEmail(resend, TEST_EMAIL, 'You’re in. Your InsiderTape trial is live.', buildWelcome());
    await sleep(600);
    await sendEmail(resend, TEST_EMAIL, 'Your InsiderTape trial ends in 2 days', buildEnding());
    console.log('Sent 2 test emails.');
    return;
  }

  const now = Date.now();
  let welcomeSent = 0, endingSent = 0, scanned = 0;
  let startingAfter = undefined;

  for (let page = 0; page < 20; page++) {
    const params = { status: 'trialing', limit: 100, expand: ['data.customer'] };
    if (startingAfter) params.starting_after = startingAfter;
    const res = await stripe.subscriptions.list(params);

    for (const sub of res.data) {
      scanned++;
      const cust = sub.customer && typeof sub.customer === 'object' ? sub.customer : null;
      const email = cust && !cust.deleted ? cust.email : null;
      if (!email) continue;

      const createdMs = (sub.created || 0) * 1000;
      const trialEndMs = (sub.trial_end || 0) * 1000;
      const meta = sub.metadata || {};

      // WELCOME: fresh trial (started in the last ~3 days), not yet welcomed.
      if (!meta.it_welcome && createdMs && createdMs >= now - 3 * 24 * HOUR) {
        if (DRY_RUN) { console.log('[dry] welcome ->', email); welcomeSent++; }
        else {
          try {
            await sendEmail(resend, email, 'You’re in. Your InsiderTape trial is live.', buildWelcome());
            await stripe.subscriptions.update(sub.id, { metadata: Object.assign({}, meta, { it_welcome: new Date().toISOString() }) });
            welcomeSent++; console.log('welcome ->', email); await sleep(500);
          } catch (e) { console.error('welcome failed for', email, e.message); }
        }
        continue; // don't double-hit a subscription in one run
      }

      // ENDING: trial ends within ~0.5-3 days, not yet reminded.
      if (!meta.it_ending && trialEndMs && trialEndMs <= now + 72 * HOUR && trialEndMs >= now + 12 * HOUR) {
        if (DRY_RUN) { console.log('[dry] ending ->', email, 'ends', new Date(trialEndMs).toISOString()); endingSent++; }
        else {
          try {
            await sendEmail(resend, email, 'Your InsiderTape trial ends in 2 days', buildEnding());
            await stripe.subscriptions.update(sub.id, { metadata: Object.assign({}, meta, { it_ending: new Date().toISOString() }) });
            endingSent++; console.log('ending ->', email); await sleep(500);
          } catch (e) { console.error('ending failed for', email, e.message); }
        }
      }
    }

    if (!res.has_more) break;
    startingAfter = res.data[res.data.length - 1].id;
  }

  console.log(`Done. Scanned ${scanned} trialing subs. Welcome: ${welcomeSent}, Ending: ${endingSent}.${DRY_RUN ? ' (DRY RUN)' : ''}`);
}

main().catch(e => { console.error(e); process.exit(1); });
