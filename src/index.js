/**
 * ============================================================
 * TIKTOK_LITE_BOT – Webhook + Google Sheets (googleapis v4)
 * STYLE: giống BOT KIM (express + fetch + googleapis)
 *
 * ENV REQUIRED:
 * - BOT_TOKEN
 * - GOOGLE_SHEET_ID
 * - GOOGLE_APPLICATION_CREDENTIALS  (default: /etc/secrets/google-service-account.json)
 * - ADMIN_TELEGRAM_ID (để bot nhắc due_date + nhận trả lời checkin)
 *
 * SHEET TABS (đã tạo):
 * SETTINGS, WALLETS, WALLET_LOG, PHONES, LOTS, LOT_RESULT,
 * PHONE_PROFIT_LOG, INVITES, CHECKIN_REWARD, GAME_REVENUE, UNDO_LOG
 * ============================================================
 */

import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* ================== APP ================== */
const app = express();
app.use(express.json());

const VERSION = "TIKTOK_LITE_BOT-v1.0-WEBHOOK-GOOGLEAPIS";
console.log("🚀 RUNNING:", VERSION);

/* ================== ENV ================== */
const BOT_TOKEN = process.env.BOT_TOKEN;
const TELEGRAM_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS ||
  "/etc/secrets/google-service-account.json";

const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID
  ? String(process.env.ADMIN_TELEGRAM_ID)
  : "";

/* ================== BASIC ROUTES ================== */
app.get("/", (_, res) => res.send("TIKTOK_LITE_BOT OK"));
app.get("/ping", (_, res) => res.json({ ok: true, version: VERSION }));

/* ================== TELEGRAM HELPERS ================== */
async function tg(method, payload) {
  const resp = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return resp.json().catch(() => ({}));
}

async function send(chatId, text, extra = {}) {
  if (!chatId) return;
  await tg("sendMessage", { chat_id: chatId, text, ...extra });
}

function buildMainKeyboard() {
  return {
    keyboard: [
      [{ text: "💰 Game: dabong 100k" }, { text: "🎁 Game: hopqua 200k" }],
      [{ text: "🔳 Game: qr 57k" }, { text: "➕ them 0.5k" }],
      [{ text: "📊 Báo cáo tháng" }, { text: "📱 Thống kê máy" }],
      [{ text: "📌 Pending 14 ngày" }, { text: "🆘 Help" }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
    is_persistent: true,
  };
}

/* ================== GOOGLE SHEETS ================== */
const auth = new google.auth.GoogleAuth({
  keyFile: GOOGLE_APPLICATION_CREDENTIALS,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

async function getValues(rangeA1) {
  const r = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: rangeA1,
  });
  return r.data.values || [];
}

async function appendValues(rangeA1, rows) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: rangeA1,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: rows },
  });
}

async function updateValues(rangeA1, rows) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: rangeA1,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: rows },
  });
}

/* ================== UTIL ================== */
function nowIso() {
  return new Date().toISOString();
}

function parseMoney(input) {
  // supports: 100k, 0.5k, 57k, 200k, 120000, 200,000
  if (!input) return null;
  const s = String(input).trim().toLowerCase().replace(/,/g, "");
  const m = s.match(/^(\d+(?:\.\d+)?)(k)?$/);
  if (m) {
    const num = Number(m[1]);
    const isK = !!m[2];
    return Math.round(isK ? num * 1000 : num);
  }
  // raw number
  if (/^\d+$/.test(s)) return Number(s);
  return null;
}

function isEmail(x) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(x || "").trim());
}

function shortGameCode(token) {
  // accept: dabong/db, hopqua/hq/hh, qr
  const t = String(token || "").toLowerCase();
  if (t === "dabong" || t === "db") return "db";
  if (t === "hopqua" || t === "hq" || t === "hh") return "hq";
  if (t === "qr") return "qr";
  return "";
}

function formatVND(n) {
  return Number(n || 0).toLocaleString();
}

/* ================== SHEET LOGIC ==================
INVITES columns (recommended):
A ts_created
B game (hq/qr)
C name
D email
E time_invited_iso
F due_date_iso
G status (pending/done/expired)
H asked (0/1)
I asked_at_iso
J checkin_reward
K done_at_iso
L note

GAME_REVENUE columns:
A ts
B game (db/hq/qr/checkin/other)
C type (invite_reward/checkin/other)
D amount
E note
F chat_id
G user_name

CHECKIN_REWARD columns:
A ts
B game (hq/qr)
C name
D email
E due_date_iso
F amount
G chat_id
H user_name
==================================================== */

async function addGameRevenue({ game, type, amount, note, chatId, userName }) {
  await appendValues("GAME_REVENUE!A1", [
    [nowIso(), game, type, amount, note || "", String(chatId || ""), userName || ""],
  ]);
}

async function addInvite({ game, name, email }) {
  const invitedAt = dayjs();
  const due = invitedAt.add(14, "day");
  await appendValues("INVITES!A1", [
    [
      nowIso(),
      game,
      name,
      email,
      invitedAt.toISOString(),
      due.toISOString(),
      "pending",
      0,
      "",
      "",
      "",
      "",
    ],
  ]);
  return { invitedAt, due };
}

async function listInvites() {
  const rows = await getValues("INVITES!A2:L");
  // keep row index (A2 => index=2)
  return rows.map((r, i) => ({
    rowNumber: i + 2,
    ts_created: r[0] || "",
    game: r[1] || "",
    name: r[2] || "",
    email: r[3] || "",
    invited_at: r[4] || "",
    due_date: r[5] || "",
    status: r[6] || "",
    asked: String(r[7] || "0"),
    asked_at: r[8] || "",
    checkin_reward: r[9] || "",
    done_at: r[10] || "",
    note: r[11] || "",
  }));
}

async function markAsked(rowNumber) {
  await updateValues(`INVITES!H${rowNumber}:I${rowNumber}`, [[1, nowIso()]]);
}

async function markDone(rowNumber, rewardAmount) {
  await updateValues(`INVITES!G${rowNumber}:K${rowNumber}`, [
    ["done", 1, nowIso(), rewardAmount, nowIso()],
  ]);
}

/* ================== REMINDER STATE ==================
- Bot sẽ hỏi admin: "Hopqua Khanh = bao nhiêu?"
- Khi admin reply "60k" thì bot cộng revenue + mark done
We store "awaiting" map in memory:
key = chatId, value = { inviteRowNumber, game, name, email, due_date }
==================================================== */
const awaitingCheckin = new Map();

async function askCheckin(inv) {
  if (!ADMIN_TELEGRAM_ID) return;
  const gameName = inv.game === "hq" ? "Hopqua" : "QR";
  const msg = `⏰ Đến hạn 14 ngày!\n${gameName} ${inv.name} (${inv.email}) = bao nhiêu? (vd: 60k)`;
  awaitingCheckin.set(String(ADMIN_TELEGRAM_ID), {
    inviteRowNumber: inv.rowNumber,
    game: inv.game,
    name: inv.name,
    email: inv.email,
    due_date: inv.due_date,
  });
  await send(ADMIN_TELEGRAM_ID, msg, { reply_markup: buildMainKeyboard() });
  await markAsked(inv.rowNumber);
}

/* ================== CRON: scan due invites ==================
- chạy mỗi 10 phút
- tìm INVITES pending, due_date <= now, asked=0
- nhắc admin và set awaiting
==================================================== */
cron.schedule("*/10 * * * *", async () => {
  try {
    if (!ADMIN_TELEGRAM_ID) return;
    const invs = await listInvites();
    const now = dayjs();
    const dueList = invs.filter((x) => {
      if (x.status !== "pending") return false;
      if (String(x.asked) === "1") return false;
      if (!x.due_date) return false;
      return dayjs(x.due_date).isBefore(now) || dayjs(x.due_date).isSame(now);
    });

    for (const inv of dueList.slice(0, 5)) {
      await askCheckin(inv);
    }
  } catch (e) {
    console.error("CRON ERROR:", e?.message || e);
  }
});

/* ================== COMMANDS ================== */
function helpText() {
  return (
    "🆘 TIKTOK_LITE_BOT – lệnh nhanh\n\n" +
    "✅ Game revenue:\n" +
    "- dabong 100k\n" +
    "- hopqua 200k\n" +
    "- qr 57k\n\n" +
    "✅ Invite (theo dõi 14 ngày):\n" +
    "- hopqua Ten email@gmail.com\n" +
    "- qr Ten email@gmail.com\n\n" +
    "✅ Thêm thu khác:\n" +
    "- them 0.5k (hoặc them 2k)\n\n" +
    "Bot sẽ tự nhắc khi đến ngày 14 và hỏi bạn nhập (vd 60k / 30k)."
  );
}

async function handleStart(chatId) {
  await send(chatId, "✅ TikTok Lite Bot sẵn sàng. Nhập lệnh theo menu hoặc gõ nhanh.", {
    reply_markup: buildMainKeyboard(),
  });
}

async function handleReportMonth(chatId) {
  // Minimal placeholder: sum GAME_REVENUE current month
  const rows = await getValues("GAME_REVENUE!A2:G");
  const monthKey = dayjs().format("YYYY-MM");
  let sum = 0;
  let count = 0;
  for (const r of rows) {
    const ts = r[0] || "";
    const amount = Number(String(r[3] || "0").replace(/,/g, "")) || 0;
    if (ts && ts.startsWith(monthKey)) {
      sum += amount;
      count += 1;
    }
  }
  await send(chatId, `📊 Báo cáo tháng ${monthKey}\n• Tổng dòng: ${count}\n• Tổng thu (game+checkin+other): ${formatVND(sum)}`, {
    reply_markup: buildMainKeyboard(),
  });
}

async function handlePending(chatId) {
  const invs = await listInvites();
  const pend = invs.filter((x) => x.status === "pending");
  if (!pend.length) {
    await send(chatId, "📌 Không có nick pending 14 ngày.", { reply_markup: buildMainKeyboard() });
    return;
  }
  let out = `📌 Pending 14 ngày (${pend.length})\n`;
  for (const x of pend.slice(0, 30)) {
    const due = x.due_date ? dayjs(x.due_date).format("DD/MM/YYYY") : "N/A";
    out += `\n- ${x.game.toUpperCase()} | ${x.name} | ${x.email} | due: ${due} | asked:${x.asked}`;
  }
  await send(chatId, out.trim(), { reply_markup: buildMainKeyboard() });
}

/* ================== MAIN MESSAGE PARSER ================== */
async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  if (!chatId) return;

  const userName = msg.from?.first_name || "User";
  const textRaw = String(msg.text || "").trim();
  const lower = textRaw.toLowerCase();

  // menu buttons
  if (textRaw === "/start") return handleStart(chatId);
  if (textRaw === "🆘 Help") return send(chatId, helpText(), { reply_markup: buildMainKeyboard() });
  if (textRaw === "📊 Báo cáo tháng") return handleReportMonth(chatId);
  if (textRaw === "📌 Pending 14 ngày") return handlePending(chatId);
  if (textRaw.startsWith("/ping")) return send(chatId, "pong ✅", { reply_markup: buildMainKeyboard() });

  // checkin reply (admin)
  // if admin has awaiting request and user replies "60k"
  if (ADMIN_TELEGRAM_ID && String(chatId) === String(ADMIN_TELEGRAM_ID)) {
    const awaiting = awaitingCheckin.get(String(chatId));
    const amt = parseMoney(textRaw);
    if (awaiting && amt != null) {
      // log CHECKIN_REWARD + GAME_REVENUE(checkin)
      await appendValues("CHECKIN_REWARD!A1", [
        [nowIso(), awaiting.game, awaiting.name, awaiting.email, awaiting.due_date, amt, String(chatId), userName],
      ]);
      await addGameRevenue({
        game: awaiting.game,
        type: "checkin",
        amount: amt,
        note: `${awaiting.name} ${awaiting.email}`,
        chatId,
        userName,
      });
      await markDone(awaiting.inviteRowNumber, amt);
      awaitingCheckin.delete(String(chatId));
      await send(chatId, `✅ Đã ghi nhận checkin: ${awaiting.game.toUpperCase()} ${awaiting.name} = ${formatVND(amt)}`, {
        reply_markup: buildMainKeyboard(),
      });
      return;
    }
  }

  // tokenize
  const parts = textRaw.split(/\s+/).filter(Boolean);
  if (!parts.length) return;

  const cmd = parts[0].toLowerCase();

  // them 0.5k
  if (cmd === "them") {
    const amt = parseMoney(parts[1]);
    if (amt == null) {
      await send(chatId, "❌ Sai tiền. Ví dụ: them 0.5k", { reply_markup: buildMainKeyboard() });
      return;
    }
    await addGameRevenue({ game: "other", type: "other", amount: amt, note: "them", chatId, userName });
    await send(chatId, `✅ Đã cộng thu khác: ${formatVND(amt)}`, { reply_markup: buildMainKeyboard() });
    return;
  }

  // game commands: dabong / hopqua / qr
  const game = shortGameCode(cmd);
  if (game) {
    // Case A: reward amount (e.g. hopqua 200k, qr 57k, dabong 100k)
    if (parts.length >= 2) {
      const maybeAmt = parseMoney(parts[1]);
      const maybeName = parts[1];
      const maybeEmail = parts[2];

      // invite: hopqua Khanh mail@gmail.com
      if (parts.length >= 3 && maybeAmt == null && isEmail(maybeEmail)) {
        const name = maybeName;
        const email = maybeEmail;

        const { due } = await addInvite({ game, name, email });

        const dueFmt = dayjs(due).format("DD/MM/YYYY (ddd)");
        await send(
          chatId,
          `✅ Đã lưu INVITE:\n- game: ${game.toUpperCase()}\n- name: ${name}\n- email: ${email}\n- due: ${dueFmt}\n\n⏰ Bot sẽ nhắc khi tới hạn.`,
          { reply_markup: buildMainKeyboard() }
        );
        return;
      }

      // reward: hopqua 200k
      if (maybeAmt != null) {
        // db: invite_reward; hq/qr: invite_reward
        await addGameRevenue({
          game,
          type: "invite_reward",
          amount: maybeAmt,
          note: cmd,
          chatId,
          userName,
        });
        await send(chatId, `✅ Đã cộng doanh thu ${game.toUpperCase()}: ${formatVND(maybeAmt)}`, {
          reply_markup: buildMainKeyboard(),
        });
        return;
      }
    }

    await send(
      chatId,
      "❌ Sai cú pháp.\nVí dụ:\n- dabong 100k\n- hopqua 200k\n- hopqua Khanh mail@gmail.com\n- qr 57k\n- qr Khanh mail@gmail.com",
      { reply_markup: buildMainKeyboard() }
    );
    return;
  }

  // fallback
  await send(chatId, "❓ Không hiểu lệnh. Bấm 🆘 Help để xem cú pháp.", { reply_markup: buildMainKeyboard() });
}

/* ================== WEBHOOK ================== */
app.post("/webhook", async (req, res) => {
  res.sendStatus(200);
  try {
    const body = req.body;
    if (body?.message) {
      await handleTextMessage(body.message);
    }
  } catch (e) {
    console.error("WEBHOOK ERROR:", e?.message || e);
  }
});

/* ================== START ================== */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ TIKTOK_LITE_BOT READY on", PORT, "|", VERSION));
