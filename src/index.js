// src/index.js
/**
 * ============================================================
 * TIKTOK_LITE_BOT – Webhook + Google Sheets (googleapis v4)
 *
 * ENV REQUIRED:
 * - BOT_TOKEN
 * - GOOGLE_SHEET_ID
 * - GOOGLE_APPLICATION_CREDENTIALS (default: /etc/secrets/google-service-account.json)
 * - ADMIN_TELEGRAM_ID
 * ============================================================
 */

import express from "express";
import fetch from "node-fetch";
import cron from "node-cron";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import { google } from "googleapis";

dayjs.extend(utc);

const VERSION = "v2.0-inline-menu+reset";

const BOT_TOKEN = process.env.BOT_TOKEN;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "/etc/secrets/google-service-account.json";
const ADMIN_TELEGRAM_ID = String(process.env.ADMIN_TELEGRAM_ID || "").trim();

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");
if (!ADMIN_TELEGRAM_ID) throw new Error("Missing ADMIN_TELEGRAM_ID");

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

async function clearValues(rangeA1) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: rangeA1,
  });
}

/* ================== TELEGRAM ================== */
async function tg(method, payload) {
  const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
  const resp = await fetch(url, {
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

async function edit(chatId, messageId, text, extra = {}) {
  if (!chatId || !messageId) return;
  await tg("editMessageText", { chat_id: chatId, message_id: messageId, text, ...extra });
}

function ik(rows) {
  return { inline_keyboard: rows };
}

/* ================== INLINE MENUS (LEFT/RIGHT) ================== */
function buildHomeMenu() {
  return ik([
    [
      { text: "⬅️ MENU TRÁI", callback_data: "menu:left" },
      { text: "➡️ MENU PHẢI", callback_data: "menu:right" },
    ],
    [{ text: "🆘 Help", callback_data: "menu:help" }],
  ]);
}

function buildLeftMenu() {
  return ik([
    [
      { text: "💰 Dabong", callback_data: "quick:db" },
      { text: "🎁 Hopqua", callback_data: "quick:hq" },
    ],
    [
      { text: "🔳 QR", callback_data: "quick:qr" },
      { text: "➕ Thêm thu", callback_data: "quick:them" },
    ],
    [{ text: "⬅️ Back", callback_data: "menu:home" }],
  ]);
}

function buildRightMenu() {
  return ik([
    [{ text: "📊 Báo cáo tháng", callback_data: "action:report_month" }],
    [{ text: "📌 Pending 14 ngày", callback_data: "action:pending" }],
    [{ text: "📱 Thống kê máy", callback_data: "action:phone_stats" }],
    [{ text: "♻️ RESET (xóa dữ liệu)", callback_data: "action:reset" }],
    [{ text: "⬅️ Back", callback_data: "menu:home" }],
  ]);
}

function buildResetConfirmMenu() {
  return ik([
    [{ text: "✅ XÓA HẾT & CHẠY LẠI", callback_data: "reset:confirm" }],
    [{ text: "❌ HỦY", callback_data: "reset:cancel" }],
  ]);
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
  if (/^\d+$/.test(s)) return Number(s);
  return null;
}

function isEmail(x) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(x || "").trim());
}

function shortGameCode(token) {
  const t = String(token || "").toLowerCase();
  if (t === "dabong" || t === "db") return "db";
  if (t === "hopqua" || t === "hq" || t === "hh") return "hq";
  if (t === "qr") return "qr";
  return "";
}

function formatVND(n) {
  const x = Number(n || 0);
  return x.toLocaleString("vi-VN") + "đ";
}

function isAdmin(chatId) {
  return String(chatId) === ADMIN_TELEGRAM_ID;
}

/* ================== HELP ================== */
function helpText() {
  return (
    "📌 CÚ PHÁP NHANH:\n" +
    "- dabong 100k\n" +
    "- hopqua 200k\n" +
    "- qr 57k\n" +
    "- them 0.5k\n\n" +
    "📌 INVITE 14 NGÀY:\n" +
    "- hopqua Ten email@gmail.com\n" +
    "- qr Ten email@gmail.com\n\n" +
    "📌 ADMIN trả lời khi bot hỏi checkin:\n" +
    "- 60k\n\n" +
    "📌 LỆNH:\n" +
    "- /start\n" +
    "- /help\n" +
    "- /pending\n" +
    "- /report\n" +
    "- /reset (ADMIN)\n"
  );
}

/* ================== INVITES (14 days) ================== */
function calcInviteDates(invitedAtIso = null) {
  const invitedAt = invitedAtIso ? dayjs(invitedAtIso) : dayjs();
  const due = invitedAt.add(14, "day");
  return { invitedAt, due };
}

async function listInvites() {
  const rows = await getValues("INVITES!A2:L");
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

async function addInvite({ game, name, email }) {
  const { invitedAt, due } = calcInviteDates();
  const row = [
    nowIso(), // A ts_created
    game, // B
    name, // C
    email, // D
    invitedAt.toISOString(), // E invited_at
    due.toISOString(), // F due_date
    "pending", // G status
    0, // H asked
    "", // I asked_at
    "", // J checkin_reward
    "", // K done_at
    "", // L note
  ];
  await appendValues("INVITES!A:L", [row]);
  return { invitedAt, due };
}

async function markAsked(rowNumber) {
  await updateValues(`INVITES!H${rowNumber}:I${rowNumber}`, [[1, nowIso()]]);
}

async function markDone(rowNumber, rewardAmount) {
  await updateValues(`INVITES!G${rowNumber}:K${rowNumber}`, [["done", 1, nowIso(), rewardAmount, nowIso()]]);
}

/* ================== GAME REVENUE ================== */
async function addGameRevenue({ game, amount, note = "" }) {
  const row = [nowIso(), game, amount, note];
  await appendValues("GAME_REVENUE!A:D", [row]);
}

async function addCheckinRevenue({ game, amount, name, email }) {
  // CHECKIN_REWARD
  await appendValues("CHECKIN_REWARD!A:F", [[nowIso(), game, name, email, amount, "auto_due"]]);
  // GAME_REVENUE (as "checkin")
  await addGameRevenue({ game: `${game}_checkin`, amount, note: `${name} ${email}` });
}

/* ================== REPORTS ================== */
async function handleReportMonth(chatId) {
  const rows = await getValues("GAME_REVENUE!A2:D");
  const month = dayjs().format("YYYY-MM");
  let total = 0;

  for (const r of rows) {
    const ts = r[0];
    const amt = Number(r[2] || 0);
    if (!ts) continue;
    if (dayjs(ts).format("YYYY-MM") === month) total += amt;
  }

  await send(chatId, `📊 Báo cáo tháng ${month}\nTổng thu: ${formatVND(total)}`, {
    reply_markup: buildHomeMenu(),
  });
}

async function handlePending(chatId) {
  const invites = await listInvites();
  const pending = invites
    .filter((x) => x.status === "pending")
    .sort((a, b) => String(a.due_date).localeCompare(String(b.due_date)));

  if (!pending.length) {
    await send(chatId, "✅ Không có invite pending.", { reply_markup: buildHomeMenu() });
    return;
  }

  const lines = pending.slice(0, 30).map((x) => {
    const due = x.due_date ? dayjs(x.due_date).format("DD/MM") : "??";
    return `- ${x.game.toUpperCase()} | ${x.name} | ${x.email} | due ${due} | asked=${x.asked}`;
  });

  await send(chatId, `📌 Pending 14 ngày (${pending.length})\n` + lines.join("\n"), {
    reply_markup: buildHomeMenu(),
  });
}

async function handlePhoneStats(chatId) {
  await send(chatId, "📱 Phần MÁY/LÔ chưa triển khai (đúng roadmap). Khi bạn cần mình sẽ làm tiếp.", {
    reply_markup: buildHomeMenu(),
  });
}

/* ================== DUE CHECK (CRON) ================== */
async function checkDueInvitesAndPingAdmin() {
  const invites = await listInvites();
  const now = dayjs();

  // due: pending, not asked, due_date <= now
  const dueList = invites.filter((x) => {
    if (x.status !== "pending") return false;
    if (String(x.asked || "0") === "1") return false;
    if (!x.due_date) return false;
    return dayjs(x.due_date).isBefore(now) || dayjs(x.due_date).isSame(now);
  });

  for (const x of dueList) {
    // mark asked
    await markAsked(x.rowNumber);

    const gameName = x.game === "hq" ? "Hopqua" : x.game === "qr" ? "QR" : x.game;
    await send(ADMIN_TELEGRAM_ID, `⏰ ĐẾN HẠN CHECKIN!\n${gameName} ${x.name}\nTrả lời số tiền (vd: 60k)`, {
      reply_markup: buildHomeMenu(),
    });
  }
}

// every 10 minutes
cron.schedule("*/10 * * * *", async () => {
  try {
    await checkDueInvitesAndPingAdmin();
  } catch (e) {
    console.error("CRON ERROR:", e?.message || e);
  }
});

/* ================== RESET (ADMIN ONLY) ================== */
const RESET_CLEAR_RANGES = [
  "SETTINGS!A2:Z",
  "WALLETS!A2:Z",
  "WALLET_LOG!A2:Z",
  "PHONES!A2:Z",
  "LOTS!A2:Z",
  "LOT_RESULT!A2:Z",
  "PHONE_PROFIT_LOG!A2:Z",
  "INVITES!A2:Z",
  "CHECKIN_REWARD!A2:Z",
  "GAME_REVENUE!A2:Z",
  "UNDO_LOG!A2:Z",
];

async function resetAllData() {
  for (const r of RESET_CLEAR_RANGES) {
    try {
      await clearValues(r);
    } catch (e) {
      console.error("RESET clear error:", r, e?.message || e);
    }
  }
}

/* ================== MESSAGE HANDLERS ================== */
async function handleStart(chatId) {
  await send(chatId, `✅ TIKTOK_LITE_BOT READY (${VERSION})\nBấm menu để mở chức năng.`, {
    reply_markup: buildHomeMenu(),
  });
}

async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  const text = String(msg.text || "").trim();
  if (!chatId || !text) return;

  const lower = text.toLowerCase();

  // commands
  if (lower === "/start" || lower === "start") return handleStart(chatId);
  if (lower === "/help" || lower === "help" || lower.includes("🆘")) {
    return send(chatId, helpText(), { reply_markup: buildHomeMenu() });
  }
  if (lower === "/pending") return handlePending(chatId);
  if (lower === "/report") return handleReportMonth(chatId);

  // reset command (admin)
  if (lower === "/reset" || lower === "reset" || lower === "resert") {
    if (!isAdmin(chatId)) return send(chatId, "❌ Bạn không có quyền dùng RESET.", { reply_markup: buildHomeMenu() });
    return send(
      chatId,
      "⚠️ RESET sẽ XÓA TOÀN BỘ DỮ LIỆU (trừ dòng tiêu đề) trên các sheet.\nBạn chắc chắn chứ?",
      { reply_markup: buildResetConfirmMenu() }
    );
  }

  // admin replies to due ping: amount only (e.g. 60k)
  if (isAdmin(chatId)) {
    const amtOnly = parseMoney(lower);
    if (amtOnly != null) {
      // find latest asked pending invite (asked=1, status=pending, checkin_reward empty) sort asked_at desc
      const invites = await listInvites();
      const cand = invites
        .filter((x) => x.status === "pending" && String(x.asked || "0") === "1" && !x.checkin_reward)
        .sort((a, b) => String(b.asked_at).localeCompare(String(a.asked_at)))[0];

      if (cand) {
        await addCheckinRevenue({ game: cand.game, amount: amtOnly, name: cand.name, email: cand.email });
        await markDone(cand.rowNumber, amtOnly);
        await send(chatId, `✅ Đã ghi check-in: ${cand.game.toUpperCase()} ${cand.name} = ${formatVND(amtOnly)}`, {
          reply_markup: buildHomeMenu(),
        });
        return;
      }
    }
  }

  // quick menu text fallback (old keyboard texts)
  if (lower.startsWith("📊")) return handleReportMonth(chatId);
  if (lower.startsWith("📌")) return handlePending(chatId);
  if (lower.startsWith("📱")) return handlePhoneStats(chatId);

  // parse normal commands: dabong/hopqua/qr/them
  const parts = text.split(/\s+/).filter(Boolean);
  const cmd = parts[0];
  const game = shortGameCode(cmd);

  // "them 0.5k" => add revenue with game = "them"
  if (cmd.toLowerCase() === "them") {
    const amt = parseMoney(parts[1]);
    if (amt == null) return send(chatId, "❌ Sai cú pháp. Ví dụ: them 0.5k", { reply_markup: buildHomeMenu() });
    await addGameRevenue({ game: "them", amount: amt });
    return send(chatId, `✅ Đã ghi THÊM: ${formatVND(amt)}`, { reply_markup: buildHomeMenu() });
  }

  // game commands: dabong / hopqua / qr
  if (game) {
    // Case invite: hopqua Khanh mail@gmail.com
    if (parts.length >= 3) {
      const maybeAmt = parseMoney(parts[1]);
      const maybeName = parts[1];
      const maybeEmail = parts[2];
      if (maybeAmt == null && isEmail(maybeEmail)) {
        const { due } = await addInvite({ game, name: maybeName, email: maybeEmail });
        return send(
          chatId,
          `✅ Đã lưu invite: ${game.toUpperCase()} | ${maybeName} | ${maybeEmail}\n⏰ Due: ${dayjs(due).format(
            "DD/MM/YYYY"
          )}`,
          { reply_markup: buildHomeMenu() }
        );
      }
    }

    // Case revenue: hopqua 200k
    if (parts.length >= 2) {
      const amt = parseMoney(parts[1]);
      if (amt == null) {
        return send(chatId, "❌ Sai cú pháp. Ví dụ: hopqua 200k (hoặc hopqua Ten email@gmail.com)", {
          reply_markup: buildHomeMenu(),
        });
      }
      await addGameRevenue({ game, amount: amt });
      return send(chatId, `✅ Đã ghi ${game.toUpperCase()}: ${formatVND(amt)}`, { reply_markup: buildHomeMenu() });
    }

    return send(chatId, "❌ Thiếu dữ liệu. Ví dụ: dabong 100k", { reply_markup: buildHomeMenu() });
  }

  // unknown
  await send(chatId, "❓ Không hiểu. Bấm 🆘 Help để xem cú pháp.", { reply_markup: buildHomeMenu() });
}

async function handleCallbackQuery(cq) {
  const chatId = cq.message?.chat?.id;
  const messageId = cq.message?.message_id;
  const data = String(cq.data || "");

  await tg("answerCallbackQuery", { callback_query_id: cq.id }).catch(() => {});
  if (!chatId || !messageId) return;

  // MENUS
  if (data === "menu:home") {
    return edit(chatId, messageId, "🏠 Menu chính", { reply_markup: buildHomeMenu() });
  }
  if (data === "menu:left") {
    return edit(chatId, messageId, "⬅️ MENU TRÁI (Thu nhanh)", { reply_markup: buildLeftMenu() });
  }
  if (data === "menu:right") {
    return edit(chatId, messageId, "➡️ MENU PHẢI (Báo cáo/Quản trị)", { reply_markup: buildRightMenu() });
  }
  if (data === "menu:help") {
    return edit(chatId, messageId, helpText(), { reply_markup: buildHomeMenu() });
  }

  // QUICK EXAMPLES
  if (data.startsWith("quick:")) {
    const k = data.split(":")[1];
    const examples = {
      db: "dabong 100k",
      hq: "hopqua 200k\nhoặc: hopqua Ten email@gmail.com",
      qr: "qr 57k\nhoặc: qr Ten email@gmail.com",
      them: "them 0.5k",
    };
    return send(chatId, `📌 Gửi theo mẫu:\n${examples[k] || ""}`, { reply_markup: buildHomeMenu() });
  }

  // ACTIONS
  if (data === "action:report_month") return handleReportMonth(chatId);
  if (data === "action:pending") return handlePending(chatId);
  if (data === "action:phone_stats") return handlePhoneStats(chatId);

  // RESET (ADMIN)
  if (data === "action:reset") {
    if (!isAdmin(chatId)) return send(chatId, "❌ Bạn không có quyền dùng RESET.", { reply_markup: buildHomeMenu() });
    return edit(
      chatId,
      messageId,
      "⚠️ RESET sẽ XÓA TOÀN BỘ DỮ LIỆU (trừ dòng tiêu đề) trên các sheet.\nBạn chắc chắn chứ?",
      { reply_markup: buildResetConfirmMenu() }
    );
  }
  if (data === "reset:cancel") {
    return edit(chatId, messageId, "✅ Đã hủy RESET.", { reply_markup: buildHomeMenu() });
  }
  if (data === "reset:confirm") {
    if (!isAdmin(chatId)) return edit(chatId, messageId, "❌ Bạn không có quyền dùng RESET.", { reply_markup: buildHomeMenu() });
    await edit(chatId, messageId, "⏳ Đang RESET dữ liệu...", {});
    await resetAllData();
    return edit(chatId, messageId, "✅ RESET xong. Bot sẵn sàng chạy mới từ đầu.", { reply_markup: buildHomeMenu() });
  }
}

/* ================== EXPRESS WEBHOOK ================== */
const app = express();
app.use(express.json());

app.get("/", (req, res) => res.status(200).send(`OK ${VERSION}`));

app.post("/webhook", async (req, res) => {
  res.sendStatus(200);
  try {
    const body = req.body;
    if (body?.message) await handleTextMessage(body.message);
    if (body?.callback_query) await handleCallbackQuery(body.callback_query);
  } catch (e) {
    console.error("WEBHOOK ERROR:", e?.message || e);
  }
});

/* ================== START ================== */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ TIKTOK_LITE_BOT READY on", PORT, "|", VERSION));
