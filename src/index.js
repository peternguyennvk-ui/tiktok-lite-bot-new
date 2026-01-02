// src/index.js
/**
 * ============================================================
 * TIKTOK_LITE_BOT – Render/Node Webhook + Google Sheets + Cron
 * FINAL: Menu Left/Right + Reports + Reset(pass) + Edit last + Gemini
 * Currency display: WON (₩)
 *
 * ENV REQUIRED:
 * - BOT_TOKEN
 * - GOOGLE_SHEET_ID
 * - GOOGLE_APPLICATION_CREDENTIALS (default: /etc/secrets/google-service-account.json)
 * OPTIONAL:
 * - ADMIN_TELEGRAM_ID (để cron nhắc 14 ngày + nhận reply checkin)
 *
 * SHEETS expected:
 * - SETTINGS (A:key, B:value) header at row1
 * - GAME_REVENUE, INVITES, CHECKIN_REWARD, PHONE_PROFIT_LOG, PHONES, ...
 *
 * NOTE:
 * - Không dùng PropertiesService (Google Apps Script) -> Node only
 * ============================================================
 */

import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* ================== CONFIG ================== */
const VERSION = "v5.0-final-noGAS-won-gemini";
const BOT_TOKEN = process.env.BOT_TOKEN;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "/etc/secrets/google-service-account.json";
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID ? String(process.env.ADMIN_TELEGRAM_ID).trim() : "";

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");

const TELEGRAM_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

/* ================== EXPRESS ================== */
const app = express();
app.use(express.json());

app.get("/", (_, res) => res.status(200).send(`OK ${VERSION}`));
app.get("/ping", (_, res) => res.status(200).json({ ok: true, version: VERSION }));

/* ================== TELEGRAM ================== */
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

/* ================== KEYBOARDS (Reply keyboard like your screenshot) ================== */
function kb(rows) {
  return {
    keyboard: rows,
    resize_keyboard: true,
    one_time_keyboard: false,
    is_persistent: true,
  };
}

function mainKb() {
  return kb([[{ text: "⬅️ Menu" }, { text: "➡️ Menu" }]]);
}

function leftKb() {
  return kb([
    [{ text: "🎁 Hộp quà (mời)" }, { text: "🔳 QR (mời)" }],
    [{ text: "⚽ Đá bóng (thu)" }, { text: "🎁 Hộp quà (thu)" }],
    [{ text: "🔳 QR (thu)" }, { text: "➕ Thu khác" }],
    [{ text: "⬅️ Back" }],
  ]);
}

function rightKb() {
  return kb([
    [{ text: "1️⃣ Xem tổng doanh thu" }],
    [{ text: "2️⃣ Doanh thu tháng này" }, { text: "3️⃣ Doanh thu tháng trước" }],
    [{ text: "4️⃣ Thống kê ĐB / HQ / QR" }],
    [{ text: "7️⃣ Lời lỗ mua máy" }],
    [{ text: "📘 Hướng dẫn" }],
    [{ text: "🔑 Nhập Gemini Key" }, { text: "🤖 AI: Bật/Tắt" }],
    [{ text: "8️⃣ Xóa sạch dữ liệu" }],
    [{ text: "⬅️ Back" }],
  ]);
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

async function clearValues(rangeA1) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: rangeA1,
  });
}

/* ================== MONEY / PARSE ================== */
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
    if (!Number.isFinite(num)) return null;
    return Math.round(isK ? num * 1000 : num);
  }
  if (/^\d+$/.test(s)) return Number(s);
  return null;
}

function formatMoneyWon(n) {
  return "₩" + Number(n || 0).toLocaleString("ko-KR");
}

function isEmail(x) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(x || "").trim());
}

function shortGameCode(token) {
  const t = String(token || "").toLowerCase();
  if (t === "dabong" || t === "db") return "db";
  if (t === "hopqua" || t === "hq" || t === "hh") return "hq";
  if (t === "qr") return "qr";
  if (t === "them" || t === "other") return "other";
  return "";
}

function monthKey(ts) {
  if (!ts) return "";
  return String(ts).slice(0, 7); // YYYY-MM from ISO
}

/* ================== SETTINGS (Gemini key / toggle / model) ================== */
async function getSetting(key) {
  const rows = await getValues("SETTINGS!A2:B");
  for (const r of rows) {
    const k = String(r[0] || "").trim();
    if (k === key) return String(r[1] || "");
  }
  return "";
}

async function setSetting(key, value) {
  const rows = await getValues("SETTINGS!A2:B");
  for (let i = 0; i < rows.length; i++) {
    const k = String(rows[i][0] || "").trim();
    if (k === key) {
      const rowNumber = i + 2;
      await updateValues(`SETTINGS!A${rowNumber}:B${rowNumber}`, [[key, value]]);
      return;
    }
  }
  await appendValues("SETTINGS!A1", [[key, value]]);
}

async function getGeminiConfig() {
  const apiKey = (await getSetting("GEMINI_API_KEY")).trim();
  const enabled = (await getSetting("GEMINI_ENABLED")).trim() === "1";
  const model = (await getSetting("GEMINI_MODEL")).trim() || "gemini-2.0-flash";
  return { apiKey, enabled, model };
}

/* ================== DATABASE (Sheets) ================== */
/**
 * GAME_REVENUE columns (suggested):
 * A ts
 * B game (db/hq/qr/other)
 * C type (invite_reward/checkin/other/adjust)
 * D amount (number)
 * E note
 * F chat_id
 * G user_name
 *
 * INVITES columns:
 * A ts_created
 * B game (hq/qr)
 * C name
 * D email
 * E invited_at_iso
 * F due_date_iso
 * G status (pending/done)
 * H asked (0/1)
 * I asked_at_iso
 * J checkin_reward
 * K done_at_iso
 * L note
 */

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
      nowIso(), // A
      game, // B
      name, // C
      email, // D
      invitedAt.toISOString(), // E
      due.toISOString(), // F
      "pending", // G
      0, // H
      "", // I
      "", // J
      "", // K
      "", // L
    ],
  ]);
  return { invitedAt, due };
}

async function listInvites() {
  const rows = await getValues("INVITES!A2:L");
  return rows.map((r, i) => ({
    rowNumber: i + 2,
    game: (r[1] || "").toLowerCase(),
    name: r[2] || "",
    email: r[3] || "",
    due_date: r[5] || "",
    status: (r[6] || "").toLowerCase(),
    asked: String(r[7] || "0"),
    asked_at: r[8] || "",
    checkin_reward: r[9] || "",
  }));
}

async function markAsked(rowNumber) {
  await updateValues(`INVITES!H${rowNumber}:I${rowNumber}`, [[1, nowIso()]]);
}

async function markDone(rowNumber, rewardAmount) {
  // update G..K in one go if your sheet matches; otherwise safe to update key cells
  await updateValues(`INVITES!G${rowNumber}:K${rowNumber}`, [
    ["done", 1, nowIso(), rewardAmount, nowIso()],
  ]);
}

async function addCheckinReward({ game, name, email, due_date, amount, chatId, userName }) {
  await appendValues("CHECKIN_REWARD!A1", [
    [nowIso(), game, name, email, due_date || "", amount, String(chatId || ""), userName || ""],
  ]);
}

/* ================== REPORTS ================== */
async function readGameRevenue() {
  const rows = await getValues("GAME_REVENUE!A2:G");
  return rows.map((r) => ({
    ts: r[0] || "",
    game: (r[1] || "").toLowerCase(),
    type: (r[2] || "").toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    chat_id: String(r[5] || ""),
  }));
}

async function reportTotal(chatId) {
  const rows = await readGameRevenue();
  const sum = rows.reduce((a, b) => a + b.amount, 0);
  await send(chatId, `📌 TỔNG DOANH THU\n= ${formatMoneyWon(sum)}`, { reply_markup: rightKb() });
}

async function reportThisMonth(chatId) {
  const m = dayjs().format("YYYY-MM");
  await reportMonth(chatId, m);
}

async function reportLastMonth(chatId) {
  const m = dayjs().subtract(1, "month").format("YYYY-MM");
  await reportMonth(chatId, m);
}

async function reportMonth(chatId, m) {
  const rows = await readGameRevenue();
  const sum = rows.filter((x) => monthKey(x.ts) === m).reduce((a, b) => a + b.amount, 0);
  await send(chatId, `📊 DOANH THU THÁNG ${m}\n= ${formatMoneyWon(sum)}`, { reply_markup: rightKb() });
}

async function reportStatsGames(chatId) {
  const rev = await readGameRevenue();
  const inv = await listInvites();

  // "Số người đá bóng": mỗi dòng revenue db invite_reward = 1 người
  const dbCount = rev.filter((x) => x.game === "db" && x.type === "invite_reward").length;

  // Hộp quà + QR: số người = số invite
  const hqCount = inv.filter((x) => x.game === "hq").length;
  const qrCount = inv.filter((x) => x.game === "qr").length;

  // doanh thu theo game (gồm cả checkin/adjust nếu có)
  const dbSum = rev.filter((x) => x.game === "db").reduce((a, b) => a + b.amount, 0);
  const hqSum = rev.filter((x) => x.game === "hq").reduce((a, b) => a + b.amount, 0);
  const qrSum = rev.filter((x) => x.game === "qr").reduce((a, b) => a + b.amount, 0);

  const out =
    `4️⃣ THỐNG KÊ ĐB / HQ / QR\n\n` +
    `⚽ Đá bóng: người = ${dbCount} | doanh thu = ${formatMoneyWon(dbSum)}\n` +
    `🎁 Hộp quà: người = ${hqCount} | doanh thu = ${formatMoneyWon(hqSum)}\n` +
    `🔳 QR: người = ${qrCount} | doanh thu = ${formatMoneyWon(qrSum)}\n`;

  await send(chatId, out, { reply_markup: rightKb() });
}

async function reportPhoneProfit(chatId) {
  // PHONE_PROFIT_LOG: lấy số ở ô cuối có thể parseMoney -> coi như profit của dòng
  const logs = await getValues("PHONE_PROFIT_LOG!A2:Z");
  let count = 0;
  let sum = 0;
  let loi = 0, hue = 0, tach = 0;

  for (const r of logs) {
    let found = null;
    for (let i = r.length - 1; i >= 0; i--) {
      const amt = parseMoney(r[i]);
      if (amt != null) {
        found = amt;
        break;
      }
    }
    if (found == null) continue;
    count += 1;
    sum += found;
    if (found > 0) loi += 1;
    else if (found === 0) hue += 1;
    else tach += 1;
  }

  // số máy đã mua: số dòng PHONES có dữ liệu
  const phones = await getValues("PHONES!A2:Z");
  const totalPhones = phones.filter((r) => r.some((c) => String(c || "").trim() !== "")).length;

  const out =
    `7️⃣ LỜI/LỖ MUA MÁY\n\n` +
    `• Số máy đã mua (PHONES): ${totalPhones}\n` +
    `• Số log lời/lỗ (PHONE_PROFIT_LOG): ${count}\n\n` +
    `• Lời: ${loi}\n` +
    `• Huề: ${hue}\n` +
    `• Tạch: ${tach}\n\n` +
    `💰 Tổng lời/lỗ: ${formatMoneyWon(sum)}\n` +
    `(Số dương = lời, 0 = huề, số âm = tạch)`;

  await send(chatId, out, { reply_markup: rightKb() });
}

/* ================== HELP ================== */
function helpTextWon() {
  return (
    "📘 HƯỚNG DẪN – lệnh nhập tay (WON ₩)\n\n" +
    "✅ Thu game:\n" +
    "- dabong 100k\n" +
    "- hopqua 200k\n" +
    "- qr 57k\n\n" +
    "✅ Invite (14 ngày):\n" +
    "- hopqua Ten email@gmail.com\n" +
    "- qr Ten email@gmail.com\n\n" +
    "✅ Thu khác:\n" +
    "- them 0.5k\n\n" +
    "✅ Sửa lệnh thu gần nhất:\n" +
    "- /edit last\n\n" +
    "✅ AI:\n" +
    "- /ai <câu hỏi>\n" +
    "- (Bật AI ở ➡️ Menu → 🤖 AI: Bật/Tắt)\n"
  );
}

/* ================== RESET ================== */
const RESET_PASS = "12345";
const RESET_CLEAR_RANGES = [
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
  "SETTINGS!A2:Z", // bạn muốn xóa sạch làm lại -> xóa luôn key (đúng yêu cầu)
];

async function resetAllData() {
  for (const r of RESET_CLEAR_RANGES) {
    try {
      await clearValues(r);
    } catch (e) {
      console.error("RESET error:", r, e?.message || e);
    }
  }
}

/* ================== SESSIONS (multi-step input) ================== */
const sessions = new Map();
/**
 * flow:
 * - invite: {game:hq|qr, step:name|email, data:{}}
 * - revenue: {game:db|hq|qr|other, step:amount}
 * - reset: {step:pass}
 * - gemini_key: {step:key}
 * - edit: {step:amount, data:{game,type,amount}}
 */
function setSession(chatId, sess) {
  sessions.set(String(chatId), sess);
}
function getSession(chatId) {
  return sessions.get(String(chatId));
}
function clearSession(chatId) {
  sessions.delete(String(chatId));
}

/* ================== EDIT LAST (robust: read from sheet) ================== */
async function getLastRevenueForChat(chatId) {
  // Scan from end (limit 2000 rows for safety)
  const rows = await getValues("GAME_REVENUE!A2:G");
  const target = String(chatId);
  for (let i = rows.length - 1; i >= 0 && i >= rows.length - 2000; i--) {
    const r = rows[i];
    const game = (r[1] || "").toLowerCase();
    const type = (r[2] || "").toLowerCase();
    const amount = Number(String(r[3] || "0").replace(/,/g, "")) || 0;
    const cid = String(r[5] || "");
    if (cid === target && ["db", "hq", "qr", "other"].includes(game) && amount !== 0) {
      // last revenue entry (including adjust/checkin/other)
      return { game, type: type || "invite_reward", amount };
    }
  }
  return null;
}

async function startEditLast(chatId) {
  const last = await getLastRevenueForChat(chatId);
  if (!last) {
    await send(chatId, "❌ Không tìm thấy lệnh thu gần nhất để sửa.", { reply_markup: mainKb() });
    return;
  }
  setSession(chatId, { flow: "edit", step: "amount", data: last });
  await send(
    chatId,
    `✏️ SỬA LỆNH GẦN NHẤT\nLệnh gần nhất: ${last.game.toUpperCase()} ${formatMoneyWon(last.amount)}\nNhập số tiền MỚI (vd 80k):`,
    { reply_markup: mainKb() }
  );
}

/* ================== GEMINI ================== */
async function geminiGenerate(apiKey, model, prompt, responseMimeType = null) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const body = {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    generationConfig: responseMimeType ? { responseMimeType } : undefined,
  };

  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const json = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg = json?.error?.message || json?.message || `Gemini HTTP ${resp.status}`;
    throw new Error(msg);
  }
  const text =
    json?.candidates?.[0]?.content?.parts?.map((p) => p.text || "").join("") || "";
  return text.trim();
}

function buildParsePrompt(userText) {
  return (
    "Bạn là bộ phân tích lệnh cho bot TikTok Lite. " +
    "Trả về DUY NHẤT 1 object JSON, không giải thích.\n\n" +
    "Các action hợp lệ:\n" +
    '1) {"action":"revenue","game":"db|hq|qr|other","amount":<number>,"type":"invite_reward|other"}\n' +
    '2) {"action":"invite","game":"hq|qr","name":"...","email":"..."}\n' +
    '3) {"action":"unknown"}\n\n' +
    "Quy ước:\n" +
    "- amount là số WON (₩). 100k nghĩa là 100000.\n\n" +
    `Input: ${userText}\n\nJSON:`
  );
}

async function tryGeminiParse(chatId, userName, userText) {
  const { apiKey, enabled, model } = await getGeminiConfig();
  if (!enabled) return false;

  if (!apiKey) {
    await send(chatId, "⚠️ AI đang bật nhưng chưa có Gemini Key. Vào ➡️ Menu → 🔑 Nhập Gemini Key.", {
      reply_markup: rightKb(),
    });
    return true;
  }

  try {
    const prompt = buildParsePrompt(userText);
    const out = await geminiGenerate(apiKey, model, prompt, "application/json");
    let obj = null;
    try {
      obj = JSON.parse(out);
    } catch {
      const m = out.match(/\{[\s\S]*\}/);
      if (m) obj = JSON.parse(m[0]);
    }
    if (!obj || !obj.action) return false;

    if (obj.action === "revenue") {
      const game = String(obj.game || "").toLowerCase();
      const type = String(obj.type || "other").toLowerCase();
      const amount = Number(obj.amount);
      if (!["db", "hq", "qr", "other"].includes(game)) return false;
      if (!Number.isFinite(amount)) return false;

      await addGameRevenue({
        game,
        type: type === "invite_reward" ? "invite_reward" : "other",
        amount: Math.round(amount),
        note: "AI_PARSE",
        chatId,
        userName,
      });

      await send(chatId, `🤖✅ AI ghi thu: ${game.toUpperCase()} ${formatMoneyWon(Math.round(amount))}`, {
        reply_markup: mainKb(),
      });
      return true;
    }

    if (obj.action === "invite") {
      const game = String(obj.game || "").toLowerCase();
      const name = String(obj.name || "").trim();
      const email = String(obj.email || "").trim();
      if (!["hq", "qr"].includes(game)) return false;
      if (!name || !isEmail(email)) return false;

      const { due } = await addInvite({ game, name, email });
      const dueFmt = dayjs(due).format("DD/MM/YYYY");
      await send(
        chatId,
        `🤖✅ AI lưu INVITE:\n- game: ${game.toUpperCase()}\n- name: ${name}\n- email: ${email}\n- due: ${dueFmt}`,
        { reply_markup: mainKb() }
      );
      return true;
    }

    return false;
  } catch (e) {
    await send(chatId, `⚠️ AI lỗi: ${e?.message || e}`, { reply_markup: mainKb() });
    return true;
  }
}

/* ================== CRON DUE INVITES ================== */
const awaitingCheckin = new Map();

async function askCheckin(inv) {
  if (!ADMIN_TELEGRAM_ID) return;
  const label = inv.game === "hq" ? "Hopqua" : "QR";
  awaitingCheckin.set(ADMIN_TELEGRAM_ID, inv);

  await send(
    ADMIN_TELEGRAM_ID,
    `⏰ ĐẾN HẠN 14 NGÀY!\n${label} ${inv.name} (${inv.email})\nTrả lời số tiền (vd: 60k)`,
    { reply_markup: mainKb() }
  );
  await markAsked(inv.rowNumber);
}

cron.schedule("*/10 * * * *", async () => {
  try {
    if (!ADMIN_TELEGRAM_ID) return;

    const invites = await listInvites();
    const now = dayjs();

    const dueList = invites.filter((x) => {
      if (x.status !== "pending") return false;
      if (x.asked === "1") return false;
      if (!x.due_date) return false;
      const d = dayjs(x.due_date);
      return d.isBefore(now) || d.isSame(now);
    });

    for (const inv of dueList.slice(0, 5)) {
      await askCheckin(inv);
    }
  } catch (e) {
    console.error("CRON ERROR:", e?.message || e);
  }
});

/* ================== MAIN HANDLER ================== */
async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  if (!chatId) return;
  const userName = msg.from?.first_name || "User";
  const text = String(msg.text || "").trim();

  // Commands
  if (text === "/start") {
    clearSession(chatId);
    await send(chatId, `✅ Bot sẵn sàng (${VERSION})`, { reply_markup: mainKb() });
    return;
  }
  if (text === "/help") {
    await send(chatId, helpTextWon(), { reply_markup: mainKb() });
    return;
  }
  if (text === "/edit last") {
    await startEditLast(chatId);
    return;
  }
  if (text.startsWith("/ai ")) {
    const q = text.slice(4).trim();
    const { apiKey, model } = await getGeminiConfig();
    if (!apiKey) {
      await send(chatId, "⚠️ Chưa có Gemini Key. Vào ➡️ Menu → 🔑 Nhập Gemini Key.", { reply_markup: rightKb() });
      return;
    }
    try {
      const ans = await geminiGenerate(apiKey, model, q, null);
      await send(chatId, `🤖 ${ans}`, { reply_markup: mainKb() });
    } catch (e) {
      await send(chatId, `⚠️ AI lỗi: ${e?.message || e}`, { reply_markup: mainKb() });
    }
    return;
  }

  // Menu navigation
  if (text === "⬅️ Menu") {
    clearSession(chatId);
    await send(chatId, "⬅️ MENU TRÁI – Nhập liệu", { reply_markup: leftKb() });
    return;
  }
  if (text === "➡️ Menu") {
    clearSession(chatId);
    await send(chatId, "➡️ MENU PHẢI – Báo cáo/AI/Reset", { reply_markup: rightKb() });
    return;
  }
  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, "🏠 Menu chính", { reply_markup: mainKb() });
    return;
  }

  // Right menu actions
  if (text === "1️⃣ Xem tổng doanh thu") return reportTotal(chatId);
  if (text === "2️⃣ Doanh thu tháng này") return reportThisMonth(chatId);
  if (text === "3️⃣ Doanh thu tháng trước") return reportLastMonth(chatId);
  if (text === "4️⃣ Thống kê ĐB / HQ / QR") return reportStatsGames(chatId);
  if (text === "7️⃣ Lời lỗ mua máy") return reportPhoneProfit(chatId);

  if (text === "📘 Hướng dẫn") {
    await send(chatId, helpTextWon(), { reply_markup: rightKb() });
    return;
  }

  if (text === "🔑 Nhập Gemini Key") {
    setSession(chatId, { flow: "gemini_key", step: "key" });
    await send(chatId, "🔑 Dán Gemini API Key vào đây:", { reply_markup: rightKb() });
    return;
  }

  if (text === "🤖 AI: Bật/Tắt") {
    const { enabled } = await getGeminiConfig();
    await setSetting("GEMINI_ENABLED", enabled ? "0" : "1");
    const after = enabled ? "TẮT" : "BẬT";
    await send(chatId, `🤖 AI đã ${after}.`, { reply_markup: rightKb() });
    return;
  }

  if (text === "8️⃣ Xóa sạch dữ liệu") {
    setSession(chatId, { flow: "reset", step: "pass" });
    await send(chatId, "⚠️ Nhập PASS 12345 để XÓA SẠCH:", { reply_markup: rightKb() });
    return;
  }

  // Left menu actions -> start flows
  if (text === "🎁 Hộp quà (mời)") {
    setSession(chatId, { flow: "invite", game: "hq", step: "name", data: {} });
    await send(chatId, "🎁 Hộp quà – nhập TÊN:", { reply_markup: leftKb() });
    return;
  }
  if (text === "🔳 QR (mời)") {
    setSession(chatId, { flow: "invite", game: "qr", step: "name", data: {} });
    await send(chatId, "🔳 QR – nhập TÊN:", { reply_markup: leftKb() });
    return;
  }
  if (text === "⚽ Đá bóng (thu)") {
    setSession(chatId, { flow: "revenue", game: "db", step: "amount" });
    await send(chatId, "⚽ Đá bóng – nhập SỐ TIỀN (vd 100k):", { reply_markup: leftKb() });
    return;
  }
  if (text === "🎁 Hộp quà (thu)") {
    setSession(chatId, { flow: "revenue", game: "hq", step: "amount" });
    await send(chatId, "🎁 Hộp quà – nhập SỐ TIỀN (vd 200k):", { reply_markup: leftKb() });
    return;
  }
  if (text === "🔳 QR (thu)") {
    setSession(chatId, { flow: "revenue", game: "qr", step: "amount" });
    await send(chatId, "🔳 QR – nhập SỐ TIỀN (vd 57k):", { reply_markup: leftKb() });
    return;
  }
  if (text === "➕ Thu khác") {
    setSession(chatId, { flow: "revenue", game: "other", step: "amount" });
    await send(chatId, "➕ Thu khác – nhập SỐ TIỀN (vd 0.5k):", { reply_markup: leftKb() });
    return;
  }

  // Session handling
  const sess = getSession(chatId);
  if (sess) {
    // reset
    if (sess.flow === "reset" && sess.step === "pass") {
      clearSession(chatId);
      if (text !== RESET_PASS) {
        await send(chatId, "❌ Sai PASS. Hủy xóa.", { reply_markup: rightKb() });
        return;
      }
      await send(chatId, "⏳ Đang xóa dữ liệu...", { reply_markup: rightKb() });
      await resetAllData();
      await send(chatId, "✅ Đã XÓA SẠCH dữ liệu. Bot chạy mới từ đầu.", { reply_markup: mainKb() });
      return;
    }

    // gemini key
    if (sess.flow === "gemini_key" && sess.step === "key") {
      clearSession(chatId);
      const key = text.trim();
      if (key.length < 20) {
        await send(chatId, "❌ Key có vẻ không đúng. Dán lại Gemini API Key.", { reply_markup: rightKb() });
        return;
      }
      await setSetting("GEMINI_API_KEY", key);
      const curModel = (await getSetting("GEMINI_MODEL")).trim();
      if (!curModel) await setSetting("GEMINI_MODEL", "gemini-2.0-flash");
      await send(chatId, "✅ Đã lưu Gemini Key vào SETTINGS.", { reply_markup: rightKb() });
      return;
    }

    // invite flow
    if (sess.flow === "invite") {
      if (sess.step === "name") {
        const name = text.trim();
        if (name.length < 2) {
          await send(chatId, "❌ Tên không hợp lệ. Nhập lại TÊN:", { reply_markup: leftKb() });
          return;
        }
        sess.data.name = name;
        sess.step = "email";
        setSession(chatId, sess);
        await send(chatId, "📧 Nhập EMAIL:", { reply_markup: leftKb() });
        return;
      }
      if (sess.step === "email") {
        const email = text.trim();
        if (!isEmail(email)) {
          await send(chatId, "❌ Email không hợp lệ. Nhập lại EMAIL:", { reply_markup: leftKb() });
          return;
        }
        const { due } = await addInvite({ game: sess.game, name: sess.data.name, email });
        clearSession(chatId);
        await send(
          chatId,
          `✅ Đã lưu INVITE:\n- game: ${sess.game.toUpperCase()}\n- name: ${sess.data.name}\n- email: ${email}\n- due: ${dayjs(due).format("DD/MM/YYYY")}\n\n⏰ Bot sẽ nhắc khi tới hạn.`,
          { reply_markup: leftKb() }
        );
        return;
      }
    }

    // revenue flow
    if (sess.flow === "revenue" && sess.step === "amount") {
      const amt = parseMoney(text);
      if (amt == null) {
        await send(chatId, "❌ Sai tiền. Nhập lại (vd 100k / 0.5k):", { reply_markup: leftKb() });
        return;
      }
      const type = sess.game === "other" ? "other" : "invite_reward";
      await addGameRevenue({ game: sess.game, type, amount: amt, note: "menu", chatId, userName });
      clearSession(chatId);
      await send(chatId, `✅ Đã ghi ${sess.game.toUpperCase()}: ${formatMoneyWon(amt)}`, { reply_markup: leftKb() });
      return;
    }

    // edit flow (adjust)
    if (sess.flow === "edit" && sess.step === "amount") {
      const newAmt = parseMoney(text);
      if (newAmt == null) {
        await send(chatId, "❌ Sai tiền. Nhập lại (vd 80k):", { reply_markup: mainKb() });
        return;
      }
      const { game, amount: oldAmt } = sess.data;

      // Adjustment entries (safe): remove old + add new
      await addGameRevenue({ game, type: "adjust", amount: -oldAmt, note: "EDIT_LAST_REMOVE", chatId, userName });
      await addGameRevenue({ game, type: "adjust", amount: newAmt, note: "EDIT_LAST_ADD", chatId, userName });

      clearSession(chatId);
      await send(
        chatId,
        `✅ Đã sửa: ${game.toUpperCase()} ${formatMoneyWon(oldAmt)} → ${formatMoneyWon(newAmt)}`,
        { reply_markup: mainKb() }
      );
      return;
    }
  }

  // Admin checkin reply
  if (ADMIN_TELEGRAM_ID && String(chatId) === ADMIN_TELEGRAM_ID) {
    const inv = awaitingCheckin.get(ADMIN_TELEGRAM_ID);
    const amt = parseMoney(text);
    if (inv && amt != null) {
      await addCheckinReward({
        game: inv.game,
        name: inv.name,
        email: inv.email,
        due_date: inv.due_date,
        amount: amt,
        chatId,
        userName,
      });
      await addGameRevenue({
        game: inv.game,
        type: "checkin",
        amount: amt,
        note: `${inv.name} ${inv.email}`,
        chatId,
        userName,
      });
      await markDone(inv.rowNumber, amt);
      awaitingCheckin.delete(ADMIN_TELEGRAM_ID);

      await send(chatId, `✅ Checkin: ${inv.game.toUpperCase()} ${inv.name} = ${formatMoneyWon(amt)}`, {
        reply_markup: mainKb(),
      });
      return;
    }
  }

  // Manual typing commands (fast input)
  const parts = text.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) {
    const cmd = parts[0].toLowerCase();

    // them 0.5k
    if (cmd === "them") {
      const amt = parseMoney(parts[1]);
      if (amt == null) {
        await send(chatId, "❌ Sai cú pháp. Ví dụ: them 0.5k", { reply_markup: mainKb() });
        return;
      }
      await addGameRevenue({ game: "other", type: "other", amount: amt, note: "them", chatId, userName });
      await send(chatId, `✅ Đã cộng thu khác: ${formatMoneyWon(amt)}`, { reply_markup: mainKb() });
      return;
    }

    const game = shortGameCode(cmd);

    // invite: hopqua Ten mail@gmail.com (hq/qr)
    if ((game === "hq" || game === "qr") && parts.length >= 3 && isEmail(parts[2]) && parseMoney(parts[1]) == null) {
      const { due } = await addInvite({ game, name: parts[1], email: parts[2] });
      await send(
        chatId,
        `✅ Đã lưu INVITE:\n- game: ${game.toUpperCase()}\n- name: ${parts[1]}\n- email: ${parts[2]}\n- due: ${dayjs(due).format("DD/MM/YYYY")}`,
        { reply_markup: mainKb() }
      );
      return;
    }

    // revenue: db/hq/qr 100k
    if (game) {
      const amt = parseMoney(parts[1]);
      if (amt == null) {
        await send(chatId, "❌ Sai tiền. Ví dụ: dabong 100k", { reply_markup: mainKb() });
        return;
      }
      const type = game === "other" ? "other" : "invite_reward";
      await addGameRevenue({ game, type, amount: amt, note: cmd, chatId, userName });
      await send(chatId, `✅ Đã ghi ${game.toUpperCase()}: ${formatMoneyWon(amt)}`, { reply_markup: mainKb() });
      return;
    }
  }

  // AI fallback parse (if enabled)
  const aiHandled = await tryGeminiParse(chatId, userName, text);
  if (aiHandled) return;

  // fallback unknown
  await send(chatId, "❓ Không hiểu. Vào ➡️ Menu → 📘 Hướng dẫn (hoặc bật AI).", { reply_markup: mainKb() });
}

/* ================== WEBHOOK ================== */
app.post("/webhook", async (req, res) => {
  res.sendStatus(200);
  try {
    const body = req.body;
    if (body?.message) await handleTextMessage(body.message);
  } catch (e) {
    console.error("WEBHOOK ERROR:", e?.message || e);
  }
});

/* ================== START ================== */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ TIKTOK_LITE_BOT READY on", PORT, "|", VERSION));
