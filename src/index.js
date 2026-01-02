// src/index.js
/**
 * =================================================================================================
 *  TIKTOK_LITE_BOT – FINAL CUTE EDITION ✅ (Webhook + Sheets + Smart Parse + Machine/Wallet + Gemini)
 * =================================================================================================
 *  ✅ Render stable: binds 0.0.0.0 + /ping endpoint
 *  ✅ Menus: Main (⬅️/➡️), Left (input), Right (reports/help/ai/reset + machine/wallet)
 *  ✅ Currency display: WON (₩) everywhere
 *  ✅ Reset: hỏi pass nhưng KHÔNG lộ pass trong chat
 *  ✅ Cute replies: ngộ nghĩnh đáng yêu hài hước 😝
 *  ✅ Smart Parse FREE (no key) + Gemini fallback optional
 *
 *  REQUIRED ENV:
 *   - BOT_TOKEN
 *   - GOOGLE_SHEET_ID
 *   - GOOGLE_APPLICATION_CREDENTIALS (path to SA json)
 *
 *  OPTIONAL ENV:
 *   - ADMIN_TELEGRAM_ID
 *
 *  NOTE: Telegram limitation: cannot auto pre-fill input box via keyboard button.
 *        We do guided flows (bot hỏi từng bước) + lệnh nhập tay vẫn giữ nguyên.
 */

import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * SECTION 1 — Env & constants
 * ========================= */
const VERSION = "FINAL-CUTE-SMARTPARSE-MACHINE-WALLET-GEMINI-WON";
const BOT_TOKEN = process.env.BOT_TOKEN;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "/etc/secrets/google-service-account.json";
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID ? String(process.env.ADMIN_TELEGRAM_ID).trim() : "";

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");

const TELEGRAM_API = `https://api.telegram.org/bot${BOT_TOKEN}`;
const RESET_PASS = "12345";

/* =========================
 * SECTION 2 — Money display WON
 * ========================= */
function moneyWON(n) {
  return "₩" + Number(n || 0).toLocaleString("ko-KR");
}

/* =========================
 * SECTION 3 — Express server
 * ========================= */
const app = express();
app.use(express.json());

app.get("/", (_, res) => res.status(200).send(`OK ${VERSION}`));
app.get("/ping", (_, res) => res.status(200).json({ ok: true, version: VERSION }));

/* =========================
 * SECTION 4 — Telegram helpers + Cute layer
 * ========================= */
async function tg(method, payload) {
  const resp = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const j = await resp.json().catch(() => ({}));
  return j;
}

function cuteify(text) {
  const s0 = String(text ?? "");
  const isLong = s0.length > 600;

  let s = s0
    .replaceAll("✅ Đã", "✅ Xong rồi nè")
    .replaceAll("✅ Bot sẵn sàng", "✅ Bot lên đồ xong rồi nè")
    .replaceAll("❌ Sai", "❌ Ôi hông đúng rồi bạn iu")
    .replaceAll("❌ Không tìm thấy", "🥺 Mình tìm hoài mà hổng thấy á")
    .replaceAll("⚠️", "⚠️ Ui ui")
    .replaceAll("Không hiểu", "Nhập sai rồi bạn iu ơi ^^")
    .replaceAll("Nhập lại", "Bạn nhập lại giúp mình nha~")
    .replaceAll("Nhập ", "Bạn nhập ");

  const tailsShort = [" 😚", " 🫶", " ✨", " (iu iu)", " ^^", " 🥳", " 😝", " 🤭", " 💖"];
  const tailsLong = ["\n\n(Thiếu gì cứ gọi mình nha 😚)", "\n\n(Mình ở đây nè 🫶)", "\n\n(Okelaaa ✨)"];

  const endsWithEmojiLike = /[\u{1F300}-\u{1FAFF}\u2600-\u27BF]$/u.test(s.trim());
  const endsWithCaret = /\^+$/.test(s.trim());

  if (!endsWithEmojiLike && !endsWithCaret) {
    const idx = (s.length + (isLong ? 7 : 3)) % (isLong ? tailsLong.length : tailsShort.length);
    s = s + (isLong ? tailsLong[idx] : tailsShort[idx]);
  }
  return s;
}

async function send(chatId, text, extra = {}) {
  if (!chatId) return;
  const raw = extra?.__raw === true;
  const { __raw, ...rest } = extra;
  await tg("sendMessage", {
    chat_id: chatId,
    text: raw ? String(text ?? "") : cuteify(text),
    ...rest,
  });
}

/* =========================
 * SECTION 5 — Reply keyboards
 * ========================= */
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
    [{ text: "🎁 Mời Hộp Quà" }, { text: "🔳 Mời QR" }],
    [{ text: "⚽ Thu Đá Bóng" }, { text: "🎁 Thu Hộp Quà" }],
    [{ text: "🔳 Thu QR" }, { text: "➕ Thu Khác" }],
    [{ text: "📱 Mua Máy" }, { text: "✅ KQ Máy (Lời/Huề/Tạch)" }],
    [{ text: "⬅️ Back" }],
  ]);
}

function rightKb() {
  return kb([
    [{ text: "💰 1) Tổng Doanh Thu" }],
    [{ text: "📅 2) Tháng Này" }, { text: "⏮️ 3) Tháng Trước" }],
    [{ text: "📊 4) Thống Kê Game" }],
    [{ text: "📱 7) Lời/Lỗ Máy" }],
    [{ text: "💼 Xem Ví" }, { text: "🧾 Chỉnh Số Dư Ví" }],
    [{ text: "📘 Hướng Dẫn Lệnh" }],
    [{ text: "🔑 Nhập Gemini Key" }, { text: "🤖 AI: Bật/Tắt" }],
    [{ text: "🧠 Smart Parse: Bật/Tắt" }],
    [{ text: "🧨 Xóa Sạch Dữ Liệu" }],
    [{ text: "⬅️ Back" }],
  ]);
}

/* =========================
 * SECTION 6 — Google Sheets setup
 * ========================= */
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

/* =========================
 * SECTION 7 — Common utils
 * ========================= */
function nowIso() {
  return new Date().toISOString();
}

function isEmail(x) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(x || "").trim());
}

function parseMoney(input) {
  if (input == null) return null;
  let s = String(input).trim().toLowerCase();
  s = s.replace(/₩/g, "");
  s = s.replace(/\bwon\b/g, "");
  s = s.replace(/\s+/g, " ").trim();
  s = s.replace(/(\d)\s+k\b/g, "$1k");
  s = s.replace(/,/g, "");

  const m = s.match(/^(\d+(?:\.\d+)?)(k)?$/);
  if (!m) return null;
  const num = Number(m[1]);
  if (!Number.isFinite(num)) return null;
  const isK = !!m[2];
  return Math.round(isK ? num * 1000 : num);
}

function removeDiacritics(str) {
  if (!str) return "";
  return String(str)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D");
}

function normalizeForParse(text) {
  const raw = String(text || "");
  const emails = [];
  let tmp = raw.replace(/[^\s@]+@[^\s@]+\.[^\s@]+/g, (m) => {
    emails.push(m);
    return `__EMAIL_${emails.length - 1}__`;
  });

  tmp = removeDiacritics(tmp).toLowerCase();
  tmp = tmp.replace(/[，]/g, ",");
  tmp = tmp.replace(/\s+/g, " ").trim();
  tmp = tmp.replace(/__email_(\d+)__/g, (_, i) => emails[Number(i)] || "");
  return tmp;
}

function extractEmail(text) {
  const m = String(text || "").match(/[^\s@]+@[^\s@]+\.[^\s@]+/);
  return m ? m[0] : "";
}

function extractMoneyFromText(text) {
  const t = String(text || "");
  const patterns = [
    /₩\s*\d[\d,]*(?:\.\d+)?\s*k?/i,
    /\d[\d,]*(?:\.\d+)?\s*k\b/i,
    /\d[\d,]*(?:\.\d+)?\b/i,
  ];
  for (const p of patterns) {
    const m = t.match(p);
    if (m) {
      const amt = parseMoney(m[0].replace(/\s+/g, ""));
      if (amt != null) return amt;
    }
  }
  return null;
}

function detectGameFromText(normText) {
  const t = ` ${normText} `;
  const otherKeys = [" them ", " thu them ", " thu khac ", " ngoai game ", " ads ", " like ", " ngoai "];
  if (otherKeys.some((k) => t.includes(k))) return "other";
  const dbKeys = [" dabong ", " da bong ", " db ", " bong "];
  if (dbKeys.some((k) => t.includes(k))) return "db";
  const hqKeys = [" hopqua ", " hop qua ", " hq ", " hop "];
  if (hqKeys.some((k) => t.includes(k))) return "hq";
  const qrKeys = [" qr ", " qrcode ", " ma qr "];
  if (qrKeys.some((k) => t.includes(k))) return "qr";
  return "";
}

/* =========================
 * SECTION 8 — SETTINGS / Toggles
 * ========================= */
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

async function isSmartParseEnabled() {
  const v = (await getSetting("SMART_PARSE_ENABLED")).trim();
  if (v === "") return true;
  return v === "1";
}

/* =========================
 * SECTION 9 — Game revenue layer
 * ========================= */
async function addGameRevenue({ game, type, amount, note, chatId, userName }) {
  await appendValues("GAME_REVENUE!A1", [[nowIso(), game, type, amount, note || "", String(chatId || ""), userName || ""]]);
  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "revenue_add", game, type, amount, note || "", String(chatId || ""), userName || ""]]);
  } catch (_) {}
}

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

/* =========================
 * SECTION 10 — Invites + checkin
 * ========================= */
async function addInvite({ game, name, email }) {
  const invitedAt = dayjs();
  const due = invitedAt.add(14, "day");
  await appendValues("INVITES!A1", [[nowIso(), game, name, email, invitedAt.toISOString(), due.toISOString(), "pending", 0, "", "", "", ""]]);
  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "invite_add", game, name, email, due.toISOString()]]);
  } catch (_) {}
  return { invitedAt, due };
}

async function listInvites() {
  const rows = await getValues("INVITES!A2:L");
  return rows.map((r, i) => ({
    rowNumber: i + 2,
    game: (r[1] || "").toLowerCase(),
    name: r[2] || "",
    email: r[3] || "",
    invited_at: r[4] || "",
    due_date: r[5] || "",
    status: (r[6] || "").toLowerCase(),
    asked: String(r[7] || "0"),
  }));
}

async function markAsked(rowNumber) {
  await updateValues(`INVITES!H${rowNumber}:I${rowNumber}`, [[1, nowIso()]]);
}

async function markDone(rowNumber, rewardAmount) {
  await updateValues(`INVITES!G${rowNumber}:K${rowNumber}`, [["done", 1, nowIso(), rewardAmount, nowIso()]]);
}

async function addCheckinReward({ game, name, email, due_date, amount, chatId, userName }) {
  await appendValues("CHECKIN_REWARD!A1", [[nowIso(), game, name, email, due_date || "", amount, String(chatId || ""), userName || ""]]);
}

/* =========================
 * SECTION 11 — Wallet + Machine
 * ========================= */
async function listWallets() {
  const rows = await getValues("WALLETS!A2:B");
  const wallets = [];
  for (const r of rows) {
    const code = String(r[0] || "").trim().toLowerCase();
    const name = String(r[1] || "").trim();
    if (!code) continue;
    wallets.push({ code, name: name || code.toUpperCase() });
  }
  if (wallets.length === 0) {
    return [
      { code: "uri", name: "URI" },
      { code: "hana", name: "HANA" },
      { code: "kt", name: "KT" },
    ];
  }
  return wallets;
}

async function readWalletLog() {
  const rows = await getValues("WALLET_LOG!A2:H");
  return rows.map((r) => ({
    wallet: String(r[1] || "").trim().toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    ref_type: String(r[4] || "").trim().toLowerCase(),
  }));
}

async function addWalletLog({ wallet, type, amount, ref_type, ref_id, note, chatId }) {
  await appendValues("WALLET_LOG!A1", [[nowIso(), wallet, type, amount, ref_type || "", ref_id || "", note || "", String(chatId || "")]]);
  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "wallet_log_add", wallet, type, amount]]);
  } catch (_) {}
}

async function walletBalances() {
  const wallets = await listWallets();
  const logs = await readWalletLog();
  const map = new Map();
  for (const w of wallets) map.set(w.code, 0);
  for (const l of logs) {
    if (!l.wallet) continue;
    const cur = map.get(l.wallet) ?? 0;
    map.set(l.wallet, cur + l.amount);
  }
  return wallets.map((w) => ({ code: w.code, name: w.name, balance: map.get(w.code) ?? 0 }));
}

async function nextPhoneId() {
  const rows = await getValues("PHONES!A2:A");
  let max = 0;
  for (const r of rows) {
    const id = String(r[0] || "").trim();
    const m = id.match(/^P(\d+)$/i);
    if (m) {
      const n = Number(m[1]);
      if (Number.isFinite(n) && n > max) max = n;
    }
  }
  return "P" + String(max + 1).padStart(4, "0");
}

async function addPhone({ buy_price, wallet, note }) {
  const phone_id = await nextPhoneId();
  await appendValues("PHONES!A1", [[phone_id, nowIso(), buy_price, wallet, "new", note || ""]]);

  await addWalletLog({
    wallet,
    type: "machine_buy",
    amount: -Math.abs(buy_price),
    ref_type: "phone",
    ref_id: phone_id,
    note: note || "",
    chatId: "",
  });

  return { phone_id, status: "new" };
}

async function findPhoneRow(phone_id) {
  const rows = await getValues("PHONES!A2:F");
  for (let i = 0; i < rows.length; i++) {
    const id = String(rows[i][0] || "").trim().toUpperCase();
    if (id === String(phone_id).trim().toUpperCase()) {
      return { rowNumber: i + 2, row: rows[i] };
    }
  }
  return null;
}

async function updatePhoneStatus(phone_id, status) {
  const found = await findPhoneRow(phone_id);
  if (!found) return false;
  await updateValues(`PHONES!E${found.rowNumber}:E${found.rowNumber}`, [[status]]);
  return true;
}

async function addPhoneProfitLog({ phone_id, result, amount, note, wallet, chatId }) {
  await appendValues("PHONE_PROFIT_LOG!A1", [[nowIso(), phone_id, result, amount, note || "", wallet || "", String(chatId || "")]]);
}

async function readPhones() {
  const rows = await getValues("PHONES!A2:F");
  return rows
    .filter((r) => r.some((c) => String(c || "").trim() !== ""))
    .map((r) => ({
      phone_id: String(r[0] || "").trim().toUpperCase(),
      buy_price: Number(String(r[2] || "0").replace(/,/g, "")) || 0,
      wallet: String(r[3] || "").trim().toLowerCase(),
      status: String(r[4] || "").trim().toLowerCase(),
    }));
}

async function readPhoneProfitLogs() {
  const rows = await getValues("PHONE_PROFIT_LOG!A2:G");
  return rows.map((r) => ({
    phone_id: String(r[1] || "").trim().toUpperCase(),
    result: String(r[2] || "").trim().toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    wallet: String(r[5] || "").trim().toLowerCase(),
  }));
}

async function recordMachineResult({ phone_id, result, amountAbs, note, chatId }) {
  const phone = await findPhoneRow(phone_id);
  if (!phone) return { ok: false, error: "Máy này hổng có trong danh sách á 😭 (Kiểm tra lại mã máy nha)" };

  const wallet = String(phone.row[3] || "").trim().toLowerCase() || "unknown";
  let signed = 0;
  let status = "ok";

  if (result === "loi") {
    signed = Math.abs(amountAbs);
    status = "ok";
  } else if (result === "hue") {
    signed = 0;
    status = "ok";
  } else if (result === "tach") {
    signed = -Math.abs(amountAbs);
    status = "tach";
  } else {
    return { ok: false, error: "KQ máy không hợp lệ (loi/hue/tach) nha bạn iu 😵‍💫" };
  }

  await addPhoneProfitLog({ phone_id, result, amount: signed, note: note || "", wallet, chatId });

  const ledgerType = result === "loi" ? "machine_profit" : result === "tach" ? "machine_loss" : "machine_break_even";
  await addWalletLog({ wallet, type: ledgerType, amount: signed, ref_type: "phone", ref_id: phone_id, note: note || "", chatId });

  await updatePhoneStatus(phone_id, status);
  return { ok: true, wallet, signed, status };
}

/* =========================
 * SECTION 12 — Reports
 * ========================= */
function monthKey(ts) {
  if (!ts) return "";
  return String(ts).slice(0, 7);
}

async function reportTotalRevenue(chatId) {
  const rows = await readGameRevenue();
  const sum = rows.reduce((a, b) => a + b.amount, 0);
  await send(chatId, `💰 TỔNG DOANH THU (WON)\n= ${moneyWON(sum)}`, { reply_markup: rightKb() });
}

async function reportRevenueMonth(chatId, mKey) {
  const rows = await readGameRevenue();
  const sum = rows.filter((x) => monthKey(x.ts) === mKey).reduce((a, b) => a + b.amount, 0);
  await send(chatId, `📅 DOANH THU THÁNG ${mKey}\n= ${moneyWON(sum)}`, { reply_markup: rightKb() });
}

async function reportThisMonth(chatId) {
  const m = dayjs().format("YYYY-MM");
  await reportRevenueMonth(chatId, m);
}

/** ✅ FIX DỨT ĐIỂM: bỏ label rác, chỉ còn code chuẩn */
async function reportLastMonth(chatId) {
  const m = dayjs().subtract(1, "month").format("YYYY-MM");
  await reportRevenueMonth(chatId, m);
}

async function reportStatsGames(chatId) {
  const rev = await readGameRevenue();
  const inv = await listInvites();

  const dbCount = rev.filter((x) => x.game === "db" && x.type === "invite_reward").length;
  const hqCount = inv.filter((x) => x.game === "hq").length;
  const qrCount = inv.filter((x) => x.game === "qr").length;

  const dbSum = rev.filter((x) => x.game === "db").reduce((a, b) => a + b.amount, 0);
  const hqSum = rev.filter((x) => x.game === "hq").reduce((a, b) => a + b.amount, 0);
  const qrSum = rev.filter((x) => x.game === "qr").reduce((a, b) => a + b.amount, 0);

  const out =
    `📊 THỐNG KÊ GAME (WON)\n\n` +
    `⚽ Đá bóng: người = ${dbCount} | doanh thu = ${moneyWON(dbSum)}\n` +
    `🎁 Hộp quà: người = ${hqCount} | doanh thu = ${moneyWON(hqSum)}\n` +
    `🔳 QR: người = ${qrCount} | doanh thu = ${moneyWON(qrSum)}\n`;

  await send(chatId, out, { reply_markup: rightKb() });
}

async function reportWallets(chatId) {
  const balances = await walletBalances();
  let total = 0;
  const lines = balances.map((b) => (total += b.balance, `• ${b.name} (${b.code}): ${moneyWON(b.balance)}`));
  const out = `💼 SỐ DƯ CÁC VÍ\n\n${lines.join("\n")}\n\nTổng: ${moneyWON(total)}`;
  await send(chatId, out, { reply_markup: rightKb() });
}

async function reportMachineProfit(chatId) {
  const phones = await readPhones();
  const logs = await readPhoneProfitLogs();
  const walletLog = await readWalletLog();

  const totalPhones = phones.length;
  const okCount = phones.filter((p) => p.status === "ok").length;
  const tachCount = phones.filter((p) => p.status === "tach").length;
  const newCount = phones.filter((p) => p.status === "new").length;

  let loi = 0,
    hue = 0,
    tach = 0,
    sumProfit = 0;
  for (const l of logs) {
    sumProfit += l.amount;
    if (l.amount > 0) loi++;
    else if (l.amount === 0) hue++;
    else tach++;
  }

  const totalBuy = phones.reduce((a, b) => a + (b.buy_price || 0), 0);
  const net = sumProfit - totalBuy;

  const perWallet = new Map();
  for (const w of await listWallets()) perWallet.set(w.code, 0);

  for (const wl of walletLog) {
    if (wl.ref_type !== "phone") continue;
    if (!wl.wallet) continue;
    const cur = perWallet.get(wl.wallet) ?? 0;
    perWallet.set(wl.wallet, cur + wl.amount);
  }

  const perWalletLines = [...perWallet.entries()].map(([code, amt]) => `• ${code}: ${moneyWON(amt)}`);

  const out =
    `📱 LỜI/LỖ MUA MÁY (WON)\n\n` +
    `📦 Số máy: ${totalPhones}\n` +
    `• New: ${newCount}\n` +
    `• OK: ${okCount}\n` +
    `• Tạch: ${tachCount}\n\n` +
    `🧾 Log kết quả: ${logs.length}\n` +
    `• Lời: ${loi}\n` +
    `• Huề: ${hue}\n` +
    `• Tạch: ${tach}\n\n` +
    `💸 Tổng tiền mua: ${moneyWON(totalBuy)}\n` +
    `💰 Tổng lời/lỗ log: ${moneyWON(sumProfit)}\n` +
    `🧮 Net (log - mua): ${moneyWON(net)}\n\n` +
    `🏦 Theo ví (ref=phone):\n${perWalletLines.join("\n")}`;

  await send(chatId, out, { reply_markup: rightKb() });
}

/* =========================
 * SECTION 13 — Help
 * ========================= */
function helpText() {
  return (
    `📘 HƯỚNG DẪN LỆNH (WON ₩)\n\n` +
    `✅ Doanh thu game:\n` +
    `- dabong 100k   | db 100k\n` +
    `- hopqua 200k   | hq 200k\n` +
    `- qr 57k\n` +
    `- them 0.5k (thu khác)\n\n` +
    `✅ Invite 14 ngày:\n` +
    `- hopqua Ten email@gmail.com\n` +
    `- qr Ten email@gmail.com\n\n` +
    `✅ Máy + Ví:\n` +
    `- muamay 1200k hana [note]\n` +
    `- mayloi P0001 300k [note]\n` +
    `- mayhue P0001 [note]\n` +
    `- maytach P0001 800k [note]\n\n` +
    `✅ Ví:\n` +
    `- vi           (xem tất cả)\n` +
    `- vi hana      (xem riêng)\n` +
    `- sodu hana 5000k  (chỉnh ví về số dư mục tiêu)\n\n` +
    `🧠 Smart Parse (miễn phí):\n` +
    `- hiểu nhiều kiểu gõ “lỏng” mà không cần key\n`
  );
}

/* =========================
 * SECTION 14 — Reset data
 * ========================= */
const RESET_CLEAR_RANGES = [
  "WALLETS!A2:Z",
  "WALLET_LOG!A2:Z",
  "PHONES!A2:Z",
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

/* =========================
 * SECTION 15 — Sessions
 * ========================= */
const sessions = new Map();
function setSession(chatId, sess) {
  sessions.set(String(chatId), sess);
}
function getSession(chatId) {
  return sessions.get(String(chatId));
}
function clearSession(chatId) {
  sessions.delete(String(chatId));
}

/* =========================
 * SECTION 16 — Smart Parse (FREE)
 * ========================= */
function detectWalletFromText(normText) {
  const t = ` ${normText} `;
  for (const c of [" hana ", " uri ", " kt "]) if (t.includes(c)) return c.trim();
  return "";
}
function detectPhoneIdFromText(text) {
  const m = String(text || "").toUpperCase().match(/\bP\d{1,6}\b/);
  return m ? m[0] : "";
}
function detectMachineResultFromText(normText) {
  const t = ` ${normText} `;
  if (t.includes(" mayloi ") || t.includes(" loi ") || t.includes(" lai ")) return "loi";
  if (t.includes(" mayhue ") || t.includes(" hue ") || t.includes(" hoa ")) return "hue";
  if (t.includes(" maytach ") || t.includes(" tach ") || t.includes(" chet ") || t.includes(" tac ")) return "tach";
  return "";
}
function looksLikeBuyMachine(normText) {
  const t = ` ${normText} `;
  return t.includes(" muamay ") || t.includes(" mua may ") || (t.includes(" mua ") && t.includes(" may "));
}
function looksLikeWalletAdjust(normText) {
  const t = ` ${normText} `;
  return t.includes(" sodu ") || t.includes(" so du ") || t.includes(" chinh so du ");
}
function looksLikeWalletView(normText) {
  const t = ` ${normText} `;
  return t.startsWith("vi ") || t === "vi" || t.includes(" xem vi ");
}
function smartParse(text) {
  const norm = normalizeForParse(text);
  const email = extractEmail(text);
  const phone_id = detectPhoneIdFromText(text);
  const amount = extractMoneyFromText(text);
  const game = detectGameFromText(norm);
  const wallet = detectWalletFromText(norm);
  const machineResult = detectMachineResultFromText(norm);

  if (looksLikeWalletView(norm)) return { action: "wallet_view", wallet: wallet || "" };

  if (looksLikeWalletAdjust(norm)) {
    if (wallet && amount != null) return { action: "wallet_adjust", wallet, target: amount };
    return { action: "wallet_adjust_incomplete", wallet: wallet || "", target: amount };
  }

  if (looksLikeBuyMachine(norm)) {
    if (amount != null && wallet) return { action: "machine_buy", price: amount, wallet, note: "" };
    return { action: "machine_buy_incomplete", price: amount, wallet: wallet || "" };
  }

  if (machineResult) {
    if (phone_id && machineResult === "hue") return { action: "machine_result", phone_id, result: "hue", amount: 0, note: "" };
    if (phone_id && amount != null) return { action: "machine_result", phone_id, result: machineResult, amount, note: "" };
    return { action: "machine_result_incomplete", phone_id: phone_id || "", result: machineResult, amount };
  }

  if (email && (game === "hq" || game === "qr")) {
    let nameGuess = String(text || "").replace(email, " ");
    const rawTokens = String(nameGuess).trim().split(/\s+/).filter(Boolean);
    const filtered = rawTokens.filter((tok) => {
      const t = normalizeForParse(tok);
      const bad = ["hopqua", "hop", "qua", "hq", "qr", "moi", "invite", "moi:", "moi-"];
      return !bad.includes(t);
    });
    const name = filtered.join(" ").trim() || "NoName";
    return { action: "invite", game, name, email };
  }

  if (game && amount != null) return { action: "revenue", game, amount };
  if (!game && amount != null && (norm.includes(" thu ") || norm.includes(" them ") || norm.includes(" ngoai "))) {
    return { action: "revenue", game: "other", amount };
  }

  return { action: "unknown" };
}

/* =========================
 * SECTION 17 — Strict command parsing
 * ========================= */
function parseStrictCommand(text) {
  const raw = String(text || "").trim();
  const parts = raw.split(/\s+/).filter(Boolean);
  const cmd = (parts[0] || "").toLowerCase();
  const gameMap = { dabong: "db", db: "db", hopqua: "hq", hq: "hq", qr: "qr" };

  if (cmd === "them") {
    const amt = parseMoney(parts[1]);
    if (amt == null) return null;
    return { action: "revenue", game: "other", amount: amt, note: "them" };
  }

  if (cmd === "vi") {
    const w = parts[1] ? String(parts[1]).trim().toLowerCase() : "";
    return { action: "wallet_view", wallet: w };
  }

  if (cmd === "sodu") {
    const w = parts[1] ? String(parts[1]).trim().toLowerCase() : "";
    const amt = parseMoney(parts[2]);
    if (!w || amt == null) return null;
    return { action: "wallet_adjust", wallet: w, target: amt };
  }

  if (cmd === "muamay") {
    const price = parseMoney(parts[1]);
    const wallet = parts[2] ? String(parts[2]).trim().toLowerCase() : "";
    const note = parts.slice(3).join(" ");
    if (price == null || !wallet) return null;
    return { action: "machine_buy", price, wallet, note };
  }

  if (cmd === "mayloi" || cmd === "maytach" || cmd === "mayhue") {
    const phone_id = parts[1] ? String(parts[1]).trim().toUpperCase() : "";
    const note = cmd === "mayhue" ? parts.slice(2).join(" ") : parts.slice(3).join(" ");
    if (!phone_id) return null;
    if (cmd === "mayhue") return { action: "machine_result", phone_id, result: "hue", amount: 0, note };
    const amt = parseMoney(parts[2]);
    if (amt == null) return null;
    if (cmd === "mayloi") return { action: "machine_result", phone_id, result: "loi", amount: amt, note };
    if (cmd === "maytach") return { action: "machine_result", phone_id, result: "tach", amount: amt, note };
  }

  if (gameMap[cmd]) {
    const game = gameMap[cmd];
    if ((game === "hq" || game === "qr") && parts.length >= 3 && isEmail(parts[2]) && parseMoney(parts[1]) == null) {
      return { action: "invite", game, name: parts[1], email: parts[2] };
    }
    const amt = parseMoney(parts[1]);
    if (amt != null) return { action: "revenue", game, amount: amt, note: cmd };
  }

  return null;
}

/* =========================
 * SECTION 18 — Execute actions
 * ========================= */
async function executeAction(chatId, userName, a) {
  if (a.action === "revenue") {
    const game = a.game;
    const amount = a.amount;
    const type = game === "other" ? "other" : "invite_reward";
    await addGameRevenue({ game, type, amount, note: a.note || "input", chatId, userName });
    await send(chatId, `✅ Đã ghi doanh thu ${game.toUpperCase()}: ${moneyWON(amount)}`, { reply_markup: mainKb() });
    return true;
  }

  if (a.action === "invite") {
    const { due } = await addInvite({ game: a.game, name: a.name, email: a.email });
    await send(
      chatId,
      `✅ Đã lưu INVITE ${a.game.toUpperCase()}:\n- ${a.name}\n- ${a.email}\n- Due: ${dayjs(due).format("DD/MM/YYYY")}\n\n⏰ Tới hạn mình nhắc liền!`,
      { reply_markup: mainKb() }
    );
    return true;
  }

  if (a.action === "wallet_view") {
    const balances = await walletBalances();
    if (!a.wallet) {
      let total = 0;
      const lines = balances.map((b) => (total += b.balance, `• ${b.name} (${b.code}): ${moneyWON(b.balance)}`));
      await send(chatId, `💼 SỐ DƯ CÁC VÍ\n\n${lines.join("\n")}\n\nTổng: ${moneyWON(total)}`, { reply_markup: rightKb() });
      return true;
    }
    const w = String(a.wallet).trim().toLowerCase();
    const found = balances.find((b) => b.code === w);
    if (!found) {
      await send(chatId, `🥺 Mình không thấy ví '${w}' á.`, { reply_markup: rightKb() });
      return true;
    }
    await send(chatId, `💼 VÍ ${found.name} (${found.code})\n= ${moneyWON(found.balance)}`, { reply_markup: rightKb() });
    return true;
  }

  if (a.action === "wallet_adjust") {
    const wallet = String(a.wallet || "").trim().toLowerCase();
    const target = Number(a.target);
    if (!wallet || !Number.isFinite(target)) {
      await send(chatId, "Sai cú pháp chỉnh ví. Ví dụ: sodu hana 5000k", { reply_markup: rightKb() });
      return true;
    }
    const balances = await walletBalances();
    const cur = balances.find((b) => b.code === wallet)?.balance ?? 0;
    const delta = Math.round(target) - cur;
    await addWalletLog({ wallet, type: "adjust", amount: delta, ref_type: "wallet", ref_id: wallet, note: "ADJUST_TO_TARGET", chatId });
    await send(chatId, `✅ Đã chỉnh ví ${wallet}: ${moneyWON(cur)} → ${moneyWON(Math.round(target))}`, { reply_markup: rightKb() });
    return true;
  }

  if (a.action === "machine_buy") {
    const price = Number(a.price);
    const wallet = String(a.wallet || "").trim().toLowerCase();
    const note = a.note || "";
    if (!Number.isFinite(price) || price <= 0 || !wallet) {
      await send(chatId, "Sai cú pháp mua máy. Ví dụ: muamay 1200k hana", { reply_markup: leftKb() });
      return true;
    }
    const r = await addPhone({ buy_price: Math.round(price), wallet, note });
    await send(chatId, `✅ Đã mua máy: ${r.phone_id}\nGiá: ${moneyWON(Math.round(price))}\nVí: ${wallet}\nTrạng thái: ${r.status}`, { reply_markup: leftKb() });
    return true;
  }

  if (a.action === "machine_result") {
    const phone_id = String(a.phone_id || "").trim().toUpperCase();
    const result = String(a.result || "").trim().toLowerCase();
    const amount = Number(a.amount);
    const note = a.note || "";

    if (!phone_id || !["loi", "hue", "tach"].includes(result)) {
      await send(chatId, "Sai cú pháp máy. Ví dụ: mayloi P0001 300k", { reply_markup: leftKb() });
      return true;
    }
    const amountAbs = result === "hue" ? 0 : Math.round(Math.abs(amount));
    if (result !== "hue" && (!Number.isFinite(amountAbs) || amountAbs <= 0)) {
      await send(chatId, "Sai số tiền. Ví dụ: maytach P0001 800k", { reply_markup: leftKb() });
      return true;
    }

    const rr = await recordMachineResult({ phone_id, result, amountAbs, note, chatId });
    if (!rr.ok) {
      await send(chatId, rr.error, { reply_markup: leftKb() });
      return true;
    }
    const shown = result === "hue" ? "₩0" : moneyWON(Math.abs(rr.signed));
    await send(chatId, `✅ Đã ghi kết quả máy ${phone_id}\nKQ: ${result}\nTiền: ${shown}\nVí: ${rr.wallet}\nStatus: ${rr.status}`, { reply_markup: leftKb() });
    return true;
  }

  return false;
}

/* =========================
 * SECTION 19 — RESET flow + guided flows
 * ========================= */
async function handleSessionInput(chatId, userName, text) {
  const sess = getSession(chatId);
  if (!sess) return false;

  if (sess.flow === "reset" && sess.step === "pass") {
    clearSession(chatId);
    if (text !== RESET_PASS) {
      await send(chatId, "Nhập sai rồi bạn iu ơi ^^  (Nếu cần thì hỏi admin xin pass nha 😝)", { reply_markup: rightKb() });
      return true;
    }
    await send(chatId, "⏳ Đang xóa sạch dữ liệu... (rụng tim nhẹ 😵‍💫)", { reply_markup: rightKb() });
    await resetAllData();
    await send(chatId, "🎉 Done! Dữ liệu đã sạch bong kin kít. Chạy lại từ đầu thôi nè~", { reply_markup: mainKb() });
    return true;
  }

  if (sess.flow === "invite") {
    if (sess.step === "name") {
      const name = String(text || "").trim();
      if (name.length < 2) {
        await send(chatId, "Tên này hơi ngắn á 😝 Bạn nhập lại TÊN nha~", { reply_markup: leftKb() });
        return true;
      }
      sess.data = sess.data || {};
      sess.data.name = name;
      sess.step = "email";
      setSession(chatId, sess);
      await send(chatId, "Giờ bạn nhập EMAIL nha~", { reply_markup: leftKb() });
      return true;
    }
    if (sess.step === "email") {
      const email = String(text || "").trim();
      if (!isEmail(email)) {
        await send(chatId, "Email này chưa chuẩn á 🥺 Bạn nhập lại EMAIL giúp mình nha~", { reply_markup: leftKb() });
        return true;
      }
      const { due } = await addInvite({ game: sess.game, name: sess.data.name, email });
      clearSession(chatId);
      await send(chatId, `✅ INVITE okela!\nGame: ${sess.game.toUpperCase()}\nTên: ${sess.data.name}\nMail: ${email}\nDue: ${dayjs(due).format("DD/MM/YYYY")}`, {
        reply_markup: leftKb(),
      });
      return true;
    }
  }

  if (sess.flow === "revenue" && sess.step === "amount") {
    const amt = parseMoney(text);
    if (amt == null) {
      await send(chatId, "Sai tiền rồi bạn iu ơi ^^ Ví dụ: 100k / 0.5k / 100000", { reply_markup: leftKb() });
      return true;
    }
    await executeAction(chatId, userName, { action: "revenue", game: sess.game, amount: amt, note: "menu" });
    clearSession(chatId);
    return true;
  }

  if (sess.flow === "wallet_adjust") {
    if (sess.step === "wallet") {
      const w = String(text || "").trim().toLowerCase();
      if (!w) {
        await send(chatId, "Bạn nhập mã ví giúp mình nha~ (hana / uri / kt)", { reply_markup: rightKb() });
        return true;
      }
      sess.data = { wallet: w };
      sess.step = "amount";
      setSession(chatId, sess);
      await send(chatId, "Nhập số dư MỤC TIÊU nha~ (vd 5000k)", { reply_markup: rightKb() });
      return true;
    }
    if (sess.step === "amount") {
      const amt = parseMoney(text);
      if (amt == null) {
        await send(chatId, "Sai tiền rồi nè 🥺 Bạn nhập lại (vd 5000k) nha~", { reply_markup: rightKb() });
        return true;
      }
      await executeAction(chatId, userName, { action: "wallet_adjust", wallet: sess.data.wallet, target: amt });
      clearSession(chatId);
      return true;
    }
  }

  if (sess.flow === "machine_buy") {
    if (sess.step === "price") {
      const price = parseMoney(text);
      if (price == null || price <= 0) {
        await send(chatId, "Giá này hông ổn á 😝 Bạn nhập GIÁ mua (vd 1200k) nha~", { reply_markup: leftKb() });
        return true;
      }
      sess.data = { price };
      sess.step = "wallet";
      setSession(chatId, sess);
      await send(chatId, "Nhập ví dùng để mua (hana / uri / kt) nha~", { reply_markup: leftKb() });
      return true;
    }
    if (sess.step === "wallet") {
      const w = String(text || "").trim().toLowerCase();
      if (!w) {
        await send(chatId, "Bạn nhập mã ví (hana / uri / kt) giúp mình nha~", { reply_markup: leftKb() });
        return true;
      }
      sess.data.wallet = w;
      sess.step = "note";
      setSession(chatId, sess);
      await send(chatId, "Nhập ghi chú (hoặc gõ '-' để bỏ qua) nha~", { reply_markup: leftKb() });
      return true;
    }
    if (sess.step === "note") {
      const note = String(text || "").trim();
      const finalNote = note === "-" ? "" : note;
      await executeAction(chatId, userName, { action: "machine_buy", price: sess.data.price, wallet: sess.data.wallet, note: finalNote });
      clearSession(chatId);
      return true;
    }
  }

  if (sess.flow === "machine_result") {
    if (sess.step === "phone") {
      const pid = String(text || "").trim().toUpperCase();
      if (!pid.match(/^P\d{1,6}$/)) {
        await send(chatId, "Mã máy sai rồi bạn iu ơi ^^ Ví dụ: P0001", { reply_markup: leftKb() });
        return true;
      }
      sess.data = { phone_id: pid };
      sess.step = "result";
      setSession(chatId, sess);
      await send(chatId, "Nhập kết quả: loi / hue / tach nha~", { reply_markup: leftKb() });
      return true;
    }
    if (sess.step === "result") {
      const r = normalizeForParse(text);
      let result = "";
      if (r.includes("loi")) result = "loi";
      else if (r.includes("hue") || r.includes("hoa")) result = "hue";
      else if (r.includes("tach") || r.includes("chet")) result = "tach";

      if (!["loi", "hue", "tach"].includes(result)) {
        await send(chatId, "Kết quả này mình hổng hiểu á 😵‍💫 Nhập: loi / hue / tach nha~", { reply_markup: leftKb() });
        return true;
      }
      sess.data.result = result;

      if (result === "hue") {
        sess.data.amount = 0;
        sess.step = "note";
        setSession(chatId, sess);
        await send(chatId, "Nhập ghi chú (hoặc '-' để bỏ qua) nha~", { reply_markup: leftKb() });
        return true;
      }

      sess.step = "amount";
      setSession(chatId, sess);
      await send(chatId, "Nhập số tiền (vd 300k). (tạch nhập số dương) nha~", { reply_markup: leftKb() });
      return true;
    }
    if (sess.step === "amount") {
      const amt = parseMoney(text);
      if (amt == null || amt < 0) {
        await send(chatId, "Sai tiền rồi bạn iu ơi ^^ Nhập lại (vd 300k) nha~", { reply_markup: leftKb() });
        return true;
      }
      sess.data.amount = amt;
      sess.step = "note";
      setSession(chatId, sess);
      await send(chatId, "Nhập ghi chú (hoặc '-' để bỏ qua) nha~", { reply_markup: leftKb() });
      return true;
    }
    if (sess.step === "note") {
      const note = String(text || "").trim();
      const finalNote = note === "-" ? "" : note;
      await executeAction(chatId, userName, { action: "machine_result", phone_id: sess.data.phone_id, result: sess.data.result, amount: sess.data.amount, note: finalNote });
      clearSession(chatId);
      return true;
    }
  }

  return false;
}

/* =========================
 * SECTION 20 — Cron remind 14-day
 * ========================= */
const awaitingCheckin = new Map();

async function askCheckin(inv) {
  if (!ADMIN_TELEGRAM_ID) return;
  awaitingCheckin.set(ADMIN_TELEGRAM_ID, inv);
  const label = inv.game === "hq" ? "🎁 Hộp quà" : "🔳 QR";
  await send(
    ADMIN_TELEGRAM_ID,
    `⏰ Tới hạn 14 ngày rồi nè!\n${label}: ${inv.name} (${inv.email})\nBạn reply số tiền (vd: 60k) nha~`,
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

/* =========================
 * SECTION 21 — Webhook handler
 * ========================= */
async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  if (!chatId) return;
  const userName = msg.from?.first_name || "User";
  const text = String(msg.text || "").trim();
  if (!text) return;

  // Admin checkin reply
  if (ADMIN_TELEGRAM_ID && String(chatId) === String(ADMIN_TELEGRAM_ID)) {
    const inv = awaitingCheckin.get(ADMIN_TELEGRAM_ID);
    if (inv) {
      const amt = parseMoney(text);
      if (amt != null) {
        await addCheckinReward({ game: inv.game, name: inv.name, email: inv.email, due_date: inv.due_date, amount: amt, chatId, userName });
        await addGameRevenue({ game: inv.game, type: "checkin", amount: amt, note: `${inv.name} ${inv.email}`, chatId, userName });
        await markDone(inv.rowNumber, amt);
        awaitingCheckin.delete(ADMIN_TELEGRAM_ID);
        await send(chatId, `✅ Check-in ${inv.game.toUpperCase()} ${inv.name}: ${moneyWON(amt)}`, { reply_markup: mainKb() });
        return;
      }
    }
  }

  if (text === "/start") {
    clearSession(chatId);
    await send(chatId, `✅ Bot sẵn sàng (${VERSION})\nGọi mình là “bé bot” cũng được 😝`, { reply_markup: mainKb() });
    return;
  }

  if (text === "/help") {
    await send(chatId, helpText(), { reply_markup: mainKb() });
    return;
  }

  if (text === "⬅️ Menu") {
    clearSession(chatId);
    await send(chatId, "⬅️ Menu Trái đây nè~ (nhập liệu siêu nhanh) ⚡", { reply_markup: leftKb() });
    return;
  }
  if (text === "➡️ Menu") {
    clearSession(chatId);
    await send(chatId, "➡️ Menu Phải đây nè~ (báo cáo + máy + ví) 📊", { reply_markup: rightKb() });
    return;
  }
  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, "Về menu chính nha bạn iu~ 🏠", { reply_markup: mainKb() });
    return;
  }

  // Right menu actions
  if (text === "💰 1) Tổng Doanh Thu") return reportTotalRevenue(chatId);
  if (text === "📅 2) Tháng Này") return reportThisMonth(chatId);
  if (text === "⏮️ 3) Tháng Trước") return reportLastMonth(chatId);
  if (text === "📊 4) Thống Kê Game") return reportStatsGames(chatId);
  if (text === "📱 7) Lời/Lỗ Máy") return reportMachineProfit(chatId);
  if (text === "💼 Xem Ví") return reportWallets(chatId);

  if (text === "🧾 Chỉnh Số Dư Ví") {
    setSession(chatId, { flow: "wallet_adjust", step: "wallet" });
    await send(chatId, "Bạn nhập mã ví cần chỉnh (hana / uri / kt) nha~", { reply_markup: rightKb() });
    return;
  }

  if (text === "📘 Hướng Dẫn Lệnh") {
    await send(chatId, helpText(), { reply_markup: rightKb() });
    return;
  }

  // ✅ PASS hidden (không lộ 12345)
  if (text === "🧨 Xóa Sạch Dữ Liệu") {
    setSession(chatId, { flow: "reset", step: "pass" });
    await send(chatId, "⚠️ Khu vực nguy hiểm nha bạn iu 😵‍💫\n🔐 Vui lòng điền pass để XÓA SẠCH dữ liệu ^^", { reply_markup: rightKb() });
    return;
  }

  // Left menu actions
  if (text === "🎁 Mời Hộp Quà") {
    setSession(chatId, { flow: "invite", game: "hq", step: "name", data: {} });
    await send(chatId, "🎁 Mời Hộp Quà – bạn nhập TÊN trước nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "🔳 Mời QR") {
    setSession(chatId, { flow: "invite", game: "qr", step: "name", data: {} });
    await send(chatId, "🔳 Mời QR – bạn nhập TÊN trước nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "⚽ Thu Đá Bóng") {
    setSession(chatId, { flow: "revenue", game: "db", step: "amount" });
    await send(chatId, "⚽ Thu Đá Bóng – bạn nhập SỐ TIỀN (vd 100k) nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "🎁 Thu Hộp Quà") {
    setSession(chatId, { flow: "revenue", game: "hq", step: "amount" });
    await send(chatId, "🎁 Thu Hộp Quà – bạn nhập SỐ TIỀN (vd 200k) nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "🔳 Thu QR") {
    setSession(chatId, { flow: "revenue", game: "qr", step: "amount" });
    await send(chatId, "🔳 Thu QR – bạn nhập SỐ TIỀN (vd 57k) nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "➕ Thu Khác") {
    setSession(chatId, { flow: "revenue", game: "other", step: "amount" });
    await send(chatId, "➕ Thu Khác – bạn nhập SỐ TIỀN (vd 0.5k) nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "📱 Mua Máy") {
    setSession(chatId, { flow: "machine_buy", step: "price" });
    await send(chatId, "📱 Mua Máy – bạn nhập GIÁ (vd 1200k) nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "✅ KQ Máy (Lời/Huề/Tạch)") {
    setSession(chatId, { flow: "machine_result", step: "phone" });
    await send(chatId, "✅ Kết Quả Máy – bạn nhập MÃ MÁY (vd P0001) nha~", { reply_markup: leftKb() });
    return;
  }

  // Session handler
  if (await handleSessionInput(chatId, userName, text)) return;

  // Strict commands
  const strict = parseStrictCommand(text);
  if (strict) {
    await executeAction(chatId, userName, strict);
    return;
  }

  // Smart Parse
  if (await isSmartParseEnabled()) {
    const sp = smartParse(text);

    if (sp.action === "wallet_view") return void (await executeAction(chatId, userName, sp));
    if (sp.action === "wallet_adjust") return void (await executeAction(chatId, userName, sp));
    if (sp.action === "machine_buy") return void (await executeAction(chatId, userName, sp));
    if (sp.action === "machine_result") return void (await executeAction(chatId, userName, sp));
    if (sp.action === "invite") return void (await executeAction(chatId, userName, sp));
    if (sp.action === "revenue") return void (await executeAction(chatId, userName, sp));
  }

  // Unknown message (as you asked)
  await send(
    chatId,
    "Nhập sai rồi bạn iu ơi ^^  Vào ➡️ Menu → 📘 Hướng dẫn nha~\n(hoặc bật 🧠 Smart Parse để mình hiểu bạn hơn 😚)",
    { reply_markup: mainKb(), __raw: true }
  );
}

/* =========================
 * SECTION 22 — Webhook route
 * ========================= */
app.post("/webhook", async (req, res) => {
  res.sendStatus(200);
  try {
    if (req.body?.message) await handleTextMessage(req.body.message);
  } catch (e) {
    console.error("WEBHOOK ERROR:", e?.message || e);
  }
});

/* =========================
 * SECTION 23 — BOOT (Render stable)
 * ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ BOT READY on ${PORT} | ${VERSION}`);
});
