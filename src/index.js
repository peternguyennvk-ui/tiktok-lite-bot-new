// src/index.js
import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * ENV
 * ========================= */
const VERSION = "LOT-MAxx-SMARTPARSE-WALLET-SELL-ANALYSIS-PHONE-LIST-V2";
const BOT_TOKEN = process.env.BOT_TOKEN;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "/etc/secrets/google-service-account.json";

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");

const TELEGRAM_API = `https://api.telegram.org/bot${BOT_TOKEN}`;
const RESET_PASS = "12345";

/* =========================
 * Helpers
 * ========================= */
function moneyWON(n) {
  return "₩" + Number(n || 0).toLocaleString("ko-KR");
}
function wonText(n) {
  return Number(n || 0).toLocaleString("ko-KR") + " WON";
}
function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}
function displayLot(lot) {
  const s = String(lot || "").trim().toUpperCase();
  const m = s.match(/^MA(\d+)$/);
  if (!m) return s;
  return `MÃ ${String(Number(m[1])).padStart(2, "0")}`;
}

function cuteifyHtml(text) {
  const tails = [" 😚", " 🫶", " ✨", " ^^", " 😝", " 🤭", " 💖"];
  let s = String(text ?? "");
  s = s.replaceAll("Không hiểu", "Nhập sai rồi bạn iu ơi ^^");

  const endsEmoji = /[\u{1F300}-\u{1FAFF}\u2600-\u27BF]$/u.test(s.trim());
  const endsCaret = /\^+$/.test(s.trim());
  if (!endsEmoji && !endsCaret) {
    const idx = (s.length + 3) % tails.length;
    s += tails[idx];
  }
  return s;
}

/* =========================
 * Express
 * ========================= */
const app = express();
app.use(express.json());

app.get("/", (_, res) => res.status(200).send(`OK ${VERSION}`));
app.get("/ping", (_, res) => res.status(200).json({ ok: true, version: VERSION }));

/* =========================
 * Telegram
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

// ✅ HTML send (bold/italic/code)
async function send(chatId, html, extra = {}) {
  if (!chatId) return;
  const raw = extra?.__raw === true;
  const { __raw, ...rest } = extra;
  const finalHtml = raw ? String(html ?? "") : cuteifyHtml(String(html ?? ""));
  await tg("sendMessage", {
    chat_id: chatId,
    text: finalHtml,
    parse_mode: "HTML",
    disable_web_page_preview: true,
    ...rest,
  });
}

/* =========================
 * Keyboards
 *  - Back must be on top inside left/right menus
 * ========================= */
function kb(rows) {
  return { keyboard: rows, resize_keyboard: true, one_time_keyboard: false, is_persistent: true };
}
function mainKb() {
  return kb([[{ text: "⬅️ Menu" }, { text: "➡️ Menu" }]]);
}
function leftKb() {
  return kb([
    [{ text: "⬅️ Back" }],
    [{ text: "📱 Mua Máy (Lô)" }, { text: "💸 Bán Máy" }],
    [{ text: "🧪 Kiểm Tra Máy (Tất cả)" }, { text: "🧪 20 Lô Gần Nhất" }],
    [{ text: "📋 Danh Sách Máy" }],
    [{ text: "⚽ Thu Đá Bóng" }, { text: "🎁 Thu Hộp Quà" }],
    [{ text: "🔳 Thu QR" }, { text: "➕ Thu Khác" }],
  ]);
}
function rightKb() {
  return kb([
    [{ text: "⬅️ Back" }],
    [{ text: "📊 Phân Tích" }],
    [{ text: "💰 Tổng Doanh Thu" }],
    [{ text: "✏️ Sửa Số Dư Tổng Doanh Thu" }],
    [{ text: "📅 Tháng Này" }, { text: "⏮️ Tháng Trước" }],
    [{ text: "📊 Thống Kê Game" }],
    [{ text: "💼 Xem Ví" }],
    [{ text: "✏️ Sửa Số Dư Ví" }],
    [{ text: "📘 Hướng Dẫn" }],
    [{ text: "🧠 Smart Parse: Bật/Tắt" }],
    [{ text: "🧨 Xóa Sạch Dữ Liệu" }],
  ]);
}

/* =========================
 * Sheets
 * ========================= */
const auth = new google.auth.GoogleAuth({
  keyFile: GOOGLE_APPLICATION_CREDENTIALS,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

async function getValues(rangeA1) {
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range: rangeA1 });
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
  await sheets.spreadsheets.values.clear({ spreadsheetId: GOOGLE_SHEET_ID, range: rangeA1 });
}

/* =========================
 * Normalize + Money
 * ========================= */
function nowIso() {
  return new Date().toISOString();
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
  tmp = tmp.replace(/([a-z]+)(\d)/g, "$1 $2");
  tmp = tmp.replace(/(\d)([a-z]+)/g, "$1 $2");
  tmp = tmp.replace(/[，]/g, ",").replace(/\s+/g, " ").trim();
  tmp = tmp.replace(/__email_(\d+)__/g, (_, i) => emails[Number(i)] || "");
  return tmp;
}
function parseMoney(input) {
  if (input == null) return null;
  let s = String(input).trim().toLowerCase();
  s = s.replace(/₩/g, "").replace(/\bwon\b/g, "").replace(/,/g, "");
  s = s.replace(/\s+/g, " ").trim();
  s = s.replace(/(\d)\s+k\b/g, "$1k");
  const m = s.match(/^(\d+(?:\.\d+)?)(k)?$/);
  if (!m) return null;
  const num = Number(m[1]);
  if (!Number.isFinite(num)) return null;
  return Math.round(m[2] ? num * 1000 : num);
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
function normalizeSpaces(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

/* =========================
 * SETTINGS
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
async function isSmartParseEnabled() {
  const v = (await getSetting("SMART_PARSE_ENABLED")).trim();
  if (v === "") return true;
  return v === "1";
}
async function toggleSmartParse() {
  const cur = await isSmartParseEnabled();
  await setSetting("SMART_PARSE_ENABLED", cur ? "0" : "1");
  return !cur;
}

/* =========================
 * Payouts for ANALYSIS (NOT main revenue)
 * ========================= */
async function getMachinePayouts() {
  // Defaults per your rule
  const hq = parseMoney((await getSetting("MACHINE_PAYOUT_HQ")) || "150k") ?? 150000;
  const qr = parseMoney((await getSetting("MACHINE_PAYOUT_QR")) || "57k") ?? 57000;
  const db = parseMoney((await getSetting("MACHINE_PAYOUT_DB")) || "100k") ?? 100000;
  return { hq, qr, db };
}

/* =========================
 * Wallets
 * ========================= */
function parseWalletShortcut(text) {
  const norm = normalizeForParse(text);
  const t = ` ${norm} `;
  if (t.includes(" hn ") || t.includes(" hana ")) return "hana";
  if (t.includes(" uri ")) return "uri";
  if (t.includes(" kt ")) return "kt";
  if (t.includes(" tm ") || t.includes(" tien mat ") || t.includes(" tienmat ")) return "tm";
  return "";
}

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
      { code: "kt", name: "Viễn Thông KT" },
      { code: "tm", name: "TIỀN MẶT" },
    ];
  }
  if (!wallets.find((w) => w.code === "tm")) wallets.push({ code: "tm", name: "TIỀN MẶT" });
  const kt = wallets.find((w) => w.code === "kt");
  if (kt && (!kt.name || kt.name.toUpperCase() === "KT")) kt.name = "Viễn Thông KT";
  if (!kt) wallets.push({ code: "kt", name: "Viễn Thông KT" });
  return wallets;
}

async function readWalletLog() {
  const rows = await getValues("WALLET_LOG!A2:H");
  return rows.map((r) => ({
    wallet: String(r[1] || "").trim().toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    ref_type: String(r[4] || "").trim().toLowerCase(),
    ref_id: String(r[5] || "").trim().toUpperCase(),
    type: String(r[2] || "").trim().toLowerCase(),
    note: String(r[6] || ""),
  }));
}

async function addWalletLog({ wallet, type, amount, ref_type, ref_id, note, chatId }) {
  await appendValues("WALLET_LOG!A1", [
    [nowIso(), wallet, type, amount, ref_type || "", String(ref_id || ""), note || "", String(chatId || "")],
  ]);
  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "wallet_log_add", wallet, type, amount, String(ref_id || "")]]);
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

// ✅ set absolute balance by adding an adjust delta
async function setWalletBalanceAbsolute(walletCode, newBalance, chatId) {
  const balances = await walletBalances();
  const w = balances.find((x) => x.code === walletCode);
  const current = w ? w.balance : 0;
  const delta = Math.round(newBalance - current);
  if (delta === 0) return { current, newBalance, delta: 0 };

  await addWalletLog({
    wallet: walletCode,
    type: "wallet_adjust",
    amount: delta,
    ref_type: "wallet",
    ref_id: walletCode,
    note: `SET_BALANCE ${current} -> ${newBalance}`,
    chatId,
  });

  return { current, newBalance, delta };
}

/* =========================
 * Game Revenue (manual only)
 * ========================= */
async function addGameRevenue({ game, type, amount, note, chatId, userName }) {
  await appendValues("GAME_REVENUE!A1", [[nowIso(), game, type, amount, note || "", String(chatId || ""), userName || ""]]);
  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "revenue_add", game, type, amount, note || ""]]);
  } catch (_) {}
}
async function readGameRevenue() {
  const rows = await getValues("GAME_REVENUE!A2:G");
  return rows.map((r) => ({
    ts: r[0] || "",
    game: (r[1] || "").toLowerCase(),
    type: (r[2] || "").toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    note: String(r[4] || ""),
  }));
}
function monthKey(ts) {
  if (!ts) return "";
  return String(ts).slice(0, 7);
}
async function setTotalRevenueAbsolute(newTotal, chatId, userName) {
  const rows = await readGameRevenue();
  const current = rows.reduce((a, b) => a + b.amount, 0);
  const delta = Math.round(newTotal - current);
  if (delta === 0) return { current, newTotal, delta: 0 };
  await addGameRevenue({
    game: "other",
    type: "revenue_adjust",
    amount: delta,
    note: `SET_TOTAL ${current} -> ${newTotal}`,
    chatId,
    userName,
  });
  return { current, newTotal, delta };
}

/* =========================
 * LOTS + PHONES
 * ========================= */
async function nextLotCode() {
  const rows = await getValues("LOTS!A2:A");
  let max = 0;
  for (const r of rows) {
    const s = String(r[0] || "").trim().toUpperCase();
    const m = s.match(/^MA(\d+)$/);
    if (m) {
      const n = Number(m[1]);
      if (Number.isFinite(n) && n > max) max = n;
    }
  }
  return "MA" + String(max + 1).padStart(2, "0");
}

async function addLot({ qty, model, total_price, wallet, note }) {
  const lot = await nextLotCode();
  const unit = Math.round(total_price / qty);

  await appendValues("LOTS!A1", [[lot, nowIso(), qty, model, total_price, unit, wallet, note || ""]]);

  await addWalletLog({
    wallet,
    type: "lot_buy",
    amount: -Math.abs(total_price),
    ref_type: "lot",
    ref_id: lot,
    note: note || "",
    chatId: "",
  });

  const ids = [];
  for (let i = 1; i <= qty; i++) {
    const phone_id = `${lot}-${i}`;
    ids.push(phone_id);
    await appendValues("PHONES!A1", [[phone_id, lot, nowIso(), unit, "new", "none", note || ""]]);
  }

  return { lot, unit, ids };
}

async function readLots() {
  const rows = await getValues("LOTS!A2:H");
  return rows
    .filter((r) => r.some((c) => String(c || "").trim() !== ""))
    .map((r) => ({
      lot: String(r[0] || "").trim().toUpperCase(),
      ts: String(r[1] || ""),
      qty: Number(String(r[2] || "0").replace(/,/g, "")) || 0,
      model: String(r[3] || ""),
      total: Number(String(r[4] || "0").replace(/,/g, "")) || 0,
      unit: Number(String(r[5] || "0").replace(/,/g, "")) || 0,
      wallet: String(r[6] || "").trim().toLowerCase(),
      note: String(r[7] || ""),
    }));
}

async function getLotByCode(lotCode) {
  const lots = await readLots();
  return lots.find((l) => l.lot === String(lotCode || "").trim().toUpperCase()) || null;
}

async function readPhones() {
  const rows = await getValues("PHONES!A2:G");
  return rows
    .filter((r) => r.some((c) => String(c || "").trim() !== ""))
    .map((r) => ({
      phone_id: String(r[0] || "").trim(),
      lot: String(r[1] || "").trim().toUpperCase(),
      unit: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
      status: String(r[4] || "").trim().toLowerCase(),
      game: String(r[5] || "").trim().toLowerCase(),
      note: String(r[6] || ""),
    }));
}

async function updatePhoneRowById(phone_id, patch) {
  const rows = await getValues("PHONES!A2:G");
  for (let i = 0; i < rows.length; i++) {
    const id = String(rows[i][0] || "").trim();
    if (id === phone_id) {
      const rowNumber = i + 2;
      const status = patch.status ?? String(rows[i][4] || "");
      const game = patch.game ?? String(rows[i][5] || "");
      await updateValues(`PHONES!E${rowNumber}:F${rowNumber}`, [[status, game]]);
      return true;
    }
  }
  return false;
}

/* =========================
 * Parse model/game/lot/wallet
 * ========================= */
function detectModelToken(norm) {
  const t = ` ${norm} `;
  const map = [
    { keys: [" ss ", " samsung ", " sam "], model: "Samsung" },
    { keys: [" ip ", " iphone ", " i phone "], model: "iPhone" },
    { keys: [" lg "], model: "LG" },
    { keys: [" oppo "], model: "Oppo" },
    { keys: [" vivo "], model: "Vivo" },
    { keys: [" xiaomi ", " mi "], model: "Xiaomi" },
    { keys: [" redmi "], model: "Redmi" },
    { keys: [" nokia "], model: "Nokia" },
    { keys: [" pixel "], model: "Pixel" },
  ];
  for (const item of map) if (item.keys.some((k) => t.includes(k))) return item.model;
  if (norm.match(/\b\d+ss\b/) || t.includes(" ss ")) return "Samsung";
  if (norm.match(/\b\d+ip\b/) || t.includes(" ip ")) return "iPhone";
  if (t.includes(" lg ")) return "LG";
  return "Unknown";
}

function parseBuySentence(text) {
  const raw = normalizeSpaces(text).toLowerCase();
  const norm = normalizeForParse(raw);
  if (!norm.includes("mua")) return null;

  const totalPrice = extractMoneyFromText(text);
  if (totalPrice == null) return { incomplete: true };

  let qty = 1;
  const mCompact = norm.match(/\bmua\s+(\d+)\s*(ss|ip|lg)\b/);
  if (mCompact) qty = Number(mCompact[1]) || 1;
  else {
    const mQty = norm.match(/\bmua\s+(\d+)\b/);
    if (mQty) qty = Number(mQty[1]) || 1;
  }
  qty = Math.max(1, Math.min(50, qty));

  const model = detectModelToken(norm);
  const wallet = parseWalletShortcut(text);

  let note = raw
    .replace(/\b(mua)\b/g, "")
    .replace(/\b(dt|đt|dien thoai|dien-thoai|may)\b/g, "")
    .replace(/\b\d+\s*(ss|ip|lg)\b/g, "")
    .replace(/\bss\b|\bsamsung\b|\bip\b|\biphone\b|\blg\b/g, "")
    .replace(/\bhn\b|\bhana\b|\buri\b|\bkt\b|\btm\b|\btien mat\b|\btienmat\b/g, "")
    .replace(/₩/g, "")
    .replace(/\d[\d,]*(?:\.\d+)?\s*k\b|\d[\d,]*(?:\.\d+)?\b/g, "");
  note = normalizeSpaces(note);

  return { qty, model, totalPrice, wallet, note };
}

function parseLotCode(text) {
  const norm = normalizeForParse(text);
  let m = norm.match(/\b(ma|ma so|ma)\s*0*(\d{1,3})\b/);
  if (!m) m = norm.match(/\bma0*(\d{1,3})\b/);
  if (!m) return "";
  const n = Number(m[2]);
  if (!Number.isFinite(n)) return "";
  return "MA" + String(n).padStart(2, "0");
}

function parseSellSentence(text) {
  const norm = normalizeForParse(text);
  if (!norm.includes("ban")) return null;

  const lot = parseLotCode(text);
  if (!lot) return { incomplete: true };

  const totalPrice = extractMoneyFromText(text);
  if (totalPrice == null) return { incomplete: true };

  let qty = 1;
  const mQty = norm.match(/\bban\s+(\d+)\b/);
  if (mQty) qty = Number(mQty[1]) || 1;
  qty = Math.max(1, Math.min(50, qty));

  const wallet = parseWalletShortcut(text);
  return { lot, qty, totalPrice, wallet };
}

function detectGameToken(norm) {
  const t = ` ${norm} `;
  if (t.includes(" hq ") || t.includes(" hopqua ") || t.includes(" hop qua ")) return "hq";
  if (t.includes(" qr ")) return "qr";
  if (t.includes(" db ") || t.includes(" dabong ") || t.includes(" da bong ")) return "db";
  return "";
}

/**
 * New resolve parser:
 * Supports:
 * - ma01 hq1 tach2
 * - ma01 qr 1 db 1 lo 1
 * - ma01 loi 1 hq tach 2
 * - ma01 loi1hq 1qr tach1
 * Also supports hue/hoa/von for huề
 */
function parseLotResolve(text) {
  const norm = normalizeForParse(text);
  const lot = parseLotCode(text);
  if (!lot) return null;

  // tokens already separated by normalizeForParse (letters/numbers split)
  const tokens = norm.split(" ").filter(Boolean);

  const segments = [];
  let i = 0;

  const readNumber = () => {
    if (i < tokens.length && /^\d+$/.test(tokens[i])) {
      const n = Number(tokens[i]);
      i += 1;
      return Number.isFinite(n) ? n : null;
    }
    return null;
  };

  const pushSeg = (kind, count, game = "") => {
    segments.push({ kind, count: Math.max(0, Math.min(50, count || 0)), game: game || "" });
  };

  while (i < tokens.length) {
    const tk = tokens[i];

    // Skip the lot marker itself
    if (tk === "ma" || tk.startsWith("ma")) {
      i += 1;
      continue;
    }

    // shorthand game count: hq 1 / qr 1 / db 1
    if (tk === "hq" || tk === "qr" || tk === "db") {
      i += 1;
      const n = readNumber();
      if (n != null) pushSeg("loi", n, tk);
      continue;
    }

    // words for profit/loss/hue
    const isLoi = tk === "loi" || tk === "lai" || tk === "ok" || tk === "an" || tk === "duoc";
    const isLo = tk === "lo" || tk === "tach" || tk === "chet" || tk === "tac";
    const isHue = tk === "hue" || tk === "hoa" || tk === "von" || tk === "thuvon";

    if (isLoi) {
      i += 1;
      const n = readNumber() ?? 1;

      // optional: consume "may"/"dt"
      if (i < tokens.length && (tokens[i] === "may" || tokens[i] === "dt")) i++;

      // optional game after loi: hq/qr/db
      let g = "";
      if (i < tokens.length && (tokens[i] === "hq" || tokens[i] === "qr" || tokens[i] === "db")) {
        g = tokens[i];
        i += 1;
      }
      // If loi but no game => mark "ASK"
      pushSeg("loi", n, g || "ASK");
      continue;
    }

    if (isLo) {
      i += 1;
      const n = readNumber() ?? 1;
      pushSeg("tach", n, "");
      continue;
    }

    if (isHue) {
      i += 1;
      const n = readNumber() ?? 1;
      pushSeg("hue", n, "");
      continue;
    }

    // compact forms: hq1 / qr2 / db1 / tach2 / lo3 / hue1
    const compact = tk.match(/^(hq|qr|db|tach|lo|hue|hoa|von)(\d+)$/);
    if (compact) {
      const key = compact[1];
      const n = Number(compact[2]) || 0;
      if (key === "hq" || key === "qr" || key === "db") pushSeg("loi", n, key);
      else if (key === "tach" || key === "lo") pushSeg("tach", n, "");
      else pushSeg("hue", n, "");
      i += 1;
      continue;
    }

    i += 1;
  }

  // Remove any zero segments
  const cleaned = segments.filter((s) => s.count > 0);

  if (cleaned.length === 0) return { lot, segments: [] };
  return { lot, segments: cleaned };
}

/* =========================
 * Apply resolve / sell
 * ========================= */
async function applyLotResolve({ chatId, lot, segments }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy mã lô <code>${escapeHtml(displayLot(lot))}</code> á. Bạn check lại nha~`, { reply_markup: leftKb() });
    return true;
  }

  // If any segment has game ASK => ask user to choose game
  const askIdx = segments.findIndex((s) => s.kind === "loi" && s.game === "ASK");
  if (askIdx >= 0) {
    // Save session to continue after selecting game
    setSession(chatId, { flow: "resolve_ask_game", step: "pick", data: { lot, segments, askIdx } });
    await send(
      chatId,
      `Bạn muốn <b>lời</b> là ăn game nào? Chọn 1 cái nha:\n🎁 <code>hq</code> | 🔳 <code>qr</code> | ⚽ <code>db</code>`,
      { reply_markup: kb([[{ text: "hq" }, { text: "qr" }, { text: "db" }], [{ text: "⬅️ Back" }]]) }
    );
    return true;
  }

  const payouts = await getMachinePayouts();

  // pick phones to assign results:
  // Priority: new first, then others except sold
  const pick = (n) => {
    const pending = lotPhones.filter((p) => p.status === "new");
    const pool = pending.length > 0 ? pending : lotPhones.filter((p) => p.status !== "sold");
    return pool.slice(0, n).map((p) => p.phone_id);
  };

  let loiHQ = 0;
  let loiQR = 0;
  let loiDB = 0;
  let totalLo = 0;
  let totalHue = 0;

  for (const seg of segments) {
    const ids = pick(seg.count);
    if (ids.length === 0) continue;

    if (seg.kind === "tach") {
      for (const id of ids) await updatePhoneRowById(id, { status: "tach", game: "none" });
      totalLo += ids.length;
      continue;
    }

    if (seg.kind === "hue") {
      for (const id of ids) await updatePhoneRowById(id, { status: "hue", game: "none" });
      totalHue += ids.length;
      continue;
    }

    // seg.kind = loi
    const game = (seg.game || "hq").toLowerCase();
    for (const id of ids) await updatePhoneRowById(id, { status: "ok", game });

    if (game === "hq") loiHQ += ids.length;
    else if (game === "qr") loiQR += ids.length;
    else if (game === "db") loiDB += ids.length;
    else loiHQ += ids.length; // fallback
  }

  const totalGame = loiHQ * payouts.hq + loiQR * payouts.qr + loiDB * payouts.db;
  const html =
    `🧾 <b>CHỐT LÔ</b> <b>${escapeHtml(displayLot(lot))}</b>\n` +
    `✅ <b>Lời:</b> <code>${loiHQ + loiQR + loiDB}</code> MÁY ` +
    `(HQ:<code>${loiHQ}</code> / QR:<code>${loiQR}</code> / DB:<code>${loiDB}</code>)\n` +
    `😵 <b>Lỗ:</b> <code>${totalLo}</code> MÁY TẠCH\n` +
    `😌 <b>Huề:</b> <code>${totalHue}</code>\n` +
    `🎮 <b>Tổng thu game (phân tích):</b> <b>${wonText(totalGame)}</b>`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

async function sellFromLot({ chatId, lot, qty, totalPrice, wallet }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy lô <code>${escapeHtml(displayLot(lot))}</code> luôn á. Bạn check lại mã nha~`, { reply_markup: leftKb() });
    return true;
  }

  const sellable = lotPhones
    .filter((p) => p.status !== "sold")
    .sort((a, b) => {
      const rank = (s) => (s === "new" ? 0 : s === "ok" ? 1 : s === "hue" ? 2 : s === "tach" ? 3 : 4);
      return rank(a.status) - rank(b.status);
    });

  const ids = sellable.slice(0, qty).map((p) => p.phone_id);
  if (ids.length === 0) {
    await send(chatId, `Lô <code>${escapeHtml(displayLot(lot))}</code> bán hết sạch rồi 😝`, { reply_markup: leftKb() });
    return true;
  }

  // IMPORTANT: keep game (do NOT set game=none), only set status sold
  for (const id of ids) await updatePhoneRowById(id, { status: "sold" });

  await addWalletLog({
    wallet,
    type: "machine_sell",
    amount: Math.abs(Math.round(totalPrice)),
    ref_type: "lot",
    ref_id: lot,
    note: `SELL x${ids.length}`,
    chatId,
  });

  const lotRow = await getLotByCode(lot);
  const note = (lotRow?.note || "").trim();
  const notePart = note ? ` ${escapeHtml(note)}` : "";

  const html =
    `💸 <b>BÁN XONG</b> 🥳\n` +
    `• Lô: <b>${escapeHtml(displayLot(lot))}</b>\n` +
    `• Số máy: <code>${ids.length}</code> máy${notePart}\n` +
    `• Tiền về ví <code>${escapeHtml(wallet.toUpperCase())}</code>: <b>${moneyWON(Math.round(totalPrice))}</b>\n\n` +
    `Phân tích lô sẽ tự cộng tiền bán này vào nhé 😝 💖`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

/* =========================
 * Reports (manual revenue)
 * ========================= */
async function reportTotalRevenue(chatId) {
  const rows = await readGameRevenue();
  const sum = rows.reduce((a, b) => a + b.amount, 0);
  await send(chatId, `💰 <b>TỔNG DOANH THU</b>\n= <b>${moneyWON(sum)}</b>`, { reply_markup: rightKb() });
}
async function reportThisMonth(chatId) {
  const m = dayjs().format("YYYY-MM");
  const rows = await readGameRevenue();
  const sum = rows.filter((x) => monthKey(x.ts) === m).reduce((a, b) => a + b.amount, 0);
  await send(chatId, `📅 <b>DOANH THU THÁNG</b> <code>${m}</code>\n= <b>${moneyWON(sum)}</b>`, { reply_markup: rightKb() });
}
async function reportLastMonth(chatId) {
  const m = dayjs().subtract(1, "month").format("YYYY-MM");
  const rows = await readGameRevenue();
  const sum = rows.filter((x) => monthKey(x.ts) === m).reduce((a, b) => a + b.amount, 0);
  await send(chatId, `⏮️ <b>DOANH THU THÁNG</b> <code>${m}</code>\n= <b>${moneyWON(sum)}</b>`, { reply_markup: rightKb() });
}
async function reportWallets(chatId) {
  const balances = await walletBalances();
  let total = 0;
  const lines = balances.map((b) => {
    total += b.balance;
    return `• <b>${escapeHtml(b.name)}</b> (<code>${escapeHtml(b.code)}</code>): <b>${moneyWON(b.balance)}</b>`;
  });
  await send(chatId, `💼 <b>SỐ DƯ CÁC VÍ</b>\n\n${lines.join("\n")}\n\n<b>Tổng:</b> <b>${moneyWON(total)}</b>`, { reply_markup: rightKb() });
}
async function reportStatsGames(chatId) {
  const rev = await readGameRevenue();
  const dbSum = rev.filter((x) => x.game === "db").reduce((a, b) => a + b.amount, 0);
  const hqSum = rev.filter((x) => x.game === "hq").reduce((a, b) => a + b.amount, 0);
  const qrSum = rev.filter((x) => x.game === "qr").reduce((a, b) => a + b.amount, 0);
  await send(
    chatId,
    `📊 <b>THỐNG KÊ GAME</b>\n\n⚽ <b>DB</b>: <b>${moneyWON(dbSum)}</b>\n🎁 <b>HQ</b>: <b>${moneyWON(hqSum)}</b>\n🔳 <b>QR</b>: <b>${moneyWON(qrSum)}</b>`,
    { reply_markup: rightKb() }
  );
}

/* =========================
 * Analysis (machines)
 * ========================= */
function bar(label, value, total, width = 18) {
  const pct = total > 0 ? value / total : 0;
  const filled = Math.round(pct * width);
  const empty = Math.max(0, width - filled);
  const block = "█".repeat(filled) + " ".repeat(empty);
  const p = Math.round(pct * 100);
  return `${label.padEnd(5)}: ${block} ${String(p).padStart(3)}% (${value})`;
}
function moneyBar(label, amount, max, width = 18) {
  const pct = max > 0 ? amount / max : 0;
  const filled = Math.round(pct * width);
  const empty = Math.max(0, width - filled);
  const block = "█".repeat(filled) + " ".repeat(empty);
  return `${label.padEnd(9)}: ${block} ${moneyWON(amount)}`;
}

async function analysisMachines(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const logs = await readWalletLog();
  const payouts = await getMachinePayouts();

  const totalBuy = lots.reduce((a, b) => a + b.total, 0);

  const hq = phones.filter((p) => p.game === "hq").length;
  const qr = phones.filter((p) => p.game === "qr").length;
  const db = phones.filter((p) => p.game === "db").length;

  const gameSum = hq * payouts.hq + qr * payouts.qr + db * payouts.db;

  const sellSum = logs
    .filter((l) => l.type === "machine_sell" && l.ref_type === "lot")
    .reduce((a, b) => a + b.amount, 0);

  const netTemp = gameSum - totalBuy;
  const netReal = sellSum + gameSum - totalBuy;

  const sold = phones.filter((p) => p.status === "sold").length;
  const neu = phones.filter((p) => p.status === "new").length;
  const hue = phones.filter((p) => p.status === "hue").length;
  const lo = phones.filter((p) => p.status === "tach").length;

  const loiTotal = phones.filter((p) => p.game === "hq" || p.game === "qr" || p.game === "db").length;
  const loiKeep = phones.filter((p) => (p.game === "hq" || p.game === "qr" || p.game === "db") && p.status !== "sold").length;

  const totalPhones = phones.length || 1;
  const maxMoney = Math.max(totalBuy, gameSum, sellSum, 1);

  const html =
    `📊 <b>PHÂN TÍCH MUA MÁY</b>\n\n` +
    `💳 <b>Đã bỏ ra mua máy:</b> <b>${moneyWON(totalBuy)}</b>\n` +
    `💰 <b>Số tiền thu được:</b> <b>${moneyWON(gameSum + sellSum)}</b>\n` +
    `   • Thu game (HQ/QR/DB): <b>${moneyWON(gameSum)}</b>\n` +
    `   • Thu bán máy: <b>${moneyWON(sellSum)}</b>\n\n` +
    `🧮 <b>Net tạm:</b> <b>${moneyWON(netTemp)}</b>\n` +
    `🧮 <b>Net thực:</b> <b>${moneyWON(netReal)}</b>\n\n` +
    `Máy (tổng kết quả - tính kể cả Sold)\n` +
    `• Lời: <b>${loiTotal}</b> máy (HQ:${hq} / QR:${qr} / DB:${db})\n` +
    `• Lỗ: <b>${lo}</b> máy\n` +
    `• Huề: <b>${hue}</b> máy\n` +
    `• Chưa làm (New): <b>${neu}</b> máy\n` +
    `• Đã bán (Sold): <b>${sold}</b> máy\n` +
    `• Lời còn giữ: <b>${loiKeep}</b> máy\n\n` +
    `📌 <b>Biểu đồ trạng thái</b>\n` +
    `<code>${escapeHtml(bar("New", neu, totalPhones))}\n${escapeHtml(bar("Lời", loiTotal, totalPhones))}\n${escapeHtml(bar("Lỗ", lo, totalPhones))}\n${escapeHtml(bar("Sold", sold, totalPhones))}</code>\n\n` +
    `💸 <b>Biểu đồ tiền</b>\n` +
    `<code>${escapeHtml(moneyBar("Bỏ ra", totalBuy, maxMoney))}\n${escapeHtml(moneyBar("Thu game", gameSum, maxMoney))}\n${escapeHtml(moneyBar("Thu bán", sellSum, maxMoney))}</code>\n\n` +
    `<i>(HQ=${payouts.hq / 1000}k, QR=${payouts.qr / 1000}k, DB=${payouts.db / 1000}k chỉ dùng cho phân tích máy)</i>`;

  await send(chatId, html, { reply_markup: rightKb() });
}

/* =========================
 * Lot list (All / 20 newest)
 * ========================= */
async function sumSellByLot() {
  const logs = await readWalletLog();
  const map = new Map();
  for (const l of logs) {
    if (l.type !== "machine_sell") continue;
    if (l.ref_type !== "lot") continue;
    if (!l.ref_id) continue;
    map.set(l.ref_id, (map.get(l.ref_id) || 0) + l.amount);
  }
  return map;
}

async function listLotsPretty(chatId, mode = "all") {
  const lots = await readLots();
  const phones = await readPhones();
  const payouts = await getMachinePayouts();
  const sellByLot = await sumSellByLot();

  if (lots.length === 0) {
    await send(chatId, `Chưa có lô nào hết á 😝\nBấm <b>📱 Mua Máy (Lô)</b> để tạo lô nha~`, { reply_markup: leftKb() });
    return;
  }

  const sorted = [...lots].sort((a, b) => (a.ts < b.ts ? 1 : -1));
  const picked = mode === "20" ? sorted.slice(0, 20) : sorted;

  const lines = picked.map((l) => {
    const ps = phones.filter((p) => p.lot === l.lot);

    const sold = ps.filter((p) => p.status === "sold").length;
    const neu = ps.filter((p) => p.status === "new").length;
    const hue = ps.filter((p) => p.status === "hue").length;
    const lo = ps.filter((p) => p.status === "tach").length;

    const hq = ps.filter((p) => p.game === "hq").length;
    const qr = ps.filter((p) => p.game === "qr").length;
    const db = ps.filter((p) => p.game === "db").length;

    const loiTotal = hq + qr + db;
    const loiKeep = ps.filter((p) => (p.game === "hq" || p.game === "qr" || p.game === "db") && p.status !== "sold").length;
    const conLai = Math.max(0, (l.qty || 0) - sold);

    const gameSum = hq * payouts.hq + qr * payouts.qr + db * payouts.db;
    const laiTam = gameSum - (l.total || 0);
    const daBan = sellByLot.get(l.lot) || 0;
    const laiThuc = daBan + gameSum - (l.total || 0);

    return (
      `• <b>${escapeHtml(l.lot)}</b>: Mua <code>${l.qty}</code> máy <b>${escapeHtml(l.model)}</b> | Tổng <b>${moneyWON(l.total)}</b> | Ví <code>${escapeHtml(String(l.wallet || "").toUpperCase())}</code>\n\n` +
      `  Trạng thái: Lời <code>${loiTotal}</code> máy (HQ:<code>${hq}</code> / QR:<code>${qr}</code> / DB:<code>${db}</code>) / Huề <code>${hue}</code> / Lỗ <code>${lo}</code> / New <code>${neu}</code> / Sold <code>${sold}</code>\n` +
      `  Lời còn giữ: <code>${loiKeep}</code> máy | Còn lại: <code>${conLai}</code> máy\n\n` +
      `  Tổng thu game: <b>${moneyWON(gameSum)}</b>\n` +
      `  Lãi tạm: <b>${moneyWON(gameSum)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(laiTam)}</b>\n` +
      `  Đã bán: <b>${moneyWON(daBan)}</b>\n` +
      `  Lãi thực: <b>${moneyWON(daBan)}</b> + <b>${moneyWON(gameSum)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(laiThuc)}</b>`
    );
  });

  const title = mode === "20" ? "🧪 <b>DANH SÁCH LÔ MÁY</b> (20 lô gần nhất)" : "🧪 <b>DANH SÁCH LÔ MÁY</b> (Tất cả)";
  const html = `${title}\n\n${lines.join("\n\n")}`;
  await send(chatId, html, { reply_markup: leftKb() });
}

/* =========================
 * Phone list pagination (C)
 * ========================= */
const PHONE_PAGE_SIZE = 30;

async function renderPhoneList(chatId, filterLot = "", page = 1) {
  const phones = await readPhones();
  const lots = await readLots();
  const lotNoteMap = new Map(lots.map((l) => [l.lot, l.note || ""]));

  let list = phones;
  const lot = filterLot ? String(filterLot).trim().toUpperCase() : "";
  if (lot) list = list.filter((p) => p.lot === lot);

  list = list.sort((a, b) => (a.phone_id < b.phone_id ? 1 : -1)); // newest-ish
  const total = list.length;
  const totalPages = Math.max(1, Math.ceil(total / PHONE_PAGE_SIZE));
  const safePage = Math.min(Math.max(1, page), totalPages);

  const start = (safePage - 1) * PHONE_PAGE_SIZE;
  const items = list.slice(start, start + PHONE_PAGE_SIZE);

  const fmtStatus = (s) => {
    if (s === "new") return "New";
    if (s === "ok") return "Lời";
    if (s === "tach") return "Lỗ";
    if (s === "hue") return "Huề";
    if (s === "sold") return "Sold";
    return s || "-";
    };
  const fmtGame = (g) => {
    if (g === "hq") return "HQ";
    if (g === "qr") return "QR";
    if (g === "db") return "DB";
    return g && g !== "none" ? g.toUpperCase() : "-";
  };

  const lines = items.map((p) => {
    const note = lotNoteMap.get(p.lot) || p.note || "";
    const notePart = note ? ` | ${escapeHtml(note)}` : "";
    return `• <b>${escapeHtml(p.phone_id)}</b> (${escapeHtml(displayLot(p.lot))}) — <b>${escapeHtml(fmtStatus(p.status))}</b> / <code>${escapeHtml(fmtGame(p.game))}</code>${notePart}`;
  });

  const header =
    `📋 <b>DANH SÁCH MÁY</b>\n` +
    `${lot ? `Lọc: <code>${escapeHtml(displayLot(lot))}</code>\n` : ""}` +
    `Trang <b>${safePage}</b>/<b>${totalPages}</b> — Tổng <b>${total}</b> máy\n\n`;

  const html = header + (lines.length ? lines.join("\n") : "<i>Không có máy nào.</i>");

  // session store
  setSession(chatId, { flow: "phone_list", step: "nav", data: { lot, page: safePage, totalPages } });

  const navKb = kb([
    [{ text: "⬅️ Back" }],
    [{ text: "⬅️ Prev" }, { text: "➡️ Next" }],
    [{ text: "🔎 Lọc theo lô" }, { text: "🧪 Xem Tất Cả" }],
  ]);

  await send(chatId, html, { reply_markup: navKb });
}

/* =========================
 * Quick revenue (manual)
 * ========================= */
function detectGameFromText(normText) {
  const t = ` ${normText} `;
  if (t.includes(" them ") || t.includes(" thu khac ") || t.includes(" ngoai ")) return "other";
  if (t.includes(" dabong ") || t.includes(" da bong ") || t.includes(" db ")) return "db";
  if (t.includes(" hopqua ") || t.includes(" hop qua ") || t.includes(" hq ")) return "hq";
  if (t.includes(" qr ")) return "qr";
  return "";
}

/* =========================
 * Reset
 * ========================= */
const RESET_CLEAR_RANGES = ["LOTS!A2:Z", "PHONES!A2:Z", "GAME_REVENUE!A2:Z", "WALLET_LOG!A2:Z", "UNDO_LOG!A2:Z"];
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
 * Sessions
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
 * Help
 * ========================= */
function helpText() {
  return (
    `📘 <b>HƯỚNG DẪN</b> (WON ₩)\n\n` +
    `✅ <b>Mua lô</b> (tiền là <b>TỔNG</b>):\n` +
    `• <code>mua 3ss 50k uri</code>\n` +
    `• <code>mua ip 35k</code>\n\n` +
    `Ví tắt: <code>hana/hn</code> | <code>uri</code> | <code>kt</code> | <code>tm</code>\n\n` +
    `✅ <b>Chốt lô</b> (gõ nhanh):\n` +
    `• <code>ma01 hq1 tach2</code>\n` +
    `• <code>ma 01 qr1 db1 lo1</code>\n` +
    `• <code>ma01 hue1</code>\n\n` +
    `✅ <b>Bán</b> (tiền là <b>TỔNG</b>):\n` +
    `• <code>ban 2 ss 80k ma01 tm</code>\n\n` +
    `✅ <b>Doanh thu chính</b> (manual):\n` +
    `• <code>hq 100k khanh mail@gmail.com</code>\n` +
    `• <code>db 60k</code> / <code>qr 57k</code> / <code>them 1k</code>\n\n` +
    `<i>Tip:</i> Bạn gõ tắt + không dấu thoải mái, mình trả lời cho bạn có dấu cho dễ đọc nè 😚`
  );
}

/* =========================
 * SESSION handler
 * ========================= */
async function handleSessionInput(chatId, userName, text) {
  const sess = getSession(chatId);
  if (!sess) return false;

  // RESET
  if (sess.flow === "reset" && sess.step === "pass") {
    clearSession(chatId);
    if (text !== RESET_PASS) {
      await send(chatId, `Nhập sai rồi bạn iu ơi ^^  <i>(Nếu cần thì hỏi admin xin pass nha 😝)</i>`, { reply_markup: rightKb() });
      return true;
    }
    await send(chatId, `⏳ <b>Đang xóa sạch dữ liệu...</b> (rụng tim nhẹ 😵‍💫)`, { reply_markup: rightKb() });
    await resetAllData();
    await send(chatId, `🎉 <b>Done!</b> Dữ liệu sạch bong rồi nè. Chạy lại từ đầu thôi~`, { reply_markup: mainKb() });
    return true;
  }

  // BUY LOT
  if (sess.flow === "buy_lot" && sess.step === "sentence") {
    const parsed = parseBuySentence(text);
    if (!parsed || parsed.incomplete) {
      await send(chatId, `Bạn gõ kiểu: <code>mua 3ss 50k</code> / <code>mua ip 35k uri</code> nha~`, { reply_markup: leftKb() });
      return true;
    }
    sess.data = parsed;

    if (parsed.wallet) {
      sess.step = "note";
      setSession(chatId, sess);
      const modelText = parsed.model && parsed.model !== "Unknown" ? `<b>${escapeHtml(parsed.model)}</b>` : "";
      await send(
        chatId,
        `Okie 😚 <b>Mua lô</b> <code>${parsed.qty}</code> máy ${modelText ? modelText : ""}, tổng <b>${moneyWON(parsed.totalPrice)}</b>\nVí: <code>${escapeHtml(parsed.wallet.toUpperCase())}</code>\n\nNhập <i>note</i> (vd <code>Note4</code>) hoặc <code>-</code> để bỏ qua nha~`,
        { reply_markup: leftKb() }
      );
      return true;
    }

    sess.step = "wallet";
    setSession(chatId, sess);
    const modelText = parsed.model && parsed.model !== "Unknown" ? `<b>${escapeHtml(parsed.model)}</b>` : "";
    await send(
      chatId,
      `Okie 😚 <b>Mua lô</b> <code>${parsed.qty}</code> máy ${modelText ? modelText : ""}, tổng <b>${moneyWON(parsed.totalPrice)}</b>\n\nTính tiền <b>ví nào</b>? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
    return true;
  }

  if (sess.flow === "buy_lot" && sess.step === "wallet") {
    const wallet = parseWalletShortcut(text) || normalizeForParse(text).trim();
    const w = ["hana", "uri", "kt", "tm"].includes(wallet) ? wallet : "";
    if (!w) {
      await send(chatId, `Ví chưa đúng á 😝 Nhập <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code> nha~`, { reply_markup: leftKb() });
      return true;
    }
    sess.data.wallet = w;
    sess.step = "note";
    setSession(chatId, sess);
    await send(chatId, `Nhập <i>note</i> (vd <code>Note4</code>) hoặc <code>-</code> để bỏ qua nha~`, { reply_markup: leftKb() });
    return true;
  }

  if (sess.flow === "buy_lot" && sess.step === "note") {
    const note = String(text || "").trim();
    const extra = note === "-" ? "" : note;

    const finalNote = normalizeSpaces([sess.data.note, extra].filter(Boolean).join(" | "));
    const r = await addLot({
      qty: sess.data.qty,
      model: sess.data.model,
      total_price: Math.round(sess.data.totalPrice),
      wallet: sess.data.wallet,
      note: finalNote,
    });

    clearSession(chatId);

    const modelText = sess.data.model && sess.data.model !== "Unknown" ? escapeHtml(sess.data.model) : "";
    const modelLine = modelText ? `Mua: <code>${sess.data.qty}</code> máy <b>${modelText}</b>\n` : `Mua: <code>${sess.data.qty}</code> máy\n`;

    const html =
      `✅ <b>Xong rồi nè</b> 🥳\n` +
      `Tạo lô: <code>${escapeHtml(displayLot(r.lot))}</code>\n` +
      modelLine +
      `Tổng: <b>${moneyWON(Math.round(sess.data.totalPrice))}</b>\n` +
      `Ví: <code>${escapeHtml(String(sess.data.wallet || "").toUpperCase())}</code>`;

    await send(chatId, html, { reply_markup: leftKb() });
    return true;
  }

  // SELL
  if (sess.flow === "sell" && sess.step === "sentence") {
    const parsed = parseSellSentence(text);
    if (!parsed || parsed.incomplete) {
      await send(chatId, `Bạn gõ: <code>ban 2 ss 80k ma01 tm</code> nha~`, { reply_markup: leftKb() });
      return true;
    }
    sess.data = parsed;

    if (parsed.wallet) {
      clearSession(chatId);
      await sellFromLot({ chatId, lot: parsed.lot, qty: parsed.qty, totalPrice: parsed.totalPrice, wallet: parsed.wallet });
      return true;
    }

    sess.step = "wallet";
    setSession(chatId, sess);
    await send(
      chatId,
      `Bạn đang <b>bán</b> lô <code>${escapeHtml(displayLot(parsed.lot))}</code> x<code>${parsed.qty}</code>, tiền <b>${moneyWON(parsed.totalPrice)}</b>\n\nTiền về <b>ví nào</b>? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
    return true;
  }

  if (sess.flow === "sell" && sess.step === "wallet") {
    const wallet = parseWalletShortcut(text) || normalizeForParse(text).trim();
    const w = ["hana", "uri", "kt", "tm"].includes(wallet) ? wallet : "";
    if (!w) {
      await send(chatId, `Ví chưa đúng á 😝 Nhập <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code> nha~`, { reply_markup: leftKb() });
      return true;
    }
    const d = sess.data;
    clearSession(chatId);
    await sellFromLot({ chatId, lot: d.lot, qty: d.qty, totalPrice: d.totalPrice, wallet: w });
    return true;
  }

  // WALLET EDIT
  if (sess.flow === "wallet_edit" && sess.step === "wallet") {
    const wallet = parseWalletShortcut(text) || normalizeForParse(text).trim();
    const w = ["hana", "uri", "kt", "tm"].includes(wallet) ? wallet : "";
    if (!w) {
      await send(chatId, `Ví chưa đúng á 😝 Nhập <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code> nha~`, { reply_markup: rightKb() });
      return true;
    }
    sess.data = { wallet: w };
    sess.step = "amount";
    setSession(chatId, sess);
    await send(chatId, `Okie. Bạn nhập <b>số dư mới</b> cho ví <code>${escapeHtml(w.toUpperCase())}</code> (vd <code>120k</code>) nha~`, { reply_markup: rightKb() });
    return true;
  }

  if (sess.flow === "wallet_edit" && sess.step === "amount") {
    const amt = extractMoneyFromText(text);
    if (amt == null) {
      await send(chatId, `Nhập số dư kiểu <code>120k</code> nha bạn iu~`, { reply_markup: rightKb() });
      return true;
    }
    const w = sess.data.wallet;
    clearSession(chatId);

    const r = await setWalletBalanceAbsolute(w, amt, chatId);
    const html =
      `✏️ <b>SỬA SỐ DƯ VÍ</b> <code>${escapeHtml(w.toUpperCase())}</code>\n\n` +
      `Cũ: <b>${moneyWON(r.current)}</b>\n` +
      `Mới: <b>${moneyWON(r.newBalance)}</b>\n` +
      `Bù chênh: <code>${r.delta >= 0 ? "+" : ""}${moneyWON(r.delta)}</code>\n\n` +
      `<i>(Bot ghi 1 dòng “adjust” để cân lại số dư nha 😚)</i>`;

    await send(chatId, html, { reply_markup: rightKb() });
    return true;
  }

  // REVENUE EDIT (TOTAL)
  if (sess.flow === "revenue_edit" && sess.step === "amount") {
    const amt = extractMoneyFromText(text);
    if (amt == null) {
      await send(chatId, `Nhập số kiểu <code>500k</code> nha bạn iu~`, { reply_markup: rightKb() });
      return true;
    }
    clearSession(chatId);
    const r = await setTotalRevenueAbsolute(amt, chatId, userName);
    const html =
      `✏️ <b>SỬA SỐ DƯ TỔNG DOANH THU</b>\n\n` +
      `Cũ: <b>${moneyWON(r.current)}</b>\n` +
      `Mới: <b>${moneyWON(r.newTotal)}</b>\n` +
      `Bù chênh: <code>${r.delta >= 0 ? "+" : ""}${moneyWON(r.delta)}</code>\n\n` +
      `<i>(Bot ghi 1 dòng “revenue_adjust” để cân lại tổng nha 😚)</i>`;
    await send(chatId, html, { reply_markup: rightKb() });
    return true;
  }

  // RESOLVE ASK GAME
  if (sess.flow === "resolve_ask_game" && sess.step === "pick") {
    const g = normalizeForParse(text).trim();
    if (!["hq", "qr", "db"].includes(g)) {
      await send(chatId, `Chọn <code>hq</code> / <code>qr</code> / <code>db</code> nha 😚`, { reply_markup: kb([[{ text: "hq" }, { text: "qr" }, { text: "db" }], [{ text: "⬅️ Back" }]]) });
      return true;
    }
    const { lot, segments, askIdx } = sess.data;
    segments[askIdx].game = g;
    clearSession(chatId);
    await applyLotResolve({ chatId, lot, segments });
    return true;
  }

  // PHONE LIST NAV
  if (sess.flow === "phone_list" && sess.step === "nav") {
    const t = String(text || "").trim();
    const data = sess.data || {};
    if (t === "⬅️ Prev") {
      const nextPage = Math.max(1, (data.page || 1) - 1);
      await renderPhoneList(chatId, data.lot || "", nextPage);
      return true;
    }
    if (t === "➡️ Next") {
      const nextPage = Math.min(data.totalPages || 1, (data.page || 1) + 1);
      await renderPhoneList(chatId, data.lot || "", nextPage);
      return true;
    }
    if (t === "🧪 Xem Tất Cả") {
      await renderPhoneList(chatId, "", 1);
      return true;
    }
    if (t === "🔎 Lọc theo lô") {
      setSession(chatId, { flow: "phone_list", step: "filter", data: { page: 1 } });
      await send(chatId, `Nhập mã lô (vd <code>ma01</code>) để lọc nha 😚`, { reply_markup: kb([[{ text: "⬅️ Back" }]]) });
      return true;
    }
    return false;
  }

  if (sess.flow === "phone_list" && sess.step === "filter") {
    const lot = parseLotCode(text);
    if (!lot) {
      await send(chatId, `Nhập kiểu <code>ma01</code> nha 😝`, { reply_markup: kb([[{ text: "⬅️ Back" }]]) });
      return true;
    }
    await renderPhoneList(chatId, lot, 1);
    return true;
  }

  return false;
}

/* =========================
 * Cron placeholder
 * ========================= */
cron.schedule("*/30 * * * *", async () => {});

/* =========================
 * Main handler
 * ========================= */
async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  if (!chatId) return;
  const userName = msg.from?.first_name || "User";
  const text = String(msg.text || "").trim();
  if (!text) return;

  if (text === "/start") {
    clearSession(chatId);
    await send(chatId, `✅ <b>Bot sẵn sàng</b> rồi nè <i>(${escapeHtml(VERSION)})</i>`, { reply_markup: mainKb() });
    return;
  }
  if (text === "/help") {
    await send(chatId, helpText(), { reply_markup: mainKb() });
    return;
  }

  // menus
  if (text === "⬅️ Menu") {
    clearSession(chatId);
    await send(chatId, `⬅️ <b>Menu Trái</b> đây nè~ <i>(nhập liệu siêu nhanh)</i> ⚡`, { reply_markup: leftKb() });
    return;
  }
  if (text === "➡️ Menu") {
    clearSession(chatId);
    await send(chatId, `➡️ <b>Menu Phải</b> đây nè~ <i>(báo cáo + ví + phân tích)</i> 📊`, { reply_markup: rightKb() });
    return;
  }
  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, `Về <b>menu chính</b> nha bạn iu~ 🏠`, { reply_markup: mainKb() });
    return;
  }

  // right menu
  if (text === "📊 Phân Tích") return analysisMachines(chatId);
  if (text === "💰 Tổng Doanh Thu") return reportTotalRevenue(chatId);
  if (text === "📅 Tháng Này") return reportThisMonth(chatId);
  if (text === "⏮️ Tháng Trước") return reportLastMonth(chatId);
  if (text === "📊 Thống Kê Game") return reportStatsGames(chatId);
  if (text === "💼 Xem Ví") return reportWallets(chatId);
  if (text === "📘 Hướng Dẫn") return send(chatId, helpText(), { reply_markup: rightKb() });

  if (text === "✏️ Sửa Số Dư Ví") {
    setSession(chatId, { flow: "wallet_edit", step: "wallet", data: {} });
    await send(chatId, `✏️ <b>Sửa số dư ví</b>\nBạn chọn ví: <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code>`, { reply_markup: rightKb() });
    return;
  }

  if (text === "✏️ Sửa Số Dư Tổng Doanh Thu") {
    setSession(chatId, { flow: "revenue_edit", step: "amount", data: {} });
    await send(chatId, `✏️ <b>Sửa số dư tổng doanh thu</b>\nNhập <b>số tổng mới</b> (vd <code>500k</code>) nha~`, { reply_markup: rightKb() });
    return;
  }

  if (text === "🧠 Smart Parse: Bật/Tắt") {
    const on = await toggleSmartParse();
    await send(chatId, `🧠 Smart Parse hiện đang: <b>${on ? "BẬT ✅" : "TẮT ❌"}</b>`, { reply_markup: rightKb() });
    return;
  }

  if (text === "🧨 Xóa Sạch Dữ Liệu") {
    setSession(chatId, { flow: "reset", step: "pass" });
    await send(chatId, `⚠️ <b>Khu vực nguy hiểm</b> nha bạn iu 😵‍💫\n🔐 Vui lòng điền pass để <b>XÓA SẠCH</b> dữ liệu ^^`, { reply_markup: rightKb() });
    return;
  }

  // left menu
  if (text === "📱 Mua Máy (Lô)") {
    setSession(chatId, { flow: "buy_lot", step: "sentence", data: {} });
    await send(chatId, `📱 <b>Mua Máy (Lô)</b>\nBạn gõ: <code>mua 3ss 50k</code> hoặc <code>mua ip 35k uri</code> nha~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "💸 Bán Máy") {
    setSession(chatId, { flow: "sell", step: "sentence", data: {} });
    await send(chatId, `💸 <b>Bán Máy</b>\nBạn gõ: <code>ban 2 ss 80k ma01 tm</code> nha~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "🧪 Kiểm Tra Máy (Tất cả)") return listLotsPretty(chatId, "all");
  if (text === "🧪 20 Lô Gần Nhất") return listLotsPretty(chatId, "20");

  if (text === "📋 Danh Sách Máy") {
    return renderPhoneList(chatId, "", 1);
  }

  // session
  if (await handleSessionInput(chatId, userName, text)) return;

  // resolve lot (new parser)
  const lotCmd = parseLotResolve(text);
  if (lotCmd && lotCmd.segments && lotCmd.segments.length > 0) {
    await applyLotResolve({ chatId, lot: lotCmd.lot, segments: lotCmd.segments });
    return;
  }

  // sell direct (smart)
  const sell = parseSellSentence(text);
  if (sell && !sell.incomplete) {
    if (sell.wallet) {
      await sellFromLot({ chatId, lot: sell.lot, qty: sell.qty, totalPrice: sell.totalPrice, wallet: sell.wallet });
      return;
    }
    setSession(chatId, { flow: "sell", step: "wallet", data: sell });
    await send(
      chatId,
      `Mình hiểu bạn đang <b>bán</b> lô <code>${escapeHtml(displayLot(sell.lot))}</code> x<code>${sell.qty}</code> giá <b>${moneyWON(sell.totalPrice)}</b>\n\nTiền về ví nào? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
    return;
  }

  // quick revenue (manual)
  const norm = normalizeForParse(text);
  const game = detectGameFromText(norm);
  const amt = extractMoneyFromText(text);

  if (game && amt != null) {
    const g = game === "other" ? "other" : game;
    const type = g === "other" ? "other" : "manual";
    await addGameRevenue({ game: g, type, amount: amt, note: text, chatId, userName });
    await send(chatId, `✅ <b>Đã ghi doanh thu</b> <code>${escapeHtml(g.toUpperCase())}</code>: <b>${moneyWON(amt)}</b>`, { reply_markup: mainKb() });
    return;
  }

  // Smart Parse buy lot without menu
  if (await isSmartParseEnabled()) {
    const buy = parseBuySentence(text);
    if (buy && !buy.incomplete) {
      if (buy.wallet) {
        setSession(chatId, { flow: "buy_lot", step: "note", data: buy });
        const modelText = buy.model && buy.model !== "Unknown" ? `<b>${escapeHtml(buy.model)}</b>` : "";
        await send(
          chatId,
          `Okie 😚 <b>Mua lô</b> <code>${buy.qty}</code> máy ${modelText ? modelText : ""}, tổng <b>${moneyWON(buy.totalPrice)}</b>\nVí: <code>${escapeHtml(buy.wallet.toUpperCase())}</code>\nNhập note (vd <code>Note4</code>) hoặc <code>-</code> nha~`,
          { reply_markup: leftKb() }
        );
        return;
      }
      setSession(chatId, { flow: "buy_lot", step: "wallet", data: buy });
      const modelText = buy.model && buy.model !== "Unknown" ? `<b>${escapeHtml(buy.model)}</b>` : "";
      await send(
        chatId,
        `Mình hiểu bạn mua lô <code>${buy.qty}</code> máy ${modelText ? modelText : ""}, tổng <b>${moneyWON(buy.totalPrice)}</b>\nTính tiền ví nào? (<code>hana/uri/kt/tm</code>)`,
        { reply_markup: leftKb() }
      );
      return;
    }
  }

  // unknown
  await send(
    chatId,
    `Nhập sai rồi bạn iu ơi ^^\nVào ➡️ <b>Menu</b> → <b>📘 Hướng Dẫn</b> nha~\n<i>(hoặc bật 🧠 Smart Parse để mình hiểu bạn hơn 😚)</i>`,
    { reply_markup: mainKb(), __raw: true }
  );
}

/* =========================
 * Webhook
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
 * Boot
 * ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ BOT READY on ${PORT} | ${VERSION}`);
});
