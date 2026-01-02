// src/index.js
/**
 * =====================================================================================
 *  TIKTOK_LITE_BOT — LOT MODE ✅ (MA01...) + Smart Parse + Wallet + Sell + Cute + WON
 * =====================================================================================
 *  ✅ Buy LOT: "mua 2ss 88k" => qty=2, model=Samsung, TOTAL=88k, ask wallet if missing
 *  ✅ Buy with wallet shortcut: "mua lg35k hn" => wallet=hana auto, no ask
 *  ✅ LOT code: MA01, MA02...
 *  ✅ Resolve LOT: "ma 01 loi 1 qr tach 1" (VN có/không dấu)
 *  ✅ Sell:
 *      - "ban ss 50k ma 01" => bán 1 máy trong lô MA01, tiền + vào ví user chọn
 *      - "ban 2 ss 80k ma01" => bán 2 máy (tức bán hết lô 2 máy), 80k là TỔNG tiền thu về
 *
 *  REQUIRED ENV:
 *   - BOT_TOKEN
 *   - GOOGLE_SHEET_ID
 *   - GOOGLE_APPLICATION_CREDENTIALS (path to SA json)
 */

import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * SECTION 1 — Env & constants
 * ========================= */
const VERSION = "LOT-MAxx-SMARTPARSE-WALLET-SELL-CUTE-WON";
const BOT_TOKEN = process.env.BOT_TOKEN;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "/etc/secrets/google-service-account.json";

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");

const TELEGRAM_API = `https://api.telegram.org/bot${BOT_TOKEN}`;
const RESET_PASS = "12345";

/* =========================
 * SECTION 2 — Money (WON)
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
 * SECTION 4 — Telegram helpers + Cute
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
  let s = s0
    .replaceAll("✅ Đã", "✅ Xong rồi nè")
    .replaceAll("❌ Sai", "❌ Ôi hông đúng rồi bạn iu")
    .replaceAll("⚠️", "⚠️ Ui ui")
    .replaceAll("Không hiểu", "Nhập sai rồi bạn iu ơi ^^")
    .replaceAll("Nhập lại", "Bạn nhập lại giúp mình nha~")
    .replaceAll("Nhập ", "Bạn nhập ");

  const tails = [" 😚", " 🫶", " ✨", " ^^", " 😝", " 🤭", " 💖"];
  const endsEmoji = /[\u{1F300}-\u{1FAFF}\u2600-\u27BF]$/u.test(s.trim());
  const endsCaret = /\^+$/.test(s.trim());
  if (!endsEmoji && !endsCaret) {
    const idx = (s.length + 3) % tails.length;
    s += tails[idx];
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
 * SECTION 5 — Keyboards
 * ========================= */
function kb(rows) {
  return { keyboard: rows, resize_keyboard: true, one_time_keyboard: false, is_persistent: true };
}
function mainKb() {
  return kb([[{ text: "⬅️ Menu" }, { text: "➡️ Menu" }]]);
}
function leftKb() {
  return kb([
    [{ text: "📱 Mua Máy (Lô)" }, { text: "💸 Bán Máy" }],
    [{ text: "🧪 Kiểm Tra Máy" }],
    [{ text: "⚽ Thu Đá Bóng" }, { text: "🎁 Thu Hộp Quà" }],
    [{ text: "🔳 Thu QR" }, { text: "➕ Thu Khác" }],
    [{ text: "⬅️ Back" }],
  ]);
}
function rightKb() {
  return kb([
    [{ text: "💰 Tổng Doanh Thu" }],
    [{ text: "📅 Tháng Này" }, { text: "⏮️ Tháng Trước" }],
    [{ text: "📊 Thống Kê Game" }],
    [{ text: "📱 Lời/Lỗ Máy" }],
    [{ text: "💼 Xem Ví" }],
    [{ text: "📘 Hướng Dẫn" }],
    [{ text: "🧠 Smart Parse: Bật/Tắt" }],
    [{ text: "🧨 Xóa Sạch Dữ Liệu" }],
    [{ text: "⬅️ Back" }],
  ]);
}

/* =========================
 * SECTION 6 — Google Sheets
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
 * SECTION 7 — Text normalize + parse money
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
  // tách chữ-số dính liền: "lg35k" -> "lg 35k"
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
 * SECTION 8 — SETTINGS
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
 * SECTION 9 — Payout config (default)
 * ========================= */
async function getPayouts() {
  const hq = parseMoney((await getSetting("PAYOUT_HQ")) || "100k") ?? 100000;
  const qr = parseMoney((await getSetting("PAYOUT_QR")) || "57k") ?? 57000;
  const db = parseMoney((await getSetting("PAYOUT_DB")) || "100k") ?? 100000;
  return { hq, qr, db };
}

/* =========================
 * SECTION 10 — Wallets
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

  // fallback nếu chưa tạo sheet WALLETS
  if (wallets.length === 0) {
    return [
      { code: "uri", name: "URI" },
      { code: "hana", name: "HANA" },
      { code: "kt", name: "Viễn Thông KT" },
      { code: "tm", name: "TIỀN MẶT" },
    ];
  }

  // ensure tm exists
  if (!wallets.find((w) => w.code === "tm")) wallets.push({ code: "tm", name: "TIỀN MẶT" });

  // ensure kt name
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
    ref_id: String(r[5] || "").trim(),
    type: String(r[2] || "").trim().toLowerCase(),
  }));
}

async function addWalletLog({ wallet, type, amount, ref_type, ref_id, note, chatId }) {
  await appendValues("WALLET_LOG!A1", [[nowIso(), wallet, type, amount, ref_type || "", ref_id || "", note || "", String(chatId || "")]]);
  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "wallet_log_add", wallet, type, amount, ref_id || ""]]);
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

/* =========================
 * SECTION 11 — GAME_REVENUE
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

/* =========================
 * SECTION 12 — LOTS + PHONES (LOT MODE)
 * =========================
 * LOTS:
 *  A lot_code (MA01)
 *  B created_at
 *  C qty
 *  D model
 *  E total_price
 *  F unit_price
 *  G wallet
 *  H note
 *
 * PHONES:
 *  A phone_id   (MA01-1, MA01-2...)
 *  B lot_code
 *  C created_at
 *  D unit_price
 *  E status     (new/ok/tach/sold)
 *  F result_game (hq/qr/db/none)
 *  G note
 */
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
 * SECTION 13 — Parse "mua ..." (LOT) with wallet shortcut
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
  if (totalPrice == null) return { incomplete: true, reason: "missing_price" };

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
    .replace(/\b(dt|đt|dien thoai|dien-thoai)\b/g, "")
    .replace(/\b\d+\s*(ss|ip|lg)\b/g, "")
    .replace(/\bss\b|\bsamsung\b|\bip\b|\biphone\b|\blg\b/g, "")
    .replace(/\bhn\b|\bhana\b|\buri\b|\bkt\b|\btm\b|\btien mat\b|\btienmat\b/g, "")
    .replace(/₩/g, "")
    .replace(/\d[\d,]*(?:\.\d+)?\s*k\b|\d[\d,]*(?:\.\d+)?\b/g, "");
  note = normalizeSpaces(note);

  return { qty, model, totalPrice, wallet, note };
}

/* =========================
 * SECTION 14 — Parse "bán ..." (SELL)
 * ========================= */
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
  if (!lot) return { incomplete: true, reason: "missing_lot" };

  const totalPrice = extractMoneyFromText(text);
  if (totalPrice == null) return { incomplete: true, reason: "missing_price" };

  let qty = 1;
  const mQty = norm.match(/\bban\s+(\d+)\b/);
  if (mQty) qty = Number(mQty[1]) || 1;
  qty = Math.max(1, Math.min(50, qty));

  const model = detectModelToken(norm);
  const wallet = parseWalletShortcut(text);

  return { lot, qty, totalPrice, model, wallet };
}

/* =========================
 * SECTION 15 — Parse "mã 01 ..." resolve lot results (profit/tach/hue)
 * ========================= */
function detectGameToken(norm) {
  const t = ` ${norm} `;
  if (t.includes(" hq ") || t.includes(" hopqua ") || t.includes(" hop qua ")) return "hq";
  if (t.includes(" qr ")) return "qr";
  if (t.includes(" db ") || t.includes(" dabong ") || t.includes(" da bong ")) return "db";
  return "";
}

function parseLotResolve(text) {
  const norm = normalizeForParse(text);
  const lot = parseLotCode(text);
  if (!lot) return null;

  const tokens = norm.split(" ").filter(Boolean);
  const segments = [];
  let i = 0;

  while (i < tokens.length) {
    const tk = tokens[i];

    const isLoi = tk === "loi" || tk === "lai" || tk === "an" || tk === "duoc" || tk === "ok";
    const isTach = tk === "tach" || tk === "chet" || tk === "tac";
    const isHue = tk === "hue" || tk === "hoa" || tk === "thuvon" || tk === "thu" || tk === "von";

    if (isLoi || isTach || isHue) {
      const kind = isLoi ? "loi" : isTach ? "tach" : "hue";
      let count = 1;

      if (i + 1 < tokens.length && tokens[i + 1].match(/^\d+$/)) {
        count = Number(tokens[i + 1]);
        i += 2;
      } else {
        i += 1;
      }

      if (i < tokens.length && (tokens[i] === "may" || tokens[i] === "dt")) i++;

      let game = "";
      if (kind === "loi") {
        const rest = tokens.slice(i, i + 4).join(" ");
        game = detectGameToken(rest) || detectGameToken(tokens[i] || "");
        if (game) i++;
      }

      segments.push({ kind, count: Math.max(0, Math.min(50, count)), game: game || "" });
      continue;
    }
    i++;
  }

  if (segments.length === 0) {
    if (norm.includes("tach")) segments.push({ kind: "tach", count: 1, game: "" });
    else if (norm.includes("hue") || norm.includes("hoa") || norm.includes("thuvon")) segments.push({ kind: "hue", count: 1, game: "" });
    else if (norm.includes("loi") || norm.includes("lai")) segments.push({ kind: "loi", count: 1, game: "" });
  }

  return { lot, segments };
}

/* =========================
 * SECTION 16 — Apply LOT resolve to PHONES + revenue
 * ========================= */
async function applyLotResolve({ chatId, userName, lot, segments }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Mình không thấy mã lô ${lot} á (check lại giúp mình nha)`, { reply_markup: leftKb() });
    return true;
  }

  const payouts = await getPayouts();

  // pick remaining phones first: status=new preferred, ignore sold
  const pick = (n) => {
    const pending = lotPhones.filter((p) => p.status === "new");
    const pool = pending.length > 0 ? pending : lotPhones.filter((p) => p.status !== "sold");
    return pool.slice(0, n).map((p) => p.phone_id);
  };

  let totalRev = 0;
  let totalOk = 0;
  let totalTach = 0;
  let totalHue = 0;

  for (const seg of segments) {
    const ids = pick(seg.count);
    if (ids.length === 0) continue;

    if (seg.kind === "tach") {
      for (const id of ids) await updatePhoneRowById(id, { status: "tach", game: "none" });
      totalTach += ids.length;
      continue;
    }

    if (seg.kind === "hue") {
      for (const id of ids) await updatePhoneRowById(id, { status: "ok", game: "none" });
      totalHue += ids.length;
      continue;
    }

    const game = seg.game || "hq";
    const per = payouts[game] ?? payouts.hq;

    for (const id of ids) await updatePhoneRowById(id, { status: "ok", game });
    totalOk += ids.length;

    const revenue = ids.length * per;
    totalRev += revenue;

    await addGameRevenue({
      game,
      type: "lot_profit",
      amount: revenue,
      note: `LOT:${lot} x${ids.length} ${game}`,
      chatId,
      userName,
    });
  }

  const msg =
    `🧾 CHỐT LÔ ${lot} xong rồi bạn iu 🥳\n` +
    `✅ OK: ${totalOk}\n` +
    `😵 Tạch: ${totalTach}\n` +
    `😌 Huề/Thu vốn: ${totalHue}\n` +
    (totalRev > 0 ? `💰 Doanh thu cộng: ${moneyWON(totalRev)}\n` : ``) +
    `\nTip: "ma 01 loi 1 qr tach 1" / "ma01 loi 2 hq"`;

  await send(chatId, msg, { reply_markup: leftKb() });
  return true;
}

/* =========================
 * SECTION 17 — SELL apply
 * ========================= */
async function sellFromLot({ chatId, lot, qty, totalPrice, wallet }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy lô ${lot} luôn á (bạn check lại mã nha)`, { reply_markup: leftKb() });
    return true;
  }

  // pick sellable: prefer new then ok; never sell sold
  const sellable = lotPhones
    .filter((p) => p.status !== "sold")
    .sort((a, b) => {
      const rank = (s) => (s === "new" ? 0 : s === "ok" ? 1 : s === "tach" ? 2 : 3);
      return rank(a.status) - rank(b.status);
    });

  const ids = sellable.slice(0, qty).map((p) => p.phone_id);
  if (ids.length === 0) {
    await send(chatId, `Lô ${lot} hình như bán hết sạch rồi 😝 (không còn máy để bán nữa)`, { reply_markup: leftKb() });
    return true;
  }

  for (const id of ids) await updatePhoneRowById(id, { status: "sold", game: "none" });

  // totalPrice là TỔNG tiền thu về (đúng yêu cầu)
  await addWalletLog({
    wallet,
    type: "machine_sell",
    amount: Math.abs(Math.round(totalPrice)),
    ref_type: "lot",
    ref_id: lot,
    note: `SELL x${ids.length}`,
    chatId,
  });

  await send(
    chatId,
    `💸 BÁN XONG RỒI NÈ 🥳\nLô: ${lot}\nBán: ${ids.length} máy\nTiền về ví ${wallet.toUpperCase()}: ${moneyWON(Math.round(totalPrice))}\n\n(Chốt đơn mượt ghê 😝)`,
    { reply_markup: leftKb() }
  );
  return true;
}

/* =========================
 * SECTION 18 — Reports
 * ========================= */
async function reportTotalRevenue(chatId) {
  const rows = await readGameRevenue();
  const sum = rows.reduce((a, b) => a + b.amount, 0);
  await send(chatId, `💰 TỔNG DOANH THU (WON)\n= ${moneyWON(sum)}`, { reply_markup: rightKb() });
}
async function reportThisMonth(chatId) {
  const m = dayjs().format("YYYY-MM");
  const rows = await readGameRevenue();
  const sum = rows.filter((x) => monthKey(x.ts) === m).reduce((a, b) => a + b.amount, 0);
  await send(chatId, `📅 DOANH THU THÁNG ${m}\n= ${moneyWON(sum)}`, { reply_markup: rightKb() });
}
async function reportLastMonth(chatId) {
  const m = dayjs().subtract(1, "month").format("YYYY-MM");
  const rows = await readGameRevenue();
  const sum = rows.filter((x) => monthKey(x.ts) === m).reduce((a, b) => a + b.amount, 0);
  await send(chatId, `⏮️ DOANH THU THÁNG ${m}\n= ${moneyWON(sum)}`, { reply_markup: rightKb() });
}
async function reportWallets(chatId) {
  const balances = await walletBalances();
  let total = 0;
  const lines = balances.map((b) => {
    total += b.balance;
    return `• ${b.name} (${b.code}): ${moneyWON(b.balance)}`;
  });
  await send(chatId, `💼 SỐ DƯ CÁC VÍ\n\n${lines.join("\n")}\n\nTổng: ${moneyWON(total)}`, { reply_markup: rightKb() });
}
async function reportStatsGames(chatId) {
  const rev = await readGameRevenue();
  const dbSum = rev.filter((x) => x.game === "db").reduce((a, b) => a + b.amount, 0);
  const hqSum = rev.filter((x) => x.game === "hq").reduce((a, b) => a + b.amount, 0);
  const qrSum = rev.filter((x) => x.game === "qr").reduce((a, b) => a + b.amount, 0);
  await send(
    chatId,
    `📊 THỐNG KÊ GAME (WON)\n\n⚽ DB: ${moneyWON(dbSum)}\n🎁 HQ: ${moneyWON(hqSum)}\n🔳 QR: ${moneyWON(qrSum)}`,
    { reply_markup: rightKb() }
  );
}

async function reportMachinePnL(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const rev = await readGameRevenue();

  const revByLot = new Map();
  for (const r of rev) {
    const m = String(r.note || "").match(/LOT:(MA\d+)/i);
    if (!m) continue;
    const lot = m[1].toUpperCase();
    revByLot.set(lot, (revByLot.get(lot) || 0) + r.amount);
  }

  let totalBuy = 0;
  let totalRev = 0;
  for (const l of lots) {
    totalBuy += l.total;
    totalRev += revByLot.get(l.lot) || 0;
  }
  const net = totalRev - totalBuy;

  const ok = phones.filter((p) => p.status === "ok").length;
  const tach = phones.filter((p) => p.status === "tach").length;
  const neu = phones.filter((p) => p.status === "new").length;
  const sold = phones.filter((p) => p.status === "sold").length;

  await send(
    chatId,
    `📱 LỜI/LỖ MÁY (WON)\n\n📦 Lô: ${lots.length}\n📱 Máy: ${phones.length}\n• New: ${neu}\n• OK: ${ok}\n• Tạch: ${tach}\n• Sold: ${sold}\n\n💸 Tổng mua: ${moneyWON(totalBuy)}\n💰 Tổng doanh thu máy (lot_profit): ${moneyWON(totalRev)}\n🧮 Net: ${moneyWON(net)}`,
    { reply_markup: rightKb() }
  );
}

async function listLotsPretty(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const rev = await readGameRevenue();

  const revByLot = new Map();
  for (const r of rev) {
    const m = String(r.note || "").match(/LOT:(MA\d+)/i);
    if (!m) continue;
    const lot = m[1].toUpperCase();
    revByLot.set(lot, (revByLot.get(lot) || 0) + r.amount);
  }

  if (lots.length === 0) {
    await send(chatId, "Chưa có lô nào hết á 😝 Bấm 📱 Mua Máy (Lô) để tạo lô nha~", { reply_markup: leftKb() });
    return;
  }

  const sorted = [...lots].sort((a, b) => (a.ts < b.ts ? 1 : -1)).slice(0, 20);

  const lines = sorted.map((l) => {
    const ps = phones.filter((p) => p.lot === l.lot);
    const ok = ps.filter((p) => p.status === "ok").length;
    const tach = ps.filter((p) => p.status === "tach").length;
    const neu = ps.filter((p) => p.status === "new").length;
    const sold = ps.filter((p) => p.status === "sold").length;

    const hq = ps.filter((p) => p.game === "hq").length;
    const qr = ps.filter((p) => p.game === "qr").length;
    const db = ps.filter((p) => p.game === "db").length;

    const r = revByLot.get(l.lot) || 0;

    let st = "⏳ Chưa chốt";
    if (tach + sold === l.qty) st = "😵 Tạch/Sold hết";
    else if (ok + sold === l.qty && neu === 0 && tach === 0) st = "✅ OK/Sold hết";
    else if (ok > 0 || tach > 0 || sold > 0) st = "🧩 Có biến động";

    return (
      `• ${l.lot}: Mua ${l.qty} máy ${l.model} | Tổng ${moneyWON(l.total)} | Ví ${String(l.wallet || "").toUpperCase()}\n` +
      `  Trạng thái: ${st} (new:${neu} ok:${ok} tạch:${tach} sold:${sold})\n` +
      `  Game: HQ:${hq} QR:${qr} DB:${db} | Doanh thu: ${moneyWON(r)}`
    );
  });

  await send(
    chatId,
    `🧪 DANH SÁCH LÔ MÁY (20 lô gần nhất)\n\n${lines.join("\n\n")}\n\nChốt lô: "ma 01 loi 1 qr tach 1"\nBán: "ban 2 ss 80k ma01 tm"`,
    { reply_markup: leftKb() }
  );
}

/* =========================
 * SECTION 19 — Revenue quick (db/hq/qr/them)
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
 * SECTION 20 — Reset
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
 * SECTION 21 — Sessions
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
 * SECTION 22 — Help
 * ========================= */
function helpText() {
  return (
    `📘 HƯỚNG DẪN (WON ₩)\n\n` +
    `✅ Mua lô máy (TỔNG TIỀN):\n` +
    `- mua 2ss 88k          (2 Samsung, tổng 88k)\n` +
    `- mua ip 35k           (1 iPhone, tổng 35k)\n` +
    `- mua lg35k hn         (1 LG, tổng 35k, ví HANA)\n` +
    `- mua 2 dt ss 45k uri  (2 Samsung, tổng 45k, ví URI)\n\n` +
    `👉 Nếu bạn không ghi ví trong câu, bot sẽ hỏi “tính tiền ví nào?”\n` +
    `Ví tắt: hn/hana | uri | kt (Viễn Thông KT) | tm (tiền mặt)\n\n` +
    `✅ Bán máy (tiền + về ví):\n` +
    `- ban ss 50k ma 01      (bán 1 máy của MA01, hỏi ví)\n` +
    `- ban 2 ss 80k ma01 tm  (bán 2 máy, 80k là TỔNG tiền)\n\n` +
    `✅ Chốt kết quả theo mã lô:\n` +
    `- ma 01 loi 2 hq\n` +
    `- ma01 tach 2\n` +
    `- ma 01 loi 1 qr tach 1\n\n` +
    `✅ Thu nhập nhanh:\n` +
    `- db 100k / dabong 100k\n` +
    `- hq 200k / hopqua 200k\n` +
    `- qr 57k\n` +
    `- them 0.5k\n\n` +
    `🧠 Smart Parse: hiểu tiếng Việt có/không dấu, gõ lỏng lẻo vẫn hiểu 😝`
  );
}

/* =========================
 * SECTION 23 — Handle session input
 * ========================= */
async function handleSessionInput(chatId, userName, text) {
  const sess = getSession(chatId);
  if (!sess) return false;

  // RESET
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

  // BUY LOT
  if (sess.flow === "buy_lot" && sess.step === "sentence") {
    const parsed = parseBuySentence(text);
    if (!parsed || parsed.incomplete) {
      await send(chatId, "Nhập kiểu: `mua 2ss 88k` / `mua lg35k hn` nha bạn iu~", { reply_markup: leftKb() });
      return true;
    }

    sess.data = parsed;

    // wallet in sentence => skip ask
    if (parsed.wallet) {
      sess.step = "note";
      setSession(chatId, sess);
      await send(
        chatId,
        `Okie 😚 Mua lô ${parsed.qty} máy ${parsed.model}, tổng ${moneyWON(parsed.totalPrice)}.\nVí: ${parsed.wallet.toUpperCase()}\nNhập ghi chú thêm (hoặc '-' để bỏ qua) nha~`,
        { reply_markup: leftKb() }
      );
      return true;
    }

    sess.step = "wallet";
    setSession(chatId, sess);
    await send(chatId, `Okie 😚 Bạn mua lô ${parsed.qty} máy ${parsed.model}, tổng ${moneyWON(parsed.totalPrice)}.\nTính tiền ví nào? (hana/uri/kt/tm)`, {
      reply_markup: leftKb(),
    });
    return true;
  }

  if (sess.flow === "buy_lot" && sess.step === "wallet") {
    const wallet = parseWalletShortcut(text) || normalizeForParse(text).trim();
    const w = ["hana", "uri", "kt", "tm"].includes(wallet) ? wallet : "";
    if (!w) {
      await send(chatId, "Ví chưa đúng á 😝 Nhập hana / uri / kt / tm nha~", { reply_markup: leftKb() });
      return true;
    }
    sess.data.wallet = w;
    sess.step = "note";
    setSession(chatId, sess);
    await send(chatId, "Nhập ghi chú thêm (hoặc '-' để bỏ qua) nha~", { reply_markup: leftKb() });
    return true;
  }

  if (sess.flow === "buy_lot" && sess.step === "note") {
    const note = String(text || "").trim();
    const extra = note === "-" ? "" : note;

    const finalNote = normalizeSpaces([sess.data.model, sess.data.note, extra].filter(Boolean).join(" | "));
    const r = await addLot({
      qty: sess.data.qty,
      model: sess.data.model,
      total_price: Math.round(sess.data.totalPrice),
      wallet: sess.data.wallet,
      note: finalNote,
    });

    clearSession(chatId);

    await send(
      chatId,
      `✅ Xong rồi nè 🥳\nTạo lô: ${r.lot}\nMua ${sess.data.qty} máy ${sess.data.model}\nTổng: ${moneyWON(Math.round(sess.data.totalPrice))}\nVí: ${String(sess.data.wallet || "").toUpperCase()}\n\nChốt lô: "ma ${r.lot.slice(2)} loi 1 qr tach 1"\nBán: "ban 2 ss 80k ma${r.lot.slice(2)} tm"`,
      { reply_markup: leftKb() }
    );
    return true;
  }

  // SELL
  if (sess.flow === "sell" && sess.step === "sentence") {
    const parsed = parseSellSentence(text);
    if (!parsed || parsed.incomplete) {
      await send(chatId, "Bạn gõ kiểu: `ban ss 50k ma 01` hoặc `ban 2 ss 80k ma01 tm` nha~", { reply_markup: leftKb() });
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
    await send(chatId, `Bán lô ${parsed.lot} x${parsed.qty}, tiền ${moneyWON(parsed.totalPrice)}.\nTiền về ví nào? (hana/uri/kt/tm)`, { reply_markup: leftKb() });
    return true;
  }

  if (sess.flow === "sell" && sess.step === "wallet") {
    const wallet = parseWalletShortcut(text) || normalizeForParse(text).trim();
    const w = ["hana", "uri", "kt", "tm"].includes(wallet) ? wallet : "";
    if (!w) {
      await send(chatId, "Ví chưa đúng á 😝 Nhập hana / uri / kt / tm nha~", { reply_markup: leftKb() });
      return true;
    }
    const d = sess.data;
    clearSession(chatId);
    await sellFromLot({ chatId, lot: d.lot, qty: d.qty, totalPrice: d.totalPrice, wallet: w });
    return true;
  }

  return false;
}

/* =========================
 * SECTION 24 — Cron (placeholder)
 * ========================= */
cron.schedule("*/30 * * * *", async () => {});

/* =========================
 * SECTION 25 — Main message handler
 * ========================= */
async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  if (!chatId) return;
  const userName = msg.from?.first_name || "User";
  const text = String(msg.text || "").trim();
  if (!text) return;

  if (text === "/start") {
    clearSession(chatId);
    await send(chatId, `✅ Bot lên đồ xong rồi nè (${VERSION})\nGọi mình là “bé bot” cũng được 😝`, { reply_markup: mainKb() });
    return;
  }

  if (text === "/help") {
    await send(chatId, helpText(), { reply_markup: mainKb() });
    return;
  }

  // menus
  if (text === "⬅️ Menu") {
    clearSession(chatId);
    await send(chatId, "⬅️ Menu Trái đây nè~ (nhập liệu siêu nhanh) ⚡", { reply_markup: leftKb() });
    return;
  }
  if (text === "➡️ Menu") {
    clearSession(chatId);
    await send(chatId, "➡️ Menu Phải đây nè~ (báo cáo + ví + reset) 📊", { reply_markup: rightKb() });
    return;
  }
  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, "Về menu chính nha bạn iu~ 🏠", { reply_markup: mainKb() });
    return;
  }

  // right menu actions
  if (text === "💰 Tổng Doanh Thu") return reportTotalRevenue(chatId);
  if (text === "📅 Tháng Này") return reportThisMonth(chatId);
  if (text === "⏮️ Tháng Trước") return reportLastMonth(chatId);
  if (text === "📊 Thống Kê Game") return reportStatsGames(chatId);
  if (text === "📱 Lời/Lỗ Máy") return reportMachinePnL(chatId);
  if (text === "💼 Xem Ví") return reportWallets(chatId);
  if (text === "📘 Hướng Dẫn") return send(chatId, helpText(), { reply_markup: rightKb() });

  if (text === "🧠 Smart Parse: Bật/Tắt") {
    const on = await toggleSmartParse();
    await send(chatId, `🧠 Smart Parse hiện đang: ${on ? "BẬT ✅" : "TẮT ❌"}`, { reply_markup: rightKb() });
    return;
  }

  if (text === "🧨 Xóa Sạch Dữ Liệu") {
    setSession(chatId, { flow: "reset", step: "pass" });
    await send(chatId, "⚠️ Khu vực nguy hiểm nha bạn iu 😵‍💫\n🔐 Vui lòng điền pass để XÓA SẠCH dữ liệu ^^", { reply_markup: rightKb() });
    return;
  }

  // left menu
  if (text === "📱 Mua Máy (Lô)") {
    setSession(chatId, { flow: "buy_lot", step: "sentence", data: {} });
    await send(chatId, "📱 Mua Máy (Lô)\nBạn gõ: `mua 2ss 88k` hoặc `mua lg35k hn` nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "💸 Bán Máy") {
    setSession(chatId, { flow: "sell", step: "sentence", data: {} });
    await send(chatId, "💸 Bán Máy\nBạn gõ: `ban ss 50k ma 01` hoặc `ban 2 ss 80k ma01 tm` nha~", { reply_markup: leftKb() });
    return;
  }
  if (text === "🧪 Kiểm Tra Máy") return listLotsPretty(chatId);

  // session
  if (await handleSessionInput(chatId, userName, text)) return;

  // resolve lot profit/tach/hue
  const lotCmd = parseLotResolve(text);
  if (lotCmd && lotCmd.segments && lotCmd.segments.length > 0) {
    await applyLotResolve({ chatId, userName, lot: lotCmd.lot, segments: lotCmd.segments });
    return;
  }

  // sell direct without menu
  const sell = parseSellSentence(text);
  if (sell && !sell.incomplete) {
    if (sell.wallet) {
      await sellFromLot({ chatId, lot: sell.lot, qty: sell.qty, totalPrice: sell.totalPrice, wallet: sell.wallet });
      return;
    }
    setSession(chatId, { flow: "sell", step: "wallet", data: sell });
    await send(chatId, `Mình hiểu bạn đang bán lô ${sell.lot} x${sell.qty} giá ${moneyWON(sell.totalPrice)}.\nTiền về ví nào? (hana/uri/kt/tm)`, {
      reply_markup: leftKb(),
    });
    return;
  }

  // quick revenue input
  const norm = normalizeForParse(text);
  const game = detectGameFromText(norm);
  const amt = extractMoneyFromText(text);

  if (game && amt != null) {
    const g = game === "other" ? "other" : game;
    const type = g === "other" ? "other" : "manual";
    await addGameRevenue({ game: g, type, amount: amt, note: "input", chatId, userName });
    await send(chatId, `✅ Đã ghi doanh thu ${g.toUpperCase()}: ${moneyWON(amt)}`, { reply_markup: mainKb() });
    return;
  }

  // Smart Parse buy lot even without menu
  if (await isSmartParseEnabled()) {
    const buy = parseBuySentence(text);
    if (buy && !buy.incomplete) {
      if (buy.wallet) {
        setSession(chatId, { flow: "buy_lot", step: "note", data: buy });
        await send(chatId, `Okie 😚 Mua lô ${buy.qty} máy ${buy.model}, tổng ${moneyWON(buy.totalPrice)}.\nVí: ${buy.wallet.toUpperCase()}\nNhập note (hoặc '-') nha~`, {
          reply_markup: leftKb(),
        });
        return;
      }
      setSession(chatId, { flow: "buy_lot", step: "wallet", data: buy });
      await send(chatId, `Mình hiểu bạn mua lô ${buy.qty} máy ${buy.model}, tổng ${moneyWON(buy.totalPrice)}.\nTính tiền ví nào? (hana/uri/kt/tm)`, {
        reply_markup: leftKb(),
      });
      return;
    }
  }

  // unknown
  await send(
    chatId,
    "Nhập sai rồi bạn iu ơi ^^  Vào ➡️ Menu → 📘 Hướng dẫn nha~\n(hoặc bật 🧠 Smart Parse để mình hiểu bạn hơn 😚)",
    { reply_markup: mainKb(), __raw: true }
  );
}

/* =========================
 * SECTION 26 — Webhook route
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
 * SECTION 27 — BOOT (Render)
 * ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ BOT READY on ${PORT} | ${VERSION}`);
});
