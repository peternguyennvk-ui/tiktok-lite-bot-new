// src/index.js
import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * ENV
 * ========================= */
const VERSION = "LOT-MAxx-SMARTPARSE-WALLET-SELL-CUTE-HTML-WON-FIXED-ANALYSIS-V2";
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
function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}
function normalizeSpaces(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}
function lotDisplay(lot) {
  // lot = "MA01" -> "MÃ 01"
  const s = String(lot || "").trim().toUpperCase();
  const m = s.match(/^MA(\d{1,3})$/);
  if (!m) return s;
  return `MÃ ${String(Number(m[1])).padStart(2, "0")}`;
}
function lotDisplayShort(lot) {
  // "MA01" -> "01"
  const s = String(lot || "").trim().toUpperCase();
  const m = s.match(/^MA(\d{1,3})$/);
  if (!m) return s.replace(/^MA/i, "");
  return String(Number(m[1])).padStart(2, "0");
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
 * (Back luôn ở trên cùng)
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
    [{ text: "📄 Danh Sách Máy" }],
    [{ text: "⚽ Thu Đá Bóng" }, { text: "🎁 Thu Hộp Quà" }],
    [{ text: "🔳 Thu QR" }, { text: "➕ Thu Khác" }],
  ]);
}
function rightKb() {
  return kb([
    [{ text: "⬅️ Back" }],
    [{ text: "📊 Phân Tích" }],
    [{ text: "💰 Tổng Doanh Thu" }],
    [{ text: "📅 Tháng Này" }, { text: "⏮️ Tháng Trước" }],
    [{ text: "📊 Thống Kê Game" }],
    [{ text: "💼 Xem Ví" }],
    [{ text: "✏️ Sửa Số Dư Ví" }],
    [{ text: "✏️ Sửa Tổng Doanh Thu" }],
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
 * Payouts (CHỈ DÙNG CHO PHÂN TÍCH MÁY)
 * HQ=150k, QR=57k, DB=100k
 * ========================= */
async function getPayoutsAnalysis() {
  // cố định theo yêu cầu
  return { hq: 150000, qr: 57000, db: 100000 };
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
 * Game Revenue (DOANH THU CHÍNH - BẠN TỰ GHI)
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
 * LOTS + PHONES
 * Status chuẩn:
 * - new (chưa làm)
 * - loi (ăn HQ/QR/DB) -> có game hq/qr/db
 * - lo  (tạch/lỗ)     -> game none
 * - hue (huề)         -> game none
 * - sold (đã bán)     -> GIỮ NGUYÊN game (nếu có) để phân tích "kể cả sold"
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

  // trừ tiền ví (mua lô)
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
    // PHONES columns: A phone_id, B lot, C ts, D unit, E status, F game, G note
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
      status: String(r[4] || "").trim().toLowerCase(), // new/loi/lo/hue/sold
      game: String(r[5] || "").trim().toLowerCase(),   // hq/qr/db/none
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
    .replace(/\b(dt|đt|dien thoai|dien-thoai)\b/g, "")
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

  const model = detectModelToken(norm);
  const wallet = parseWalletShortcut(text);

  return { lot, qty, totalPrice, model, wallet };
}

/* =========================
 * Parse CHỐT LÔ (hỗ trợ: ma01 hq1 tach2 ...)
 * ========================= */
function parseLotResolve(text) {
  const norm = normalizeForParse(text);
  const lot = parseLotCode(text);
  if (!lot) return null;

  const tokens = norm.split(" ").filter(Boolean);
  const segments = [];

  const pushSeg = (kind, count, game = "") => {
    segments.push({ kind, count: Math.max(0, Math.min(50, count || 0)), game: game || "" });
  };

  // helper: read number after token if any, else default 1
  const readCount = (i) => {
    let count = 1;
    if (i + 1 < tokens.length && /^\d+$/.test(tokens[i + 1])) return { count: Number(tokens[i + 1]) || 1, step: 2 };
    // token dạng hq1, tach2...
    const m = tokens[i].match(/^(hq|qr|db|tach|lo|hue|hoa|von|loi)(\d+)$/);
    if (m) return { count: Number(m[2]) || 1, step: 1, embedded: true };
    return { count, step: 1 };
  };

  let i = 0;
  while (i < tokens.length) {
    const tk = tokens[i];

    // hq/qr/db => coi như "lời"
    if (tk === "hq" || tk === "qr" || tk === "db" || /^(hq|qr|db)\d+$/.test(tk)) {
      const { count, step } = readCount(i);
      const g = tk.startsWith("hq") ? "hq" : tk.startsWith("qr") ? "qr" : "db";
      pushSeg("loi", count, g);
      i += step;
      continue;
    }

    // "loi" có/không game
    if (tk === "loi" || tk === "lai" || /^loi\d+$/.test(tk) || /^lai\d+$/.test(tk)) {
      const { count, step } = readCount(i);
      i += step;

      // bỏ qua "may"/"dt"
      if (i < tokens.length && (tokens[i] === "may" || tokens[i] === "dt")) i++;

      // game có thể nằm ngay sau
      let g = "";
      if (i < tokens.length) {
        const t2 = tokens[i];
        if (t2 === "hq" || t2 === "qr" || t2 === "db" || /^(hq|qr|db)\d+$/.test(t2)) {
          g = t2.startsWith("hq") ? "hq" : t2.startsWith("qr") ? "qr" : "db";
          // nếu token là hq2 thì nó là 1 segment riêng, không ăn vào đây (tránh double)
          if (!/^(hq|qr|db)\d+$/.test(t2)) i += 1;
        }
      }
      pushSeg("loi", count, g); // g có thể rỗng -> sẽ hỏi lại
      continue;
    }

    // lỗ: tach / lo
    if (tk === "tach" || tk === "lo" || /^tach\d+$/.test(tk) || /^lo\d+$/.test(tk)) {
      const { count, step } = readCount(i);
      pushSeg("lo", count, "");
      i += step;
      continue;
    }

    // huề: hue/hoa/von
    if (tk === "hue" || tk === "hoa" || tk === "von" || /^hue\d+$/.test(tk) || /^hoa\d+$/.test(tk) || /^von\d+$/.test(tk)) {
      const { count, step } = readCount(i);
      pushSeg("hue", count, "");
      i += step;
      continue;
    }

    i++;
  }

  // fallback tối thiểu
  if (segments.length === 0) return { lot, segments: [] };

  return { lot, segments };
}

/* =========================
 * Apply resolve (CHỈ cập nhật PHONES, KHÔNG cộng doanh thu chính)
 * Nếu "loi" thiếu game -> hỏi lại HQ/QR/DB
 * ========================= */
async function applyLotResolve({ chatId, userName, lot, segments }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy mã lô <code>${escapeHtml(lotDisplay(lot))}</code> á. Bạn check lại nha~`, { reply_markup: leftKb() });
    return true;
  }

  // nếu có segment "loi" thiếu game => hỏi
  const missingGame = segments.find((s) => s.kind === "loi" && !s.game);
  if (missingGame) {
    setSession(chatId, { flow: "resolve_game", step: "pick", data: { lot, segments } });
    await send(
      chatId,
      `Bạn ghi <b>lời</b> mà chưa nói game á 😝\nLời đó là <b>HQ</b> hay <b>QR</b> hay <b>DB</b>?\nNhập: <code>hq</code> / <code>qr</code> / <code>db</code>`,
      { reply_markup: leftKb() }
    );
    return true;
  }

  const pick = (n) => {
    // lấy máy chưa chốt trước (new), nếu hết thì lấy máy chưa sold
    const pending = lotPhones.filter((p) => p.status === "new");
    const pool = pending.length > 0 ? pending : lotPhones.filter((p) => p.status !== "sold");
    return pool.slice(0, n).map((p) => p.phone_id);
  };

  let cHQ = 0,
    cQR = 0,
    cDB = 0,
    cLoi = 0,
    cLo = 0,
    cHue = 0;

  for (const seg of segments) {
    const ids = pick(seg.count);
    if (ids.length === 0) continue;

    if (seg.kind === "lo") {
      for (const id of ids) await updatePhoneRowById(id, { status: "lo", game: "none" });
      cLo += ids.length;
      continue;
    }

    if (seg.kind === "hue") {
      for (const id of ids) await updatePhoneRowById(id, { status: "hue", game: "none" });
      cHue += ids.length;
      continue;
    }

    // loi
    const g = seg.game || "hq";
    for (const id of ids) await updatePhoneRowById(id, { status: "loi", game: g });
    cLoi += ids.length;
    if (g === "hq") cHQ += ids.length;
    if (g === "qr") cQR += ids.length;
    if (g === "db") cDB += ids.length;
  }

  const payouts = await getPayoutsAnalysis();
  const totalGame = cHQ * payouts.hq + cQR * payouts.qr + cDB * payouts.db;

  // format đúng yêu cầu: CHỐT LÔ MÃ 01 ... không gợi ý bán
  const html =
    `🧾 <b>CHỐT LÔ ${escapeHtml(lotDisplay(lot))}</b>\n` +
    `✅ <b>Lời</b>: <code>${cLoi}</code> MÁY (HQ:<code>${cHQ}</code> / QR:<code>${cQR}</code> / DB:<code>${cDB}</code>)\n` +
    `😵 <b>Lỗ</b>: <code>${cLo}</code> MÁY TẠCH\n` +
    `😌 <b>Huề</b>: <code>${cHue}</code>\n` +
    `🎮 <b>Tổng thu game (phân tích)</b>: <b>${moneyWON(totalGame)}</b>`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

/* =========================
 * SELL (GIỮ NGUYÊN game khi sold)
 * ========================= */
async function sellFromLot({ chatId, lot, qty, totalPrice, wallet }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy lô <code>${escapeHtml(lotDisplay(lot))}</code> luôn á. Bạn check lại mã nha~`, { reply_markup: leftKb() });
    return true;
  }

  const sellable = lotPhones
    .filter((p) => p.status !== "sold")
    .sort((a, b) => {
      // ưu tiên new trước, rồi hue, rồi lo, rồi loi
      const rank = (s) => (s === "new" ? 0 : s === "hue" ? 1 : s === "lo" ? 2 : s === "loi" ? 3 : 9);
      return rank(a.status) - rank(b.status);
    });

  const ids = sellable.slice(0, qty).map((p) => p.phone_id);
  if (ids.length === 0) {
    await send(chatId, `Lô <code>${escapeHtml(lotDisplay(lot))}</code> bán hết sạch rồi 😝`, { reply_markup: leftKb() });
    return true;
  }

  // GIỮ NGUYÊN GAME khi sold (để phân tích "kể cả sold" luôn đúng)
  for (const id of ids) await updatePhoneRowById(id, { status: "sold" });

  // ghi log tiền bán
  await addWalletLog({
    wallet,
    type: "machine_sell",
    amount: Math.abs(Math.round(totalPrice)),
    ref_type: "lot",
    ref_id: lot,
    note: `SELL x${ids.length}`,
    chatId,
  });

  // lấy note từ LOT (giữ đúng như người dùng nhập)
  const lots = await readLots();
  const l = lots.find((x) => x.lot === lot);
  const note = normalizeSpaces(String(l?.note || ""));
  const noteText = note ? ` ${escapeHtml(note)}` : "";

  const html =
    `💸 <b>BÁN XONG</b> 🥳\n` +
    `• Lô: <b>${escapeHtml(lotDisplay(lot))}</b>\n` +
    `• Số máy: <code>${ids.length}</code> máy${noteText}\n` +
    `• Tiền về ví <b>${escapeHtml(wallet.toUpperCase())}</b>: <b>${moneyWON(Math.round(totalPrice))}</b>\n\n` +
    `Phân tích lô sẽ tự cộng tiền bán này vào nhé 😝 💖`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

/* =========================
 * Reports - DOANH THU CHÍNH (bạn tự ghi)
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
 * PHÂN TÍCH MUA MÁY (đúng spec - tính kể cả sold)
 * ========================= */
function bar(pct, width = 18) {
  const n = Math.round((pct / 100) * width);
  return "█".repeat(n).padEnd(width, " ");
}
async function reportMachineAnalysis(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const logs = await readWalletLog();
  const payouts = await getPayoutsAnalysis();

  const buy = lots.reduce((a, b) => a + (b.total || 0), 0);

  // Thu bán = sum machine_sell
  const sell = logs
    .filter((l) => l.type === "machine_sell" && l.ref_type === "lot")
    .reduce((a, b) => a + (b.amount || 0), 0);

  // Thu game = đếm theo TỔNG KẾT QUẢ (kể cả sold) => dựa vào game hq/qr/db
  const hqCount = phones.filter((p) => p.game === "hq").length;
  const qrCount = phones.filter((p) => p.game === "qr").length;
  const dbCount = phones.filter((p) => p.game === "db").length;
  const thuGame = hqCount * payouts.hq + qrCount * payouts.qr + dbCount * payouts.db;

  const thu = thuGame + sell;
  const netTam = thuGame - buy;
  const netThuc = thu - buy;

  // Đếm trạng thái (tổng kết quả - tính kể cả sold)
  const loiTotal = hqCount + qrCount + dbCount; // kể cả sold vì game giữ nguyên
  const loTotal = phones.filter((p) => p.status === "lo").length;
  const hueTotal = phones.filter((p) => p.status === "hue").length;
  const newTotal = phones.filter((p) => p.status === "new").length;
  const soldTotal = phones.filter((p) => p.status === "sold").length;

  // Lời còn giữ = có game nhưng chưa sold
  const loiConGiu = phones.filter((p) => p.status !== "sold" && (p.game === "hq" || p.game === "qr" || p.game === "db")).length;

  const totalPhones = phones.length || 1;
  const pctNew = (newTotal / totalPhones) * 100;
  const pctLoi = (loiTotal / totalPhones) * 100;
  const pctLo = (loTotal / totalPhones) * 100;
  const pctSold = (soldTotal / totalPhones) * 100;

  const html =
    `📊 <b>PHÂN TÍCH MUA MÁY</b>\n\n` +
    `💳 <b>Đã bỏ ra mua máy</b>: <b>${moneyWON(buy)}</b>\n` +
    `💰 <b>Số tiền thu được</b>: <b>${moneyWON(thu)}</b>\n` +
    `   • Thu game (HQ/QR/DB): <b>${moneyWON(thuGame)}</b>\n` +
    `   • Thu bán máy: <b>${moneyWON(sell)}</b>\n\n` +
    `🧮 <b>Net tạm</b> (game - mua): <b>${moneyWON(netTam)}</b>\n` +
    `🧮 <b>Net thực</b> (game + bán - mua): <b>${moneyWON(netThuc)}</b>\n\n` +
    `<b>Máy (tổng kết quả - tính kể cả Sold)</b>\n` +
    `• Lời: <code>${loiTotal}</code> máy (HQ:<code>${hqCount}</code> / QR:<code>${qrCount}</code> / DB:<code>${dbCount}</code>)\n` +
    `• Lỗ: <code>${loTotal}</code> máy\n` +
    `• Huề: <code>${hueTotal}</code> máy\n` +
    `• Chưa làm (New): <code>${newTotal}</code> máy\n` +
    `• Đã bán (Sold): <code>${soldTotal}</code> máy\n` +
    `• Lời còn giữ: <code>${loiConGiu}</code> máy\n\n` +
    `📌 <b>Biểu đồ trạng thái</b>\n` +
    `New  : ${bar(pctNew)} ${pctNew.toFixed(0)}% (${newTotal})\n` +
    `Lời  : ${bar(pctLoi)} ${pctLoi.toFixed(0)}% (${loiTotal})\n` +
    `Lỗ   : ${bar(pctLo)} ${pctLo.toFixed(0)}% (${loTotal})\n` +
    `Sold : ${bar(pctSold)} ${pctSold.toFixed(0)}% (${soldTotal})\n\n` +
    `💸 <b>Biểu đồ tiền</b>\n` +
    `Bỏ ra (mua): ${bar(100)} ${moneyWON(buy)}\n` +
    `Thu game    : ${bar(buy ? (thuGame / buy) * 100 : 0)} ${moneyWON(thuGame)}\n` +
    `Thu bán     : ${bar(buy ? (sell / buy) * 100 : 0)} ${moneyWON(sell)}\n\n` +
    `<i>(HQ=150k, QR=57k, DB=100k chỉ dùng cho phân tích máy nha 😚)</i>`;

  await send(chatId, html, { reply_markup: rightKb() });
}

/* =========================
 * DANH SÁCH LÔ MÁY (Tất cả / 20 lô gần nhất) - FIX ĐÚNG
 * - Lời (tổng kết quả) tính theo GAME kể cả sold
 * - Đã bán lấy từ WALLET_LOG theo ref_id=MAxx
 * - Bỏ gợi ý "Chốt lô/Bán"
 * ========================= */
async function listLotsPretty(chatId, mode = "all") {
  const lots = await readLots();
  const phones = await readPhones();
  const logs = await readWalletLog();
  const payouts = await getPayoutsAnalysis();

  if (lots.length === 0) {
    await send(chatId, `Chưa có lô nào hết á 😝\nBấm <b>📱 Mua Máy (Lô)</b> để tạo lô nha~`, { reply_markup: leftKb() });
    return;
  }

  const sorted = [...lots].sort((a, b) => (a.ts < b.ts ? 1 : -1));
  const slice = mode === "recent20" ? sorted.slice(0, 20) : sorted;

  const lines = slice.map((l) => {
    const ps = phones.filter((p) => p.lot === l.lot);

    const newCount = ps.filter((p) => p.status === "new").length;
    const loCount = ps.filter((p) => p.status === "lo").length;
    const hueCount = ps.filter((p) => p.status === "hue").length;
    const soldCount = ps.filter((p) => p.status === "sold").length;

    // LỜI theo tổng kết quả (kể cả sold) => dựa vào game
    const hq = ps.filter((p) => p.game === "hq").length;
    const qr = ps.filter((p) => p.game === "qr").length;
    const db = ps.filter((p) => p.game === "db").length;
    const loiTotal = hq + qr + db;

    // lời còn giữ = có game nhưng chưa sold
    const loiHold = ps.filter((p) => p.status !== "sold" && (p.game === "hq" || p.game === "qr" || p.game === "db")).length;
    const remain = Math.max(0, (l.qty || 0) - soldCount);

    const thuGame = hq * payouts.hq + qr * payouts.qr + db * payouts.db;

    const daBan = logs
      .filter((x) => x.type === "machine_sell" && x.ref_type === "lot" && String(x.ref_id || "").toUpperCase() === l.lot)
      .reduce((a, b) => a + (b.amount || 0), 0);

    const laiTam = thuGame - (l.total || 0);
    const laiThuc = daBan + thuGame - (l.total || 0);

    return (
      `• <b>${escapeHtml(l.lot)}</b>: Mua <code>${l.qty}</code> máy <b>${escapeHtml(l.model || "")}</b> | Tổng <b>${moneyWON(l.total)}</b> | Ví <code>${escapeHtml(String(l.wallet || "").toUpperCase())}</code>\n\n` +
      `  Trạng thái: Lời <code>${loiTotal}</code> máy (HQ:<code>${hq}</code> / QR:<code>${qr}</code> / DB:<code>${db}</code>) / Huề <code>${hueCount}</code> / Lỗ <code>${loCount}</code> / New <code>${newCount}</code> / Sold <code>${soldCount}</code>\n` +
      `  Lời còn giữ: <code>${loiHold}</code> máy | Còn lại: <code>${remain}</code> máy\n\n` +
      `  Tổng thu game: <b>${moneyWON(thuGame)}</b>\n` +
      `  Lãi tạm: <b>${moneyWON(thuGame)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(laiTam)}</b>\n` +
      `  Đã bán: <b>${moneyWON(daBan)}</b>\n` +
      `  Lãi thực: <b>${moneyWON(daBan)}</b> + <b>${moneyWON(thuGame)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(laiThuc)}</b>`
    );
  });

  const title = mode === "recent20" ? "🧪 <b>DANH SÁCH LÔ MÁY</b> (20 lô gần nhất)" : "🧪 <b>DANH SÁCH LÔ MÁY</b> (Tất cả)";
  const html = `${title}\n\n${lines.join("\n\n")}`;

  await send(chatId, html, { reply_markup: leftKb() });
}

/* =========================
 * DANH SÁCH MÁY (phân trang + lọc lô)
 * ========================= */
function phoneListKb() {
  return kb([
    [{ text: "⬅️ Back" }],
    [{ text: "⬅️ Trang Trước" }, { text: "➡️ Trang Sau" }],
    [{ text: "🔎 Lọc Theo Lô" }, { text: "🧹 Bỏ Lọc" }],
  ]);
}
async function showPhoneList(chatId, page = 1, lotFilter = "") {
  const perPage = 30;
  const phones = await readPhones();
  const lots = await readLots();
  const lotMap = new Map(lots.map((l) => [l.lot, l]));
  const filterLot = lotFilter ? lotFilter.toUpperCase() : "";

  const list = filterLot ? phones.filter((p) => p.lot === filterLot) : phones;
  const total = list.length;
  const pages = Math.max(1, Math.ceil(total / perPage));
  const p = Math.max(1, Math.min(pages, page));
  const start = (p - 1) * perPage;
  const slice = list.slice(start, start + perPage);

  const fmtStatus = (st) => {
    if (st === "new") return "New";
    if (st === "loi") return "Lời";
    if (st === "lo") return "Lỗ";
    if (st === "hue") return "Huề";
    if (st === "sold") return "Đã bán";
    return st || "—";
  };
  const fmtGame = (g) => (g === "hq" ? "HQ" : g === "qr" ? "QR" : g === "db" ? "DB" : "");

  const lines = slice.map((x) => {
    const l = lotMap.get(x.lot);
    const note = normalizeSpaces(String(x.note || l?.note || ""));
    const extra = note ? ` | ${escapeHtml(note)}` : "";
    const g = fmtGame(x.game);
    const gTxt = g ? ` (${g})` : "";
    return `• <b>${escapeHtml(x.phone_id)}</b> | Lô <b>${escapeHtml(lotDisplay(x.lot))}</b> | ${fmtStatus(x.status)}${gTxt}${extra}`;
  });

  const header = `📄 <b>DANH SÁCH MÁY</b> ${filterLot ? `(Lô ${escapeHtml(lotDisplay(filterLot))})` : "(Tất cả)"}\nTrang <b>${p}</b>/<b>${pages}</b> | Tổng: <b>${total}</b>\n`;
  const html = header + "\n" + (lines.length ? lines.join("\n") : "<i>Không có máy nào.</i>");

  setSession(chatId, { flow: "phone_list", step: "view", data: { page: p, lot: filterLot } });
  await send(chatId, html, { reply_markup: phoneListKb() });
}

/* =========================
 * Quick revenue (doanh thu chính)
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
    `• <code>mua 3ss 50k</code>\n` +
    `• <code>mua ip 35k uri</code>\n\n` +
    `Ví tắt: <code>hana/hn</code> | <code>uri</code> | <code>kt</code> | <code>tm</code>\n\n` +
    `✅ <b>Chốt lô</b> (gõ tắt được):\n` +
    `• <code>ma01 hq1 tach2</code>\n` +
    `• <code>ma 01 loi 1 hq loi 1 qr lo 1</code>\n` +
    `• <code>ma01 hue1</code>\n\n` +
    `✅ <b>Bán</b> (tiền là <b>TỔNG</b>):\n` +
    `• <code>ban 2 ss 50k ma01 uri</code>\n\n` +
    `✅ <b>Thu nhanh (doanh thu chính)</b>:\n` +
    `• <code>db 100k</code> / <code>hq 200k</code> / <code>qr 57k</code> / <code>them 0.5k</code>\n\n` +
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
      await send(
        chatId,
        `Okie 😚 Mua lô <code>${parsed.qty}</code> máy <b>${escapeHtml(parsed.model)}</b>, tổng <b>${moneyWON(parsed.totalPrice)}</b>\nVí: <code>${escapeHtml(parsed.wallet.toUpperCase())}</code>\n\nNhập note (vd <code>Note4</code>) hoặc <code>-</code> để bỏ qua nha~`,
        { reply_markup: leftKb() }
      );
      return true;
    }

    sess.step = "wallet";
    setSession(chatId, sess);
    await send(chatId, `Okie 😚 Mua lô <code>${parsed.qty}</code> máy <b>${escapeHtml(parsed.model)}</b>, tổng <b>${moneyWON(parsed.totalPrice)}</b>\n\nTính tiền ví nào? (<code>hana/uri/kt/tm</code>)`, {
      reply_markup: leftKb(),
    });
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
    await send(chatId, `Nhập note (vd <code>Note4</code>) hoặc <code>-</code> để bỏ qua nha~`, { reply_markup: leftKb() });
    return true;
  }

  if (sess.flow === "buy_lot" && sess.step === "note") {
    const note = String(text || "").trim();
    const extra = note === "-" ? "" : note;

    const finalNote = normalizeSpaces([sess.data.note, extra].filter(Boolean).join(" ").trim());
    const r = await addLot({
      qty: sess.data.qty,
      model: sess.data.model,
      total_price: Math.round(sess.data.totalPrice),
      wallet: sess.data.wallet,
      note: finalNote, // lưu đúng như nhập
    });

    clearSession(chatId);

    const html =
      `✅ <b>Xong rồi nè</b> 🥳\n` +
      `Tạo lô: <b>${escapeHtml(lotDisplay(r.lot))}</b>\n` +
      `Mua: <code>${sess.data.qty}</code> máy <b>${escapeHtml(sess.data.model)}</b>\n` +
      `Tổng: <b>${moneyWON(Math.round(sess.data.totalPrice))}</b>\n` +
      `Ví: <code>${escapeHtml(String(sess.data.wallet || "").toUpperCase())}</code>`;

    await send(chatId, html, { reply_markup: leftKb() });
    return true;
  }

  // SELL
  if (sess.flow === "sell" && sess.step === "sentence") {
    const parsed = parseSellSentence(text);
    if (!parsed || parsed.incomplete) {
      await send(chatId, `Bạn gõ: <code>ban 2 ss 50k ma01 uri</code> nha~`, { reply_markup: leftKb() });
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
    await send(chatId, `Mình hiểu bạn đang bán lô <b>${escapeHtml(lotDisplay(parsed.lot))}</b> x<code>${parsed.qty}</code> giá <b>${moneyWON(parsed.totalPrice)}</b>\n\nTiền về ví nào? (<code>hana/uri/kt/tm</code>)`, {
      reply_markup: leftKb(),
    });
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
      `Bù chênh: <code>${r.delta >= 0 ? "+" : ""}${moneyWON(r.delta)}</code>`;
    await send(chatId, html, { reply_markup: rightKb() });
    return true;
  }

  // EDIT TOTAL REVENUE (doanh thu chính)
  if (sess.flow === "revenue_edit" && sess.step === "amount") {
    const amt = extractMoneyFromText(text);
    if (amt == null) {
      await send(chatId, `Nhập tổng doanh thu kiểu <code>500k</code> nha bạn iu~`, { reply_markup: rightKb() });
      return true;
    }
    const rows = await readGameRevenue();
    const current = rows.reduce((a, b) => a + b.amount, 0);
    const target = Math.round(amt);
    const delta = target - current;

    clearSession(chatId);

    if (delta === 0) {
      await send(chatId, `Tổng doanh thu đang đúng rồi nè 😚\n= <b>${moneyWON(target)}</b>`, { reply_markup: rightKb() });
      return true;
    }

    await addGameRevenue({
      game: "other",
      type: "revenue_adjust",
      amount: delta,
      note: `SET_TOTAL_REVENUE ${current} -> ${target}`,
      chatId,
      userName,
    });

    await send(chatId, `✏️ <b>SỬA TỔNG DOANH THU</b>\nCũ: <b>${moneyWON(current)}</b>\nMới: <b>${moneyWON(target)}</b>\nBù chênh: <b>${moneyWON(delta)}</b>`, {
      reply_markup: rightKb(),
    });
    return true;
  }

  // RESOLVE GAME PICK
  if (sess.flow === "resolve_game" && sess.step === "pick") {
    const n = normalizeForParse(text).trim();
    const g = n === "hq" ? "hq" : n === "qr" ? "qr" : n === "db" ? "db" : "";
    if (!g) {
      await send(chatId, `Nhập <code>hq</code> / <code>qr</code> / <code>db</code> thôi nha 😝`, { reply_markup: leftKb() });
      return true;
    }
    const { lot, segments } = sess.data;
    // gán game cho tất cả segment loi bị thiếu
    const fixed = segments.map((s) => (s.kind === "loi" && !s.game ? { ...s, game: g } : s));
    clearSession(chatId);
    await applyLotResolve({ chatId, userName, lot, segments: fixed });
    return true;
  }

  // PHONE LIST actions
  if (sess.flow === "phone_list" && sess.step === "filter_lot") {
    const lot = parseLotCode(text);
    if (!lot) {
      await send(chatId, `Nhập mã kiểu <code>ma01</code> nha bạn iu 😚`, { reply_markup: phoneListKb() });
      return true;
    }
    await showPhoneList(chatId, 1, lot);
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
    await send(chatId, `⬅️ <b>Menu Trái</b> đây nè~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "➡️ Menu") {
    clearSession(chatId);
    await send(chatId, `➡️ <b>Menu Phải</b> đây nè~ (báo cáo + ví + phân tích) 📊`, { reply_markup: rightKb() });
    return;
  }
  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, `Về <b>menu chính</b> nha bạn iu~ 🏠`, { reply_markup: mainKb() });
    return;
  }

  // right menu
  if (text === "📊 Phân Tích") return reportMachineAnalysis(chatId);
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

  if (text === "✏️ Sửa Tổng Doanh Thu") {
    setSession(chatId, { flow: "revenue_edit", step: "amount", data: {} });
    await send(chatId, `✏️ <b>Sửa tổng doanh thu</b>\nBạn nhập <b>tổng mới</b> (vd <code>500k</code>) nha~`, { reply_markup: rightKb() });
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
    await send(chatId, `💸 <b>Bán Máy</b>\nBạn gõ: <code>ban 2 ss 50k ma01 uri</code> nha~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "🧪 Kiểm Tra Máy (Tất cả)") return listLotsPretty(chatId, "all");
  if (text === "🧪 20 Lô Gần Nhất") return listLotsPretty(chatId, "recent20");
  if (text === "📄 Danh Sách Máy") return showPhoneList(chatId, 1, "");

  // phone list navigation
  const sess = getSession(chatId);
  if (sess?.flow === "phone_list" && sess.step === "view") {
    const { page, lot } = sess.data || { page: 1, lot: "" };
    if (text === "⬅️ Trang Trước") return showPhoneList(chatId, Math.max(1, page - 1), lot);
    if (text === "➡️ Trang Sau") return showPhoneList(chatId, page + 1, lot);
    if (text === "🔎 Lọc Theo Lô") {
      setSession(chatId, { flow: "phone_list", step: "filter_lot", data: { page, lot } });
      await send(chatId, `Nhập mã lô cần lọc (vd <code>ma01</code>) nha~`, { reply_markup: phoneListKb() });
      return;
    }
    if (text === "🧹 Bỏ Lọc") return showPhoneList(chatId, 1, "");
  }

  // session flows
  if (await handleSessionInput(chatId, userName, text)) return;

  // resolve lot (chốt lô)
  const lotCmd = parseLotResolve(text);
  if (lotCmd && lotCmd.segments && lotCmd.segments.length > 0) {
    await applyLotResolve({ chatId, userName, lot: lotCmd.lot, segments: lotCmd.segments });
    return;
  }

  // sell direct
  const sell = parseSellSentence(text);
  if (sell && !sell.incomplete) {
    if (sell.wallet) {
      await sellFromLot({ chatId, lot: sell.lot, qty: sell.qty, totalPrice: sell.totalPrice, wallet: sell.wallet });
      return;
    }
    setSession(chatId, { flow: "sell", step: "wallet", data: sell });
    await send(chatId, `Mình hiểu bạn đang bán lô <b>${escapeHtml(lotDisplay(sell.lot))}</b> x<code>${sell.qty}</code> giá <b>${moneyWON(sell.totalPrice)}</b>\n\nTiền về ví nào? (<code>hana/uri/kt/tm</code>)`, {
      reply_markup: leftKb(),
    });
    return;
  }

  // quick revenue (doanh thu chính)
  const norm = normalizeForParse(text);
  const game = detectGameFromText(norm);
  const amt = extractMoneyFromText(text);
  if (game && amt != null) {
    const g = game === "other" ? "other" : game;
    const type = g === "other" ? "other" : "manual";
    await addGameRevenue({ game: g, type, amount: amt, note: "input", chatId, userName });
    await send(chatId, `✅ <b>Đã ghi doanh thu</b> <code>${escapeHtml(g.toUpperCase())}</code>: <b>${moneyWON(amt)}</b>`, { reply_markup: mainKb() });
    return;
  }

  // Smart Parse buy lot without menu
  if (await isSmartParseEnabled()) {
    const buy = parseBuySentence(text);
    if (buy && !buy.incomplete) {
      if (buy.wallet) {
        setSession(chatId, { flow: "buy_lot", step: "note", data: buy });
        await send(
          chatId,
          `Okie 😚 Mua lô <code>${buy.qty}</code> máy <b>${escapeHtml(buy.model)}</b>, tổng <b>${moneyWON(buy.totalPrice)}</b>\nVí: <code>${escapeHtml(buy.wallet.toUpperCase())}</code>\nNhập note (vd <code>Note4</code>) hoặc <code>-</code> nha~`,
          { reply_markup: leftKb() }
        );
        return;
      }
      setSession(chatId, { flow: "buy_lot", step: "wallet", data: buy });
      await send(chatId, `Mình hiểu bạn mua lô <code>${buy.qty}</code> máy <b>${escapeHtml(buy.model)}</b>, tổng <b>${moneyWON(buy.totalPrice)}</b>\nTính tiền ví nào? (<code>hana/uri/kt/tm</code>)`, {
        reply_markup: leftKb(),
      });
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
