// src/index.js
import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * ENV
 * ========================= */
const VERSION = "LOT-MAxx-SMARTPARSE-WALLET-SELL-CUTE-HTML-WON";
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

function cuteifyHtml(text) {
  // text assumed already HTML-safe OR we will escape before.
  // Keep cute but consistent.
  const tails = [" 😚", " 🫶", " ✨", " ^^", " 😝", " 🤭", " 💖"];
  let s = String(text ?? "");

  // đồng bộ câu "không hiểu"
  s = s.replaceAll("Không hiểu", "Nhập sai rồi bạn iu ơi ^^");

  // add tail only if not already ended with emoji-ish
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
    [{ text: "✏️ Sửa Số Dư Ví" }],
    [{ text: "📘 Hướng Dẫn" }],
    [{ text: "🧠 Smart Parse: Bật/Tắt" }],
    [{ text: "🧨 Xóa Sạch Dữ Liệu" }],
    [{ text: "⬅️ Back" }],
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
 * Payouts
 * ========================= */
async function getPayouts() {
  const hq = parseMoney((await getSetting("PAYOUT_HQ")) || "100k") ?? 100000;
  const qr = parseMoney((await getSetting("PAYOUT_QR")) || "57k") ?? 57000;
  const db = parseMoney((await getSetting("PAYOUT_DB")) || "100k") ?? 100000;
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
 * Game Revenue
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
 * Apply resolve / sell
 * ========================= */
async function applyLotResolve({ chatId, userName, lot, segments }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy mã lô <code>${escapeHtml(lot)}</code> á. Bạn check lại nha~`, { reply_markup: leftKb() });
    return true;
  }

  const payouts = await getPayouts();
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

  const html =
    `🧾 <b>CHỐT LÔ</b> <code>${escapeHtml(lot)}</code>\n` +
    `✅ <b>OK</b>: <code>${totalOk}</code>\n` +
    `😵 <b>Tạch</b>: <code>${totalTach}</code>\n` +
    `😌 <b>Huề</b>: <code>${totalHue}</code>\n` +
    (totalRev > 0 ? `💰 <b>Doanh thu cộng</b>: <b>${moneyWON(totalRev)}</b>\n` : "") +
    `\n<i>Mẹo:</i> <code>ma 01 loi 1 qr tach 1</code>`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

async function sellFromLot({ chatId, lot, qty, totalPrice, wallet }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy lô <code>${escapeHtml(lot)}</code> luôn á. Bạn check lại mã nha~`, { reply_markup: leftKb() });
    return true;
  }

  const sellable = lotPhones
    .filter((p) => p.status !== "sold")
    .sort((a, b) => {
      const rank = (s) => (s === "new" ? 0 : s === "ok" ? 1 : s === "tach" ? 2 : 3);
      return rank(a.status) - rank(b.status);
    });

  const ids = sellable.slice(0, qty).map((p) => p.phone_id);
  if (ids.length === 0) {
    await send(chatId, `Lô <code>${escapeHtml(lot)}</code> bán hết sạch rồi 😝`, { reply_markup: leftKb() });
    return true;
  }

  for (const id of ids) await updatePhoneRowById(id, { status: "sold", game: "none" });

  // ✅ totalPrice = TỔNG tiền thu về
  await addWalletLog({
    wallet,
    type: "machine_sell",
    amount: Math.abs(Math.round(totalPrice)),
    ref_type: "lot",
    ref_id: lot,
    note: `SELL x${ids.length}`,
    chatId,
  });

  const html =
    `💸 <b>BÁN XONG</b> 🥳\n` +
    `• Lô: <code>${escapeHtml(lot)}</code>\n` +
    `• Số máy: <code>${ids.length}</code>\n` +
    `• Tiền về ví <code>${escapeHtml(wallet.toUpperCase())}</code>: <b>${moneyWON(Math.round(totalPrice))}</b>\n` +
    `\n<i>Chốt đơn mượt ghê 😝</i>`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

/* =========================
 * Reports
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

  const html =
    `📱 <b>LỜI/LỖ MÁY</b>\n\n` +
    `📦 Lô: <code>${lots.length}</code>\n` +
    `📱 Máy: <code>${phones.length}</code>\n` +
    `• New: <code>${neu}</code>\n` +
    `• OK: <code>${ok}</code>\n` +
    `• Tạch: <code>${tach}</code>\n` +
    `• Sold: <code>${sold}</code>\n\n` +
    `💸 <b>Tổng mua</b>: <b>${moneyWON(totalBuy)}</b>\n` +
    `💰 <b>Doanh thu lot_profit</b>: <b>${moneyWON(totalRev)}</b>\n` +
    `🧮 <b>Net</b>: <b>${moneyWON(net)}</b>`;

  await send(chatId, html, { reply_markup: rightKb() });
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
    await send(chatId, `Chưa có lô nào hết á 😝\nBấm <b>📱 Mua Máy (Lô)</b> để tạo lô nha~`, { reply_markup: leftKb() });
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
      `• <b>${escapeHtml(l.lot)}</b>: Mua <code>${l.qty}</code> máy <b>${escapeHtml(l.model)}</b> | Tổng <b>${moneyWON(l.total)}</b> | Ví <code>${escapeHtml(String(l.wallet || "").toUpperCase())}</code>\n` +
      `  Trạng thái: <i>${escapeHtml(st)}</i> (new:<code>${neu}</code> ok:<code>${ok}</code> tạch:<code>${tach}</code> sold:<code>${sold}</code>)\n` +
      `  Game: HQ:<code>${hq}</code> QR:<code>${qr}</code> DB:<code>${db}</code> | Doanh thu: <b>${moneyWON(r)}</b>`
    );
  });

  const html =
    `🧪 <b>DANH SÁCH LÔ MÁY</b> (20 lô gần nhất)\n\n` +
    `${lines.join("\n\n")}\n\n` +
    `<i>Chốt lô:</i> <code>ma 01 loi 1 qr tach 1</code>\n` +
    `<i>Bán:</i> <code>ban 2 ss 80k ma01 tm</code>`;

  await send(chatId, html, { reply_markup: leftKb() });
}

/* =========================
 * Quick revenue
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
    `• <code>mua 2ss 88k</code>\n` +
    `• <code>mua ip 35k</code>\n` +
    `• <code>mua lg35k hn</code> (ví HANA)\n` +
    `• <code>mua 2 dt ss 45k uri</code>\n\n` +
    `Ví tắt: <code>hana/hn</code> | <code>uri</code> | <code>kt</code> | <code>tm</code>\n\n` +
    `✅ <b>Bán</b> (tiền là <b>TỔNG</b>):\n` +
    `• <code>ban ss 50k ma 01</code> (bán 1 máy)\n` +
    `• <code>ban 2 ss 80k ma01 tm</code> (bán 2 máy)\n\n` +
    `✅ <b>Chốt lô</b>:\n` +
    `• <code>ma 01 loi 2 hq</code>\n` +
    `• <code>ma01 tach 2</code>\n` +
    `• <code>ma 01 loi 1 qr tach 1</code>\n\n` +
    `✅ <b>Thu nhanh</b>:\n` +
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
      await send(chatId, `Bạn gõ kiểu: <code>mua 2ss 88k</code> / <code>mua lg35k hn</code> nha~`, { reply_markup: leftKb() });
      return true;
    }
    sess.data = parsed;

    if (parsed.wallet) {
      sess.step = "note";
      setSession(chatId, sess);
      await send(
        chatId,
        `Okie 😚 <b>Mua lô</b> <code>${parsed.qty}</code> máy <b>${escapeHtml(parsed.model)}</b>, tổng <b>${moneyWON(parsed.totalPrice)}</b>\nVí: <code>${escapeHtml(parsed.wallet.toUpperCase())}</code>\n\nNhập <i>ghi chú</i> (hoặc <code>-</code> để bỏ qua) nha~`,
        { reply_markup: leftKb() }
      );
      return true;
    }

    sess.step = "wallet";
    setSession(chatId, sess);
    await send(
      chatId,
      `Okie 😚 <b>Mua lô</b> <code>${parsed.qty}</code> máy <b>${escapeHtml(parsed.model)}</b>, tổng <b>${moneyWON(parsed.totalPrice)}</b>\n\nTính tiền <b>ví nào</b>? (<code>hana/uri/kt/tm</code>)`,
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
    await send(chatId, `Nhập <i>ghi chú</i> (hoặc <code>-</code> để bỏ qua) nha~`, { reply_markup: leftKb() });
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

    const html =
      `✅ <b>Xong rồi nè</b> 🥳\n` +
      `Tạo lô: <code>${escapeHtml(r.lot)}</code>\n` +
      `Mua: <code>${sess.data.qty}</code> máy <b>${escapeHtml(sess.data.model)}</b>\n` +
      `Tổng: <b>${moneyWON(Math.round(sess.data.totalPrice))}</b>\n` +
      `Ví: <code>${escapeHtml(String(sess.data.wallet || "").toUpperCase())}</code>\n\n` +
      `<i>Chốt lô:</i> <code>ma ${r.lot.slice(2)} loi 1 qr tach 1</code>\n` +
      `<i>Bán:</i> <code>ban ${sess.data.qty} ss 80k ma${r.lot.slice(2)} tm</code>`;

    await send(chatId, html, { reply_markup: leftKb() });
    return true;
  }

  // SELL
  if (sess.flow === "sell" && sess.step === "sentence") {
    const parsed = parseSellSentence(text);
    if (!parsed || parsed.incomplete) {
      await send(chatId, `Bạn gõ: <code>ban ss 50k ma 01</code> hoặc <code>ban 2 ss 80k ma01 tm</code> nha~`, { reply_markup: leftKb() });
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
      `Bạn đang <b>bán</b> lô <code>${escapeHtml(parsed.lot)}</code> x<code>${parsed.qty}</code>, tiền <b>${moneyWON(parsed.totalPrice)}</b>\n\nTiền về <b>ví nào</b>? (<code>hana/uri/kt/tm</code>)`,
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
    await send(chatId, `➡️ <b>Menu Phải</b> đây nè~ <i>(báo cáo + ví + reset)</i> 📊`, { reply_markup: rightKb() });
    return;
  }
  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, `Về <b>menu chính</b> nha bạn iu~ 🏠`, { reply_markup: mainKb() });
    return;
  }

  // right menu
  if (text === "💰 Tổng Doanh Thu") return reportTotalRevenue(chatId);
  if (text === "📅 Tháng Này") return reportThisMonth(chatId);
  if (text === "⏮️ Tháng Trước") return reportLastMonth(chatId);
  if (text === "📊 Thống Kê Game") return reportStatsGames(chatId);
  if (text === "📱 Lời/Lỗ Máy") return reportMachinePnL(chatId);
  if (text === "💼 Xem Ví") return reportWallets(chatId);
  if (text === "📘 Hướng Dẫn") return send(chatId, helpText(), { reply_markup: rightKb() });

  if (text === "✏️ Sửa Số Dư Ví") {
    setSession(chatId, { flow: "wallet_edit", step: "wallet", data: {} });
    await send(chatId, `✏️ <b>Sửa số dư ví</b>\nBạn chọn ví: <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code>`, { reply_markup: rightKb() });
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
    await send(chatId, `📱 <b>Mua Máy (Lô)</b>\nBạn gõ: <code>mua 2ss 88k</code> hoặc <code>mua lg35k hn</code> nha~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "💸 Bán Máy") {
    setSession(chatId, { flow: "sell", step: "sentence", data: {} });
    await send(chatId, `💸 <b>Bán Máy</b>\nBạn gõ: <code>ban ss 50k ma 01</code> hoặc <code>ban 2 ss 80k ma01 tm</code> nha~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "🧪 Kiểm Tra Máy") return listLotsPretty(chatId);

  // session
  if (await handleSessionInput(chatId, userName, text)) return;

  // resolve lot
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
    await send(
      chatId,
      `Mình hiểu bạn đang <b>bán</b> lô <code>${escapeHtml(sell.lot)}</code> x<code>${sell.qty}</code> giá <b>${moneyWON(sell.totalPrice)}</b>\n\nTiền về ví nào? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
    return;
  }

  // quick revenue
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
        await send(chatId, `Okie 😚 <b>Mua lô</b> <code>${buy.qty}</code> máy <b>${escapeHtml(buy.model)}</b>, tổng <b>${moneyWON(buy.totalPrice)}</b>\nVí: <code>${escapeHtml(buy.wallet.toUpperCase())}</code>\nNhập note (hoặc <code>-</code>) nha~`, {
          reply_markup: leftKb(),
        });
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
