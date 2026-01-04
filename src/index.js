// src/index.js
import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * ENV
 * ========================= */
const VERSION =
  "LOT-MAxx-SMARTPARSE-WALLET-SELL-CUTE-HTML | SPEC-V6 + MAIL_LOG(VN+NOACCENTS) + MENU(MAIL+DSMOI)";
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
function nowIso() {
  return new Date().toISOString();
}
function normalizeSpaces(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
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
function titleCaseVi(s) {
  const t = normalizeSpaces(String(s || ""));
  if (!t) return "";
  return t
    .split(" ")
    .map((w) => {
      if (!w) return w;
      // keep dots like "A." intact
      if (w.endsWith(".")) {
        const core = w.slice(0, -1);
        if (!core) return w;
        return core.charAt(0).toUpperCase() + core.slice(1) + ".";
      }
      return w.charAt(0).toUpperCase() + w.slice(1);
    })
    .join(" ");
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
    [{ text: "📊 Phân Tích Mua Máy" }],
    [{ text: "♻️ Reset Lô" }, { text: "✏️ Sửa Lô / Đổi Mã" }],
    // ✅ MAIL FEATURE MENU
    [{ text: "📋 Danh sách đã mời" }, { text: "📧 Mail" }],
    [{ text: "⚽ Thu Đá Bóng" }, { text: "🎁 Thu Hộp Quà" }],
    [{ text: "🔳 Thu QR" }, { text: "➕ Thu Khác" }],
  ]);
}
function rightKb() {
  return kb([
    [{ text: "⬅️ Back" }],
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
 * Machine Analysis Payouts (ONLY FOR PHÂN TÍCH MÁY)
 * ========================= */
async function getMachinePayouts() {
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
    ts: String(r[0] || ""),
    wallet: String(r[1] || "").trim().toLowerCase(),
    type: String(r[2] || "").trim().toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    ref_type: String(r[4] || "").trim().toLowerCase(),
    ref_id: String(r[5] || "").trim().toUpperCase(),
    note: String(r[6] || ""),
  }));
}

async function addWalletLog({ wallet, type, amount, ref_type, ref_id, note, chatId }) {
  await appendValues("WALLET_LOG!A1", [
    [nowIso(), wallet, type, amount, ref_type || "", (ref_id || "").toUpperCase(), note || "", String(chatId || "")],
  ]);
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
 * Game Revenue (MAIN doanh thu - manual + adjust ONLY)
 * ========================= */
async function addGameRevenue({ game, type, amount, note, chatId, userName }) {
  await appendValues("GAME_REVENUE!A1", [
    [nowIso(), game, type, amount, note || "", String(chatId || ""), userName || ""],
  ]);
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

async function addLot({ qty, model, total_price, wallet, note, chatId }) {
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
    chatId,
  });

  for (let i = 1; i <= qty; i++) {
    const phone_id = `${lot}-${i}`;
    await appendValues("PHONES!A1", [[phone_id, lot, nowIso(), unit, "new", "none", note || "", "", ""]]);
  }

  return { lot, unit };
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
  const rows = await getValues("PHONES!A2:I");
  return rows
    .filter((r) => r.some((c) => String(c || "").trim() !== ""))
    .map((r) => {
      const status = String(r[4] || "").trim();
      const soldFlag = String(r[7] || "").trim().toLowerCase();
      const sold = soldFlag === "1" || soldFlag === "true" || soldFlag === "sold";
      return {
        phone_id: String(r[0] || "").trim(),
        lot: String(r[1] || "").trim().toUpperCase(),
        unit: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
        status,
        game: String(r[5] || "").trim().toLowerCase(),
        note: String(r[6] || ""),
        sold,
        sold_ts: String(r[8] || ""),
      };
    });
}

async function updatePhoneRowById(phone_id, patch) {
  const rows = await getValues("PHONES!A2:I");
  for (let i = 0; i < rows.length; i++) {
    const id = String(rows[i][0] || "").trim();
    if (id === phone_id) {
      const rowNumber = i + 2;
      const curStatus = String(rows[i][4] || "");
      const curGame = String(rows[i][5] || "");
      const status = patch.status ?? curStatus;
      const game = patch.game ?? curGame;
      await updateValues(`PHONES!E${rowNumber}:F${rowNumber}`, [[status, game]]);
      return true;
    }
  }
  return false;
}

async function markPhonesSoldByIds(ids) {
  const rows = await getValues("PHONES!A2:I");
  const now = nowIso();
  for (let i = 0; i < rows.length; i++) {
    const id = String(rows[i][0] || "").trim();
    if (!ids.includes(id)) continue;
    const rowNumber = i + 2;
    await updateValues(`PHONES!H${rowNumber}:I${rowNumber}`, [["1", now]]);
  }
}

/* =========================
 * reset lot results + rename lot
 * ========================= */
async function resetLotResults(lot) {
  lot = String(lot || "").trim().toUpperCase();
  const rows = await getValues("PHONES!A2:I");
  let changed = 0;
  for (let i = 0; i < rows.length; i++) {
    const phoneLot = String(rows[i][1] || "").trim().toUpperCase();
    if (phoneLot !== lot) continue;
    const rowNumber = i + 2;
    await updateValues(`PHONES!E${rowNumber}:F${rowNumber}`, [["new", "none"]]);
    changed++;
  }
  return changed;
}

async function renameLotEverywhere(oldLot, newLot) {
  oldLot = String(oldLot || "").trim().toUpperCase();
  newLot = String(newLot || "").trim().toUpperCase();

  if (!/^MA\d{2,3}$/.test(oldLot) || !/^MA\d{2,3}$/.test(newLot)) {
    return { ok: false, reason: "Mã lô phải dạng MA01 / MA09 ..." };
  }

  const lots = await readLots();
  if (!lots.find((l) => l.lot === oldLot)) return { ok: false, reason: `Không thấy lô ${oldLot}` };
  if (lots.find((l) => l.lot === newLot)) return { ok: false, reason: `Mã ${newLot} đã tồn tại` };

  {
    const rows = await getValues("LOTS!A2:H");
    for (let i = 0; i < rows.length; i++) {
      const lotCode = String(rows[i][0] || "").trim().toUpperCase();
      if (lotCode !== oldLot) continue;
      const rowNumber = i + 2;
      await updateValues(`LOTS!A${rowNumber}:A${rowNumber}`, [[newLot]]);
    }
  }

  {
    const rows = await getValues("PHONES!A2:I");
    for (let i = 0; i < rows.length; i++) {
      const phoneLot = String(rows[i][1] || "").trim().toUpperCase();
      if (phoneLot !== oldLot) continue;

      const oldId = String(rows[i][0] || "").trim();
      const suffix = oldId.startsWith(oldLot + "-")
        ? oldId.slice((oldLot + "-").length)
        : oldId.split("-").slice(1).join("-");
      const newId = `${newLot}-${suffix}`;

      const rowNumber = i + 2;
      await updateValues(`PHONES!A${rowNumber}:B${rowNumber}`, [[newId, newLot]]);
    }
  }

  {
    const rows = await getValues("WALLET_LOG!A2:H");
    for (let i = 0; i < rows.length; i++) {
      const refType = String(rows[i][4] || "").trim().toLowerCase();
      const refId = String(rows[i][5] || "").trim().toUpperCase();
      if (refType !== "lot") continue;
      if (refId !== oldLot) continue;
      const rowNumber = i + 2;
      await updateValues(`WALLET_LOG!F${rowNumber}:F${rowNumber}`, [[newLot]]);
    }
  }

  return { ok: true };
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

  let note = raw;
  note = note.replace(/\bmua\b/gi, " ");
  note = note.replace(/\b\d+\s*(ss|ip|lg)\b/gi, " ");
  note = note.replace(/\bss\b|\bsamsung\b|\bip\b|\biphone\b|\blg\b/gi, " ");
  note = note.replace(/\bhn\b|\bhana\b|\buri\b|\bkt\b|\btm\b|\btien mat\b|\btienmat\b/gi, " ");
  note = note.replace(/₩\s*\d[\d,]*(?:\.\d+)?\s*k?/gi, " ");
  note = note.replace(/\b\d[\d,]*(?:\.\d+)?\s*k\b/gi, " ");
  note = note.replace(/\b\d[\d,]*(?:\.\d+)?\b/gi, " ");
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

/* =========================
 * Resolve lot parsing (ma01 hq1 tach2 / ma01 3 tach)
 * ========================= */
function explodeCompactToken(tk) {
  const m = tk.match(/^(hq|qr|db|tach|tạch|chet|tac|hue|huề|hoa)(\d+)$/);
  if (!m) return [tk];
  return [m[1], m[2]];
}

// ✅ FIX: support "3 tach" / "2 hq" (number BEFORE keyword)
function parseLotResolve(text) {
  const norm = normalizeForParse(text);
  const lot = parseLotCode(text);
  if (!lot) return null;

  let tokens = norm.split(" ").filter(Boolean);
  tokens = tokens.flatMap(explodeCompactToken);

  const segments = [];
  let missingGame = false;
  let missingGameCount = 0;

  const isGameWord = (x) => x === "hq" || x === "qr" || x === "db";
  const isAnWord = (x) => x === "loi" || x === "lai" || x === "an" || x === "duoc" || x === "ok";
  const isTachWord = (x) => x === "tach" || x === "tạch" || x === "chet" || x === "tac";
  const isHueWord = (x) => x === "hue" || x === "huề" || x === "hoa";
  const clampCount = (n) => Math.max(0, Math.min(50, Number(n) || 0));

  let i = 0;
  while (i < tokens.length) {
    let tk = tokens[i];

    // ✅ NEW: "3 tach" / "2 hq" / "4 hue" / "3 an hq"
    if (tk && /^\d+$/.test(tk) && i + 1 < tokens.length) {
      const n = clampCount(tk);
      const next = tokens[i + 1];

      if (isGameWord(next)) {
        segments.push({ kind: "an", count: n, game: next });
        i += 2;
        continue;
      }

      if (isTachWord(next)) {
        segments.push({ kind: "tach", count: n, game: "" });
        i += 2;
        continue;
      }

      if (isHueWord(next)) {
        segments.push({ kind: "hue", count: n, game: "" });
        i += 2;
        continue;
      }

      if (isAnWord(next)) {
        const g = tokens[i + 2] || "";
        if (isGameWord(g)) {
          segments.push({ kind: "an", count: n, game: g });
          i += 3;
          continue;
        } else {
          missingGame = true;
          missingGameCount += n;
          segments.push({ kind: "an", count: n, game: "" });
          i += 2;
          continue;
        }
      }
      // else fall through
    }

    // existing: "hq 2" or "hq2"
    if (isGameWord(tk)) {
      let count = 1;
      if (i + 1 < tokens.length && /^\d+$/.test(tokens[i + 1])) {
        count = clampCount(tokens[i + 1]);
        i += 2;
      } else i += 1;
      segments.push({ kind: "an", count, game: tk });
      continue;
    }

    const isAn = isAnWord(tk);
    const isTach = isTachWord(tk);
    const isHue = isHueWord(tk);

    if (isAn || isTach || isHue) {
      const kind = isAn ? "an" : isTach ? "tach" : "hue";
      let count = 1;

      if (i + 1 < tokens.length && /^\d+$/.test(tokens[i + 1])) {
        count = clampCount(tokens[i + 1]);
        i += 2;
      } else i += 1;

      if (i < tokens.length && (tokens[i] === "may" || tokens[i] === "dt")) i++;

      let game = "";
      if (kind === "an") {
        const next = tokens[i] || "";
        if (isGameWord(next)) {
          game = next;
          i++;
        } else {
          missingGame = true;
          missingGameCount += count;
        }
      }

      segments.push({ kind, count, game });
      continue;
    }

    i++;
  }

  return { lot, segments, missingGame, missingGameCount };
}

/* =========================
 * Commands: reset / sua
 * ========================= */
function parseLotResetCommand(text) {
  const norm = normalizeForParse(text);
  const lot = parseLotCode(text);
  if (!lot) return null;
  const t = ` ${norm} `;
  if (t.includes(" reset ") || t.includes(" rs ") || t.includes(" clear ")) {
    return { lot };
  }
  return null;
}

function parseSuaCommand(text) {
  const norm = normalizeForParse(text);
  const t = norm.trim();

  if (!t.startsWith("sua ")) return null;

  // rename: "sua ma01 ma09"
  const mRename = t.match(/^sua\s+(ma0*\d{1,3})\s+(ma0*\d{1,3})\b/);
  if (mRename) {
    const oldLot = parseLotCode(mRename[1]);
    const newLot = parseLotCode(mRename[2]);
    if (oldLot && newLot) return { type: "rename", oldLot, newLot };
  }

  // reset: "sua ma01 reset"
  const lot = parseLotCode(t);
  if (!lot) return null;
  const tt = ` ${t} `;
  if (tt.includes(" reset ") || tt.includes(" rs ") || tt.includes(" clear ")) return { type: "reset", lot };

  // overwrite resolve: "sua ma01 hq1 tach2" or "sua ma01 3 tach"
  const rest = t.replace(/^sua\s+/, "");
  const parsed = parseLotResolve(rest);
  if (parsed && parsed.lot && parsed.segments && parsed.segments.length > 0) {
    return {
      type: "overwrite_resolve",
      lot: parsed.lot,
      segments: parsed.segments,
      missingGame: parsed.missingGame,
      missingGameCount: parsed.missingGameCount,
    };
  }

  return null;
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
 * Note -> device label
 * ========================= */
function extractDeviceLabelFromLotNote(lotNote, fallbackModel) {
  const s = String(lotNote || "");
  const parts = s.split("|").map((x) => normalizeSpaces(x));
  for (let i = parts.length - 1; i >= 0; i--) {
    const p = parts[i];
    if (!p) continue;
    const low = removeDiacritics(p).toLowerCase();
    const mLow = removeDiacritics(fallbackModel || "").toLowerCase();
    if (mLow && low.includes(mLow)) continue;
    const m = p.match(/[A-Za-z]+\s*\d+[A-Za-z]?/);
    if (m) return normalizeSpaces(m[0]);
  }
  return "";
}

/* =========================
 * SPEC: NEW/SOLD
 * ========================= */
function normStatus(s) {
  return removeDiacritics(String(s || "")).toLowerCase().trim();
}
function isProfitPhone(p) {
  const g = String(p?.game || "").toLowerCase();
  return g === "hq" || g === "qr" || g === "db";
}
function isLossPhone(p) {
  const st = normStatus(p?.status);
  return st === "tach" || st === "tac" || st === "chet";
}
function isTiePhone(p) {
  const st = normStatus(p?.status);
  return st === "hue" || st === "hoa";
}
function isNewPhone(p) {
  if (!p) return false;
  if (isProfitPhone(p)) return false;
  if (isLossPhone(p)) return false;
  if (isTiePhone(p)) return false;
  return true;
}

/* =========================
 * Apply resolve / sell
 * ========================= */
async function applyLotResolve({ chatId, lot, segments }) {
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy mã lô <code>${escapeHtml(lot)}</code> á. Bạn check lại nha~`, {
      reply_markup: leftKb(),
    });
    return true;
  }

  // pick: ưu tiên máy NEW; nếu hết thì pick máy chưa bán
  const used = new Set();
  const pick = (n) => {
    const pending = lotPhones.filter((p) => !used.has(p.phone_id) && normStatus(p.status) === "new");
    const pool = pending.length > 0 ? pending : lotPhones.filter((p) => !used.has(p.phone_id) && !p.sold);
    const ids = pool.slice(0, n).map((p) => p.phone_id);
    ids.forEach((id) => used.add(id));
    return ids;
  };

  let hqCount = 0,
    qrCount = 0,
    dbCount = 0,
    tachCount = 0,
    hueCount = 0;

  for (const seg of segments) {
    const ids = pick(seg.count);
    if (ids.length === 0) continue;

    if (seg.kind === "tach") {
      for (const id of ids) await updatePhoneRowById(id, { status: "tach", game: "none" });
      tachCount += ids.length;
      continue;
    }

    if (seg.kind === "hue") {
      for (const id of ids) await updatePhoneRowById(id, { status: "hue", game: "none" });
      hueCount += ids.length;
      continue;
    }

    const g = (seg.game || "").toLowerCase();
    if (g !== "hq" && g !== "qr" && g !== "db") continue;

    for (const id of ids) await updatePhoneRowById(id, { status: "ok", game: g });

    if (g === "hq") hqCount += ids.length;
    if (g === "qr") qrCount += ids.length;
    if (g === "db") dbCount += ids.length;
  }

  const payouts = await getMachinePayouts();
  const totalGame = hqCount * payouts.hq + qrCount * payouts.qr + dbCount * payouts.db;

  const html =
    `🧾 <b>CHỐT LÔ MÃ ${escapeHtml(lot.slice(2))}</b>\n` +
    `✅ <b>Ăn được:</b> <b>${hqCount + qrCount + dbCount}</b> MÁY (HQ:${hqCount} / QR:${qrCount} / DB:${dbCount})\n` +
    `😵 <b>Tạch:</b> <b>${tachCount}</b> MÁY\n` +
    `😌 <b>Huề:</b> <b>${hueCount}</b>\n` +
    `🎮 <b>Tổng thu game (phân tích):</b> <b>${moneyWON(totalGame)}</b>`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

async function sellFromLot({ chatId, lot, qty, totalPrice, wallet }) {
  const lots = await readLots();
  const lotRow = lots.find((x) => x.lot === lot);
  const phones = await readPhones();
  const lotPhones = phones.filter((p) => p.lot === lot);

  if (lotPhones.length === 0) {
    await send(chatId, `🥺 Không thấy lô <code>${escapeHtml(lot)}</code> luôn á. Bạn check lại mã nha~`, {
      reply_markup: leftKb(),
    });
    return true;
  }

  const sellable = lotPhones.filter((p) => !p.sold);
  const ids = sellable.slice(0, qty).map((p) => p.phone_id);

  if (ids.length === 0) {
    await send(chatId, `Lô <code>${escapeHtml(lot)}</code> bán hết sạch rồi 😝`, { reply_markup: leftKb() });
    return true;
  }

  await markPhonesSoldByIds(ids);

  // Đã bán = WALLET_LOG type=machine_sell (không đổi HQ/QR/DB/Tạch)
  await addWalletLog({
    wallet,
    type: "machine_sell",
    amount: Math.abs(Math.round(totalPrice)),
    ref_type: "lot",
    ref_id: lot,
    note: `SELL x${ids.length}`,
    chatId,
  });

  const deviceLabel = extractDeviceLabelFromLotNote(lotRow?.note || "", lotRow?.model || "");
  const deviceText = deviceLabel ? ` ${escapeHtml(deviceLabel)}` : "";

  const html =
    `💸 <b>BÁN XONG</b> 🥳\n` +
    `• Lô: <b>MÃ ${escapeHtml(lot.slice(2))}</b>\n` +
    `• Số máy: <b>${ids.length}</b> máy${deviceText}\n` +
    `• Tiền về ví <code>${escapeHtml(wallet.toUpperCase())}</code>: <b>${moneyWON(Math.round(totalPrice))}</b>\n\n` +
    `Phân tích lô sẽ tự cộng tiền bán này vào nhé 😝`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

/* =========================
 * Reports (MAIN doanh thu)
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
  await send(chatId, `💼 <b>SỐ DƯ CÁC VÍ</b>\n\n${lines.join("\n")}\n\n<b>Tổng:</b> <b>${moneyWON(total)}</b>`, {
    reply_markup: rightKb(),
  });
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
 * Danh sách lô
 * ========================= */
async function computeLotSummary(lot, phones, walletLogs) {
  const lotPhones = phones.filter((p) => p.lot === lot.lot);

  const soldMoney = walletLogs
    .filter((l) => l.type === "machine_sell" && l.ref_type === "lot" && String(l.ref_id || "").toUpperCase() === lot.lot)
    .reduce((a, b) => a + (b.amount || 0), 0);

  const soldCount = lotPhones.filter((p) => !!p.sold).length;
  const remainCount = lotPhones.filter((p) => !p.sold).length;

  const hq = lotPhones.filter((p) => String(p.game || "") === "hq").length;
  const qr = lotPhones.filter((p) => String(p.game || "") === "qr").length;
  const db = lotPhones.filter((p) => String(p.game || "") === "db").length;

  const tach = lotPhones.filter((p) => isLossPhone(p)).length;
  const hue = lotPhones.filter((p) => isTiePhone(p)).length;

  const neu = lotPhones.filter((p) => isNewPhone(p)).length;
  const anCount = hq + qr + db;

  const payouts = await getMachinePayouts();
  const gameMoney = hq * payouts.hq + qr * payouts.qr + db * payouts.db;

  const laiTam = gameMoney - (lot.total || 0);
  const laiThuc = gameMoney + soldMoney - (lot.total || 0);

  // ✅ SPEC: Tạm lỗ = max(0, tiền mua − (thu game + thu bán))
  const recovered = gameMoney + soldMoney;
  const tempLoss = Math.max(0, (lot.total || 0) - recovered);

  return {
    soldMoney,
    soldCount,
    remainCount,
    hq,
    qr,
    db,
    anCount,
    tach,
    hue,
    neu,
    gameMoney,
    laiTam,
    laiThuc,
    tempLoss,
  };
}

async function listLotsAll(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const walletLogs = await readWalletLog();

  if (lots.length === 0) {
    await send(chatId, `Chưa có lô nào hết á 😝\nBấm <b>📱 Mua Máy (Lô)</b> để tạo lô nha~`, { reply_markup: leftKb() });
    return;
  }

  const sorted = [...lots].sort((a, b) => (a.ts < b.ts ? 1 : -1));

  const lines = [];
  for (const l of sorted) {
    const s = await computeLotSummary(l, phones, walletLogs);

    const statusLine =
      `  Trạng thái: Ăn <b>${s.anCount}</b> (HQ:${s.hq} / QR:${s.qr} / DB:${s.db}) / Huề <b>${s.hue}</b> / Tạch <b>${s.tach}</b> / New <b>${s.neu}</b> / Sold <b>${s.soldCount}</b>\n` +
      `  Còn lại: <b>${s.remainCount}</b> máy chưa bán\n` +
      (s.tempLoss > 0 ? `  ⚠️ <b>Tạm lỗ:</b> <b>${moneyWON(s.tempLoss)}</b>\n` : "") +
      `\n  Thu game: <b>${moneyWON(s.gameMoney)}</b>\n` +
      `  Thu bán: <b>${moneyWON(s.soldMoney)}</b>\n` +
      `  Lãi tạm: <b>${moneyWON(s.gameMoney)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(s.laiTam)}</b>\n` +
      `  Lãi thực: <b>${moneyWON(s.gameMoney)}</b> + <b>${moneyWON(s.soldMoney)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(s.laiThuc)}</b>`;

    lines.push(
      `• <b>${escapeHtml(l.lot)}</b>: Mua <b>${l.qty}</b> máy <b>${escapeHtml(l.model || "")}</b> | Tổng <b>${moneyWON(
        l.total
      )}</b> | Ví <code>${escapeHtml(String(l.wallet || "").toUpperCase())}</code>\n\n${statusLine}`
    );
  }

  const html = `🧪 <b>DANH SÁCH LÔ MÁY</b> (Tất cả)\n\n${lines.join("\n\n")}`;
  await send(chatId, html, { reply_markup: leftKb() });
}

async function listLots20(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const walletLogs = await readWalletLog();

  if (lots.length === 0) {
    await send(chatId, `Chưa có lô nào hết á 😝\nBấm <b>📱 Mua Máy (Lô)</b> để tạo lô nha~`, { reply_markup: leftKb() });
    return;
  }

  const sorted = [...lots].sort((a, b) => (a.ts < b.ts ? 1 : -1)).slice(0, 20);

  const lines = [];
  for (const l of sorted) {
    const s = await computeLotSummary(l, phones, walletLogs);

    const statusLine =
      `  Trạng thái: Ăn <b>${s.anCount}</b> (HQ:${s.hq} / QR:${s.qr} / DB:${s.db}) / Huề <b>${s.hue}</b> / Tạch <b>${s.tach}</b> / New <b>${s.neu}</b> / Sold <b>${s.soldCount}</b>\n` +
      `  Còn lại: <b>${s.remainCount}</b> máy chưa bán\n` +
      (s.tempLoss > 0 ? `  ⚠️ <b>Tạm lỗ:</b> <b>${moneyWON(s.tempLoss)}</b>\n` : "") +
      `\n  Thu game: <b>${moneyWON(s.gameMoney)}</b>\n` +
      `  Thu bán: <b>${moneyWON(s.soldMoney)}</b>\n` +
      `  Lãi tạm: <b>${moneyWON(s.gameMoney)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(s.laiTam)}</b>\n` +
      `  Lãi thực: <b>${moneyWON(s.gameMoney)}</b> + <b>${moneyWON(s.soldMoney)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(s.laiThuc)}</b>`;

    lines.push(
      `• <b>${escapeHtml(l.lot)}</b>: Mua <b>${l.qty}</b> máy <b>${escapeHtml(l.model || "")}</b> | Tổng <b>${moneyWON(
        l.total
      )}</b> | Ví <code>${escapeHtml(String(l.wallet || "").toUpperCase())}</code>\n\n${statusLine}`
    );
  }

  const html = `🧪 <b>DANH SÁCH LÔ MÁY</b> (20 lô gần nhất)\n\n${lines.join("\n\n")}`;
  await send(chatId, html, { reply_markup: leftKb() });
}

/* =========================
 * Danh sách máy
 * ========================= */
async function listPhonesPretty(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  if (phones.length === 0) {
    await send(chatId, `Chưa có máy nào hết á 😝`, { reply_markup: leftKb() });
    return;
  }

  const lotMap = new Map(lots.map((l) => [l.lot, l]));
  const sorted = [...phones].sort((a, b) => (a.phone_id < b.phone_id ? 1 : -1)).slice(0, 60);

  const lines = sorted.map((p) => {
    const l = lotMap.get(p.lot);
    const lotNo = p.lot?.slice(2) || "";
    const device = extractDeviceLabelFromLotNote(l?.note || p.note || "", l?.model || "");
    const deviceText = device ? ` ${escapeHtml(device)}` : "";
    const soldText = p.sold ? "✅ Đã bán" : "⏳ Còn giữ";

    const result = isProfitPhone(p)
      ? p.game === "hq"
        ? "🎁 HQ"
        : p.game === "qr"
        ? "🔳 QR"
        : "⚽ DB"
      : isLossPhone(p)
      ? "😵 Tạch"
      : isTiePhone(p)
      ? "😌 Huề"
      : "🆕 New";

    return `• <b>Mã ${escapeHtml(lotNo)}</b> - <code>${escapeHtml(p.phone_id)}</code> | ${result} | ${soldText} |${deviceText}`;
  });

  const html = `📋 <b>DANH SÁCH MÁY</b> (60 máy gần nhất)\n\n${lines.join("\n")}`;
  await send(chatId, html, { reply_markup: leftKb() });
}

/* =========================
 * PHÂN TÍCH MUA MÁY:
 * - theo từng lô (10 lô / message, tự gửi tiếp)
 * - cuối: tổng quát ALL lô
 * ========================= */
function bar(pctVal, width = 18) {
  const n = Math.max(0, Math.min(width, Math.round((pctVal / 100) * width)));
  return "█".repeat(n) + " ".repeat(width - n);
}
function pct(n, d) {
  if (!d || d <= 0) return 0;
  return Math.round((n / d) * 100);
}
function chunkBy10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}
function safeSplitBlocks(blocks, header, maxLen = 3600) {
  const pages = [];
  let cur = header;
  for (const b of blocks) {
    const next = (cur ? cur + "\n\n" : "") + b;
    if (next.length > maxLen) {
      if (cur) pages.push(cur);
      cur = header + "\n\n" + b;
      if (cur.length > maxLen) {
        pages.push(cur.slice(0, maxLen));
        cur = header + "\n\n" + b.slice(maxLen);
      }
    } else {
      cur = next;
    }
  }
  if (cur) pages.push(cur);
  return pages;
}

async function reportMachineAnalysis(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const walletLogs = await readWalletLog();
  const payouts = await getMachinePayouts();

  if (lots.length === 0) {
    await send(chatId, `Chưa có lô nào để phân tích hết á 😝`, { reply_markup: leftKb() });
    return;
  }

  const sortedLots = [...lots].sort((a, b) => (a.ts < b.ts ? 1 : -1));

  // A) THEO LÔ
  const lotBlocks = [];
  let tempLossTotal = 0;

  for (const l of sortedLots) {
    const s = await computeLotSummary(l, phones, walletLogs);
    tempLossTotal += s.tempLoss || 0;

    const lotNo = l.lot.slice(2);
    const deviceLabel = extractDeviceLabelFromLotNote(l.note || "", l.model || "");
    const deviceText = deviceLabel ? ` | <b>${escapeHtml(deviceLabel)}</b>` : "";

    const block =
      `🧾 <b>PHÂN TÍCH LÔ MÃ ${escapeHtml(lotNo)}</b>${deviceText}\n` +
      `• Mua: <b>${l.qty}</b> máy <b>${escapeHtml(l.model || "")}</b> | Tổng: <b>${moneyWON(l.total)}</b>\n` +
      `• Trạng thái: Ăn <b>${s.anCount}</b> (HQ:${s.hq}/QR:${s.qr}/DB:${s.db}) / Huề <b>${s.hue}</b> / Tạch <b>${s.tach}</b> / New <b>${s.neu}</b> / Sold <b>${s.soldCount}</b>\n` +
      `• Còn lại: <b>${s.remainCount}</b> máy chưa bán\n` +
      (s.tempLoss > 0 ? `⚠️ <b>Tạm lỗ:</b> <b>${moneyWON(s.tempLoss)}</b>\n` : "") +
      `\n🎮 Thu game: <b>${moneyWON(s.gameMoney)}</b>\n` +
      `💸 Thu bán: <b>${moneyWON(s.soldMoney)}</b>\n` +
      `🧮 Lãi tạm: <b>${moneyWON(s.gameMoney)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(s.laiTam)}</b>\n` +
      `🧾 Lãi thực: <b>${moneyWON(s.gameMoney)}</b> + <b>${moneyWON(s.soldMoney)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(s.laiThuc)}</b>`;

    lotBlocks.push(block);
  }

  const group10 = chunkBy10(lotBlocks);
  for (let pageIndex = 0; pageIndex < group10.length; pageIndex++) {
    const header = `📊 <b>PHÂN TÍCH MUA MÁY</b> (theo lô) • Trang ${pageIndex + 1}/${group10.length}`;
    const pages = safeSplitBlocks(group10[pageIndex], header);
    for (const p of pages) {
      await send(chatId, p, { reply_markup: leftKb() });
    }
  }

  // B) TỔNG QUÁT ALL
  const totalBuy = lots.reduce((a, b) => a + (b.total || 0), 0);

  const totalSell = walletLogs
    .filter((l) => l.type === "machine_sell" && l.ref_type === "lot")
    .reduce((a, b) => a + (b.amount || 0), 0);

  const hq = phones.filter((p) => String(p.game || "") === "hq").length;
  const qr = phones.filter((p) => String(p.game || "") === "qr").length;
  const db = phones.filter((p) => String(p.game || "") === "db").length;
  const totalGame = hq * payouts.hq + qr * payouts.qr + db * payouts.db;

  const totalBack = totalGame + totalSell;
  const net = totalBack - totalBuy;

  const totalPhones = phones.length || 0;
  const tach = phones.filter((p) => isLossPhone(p)).length;
  const hue = phones.filter((p) => isTiePhone(p)).length;
  const anCount = hq + qr + db;
  const neu = phones.filter((p) => isNewPhone(p)).length;
  const sold = phones.filter((p) => !!p.sold).length;

  // % lời/lỗ theo TIỀN
  const base = Math.max(1, totalBuy);
  const loiPct = net >= 0 ? Math.round((net / base) * 100) : 0;
  const loPct = net < 0 ? Math.round((Math.abs(net) / base) * 100) : 0;

  const machineNewPct = totalPhones > 0 ? pct(neu, totalPhones) : 0;
  const machineAnPct = totalPhones > 0 ? pct(anCount, totalPhones) : 0;
  const machineTachPct = totalPhones > 0 ? pct(tach, totalPhones) : 0;
  const machineHuePct = totalPhones > 0 ? pct(hue, totalPhones) : 0;
  const machineSoldPct = totalPhones > 0 ? pct(sold, totalPhones) : 0;

  const maxMoney = Math.max(1, totalBuy, totalGame, totalSell, totalBack);
  const moneyPct = (x) => Math.round((x / maxMoney) * 100);

  const html =
    `📊 <b>PHÂN TÍCH MUA MÁY</b> (tổng quát ALL lô)\n\n` +
    `💳 <b>Tổng tiền mua:</b> <b>${moneyWON(totalBuy)}</b>\n` +
    `🎮 <b>Thu game (HQ/QR/DB):</b> <b>${moneyWON(totalGame)}</b>\n` +
    `💸 <b>Thu bán:</b> <b>${moneyWON(totalSell)}</b>\n` +
    `💰 <b>Tổng thu về:</b> <b>${moneyWON(totalBack)}</b>\n` +
    `🧾 <b>Lời còn lại:</b> <b>${moneyWON(net)}</b>\n` +
    `⚠️ <b>Tạm lỗ đang giữ:</b> <b>${moneyWON(tempLossTotal)}</b>\n\n` +
    `Máy (đếm theo kết quả + Sold độc lập)\n` +
    `• Ăn được: <b>${anCount}</b> máy (HQ:${hq} / QR:${qr} / DB:${db})\n` +
    `• Tạch: <b>${tach}</b> máy\n` +
    `• Huề: <b>${hue}</b> máy\n` +
    `• New: <b>${neu}</b> máy\n` +
    `• Sold: <b>${sold}</b> máy\n\n` +
    `📌 <b>Biểu đồ máy</b>\n` +
    `New  : ${bar(machineNewPct)} ${machineNewPct}% (${neu})\n` +
    `Ăn   : ${bar(machineAnPct)} ${machineAnPct}% (${anCount})\n` +
    `Tạch : ${bar(machineTachPct)} ${machineTachPct}% (${tach})\n` +
    `Huề  : ${bar(machineHuePct)} ${machineHuePct}% (${hue})\n` +
    `Sold : ${bar(machineSoldPct)} ${machineSoldPct}% (${sold})\n\n` +
    `💸 <b>Biểu đồ tiền</b>\n` +
    `Mua        : ${bar(moneyPct(totalBuy))} ${moneyWON(totalBuy)}\n` +
    `Thu game   : ${bar(moneyPct(totalGame))} ${moneyWON(totalGame)}\n` +
    `Thu bán    : ${bar(moneyPct(totalSell))} ${moneyWON(totalSell)}\n` +
    `Tổng thu về: ${bar(moneyPct(totalBack))} ${moneyWON(totalBack)}\n\n` +
    `Lời : ${loiPct}%\n` +
    `Lỗ  : ${loPct}%`;

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

// ✅ NEW: only accept quick revenue when FIRST TOKEN is command (hq/qr/db/them)
function detectQuickRevenueFirstToken(normText) {
  const t = normalizeSpaces(String(normText || ""));
  if (!t) return "";
  const first = t.split(" ")[0] || "";
  if (first === "hq" || first === "qr" || first === "db") return first;
  if (first === "them" || first === "thu" || first === "khac" || first === "ngoai") return "other";
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
 * Help
 * ========================= */
function helpText() {
  return (
    `📘 <b>HƯỚNG DẪN</b> (WON ₩)\n\n` +
    `✅ <b>Mua lô</b> (tiền là <b>TỔNG</b>):\n` +
    `• <code>mua 3ss 50k uri note4</code>\n\n` +
    `✅ <b>Chốt lô</b> (đều OK):\n` +
    `• <code>ma01 hq1 tach2</code>\n` +
    `• <code>ma01 tach 3</code>\n` +
    `• <code>ma01 3 tach</code>\n\n` +
    `✅ <b>Reset lô</b>:\n` +
    `• <code>ma01 reset</code>\n\n` +
    `✅ <b>Sửa (overwrite)</b>:\n` +
    `• <code>sua ma01 hq1 tach2</code>\n` +
    `• <code>sua ma01 reset</code>\n` +
    `• <code>sua ma01 ma09</code>\n\n` +
    `✅ <b>Bán</b> (tiền là <b>TỔNG</b>):\n` +
    `• <code>ban 2 ss 50k ma01 uri</code>\n\n` +
    `✅ <b>Thu nhanh (doanh thu chính)</b>:\n` +
    `• <code>db 100k</code> / <code>hq 200k</code> / <code>qr 57k</code> / <code>them 0.5k</code>\n\n` +
    `✅ <b>MAIL (mời mượn máy)</b>:\n` +
    `• <code>A Bình minhtiktok@ hq</code>\n` +
    `• <code>Abinh minhtiktok@ tạch</code>\n` +
    `• <code>A Tiến minhtik@ 01079876999 db</code> (số phía sau là ghi chú)\n` +
    `• Sửa: <code>sua A minhtiktok@ tach</code>\n\n` +
    `<i>Tip:</i> Gõ có dấu hay không dấu đều hiểu 😚`
  );
}

/* =========================
 * SESSION handler (existing flows)
 * ========================= */
async function handleSessionInput(chatId, userName, text) {
  const sess = getSession(chatId);
  if (!sess) return false;

  // DANGEROUS RESET
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
      await send(chatId, `Bạn gõ kiểu: <code>mua 3ss 50k uri note4</code> nha~`, { reply_markup: leftKb() });
      return true;
    }

    // Nếu có wallet + có note => tạo lô luôn, KHÔNG hỏi note nữa
    if (parsed.wallet && parsed.note) {
      const finalNote = normalizeSpaces([parsed.model, parsed.note].filter(Boolean).join(" | "));
      const r = await addLot({
        qty: parsed.qty,
        model: parsed.model,
        total_price: Math.round(parsed.totalPrice),
        wallet: parsed.wallet,
        note: finalNote,
        chatId,
      });

      clearSession(chatId);

      const deviceLabel = extractDeviceLabelFromLotNote(finalNote, parsed.model);
      const noteLine = deviceLabel ? `Note: <b>${escapeHtml(deviceLabel)}</b>\n` : "";

      const html =
        `✅ <b>Xong rồi nè</b> 🥳\n` +
        `Tạo lô: <b>MÃ ${escapeHtml(r.lot.slice(2))}</b>\n` +
        `Mua: <b>${parsed.qty}</b> máy <b>${escapeHtml(parsed.model)}</b>\n` +
        noteLine +
        `Tổng: <b>${moneyWON(Math.round(parsed.totalPrice))}</b>\n` +
        `Ví: <code>${escapeHtml(String(parsed.wallet || "").toUpperCase())}</code>`;

      await send(chatId, html, { reply_markup: leftKb() });
      return true;
    }

    sess.data = parsed;

    if (parsed.wallet) {
      sess.step = "note";
      setSession(chatId, sess);
      await send(
        chatId,
        `Okie 😚 <b>Mua lô</b> <code>${parsed.qty}</code> máy <b>${escapeHtml(parsed.model)}</b>, tổng <b>${moneyWON(
          parsed.totalPrice
        )}</b>\nVí: <code>${escapeHtml(parsed.wallet.toUpperCase())}</code>\n\nNhập <i>note</i> (vd <b>Note4</b>) hoặc <code>-</code> để bỏ qua nha~`,
        { reply_markup: leftKb() }
      );
      return true;
    }

    sess.step = "wallet";
    setSession(chatId, sess);
    await send(
      chatId,
      `Okie 😚 <b>Mua lô</b> <code>${parsed.qty}</code> máy <b>${escapeHtml(parsed.model)}</b>, tổng <b>${moneyWON(
        parsed.totalPrice
      )}</b>\n\nTính tiền <b>ví nào</b>? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
    return true;
  }

  if (sess.flow === "buy_lot" && sess.step === "wallet") {
    const wallet = parseWalletShortcut(text) || normalizeForParse(text).trim();
    const w = ["hana", "uri", "kt", "tm"].includes(wallet) ? wallet : "";
    if (!w) {
      await send(chatId, `Ví chưa đúng á 😝 Nhập <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code> nha~`, {
        reply_markup: leftKb(),
      });
      return true;
    }
    sess.data.wallet = w;

    if (sess.data.note) {
      const parsed = sess.data;
      const finalNote = normalizeSpaces([parsed.model, parsed.note].filter(Boolean).join(" | "));
      const r = await addLot({
        qty: parsed.qty,
        model: parsed.model,
        total_price: Math.round(parsed.totalPrice),
        wallet: parsed.wallet,
        note: finalNote,
        chatId,
      });
      clearSession(chatId);

      const deviceLabel = extractDeviceLabelFromLotNote(finalNote, parsed.model);
      const noteLine = deviceLabel ? `Note: <b>${escapeHtml(deviceLabel)}</b>\n` : "";

      const html =
        `✅ <b>Xong rồi nè</b> 🥳\n` +
        `Tạo lô: <b>MÃ ${escapeHtml(r.lot.slice(2))}</b>\n` +
        `Mua: <b>${parsed.qty}</b> máy <b>${escapeHtml(parsed.model)}</b>\n` +
        noteLine +
        `Tổng: <b>${moneyWON(Math.round(parsed.totalPrice))}</b>\n` +
        `Ví: <code>${escapeHtml(String(parsed.wallet || "").toUpperCase())}</code>`;

      await send(chatId, html, { reply_markup: leftKb() });
      return true;
    }

    sess.step = "note";
    setSession(chatId, sess);
    await send(chatId, `Nhập <i>note</i> (vd <b>Note4</b>) hoặc <code>-</code> để bỏ qua nha~`, { reply_markup: leftKb() });
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
      chatId,
    });

    clearSession(chatId);

    const deviceLabel = extractDeviceLabelFromLotNote(finalNote, sess.data.model);
    const noteLine = deviceLabel ? `Note: <b>${escapeHtml(deviceLabel)}</b>\n` : "";

    const html =
      `✅ <b>Xong rồi nè</b> 🥳\n` +
      `Tạo lô: <b>MÃ ${escapeHtml(r.lot.slice(2))}</b>\n` +
      `Mua: <b>${sess.data.qty}</b> máy <b>${escapeHtml(sess.data.model)}</b>\n` +
      noteLine +
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
    await send(
      chatId,
      `Bạn đang <b>bán</b> lô <b>MÃ ${escapeHtml(parsed.lot.slice(2))}</b> x<code>${parsed.qty}</code>, tiền <b>${moneyWON(
        parsed.totalPrice
      )}</b>\n\nTiền về <b>ví nào</b>? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
    return true;
  }

  if (sess.flow === "sell" && sess.step === "wallet") {
    const wallet = parseWalletShortcut(text) || normalizeForParse(text).trim();
    const w = ["hana", "uri", "kt", "tm"].includes(wallet) ? wallet : "";
    if (!w) {
      await send(chatId, `Ví chưa đúng á 😝 Nhập <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code> nha~`, {
        reply_markup: leftKb(),
      });
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
      await send(chatId, `Ví chưa đúng á 😝 Nhập <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code> nha~`, {
        reply_markup: rightKb(),
      });
      return true;
    }
    sess.data = { wallet: w };
    sess.step = "amount";
    setSession(chatId, sess);
    await send(chatId, `Okie. Bạn nhập <b>số dư mới</b> cho ví <code>${escapeHtml(w.toUpperCase())}</code> (vd <code>120k</code>) nha~`, {
      reply_markup: rightKb(),
    });
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

  // REVENUE EDIT (MAIN)
  if (sess.flow === "revenue_edit" && sess.step === "amount") {
    const amt = extractMoneyFromText(text);
    if (amt == null) {
      await send(chatId, `Nhập kiểu <code>120k</code> nha bạn iu~`, { reply_markup: rightKb() });
      return true;
    }
    clearSession(chatId);

    await addGameRevenue({ game: "all", type: "revenue_adjust", amount: amt, note: "SET_TOTAL", chatId, userName });
    await send(chatId, `✅ <b>Đã cộng chỉnh doanh thu</b>: <b>${moneyWON(amt)}</b>`, { reply_markup: rightKb() });
    return true;
  }

  // RESOLVE missing game (normal + overwrite)
  if (sess.flow === "resolve_need_game" && sess.step === "game") {
    const n = normalizeForParse(text).trim();
    const g = n.includes("hq") ? "hq" : n.includes("qr") ? "qr" : n.includes("db") ? "db" : "";
    if (!g) {
      await send(chatId, `Bạn chọn <code>hq</code> / <code>qr</code> / <code>db</code> nha~`, { reply_markup: leftKb() });
      return true;
    }

    const data = sess.data || {};
    clearSession(chatId);

    if (data.overwrite === true && data.lot && Array.isArray(data.segments)) {
      const filled = data.segments.map((s) => {
        if (s.kind === "an" && (!s.game || s.game === "")) return { ...s, game: g };
        return s;
      });
      await resetLotResults(data.lot);
      await applyLotResolve({ chatId, lot: data.lot, segments: filled });
      return true;
    }

    const lot = data.lot;
    const count = Number(data.count || 1) || 1;
    await applyLotResolve({ chatId, lot, segments: [{ kind: "an", count, game: g }] });
    return true;
  }

  return false;
}

/* =========================
 * MAIL_LOG FEATURE (per spec)
 * Sheet: MAIL_LOG columns:
 * A id | B name | C mail | D result(HQ/QR/DB/TACH) | E note | F created_at | G status(ACTIVE/DONE)
 * ========================= */

// Always default after @ is gmail.com unless user typed another domain.
// - "xxx@" => "xxx@gmail.com"
// - "xxx"  => "xxx@gmail.com"
// - "xxx@yahoo.com" => keep
function normalizeMailFull(raw) {
  const s = removeDiacritics(String(raw || "").trim().toLowerCase());
  if (!s) return "";
  if (!s.includes("@")) return `${s}@gmail.com`;
  if (s.endsWith("@")) return `${s}gmail.com`;
  return s;
}

// For menu "📧 Mail": want only localpart + "@", nothing else.
// - gmail => "xxx@"
// - other domain => keep full "xxx@yahoo.com" (still a mail)
function toMailShortForCopy(full) {
  const s = String(full || "").trim().toLowerCase();
  if (!s.includes("@")) return "";
  if (s.endsWith("@gmail.com")) return s.replace(/@gmail\.com$/i, "@");
  return s;
}

function detectMailResultToken(normTk) {
  const tk = normTk;
  if (!tk) return "";
  // tạch
  if (tk === "tach" || tk === "tạch" || tk === "tac" || tk === "chet") return "TACH";
  // qr
  if (tk === "qr") return "QR";
  // db / bd
  if (tk === "db" || tk === "bd" || tk === "da" || tk === "bong" || tk === "dabong") return "DB";
  // hq / hộp quà
  if (tk === "hq" || tk === "hopqua" || tk === "hop" || tk === "qua" || tk === "hopqua") return "HQ";
  return "";
}

function prettyResultText(result) {
  if (result === "TACH") return "tạch";
  if (result === "HQ") return "ok hộp quà";
  if (result === "QR") return "ok QR";
  if (result === "DB") return "ok đá bóng";
  return "";
}

// ✅ FIX DỨT ĐIỂM: parse MAIL theo rawTokens, normalize từng token riêng lẻ
// - Không dùng normTokens[] để tránh lệch index khi token có số (minhtiktok29@)
function parseMailLine(text) {
  const raw = normalizeSpaces(String(text || ""));
  if (!raw) return null;

  const rawTokens = raw.split(/\s+/).filter(Boolean);
  if (rawTokens.length < 3) return null;

  // must start with A / Abinh / A.Bình ... (case-insensitive)
  const firstRaw = rawTokens[0] || "";
  const firstNorm = removeDiacritics(firstRaw).toLowerCase();

  let startIdx = 0;

  // Case: "A" or "A." => skip token 0
  if (firstNorm === "a" || firstNorm === "a." || firstNorm === "a," || firstNorm === "a-") {
    startIdx = 1;
  } else if (firstNorm.startsWith("a") && firstNorm.length > 1) {
    // Case: "Abinh" / "A.Binh" / "A-Binh" => strip the leading A marker and keep the rest as name token
    const stripped = firstRaw.replace(/^A[\.\,\-\_]?/i, "");
    rawTokens[0] = stripped || firstRaw;
    startIdx = 0;
  } else {
    return null;
  }

  let nameParts = [];
  let mailToken = "";
  let result = "";
  let noteParts = [];
  let foundMail = false;

  for (let i = startIdx; i < rawTokens.length; i++) {
    const rt = rawTokens[i];
    const nt = normalizeForParse(rt); // normalize token-by-token (no index mismatch)

    // mail detection: ANY token containing "@"
    if (!foundMail && rt.includes("@")) {
      mailToken = rt;
      foundMail = true;
      continue;
    }

    const res = detectMailResultToken(nt);
    if (res) {
      result = res;
      continue;
    }

    if (!foundMail) {
      nameParts.push(rt);
    } else {
      noteParts.push(rt);
    }
  }

  const name = normalizeSpaces(nameParts.join(" "));
  const mail = normalizeMailFull(mailToken);

  if (!name || !mail || !result) return null;

  const note = normalizeSpaces(noteParts.join(" "));
  return { name, mail, result, note };
}

// "sua A minhtiktok@ tach (note?)"  OR "sua A ...@ hq"
function parseMailEdit(text) {
  const raw = normalizeSpaces(String(text || ""));
  const norm = normalizeForParse(raw);
  if (!norm.startsWith("sua ")) return null;

  // remove "sua "
  const restRaw = raw.slice(raw.toLowerCase().indexOf("sua") + 3).trim();
  const restNorm = normalizeForParse(restRaw);

  // allow "A ..." after sua
  const parsed = parseMailLine(restRaw);
  if (parsed) return parsed;

  // also allow: "sua mail xxx@ hq ..."
  if (restNorm.startsWith("mail ")) {
    const tRaw = restRaw.replace(/^mail\s+/i, "");
    const toks = tRaw.split(/\s+/).filter(Boolean);
    let mailTk = "";
    let result = "";
    let note = "";
    for (const tk of toks) {
      const nt = normalizeForParse(tk);
      if (!mailTk && tk.includes("@")) mailTk = tk;
      const r = detectMailResultToken(nt);
      if (r) result = r;
      else if (mailTk) note += tk + " ";
    }
    mailTk = normalizeMailFull(mailTk);
    note = normalizeSpaces(note);
    if (!mailTk || !result) return null;
    return { name: "", mail: mailTk, result, note };
  }

  return null;
}

async function nextMailId() {
  const rows = await getValues("MAIL_LOG!A2:A");
  let max = 0;
  for (const r of rows) {
    const m = String(r[0] || "").match(/^MAIL(\d+)$/i);
    if (m) {
      const n = Number(m[1]);
      if (Number.isFinite(n)) max = Math.max(max, n);
    }
  }
  return "MAIL" + String(max + 1).padStart(2, "0");
}

async function addMailLog({ name, mail, result, note }) {
  const id = await nextMailId();
  await appendValues("MAIL_LOG!A1", [[id, name, mail, result, note || "", nowIso(), "ACTIVE"]]);
  return id;
}

async function readMailLog() {
  const rows = await getValues("MAIL_LOG!A2:G");
  return rows
    .filter((r) => r.some((c) => String(c || "").trim() !== ""))
    .map((r, idx) => ({
      rowNumber: idx + 2, // sheet row number
      id: String(r[0] || ""),
      name: String(r[1] || ""),
      mail: String(r[2] || ""),
      result: String(r[3] || ""),
      note: String(r[4] || ""),
      created_at: String(r[5] || ""),
      status: String(r[6] || "ACTIVE"),
    }));
}

function daysLeftForOk(createdAtIso) {
  const d0 = dayjs(createdAtIso);
  if (!d0.isValid()) return 0;
  const diff = dayjs().startOf("day").diff(d0.startOf("day"), "day");
  return Math.max(0, 14 - diff);
}

function shouldAutoDone(row) {
  // only OK (HQ/QR/DB) auto-done after >14 days
  if (!row) return false;
  if (String(row.result || "").toUpperCase() === "TACH") return false;
  const left = daysLeftForOk(row.created_at);
  return left <= 0;
}

async function autoDoneIfNeeded(rows) {
  // Update status to DONE for OK rows beyond 14 days
  const toUpdate = rows.filter((r) => String(r.status || "").toUpperCase() !== "DONE" && shouldAutoDone(r));
  for (const r of toUpdate) {
    await updateValues(`MAIL_LOG!G${r.rowNumber}:G${r.rowNumber}`, [["DONE"]]);
    r.status = "DONE";
  }
}

async function updateLatestMailByMail({ mail, result, note, name }) {
  const rows = await readMailLog();
  const targetMail = normalizeMailFull(mail);
  const matches = rows
    .filter((r) => normalizeMailFull(r.mail) === targetMail)
    .sort((a, b) => (a.created_at > b.created_at ? 1 : -1)); // cũ->mới
  const last = matches[matches.length - 1];
  if (!last) return { ok: false, reason: "Không tìm thấy mail để sửa" };

  // Update result (D), note (E) if provided, name (B) if provided
  const updates = [];
  if (result) updates.push({ col: "D", val: String(result).toUpperCase() });
  if (note != null && note !== "") updates.push({ col: "E", val: String(note) });
  if (name != null && name !== "") updates.push({ col: "B", val: String(name) });

  for (const u of updates) {
    await updateValues(`MAIL_LOG!${u.col}${last.rowNumber}:${u.col}${last.rowNumber}`, [[u.val]]);
  }

  return { ok: true, row: last.rowNumber };
}

async function sendDanhSachDaMoi(chatId) {
  const rows = await readMailLog();
  await autoDoneIfNeeded(rows);

  // sort cũ -> mới
  rows.sort((a, b) => (a.created_at > b.created_at ? 1 : -1));

  const tach = rows.filter((r) => String(r.result || "").toUpperCase() === "TACH");
  const ok = rows.filter((r) => String(r.result || "").toUpperCase() !== "TACH");

  const out = [];
  out.push(`📋 <b>DANH SÁCH ĐÃ MỜI</b>\n`);

  out.push(`❌ <b>TẠCH</b>`);
  if (tach.length === 0) out.push(`<i>(trống)</i>`);
  for (const r of tach) {
    const nm = titleCaseVi(r.name);
    const dt = dayjs(r.created_at).isValid() ? dayjs(r.created_at).format("DD/MM/YYYY") : "";
    out.push(`• Mời <b>A. ${escapeHtml(nm)}</b> / ${escapeHtml(dt)}`);
  }

  out.push(`\n✅ <b>OK</b>`);
  if (ok.length === 0) out.push(`<i>(trống)</i>`);
  for (const r of ok) {
    const nm = titleCaseVi(r.name);
    const dt = dayjs(r.created_at).isValid() ? dayjs(r.created_at).format("DD/MM/YYYY") : "";
    const left = daysLeftForOk(r.created_at);
    const resTxt = prettyResultText(String(r.result || "").toUpperCase());
    out.push(
      `• Mời <b>A. ${escapeHtml(nm)}</b> ${escapeHtml(resTxt)} / ${escapeHtml(dt)} (còn <b>${left}</b> ngày điểm danh)`
    );
  }

  await send(chatId, out.join("\n"), { reply_markup: leftKb() });
}

async function sendMailOnlyList(chatId) {
  const rows = await readMailLog();
  // collect unique mails only, preserve first-seen order cũ->mới
  rows.sort((a, b) => (a.created_at > b.created_at ? 1 : -1));

  const seen = new Set();
  const mails = [];
  for (const r of rows) {
    const full = String(r.mail || "").trim().toLowerCase();
    if (!full || !full.includes("@")) continue; // skip phone-only lines
    const short = toMailShortForCopy(full);
    if (!short) continue;
    if (seen.has(short)) continue;
    seen.add(short);
    mails.push(short);
  }

  await send(chatId, mails.join("\n"), { reply_markup: leftKb(), __raw: true });
}

/* =========================
 * Cron placeholder
 * ========================= */
cron.schedule("*/30 * * * *", async () => {
  // Không auto spam. Status DONE sẽ được update khi bấm danh sách đã mời.
});

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
    await send(chatId, `➡️ <b>Menu Phải</b> đây nè~`, { reply_markup: rightKb() });
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
  if (text === "💼 Xem Ví") return reportWallets(chatId);
  if (text === "📘 Hướng Dẫn") return send(chatId, helpText(), { reply_markup: rightKb() });

  if (text === "✏️ Sửa Số Dư Ví") {
    setSession(chatId, { flow: "wallet_edit", step: "wallet", data: {} });
    await send(chatId, `✏️ <b>Sửa số dư ví</b>\nBạn chọn ví: <code>hana</code> / <code>uri</code> / <code>kt</code> / <code>tm</code>`, {
      reply_markup: rightKb(),
    });
    return;
  }

  if (text === "✏️ Sửa Tổng Doanh Thu") {
    setSession(chatId, { flow: "revenue_edit", step: "amount", data: {} });
    await send(chatId, `✏️ <b>Sửa tổng doanh thu</b>\nNhập số tiền cần <b>cộng chỉnh</b> (vd <code>60k</code>) nha~`, {
      reply_markup: rightKb(),
    });
    return;
  }

  if (text === "🧠 Smart Parse: Bật/Tắt") {
    const on = await toggleSmartParse();
    await send(chatId, `🧠 Smart Parse hiện đang: <b>${on ? "BẬT ✅" : "TẮT ❌"}</b>`, { reply_markup: rightKb() });
    return;
  }

  if (text === "🧨 Xóa Sạch Dữ Liệu") {
    setSession(chatId, { flow: "reset", step: "pass" });
    await send(chatId, `⚠️ <b>Khu vực nguy hiểm</b> nha bạn iu 😵‍💫\n🔐 Vui lòng điền pass để <b>XÓA SẠCH</b> dữ liệu ^^`, {
      reply_markup: rightKb(),
    });
    return;
  }

  // left menu
  if (text === "📱 Mua Máy (Lô)") {
    setSession(chatId, { flow: "buy_lot", step: "sentence", data: {} });
    await send(chatId, `📱 <b>Mua Máy (Lô)</b>\nBạn gõ: <code>mua 3ss 50k uri note4</code> nha~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "💸 Bán Máy") {
    setSession(chatId, { flow: "sell", step: "sentence", data: {} });
    await send(chatId, `💸 <b>Bán Máy</b>\nBạn gõ: <code>ban 2 ss 50k ma01 uri</code> nha~`, { reply_markup: leftKb() });
    return;
  }
  if (text === "🧪 Kiểm Tra Máy (Tất cả)") return listLotsAll(chatId);
  if (text === "🧪 20 Lô Gần Nhất") return listLots20(chatId);
  if (text === "📋 Danh Sách Máy") return listPhonesPretty(chatId);
  if (text === "📊 Phân Tích Mua Máy") return reportMachineAnalysis(chatId);

  // Reset / sửa menu help
  if (text === "♻️ Reset Lô") {
    await send(
      chatId,
      `♻️ <b>Reset Lô</b>\nGõ: <code>ma01 reset</code>\n\nHoặc reset bằng SUA:\n<code>sua ma01 reset</code>`,
      { reply_markup: leftKb() }
    );
    return;
  }
  if (text === "✏️ Sửa Lô / Đổi Mã") {
    await send(
      chatId,
      `✏️ <b>Sửa Lô / Đổi Mã</b>\n\n• Sửa kết quả:\n<code>sua ma01 hq1 tach2</code>\n\n• Đổi mã lô:\n<code>sua ma01 ma09</code>`,
      { reply_markup: leftKb() }
    );
    return;
  }

  // ✅ MAIL MENU
  if (text === "📋 Danh sách đã mời") {
    await sendDanhSachDaMoi(chatId);
    return;
  }
  if (text === "📧 Mail") {
    await sendMailOnlyList(chatId);
    return;
  }

  // session
  if (await handleSessionInput(chatId, userName, text)) return;

  // ✅ MAIL edit
  const mailEdit = parseMailEdit(text);
  if (mailEdit) {
    const r = await updateLatestMailByMail({
      mail: mailEdit.mail,
      result: mailEdit.result,
      note: mailEdit.note,
      name: mailEdit.name,
    });
    if (!r.ok) {
      await send(chatId, `🥺 ${escapeHtml(r.reason || "Không sửa được")}`, { reply_markup: leftKb() });
      return;
    }
    const resTxt = prettyResultText(mailEdit.result);
    await send(chatId, `✅ Đã sửa: <b>${escapeHtml(toMailShortForCopy(mailEdit.mail))}</b> → ${escapeHtml(resTxt)} nha~`, {
      reply_markup: leftKb(),
    });
    return;
  }

  // ✅ MAIL add
  const mailLine = parseMailLine(text);
  if (mailLine) {
    await addMailLog(mailLine);
    const today = dayjs().format("DD/MM/YYYY");
    const nm = titleCaseVi(mailLine.name);
    const resTxt = prettyResultText(mailLine.result);
    await send(chatId, `Mời <b>A. ${escapeHtml(nm)}</b> ${escapeHtml(resTxt)} / ${escapeHtml(today)}`, {
      reply_markup: leftKb(),
    });
    return;
  }

  // ✅ SUA commands (lot)
  const sua = parseSuaCommand(text);
  if (sua) {
    if (sua.type === "rename") {
      const r = await renameLotEverywhere(sua.oldLot, sua.newLot);
      if (!r.ok) {
        await send(chatId, `🥺 Không đổi được mã lô: <b>${escapeHtml(r.reason || "Lỗi")}</b>`, { reply_markup: leftKb() });
        return;
      }
      await send(chatId, `✅ Đổi mã lô <b>${escapeHtml(sua.oldLot)}</b> → <b>${escapeHtml(sua.newLot)}</b> xong rồi nè~`, {
        reply_markup: leftKb(),
      });
      return;
    }

    if (sua.type === "reset") {
      const changed = await resetLotResults(sua.lot);
      await send(chatId, `✅ Reset lô <b>${escapeHtml(sua.lot)}</b> về <b>New</b> xong rồi~ (${changed} máy)`, {
        reply_markup: leftKb(),
      });
      return;
    }

    if (sua.type === "overwrite_resolve") {
      if (sua.missingGame) {
        setSession(chatId, {
          flow: "resolve_need_game",
          step: "game",
          data: { lot: sua.lot, segments: sua.segments, overwrite: true },
        });
        await send(chatId, `Bạn đang ghi <b>Ăn</b> mà chưa nói HQ/QR/DB.\nNhập <code>hq</code>/<code>qr</code>/<code>db</code> nha~`, {
          reply_markup: leftKb(),
        });
        return;
      }

      await resetLotResults(sua.lot);
      await applyLotResolve({ chatId, lot: sua.lot, segments: sua.segments });
      return;
    }
  }

  // ✅ direct reset: "ma01 reset"
  const resetCmd = parseLotResetCommand(text);
  if (resetCmd) {
    const changed = await resetLotResults(resetCmd.lot);
    await send(chatId, `✅ Reset lô <b>${escapeHtml(resetCmd.lot)}</b> về <b>New</b> xong rồi~ (${changed} máy)`, {
      reply_markup: leftKb(),
    });
    return;
  }

  // resolve lot
  const lotCmd = parseLotResolve(text);
  if (lotCmd && lotCmd.segments && lotCmd.segments.length > 0) {
    if (lotCmd.missingGame) {
      setSession(chatId, {
        flow: "resolve_need_game",
        step: "game",
        data: { lot: lotCmd.lot, count: lotCmd.missingGameCount || 1, overwrite: false },
      });
      await send(chatId, `Bạn đang ghi <b>Ăn</b> mà chưa nói HQ/QR/DB.\nNhập <code>hq</code>/<code>qr</code>/<code>db</code> nha~`, {
        reply_markup: leftKb(),
      });
      return;
    }
    await applyLotResolve({ chatId, lot: lotCmd.lot, segments: lotCmd.segments });
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
      `Mình hiểu bạn đang <b>bán</b> lô <b>MÃ ${escapeHtml(sell.lot.slice(2))}</b> x<code>${sell.qty}</code> giá <b>${moneyWON(
        sell.totalPrice
      )}</b>\n\nTiền về ví nào? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
    return;
  }

  // ✅ quick revenue (MAIN doanh thu) — FIX: chỉ nhận khi FIRST TOKEN là lệnh thu
  const norm = normalizeForParse(text);
  const quickGame = detectQuickRevenueFirstToken(norm);
  const amt = extractMoneyFromText(text);

  if (quickGame && amt != null) {
    const g = quickGame === "other" ? "other" : quickGame;
    const type = g === "other" ? "other" : "manual";
    await addGameRevenue({ game: g, type, amount: amt, note: "input", chatId, userName });
    await send(chatId, `✅ <b>Đã ghi doanh thu</b> <code>${escapeHtml(g.toUpperCase())}</code>: <b>${moneyWON(amt)}</b>`, {
      reply_markup: mainKb(),
    });
    return;
  }

  // Smart Parse buy lot without menu
  if (await isSmartParseEnabled()) {
    const buy = parseBuySentence(text);
    if (buy && !buy.incomplete) {
      if (buy.wallet && buy.note) {
        const finalNote = normalizeSpaces([buy.model, buy.note].filter(Boolean).join(" | "));
        const r = await addLot({
          qty: buy.qty,
          model: buy.model,
          total_price: Math.round(buy.totalPrice),
          wallet: buy.wallet,
          note: finalNote,
          chatId,
        });

        const deviceLabel = extractDeviceLabelFromLotNote(finalNote, buy.model);
        const noteLine = deviceLabel ? `Note: <b>${escapeHtml(deviceLabel)}</b>\n` : "";

        const html =
          `✅ <b>Xong rồi nè</b> 🥳\n` +
          `Tạo lô: <b>MÃ ${escapeHtml(r.lot.slice(2))}</b>\n` +
          `Mua: <b>${buy.qty}</b> máy <b>${escapeHtml(buy.model)}</b>\n` +
          noteLine +
          `Tổng: <b>${moneyWON(Math.round(buy.totalPrice))}</b>\n` +
          `Ví: <code>${escapeHtml(String(buy.wallet || "").toUpperCase())}</code>`;

        await send(chatId, html, { reply_markup: leftKb() });
        return;
      }

      if (buy.wallet) {
        setSession(chatId, { flow: "buy_lot", step: "note", data: buy });
        await send(
          chatId,
          `Okie 😚 <b>Mua lô</b> <code>${buy.qty}</code> máy <b>${escapeHtml(buy.model)}</b>, tổng <b>${moneyWON(
            buy.totalPrice
          )}</b>\nVí: <code>${escapeHtml(buy.wallet.toUpperCase())}</code>\nNhập note (vd <b>Note4</b>) hoặc <code>-</code> nha~`,
          { reply_markup: leftKb() }
        );
        return;
      }
      setSession(chatId, { flow: "buy_lot", step: "wallet", data: buy });
      await send(
        chatId,
        `Mình hiểu bạn mua lô <code>${buy.qty}</code> máy <b>${escapeHtml(buy.model)}</b>, tổng <b>${moneyWON(
          buy.totalPrice
        )}</b>\nTính tiền ví nào? (<code>hana/uri/kt/tm</code>)`,
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
