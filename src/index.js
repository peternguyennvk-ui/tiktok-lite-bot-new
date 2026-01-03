// src/index.js
import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * ENV
 * ========================= */
const VERSION = "LOT-MAxx-SMARTPARSE-WALLET-SELL-ANALYZE-SOLDFLAG-FIXNEW";
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
 * ========================= */
function kb(rows) {
  return { keyboard: rows, resize_keyboard: true, one_time_keyboard: false, is_persistent: true };
}
function mainKb() {
  return kb([[{ text: "⬅️ Menu" }, { text: "➡️ Menu" }]]);
}
function leftKb() {
  // Back luôn nằm trên cùng
  return kb([
    [{ text: "⬅️ Back" }],
    [{ text: "📱 Mua Máy (Lô)" }, { text: "💸 Bán Máy" }],
    [{ text: "🧪 Kiểm Tra Máy (Tất cả)" }, { text: "🧪 20 Lô Gần Nhất" }],
    [{ text: "🧾 Danh Sách Máy" }],
    [{ text: "⚽ Thu Đá Bóng" }, { text: "🎁 Thu Hộp Quà" }],
    [{ text: "🔳 Thu QR" }, { text: "➕ Thu Khác" }],
  ]);
}
function rightKb() {
  // Back luôn nằm trên cùng + bỏ "Lời/Lỗ Máy" cũ
  return kb([
    [{ text: "⬅️ Back" }],
    [{ text: "📊 Phân Tích" }],
    [{ text: "💰 Tổng Doanh Thu" }],
    [{ text: "📅 Tháng Này" }, { text: "⏮️ Tháng Trước" }],
    [{ text: "📊 Thống Kê Game" }],
    [{ text: "💼 Xem Ví" }],
    [{ text: "✏️ Sửa Số Dư Ví" }],
    [{ text: "✏️ Sửa Số Dư Tổng Doanh Thu" }],
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
function normalizeSpaces(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
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
 * Payouts (PHÂN TÍCH MÁY)
 * ========================= */
async function getMachinePayouts() {
  // theo yêu cầu: HQ=150k, QR=57k, DB=100k (chỉ dùng phân tích máy)
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
    ref_id: String(r[5] || "").trim(),
    type: String(r[2] || "").trim().toLowerCase(),
  }));
}

async function addWalletLog({ wallet, type, amount, ref_type, ref_id, note, chatId }) {
  await appendValues("WALLET_LOG!A1", [
    [nowIso(), wallet, type, amount, ref_type || "", ref_id || "", note || "", String(chatId || "")],
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
 * Game Revenue (DOANH THU CHÍNH)
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
    // PHONES columns A..I: phone_id, lot, ts, unit, status, game, note, sold_flag, sold_ts
    await appendValues("PHONES!A1", [[phone_id, lot, nowIso(), unit, "new", "none", note || "", "", ""]]);
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
  // Expect A..I
  const rows = await getValues("PHONES!A2:I");
  return rows
    .filter((r) => r.some((c) => String(c || "").trim() !== ""))
    .map((r) => ({
      phone_id: String(r[0] || "").trim(),
      lot: String(r[1] || "").trim().toUpperCase(),
      unit: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
      status: String(r[4] || "").trim().toLowerCase(), // new / win / lose / draw (or legacy ok/tach)
      game: String(r[5] || "").trim().toLowerCase(),   // hq / qr / db / none
      note: String(r[6] || ""),
      sold_flag: String(r[7] || "").trim(),            // "1" or ""
      sold_ts: String(r[8] || ""),
    }));
}

// Update E..I for a phone
async function updatePhoneRowById(phone_id, patch) {
  const rows = await getValues("PHONES!A2:I");
  for (let i = 0; i < rows.length; i++) {
    const id = String(rows[i][0] || "").trim();
    if (id === phone_id) {
      const rowNumber = i + 2;

      const curStatus = String(rows[i][4] || "");
      const curGame = String(rows[i][5] || "");
      const curSoldFlag = String(rows[i][7] || "");
      const curSoldTs = String(rows[i][8] || "");

      const status = patch.status ?? curStatus;
      const game = patch.game ?? curGame;
      const sold_flag = patch.sold_flag ?? curSoldFlag;
      const sold_ts = patch.sold_ts ?? curSoldTs;

      await updateValues(`PHONES!E${rowNumber}:I${rowNumber}`, [[status, game, String(rows[i][6] || ""), sold_flag, sold_ts]]);
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

/**
 * Parse resolve:
 * - Supports: "ma01 hq1 tach2"
 * - Supports: "ma01 loi1 hq tach2"
 * - Supports: "ma01 lo2" (lo = lỗ => tach)
 */
function parseLotResolve(text) {
  const norm = normalizeForParse(text);
  const lot = parseLotCode(text);
  if (!lot) return null;

  const tokens = norm.split(" ").filter(Boolean);
  const segments = [];
  let i = 0;

  const pushSeg = (kind, count, game = "") => {
    segments.push({ kind, count: Math.max(0, Math.min(50, count)), game: game || "" });
  };

  while (i < tokens.length) {
    const tk = tokens[i];

    // direct game tokens: hq1 / qr1 / db1 => means "win"
    const mGameCount = tk.match(/^(hq|qr|db)\s*(\d+)?$/);
    if (mGameCount) {
      const game = mGameCount[1];
      let count = mGameCount[2] ? Number(mGameCount[2]) : 1;
      // also support next token number
      if (!mGameCount[2] && i + 1 < tokens.length && tokens[i + 1].match(/^\d+$/)) {
        count = Number(tokens[i + 1]);
        i += 1;
      }
      pushSeg("win", count, game);
      i += 1;
      continue;
    }

    const isLoi = tk === "loi" || tk === "lai" || tk === "an" || tk === "duoc" || tk === "ok" || tk === "win";
    const isTach = tk === "tach" || tk === "tac" || tk === "chet" || tk === "lo" || tk === "lose";
    const isHue = tk === "hue" || tk === "hoa" || tk === "thuvon" || tk === "thu" || tk === "von" || tk === "draw";

    if (isLoi || isTach || isHue) {
      const kind = isLoi ? "win" : isTach ? "lose" : "draw";
      let count = 1;

      if (i + 1 < tokens.length && tokens[i + 1].match(/^\d+$/)) {
        count = Number(tokens[i + 1]);
        i += 2;
      } else {
        i += 1;
      }

      // optional "may/dt"
      if (i < tokens.length && (tokens[i] === "may" || tokens[i] === "dt")) i++;

      let game = "";
      if (kind === "win") {
        // allow next token game: hq/qr/db
        if (i < tokens.length) {
          const g = detectGameToken(tokens[i]);
          if (g) {
            game = g;
            i += 1;
          }
        }
      }

      pushSeg(kind, count, game);
      continue;
    }

    i++;
  }

  return { lot, segments };
}

/* =========================
 * Status helpers (legacy tolerant)
 * ========================= */
function isSoldPhone(p) {
  if (String(p.sold_flag || "").trim() === "1") return true;
  // legacy fallback: status sold
  return String(p.status || "").toLowerCase() === "sold";
}
function normStatus(p) {
  const s = String(p.status || "").toLowerCase();
  if (s === "ok") return "win";
  if (s === "tach") return "lose";
  if (s === "hue") return "draw";
  if (s === "sold") return "sold";
  if (s === "new") return "new";
  if (s === "win" || s === "lose" || s === "draw") return s;
  // unknown -> keep
  return s || "new";
}
function isWinPhone(p) {
  const st = normStatus(p);
  return st === "win" && ["hq", "qr", "db"].includes(String(p.game || "").toLowerCase());
}
function isLosePhone(p) {
  const st = normStatus(p);
  return st === "lose";
}
function isDrawPhone(p) {
  const st = normStatus(p);
  return st === "draw";
}
function isNewPhone(p) {
  const st = normStatus(p);
  // New chỉ là chưa chốt + chưa bán
  return st === "new" && !isSoldPhone(p);
}

/* =========================
 * Apply resolve / sell
 * ========================= */
async function applyLotResolve({ chatId, userName, lot, segments }) {
  const phones = await readPhones();
  const lotPhonesAll = phones.filter((p) => p.lot === lot);

  if (lotPhonesAll.length === 0) {
    await send(chatId, `🥺 Không thấy mã lô <code>${escapeHtml(lot)}</code> á. Bạn check lại nha~`, { reply_markup: leftKb() });
    return true;
  }

  const payouts = await getMachinePayouts();

  // Pick phones not yet resolved first: status new AND not sold
  const pick = (n) => {
    const pool1 = lotPhonesAll.filter((p) => normStatus(p) === "new" && !isSoldPhone(p));
    const pool2 = lotPhonesAll.filter((p) => normStatus(p) !== "sold" && !isSoldPhone(p)); // unresolved or resolved but unsold
    const pool = pool1.length > 0 ? pool1 : pool2;
    return pool.slice(0, n).map((p) => p.phone_id);
  };

  let cntWin = 0,
    cntLose = 0,
    cntDraw = 0;
  let hq = 0,
    qr = 0,
    db = 0;
  let totalGame = 0;

  for (const seg of segments) {
    const ids = pick(seg.count);
    if (ids.length === 0) continue;

    if (seg.kind === "lose") {
      for (const id of ids) await updatePhoneRowById(id, { status: "lose", game: "none" });
      cntLose += ids.length;
      continue;
    }

    if (seg.kind === "draw") {
      for (const id of ids) await updatePhoneRowById(id, { status: "draw", game: "none" });
      cntDraw += ids.length;
      continue;
    }

    // win
    const game = seg.game || ""; // if empty => ask user
    if (!game) {
      await send(
        chatId,
        `Bạn ghi <b>lời ${seg.count}</b> là ăn <b>HQ</b> hay <b>QR</b> hay <b>DB</b> á?\nVí dụ: <code>ma ${lot.slice(2)} hq${seg.count} tach0</code>`,
        { reply_markup: leftKb() }
      );
      return true;
    }

    const per = payouts[game] ?? payouts.hq;
    for (const id of ids) await updatePhoneRowById(id, { status: "win", game });
    cntWin += ids.length;

    if (game === "hq") hq += ids.length;
    if (game === "qr") qr += ids.length;
    if (game === "db") db += ids.length;

    totalGame += ids.length * per;
  }

  const html =
    `🧾 <b>CHỐT LÔ MÃ ${escapeHtml(lot.slice(2))}</b>\n` +
    `✅ <b>Lời</b>: <b>${cntWin}</b> MÁY (HQ:${hq} / QR:${qr} / DB:${db})\n` +
    `😵 <b>Lỗ</b>: <b>${cntLose}</b> MÁY TẠCH\n` +
    `😌 <b>Huề</b>: <b>${cntDraw}</b>\n` +
    `🎮 <b>Tổng thu game (phân tích)</b>: <b>${moneyWON(totalGame)}</b>`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

async function sellFromLot({ chatId, lot, qty, totalPrice, wallet }) {
  const phones = await readPhones();
  const lotPhonesAll = phones.filter((p) => p.lot === lot);

  if (lotPhonesAll.length === 0) {
    await send(chatId, `🥺 Không thấy lô <code>${escapeHtml(lot)}</code> luôn á. Bạn check lại mã nha~`, { reply_markup: leftKb() });
    return true;
  }

  // sellable: not sold_flag
  const sellable = lotPhonesAll
    .filter((p) => !isSoldPhone(p))
    .sort((a, b) => {
      // ưu tiên bán máy đã lỗ trước, rồi draw, rồi new, rồi win (tuỳ bạn – để tránh mất lời còn giữ)
      const rank = (p) => {
        const st = normStatus(p);
        if (st === "lose") return 0;
        if (st === "draw") return 1;
        if (st === "new") return 2;
        if (st === "win") return 3;
        return 4;
      };
      return rank(a) - rank(b);
    });

  const ids = sellable.slice(0, qty).map((p) => p.phone_id);
  if (ids.length === 0) {
    await send(chatId, `Lô <code>${escapeHtml(lot)}</code> bán hết sạch rồi 😝`, { reply_markup: leftKb() });
    return true;
  }

  // Mark sold_flag, keep status/game as-is
  const ts = nowIso();
  for (const id of ids) await updatePhoneRowById(id, { sold_flag: "1", sold_ts: ts });

  // wallet + log
  await addWalletLog({
    wallet,
    type: "machine_sell",
    amount: Math.abs(Math.round(totalPrice)),
    ref_type: "lot",
    ref_id: lot,
    note: `SELL x${ids.length}`,
    chatId,
  });

  // Get note name (e.g. Note4) from LOT note if available
  const lots = await readLots();
  const l = lots.find((x) => x.lot === lot);
  let noteName = "";
  if (l?.note) {
    // try to pick a short token like Note4 / S9 / etc
    const tokens = String(l.note).split("|").map((x) => x.trim()).filter(Boolean);
    // pick last non-empty part as user note
    const last = tokens[tokens.length - 1] || "";
    // if looks like phone name, use it
    if (last && last.length <= 20) noteName = last;
  }

  const html =
    `💸 <b>BÁN XONG</b> 🥳\n` +
    `• Lô: <b>MÃ ${escapeHtml(lot.slice(2))}</b>\n` +
    `• Số máy: <b>${ids.length}</b> máy${noteName ? ` <b>${escapeHtml(noteName)}</b>` : ""}\n` +
    `• Tiền về ví <code>${escapeHtml(wallet.toUpperCase())}</code>: <b>${moneyWON(Math.round(totalPrice))}</b>\n\n` +
    `Phân tích lô sẽ tự cộng tiền bán này vào nhé 😝`;

  await send(chatId, html, { reply_markup: leftKb() });
  return true;
}

/* =========================
 * Reports (Right Menu)
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
 * Machine Analysis (RIGHT MENU)
 * ========================= */
function bar(pct, width = 18) {
  const n = Math.round((pct / 100) * width);
  return "█".repeat(n).padEnd(width, " ");
}
function pctOf(part, total) {
  if (total <= 0) return 0;
  return Math.round((part / total) * 100);
}

async function machineAnalysis(chatId) {
  const lots = await readLots();
  const phones = await readPhones();
  const payouts = await getMachinePayouts();
  const logs = await readWalletLog();

  // Sold money by lot
  const soldMoneyByLot = new Map();
  for (const l of logs) {
    if (l.type !== "machine_sell") continue;
    if (l.ref_type !== "lot") continue;
    const lot = String(l.ref_id || "").toUpperCase();
    soldMoneyByLot.set(lot, (soldMoneyByLot.get(lot) || 0) + (l.amount || 0));
  }

  let totalBuy = 0;
  let totalGame = 0;
  let totalSell = 0;

  let totalWin = 0,
    totalLose = 0,
    totalDraw = 0,
    totalNew = 0,
    totalSold = 0,
    totalWinUnsold = 0;

  // per-lot stats (for optional detailed listing later)
  const byLot = new Map();

  for (const lot of lots) {
    totalBuy += lot.total;

    const ps = phones.filter((p) => p.lot === lot.lot);
    const soldCount = ps.filter((p) => isSoldPhone(p)).length;
    const newCount = ps.filter((p) => isNewPhone(p)).length;

    const winPhones = ps.filter((p) => isWinPhone(p));
    const losePhones = ps.filter((p) => isLosePhone(p));
    const drawPhones = ps.filter((p) => isDrawPhone(p));

    const hq = winPhones.filter((p) => String(p.game).toLowerCase() === "hq").length;
    const qr = winPhones.filter((p) => String(p.game).toLowerCase() === "qr").length;
    const db = winPhones.filter((p) => String(p.game).toLowerCase() === "db").length;

    const gameRev = hq * payouts.hq + qr * payouts.qr + db * payouts.db;

    const sellMoney = soldMoneyByLot.get(lot.lot) || 0;

    const winUnsold = winPhones.filter((p) => !isSoldPhone(p)).length;
    const remainUnsold = Math.max(0, ps.length - soldCount);

    totalGame += gameRev;
    totalSell += sellMoney;

    totalWin += winPhones.length;
    totalLose += losePhones.length;
    totalDraw += drawPhones.length;
    totalNew += newCount;
    totalSold += soldCount;
    totalWinUnsold += winUnsold;

    byLot.set(lot.lot, {
      lot,
      counts: {
        win: winPhones.length,
        lose: losePhones.length,
        draw: drawPhones.length,
        new: newCount,
        sold: soldCount,
        winUnsold,
        remainUnsold,
        hq,
        qr,
        db,
      },
      money: { buy: lot.total, game: gameRev, sell: sellMoney, thu: gameRev + sellMoney },
    });
  }

  const totalThu = totalGame + totalSell;
  const loiConLai = totalThu - totalBuy;

  const totalPhones = phones.length || 0;
  const pctNew = pctOf(totalNew, totalPhones);
  const pctWin = pctOf(totalWin, totalPhones);
  const pctLose = pctOf(totalLose, totalPhones);
  const pctDraw = pctOf(totalDraw, totalPhones);
  const pctSold = pctOf(totalSold, totalPhones);

  const html =
    `📊 <b>PHÂN TÍCH MUA MÁY</b>\n\n` +
    `💳 <b>Đã bỏ ra mua máy</b>: <b>${moneyWON(totalBuy)}</b>\n` +
    `💰 <b>Số tiền thu được</b>: <b>${moneyWON(totalThu)}</b>\n` +
    `   • Thu game (HQ/QR/DB): <b>${moneyWON(totalGame)}</b>\n` +
    `   • Thu bán máy: <b>${moneyWON(totalSell)}</b>\n\n` +
    `🧮 <b>Net tạm (thu game)</b>: <b>${moneyWON(totalGame)}</b>\n` +
    `🧮 <b>Net thực (game + bán)</b>: <b>${moneyWON(totalThu)}</b>\n\n` +
    `💖 <b>Thu về</b> ${moneyWON(totalThu)} - <b>mua</b> ${moneyWON(totalBuy)} = <b>${moneyWON(loiConLai)}</b>\n\n` +
    `Máy (tổng kết quả - tính kể cả Sold)\n` +
    `• Lời: <b>${totalWin}</b> máy\n` +
    `• Lỗ: <b>${totalLose}</b> máy\n` +
    `• Huề: <b>${totalDraw}</b> máy\n` +
    `• Chưa làm (New): <b>${totalNew}</b> máy\n` +
    `• Đã bán (Sold): <b>${totalSold}</b> máy\n` +
    `• Lời còn giữ: <b>${totalWinUnsold}</b> máy chưa bán\n\n` +
    `📌 <b>Biểu đồ trạng thái</b>\n` +
    `New  : ${bar(pctNew)} ${pctNew}% (${totalNew})\n` +
    `Lời  : ${bar(pctWin)} ${pctWin}% (${totalWin})\n` +
    `Lỗ   : ${bar(pctLose)} ${pctLose}% (${totalLose})\n` +
    `Huề  : ${bar(pctDraw)} ${pctDraw}% (${totalDraw})\n` +
    `Sold : ${bar(pctSold)} ${pctSold}% (${totalSold})\n\n` +
    `💸 <b>TỔNG</b>\n` +
    `• <b>TỔNG TIỀN MUA MÁY</b>: <b>${moneyWON(totalBuy)}</b>\n` +
    `• <b>TỔNG THU VỀ</b>: <b>${moneyWON(totalThu)}</b>\n` +
    `• <b>LỜI CÒN LẠI</b>: <b>${moneyWON(loiConLai)}</b>\n` +
    `• <b>LỜI</b> ${pctWin}% | <b>LỖ</b> ${pctLose}%`;

  await send(chatId, html, { reply_markup: rightKb() });
}

/* =========================
 * List Lots (ALL / 20)
 * ========================= */
async function listLotsPretty(chatId, { limit = 20, all = false } = {}) {
  const lots = await readLots();
  const phones = await readPhones();
  const payouts = await getMachinePayouts();
  const logs = await readWalletLog();

  if (lots.length === 0) {
    await send(chatId, `Chưa có lô nào hết á 😝\nBấm <b>📱 Mua Máy (Lô)</b> để tạo lô nha~`, { reply_markup: leftKb() });
    return;
  }

  const soldMoneyByLot = new Map();
  for (const l of logs) {
    if (l.type !== "machine_sell") continue;
    if (l.ref_type !== "lot") continue;
    const lot = String(l.ref_id || "").toUpperCase();
    soldMoneyByLot.set(lot, (soldMoneyByLot.get(lot) || 0) + (l.amount || 0));
  }

  const sorted = [...lots].sort((a, b) => (a.ts < b.ts ? 1 : -1));
  const slice = all ? sorted : sorted.slice(0, limit);

  const lines = slice.map((l) => {
    const ps = phones.filter((p) => p.lot === l.lot);

    const soldCount = ps.filter((p) => isSoldPhone(p)).length;
    const remainUnsold = Math.max(0, ps.length - soldCount);

    const winPhones = ps.filter((p) => isWinPhone(p));
    const losePhones = ps.filter((p) => isLosePhone(p));
    const drawPhones = ps.filter((p) => isDrawPhone(p));
    const newPhones = ps.filter((p) => isNewPhone(p));

    const hq = winPhones.filter((p) => String(p.game).toLowerCase() === "hq").length;
    const qr = winPhones.filter((p) => String(p.game).toLowerCase() === "qr").length;
    const db = winPhones.filter((p) => String(p.game).toLowerCase() === "db").length;

    const gameRev = hq * payouts.hq + qr * payouts.qr + db * payouts.db;

    const soldMoney = soldMoneyByLot.get(l.lot) || 0;

    const laiTam = gameRev - l.total;
    const laiThuc = gameRev + soldMoney - l.total;

    const winUnsold = winPhones.filter((p) => !isSoldPhone(p)).length;

    return (
      `• <b>${escapeHtml(l.lot)}</b>: Mua <code>${l.qty}</code> máy <b>${escapeHtml(l.model)}</b> | Tổng <b>${moneyWON(l.total)}</b> | Ví <code>${escapeHtml(String(l.wallet || "").toUpperCase())}</code>\n\n` +
      `  Trạng thái: Lời <b>${winPhones.length}</b> máy (HQ:${hq} / QR:${qr} / DB:${db}) / Huề <b>${drawPhones.length}</b> / Lỗ <b>${losePhones.length}</b> / New <b>${newPhones.length}</b> / Sold <b>${soldCount}</b>\n` +
      `  Lời còn giữ: <b>${winUnsold}</b> máy | Còn lại: <b>${remainUnsold}</b> máy chưa bán\n\n` +
      `  Tổng thu game: <b>${moneyWON(gameRev)}</b>\n` +
      `  Lãi tạm: <b>${moneyWON(gameRev)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(laiTam)}</b>\n` +
      `  Đã bán: <b>${moneyWON(soldMoney)}</b>\n` +
      `  Lãi thực: <b>${moneyWON(gameRev)}</b> + <b>${moneyWON(soldMoney)}</b> - <b>${moneyWON(l.total)}</b> = <b>${moneyWON(laiThuc)}</b>`
    );
  });

  const title = all ? "🧪 <b>DANH SÁCH LÔ MÁY</b> (Tất cả)\n\n" : `🧪 <b>DANH SÁCH LÔ MÁY</b> (20 lô gần nhất)\n\n`;
  await send(chatId, title + lines.join("\n\n"), { reply_markup: leftKb() });
}

/* =========================
 * List Phones (Menu Left)
 * ========================= */
async function listPhonesPretty(chatId) {
  const lots = await readLots();
  const phones = await readPhones();

  if (phones.length === 0) {
    await send(chatId, `Chưa có máy nào hết á 😝`, { reply_markup: leftKb() });
    return;
  }

  const lotMap = new Map();
  for (const l of lots) lotMap.set(l.lot, l);

  // group by lot
  const groups = new Map();
  for (const p of phones) {
    const k = p.lot || "UNKNOWN";
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(p);
  }

  const lotKeys = [...groups.keys()].sort((a, b) => (a < b ? 1 : -1)).slice(0, 30); // limit to avoid spam

  const block = lotKeys.map((lotCode) => {
    const ps = groups.get(lotCode) || [];
    const l = lotMap.get(lotCode);

    const soldCount = ps.filter((p) => isSoldPhone(p)).length;
    const remain = Math.max(0, ps.length - soldCount);

    const lines = ps.slice(0, 25).map((p) => {
      const st = normStatus(p);
      const sold = isSoldPhone(p) ? "✅ ĐÃ BÁN" : "⏳ CHƯA BÁN";
      let label = "New";
      if (st === "win") label = `Lời (${String(p.game || "").toUpperCase()})`;
      if (st === "lose") label = "Lỗ (Tạch)";
      if (st === "draw") label = "Huề";
      return `• <code>${escapeHtml(p.phone_id)}</code>: <b>${escapeHtml(label)}</b> | ${sold}`;
    });

    return (
      `📦 <b>${escapeHtml(lotCode)}</b>${l?.note ? ` (<i>${escapeHtml(l.note)}</i>)` : ""}\n` +
      `Đã bán: <b>${soldCount}</b> | Còn lại: <b>${remain}</b> máy chưa bán\n` +
      lines.join("\n")
    );
  });

  await send(chatId, `🧾 <b>DANH SÁCH MÁY</b> (gần nhất)\n\n${block.join("\n\n")}`, { reply_markup: leftKb() });
}

/* =========================
 * Quick revenue (DOANH THU CHÍNH)
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
    `✅ <b>Chốt lô</b>:\n` +
    `• <code>ma01 hq1 tach2</code>\n` +
    `• <code>ma 02 qr1 lo2</code>\n` +
    `• <code>ma01 db1 hue1 tach1</code>\n\n` +
    `✅ <b>Bán</b> (tiền là <b>TỔNG</b>):\n` +
    `• <code>ban 2 ss 50k ma01 uri</code>\n\n` +
    `✅ <b>Thu doanh thu chính</b>:\n` +
    `• <code>db 100k</code> / <code>hq 200k</code> / <code>qr 57k</code> / <code>them 0.5k</code>\n\n` +
    `<i>Tip:</i> Bạn gõ không dấu thoải mái, bot trả lời có dấu cho dễ đọc 😚`
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
      await send(chatId, `Bạn gõ kiểu: <code>mua 3ss 50k</code> / <code>mua lg35k hn</code> nha~`, { reply_markup: leftKb() });
      return true;
    }
    sess.data = parsed;

    if (parsed.wallet) {
      sess.step = "note";
      setSession(chatId, sess);
      // nếu bạn không nhập model thì không cần phản hồi model (nhưng code bạn đang tự detect model, giữ logic cũ)
      await send(
        chatId,
        `Okie 😚 Mua lô <code>${parsed.qty}</code> máy <b>${escapeHtml(parsed.model)}</b>, tổng <b>${moneyWON(parsed.totalPrice)}</b>\nVí: <code>${escapeHtml(parsed.wallet.toUpperCase())}</code>\n\nNhập <i>note</i> (vd Note4) hoặc <code>-</code> để bỏ qua nha~`,
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
    await send(chatId, `Nhập <i>note</i> (vd Note4) hoặc <code>-</code> để bỏ qua nha~`, { reply_markup: leftKb() });
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
      `Tạo lô: <b>MÃ ${escapeHtml(r.lot.slice(2))}</b>\n` +
      `Mua: <b>${sess.data.qty}</b> máy <b>${escapeHtml(sess.data.model)}</b>\n` +
      `Tổng: <b>${moneyWON(Math.round(sess.data.totalPrice))}</b>\n` +
      `Ví: <b>${escapeHtml(String(sess.data.wallet || "").toUpperCase())}</b>`;

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
      `Mình hiểu bạn đang bán lô <b>MÃ ${escapeHtml(parsed.lot.slice(2))}</b> x<code>${parsed.qty}</code>, giá <b>${moneyWON(parsed.totalPrice)}</b>\n\nTiền về ví nào? (<code>hana/uri/kt/tm</code>)`,
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
      `Bù chênh: <code>${r.delta >= 0 ? "+" : ""}${moneyWON(r.delta)}</code>`;

    await send(chatId, html, { reply_markup: rightKb() });
    return true;
  }

  // EDIT TOTAL REVENUE (doanh thu chính)
  if (sess.flow === "total_revenue_edit" && sess.step === "amount") {
    const amt = extractMoneyFromText(text);
    if (amt == null) {
      await send(chatId, `Nhập số kiểu <code>120k</code> nha~`, { reply_markup: rightKb() });
      return true;
    }
    clearSession(chatId);

    // Set total revenue by adding an adjust entry: we compute current total and add delta
    const rows = await readGameRevenue();
    const current = rows.reduce((a, b) => a + b.amount, 0);
    const delta = Math.round(amt - current);

    if (delta !== 0) {
      await addGameRevenue({ game: "other", type: "adjust_total", amount: delta, note: `SET_TOTAL ${current} -> ${amt}`, chatId, userName });
    }

    await send(
      chatId,
      `✏️ <b>SỬA SỐ DƯ TỔNG DOANH THU</b>\n\nCũ: <b>${moneyWON(current)}</b>\nMới: <b>${moneyWON(amt)}</b>\nBù chênh: <b>${moneyWON(delta)}</b>`,
      { reply_markup: rightKb() }
    );
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
    await send(chatId, `➡️ <b>Menu Phải</b> đây nè~`, { reply_markup: rightKb() });
    return;
  }
  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, `Về <b>menu chính</b> nha bạn iu~ 🏠`, { reply_markup: mainKb() });
    return;
  }

  // right menu
  if (text === "📊 Phân Tích") return machineAnalysis(chatId);
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
    setSession(chatId, { flow: "total_revenue_edit", step: "amount", data: {} });
    await send(chatId, `✏️ <b>Sửa số dư tổng doanh thu</b>\nBạn nhập <b>tổng mới</b> (vd <code>500k</code>) nha~`, { reply_markup: rightKb() });
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
  if (text === "🧪 Kiểm Tra Máy (Tất cả)") return listLotsPretty(chatId, { all: true });
  if (text === "🧪 20 Lô Gần Nhất") return listLotsPretty(chatId, { all: false, limit: 20 });
  if (text === "🧾 Danh Sách Máy") return listPhonesPretty(chatId);

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
      `Mình hiểu bạn đang bán lô <b>MÃ ${escapeHtml(sell.lot.slice(2))}</b> x<code>${sell.qty}</code> giá <b>${moneyWON(sell.totalPrice)}</b>\n\nTiền về ví nào? (<code>hana/uri/kt/tm</code>)`,
      { reply_markup: leftKb() }
    );
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
          `Okie 😚 Mua lô <code>${buy.qty}</code> máy <b>${escapeHtml(buy.model)}</b>, tổng <b>${moneyWON(buy.totalPrice)}</b>\nVí: <code>${escapeHtml(buy.wallet.toUpperCase())}</code>\nNhập note (vd Note4) hoặc <code>-</code> nha~`,
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
  await send(chatId, `Nhập sai rồi bạn iu ơi ^^\nVào ➡️ <b>Menu</b> → <b>📘 Hướng Dẫn</b> nha~`, { reply_markup: mainKb(), __raw: true });
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
