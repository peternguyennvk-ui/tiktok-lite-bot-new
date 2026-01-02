// src/index.js
/**
 * =================================================================================================
 *  TIKTOK_LITE_BOT – FINAL ONE-FILE BUILD (>= 1000 lines)
 * =================================================================================================
 *  ✅ Webhook Telegram (Express) + Google Sheets DB (googleapis) + Cron remind 14-day
 *  ✅ Menus: Main (⬅️ Menu / ➡️ Menu), Left (Input), Right (Reports/Help/AI/Reset + Machine+Wallet)
 *  ✅ Currency display: WON (₩) everywhere
 *  ✅ Keep Gemini API (optional) as last-resort fallback ONLY
 *  ✅ Add Smart Parse (FREE) before Gemini: hiểu nhiều kiểu gõ "lỏng" mà không cần key
 *  ✅ Add Machine + Wallet full feature set:
 *      - Buy machine (muamay)
 *      - Mark machine result (mayloi/mayhue/maytach)
 *      - Wallet ledger (WALLET_LOG) + report wallet balances
 *      - Wallet adjust to target balance (sodu)
 *      - Machine profit/loss report includes counts + money + per wallet
 *  ✅ Edit commands:
 *      - /edit last            (revenue) -> adjustment entries (safe)
 *      - /edit machine last    (machine) -> adjustment entries (safe)
 *  ✅ Reset data (pass=12345) – ai biết pass đều xóa được
 *
 *  NOTE ABOUT TELEGRAM LIMITATION:
 *   - Bot cannot auto-fill the user's text input box on button click (Telegram limitation).
 *   - We use multi-step prompts (ask name/email/amount) + help templates for copy/reply.
 *
 * -------------------------------------------------------------------------------------------------
 *  REQUIRED ENV:
 *   - BOT_TOKEN
 *   - GOOGLE_SHEET_ID
 *   - GOOGLE_APPLICATION_CREDENTIALS (path to service account json)
 *
 *  OPTIONAL ENV:
 *   - ADMIN_TELEGRAM_ID  (admin for 14-day check-in reminders)
 *
 * -------------------------------------------------------------------------------------------------
 *  SHEETS (tabs) expected (your existing design):
 *   - SETTINGS          (A:key, B:value) – used for GEMINI_API_KEY, GEMINI_ENABLED, GEMINI_MODEL
 *   - GAME_REVENUE      (A:ts, B:game, C:type, D:amount, E:note, F:chat_id, G:user_name)
 *   - INVITES           (A:ts_created, B:game, C:name, D:email, E:invited_at, F:due_date,
 *                        G:status, H:asked, I:asked_at, J:checkin_reward, K:done_at, L:note)
 *   - CHECKIN_REWARD    (A:ts, B:game, C:name, D:email, E:due_date, F:amount, G:chat_id, H:user_name)
 *
 *   - WALLETS           (A:wallet_code, B:wallet_name)    (balance is derived from ledger)
 *   - WALLET_LOG        (A:ts, B:wallet_code, C:type, D:amount, E:ref_type, F:ref_id, G:note, H:chat_id)
 *
 *   - PHONES            (A:phone_id, B:ts_buy, C:buy_price, D:wallet_code, E:status, F:note)
 *   - PHONE_PROFIT_LOG  (A:ts, B:phone_id, C:result, D:amount, E:note, F:wallet_code, G:chat_id)
 *
 *   - UNDO_LOG          (optional; we still append some logs for audit; real undo can be extended)
 *
 * -------------------------------------------------------------------------------------------------
 *  IMPORTANT:
 *   - No Google Apps Script APIs (NO PropertiesService). Node.js only.
 * -------------------------------------------------------------------------------------------------
 */

/* =========================
 * SECTION 0 — Imports
 * ========================= */
import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import dayjs from "dayjs";
import cron from "node-cron";

/* =========================
 * SECTION 1 — Env & constants
 * ========================= */
const VERSION = "FINAL-SMARTPARSE-MACHINE-WALLET-GEMINI-WON-ONEFILE";
const BOT_TOKEN = process.env.BOT_TOKEN;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "/etc/secrets/google-service-account.json";
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID ? String(process.env.ADMIN_TELEGRAM_ID).trim() : "";

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");

const TELEGRAM_API = `https://api.telegram.org/bot${BOT_TOKEN}`;
const RESET_PASS = "12345";

/**
 * By default, we display money as Korean Won symbol.
 * Input parser supports:
 *  - 100k / 0.5k
 *  - 100000 / 100,000
 *  - ₩100,000 / 100k won
 *
 * NOTE: We do NOT change stored numeric values; only formatting changes.
 */
function moneyWON(n) {
  return "₩" + Number(n || 0).toLocaleString("ko-KR");
}

/* =========================
 * SECTION 2 — Express server
 * ========================= */
const app = express();
app.use(express.json());

app.get("/", (_, res) => res.status(200).send(`OK ${VERSION}`));
app.get("/ping", (_, res) => res.status(200).json({ ok: true, version: VERSION }));

/* =========================
 * SECTION 3 — Telegram helpers
 * ========================= */
async function tg(method, payload) {
  const resp = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  // do not throw; telegram returns error json sometimes
  const j = await resp.json().catch(() => ({}));
  return j;
}

async function send(chatId, text, extra = {}) {
  if (!chatId) return;
  await tg("sendMessage", {
    chat_id: chatId,
    text,
    ...extra,
  });
}

/* =========================
 * SECTION 4 — Reply keyboards (as requested)
 * ========================= */
function kb(rows) {
  return {
    keyboard: rows,
    resize_keyboard: true,
    one_time_keyboard: false,
    is_persistent: true,
  };
}

/**
 * Main menu = 2 buttons only (left and right)
 */
function mainKb() {
  return kb([[{ text: "⬅️ Menu" }, { text: "➡️ Menu" }]]);
}

/**
 * Left menu = input workflows
 * - invites (HQ / QR)
 * - revenue add (DB/HQ/QR/Other)
 * - machine workflows (buy + results)
 */
function leftKb() {
  return kb([
    [{ text: "🎁 Hộp quà (mời)" }, { text: "🔳 QR (mời)" }],
    [{ text: "⚽ Đá bóng (thu)" }, { text: "🎁 Hộp quà (thu)" }],
    [{ text: "🔳 QR (thu)" }, { text: "➕ Thu khác" }],
    [{ text: "📱 Mua máy" }, { text: "✅ Máy lời/huề/tạch" }],
    [{ text: "⬅️ Back" }],
  ]);
}

/**
 * Right menu = reports + guidance + AI + reset + wallet/machine reports
 */
function rightKb() {
  return kb([
    [{ text: "1️⃣ Xem tổng doanh thu" }],
    [{ text: "2️⃣ Doanh thu tháng này" }, { text: "3️⃣ Doanh thu tháng trước" }],
    [{ text: "4️⃣ Thống kê ĐB / HQ / QR" }],
    [{ text: "7️⃣ Lời lỗ mua máy" }],
    [{ text: "💼 Xem ví" }, { text: "➕ Chỉnh số dư ví" }],
    [{ text: "📘 Hướng dẫn" }],
    [{ text: "🔑 Nhập Gemini Key" }, { text: "🤖 AI: Bật/Tắt" }],
    [{ text: "🧠 Smart Parse: Bật/Tắt" }],
    [{ text: "8️⃣ Xóa sạch dữ liệu" }],
    [{ text: "⬅️ Back" }],
  ]);
}

/* =========================
 * SECTION 5 — Google Sheets setup
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
 * SECTION 6 — Common utils
 * ========================= */
function nowIso() {
  return new Date().toISOString();
}

function isEmail(x) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(x || "").trim());
}

/**
 * Parse money:
 * - Accept "100k" => 100000
 * - Accept "0.5k" => 500
 * - Accept "100000" => 100000
 * - Accept "100,000" => 100000
 * - Accept "₩100,000" => 100000
 * - Accept "100k won" => 100000
 * - Accept "100 k" => 100000
 */
function parseMoney(input) {
  if (input == null) return null;
  let s = String(input).trim().toLowerCase();

  // remove currency symbol and words
  s = s.replace(/₩/g, "");
  s = s.replace(/\bwon\b/g, "");
  s = s.replace(/\s+/g, " ").trim();

  // normalize "100 k" -> "100k"
  s = s.replace(/(\d)\s+k\b/g, "$1k");
  s = s.replace(/,/g, "");

  // accept numbers or numbers+k
  const m = s.match(/^(\d+(?:\.\d+)?)(k)?$/);
  if (!m) return null;
  const num = Number(m[1]);
  if (!Number.isFinite(num)) return null;
  const isK = !!m[2];
  return Math.round(isK ? num * 1000 : num);
}

/**
 * Vietnamese diacritics removal to normalize matching:
 * - "đá bóng" => "da bong"
 * - "hộp quà" => "hop qua"
 */
function removeDiacritics(str) {
  if (!str) return "";
  return String(str)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D");
}

/**
 * Normalize user input for smart parsing:
 * - lower
 * - remove diacritics
 * - keep emails safe
 */
function normalizeForParse(text) {
  const raw = String(text || "");
  // keep emails intact by temporarily tokenizing them
  const emails = [];
  let tmp = raw.replace(/[^\s@]+@[^\s@]+\.[^\s@]+/g, (m) => {
    emails.push(m);
    return `__EMAIL_${emails.length - 1}__`;
  });

  tmp = removeDiacritics(tmp).toLowerCase();
  tmp = tmp.replace(/[，]/g, ",");
  tmp = tmp.replace(/\s+/g, " ").trim();

  // restore emails
  tmp = tmp.replace(/__email_(\d+)__/g, (_, i) => emails[Number(i)] || "");
  return tmp;
}

/**
 * Extract first email found (if any)
 */
function extractEmail(text) {
  const m = String(text || "").match(/[^\s@]+@[^\s@]+\.[^\s@]+/);
  return m ? m[0] : "";
}

/**
 * Extract a money token from a longer text:
 * - tries to find patterns like 100k, 0.5k, ₩100,000, 100000
 * Returns integer amount or null.
 */
function extractMoneyFromText(text) {
  const t = String(text || "");
  // capture patterns:
  //  - ₩100,000
  //  - 100,000
  //  - 100000
  //  - 100k
  //  - 0.5k
  //  - 100 k
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

/**
 * Map game keywords to codes:
 * db = đá bóng
 * hq = hộp quà
 * qr = QR
 * other = thu khác / ngoài game
 */
function detectGameFromText(normText) {
  const t = ` ${normText} `;

  // other / thu khác
  const otherKeys = [" them ", " thu them ", " thu khac ", " ngoai game ", " ads ", " like ", " ngoai "];
  if (otherKeys.some((k) => t.includes(k))) return "other";

  // đá bóng
  const dbKeys = [" dabong ", " da bong ", " db ", " bong "];
  if (dbKeys.some((k) => t.includes(k))) return "db";

  // hộp quà
  const hqKeys = [" hopqua ", " hop qua ", " hq ", " hop "];
  if (hqKeys.some((k) => t.includes(k))) return "hq";

  // qr
  const qrKeys = [" qr ", " qrcode ", " ma qr "];
  if (qrKeys.some((k) => t.includes(k))) return "qr";

  return "";
}

/* =========================
 * SECTION 7 — SETTINGS / Toggles
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
  // default ON if empty
  const v = (await getSetting("SMART_PARSE_ENABLED")).trim();
  if (v === "") return true;
  return v === "1";
}

/* =========================
 * SECTION 8 — Game revenue data layer
 * ========================= */
async function addGameRevenue({ game, type, amount, note, chatId, userName }) {
  await appendValues("GAME_REVENUE!A1", [
    [nowIso(), game, type, amount, note || "", String(chatId || ""), userName || ""],
  ]);

  // audit log (optional)
  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "revenue_add", game, type, amount, note || "", String(chatId || ""), userName || ""]]);
  } catch (_) {
    // ignore if UNDO_LOG tab missing
  }
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
 * SECTION 9 — Invite data layer + 14-day remind
 * ========================= */
async function addInvite({ game, name, email }) {
  const invitedAt = dayjs();
  const due = invitedAt.add(14, "day");

  await appendValues("INVITES!A1", [
    [
      nowIso(), // A ts_created
      game, // B game
      name, // C name
      email, // D email
      invitedAt.toISOString(), // E invited_at
      due.toISOString(), // F due_date
      "pending", // G status
      0, // H asked
      "", // I asked_at
      "", // J checkin_reward
      "", // K done_at
      "", // L note
    ],
  ]);

  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "invite_add", game, name, email, due.toISOString()]]);
  } catch (_) {}

  return { invitedAt, due };
}

async function listInvites() {
  const rows = await getValues("INVITES!A2:L");
  return rows.map((r, i) => ({
    rowNumber: i + 2,
    ts_created: r[0] || "",
    game: (r[1] || "").toLowerCase(),
    name: r[2] || "",
    email: r[3] || "",
    invited_at: r[4] || "",
    due_date: r[5] || "",
    status: (r[6] || "").toLowerCase(),
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

async function addCheckinReward({ game, name, email, due_date, amount, chatId, userName }) {
  await appendValues("CHECKIN_REWARD!A1", [
    [nowIso(), game, name, email, due_date || "", amount, String(chatId || ""), userName || ""],
  ]);
}

/* =========================
 * SECTION 10 — Wallet + Machine data layer
 * ========================= */

/**
 * Wallets:
 * - WALLETS has wallet codes; balance is derived from WALLET_LOG sum amounts
 */
async function listWallets() {
  const rows = await getValues("WALLETS!A2:B");
  const wallets = [];
  for (const r of rows) {
    const code = String(r[0] || "").trim().toLowerCase();
    const name = String(r[1] || "").trim();
    if (!code) continue;
    wallets.push({ code, name: name || code.toUpperCase() });
  }
  // if none exists, create default (uri/hana/kt) by returning them (not writing)
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
    ts: r[0] || "",
    wallet: String(r[1] || "").trim().toLowerCase(),
    type: String(r[2] || "").trim().toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    ref_type: String(r[4] || "").trim().toLowerCase(),
    ref_id: String(r[5] || "").trim(),
    note: r[6] || "",
    chat_id: String(r[7] || ""),
  }));
}

async function addWalletLog({ wallet, type, amount, ref_type, ref_id, note, chatId }) {
  await appendValues("WALLET_LOG!A1", [
    [nowIso(), wallet, type, amount, ref_type || "", ref_id || "", note || "", String(chatId || "")],
  ]);

  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "wallet_log_add", wallet, type, amount, ref_type || "", ref_id || "", note || "", String(chatId || "")]]);
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

  return wallets.map((w) => ({
    code: w.code,
    name: w.name,
    balance: map.get(w.code) ?? 0,
  }));
}

/**
 * Phones table:
 * - We generate phone_id as P0001, P0002...
 */
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
  const next = max + 1;
  return "P" + String(next).padStart(4, "0");
}

async function addPhone({ buy_price, wallet, note }) {
  const phone_id = await nextPhoneId();
  const ts_buy = nowIso();
  const status = "new";

  await appendValues("PHONES!A1", [[phone_id, ts_buy, buy_price, wallet, status, note || ""]]);

  // ledger: buying machine is expense => negative
  await addWalletLog({
    wallet,
    type: "machine_buy",
    amount: -Math.abs(buy_price),
    ref_type: "phone",
    ref_id: phone_id,
    note: note || "",
    chatId: "",
  });

  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "phone_buy", phone_id, buy_price, wallet, note || ""]]);
  } catch (_) {}

  return { phone_id, ts_buy, status };
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
  const rowNumber = found.rowNumber;
  // PHONES columns: A id, B ts_buy, C buy_price, D wallet, E status, F note
  await updateValues(`PHONES!E${rowNumber}:E${rowNumber}`, [[status]]);
  return true;
}

async function addPhoneProfitLog({ phone_id, result, amount, note, wallet, chatId }) {
  await appendValues("PHONE_PROFIT_LOG!A1", [[nowIso(), phone_id, result, amount, note || "", wallet || "", String(chatId || "")]]);
}

async function readPhoneProfitLogs() {
  const rows = await getValues("PHONE_PROFIT_LOG!A2:G");
  return rows.map((r) => ({
    ts: r[0] || "",
    phone_id: String(r[1] || "").trim().toUpperCase(),
    result: String(r[2] || "").trim().toLowerCase(),
    amount: Number(String(r[3] || "0").replace(/,/g, "")) || 0,
    note: r[4] || "",
    wallet: String(r[5] || "").trim().toLowerCase(),
    chat_id: String(r[6] || ""),
  }));
}

async function readPhones() {
  const rows = await getValues("PHONES!A2:F");
  return rows
    .filter((r) => r.some((c) => String(c || "").trim() !== ""))
    .map((r) => ({
      phone_id: String(r[0] || "").trim().toUpperCase(),
      ts_buy: r[1] || "",
      buy_price: Number(String(r[2] || "0").replace(/,/g, "")) || 0,
      wallet: String(r[3] || "").trim().toLowerCase(),
      status: String(r[4] || "").trim().toLowerCase(),
      note: r[5] || "",
    }));
}

/**
 * Record machine result:
 * - result: loi / hue / tach
 * - amount:
 *      loi  => +amount
 *      hue  => 0
 *      tach => -amount (loss)
 * - It also writes WALLET_LOG as income/expense
 * - It updates phone status: ok for loi/hue; tach for tach
 */
async function recordMachineResult({ phone_id, result, amountAbs, note, chatId }) {
  const phone = await findPhoneRow(phone_id);
  if (!phone) {
    return { ok: false, error: "Không tìm thấy máy (phone_id) trong PHONES." };
  }

  const row = phone.row;
  const wallet = String(row[3] || "").trim().toLowerCase() || "unknown";

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
    return { ok: false, error: "Kết quả máy không hợp lệ." };
  }

  await addPhoneProfitLog({
    phone_id: String(phone_id).trim().toUpperCase(),
    result,
    amount: signed,
    note: note || "",
    wallet,
    chatId,
  });

  // ledger
  const ledgerType = result === "loi" ? "machine_profit" : result === "tach" ? "machine_loss" : "machine_break_even";
  await addWalletLog({
    wallet,
    type: ledgerType,
    amount: signed,
    ref_type: "phone",
    ref_id: String(phone_id).trim().toUpperCase(),
    note: note || "",
    chatId,
  });

  await updatePhoneStatus(phone_id, status);

  try {
    await appendValues("UNDO_LOG!A1", [[nowIso(), "phone_result", phone_id, result, signed, wallet, note || "", String(chatId || "")]]);
  } catch (_) {}

  return { ok: true, wallet, signed, status };
}

/* =========================
 * SECTION 11 — Reports (Revenue + Machine + Wallet)
 * ========================= */
function monthKey(ts) {
  if (!ts) return "";
  return String(ts).slice(0, 7);
}

async function reportTotalRevenue(chatId) {
  const rows = await readGameRevenue();
  const sum = rows.reduce((a, b) => a + b.amount, 0);
  await send(chatId, `📌 TỔNG DOANH THU (WON)\n= ${moneyWON(sum)}`, { reply_markup: rightKb() });
}

async function reportRevenueMonth(chatId, mKey) {
  const rows = await readGameRevenue();
  const sum = rows.filter((x) => monthKey(x.ts) === mKey).reduce((a, b) => a + b.amount, 0);
  await send(chatId, `📊 DOANH THU THÁNG ${mKey}\n= ${moneyWON(sum)}`, { reply_markup: rightKb() });
}

async function reportThisMonth(chatId) {
  const m = dayjs().format("YYYY-MM");
  await reportRevenueMonth(chatId, m);
}

async function reportLastMonth(chatId) {
  const m = dayjs().subtract(1, "month").format("YYYY-MM");
  await reportRevenueMonth(chatId, m);
}

async function reportStatsGames(chatId) {
  const rev = await readGameRevenue();
  const inv = await listInvites();

  // "Số người đá bóng": count revenue rows game=db & type=invite_reward
  const dbCount = rev.filter((x) => x.game === "db" && x.type === "invite_reward").length;

  // invite counts
  const hqCount = inv.filter((x) => x.game === "hq").length;
  const qrCount = inv.filter((x) => x.game === "qr").length;

  // revenue sums by game
  const dbSum = rev.filter((x) => x.game === "db").reduce((a, b) => a + b.amount, 0);
  const hqSum = rev.filter((x) => x.game === "hq").reduce((a, b) => a + b.amount, 0);
  const qrSum = rev.filter((x) => x.game === "qr").reduce((a, b) => a + b.amount, 0);

  const out =
    `4️⃣ THỐNG KÊ ĐB / HQ / QR (WON)\n\n` +
    `⚽ Đá bóng: người = ${dbCount} | doanh thu = ${moneyWON(dbSum)}\n` +
    `🎁 Hộp quà: người = ${hqCount} | doanh thu = ${moneyWON(hqSum)}\n` +
    `🔳 QR: người = ${qrCount} | doanh thu = ${moneyWON(qrSum)}\n`;

  await send(chatId, out, { reply_markup: rightKb() });
}

async function reportWallets(chatId) {
  const balances = await walletBalances();
  let total = 0;

  const lines = balances.map((b) => {
    total += b.balance;
    return `• ${b.name} (${b.code}): ${moneyWON(b.balance)}`;
  });

  const out = `💼 SỐ DƯ CÁC VÍ (ledger)\n\n${lines.join("\n")}\n\nTổng: ${moneyWON(total)}`;
  await send(chatId, out, { reply_markup: rightKb() });
}

async function reportMachineProfit(chatId) {
  const phones = await readPhones();
  const logs = await readPhoneProfitLogs();
  const walletLog = await readWalletLog();

  const totalPhones = phones.length;

  // categorize by last known status in PHONES
  const okCount = phones.filter((p) => p.status === "ok").length;
  const tachCount = phones.filter((p) => p.status === "tach").length;
  const newCount = phones.filter((p) => p.status === "new").length;

  // profit stats by log
  let loi = 0, hue = 0, tach = 0, sumProfit = 0;
  for (const l of logs) {
    sumProfit += l.amount;
    if (l.amount > 0) loi++;
    else if (l.amount === 0) hue++;
    else tach++;
  }

  // total buy cost from PHONES (expense)
  const totalBuy = phones.reduce((a, b) => a + (b.buy_price || 0), 0);

  // net machine = sumProfit - totalBuy? (We keep them separate by philosophy)
  // But user asked: "lời lỗ máy cũng tính tiền luôn"
  // We'll show both: total buy, total profit/loss logs, and computed net (profit - buy).
  const net = sumProfit - totalBuy;

  // per-wallet from WALLET_LOG limited to ref_type=phone
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
    `7️⃣ LỜI/LỖ MUA MÁY (WON)\n\n` +
    `📱 Số máy: ${totalPhones}\n` +
    `• New: ${newCount}\n` +
    `• OK: ${okCount}\n` +
    `• Tạch: ${tachCount}\n\n` +
    `📒 Log kết quả máy: ${logs.length}\n` +
    `• Lời: ${loi}\n` +
    `• Huề: ${hue}\n` +
    `• Tạch: ${tach}\n\n` +
    `💸 Tổng tiền mua máy: ${moneyWON(totalBuy)}\n` +
    `💰 Tổng lời/lỗ log: ${moneyWON(sumProfit)}\n` +
    `🧾 Net (log - mua): ${moneyWON(net)}\n\n` +
    `🏦 Theo ví (ledger ref=phone):\n${perWalletLines.join("\n")}`;

  await send(chatId, out, { reply_markup: rightKb() });
}

/* =========================
 * SECTION 12 — Guidance / Help text
 * ========================= */
function helpText() {
  return (
    `📘 HƯỚNG DẪN LỆNH (WON ₩)\n\n` +
    `✅ Doanh thu game:\n` +
    `- dabong 100k   | db 100k\n` +
    `- hopqua 200k   | hq 200k\n` +
    `- qr 57k\n` +
    `- them 0.5k (thu khác)\n\n` +
    `✅ Invite 14 ngày (chỉ HQ/QR):\n` +
    `- hopqua Ten email@gmail.com\n` +
    `- qr Ten email@gmail.com\n\n` +
    `✅ Máy + Ví:\n` +
    `- muamay 1200k hana [note]\n` +
    `- mayloi P0001 300k [note]\n` +
    `- mayhue P0001 [note]\n` +
    `- maytach P0001 800k [note]   (tạch = số âm)\n\n` +
    `✅ Ví:\n` +
    `- vi           (xem tất cả)\n` +
    `- vi hana      (xem riêng)\n` +
    `- sodu hana 5000k  (chỉnh ví về đúng số dư mục tiêu)\n\n` +
    `✅ Sửa lệnh:\n` +
    `- /edit last\n` +
    `- /edit machine last\n\n` +
    `✅ AI (fallback):\n` +
    `- /ai <câu hỏi>\n` +
    `- (bật/tắt ở ➡️ Menu)\n\n` +
    `🧠 Smart Parse (miễn phí):\n` +
    `- hiểu: "da bong 100k", "moi hop qua Khanh mail@gmail.com", "thu them 2k"...\n`
  );
}

/* =========================
 * SECTION 13 — Reset data
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
  "SETTINGS!A2:Z",
];

async function resetAllData() {
  for (const r of RESET_CLEAR_RANGES) {
    try {
      await clearValues(r);
    } catch (e) {
      // ignore missing sheets; still attempt remaining
      console.error("RESET clear error:", r, e?.message || e);
    }
  }
}

/* =========================
 * SECTION 14 — Sessions for step-by-step flows
 * ========================= */
/**
 * We keep sessions in-memory (per runtime).
 * Render sleep can wipe sessions; that's OK because user can restart flow.
 *
 * Session shapes:
 *  - invite: { flow:'invite', game:'hq|qr', step:'name|email', data:{name,email} }
 *  - revenue: { flow:'revenue', game:'db|hq|qr|other', step:'amount', data:{} }
 *  - reset: { flow:'reset', step:'pass' }
 *  - gemini_key: { flow:'gemini_key', step:'key' }
 *  - wallet_adjust: { flow:'wallet_adjust', step:'wallet|amount', data:{} }
 *  - machine_buy: { flow:'machine_buy', step:'price|wallet|note', data:{} }
 *  - machine_result: { flow:'machine_result', step:'phone|result|amount|note', data:{} }
 *  - edit_revenue: { flow:'edit_revenue', step:'amount', data:{game,type,amount,chat_id} }
 *  - edit_machine: { flow:'edit_machine', step:'amount', data:{phone_id,amount,wallet,result} }
 */
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
 * SECTION 15 — Edit last logic (Revenue + Machine)
 * ========================= */
async function getLastRevenueForChat(chatId) {
  const rows = await getValues("GAME_REVENUE!A2:G");
  const target = String(chatId);
  for (let i = rows.length - 1; i >= 0 && i >= rows.length - 4000; i--) {
    const r = rows[i];
    const game = (r[1] || "").toLowerCase();
    const type = (r[2] || "").toLowerCase();
    const amount = Number(String(r[3] || "0").replace(/,/g, "")) || 0;
    const cid = String(r[5] || "");
    if (cid === target && ["db", "hq", "qr", "other"].includes(game)) {
      return { game, type: type || "invite_reward", amount };
    }
  }
  return null;
}

async function startEditLastRevenue(chatId) {
  const last = await getLastRevenueForChat(chatId);
  if (!last) {
    await send(chatId, "❌ Không tìm thấy lệnh doanh thu gần nhất để sửa.", { reply_markup: mainKb() });
    return;
  }
  setSession(chatId, { flow: "edit_revenue", step: "amount", data: last });
  await send(
    chatId,
    `✏️ SỬA DOANH THU GẦN NHẤT\nLệnh gần nhất: ${last.game.toUpperCase()} ${moneyWON(last.amount)}\nNhập số tiền MỚI (vd 80k):`,
    { reply_markup: mainKb() }
  );
}

async function getLastMachineLogForChat(chatId) {
  const rows = await getValues("PHONE_PROFIT_LOG!A2:G");
  const target = String(chatId);
  for (let i = rows.length - 1; i >= 0 && i >= rows.length - 4000; i--) {
    const r = rows[i];
    const phone_id = String(r[1] || "").trim().toUpperCase();
    const result = String(r[2] || "").trim().toLowerCase();
    const amount = Number(String(r[3] || "0").replace(/,/g, "")) || 0;
    const wallet = String(r[5] || "").trim().toLowerCase();
    const cid = String(r[6] || "");
    if (cid === target && phone_id) {
      return { phone_id, result, amount, wallet };
    }
  }
  return null;
}

async function startEditLastMachine(chatId) {
  const last = await getLastMachineLogForChat(chatId);
  if (!last) {
    await send(chatId, "❌ Không tìm thấy log máy gần nhất để sửa.", { reply_markup: mainKb() });
    return;
  }
  // We edit by asking NEW absolute amount (meaning: for loi => +, tach => -, hue => 0)
  // To keep simple for user, we ask: "nhập số mới" and interpret by result:
  // - loi expects positive
  // - tach expects positive input but we store negative
  // - hue expects 0 (still allow)
  setSession(chatId, { flow: "edit_machine", step: "amount", data: last });
  const oldDisplay = moneyWON(Math.abs(last.amount));
  await send(
    chatId,
    `✏️ SỬA LOG MÁY GẦN NHẤT\nMáy: ${last.phone_id}\nKết quả: ${last.result}\nSố cũ: ${oldDisplay}\nNhập số MỚI (vd 300k). (hue nhập 0):`,
    { reply_markup: mainKb() }
  );
}

/* =========================
 * SECTION 16 — Cron remind 14-day (admin)
 * ========================= */
const awaitingCheckin = new Map();

/**
 * Ask admin for checkin reward
 */
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

/* =========================
 * SECTION 17 — Gemini API (fallback only)
 * ========================= */
/**
 * Gemini REST:
 *  https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key=API_KEY
 * We keep it to support corner cases; Smart Parse handles most.
 */
async function geminiGenerate(apiKey, model, prompt, responseMimeType = null) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

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

  const text = json?.candidates?.[0]?.content?.parts?.map((p) => p.text || "").join("") || "";
  return text.trim();
}

function buildGeminiParsePrompt(userText) {
  return (
    "Bạn là bộ phân tích lệnh cho bot TikTok Lite. " +
    "Trả về DUY NHẤT 1 object JSON, không giải thích.\n\n" +
    "Các action hợp lệ:\n" +
    '1) {"action":"revenue","game":"db|hq|qr|other","amount":<number>}\n' +
    '2) {"action":"invite","game":"hq|qr","name":"...","email":"..."}\n' +
    '3) {"action":"machine_buy","price":<number>,"wallet":"hana|uri|kt","note":"..."}\n' +
    '4) {"action":"machine_result","phone_id":"P0001","result":"loi|hue|tach","amount":<number>,"note":"..."}\n' +
    '5) {"action":"wallet_adjust","wallet":"hana|uri|kt","target":<number>}\n' +
    '6) {"action":"unknown"}\n\n' +
    "Quy ước:\n" +
    "- amount/price/target là số WON.\n" +
    "- 'tach' là lỗ (nhập số dương, hệ thống sẽ lưu âm nếu cần).\n\n" +
    `Input: ${userText}\n\nJSON:`
  );
}

async function tryGeminiFallback(chatId, userName, text) {
  const { apiKey, enabled, model } = await getGeminiConfig();
  if (!enabled) return { handled: false };
  if (!apiKey) {
    await send(chatId, "⚠️ AI đang bật nhưng chưa có Gemini Key. Vào ➡️ Menu → 🔑 Nhập Gemini Key.", { reply_markup: rightKb() });
    return { handled: true };
  }

  try {
    const prompt = buildGeminiParsePrompt(text);
    const out = await geminiGenerate(apiKey, model, prompt, "application/json");

    let obj = null;
    try {
      obj = JSON.parse(out);
    } catch {
      const m = out.match(/\{[\s\S]*\}/);
      if (m) obj = JSON.parse(m[0]);
    }
    if (!obj || !obj.action) return { handled: false };

    // Apply parsed actions
    if (obj.action === "revenue") {
      const game = String(obj.game || "").toLowerCase();
      const amount = Number(obj.amount);
      if (!["db", "hq", "qr", "other"].includes(game) || !Number.isFinite(amount)) return { handled: false };

      const type = game === "other" ? "other" : "invite_reward";
      await addGameRevenue({ game, type, amount: Math.round(amount), note: "GEMINI_PARSE", chatId, userName });
      await send(chatId, `🤖✅ AI ghi thu: ${game.toUpperCase()} ${moneyWON(Math.round(amount))}`, { reply_markup: mainKb() });
      return { handled: true };
    }

    if (obj.action === "invite") {
      const game = String(obj.game || "").toLowerCase();
      const name = String(obj.name || "").trim();
      const email = String(obj.email || "").trim();
      if (!["hq", "qr"].includes(game) || !name || !isEmail(email)) return { handled: false };

      const { due } = await addInvite({ game, name, email });
      await send(chatId, `🤖✅ AI lưu INVITE: ${game.toUpperCase()} ${name} (${email})\nDue: ${dayjs(due).format("DD/MM/YYYY")}`, {
        reply_markup: mainKb(),
      });
      return { handled: true };
    }

    if (obj.action === "machine_buy") {
      const price = Number(obj.price);
      const wallet = String(obj.wallet || "").trim().toLowerCase();
      const note = String(obj.note || "").trim();
      if (!Number.isFinite(price) || price <= 0 || !wallet) return { handled: false };

      const r = await addPhone({ buy_price: Math.round(price), wallet, note });
      await send(chatId, `🤖✅ AI mua máy: ${r.phone_id}\nGiá: ${moneyWON(Math.round(price))}\nVí: ${wallet}`, { reply_markup: mainKb() });
      return { handled: true };
    }

    if (obj.action === "machine_result") {
      const phone_id = String(obj.phone_id || "").trim().toUpperCase();
      const result = String(obj.result || "").trim().toLowerCase();
      const amount = Number(obj.amount);
      const note = String(obj.note || "").trim();
      if (!phone_id || !["loi", "hue", "tach"].includes(result) || !Number.isFinite(amount)) return { handled: false };

      const rr = await recordMachineResult({ phone_id, result, amountAbs: Math.round(Math.abs(amount)), note, chatId });
      if (!rr.ok) {
        await send(chatId, `⚠️ AI parse OK nhưng ghi máy lỗi: ${rr.error}`, { reply_markup: mainKb() });
        return { handled: true };
      }
      await send(chatId, `🤖✅ AI ghi máy ${phone_id}: ${result} ${moneyWON(Math.abs(rr.signed))} (ví ${rr.wallet})`, { reply_markup: mainKb() });
      return { handled: true };
    }

    if (obj.action === "wallet_adjust") {
      const wallet = String(obj.wallet || "").trim().toLowerCase();
      const target = Number(obj.target);
      if (!wallet || !Number.isFinite(target)) return { handled: false };

      const balances = await walletBalances();
      const cur = balances.find((b) => b.code === wallet)?.balance ?? 0;
      const delta = Math.round(target) - cur;

      await addWalletLog({
        wallet,
        type: "adjust",
        amount: delta,
        ref_type: "wallet",
        ref_id: wallet,
        note: "GEMINI_ADJUST_TO_TARGET",
        chatId,
      });

      await send(chatId, `🤖✅ AI chỉnh ví ${wallet}: ${moneyWON(cur)} → ${moneyWON(Math.round(target))}`, { reply_markup: mainKb() });
      return { handled: true };
    }

    return { handled: false };
  } catch (e) {
    await send(chatId, `⚠️ AI lỗi: ${e?.message || e}`, { reply_markup: mainKb() });
    return { handled: true };
  }
}

/* =========================
 * SECTION 18 — Smart Parse (FREE) engine
 * ========================= */
/**
 * Smart Parse goal:
 * - If user types something not exactly in strict commands, we try to infer:
 *   - revenue (db/hq/qr/other + money)
 *   - invite (hq/qr + email + name)
 *   - machine buy (muamay + price + wallet)
 *   - machine result (loi/hue/tach + phone_id + money)
 *   - wallet view/adjust (vi, sodu)
 *
 * This runs ONLY when:
 * - Not in session flow
 * - Not matching strict command patterns
 *
 * It is enabled/disabled by setting SMART_PARSE_ENABLED
 */
function detectWalletFromText(normText) {
  const t = ` ${normText} `;
  // common wallet codes
  const candidates = [" hana ", " uri ", " kt "];
  for (const c of candidates) {
    if (t.includes(c)) return c.trim();
  }
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
  return t.includes(" muamay ") || t.includes(" mua may ") || t.includes(" mua ") && t.includes(" may ");
}

function looksLikeWalletAdjust(normText) {
  const t = ` ${normText} `;
  return t.includes(" sodu ") || t.includes(" so du ") || t.includes(" chinh so du ");
}

function looksLikeWalletView(normText) {
  const t = ` ${normText} `;
  return t.startsWith("vi ") || t === "vi" || t.includes(" xem vi ");
}

/**
 * Smart parse result object:
 * - { action:'revenue', game, amount }
 * - { action:'invite', game, name, email }
 * - { action:'machine_buy', price, wallet, note }
 * - { action:'machine_result', phone_id, result, amount, note }
 * - { action:'wallet_view', wallet? }
 * - { action:'wallet_adjust', wallet, target }
 * - { action:'unknown' }
 */
function smartParse(text) {
  const norm = normalizeForParse(text);
  const email = extractEmail(text);
  const phone_id = detectPhoneIdFromText(text);
  const amount = extractMoneyFromText(text);
  const game = detectGameFromText(norm);
  const wallet = detectWalletFromText(norm);
  const machineResult = detectMachineResultFromText(norm);

  // wallet view
  if (looksLikeWalletView(norm)) {
    return { action: "wallet_view", wallet: wallet || "" };
  }

  // wallet adjust
  if (looksLikeWalletAdjust(norm)) {
    if (wallet && amount != null) {
      return { action: "wallet_adjust", wallet, target: amount };
    }
    return { action: "wallet_adjust_incomplete", wallet: wallet || "", target: amount };
  }

  // machine buy
  if (looksLikeBuyMachine(norm)) {
    if (amount != null && wallet) {
      // note is the remaining text after removing obvious parts
      return { action: "machine_buy", price: amount, wallet, note: "" };
    }
    return { action: "machine_buy_incomplete", price: amount, wallet: wallet || "" };
  }

  // machine result
  if (machineResult) {
    // need phone_id; amount optional for hue
    if (phone_id && machineResult === "hue") {
      return { action: "machine_result", phone_id, result: "hue", amount: 0, note: "" };
    }
    if (phone_id && amount != null) {
      return { action: "machine_result", phone_id, result: machineResult, amount, note: "" };
    }
    return { action: "machine_result_incomplete", phone_id: phone_id || "", result: machineResult, amount };
  }

  // invite detection (if email exists + game hq/qr)
  if (email && (game === "hq" || game === "qr")) {
    // name extraction: remove email and keywords
    let nameGuess = String(text || "");
    nameGuess = nameGuess.replace(email, " ");
    // remove keywords commonly
    const n = normalizeForParse(nameGuess);
    // try to map back original by trimming
    // We'll take original (without diacritics) just to decide if any name remains; then use raw trimmed tokens
    const rawTokens = String(nameGuess).trim().split(/\s+/).filter(Boolean);
    // remove known tokens like hopqua/hq/qr/moi
    const filtered = rawTokens.filter((tok) => {
      const t = normalizeForParse(tok);
      const bad = ["hopqua", "hop", "qua", "hq", "qr", "moi", "invite", "moi", "moi:", "moi-"];
      return !bad.includes(t);
    });
    const name = filtered.join(" ").trim() || "NoName";
    return { action: "invite", game, name, email };
  }

  // revenue detection (game + money)
  if (game && amount != null) {
    return { action: "revenue", game, amount };
  }

  // other: if money exists but no explicit game, try infer "other"
  if (!game && amount != null) {
    // if user says "thu" or "them"
    if (norm.includes(" thu ") || norm.includes(" them ") || norm.includes(" ngoai ")) {
      return { action: "revenue", game: "other", amount };
    }
  }

  return { action: "unknown" };
}

/* =========================
 * SECTION 19 — Strict command parsing (kept)
 * ========================= */
function parseStrictCommand(text) {
  const raw = String(text || "").trim();
  const parts = raw.split(/\s+/).filter(Boolean);
  const cmd = (parts[0] || "").toLowerCase();

  // system commands handled elsewhere
  // revenue strict:
  // - dabong 100k / db 100k
  // - hopqua 200k / hq 200k
  // - qr 57k
  // - them 2k
  // invite strict:
  // - hopqua Ten mail
  // - qr Ten mail
  // machine strict:
  // - muamay 1200k hana [note...]
  // - mayloi P0001 300k [note...]
  // - mayhue P0001 [note...]
  // - maytach P0001 800k [note...]
  // wallet strict:
  // - vi / vi hana
  // - sodu hana 5000k

  const gameMap = {
    dabong: "db",
    db: "db",
    hopqua: "hq",
    hq: "hq",
    qr: "qr",
  };

  // them
  if (cmd === "them") {
    const amt = parseMoney(parts[1]);
    if (amt == null) return null;
    return { action: "revenue", game: "other", amount: amt, note: "them" };
  }

  // wallet view
  if (cmd === "vi") {
    const w = parts[1] ? String(parts[1]).trim().toLowerCase() : "";
    return { action: "wallet_view", wallet: w };
  }

  // wallet adjust to target
  if (cmd === "sodu") {
    const w = parts[1] ? String(parts[1]).trim().toLowerCase() : "";
    const amt = parseMoney(parts[2]);
    if (!w || amt == null) return null;
    return { action: "wallet_adjust", wallet: w, target: amt };
  }

  // machine buy
  if (cmd === "muamay") {
    const price = parseMoney(parts[1]);
    const wallet = parts[2] ? String(parts[2]).trim().toLowerCase() : "";
    const note = parts.slice(3).join(" ");
    if (price == null || !wallet) return null;
    return { action: "machine_buy", price, wallet, note };
  }

  // machine result
  if (cmd === "mayloi" || cmd === "maytach" || cmd === "mayhue") {
    const phone_id = parts[1] ? String(parts[1]).trim().toUpperCase() : "";
    const note = cmd === "mayhue" ? parts.slice(2).join(" ") : parts.slice(3).join(" ");
    if (!phone_id) return null;

    if (cmd === "mayhue") {
      return { action: "machine_result", phone_id, result: "hue", amount: 0, note };
    }

    const amt = parseMoney(parts[2]);
    if (amt == null) return null;

    if (cmd === "mayloi") return { action: "machine_result", phone_id, result: "loi", amount: amt, note };
    if (cmd === "maytach") return { action: "machine_result", phone_id, result: "tach", amount: amt, note };
  }

  // game revenue or invite
  if (gameMap[cmd]) {
    const game = gameMap[cmd];

    // invite if there is email and 3 tokens
    if ((game === "hq" || game === "qr") && parts.length >= 3 && isEmail(parts[2]) && parseMoney(parts[1]) == null) {
      return { action: "invite", game, name: parts[1], email: parts[2] };
    }

    // revenue if money in 2nd token
    const amt = parseMoney(parts[1]);
    if (amt != null) {
      return { action: "revenue", game, amount: amt, note: cmd };
    }
  }

  return null;
}

/* =========================
 * SECTION 20 — Action executors (central)
 * ========================= */
async function executeAction(chatId, userName, actionObj) {
  // Keep all in one place to ensure consistent output and WON formatting.
  const a = actionObj;

  if (a.action === "revenue") {
    const game = a.game;
    const amount = a.amount;
    const type = game === "other" ? "other" : "invite_reward";
    await addGameRevenue({
      game,
      type,
      amount,
      note: a.note || "input",
      chatId,
      userName,
    });
    await send(chatId, `✅ Đã ghi doanh thu ${game.toUpperCase()}: ${moneyWON(amount)}`, { reply_markup: mainKb() });
    return { ok: true };
  }

  if (a.action === "invite") {
    const game = a.game;
    const name = a.name;
    const email = a.email;
    const { due } = await addInvite({ game, name, email });
    await send(
      chatId,
      `✅ Đã lưu INVITE ${game.toUpperCase()}:\n- Tên: ${name}\n- Mail: ${email}\n- Due: ${dayjs(due).format("DD/MM/YYYY")}\n\n⏰ Bot sẽ nhắc khi tới hạn.`,
      { reply_markup: mainKb() }
    );
    return { ok: true };
  }

  if (a.action === "wallet_view") {
    // If wallet specified, show that only
    const balances = await walletBalances();
    if (!a.wallet) {
      let total = 0;
      const lines = balances.map((b) => {
        total += b.balance;
        return `• ${b.name} (${b.code}): ${moneyWON(b.balance)}`;
      });
      await send(chatId, `💼 SỐ DƯ CÁC VÍ\n\n${lines.join("\n")}\n\nTổng: ${moneyWON(total)}`, { reply_markup: rightKb() });
      return { ok: true };
    } else {
      const w = String(a.wallet).trim().toLowerCase();
      const found = balances.find((b) => b.code === w);
      if (!found) {
        await send(chatId, `❌ Không tìm thấy ví '${w}'.`, { reply_markup: rightKb() });
        return { ok: true };
      }
      await send(chatId, `💼 VÍ ${found.name} (${found.code})\n= ${moneyWON(found.balance)}`, { reply_markup: rightKb() });
      return { ok: true };
    }
  }

  if (a.action === "wallet_adjust") {
    const wallet = String(a.wallet || "").trim().toLowerCase();
    const target = Number(a.target);
    if (!wallet || !Number.isFinite(target)) {
      await send(chatId, "❌ Sai cú pháp chỉnh ví. Ví dụ: sodu hana 5000k", { reply_markup: rightKb() });
      return { ok: true };
    }
    const balances = await walletBalances();
    const cur = balances.find((b) => b.code === wallet)?.balance ?? 0;
    const delta = Math.round(target) - cur;

    await addWalletLog({
      wallet,
      type: "adjust",
      amount: delta,
      ref_type: "wallet",
      ref_id: wallet,
      note: "ADJUST_TO_TARGET",
      chatId,
    });

    await send(chatId, `✅ Đã chỉnh ví ${wallet}: ${moneyWON(cur)} → ${moneyWON(Math.round(target))}`, { reply_markup: rightKb() });
    return { ok: true };
  }

  if (a.action === "machine_buy") {
    const price = Number(a.price);
    const wallet = String(a.wallet || "").trim().toLowerCase();
    const note = a.note || "";
    if (!Number.isFinite(price) || price <= 0 || !wallet) {
      await send(chatId, "❌ Sai cú pháp mua máy. Ví dụ: muamay 1200k hana", { reply_markup: leftKb() });
      return { ok: true };
    }

    const r = await addPhone({ buy_price: Math.round(price), wallet, note });
    await send(
      chatId,
      `✅ Đã mua máy: ${r.phone_id}\nGiá: ${moneyWON(Math.round(price))}\nVí: ${wallet}\nTrạng thái: ${r.status}`,
      { reply_markup: leftKb() }
    );
    return { ok: true };
  }

  if (a.action === "machine_result") {
    const phone_id = String(a.phone_id || "").trim().toUpperCase();
    const result = String(a.result || "").trim().toLowerCase();
    const amount = Number(a.amount);
    const note = a.note || "";

    if (!phone_id || !["loi", "hue", "tach"].includes(result)) {
      await send(chatId, "❌ Sai cú pháp máy. Ví dụ: mayloi P0001 300k", { reply_markup: leftKb() });
      return { ok: true };
    }

    const amountAbs = result === "hue" ? 0 : Math.round(Math.abs(amount));
    if (result !== "hue" && (!Number.isFinite(amountAbs) || amountAbs <= 0)) {
      await send(chatId, "❌ Sai số tiền. Ví dụ: maytach P0001 800k", { reply_markup: leftKb() });
      return { ok: true };
    }

    const rr = await recordMachineResult({ phone_id, result, amountAbs, note, chatId });
    if (!rr.ok) {
      await send(chatId, `❌ ${rr.error}`, { reply_markup: leftKb() });
      return { ok: true };
    }

    const shown = result === "hue" ? "₩0" : moneyWON(Math.abs(rr.signed));
    await send(
      chatId,
      `✅ Đã ghi kết quả máy ${phone_id}\nKQ: ${result}\nTiền: ${shown}\nVí: ${rr.wallet}\nStatus: ${rr.status}`,
      { reply_markup: leftKb() }
    );
    return { ok: true };
  }

  return { ok: false };
}

/* =========================
 * SECTION 21 — Menu-driven flows (sessions)
 * ========================= */
async function handleSessionInput(chatId, userName, text) {
  const sess = getSession(chatId);
  if (!sess) return { handled: false };

  // RESET
  if (sess.flow === "reset" && sess.step === "pass") {
    clearSession(chatId);
    if (text !== RESET_PASS) {
      await send(chatId, "❌ Sai PASS. Hủy xóa.", { reply_markup: rightKb() });
      return { handled: true };
    }
    await send(chatId, "⏳ Đang xóa dữ liệu...", { reply_markup: rightKb() });
    await resetAllData();
    await send(chatId, "✅ Đã XÓA SẠCH dữ liệu. Bot chạy mới từ đầu.", { reply_markup: mainKb() });
    return { handled: true };
  }

  // GEMINI KEY
  if (sess.flow === "gemini_key" && sess.step === "key") {
    clearSession(chatId);
    const key = String(text || "").trim();
    if (key.length < 20) {
      await send(chatId, "❌ Key có vẻ không đúng. Dán lại Gemini API Key.", { reply_markup: rightKb() });
      return { handled: true };
    }
    await setSetting("GEMINI_API_KEY", key);
    const m = (await getSetting("GEMINI_MODEL")).trim();
    if (!m) await setSetting("GEMINI_MODEL", "gemini-2.0-flash");
    await send(chatId, "✅ Đã lưu Gemini Key vào SETTINGS.", { reply_markup: rightKb() });
    return { handled: true };
  }

  // INVITE FLOW
  if (sess.flow === "invite") {
    if (sess.step === "name") {
      const name = String(text || "").trim();
      if (name.length < 2) {
        await send(chatId, "❌ Tên không hợp lệ. Nhập lại TÊN:", { reply_markup: leftKb() });
        return { handled: true };
      }
      sess.data.name = name;
      sess.step = "email";
      setSession(chatId, sess);
      await send(chatId, "📧 Nhập EMAIL:", { reply_markup: leftKb() });
      return { handled: true };
    }
    if (sess.step === "email") {
      const email = String(text || "").trim();
      if (!isEmail(email)) {
        await send(chatId, "❌ Email không hợp lệ. Nhập lại EMAIL:", { reply_markup: leftKb() });
        return { handled: true };
      }
      const { due } = await addInvite({ game: sess.game, name: sess.data.name, email });
      clearSession(chatId);
      await send(
        chatId,
        `✅ Đã lưu INVITE:\n- Game: ${sess.game.toUpperCase()}\n- Tên: ${sess.data.name}\n- Mail: ${email}\n- Due: ${dayjs(due).format("DD/MM/YYYY")}`,
        { reply_markup: leftKb() }
      );
      return { handled: true };
    }
  }

  // REVENUE FLOW
  if (sess.flow === "revenue" && sess.step === "amount") {
    const amt = parseMoney(text);
    if (amt == null) {
      await send(chatId, "❌ Sai tiền. Nhập lại (vd 100k / 0.5k / 100000):", { reply_markup: leftKb() });
      return { handled: true };
    }
    await executeAction(chatId, userName, { action: "revenue", game: sess.game, amount: amt, note: "menu" });
    clearSession(chatId);
    return { handled: true };
  }

  // WALLET ADJUST FLOW
  if (sess.flow === "wallet_adjust") {
    if (sess.step === "wallet") {
      const w = String(text || "").trim().toLowerCase();
      if (!w) {
        await send(chatId, "❌ Nhập mã ví (vd: hana / uri / kt):", { reply_markup: rightKb() });
        return { handled: true };
      }
      sess.data.wallet = w;
      sess.step = "amount";
      setSession(chatId, sess);
      await send(chatId, "Nhập số dư MỤC TIÊU (vd 5000k):", { reply_markup: rightKb() });
      return { handled: true };
    }
    if (sess.step === "amount") {
      const amt = parseMoney(text);
      if (amt == null) {
        await send(chatId, "❌ Sai tiền. Nhập lại (vd 5000k):", { reply_markup: rightKb() });
        return { handled: true };
      }
      await executeAction(chatId, userName, { action: "wallet_adjust", wallet: sess.data.wallet, target: amt });
      clearSession(chatId);
      return { handled: true };
    }
  }

  // MACHINE BUY FLOW
  if (sess.flow === "machine_buy") {
    if (sess.step === "price") {
      const price = parseMoney(text);
      if (price == null || price <= 0) {
        await send(chatId, "❌ Sai tiền. Nhập giá mua (vd 1200k):", { reply_markup: leftKb() });
        return { handled: true };
      }
      sess.data.price = price;
      sess.step = "wallet";
      setSession(chatId, sess);
      await send(chatId, "Nhập ví dùng để mua (hana / uri / kt):", { reply_markup: leftKb() });
      return { handled: true };
    }
    if (sess.step === "wallet") {
      const w = String(text || "").trim().toLowerCase();
      if (!w) {
        await send(chatId, "❌ Nhập mã ví (hana / uri / kt):", { reply_markup: leftKb() });
        return { handled: true };
      }
      sess.data.wallet = w;
      sess.step = "note";
      setSession(chatId, sess);
      await send(chatId, "Nhập ghi chú (hoặc gõ '-' để bỏ qua):", { reply_markup: leftKb() });
      return { handled: true };
    }
    if (sess.step === "note") {
      const note = String(text || "").trim();
      const finalNote = note === "-" ? "" : note;
      await executeAction(chatId, userName, {
        action: "machine_buy",
        price: sess.data.price,
        wallet: sess.data.wallet,
        note: finalNote,
      });
      clearSession(chatId);
      return { handled: true };
    }
  }

  // MACHINE RESULT FLOW
  if (sess.flow === "machine_result") {
    if (sess.step === "phone") {
      const pid = String(text || "").trim().toUpperCase();
      if (!pid.match(/^P\d{1,6}$/)) {
        await send(chatId, "❌ Sai mã máy. Ví dụ: P0001", { reply_markup: leftKb() });
        return { handled: true };
      }
      sess.data.phone_id = pid;
      sess.step = "result";
      setSession(chatId, sess);
      await send(chatId, "Nhập kết quả (loi / hue / tach):", { reply_markup: leftKb() });
      return { handled: true };
    }
    if (sess.step === "result") {
      const r = normalizeForParse(text);
      let result = "";
      if (r.includes("loi")) result = "loi";
      else if (r.includes("hue") || r.includes("hoa")) result = "hue";
      else if (r.includes("tach") || r.includes("chet")) result = "tach";
      else result = "";

      if (!["loi", "hue", "tach"].includes(result)) {
        await send(chatId, "❌ Kết quả không hợp lệ. Nhập: loi / hue / tach", { reply_markup: leftKb() });
        return { handled: true };
      }
      sess.data.result = result;
      if (result === "hue") {
        sess.data.amount = 0;
        sess.step = "note";
        setSession(chatId, sess);
        await send(chatId, "Nhập ghi chú (hoặc '-' để bỏ qua):", { reply_markup: leftKb() });
        return { handled: true };
      } else {
        sess.step = "amount";
        setSession(chatId, sess);
        await send(chatId, "Nhập số tiền (vd 300k). (tạch nhập số dương):", { reply_markup: leftKb() });
        return { handled: true };
      }
    }
    if (sess.step === "amount") {
      const amt = parseMoney(text);
      if (amt == null || amt < 0) {
        await send(chatId, "❌ Sai tiền. Nhập lại (vd 300k):", { reply_markup: leftKb() });
        return { handled: true };
      }
      sess.data.amount = amt;
      sess.step = "note";
      setSession(chatId, sess);
      await send(chatId, "Nhập ghi chú (hoặc '-' để bỏ qua):", { reply_markup: leftKb() });
      return { handled: true };
    }
    if (sess.step === "note") {
      const note = String(text || "").trim();
      const finalNote = note === "-" ? "" : note;
      await executeAction(chatId, userName, {
        action: "machine_result",
        phone_id: sess.data.phone_id,
        result: sess.data.result,
        amount: sess.data.amount,
        note: finalNote,
      });
      clearSession(chatId);
      return { handled: true };
    }
  }

  // EDIT REVENUE FLOW
  if (sess.flow === "edit_revenue" && sess.step === "amount") {
    const newAmt = parseMoney(text);
    if (newAmt == null) {
      await send(chatId, "❌ Sai tiền. Nhập lại (vd 80k):", { reply_markup: mainKb() });
      return { handled: true };
    }
    const old = sess.data;

    // adjustment: remove old + add new
    await addGameRevenue({ game: old.game, type: "adjust", amount: -old.amount, note: "EDIT_LAST_REMOVE", chatId, userName });
    await addGameRevenue({ game: old.game, type: "adjust", amount: newAmt, note: "EDIT_LAST_ADD", chatId, userName });

    clearSession(chatId);
    await send(chatId, `✅ Đã sửa ${old.game.toUpperCase()}: ${moneyWON(old.amount)} → ${moneyWON(newAmt)}`, { reply_markup: mainKb() });
    return { handled: true };
  }

  // EDIT MACHINE FLOW
  if (sess.flow === "edit_machine" && sess.step === "amount") {
    const newAbs = parseMoney(text);
    if (newAbs == null || newAbs < 0) {
      await send(chatId, "❌ Sai tiền. Nhập lại (vd 300k):", { reply_markup: mainKb() });
      return { handled: true };
    }
    const old = sess.data;
    const phone_id = old.phone_id;

    // old.amount is signed; new signed depends on result
    let newSigned = 0;
    if (old.result === "loi") newSigned = Math.abs(newAbs);
    if (old.result === "tach") newSigned = -Math.abs(newAbs);
    if (old.result === "hue") newSigned = 0;

    // adjustment strategy:
    // 1) write PHONE_PROFIT_LOG adjust entries:
    //    - add a compensating entry to cancel old
    //    - add new entry
    // 2) write WALLET_LOG adjust entries similarly (ref phone)
    // This avoids editing past rows.
    const wallet = old.wallet || "unknown";

    // cancel old
    await addPhoneProfitLog({
      phone_id,
      result: "adjust",
      amount: -old.amount,
      note: "EDIT_MACHINE_REMOVE",
      wallet,
      chatId,
    });
    await addWalletLog({
      wallet,
      type: "adjust",
      amount: -old.amount,
      ref_type: "phone",
      ref_id: phone_id,
      note: "EDIT_MACHINE_REMOVE",
      chatId,
    });

    // add new
    await addPhoneProfitLog({
      phone_id,
      result: "adjust",
      amount: newSigned,
      note: "EDIT_MACHINE_ADD",
      wallet,
      chatId,
    });
    await addWalletLog({
      wallet,
      type: "adjust",
      amount: newSigned,
      ref_type: "phone",
      ref_id: phone_id,
      note: "EDIT_MACHINE_ADD",
      chatId,
    });

    clearSession(chatId);
    await send(chatId, `✅ Đã sửa log máy ${phone_id}: ${moneyWON(Math.abs(old.amount))} → ${moneyWON(Math.abs(newSigned))}`, { reply_markup: mainKb() });
    return { handled: true };
  }

  return { handled: false };
}

/* =========================
 * SECTION 22 — Admin checkin reply handling
 * ========================= */
async function handleAdminCheckinReply(chatId, userName, text) {
  if (!ADMIN_TELEGRAM_ID) return { handled: false };
  if (String(chatId) !== String(ADMIN_TELEGRAM_ID)) return { handled: false };

  const inv = awaitingCheckin.get(ADMIN_TELEGRAM_ID);
  if (!inv) return { handled: false };

  const amt = parseMoney(text);
  if (amt == null) return { handled: false };

  // record
  await addCheckinReward({
    game: inv.game,
    name: inv.name,
    email: inv.email,
    due_date: inv.due_date,
    amount: amt,
    chatId,
    userName,
  });

  // also as revenue checkin
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

  await send(chatId, `✅ Check-in ${inv.game.toUpperCase()} ${inv.name}: ${moneyWON(amt)}`, { reply_markup: mainKb() });
  return { handled: true };
}

/* =========================
 * SECTION 23 — Main text handler
 * ========================= */
async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  if (!chatId) return;
  const userName = msg.from?.first_name || "User";
  const text = String(msg.text || "").trim();
  if (!text) return;

  // 0) Admin checkin direct reply
  const adminHandled = await handleAdminCheckinReply(chatId, userName, text);
  if (adminHandled.handled) return;

  // 1) System commands
  if (text === "/start") {
    clearSession(chatId);
    await send(chatId, `✅ Bot sẵn sàng (${VERSION})`, { reply_markup: mainKb() });
    return;
  }

  if (text === "/help") {
    await send(chatId, helpText(), { reply_markup: mainKb() });
    return;
  }

  if (text === "/edit last") {
    await startEditLastRevenue(chatId);
    return;
  }

  if (text === "/edit machine last") {
    await startEditLastMachine(chatId);
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

  // 2) Menu navigation
  if (text === "⬅️ Menu") {
    clearSession(chatId);
    await send(chatId, "⬅️ MENU TRÁI – Nhập liệu", { reply_markup: leftKb() });
    return;
  }

  if (text === "➡️ Menu") {
    clearSession(chatId);
    await send(chatId, "➡️ MENU PHẢI – Báo cáo / Máy / Ví / AI", { reply_markup: rightKb() });
    return;
  }

  if (text === "⬅️ Back") {
    clearSession(chatId);
    await send(chatId, "🏠 Menu chính", { reply_markup: mainKb() });
    return;
  }

  // 3) Right menu actions
  if (text === "1️⃣ Xem tổng doanh thu") return reportTotalRevenue(chatId);
  if (text === "2️⃣ Doanh thu tháng này") return reportThisMonth(chatId);
  if (text === "3️⃣ Doanh thu tháng trước") return reportLastMonth(chatId);
  if (text === "4️⃣ Thống kê ĐB / HQ / QR") return reportStatsGames(chatId);
  if (text === "7️⃣ Lời lỗ mua máy") return reportMachineProfit(chatId);

  if (text === "💼 Xem ví") return reportWallets(chatId);

  if (text === "➕ Chỉnh số dư ví") {
    setSession(chatId, { flow: "wallet_adjust", step: "wallet", data: {} });
    await send(chatId, "Nhập mã ví cần chỉnh (hana / uri / kt):", { reply_markup: rightKb() });
    return;
  }

  if (text === "📘 Hướng dẫn") {
    await send(chatId, helpText(), { reply_markup: rightKb() });
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
    await send(chatId, `🤖 AI đã ${(enabled ? "TẮT" : "BẬT")}.`, { reply_markup: rightKb() });
    return;
  }

  if (text === "🧠 Smart Parse: Bật/Tắt") {
    const cur = await isSmartParseEnabled();
    await setSetting("SMART_PARSE_ENABLED", cur ? "0" : "1");
    await send(chatId, `🧠 Smart Parse đã ${(cur ? "TẮT" : "BẬT")}.`, { reply_markup: rightKb() });
    return;
  }

  if (text === "8️⃣ Xóa sạch dữ liệu") {
    setSession(chatId, { flow: "reset", step: "pass" });
    await send(chatId, "⚠️ Nhập PASS 12345 để XÓA SẠCH:", { reply_markup: rightKb() });
    return;
  }

  // 4) Left menu actions -> start sessions
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
    setSession(chatId, { flow: "revenue", game: "db", step: "amount", data: {} });
    await send(chatId, "⚽ Đá bóng – nhập SỐ TIỀN (vd 100k):", { reply_markup: leftKb() });
    return;
  }

  if (text === "🎁 Hộp quà (thu)") {
    setSession(chatId, { flow: "revenue", game: "hq", step: "amount", data: {} });
    await send(chatId, "🎁 Hộp quà – nhập SỐ TIỀN (vd 200k):", { reply_markup: leftKb() });
    return;
  }

  if (text === "🔳 QR (thu)") {
    setSession(chatId, { flow: "revenue", game: "qr", step: "amount", data: {} });
    await send(chatId, "🔳 QR – nhập SỐ TIỀN (vd 57k):", { reply_markup: leftKb() });
    return;
  }

  if (text === "➕ Thu khác") {
    setSession(chatId, { flow: "revenue", game: "other", step: "amount", data: {} });
    await send(chatId, "➕ Thu khác – nhập SỐ TIỀN (vd 0.5k):", { reply_markup: leftKb() });
    return;
  }

  if (text === "📱 Mua máy") {
    setSession(chatId, { flow: "machine_buy", step: "price", data: {} });
    await send(chatId, "📱 Mua máy – nhập GIÁ (vd 1200k):", { reply_markup: leftKb() });
    return;
  }

  if (text === "✅ Máy lời/huề/tạch") {
    setSession(chatId, { flow: "machine_result", step: "phone", data: {} });
    await send(chatId, "✅ Máy – nhập MÃ MÁY (vd P0001):", { reply_markup: leftKb() });
    return;
  }

  // 5) Session handler
  const sessHandled = await handleSessionInput(chatId, userName, text);
  if (sessHandled.handled) return;

  // 6) Strict command parsing
  const strict = parseStrictCommand(text);
  if (strict) {
    // execute strict actions
    await executeAction(chatId, userName, strict);
    return;
  }

  // 7) Smart Parse (free)
  const smartEnabled = await isSmartParseEnabled();
  if (smartEnabled) {
    const sp = smartParse(text);

    if (sp.action === "wallet_view") {
      await executeAction(chatId, userName, sp);
      return;
    }

    if (sp.action === "wallet_adjust") {
      await executeAction(chatId, userName, sp);
      return;
    }

    if (sp.action === "wallet_adjust_incomplete") {
      // If missing parts, start flow
      setSession(chatId, { flow: "wallet_adjust", step: "wallet", data: {} });
      await send(chatId, "🧠 Smart Parse: thiếu thông tin.\nNhập mã ví (hana/uri/kt):", { reply_markup: rightKb() });
      return;
    }

    if (sp.action === "machine_buy") {
      await executeAction(chatId, userName, sp);
      return;
    }

    if (sp.action === "machine_buy_incomplete") {
      // ask missing pieces
      setSession(chatId, { flow: "machine_buy", step: "price", data: {} });
      await send(chatId, "🧠 Smart Parse: thiếu thông tin mua máy.\nNhập GIÁ (vd 1200k):", { reply_markup: leftKb() });
      return;
    }

    if (sp.action === "machine_result") {
      await executeAction(chatId, userName, sp);
      return;
    }

    if (sp.action === "machine_result_incomplete") {
      setSession(chatId, { flow: "machine_result", step: "phone", data: {} });
      await send(chatId, "🧠 Smart Parse: thiếu thông tin máy.\nNhập MÃ MÁY (vd P0001):", { reply_markup: leftKb() });
      return;
    }

    if (sp.action === "invite") {
      // if we guessed name=NoName, ask name to confirm
      if (sp.name === "NoName") {
        setSession(chatId, { flow: "invite", game: sp.game, step: "name", data: { email: sp.email } });
        await send(chatId, `🧠 Smart Parse thấy email: ${sp.email}\nNhập TÊN:`, { reply_markup: leftKb() });
        return;
      }
      await executeAction(chatId, userName, sp);
      return;
    }

    if (sp.action === "revenue") {
      await executeAction(chatId, userName, sp);
      return;
    }
  }

  // 8) Gemini fallback (last resort)
  const gem = await tryGeminiFallback(chatId, userName, text);
  if (gem.handled) return;

  // 9) Unknown
  await send(chatId, "❓ Không hiểu. Vào ➡️ Menu → 📘 Hướng dẫn (hoặc bật Smart Parse / AI).", { reply_markup: mainKb() });
}

/* =========================
 * SECTION 24 — Webhook endpoint
 * ========================= */
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

/* =========================
 * SECTION 25 — Boot server
 * ========================= */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`✅ BOT READY on ${PORT} | ${VERSION}`);
});

/* =================================================================================================
 *  END OF FILE
 * =================================================================================================
 *
 *  QUICK TEST SCRIPT (manual):
 *   - /start
 *   - ⬅️ Menu -> 🎁 Hộp quà (mời) -> name -> email
 *   - ⬅️ Menu -> ⚽ Đá bóng (thu) -> 100k
 *   - gõ: "da bong ₩100,000"
 *   - gõ: "moi hop qua Khanh mail@gmail.com"
 *   - gõ: "muamay 1200k hana"
 *   - gõ: "mayloi P0001 300k"
 *   - ➡️ Menu -> 7️⃣ Lời lỗ mua máy / 💼 Xem ví
 *   - /edit last (then enter new amount)
 *   - /edit machine last (then enter new amount)
 *   - ➡️ Menu -> 🧠 Smart Parse: Bật/Tắt
 *   - ➡️ Menu -> 🤖 AI: Bật/Tắt + 🔑 Nhập Gemini Key
 *
 *  NOTE:
 *   - If your Render sleeps, cron reminders stop while sleeping. For 24/7 reminders you need
 *     a separate scheduler (Render Cron/Worker) or keep-alive ping.
 * =================================================================================================
 */
