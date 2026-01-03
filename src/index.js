// src/index.js
// TikTok Lite Reward Bot (Telegram)
// Spec focus: FIX "DANH SÁCH LÔ MÁY" + "PHÂN TÍCH MUA MÁY" + parsing chốt lô (ma01 hq1 tach2) + sold không làm mất kết quả
// NOTE: Giữ “đuôi cute tự động” như cũ (helper addCuteTail). Bạn có thể chỉnh nội dung đuôi ở CUTE_TAILS.

import express from "express";
import fetch from "node-fetch";

// =========================
// ENV
// =========================
const BOT_TOKEN = process.env.BOT_TOKEN || "";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";
const PORT = process.env.PORT || 3000;

const SHEET_WEBAPP_URL = process.env.SHEET_WEBAPP_URL || ""; // Google Apps Script WebApp
const SHEET_TOKEN = process.env.SHEET_TOKEN || ""; // optional auth token for sheet

if (!BOT_TOKEN) console.warn("⚠️ BOT_TOKEN missing");
if (!SHEET_WEBAPP_URL) console.warn("⚠️ SHEET_WEBAPP_URL missing");

// =========================
// CONSTANTS / PAYOUT (PHÂN TÍCH MÁY)
// =========================
const PAYOUT = {
  hq: 150_000,
  qr: 57_000,
  db: 100_000,
};

// status for each phone record in sheet
// new: chưa chốt
// ok: đã ăn game (hq/qr/db) -> lời
// tach: tạch -> lỗ
// hue: huề
// sold: đã bán (NHƯNG KHÔNG ĐƯỢC LÀM MẤT game/status trước đó; game vẫn giữ trong cột game)
const STATUS = {
  NEW: "new",
  OK: "ok",
  TACH: "tach",
  HUE: "hue",
  SOLD: "sold",
};

const GAME = {
  NONE: "",
  HQ: "hq",
  QR: "qr",
  DB: "db",
};

// =========================
// CUTE TAILS
// =========================
const CUTE_TAILS = [
  "😝💖",
  "🤭✨",
  "🥹🫶",
  "😚💕",
  "🥳💞",
  "😜💘",
];

function addCuteTail(text) {
  // giữ “đuôi cute tự động”
  const tail = CUTE_TAILS[Math.floor(Math.random() * CUTE_TAILS.length)];
  return `${text}\n\n${tail}`;
}

// =========================
// TELEGRAM HELPERS
// =========================
async function tg(method, payload) {
  const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const json = await res.json();
  if (!json.ok) {
    console.error("Telegram API error:", json);
  }
  return json;
}

async function sendMessage(chat_id, text, extra = {}) {
  return tg("sendMessage", {
    chat_id,
    text,
    parse_mode: "HTML",
    disable_web_page_preview: true,
    ...extra,
  });
}

// =========================
// SHEET API (Apps Script WebApp)
// =========================
async function sheetCall(action, data = {}) {
  if (!SHEET_WEBAPP_URL) throw new Error("SHEET_WEBAPP_URL missing");
  const res = await fetch(SHEET_WEBAPP_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(SHEET_TOKEN ? { Authorization: `Bearer ${SHEET_TOKEN}` } : {}),
    },
    body: JSON.stringify({ action, data }),
  });
  const json = await res.json();
  if (!json.ok) throw new Error(json.error || "sheet error");
  return json.data;
}

/**
 * Expected sheet actions (must exist in your Apps Script WebApp):
 * - get_user_state {chat_id}
 * - set_user_state {chat_id, state}
 * - lots_list {chat_id}
 * - lots_list_recent {chat_id, limit}
 * - lot_create {chat_id, brand, qty, total, wallet, note}
 * - phones_by_lot {chat_id, lot_id}
 * - phones_update_many {chat_id, updates:[{phone_id, status, game}]}
 * - phones_mark_sold {chat_id, lot_id, count, price, wallet}  // should mark sold for N phones that are not sold yet
 * - wallet_log_add {chat_id, type, amount, wallet, ref_id, note}
 * - wallet_logs {chat_id}
 * - phones_list_all {chat_id} // optional (Danh Sách Máy)
 */

// =========================
// SIMPLE USER STATE MACHINE
// =========================
const STATE = {
  IDLE: "idle",
  BUY_WAIT_WALLET: "buy_wait_wallet",
  BUY_WAIT_NOTE: "buy_wait_note",
  SELL_WAIT_WALLET: "sell_wait_wallet",
};

function defaultUserState() {
  return { mode: STATE.IDLE, temp: {} };
}

async function getUserState(chat_id) {
  try {
    const data = await sheetCall("get_user_state", { chat_id });
    return data?.state ? data.state : defaultUserState();
  } catch {
    return defaultUserState();
  }
}

async function setUserState(chat_id, state) {
  await sheetCall("set_user_state", { chat_id, state });
}

// =========================
// PARSING HELPERS
// =========================
function norm(s) {
  return (s || "")
    .toString()
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

// split letters+digits: "hq1" => "hq 1"
function normalizeForParse(s) {
  const raw = norm(s);
  return raw
    .replace(/([a-z]+)(\d+)/gi, "$1 $2")
    .replace(/(\d+)([a-z]+)/gi, "$1 $2")
    .replace(/[|,;]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseMoneyK(token) {
  // "50k" => 50000 ; "50000" => 50000 ; "50,000" => 50000
  const t = (token || "").toString().toLowerCase().replace(/,/g, "");
  if (/^\d+k$/.test(t)) return parseInt(t, 10) * 1000;
  if (/^\d+$/.test(t)) return parseInt(t, 10);
  return null;
}

function fmtWon(n) {
  const x = Number(n || 0);
  const sign = x < 0 ? "-" : "";
  const abs = Math.abs(x);
  return `${sign}₩${abs.toLocaleString("en-US")}`;
}

function fmtWonNoSymbol(n) {
  const x = Number(n || 0);
  return `${x.toLocaleString("en-US")}`;
}

function detectWalletToken(t) {
  const x = norm(t);
  if (["uri", "kt", "tm", "hana"].includes(x)) return x;
  return null;
}

function detectBrandToken(t) {
  const x = norm(t);
  if (["ss", "samsung"].includes(x)) return "Samsung";
  if (["ip", "iphone"].includes(x)) return "iPhone";
  return null;
}

// NOTE: "note" / model: user nhập Note4 / S9 / v.v. Nếu user nhập "-" thì bỏ qua.
function normalizeNoteInput(t) {
  const s = (t || "").toString().trim();
  if (!s) return "";
  if (s === "-" || s === "—") return "";
  return s;
}

// =========================
// COMMAND PARSERS
// =========================
function parseBuyCommand(text) {
  // examples:
  // "mua 3ss 50k"
  // "mua 2 ip 35k uri" (wallet via message OR asked later)
  const s = normalizeForParse(text);

  const parts = s.split(" ").filter(Boolean);
  if (parts.length < 3) return null;

  if (parts[0] !== "mua") return null;

  // find qty
  const qty = /^\d+$/.test(parts[1]) ? parseInt(parts[1], 10) : null;
  if (!qty || qty <= 0) return null;

  // brand token might be like "ss" or "ip"
  const brand = detectBrandToken(parts[2]);
  if (!brand) return null;

  // money token should appear next
  let total = null;
  for (let i = 3; i < parts.length; i++) {
    const m = parseMoneyK(parts[i]);
    if (m != null) {
      total = m;
      break;
    }
  }
  if (total == null) return null;

  // optional wallet present
  let wallet = null;
  for (let i = 3; i < parts.length; i++) {
    const w = detectWalletToken(parts[i]);
    if (w) {
      wallet = w;
      break;
    }
  }

  return { qty, brand, total, wallet };
}

function parseSellCommand(text) {
  // examples:
  // "ban 2ss 50k ma01 uri"
  // "ban 3 ss 40k ma01 kt"
  const s = normalizeForParse(text);
  const parts = s.split(" ").filter(Boolean);
  if (parts.length < 4) return null;
  if (parts[0] !== "ban") return null;

  // qty
  let idx = 1;
  let qty = null;

  if (/^\d+$/.test(parts[idx])) {
    qty = parseInt(parts[idx], 10);
    idx++;
  } else {
    // allow "ban 2ss"
    const m = parts[idx].match(/^(\d+)([a-z]+)$/);
    if (m) {
      qty = parseInt(m[1], 10);
      idx++;
    }
  }
  if (!qty || qty <= 0) return null;

  // brand token could be attached or separate; but we don't strictly need it
  let brandToken = parts[idx];
  let brand = detectBrandToken(brandToken);
  if (!brand) {
    // maybe was attached like "2ss" already consumed, then brandToken is money
    brand = detectBrandToken(parts[idx]) || null;
  } else {
    idx++;
  }

  // money
  let price = null;
  for (let i = idx; i < parts.length; i++) {
    const m = parseMoneyK(parts[i]);
    if (m != null) {
      price = m;
      idx = i + 1;
      break;
    }
  }
  if (price == null) return null;

  // lot id ma01
  let lot = null;
  for (let i = idx; i < parts.length; i++) {
    const p = parts[i];
    const mm = p.match(/^ma(\d+)$/);
    if (mm) {
      lot = `MA${mm[1].padStart(2, "0")}`;
      idx = i + 1;
      break;
    }
  }
  if (!lot) return null;

  // wallet optional
  let wallet = null;
  for (let i = idx; i < parts.length; i++) {
    const w = detectWalletToken(parts[i]);
    if (w) {
      wallet = w;
      break;
    }
  }

  return { qty, brand, price, lot, wallet };
}

function parseLotCode(text) {
  // ma01 / MA01 / ma 01
  const s = normalizeForParse(text);
  const parts = s.split(" ").filter(Boolean);
  for (let i = 0; i < parts.length; i++) {
    if (parts[i] === "ma" && /^\d+$/.test(parts[i + 1] || "")) {
      return `MA${parts[i + 1].padStart(2, "0")}`;
    }
    const mm = parts[i].match(/^ma(\d+)$/);
    if (mm) return `MA${mm[1].padStart(2, "0")}`;
  }
  return null;
}

function parseLotCloseCommand(text) {
  // supports:
  // "ma01 hq1 tach2"
  // "ma 01 hq 1 tach 2"
  // "chot lo ma01 hq1 qr1 tach1"
  // ALSO support legacy "loi 1 hq ..." but if "loi" without hq/qr/db -> need ask
  const s = normalizeForParse(text);
  const parts = s.split(" ").filter(Boolean);
  const lot = parseLotCode(s);
  if (!lot) return null;

  // build tokens after lot mention
  // we parse all pairs: (hq|qr|db|tach|hue|loi) + number
  const items = [];
  for (let i = 0; i < parts.length; i++) {
    const k = parts[i];
    const n = parts[i + 1];

    if (["hq", "qr", "db", "tach", "tạch", "hue", "huê", "hòa", "huề", "loi", "lời"].includes(k)) {
      if (!/^\d+$/.test(n || "")) continue;
      const count = parseInt(n, 10);

      if (k === "tạch") items.push({ type: "tach", count });
      else if (["huê", "huề", "hòa"].includes(k)) items.push({ type: "hue", count });
      else if (k === "lời") items.push({ type: "loi", count });
      else items.push({ type: k, count }); // hq/qr/db/tach/hue/loi
    }
  }

  if (!items.length) return null;

  // if "loi" exists but no hq/qr/db alongside it, we ask
  const hasLoi = items.some((x) => x.type === "loi");
  const hasAnyGame = items.some((x) => ["hq", "qr", "db"].includes(x.type));
  if (hasLoi && !hasAnyGame) {
    return { lot, needGameForLoi: true, items };
  }

  // Convert loi into "hq" by default? NO. We'll ignore "loi" if user also gave explicit game counts.
  const finalItems = [];
  for (const it of items) {
    if (it.type === "loi") continue; // do nothing; user should provide hq/qr/db explicitly
    finalItems.push(it);
  }

  return { lot, needGameForLoi: false, items: finalItems };
}

// =========================
// KEYBOARDS
// =========================
function mainKeyboard() {
  return {
    keyboard: [
      [{ text: "⬅️ Menu" }, { text: "➡️ Menu" }],
      [{ text: "📱 Mua Máy (Lô)" }, { text: "💸 Bán Máy" }],
      [{ text: "🧪 Kiểm Tra Máy (Tất cả)" }, { text: "🧪 20 Lô Gần Nhất" }],
      [{ text: "📃 Danh Sách Máy" }],
    ],
    resize_keyboard: true,
  };
}

function rightKeyboard() {
  return {
    keyboard: [
      [{ text: "📊 Phân Tích" }],
      [{ text: "⬅️ Back" }, { text: "➡️ Menu" }],
    ],
    resize_keyboard: true,
  };
}

// =========================
// BUSINESS LOGIC (SHEET DATA -> COMPUTE)
// =========================
function computeLotStats(lot, phones, walletLogs) {
  // phones: [{id, status, game, note, sold_at? ...}]
  // walletLogs: [{type, amount, ref_id, ...}]
  const totalBuy = Number(lot.total || 0);

  let soldAmount = 0;
  for (const w of walletLogs || []) {
    if (w.type === "machine_sell" && (w.ref_id || "").toUpperCase() === (lot.code || "").toUpperCase()) {
      soldAmount += Number(w.amount || 0);
    }
  }

  // Determine result counts (sold does not erase prior game)
  let cntHQ = 0,
    cntQR = 0,
    cntDB = 0;
  let cntTach = 0,
    cntHue = 0;

  let cntNew = 0;
  let cntSold = 0;

  let winNotSold = 0;
  let notSoldCount = 0;

  for (const p of phones || []) {
    const st = (p.status || "").toLowerCase();
    const gm = (p.game || "").toLowerCase();

    const isSold = st === STATUS.SOLD;
    if (isSold) cntSold++;

    // "new" is ONLY true if status is new and no result
    if (st === STATUS.NEW) cntNew++;

    // results:
    if (["hq", "qr", "db"].includes(gm)) {
      if (gm === "hq") cntHQ++;
      if (gm === "qr") cntQR++;
      if (gm === "db") cntDB++;
      if (!isSold) winNotSold++;
    } else {
      // no game
      if (st === STATUS.TACH) cntTach++;
      else if (st === STATUS.HUE) cntHue++;
    }

    if (!isSold) notSoldCount++;
  }

  const winTotal = cntHQ + cntQR + cntDB;
  const gameIncome = cntHQ * PAYOUT.hq + cntQR * PAYOUT.qr + cntDB * PAYOUT.db;

  const tempProfit = gameIncome - totalBuy;
  const realProfit = gameIncome + soldAmount - totalBuy;

  return {
    totalBuy,
    soldAmount,
    gameIncome,
    tempProfit,
    realProfit,
    cntHQ,
    cntQR,
    cntDB,
    winTotal,
    cntTach,
    cntHue,
    cntNew,
    cntSold,
    winNotSold,
    notSoldCount,
  };
}

function computeAllLotsAnalysis(lots, phonesByLot, walletLogs) {
  let sumBuy = 0;
  let sumGame = 0;
  let sumSold = 0;

  // machine counts (overlap allowed: Sold is separate label)
  let cWinHQ = 0,
    cWinQR = 0,
    cWinDB = 0;
  let cLoss = 0;
  let cHue = 0;
  let cNew = 0;
  let cSold = 0;
  let cWinNotSold = 0;

  // total phones considered: all phones across all lots
  let totalPhones = 0;

  for (const lot of lots || []) {
    const code = (lot.code || "").toUpperCase();
    const phones = phonesByLot[code] || [];
    const st = computeLotStats(lot, phones, walletLogs);

    sumBuy += st.totalBuy;
    sumGame += st.gameIncome;
    sumSold += st.soldAmount;

    cWinHQ += st.cntHQ;
    cWinQR += st.cntQR;
    cWinDB += st.cntDB;

    cLoss += st.cntTach;
    cHue += st.cntHue;
    cNew += st.cntNew;
    cSold += st.cntSold;
    cWinNotSold += st.winNotSold;

    totalPhones += phones.length;
  }

  const totalThu = sumGame + sumSold;
  const profit = totalThu - sumBuy;

  const winTotal = cWinHQ + cWinQR + cWinDB;

  return {
    sumBuy,
    sumGame,
    sumSold,
    totalThu,
    profit,
    totalPhones,
    counts: {
      winTotal,
      cWinHQ,
      cWinQR,
      cWinDB,
      cLoss,
      cHue,
      cNew,
      cSold,
      cWinNotSold,
    },
  };
}

function bar(percent, width = 18) {
  const n = Math.max(0, Math.min(width, Math.round((percent / 100) * width)));
  return "█".repeat(n) + " ".repeat(width - n);
}

function pct(part, total) {
  if (!total) return 0;
  return Math.round((part / total) * 100);
}

// =========================
// RENDERERS
// =========================
function renderLotsList(title, lots, phonesByLot, walletLogs) {
  let out = `🧪 <b>${title}</b>\n`;
  if (!lots || lots.length === 0) return addCuteTail(out + "\n(Trống)");

  for (const lot of lots) {
    const code = (lot.code || "").toUpperCase();
    const phones = phonesByLot[code] || [];
    const st = computeLotStats(lot, phones, walletLogs);

    const noteTxt = (lot.note || "").trim();
    const notePart = noteTxt ? ` ${noteTxt}` : ""; // nếu user không nhập note thì không hiện

    const wallet = (lot.wallet || "").toUpperCase();
    out += `\n• <b>${code}</b>: Mua ${lot.qty} máy ${lot.brand}${notePart} | Tổng ${fmtWon(st.totalBuy)} | Ví ${wallet}\n\n`;

    // Trạng thái: Lời / Huề / Lỗ / New / Sold
    // NEW = chỉ status new (không bị lẫn với còn lại chưa bán)
    out += `  Trạng thái: Lời ${st.winTotal} máy (HQ:${st.cntHQ} / QR:${st.cntQR} / DB:${st.cntDB}) / Huề ${st.cntHue} / Lỗ ${st.cntTach} / New ${st.cntNew} / Sold ${st.cntSold}\n`;

    // lời còn giữ = máy lời chưa bán
    out += `  Lời còn giữ: ${st.winNotSold} máy | Còn lại: ${st.notSoldCount} máy chưa bán\n\n`;

    out += `  Tổng thu game: ${fmtWon(st.gameIncome)}\n`;
    out += `  Lãi tạm: ${fmtWon(st.gameIncome)} - ${fmtWon(st.totalBuy)} = ${fmtWon(st.tempProfit)}\n`;
    out += `  Đã bán: ${fmtWon(st.soldAmount)}\n`;
    out += `  Lãi thực: ${fmtWon(st.gameIncome)} + ${fmtWon(st.soldAmount)} - ${fmtWon(st.totalBuy)} = ${fmtWon(st.realProfit)}\n`;
  }

  return addCuteTail(out.trim());
}

function renderAnalysis(lots, phonesByLot, walletLogs) {
  const a = computeAllLotsAnalysis(lots, phonesByLot, walletLogs);
  const c = a.counts;

  // Theo spec mới:
  // "Net tạm" = Thu game (không trừ mua)
  // "Net thực" = Tổng thu về (game + bán) (không trừ mua)
  // thêm dòng lời còn lại = (thu - mua)
  let out = `📊 <b>PHÂN TÍCH MUA MÁY</b>\n\n`;

  out += `💳 Đã bỏ ra mua máy: ${fmtWon(a.sumBuy)}\n`;
  out += `💰 Số tiền thu được: ${fmtWon(a.totalThu)}\n`;
  out += `   • Thu game (HQ/QR/DB): ${fmtWon(a.sumGame)}\n`;
  out += `   • Thu bán máy: ${fmtWon(a.sumSold)}\n\n`;

  out += `🧮 Net tạm (thu game): ${fmtWon(a.sumGame)}\n`;
  out += `🧮 Net thực (tổng thu): ${fmtWon(a.totalThu)}\n\n`;

  out += `✨ Thu về ${fmtWon(a.totalThu)} - ${fmtWon(a.sumBuy)} lời ${fmtWon(a.profit)}\n\n`;

  out += `Máy (tổng kết quả - tính kể cả Sold)\n`;
  out += `• Lời: ${c.winTotal} máy (HQ:${c.cWinHQ} / QR:${c.cWinQR} / DB:${c.cWinDB})\n`;
  out += `• Lỗ: ${c.cLoss} máy\n`;
  out += `• Huề: ${c.cHue} máy\n`;
  out += `• Chưa làm (New): ${c.cNew} máy\n`;
  out += `• Đã bán (Sold): ${c.cSold} máy\n`;
  out += `• Lời còn giữ: ${c.cWinNotSold} máy chưa bán\n\n`;

  // Charts
  const totalBase = a.totalPhones || 0;

  const pNew = pct(c.cNew, totalBase);
  const pWin = pct(c.winTotal, totalBase);
  const pLoss = pct(c.cLoss, totalBase);
  const pHue = pct(c.cHue, totalBase);
  const pSold = pct(c.cSold, totalBase);

  out += `📌 Biểu đồ trạng thái\n`;
  out += `New  : ${bar(pNew)} ${pNew}% (${c.cNew})\n`;
  out += `Lời  : ${bar(pWin)} ${pWin}% (${c.winTotal})\n`;
  out += `Lỗ   : ${bar(pLoss)} ${pLoss}% (${c.cLoss})\n`;
  out += `Huề  : ${bar(pHue)} ${pHue}% (${c.cHue})\n`;
  out += `Sold : ${bar(pSold)} ${pSold}% (${c.cSold})\n\n`;

  // Money chart (relative to max)
  const maxMoney = Math.max(a.sumBuy, a.sumGame, a.sumSold, 1);
  const pb = Math.round((a.sumBuy / maxMoney) * 100);
  const pg = Math.round((a.sumGame / maxMoney) * 100);
  const ps = Math.round((a.sumSold / maxMoney) * 100);

  out += `💸 Biểu đồ tiền\n`;
  out += `Bỏ ra (mua): ${bar(pb)} ${fmtWon(a.sumBuy)}\n`;
  out += `Thu game    : ${bar(pg)} ${fmtWon(a.sumGame)}\n`;
  out += `Thu bán     : ${bar(ps)} ${fmtWon(a.sumSold)}\n\n`;

  // Per-lot summary at end
  if (lots && lots.length > 1) {
    out += `━━━━━━━━━━━━━━━━\n`;
    out += `🧾 <b>TỔNG KẾT</b>\n`;
    out += `• TỔNG TIỀN MUA MÁY: ${fmtWon(a.sumBuy)}\n`;
    out += `• TỔNG THU VỀ: ${fmtWon(a.totalThu)}\n`;
    out += `• LỜI CÒN LẠI: ${fmtWon(a.profit)}\n\n`;

    const totalRes = c.winTotal + c.cLoss + c.cHue + c.cNew; // base outcome categories (sold overlaps)
    const pWin2 = pct(c.winTotal, totalRes);
    const pLoss2 = pct(c.cLoss, totalRes);
    out += `• LỜI ${pWin2}%\n`;
    out += `• LỖ ${pLoss2}%\n`;
  }

  return addCuteTail(out.trim());
}

// =========================
// CORE ACTIONS
// =========================
async function handleBuyFlow(chat_id, text) {
  const st = await getUserState(chat_id);

  // step 1: parse buy
  if (st.mode === STATE.IDLE) {
    const cmd = parseBuyCommand(text);
    if (!cmd) return false;

    // if wallet absent -> ask wallet
    const temp = { ...cmd };
    if (!cmd.wallet) {
      await setUserState(chat_id, { mode: STATE.BUY_WAIT_WALLET, temp });
      await sendMessage(
        chat_id,
        addCuteTail(`Okie 😚 Mua lô ${cmd.qty} máy <b>${cmd.brand}</b>, tổng <b>${fmtWon(cmd.total)}</b>\n\nTính tiền ví nào? (hana/uri/kt/tm) ✨`),
        { reply_markup: mainKeyboard() }
      );
      return true;
    }

    // wallet present -> ask note
    await setUserState(chat_id, { mode: STATE.BUY_WAIT_NOTE, temp });
    await sendMessage(
      chat_id,
      addCuteTail(`Okie 😚 Mua lô ${cmd.qty} máy <b>${cmd.brand}</b>, tổng <b>${fmtWon(cmd.total)}</b>\nVí: <b>${cmd.wallet.toUpperCase()}</b>\n\nNhập <b>note</b> (vd Note4) hoặc <b>-</b> để bỏ qua nha~ 🫶`),
      { reply_markup: mainKeyboard() }
    );
    return true;
  }

  // step 2: wallet
  if (st.mode === STATE.BUY_WAIT_WALLET) {
    const w = detectWalletToken(text);
    if (!w) {
      await sendMessage(chat_id, addCuteTail(`Bạn nhập ví giúp mình nha (hana/uri/kt/tm) 😝`), {
        reply_markup: mainKeyboard(),
      });
      return true;
    }
    const temp = { ...(st.temp || {}), wallet: w };
    await setUserState(chat_id, { mode: STATE.BUY_WAIT_NOTE, temp });
    await sendMessage(
      chat_id,
      addCuteTail(`Nhập <b>note</b> (vd Note4) hoặc <b>-</b> để bỏ qua nha~ 🫶`),
      { reply_markup: mainKeyboard() }
    );
    return true;
  }

  // step 3: note + create lot
  if (st.mode === STATE.BUY_WAIT_NOTE) {
    const note = normalizeNoteInput(text);
    const temp = st.temp || {};
    const qty = Number(temp.qty || 0);
    const brand = temp.brand || "";
    const total = Number(temp.total || 0);
    const wallet = temp.wallet || "";

    // create lot in sheet
    const created = await sheetCall("lot_create", {
      chat_id,
      brand,
      qty,
      total,
      wallet,
      note, // can be empty
    });

    await setUserState(chat_id, defaultUserState());

    const code = (created?.code || "").toUpperCase() || "MA??";
    const noteLine = note ? `\nNote: <b>${note}</b>` : ""; // nếu user không nhập thì không hiện
    const msg = `✅ Xong rồi nè 🥳\nTạo lô: <b>MÃ ${code.replace("MA", "")}</b>\nMua: <b>${qty}</b> máy <b>${brand}</b>${noteLine}\nTổng: <b>${fmtWon(total)}</b>\nVí: <b>${wallet.toUpperCase()}</b>`;
    await sendMessage(chat_id, addCuteTail(msg), { reply_markup: mainKeyboard() });
    return true;
  }

  return false;
}

async function handleSellFlow(chat_id, text) {
  const st = await getUserState(chat_id);

  if (st.mode === STATE.IDLE) {
    const cmd = parseSellCommand(text);
    if (!cmd) return false;

    // wallet missing -> ask
    if (!cmd.wallet) {
      await setUserState(chat_id, { mode: STATE.SELL_WAIT_WALLET, temp: cmd });
      await sendMessage(
        chat_id,
        addCuteTail(`Mình hiểu bạn đang bán lô <b>${cmd.lot}</b> x<b>${cmd.qty}</b> giá <b>${fmtWon(cmd.price)}</b>\n\nTiền về ví nào? (hana/uri/kt/tm) 😝`),
        { reply_markup: mainKeyboard() }
      );
      return true;
    }

    return await finalizeSell(chat_id, cmd);
  }

  if (st.mode === STATE.SELL_WAIT_WALLET) {
    const w = detectWalletToken(text);
    if (!w) {
      await sendMessage(chat_id, addCuteTail(`Bạn nhập ví giúp mình nha (hana/uri/kt/tm) 😝`), {
        reply_markup: mainKeyboard(),
      });
      return true;
    }
    const cmd = { ...(st.temp || {}), wallet: w };
    await setUserState(chat_id, defaultUserState());
    return await finalizeSell(chat_id, cmd);
  }

  return false;
}

async function finalizeSell(chat_id, cmd) {
  // mark sold N phones in that lot (do NOT erase game column)
  const lotCode = cmd.lot.toUpperCase();

  // get lots to find note/model for sold line
  const lots = await sheetCall("lots_list", { chat_id });
  const lot = (lots || []).find((x) => (x.code || "").toUpperCase() === lotCode);
  const noteTxt = (lot?.note || "").trim();

  await sheetCall("phones_mark_sold", {
    chat_id,
    lot_id: lotCode,
    count: cmd.qty,
    price: cmd.price,
    wallet: cmd.wallet,
  });

  // wallet log for selling money (analysis uses this)
  await sheetCall("wallet_log_add", {
    chat_id,
    type: "machine_sell",
    amount: cmd.price,
    wallet: cmd.wallet,
    ref_id: lotCode,
    note: `sell ${cmd.qty}`,
  });

  const notePart = noteTxt ? ` ${noteTxt}` : "";
  const msg = `💸 <b>BÁN XONG</b> 🥳\n• Lô: <b>MÃ ${lotCode.replace("MA", "")}</b>\n• Số máy: <b>${cmd.qty}</b> máy${notePart}\n• Tiền về ví <b>${cmd.wallet.toUpperCase()}</b>: <b>${fmtWon(cmd.price)}</b>\n\nPhân tích lô sẽ tự cộng tiền bán này vào nhé 😝 💖`;
  await sendMessage(chat_id, addCuteTail(msg), { reply_markup: mainKeyboard() });
  return true;
}

async function handleLotClose(chat_id, text) {
  const parsed = parseLotCloseCommand(text);
  if (!parsed) return false;

  if (parsed.needGameForLoi) {
    await sendMessage(
      chat_id,
      addCuteTail(`Bạn ghi <b>lời</b> là từ <b>HQ</b> hay <b>QR</b> hay <b>DB</b> vậy nè? 😚\nVí dụ: <b>${parsed.lot.toLowerCase()} hq1 tach2</b>`),
      { reply_markup: mainKeyboard() }
    );
    return true;
  }

  const lotCode = parsed.lot.toUpperCase();
  const items = parsed.items || [];

  // fetch phones of lot
  const phones = await sheetCall("phones_by_lot", { chat_id, lot_id: lotCode });
  const phoneList = phones || [];

  // pick phones by priority:
  // - when chốt result, we should only update phones that are NOT sold yet? Actually sold should still have result kept.
  // But user usually chốt before bán. If sold happened first, allow updating not-sold first then sold ones if needed.
  const available = [...phoneList].filter((p) => true);

  // Helper to allocate N phones whose status is NEW first, then others if still missing (but never overwrite existing game result)
  function pickPhonesForResult(count) {
    const picked = [];
    // prefer NEW & not sold
    for (const p of available) {
      if (picked.length >= count) break;
      if ((p.status || "").toLowerCase() === STATUS.NEW && (p.status || "").toLowerCase() !== STATUS.SOLD) {
        picked.push(p);
      }
    }
    // then NEW sold
    for (const p of available) {
      if (picked.length >= count) break;
      if ((p.status || "").toLowerCase() === STATUS.NEW && (p.status || "").toLowerCase() === STATUS.SOLD) {
        picked.push(p);
      }
    }
    // then any with empty game and not tach/hue/ok (fallback)
    for (const p of available) {
      if (picked.length >= count) break;
      const st = (p.status || "").toLowerCase();
      const gm = (p.game || "").toLowerCase();
      if (!gm && [STATUS.NEW, STATUS.SOLD].includes(st)) {
        if (!picked.includes(p)) picked.push(p);
      }
    }
    return picked.slice(0, count);
  }

  // build updates (do not destroy existing game if already set)
  const updates = [];
  let sumHQ = 0,
    sumQR = 0,
    sumDB = 0,
    sumTach = 0,
    sumHue = 0;

  for (const it of items) {
    const t = it.type;
    const count = Number(it.count || 0);
    if (count <= 0) continue;

    if (t === "hq" || t === "qr" || t === "db") {
      const picks = pickPhonesForResult(count);
      for (const p of picks) {
        // do not override existing game
        const existingGame = (p.game || "").toLowerCase();
        if (["hq", "qr", "db"].includes(existingGame)) continue;

        // keep sold status if already sold
        const st0 = (p.status || "").toLowerCase();
        const nextStatus = st0 === STATUS.SOLD ? STATUS.SOLD : STATUS.OK;

        updates.push({ phone_id: p.id, status: nextStatus, game: t });
      }
      if (t === "hq") sumHQ += count;
      if (t === "qr") sumQR += count;
      if (t === "db") sumDB += count;
    } else if (t === "tach") {
      const picks = pickPhonesForResult(count);
      for (const p of picks) {
        // don't override if already has game
        const existingGame = (p.game || "").toLowerCase();
        if (["hq", "qr", "db"].includes(existingGame)) continue;

        const st0 = (p.status || "").toLowerCase();
        const nextStatus = st0 === STATUS.SOLD ? STATUS.SOLD : STATUS.TACH;
        updates.push({ phone_id: p.id, status: nextStatus, game: GAME.NONE });
      }
      sumTach += count;
    } else if (t === "hue") {
      const picks = pickPhonesForResult(count);
      for (const p of picks) {
        const existingGame = (p.game || "").toLowerCase();
        if (["hq", "qr", "db"].includes(existingGame)) continue;

        const st0 = (p.status || "").toLowerCase();
        const nextStatus = st0 === STATUS.SOLD ? STATUS.SOLD : STATUS.HUE;
        updates.push({ phone_id: p.id, status: nextStatus, game: GAME.NONE });
      }
      sumHue += count;
    }
  }

  if (updates.length) {
    await sheetCall("phones_update_many", { chat_id, updates });
  }

  const gameIncome = sumHQ * PAYOUT.hq + sumQR * PAYOUT.qr + sumDB * PAYOUT.db;
  const winCount = sumHQ + sumQR + sumDB;

  // response exactly style requested, no "gợi ý bán"
  const msg =
    `🧾 <b>CHỐT LÔ MÃ ${lotCode.replace("MA", "")}</b>\n` +
    `✅ Lời: <b>${winCount}</b> MÁY (HQ:${sumHQ} / QR:${sumQR} / DB:${sumDB})\n` +
    `😵 Lỗ: <b>${sumTach}</b> MÁY TẠCH\n` +
    `😌 Huề: <b>${sumHue}</b>\n` +
    `🎮 Tổng thu game (phân tích): <b>${fmtWonNoSymbol(gameIncome)}</b> WON`;

  await sendMessage(chat_id, addCuteTail(msg), { reply_markup: mainKeyboard() });
  return true;
}

async function handleMenuButtons(chat_id, text) {
  const t = (text || "").trim();

  if (t === "⬅️ Menu") {
    await sendMessage(chat_id, addCuteTail("⬅️ Menu Trái đây nè~ ✨"), { reply_markup: mainKeyboard() });
    return true;
  }
  if (t === "➡️ Menu") {
    await sendMessage(chat_id, addCuteTail("➡️ Menu Phải đây nè~ (báo cáo + phân tích) 📊"), {
      reply_markup: rightKeyboard(),
    });
    return true;
  }
  if (t === "⬅️ Back") {
    await sendMessage(chat_id, addCuteTail("Về menu chính nha bạn iu~ 🏠"), { reply_markup: mainKeyboard() });
    return true;
  }

  if (t === "📱 Mua Máy (Lô)") {
    await sendMessage(chat_id, addCuteTail(`Bạn gõ: <b>mua 3ss 50k</b> hoặc <b>mua 2 ip 35k uri</b> nha~ 😙`), {
      reply_markup: mainKeyboard(),
    });
    return true;
  }

  if (t === "💸 Bán Máy") {
    await sendMessage(chat_id, addCuteTail(`Bạn gõ: <b>ban 2ss 50k ma01 uri</b> nha~ 😝`), {
      reply_markup: mainKeyboard(),
    });
    return true;
  }

  if (t === "🧪 Kiểm Tra Máy (Tất cả)") {
    const lots = await sheetCall("lots_list", { chat_id });
    const walletLogs = await sheetCall("wallet_logs", { chat_id });

    const phonesByLot = {};
    for (const lot of lots || []) {
      const code = (lot.code || "").toUpperCase();
      phonesByLot[code] = await sheetCall("phones_by_lot", { chat_id, lot_id: code });
    }

    const msg = renderLotsList("DANH SÁCH LÔ MÁY (Tất cả)", lots, phonesByLot, walletLogs);
    await sendMessage(chat_id, msg, { reply_markup: mainKeyboard() });
    return true;
  }

  if (t === "🧪 20 Lô Gần Nhất") {
    const lots = await sheetCall("lots_list_recent", { chat_id, limit: 20 });
    const walletLogs = await sheetCall("wallet_logs", { chat_id });

    const phonesByLot = {};
    for (const lot of lots || []) {
      const code = (lot.code || "").toUpperCase();
      phonesByLot[code] = await sheetCall("phones_by_lot", { chat_id, lot_id: code });
    }

    const msg = renderLotsList("DANH SÁCH LÔ MÁY (20 lô gần nhất)", lots, phonesByLot, walletLogs);
    await sendMessage(chat_id, msg, { reply_markup: mainKeyboard() });
    return true;
  }

  if (t === "📃 Danh Sách Máy") {
    // optional. If your sheet doesn't implement phones_list_all, just show notice.
    try {
      const phones = await sheetCall("phones_list_all", { chat_id });
      if (!phones || phones.length === 0) {
        await sendMessage(chat_id, addCuteTail("📃 Danh sách máy đang trống nè~ 😙"), { reply_markup: mainKeyboard() });
        return true;
      }
      let out = `📃 <b>DANH SÁCH MÁY</b>\n`;
      for (const p of phones.slice(0, 80)) {
        const code = (p.lot_code || p.lot || "").toUpperCase();
        const note = (p.note || "").trim();
        const notePart = note ? ` ${note}` : "";
        const st = (p.status || "").toLowerCase();
        const gm = (p.game || "").toLowerCase();
        const sold = st === STATUS.SOLD ? " (đã bán)" : "";
        const res =
          gm === "hq"
            ? "HQ"
            : gm === "qr"
            ? "QR"
            : gm === "db"
            ? "DB"
            : st === STATUS.TACH
            ? "TẠCH"
            : st === STATUS.HUE
            ? "HUỀ"
            : st === STATUS.NEW
            ? "NEW"
            : st === STATUS.SOLD
            ? "SOLD"
            : st || "—";
        out += `\n• ${code}${notePart}: ${res}${sold}`;
      }
      await sendMessage(chat_id, addCuteTail(out), { reply_markup: mainKeyboard() });
      return true;
    } catch {
      await sendMessage(chat_id, addCuteTail("📃 Chưa bật sheet danh sách máy nè~ 😅"), {
        reply_markup: mainKeyboard(),
      });
      return true;
    }
  }

  if (t === "📊 Phân Tích") {
    const lots = await sheetCall("lots_list", { chat_id });
    const walletLogs = await sheetCall("wallet_logs", { chat_id });

    const phonesByLot = {};
    for (const lot of lots || []) {
      const code = (lot.code || "").toUpperCase();
      phonesByLot[code] = await sheetCall("phones_by_lot", { chat_id, lot_id: code });
    }

    const msg = renderAnalysis(lots, phonesByLot, walletLogs);
    await sendMessage(chat_id, msg, { reply_markup: rightKeyboard() });
    return true;
  }

  return false;
}

// =========================
// MAIN UPDATE HANDLER
// =========================
async function handleTextMessage(msg) {
  const chat_id = msg.chat.id;
  const text = msg.text || "";

  // menu buttons
  if (await handleMenuButtons(chat_id, text)) return;

  // flows
  if (await handleBuyFlow(chat_id, text)) return;
  if (await handleSellFlow(chat_id, text)) return;

  // lot close: ma01 hq1 tach2
  if (await handleLotClose(chat_id, text)) return;

  // fallback: hint
  await sendMessage(
    chat_id,
    addCuteTail(`Nhập sai rồi bạn iu ơi ^^\nVào ⬅️ Menu → 📘 Hướng Dẫn nha~\n(hoặc bật 🧠 Smart Parse để mình hiểu bạn hơn 😚)`),
    { reply_markup: mainKeyboard() }
  );
}

// =========================
// EXPRESS WEBHOOK
// =========================
const app = express();
app.use(express.json());

app.get("/", (_, res) => res.send("OK"));

app.post("/webhook", async (req, res) => {
  try {
    if (WEBHOOK_SECRET && req.headers["x-webhook-secret"] !== WEBHOOK_SECRET) {
      return res.status(401).send("Unauthorized");
    }
    const update = req.body;

    if (update.message && update.message.text) {
      await handleTextMessage(update.message);
    }

    res.send("OK");
  } catch (e) {
    console.error("Webhook error:", e);
    res.status(200).send("OK");
  }
});

app.listen(PORT, () => {
  console.log(`✅ Bot server running on port ${PORT}`);
});
