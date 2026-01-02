/**
 * TikTok Lite Bot — Google Apps Script (Webhook)
 * - Telegram webhook (doPost)
 * - Google Sheet DB (SpreadsheetApp)
 *
 * Script Properties required:
 *   BOT_TOKEN
 *   GOOGLE_SHEET_ID
 * Optional:
 *   ADMIN_TELEGRAM_ID
 *   TZ (default Asia/Seoul)
 *
 * Tabs required in Google Sheet:
 *   INVITES, GAME_REVENUE, CHECKIN_REWARD
 * Optional:
 *   UNDO_LOG
 */

const PROP = PropertiesService.getScriptProperties();

function cfg(key, defVal = "") {
  const v = PROP.getProperty(key);
  return (v === null || v === undefined || v === "") ? defVal : v;
}

function getTZ() {
  return cfg("TZ", "Asia/Seoul");
}

function nowISO() {
  const tz = getTZ();
  return Utilities.formatDate(new Date(), tz, "yyyy-MM-dd'T'HH:mm:ssXXX");
}

function nowDateKey() {
  const tz = getTZ();
  return Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
}

function addDaysISO(isoString, days) {
  const d = isoString ? new Date(isoString) : new Date();
  d.setDate(d.getDate() + days);
  const tz = getTZ();
  return Utilities.formatDate(d, tz, "yyyy-MM-dd'T'HH:mm:ssXXX");
}

function fmtMoney(n) {
  const x = Number(n || 0);
  return x.toLocaleString("en-US");
}

// Supports: 100k => 100000, 0.5k => 500, 120000 => 120000, 12,000 => 12000
function parseMoney(input) {
  if (!input) return null;
  const s = String(input).trim().toLowerCase().replace(/,/g, "");
  const m = s.match(/^(\d+(\.\d+)?)(k)?$/);
  if (!m) return null;
  const num = Number(m[1]);
  if (Number.isNaN(num)) return null;
  return m[3] ? Math.round(num * 1000) : Math.round(num);
}

function ss() {
  const id = cfg("GOOGLE_SHEET_ID");
  if (!id) throw new Error("Missing Script Property: GOOGLE_SHEET_ID");
  return SpreadsheetApp.openById(id);
}

function sheetByName(name) {
  const sh = ss().getSheetByName(name);
  if (!sh) throw new Error(`Missing tab: ${name}`);
  return sh;
}

function getHeaderMap(sh) {
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const map = {};
  headers.forEach((h, i) => {
    if (h) map[String(h).trim()] = i + 1; // 1-based col
  });
  return map;
}

function appendRowObj(tabName, obj) {
  const sh = sheetByName(tabName);
  const map = getHeaderMap(sh);
  const lastCol = sh.getLastColumn();
  const row = new Array(lastCol).fill("");
  Object.keys(obj).forEach(k => {
    const col = map[k];
    if (col) row[col - 1] = obj[k];
  });
  sh.appendRow(row);
}

function getAllRowsObj(tabName) {
  const sh = sheetByName(tabName);
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2) return [];
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  const values = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
  return values.map((r, idx) => {
    const o = {};
    headers.forEach((h, i) => o[h] = r[i]);
    o.__row = idx + 2; // actual sheet row
    return o;
  });
}

function updateRowObj(tabName, rowNumber, patchObj) {
  const sh = sheetByName(tabName);
  const map = getHeaderMap(sh);
  Object.keys(patchObj).forEach(k => {
    const col = map[k];
    if (col) sh.getRange(rowNumber, col).setValue(patchObj[k]);
  });
}

function logUndo(action, payload) {
  let hasUndo = true;
  try { sheetByName("UNDO_LOG"); } catch (e) { hasUndo = false; }
  if (!hasUndo) return;
  appendRowObj("UNDO_LOG", {
    timestamp: nowISO(),
    action,
    payload: JSON.stringify(payload || {})
  });
}

// ===== Telegram helpers =====
function botToken() {
  const t = cfg("BOT_TOKEN");
  if (!t) throw new Error("Missing Script Property: BOT_TOKEN");
  return t;
}

function tgUrl(method) {
  return `https://api.telegram.org/bot${botToken()}/${method}`;
}

function tgSendMessage(chatId, text) {
  const payload = {
    chat_id: chatId,
    text: text
  };
  UrlFetchApp.fetch(tgUrl("sendMessage"), {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
}

function isAdmin(fromId) {
  const admin = cfg("ADMIN_TELEGRAM_ID", "");
  return admin && String(fromId) === String(admin);
}

// ===== Sessions (for due check Q&A) =====
function sessKey(chatId) { return `SESS_${chatId}`; }

function setSession(chatId, sessObj) {
  PROP.setProperty(sessKey(chatId), JSON.stringify(sessObj));
}

function getSession(chatId) {
  const raw = PROP.getProperty(sessKey(chatId));
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { return null; }
}

function clearSession(chatId) {
  PROP.deleteProperty(sessKey(chatId));
}

// ===== Business logic =====
function addGameRevenue(chatId, game, amount, type, note = "", meta = {}) {
  appendRowObj("GAME_REVENUE", {
    timestamp: nowISO(),
    game,
    type,
    amount,
    note,
    chatId,
    name: meta.name || "",
    email: meta.email || ""
  });
  logUndo("ADD_GAME_REVENUE", { chatId, game, amount, type, note, meta });
}

function createInvite(chatId, game, name, email) {
  const invitedAt = nowISO();
  const due = addDaysISO(invitedAt, 14);

  appendRowObj("INVITES", {
    timestamp: invitedAt,
    game,
    name,
    email,
    time_invited: invitedAt,
    due_date: due,
    status: "pending",
    chatId,
    last_reminded_at: "",
    checkin_reward: "",
    completed_at: ""
  });

  logUndo("ADD_INVITE", { chatId, game, name, email, time_invited: invitedAt, due_date: due });
  return { due };
}

function findLatestPendingInvite(game, name, email) {
  const rows = getAllRowsObj("INVITES");
  const g = String(game || "").toLowerCase();

  const pending = rows.filter(r => {
    const st = String(r.status || "").toLowerCase();
    const rg = String(r.game || "").toLowerCase();
    if (st !== "pending") return false;
    if (rg !== g) return false;

    const rn = String(r.name || "").toLowerCase();
    const re = String(r.email || "").toLowerCase();
    const nameOk = name && rn === String(name).toLowerCase();
    const emailOk = email && re === String(email).toLowerCase();
    return email ? emailOk : nameOk;
  });

  pending.sort((a, b) => new Date(b.time_invited || b.timestamp) - new Date(a.time_invited || a.timestamp));
  return pending[0] || null;
}

function markInviteDoneAndAddCheckin(chatId, game, name, email, reward) {
  const target = findLatestPendingInvite(game, name, email);
  if (!target) throw new Error(`Không tìm thấy invite pending cho ${game} ${name || ""}`);

  appendRowObj("CHECKIN_REWARD", {
    timestamp: nowISO(),
    game,
    name: target.name,
    email: target.email,
    reward,
    due_date: target.due_date,
    chatId
  });

  addGameRevenue(
    chatId,
    String(game).toLowerCase(),
    reward,
    "checkin_reward",
    `checkin 14 ngày: ${target.name}`,
    { name: target.name, email: target.email }
  );

  updateRowObj("INVITES", target.__row, {
    status: "done",
    checkin_reward: reward,
    completed_at: nowISO()
  });

  logUndo("DONE_INVITE_CHECKIN", { inviteRowNumber: target.__row, chatId, game, reward });
}

function reportMonth(chatId, ym) {
  const rows = getAllRowsObj("GAME_REVENUE");
  const month = ym || nowDateKey().slice(0, 7); // YYYY-MM

  const monthRows = rows.filter(r => String(r.timestamp || "").startsWith(month));
  const byGame = {};
  monthRows.forEach(r => {
    const g = String(r.game || "unknown");
    byGame[g] = (byGame[g] || 0) + (Number(r.amount) || 0);
  });

  const total = Object.keys(byGame).reduce((a, k) => a + byGame[k], 0);
  let text = `📊 Báo cáo tháng ${month}\n`;
  text += `• Tổng thu TikTok: ${fmtMoney(total)}\n`;
  Object.keys(byGame).forEach(g => {
    text += `  - ${g}: ${fmtMoney(byGame[g])}\n`;
  });

  tgSendMessage(chatId, text);
}

function listPending(chatId) {
  const rows = getAllRowsObj("INVITES");
  const now = new Date();

  const pending = rows
    .filter(r => String(r.status || "").toLowerCase() === "pending")
    .map(r => {
      const due = new Date(r.due_date);
      const overdue = !isNaN(due.getTime()) && due.getTime() <= now.getTime();
      return { r, due, overdue };
    })
    .sort((a, b) => (a.due.getTime() || 0) - (b.due.getTime() || 0));

  if (pending.length === 0) {
    tgSendMessage(chatId, "✅ Không có invite pending.");
    return;
  }

  const tz = getTZ();
  let text = `🕒 Pending invites (${pending.length})\n`;
  pending.slice(0, 50).forEach(({ r, due, overdue }) => {
    const dueStr = isNaN(due.getTime())
      ? "invalid"
      : Utilities.formatDate(due, tz, "EEE dd/MM");
    text += `• ${overdue ? "⚠️" : "⏳"} ${r.game} - ${r.name} (${r.email}) due: ${dueStr}\n`;
  });

  tgSendMessage(chatId, text);
}

// ===== Due check (trigger) =====
function runDueCheck() {
  let rows;
  try {
    rows = getAllRowsObj("INVITES");
  } catch (e) {
    console.log("Missing INVITES:", e.message);
    return;
  }

  const tz = getTZ();
  const now = new Date();
  const todayKey = nowDateKey();

  rows.forEach(r => {
    if (String(r.status || "").toLowerCase() !== "pending") return;

    const due = new Date(r.due_date);
    if (isNaN(due.getTime())) return;

    if (due.getTime() <= now.getTime()) {
      // reminded today?
      const last = r.last_reminded_at ? new Date(r.last_reminded_at) : null;
      const remindedToday = last && !isNaN(last.getTime())
        ? Utilities.formatDate(last, tz, "yyyy-MM-dd") === todayKey
        : false;
      if (remindedToday) return;

      // Ask reward in chat
      const chatId = r.chatId;
      const game = String(r.game || "").toLowerCase();
      const gameLabel = (game === "hq") ? "Hopqua" : (game === "qr") ? "QR" : game;

      setSession(chatId, {
        pending: {
          type: "ask_checkin_reward",
          data: { game, name: r.name, email: r.email }
        }
      });

      tgSendMessage(chatId, `${gameLabel} ${r.name} = bao nhiêu? (vd: 60k)`);

      updateRowObj("INVITES", r.__row, { last_reminded_at: nowISO() });
    }
  });
}

// ===== Webhook entry =====
function doPost(e) {
  try {
    const update = JSON.parse(e.postData.contents || "{}");
    const msg = update.message;
    if (!msg) return ContentService.createTextOutput("ok");

    const chatId = msg.chat && msg.chat.id;
    const text = msg.text || "";
    const fromId = msg.from && msg.from.id;

    // follow-up session
    const sess = getSession(chatId);
    if (sess && sess.pending && sess.pending.type === "ask_checkin_reward") {
      const reward = parseMoney(text);
      if (reward == null) {
        tgSendMessage(chatId, "Không parse được tiền. Ví dụ: 60k hoặc 30000");
        return ContentService.createTextOutput("ok");
      }
      const { game, name, email } = sess.pending.data;
      markInviteDoneAndAddCheckin(chatId, game, name, email, reward);
      clearSession(chatId);
      tgSendMessage(chatId, `✅ Checkin ${game} ${name}: +${fmtMoney(reward)}`);
      return ContentService.createTextOutput("ok");
    }

    // slash commands
    if (text.startsWith("/start")) {
      tgSendMessage(
        chatId,
        "✅ TIKTOK_LITE_BOT (GAS Webhook)\n\n" +
          "Gõ nhanh:\n" +
          "• dabong 100k\n" +
          "• hopqua Khanh mail@gmail.com\n" +
          "• hopqua 200k\n" +
          "• qr Khanh mail@gmail.com\n" +
          "• qr 57k\n" +
          "• them 0.5k\n\n" +
          "Báo cáo:\n" +
          "• /baocao\n" +
          "• /pending\n"
      );
      return ContentService.createTextOutput("ok");
    }

    if (text.startsWith("/help")) {
      tgSendMessage(
        chatId,
        "📌 Lệnh:\n" +
          "GAME:\n" +
          "- dabong 100k\n" +
          "- hopqua <Name> <Email>\n" +
          "- hopqua 200k\n" +
          "- qr <Name> <Email>\n" +
          "- qr 57k\n" +
          "- them 0.5k\n\n" +
          "BÁO CÁO:\n" +
          "- /baocao\n" +
          "- /pending\n"
      );
      return ContentService.createTextOutput("ok");
    }

    if (text.startsWith("/baocao")) {
      reportMonth(chatId);
      return ContentService.createTextOutput("ok");
    }

    if (text.startsWith("/pending")) {
      listPending(chatId);
      return ContentService.createTextOutput("ok");
    }

    if (text.startsWith("/undo")) {
      tgSendMessage(chatId, "⚠️ /undo: hiện mới log UNDO_LOG. Muốn rollback thật mình sẽ làm tiếp.");
      return ContentService.createTextOutput("ok");
    }

    // free-text commands
    const parts = String(text).trim().split(/\s+/);
    if (!parts[0]) return ContentService.createTextOutput("ok");
    const cmd = parts[0].toLowerCase();

    // db / dabong
    if (cmd === "dabong" || cmd === "db") {
      const amount = parseMoney(parts[1]);
      if (amount == null) {
        tgSendMessage(chatId, "Sai cú pháp. Ví dụ: dabong 100k");
        return ContentService.createTextOutput("ok");
      }
      addGameRevenue(chatId, "db", amount, "invite_reward", "dabong invite reward");
      tgSendMessage(chatId, `✅ DB +${fmtMoney(amount)}`);
      return ContentService.createTextOutput("ok");
    }

    // hopqua / hq
    if (cmd === "hopqua" || cmd === "hq") {
      if (parts.length === 2) {
        const amount = parseMoney(parts[1]);
        if (amount == null) {
          tgSendMessage(chatId, "Sai cú pháp. Ví dụ: hopqua 200k hoặc hopqua Khanh mail@gmail.com");
          return ContentService.createTextOutput("ok");
        }
        addGameRevenue(chatId, "hq", amount, "invite_reward", "hopqua invite reward");
        tgSendMessage(chatId, `✅ HQ +${fmtMoney(amount)}`);
        return ContentService.createTextOutput("ok");
      }

      if (parts.length >= 3) {
        const name = parts[1];
        const email = parts[2];
        const { due } = createInvite(chatId, "hq", name, email);
        const dueFmt = Utilities.formatDate(new Date(due), getTZ(), "EEE dd/MM");
        tgSendMessage(chatId, `✅ Đã lưu invite HQ: ${name} (${email})\n⏰ Due: ${dueFmt} (${getTZ()})`);
        return ContentService.createTextOutput("ok");
      }

      tgSendMessage(chatId, "Sai cú pháp. Ví dụ: hopqua 200k hoặc hopqua Khanh mail@gmail.com");
      return ContentService.createTextOutput("ok");
    }

    // qr
    if (cmd === "qr") {
      if (parts.length === 2) {
        const amount = parseMoney(parts[1]);
        if (amount == null) {
          tgSendMessage(chatId, "Sai cú pháp. Ví dụ: qr 57k hoặc qr Khanh mail@gmail.com");
          return ContentService.createTextOutput("ok");
        }
        addGameRevenue(chatId, "qr", amount, "invite_reward", "qr invite reward");
        tgSendMessage(chatId, `✅ QR +${fmtMoney(amount)}`);
        return ContentService.createTextOutput("ok");
      }

      if (parts.length >= 3) {
        const name = parts[1];
        const email = parts[2];
        const { due } = createInvite(chatId, "qr", name, email);
        const dueFmt = Utilities.formatDate(new Date(due), getTZ(), "EEE dd/MM");
        tgSendMessage(chatId, `✅ Đã lưu invite QR: ${name} (${email})\n⏰ Due: ${dueFmt} (${getTZ()})`);
        return ContentService.createTextOutput("ok");
      }

      tgSendMessage(chatId, "Sai cú pháp. Ví dụ: qr 57k hoặc qr Khanh mail@gmail.com");
      return ContentService.createTextOutput("ok");
    }

    // other income
    if (cmd === "them") {
      const amount = parseMoney(parts[1]);
      if (amount == null) {
        tgSendMessage(chatId, "Sai cú pháp. Ví dụ: them 0.5k");
        return ContentService.createTextOutput("ok");
      }
      addGameRevenue(chatId, "other", amount, "other_income", "other income");
      tgSendMessage(chatId, `✅ THÊM +${fmtMoney(amount)}`);
      return ContentService.createTextOutput("ok");
    }

    // admin placeholder
    if (cmd === "chinh") {
      if (!isAdmin(fromId)) {
        tgSendMessage(chatId, "⛔ Bạn không có quyền dùng lệnh này.");
        return ContentService.createTextOutput("ok");
      }
      tgSendMessage(chatId, "Lệnh admin chưa implement đầy đủ ở bản này.");
      return ContentService.createTextOutput("ok");
    }

    tgSendMessage(chatId, "Mình không hiểu lệnh. Gõ /help để xem cú pháp.");
    return ContentService.createTextOutput("ok");
  } catch (err) {
    console.log("doPost error:", err);
    return ContentService.createTextOutput("ok");
  }
}
