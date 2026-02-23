// frontend/src/features/basis/LedgerWindow.jsx
import { useEffect, useMemo, useRef, useState } from "react";
import { sharedFetchJSON } from "../../lib/sharedFetch";

/**
 * LedgerWindow (Track 1.4 + 1.5) + Withdrawals (Track 2.3)
 *
 * Deposits backend (routers/deposits.py + schemas_deposits.py):
 * - GET    /api/deposits?needs_basis=true|false|<omit>&limit<=500
 * - POST   /api/deposits
 * - PATCH  /api/deposits/lots/{lot_id}
 * - PATCH? /api/deposits/{deposit_id}        (for edit)
 * - DELETE?/api/deposits/{deposit_id}        (for delete)
 *
 * Withdrawals backend (routers/withdrawals.py or equivalent):
 * - GET    /api/withdrawals?limit<=500
 * - POST   /api/withdrawals
 * - PATCH? /api/withdrawals/{withdrawal_id}  (for edit)
 * - DELETE?/api/withdrawals/{withdrawal_id}  (for delete)
 *
 * UI notes:
 * - Tabs persist in localStorage.
 * - "Auto" controls polling; interval and limit apply per-tab (separately).
 */

function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

function trimApiBase(apiBase) {
  const s = String(apiBase || "").trim();
  return s.replace(/\/+$/, "");
}

function lsKey(suffix) {
  return `utt:deposits_window:${suffix}`;
}

function lsGet(key) {
  try {
    return window?.localStorage?.getItem(key);
  } catch {
    return null;
  }
}

function lsSet(key, val) {
  try {
    window?.localStorage?.setItem(key, String(val));
  } catch {
    // ignore
  }
}

function readBoolLS(key, fallback) {
  const v = lsGet(key);
  if (v === null || v === undefined) return fallback;
  const s = String(v).trim().toLowerCase();
  if (s === "1" || s === "true" || s === "yes" || s === "on") return true;
  if (s === "0" || s === "false" || s === "no" || s === "off") return false;
  return fallback;
}

function readIntLS(key, fallback) {
  const v = lsGet(key);
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function readStrLS(key, fallback) {
  const v = lsGet(key);
  if (v === null || v === undefined) return fallback;
  return String(v);
}

function toNum(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function fmtQty(n) {
  const x = toNum(n);
  if (x === null) return "—";
  const s = x.toFixed(12).replace(/0+$/, "").replace(/\.$/, "");
  return s || "0";
}

function fmtUsd(n) {
  const x = toNum(n);
  if (x === null) return "—";
  return x.toFixed(2);
}

function pickStr(r, keys, fallback = "") {
  if (!r || typeof r !== "object") return fallback;
  for (const k of keys) {
    const v = r[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") return String(v);
  }
  return fallback;
}

function pickNum(r, keys) {
  if (!r || typeof r !== "object") return null;
  for (const k of keys) {
    if (r[k] !== undefined && r[k] !== null) {
      const n = toNum(r[k]);
      if (n !== null) return n;
    }
  }
  return null;
}

function pickBool(r, keys, fallback = false) {
  if (!r || typeof r !== "object") return fallback;
  for (const k of keys) {
    const v = r[k];
    if (v === undefined || v === null) continue;
    if (typeof v === "boolean") return v;
    const s = String(v).trim().toLowerCase();
    if (s === "1" || s === "true" || s === "yes" || s === "on") return true;
    if (s === "0" || s === "false" || s === "no" || s === "off") return false;
  }
  return fallback;
}

function isoOrEmpty(s) {
  const v = String(s || "").trim();
  if (!v) return "";
  return v;
}

function parseDateMs(s) {
  const v = String(s || "").trim();
  if (!v) return null;
  const ms = Date.parse(v);
  return Number.isFinite(ms) ? ms : null;
}

// -------------------- API response helpers --------------------

/**
 * Backend endpoints sometimes return an "envelope" object (e.g. {deposit:{...}} or {data:{...}}).
 * These helpers unwrap common envelopes so canonicalize*() sees the actual row object.
 */
function unwrapApiRow(raw) {
  let cur = raw;
  for (let i = 0; i < 6; i++) {
    if (!cur || typeof cur !== "object") return cur;
    const candidates = ["deposit", "withdrawal", "item", "row", "data", "result"];
    let moved = false;
    for (const k of candidates) {
      const v = cur?.[k];
      if (v && typeof v === "object" && !Array.isArray(v)) {
        cur = v;
        moved = true;
        break;
      }
    }
    if (!moved) return cur;
  }
  return cur;
}

function blankToNull(s) {
  const v = String(s ?? "").trim();
  return v ? v : null;
}


const DEFAULT_XFER_VENUES = [
  "gemini",
  "coinbase",
  "kraken",
  "robinhood",
  "dex-trade",
  "crypto.com",
  "uphold",
];

// Supports simple "venue:coinbase" (and also plain "coinbase" when it is the only token)
function parseVenueScopedQuery(rawQ, knownVenues = []) {
  const q = String(rawQ ?? "").trim();
  if (!q) return { venue: "", q: "" };

  const venues = new Set((knownVenues || []).map((v) => String(v).trim()).filter(Boolean));
  const tokens = q.split(/\s+/).filter(Boolean);

  let venue = "";
  const rest = [];

  for (const t of tokens) {
    const m = /^venue:(.+)$/i.exec(t);
    if (m && !venue) {
      venue = String(m[1] || "").trim();
      continue;
    }
    rest.push(t);
  }

  // If the user typed a single token that matches a known venue, treat it as venue-scoped.
  if (!venue && rest.length === 1) {
    const only = rest[0];
    if (venues.has(only)) {
      venue = only;
      return { venue, q: "" };
    }
  }

  return { venue, q: rest.join(" ") };
}

function normalizeVenueKey(v) {
  const s = String(v ?? "").trim();
  return s;
}

function extractVenueKeysFromPayload(payload) {
  if (!payload) return [];
  const out = [];

  const pushArr = (arr) => {
    if (!Array.isArray(arr)) return;
    for (const it of arr) {
      if (it === null || it === undefined) continue;
      if (typeof it === "string" || typeof it === "number") {
        const s = normalizeVenueKey(it);
        if (s) out.push(s);
      } else if (typeof it === "object") {
        const s = normalizeVenueKey(
          pickStr(it, ["venue", "key", "id", "name", "slug"], "")
        );
        if (s) out.push(s);
      }
    }
  };

  if (Array.isArray(payload)) {
    pushArr(payload);
  } else if (typeof payload === "object") {
    // common shapes
    pushArr(payload.venues);
    pushArr(payload.enabled_venues);
    pushArr(payload.venue_keys);
    pushArr(payload.items);
    pushArr(payload.data);
    pushArr(payload.results);
    // nested envelopes
    pushArr(payload.result?.venues);
    pushArr(payload.result?.data);
    pushArr(payload.result?.items);
    pushArr(payload.data?.venues);
    pushArr(payload.data?.items);
  }

  // dedupe (case-insensitive), preserve first occurrence
  const seen = new Set();
  const deduped = [];
  for (const s of out) {
    const k = String(s).trim().toLowerCase();
    if (!k) continue;
    if (seen.has(k)) continue;
    seen.add(k);
    deduped.push(String(s).trim());
  }

  deduped.sort((a, b) => a.localeCompare(b));
  return deduped;
}


function normalizeMethodInError(msg, method) {
  const m = String(method || "").toUpperCase();
  const s = String(msg || "");
  if (!m || !s) return s;

  // sharedFetchJSON errors sometimes prefix with "GET" even when a different method was used.
  // We also see "Deposits: GET ..." / "Withdrawals: GET ..." where GET isn't at the start.
  return s.replace(/(^|:\s*)GET\s+/i, `$1${m} `);
}

function ensureString(v) {
  return v === null || v === undefined ? "" : String(v);
}

function isoDaysAgo(days) {
  const n = Math.max(1, Math.floor(Number(days) || 1));
  const d = new Date(Date.now() - n * 86400 * 1000);
  return d.toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rawFetchJSON(url, opts = {}) {
  const method = (opts && opts.method) ? String(opts.method).toUpperCase() : "GET";
  const timeoutMs = Number.isFinite(opts.timeoutMs) ? Number(opts.timeoutMs) : 20000;

  // Strip our custom option so it doesn't get passed to fetch()
  const { timeoutMs: _ignoredTimeout, ...fetchOpts } = opts || {};

  // Support BOTH:
  // - our own timeout AbortController
  // - an external AbortSignal (Cancel button / unmount) passed via opts.signal
  const externalSignal = fetchOpts && fetchOpts.signal ? fetchOpts.signal : undefined;

  const ctrl = new AbortController();
  const startedAt = Date.now();
  let t = null;
  let abortedByTimeout = false;
  let externalAbortHandler = null;

  if (externalSignal) {
    if (externalSignal.aborted) {
      try { ctrl.abort(); } catch {}
    } else {
      externalAbortHandler = () => {
        try { ctrl.abort(); } catch {}
      };
      try {
        externalSignal.addEventListener("abort", externalAbortHandler, { once: true });
      } catch {
        // ignore; some signals may not support addEventListener
      }
    }
  }

  if (timeoutMs > 0) {
    t = setTimeout(() => {
      abortedByTimeout = true;
      try { ctrl.abort(); } catch {}
    }, timeoutMs);
  }

  // Do not forward the external signal to fetch directly; we merge into ctrl above.
  const { signal: _ignoredSignal, ...restFetchOpts } = fetchOpts || {};

  try {
    const res = await fetch(url, {
      ...restFetchOpts,
      signal: ctrl.signal,
    });

    const ct = (res.headers.get("content-type") || "").toLowerCase();
    const isJson = ct.includes("application/json");

    if (!res.ok) {
      let detail = "";
      try {
        detail = isJson ? JSON.stringify(await res.json()) : String(await res.text());
      } catch {
        // ignore parse errors
      }
      throw new Error(`${method} ${url} -> HTTP ${res.status}${detail ? `: ${detail}` : ""}`);
    }

    if (res.status === 204) return null;
    return isJson ? await res.json() : await res.text();
  } catch (err) {
    const elapsed = Date.now() - startedAt;

    // AbortController: timeout OR user cancel / unmount
    if (err && (err.name === "AbortError" || String(err).includes("AbortError"))) {
      if (abortedByTimeout) {
        const e = new Error(`${method} ${url} timed out after ${elapsed}ms (timeout=${timeoutMs}ms)`);
        e.name = "TimeoutError";
        throw e;
      }
      const e = new Error(`${method} ${url} canceled after ${elapsed}ms`);
      e.name = "AbortError";
      throw e;
    }

    // CORS / network / connection reset all look like "TypeError: Failed to fetch"
    const msg = (err && err.message) ? err.message : String(err);
    throw new Error(`${method} ${url} network error: ${msg}`);
  } finally {
    if (t) clearTimeout(t);
    if (externalSignal && externalAbortHandler) {
      try { externalSignal.removeEventListener("abort", externalAbortHandler); } catch {}
    }
  }
}



/**
 * Adds a cache-busting query param to GET URLs so we don't get stale rows from any caching layer.
 * Safe for absolute or relative URLs.
 */
function withCacheBust(url) {
  try {
    const u = url.startsWith("http") ? new URL(url) : new URL(url, window.location.origin);
    u.searchParams.set("_cb", String(Date.now()));
    return u.toString();
  } catch {
    const sep = url.includes("?") ? "&" : "?";
    return `${url}${sep}_cb=${Date.now()}`;
  }
}


// -------------------- Write helpers (avoid sharedFetchJSON for PATCH/PUT/POST) --------------------

async function writeJSONWithFallback(
  url,
  payload,
  {
    // For write operations, do NOT reuse a global "list fetch" abort signal.
    // Pass `signal` only if you explicitly want this write to be cancelable.
    signal = undefined,
    timeoutMs = 120000,
    methods = ["PATCH", "PUT", "POST"],
  } = {}
) {
  const body = JSON.stringify(payload ?? {});
  const headers = { "Content-Type": "application/json" };

  let lastErr = null;

  for (const method of methods) {
    try {
      return await rawFetchJSON(url, {
        method,
        headers,
        body,
        signal,
        timeoutMs,
      });
    } catch (e) {
      lastErr = e;
      // try next method
    }
  }

  // Fallback to sharedFetchJSON (older helper) as a last resort.
  // Do NOT pass a global abort signal here, for the same reason as above.
  const method = methods[methods.length - 1] || "POST";
  return await sharedFetchJSON(url, { method, headers, body });
}

// Same as writeJSONWithFallback, but returns { methodUsed, json } so callers can report which verb worked.
async function writeJSONWithFallbackWithMethod(
  url,
  payload,
  { signal = undefined, timeoutMs = 120000, methods = ["PATCH", "PUT", "POST"] } = {}
) {
  const body = JSON.stringify(payload ?? {});
  const headers = { "Content-Type": "application/json" };

  let lastErr = null;

  for (const method of methods) {
    try {
      const json = await rawFetchJSON(url, {
        method,
        headers,
        body,
        signal,
        timeoutMs,
      });
      return { methodUsed: method, json };
    } catch (e) {
      lastErr = e;
      // try next method
    }
  }

  // Final fallback to sharedFetchJSON (no signal on purpose, same rationale as above).
  const method = methods[methods.length - 1] || "POST";
  const json = await sharedFetchJSON(url, { method, headers, body });
  return { methodUsed: method, json };
}




// -------------------- Deposits --------------------

function canonicalizeDepositRow(raw) {
  if (!raw || typeof raw !== "object") return null;

  const id = pickStr(raw, ["id", "deposit_id", "depositId"], "");
  const venue = pickStr(raw, ["venue"], "");
  const wallet = pickStr(raw, ["wallet_id", "walletId", "wallet", "account", "account_id", "accountId"], "");
  const asset = pickStr(raw, ["asset", "ticker", "currency"], "").toUpperCase();
  const amount = pickNum(raw, ["qty", "amount", "quantity", "total"]);
  const txid = pickStr(raw, ["txid", "tx_hash", "txHash", "transaction_id", "transactionId"], "");
  const receivedAt = pickStr(raw, ["deposit_time", "received_at", "receivedAt", "created_at", "createdAt"], "");
  const acquiredAt = pickStr(raw, ["acquired_at", "acquiredAt"], "");
  const needsBasis = pickBool(raw, ["needs_basis", "needsBasis", "missing_basis", "missingBasis"], false);

  const lotId = pickStr(raw, ["lot_id", "lotId", "basis_lot_id", "basisLotId"], "");
  const basisUsd = pickNum(raw, ["total_basis_usd", "basis_usd", "basisUsd", "basis_total_usd", "basisTotalUsd"]);

  // Optional fields we may edit
  const network = pickStr(raw, ["network", "chain"], "");
  const note = pickStr(raw, ["note", "memo"], "");
  const transferWithdrawalId = pickStr(raw, ["transfer_withdrawal_id", "transferWithdrawalId"], "");

  return {
    _raw: raw,
    id,
    venue,
    wallet,
    asset,
    amount,
    txid,
    receivedAt,
    acquiredAt,
    needsBasis,
    lotId,
    basisUsd,
    network,
    note,
    transferWithdrawalId,
  };
}

async function fetchDeposits(base, { needsBasisMode, limit, walletId, venue, ttlMs = 1200 }, signal) {
  const p = new URLSearchParams();

  const lim = Math.max(1, Math.min(500, Number(limit) || 200));
  p.set("limit", String(lim));

  const wallet_id = String(walletId || "").trim();
  if (wallet_id) p.set("wallet_id", wallet_id);

  const v = String(venue || "").trim();
  if (v) p.set("venue", v);

  if (needsBasisMode === "needs") p.set("needs_basis", "true");
  else if (needsBasisMode === "has") p.set("needs_basis", "false");

  let url = `${base}/api/deposits?${p.toString()}`;
  if (ttlMs <= 0) url += `&_ts=${Date.now()}`;
  const json = await sharedFetchJSON(url, { signal, ttlMs });

  const items = Array.isArray(json)
    ? json
    : Array.isArray(json?.items)
      ? json.items
      : Array.isArray(json?.data)
        ? json.data
        : Array.isArray(json?.deposits)
          ? json.deposits
          : [];

  return items.map(canonicalizeDepositRow).filter(Boolean);
}

async function patchLot(base, lotId, payload, signal) {
  const id = String(lotId || "").trim();
  if (!id) throw new Error("Missing lot_id.");

  const url = `${base}/api/deposits/lots/${encodeURIComponent(id)}`;

  return await sharedFetchJSON(url, {
    signal,
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
    ttlMs: 0,
  });
}

async function createDeposit(base, depositDraft) {
  const url = `${trimApiBase(base)}/api/deposits`;
  // Writes should not be canceled by the window's polling/list-fetch AbortController.
  return await writeJSONWithFallback(url, depositDraft, { timeoutMs: 120000, methods: ["POST"] });
}

async function patchDeposit(base, depositId, payload, signal) {
  const id = String(depositId || "").trim();
  if (!id) throw new Error("Missing deposit id for edit.");
  const url = `${trimApiBase(base)}/api/deposits/${encodeURIComponent(id)}`;

  // Prefer PATCH, but fall back to PUT/POST if the backend doesn't expose PATCH on this route.
  return await writeJSONWithFallbackWithMethod(url, payload, {
    signal,
    timeoutMs: 120000,
    methods: ["PATCH", "PUT", "POST"],
  });
}

async function linkDepositWithdrawal(base, depositId, withdrawalId, signal) {
  const did = String(depositId || "").trim();
  const wid = String(withdrawalId || "").trim();
  if (!did || !wid) throw new Error("Missing deposit_id or withdrawal_id for transfer link.");
  const url = `${trimApiBase(base)}/api/deposits/${encodeURIComponent(did)}/link_withdrawal/${encodeURIComponent(wid)}`;
  // Backend expects a POST; body typically unused, but we send {} for compatibility.
  return await rawFetchJSON(url, {
      timeoutMs: 120000,
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
    signal,
  });
}

async function unlinkDepositWithdrawal(base, depositId, withdrawalId, signal) {
  const did = String(depositId || "").trim();
  const wid = String(withdrawalId || "").trim();
  if (!did) throw new Error("Missing depositId for unlink");
  // Best-effort bidirectional unlink. If one side fails, throw to prevent dangling pointers.
  await patchDeposit(base, did, { transfer_withdrawal_id: null }, signal);
  if (wid) {
    await patchWithdrawal(base, wid, { transfer_deposit_id: null }, signal);
  }
}

async function deleteDeposit(base, depositId, signal) {
  const id = String(depositId || "").trim();
  if (!id) throw new Error("Missing deposit id.");
  // IMPORTANT:
  // We MUST bypass sharedFetchJSON for mutating calls.
  // sharedFetchJSON is optimized for GET/list reads and may fall back to GET
  // semantics (cache keying, TTL, etc.). That would turn this into a GET,
  // which your backend correctly rejects with 405.
  const url = `${trimApiBase(base)}/api/deposits/${encodeURIComponent(id)}`;
  return await rawFetchJSON(url, {
    method: "DELETE",
    timeoutMs: 120_000,
    signal,
  });
}

// -------------------- Withdrawals --------------------

function canonicalizeWithdrawalRow(raw) {
  if (!raw || typeof raw !== "object") return null;

  const id = pickStr(raw, ["id", "withdrawal_id", "withdrawalId"], "");
  const venue = pickStr(raw, ["venue"], "");
  const wallet = pickStr(raw, ["wallet_id", "walletId", "wallet", "account", "account_id", "accountId"], "");
  const asset = pickStr(raw, ["asset", "ticker", "currency"], "").toUpperCase();
  const amount = pickNum(raw, ["qty", "amount", "quantity", "total"]);
  const txid = pickStr(raw, ["txid", "tx_hash", "txHash", "transaction_id", "transactionId"], "");
  const sentAt = pickStr(raw, ["withdraw_time", "withdrawal_time", "sent_at", "sentAt", "created_at", "createdAt"], "");
  const network = pickStr(raw, ["network", "chain"], "");
  const note = pickStr(raw, ["note", "memo"], "");
  const destination = pickStr(raw, ["destination", "to", "address"], "");

  // Optional linkage field (names may vary by your schema)
  const rawInner = raw && typeof raw.raw === "object" && raw.raw ? raw.raw : null;
  const transferDepositId = pickStr(raw, ["transfer_deposit_id", "transferDepositId", "deposit_id", "depositId"], "") || (rawInner ? pickStr(rawInner, ["transfer_deposit_id", "transferDepositId", "deposit_id", "depositId"], "") : "");

  return {
    _raw: raw,
    id,
    venue,
    wallet,
    asset,
    amount,
    txid,
    sentAt,
    network,
    destination,
    note,
    transferDepositId,
  };
}

async function fetchWithdrawals(base, { limit, walletId, venue, ttlMs = 1200 }, signal) {
  const p = new URLSearchParams();
  const lim = Math.max(1, Math.min(500, Number(limit) || 200));
  p.set("limit", String(lim));

  const wallet_id = String(walletId || "").trim();
  if (wallet_id) p.set("wallet_id", wallet_id);

  const v = String(venue || "").trim();
  if (v) p.set("venue", v);

  let url = `${base}/api/withdrawals?${p.toString()}`;
  if (ttlMs <= 0) url += `&_ts=${Date.now()}`;
  const json = await sharedFetchJSON(url, { signal, ttlMs });

  const items = Array.isArray(json)
    ? json
    : Array.isArray(json?.items)
      ? json.items
      : Array.isArray(json?.data)
        ? json.data
        : Array.isArray(json?.withdrawals)
          ? json.withdrawals
          : [];

  return items.map(canonicalizeWithdrawalRow).filter(Boolean);
}

async function createWithdrawal(base, withdrawalDraft, applyLotImpact) {
  const apply = applyLotImpact === true ? "true" : "false";
  const url = `${trimApiBase(base)}/api/withdrawals?apply_lot_impact=${apply}`;
  return await writeJSONWithFallback(url, withdrawalDraft, { timeoutMs: 120000, methods: ["POST"] });
}

async function patchWithdrawal(base, withdrawalId, payload, signal) {
  const id = String(withdrawalId || "").trim();
  if (!id) throw new Error("Missing withdrawal id for edit.");
  const url = `${trimApiBase(base)}/api/withdrawals/${encodeURIComponent(id)}`;

  // Prefer PATCH, but fall back to PUT/POST if the backend doesn't expose PATCH on this route.
  return await writeJSONWithFallbackWithMethod(url, payload, {
    signal,
    timeoutMs: 120000,
    methods: ["PATCH", "PUT", "POST"],
  });
}

async function deleteWithdrawal(base, withdrawalId, signal) {
  const id = String(withdrawalId || "").trim();
  if (!id) throw new Error("Missing withdrawal id.");
  const url = `${trimApiBase(base)}/api/withdrawals/${encodeURIComponent(id)}`;
  return await rawFetchJSON(url, {
    method: "DELETE",
    timeoutMs: 120_000,
    signal,
  });
}

// -------------------- Column model --------------------

const DEFAULT_DEP_COL_ORDER = ["asset", "venue", "wallet", "amount", "receivedAt", "transferWithdrawalId", "lotId", "basisUsd", "actions"];
const DEFAULT_WD_COL_ORDER = ["asset", "venue", "wallet", "amount", "sentAt", "destination", "txid", "transferDepositId", "actions"];

function normalizeColOrder(order, defaultOrder) {
  const want = Array.isArray(order) ? order.map(String) : [];
  const allowed = new Set(defaultOrder);
  const dedup = [];
  for (const k of want) {
    if (allowed.has(k) && !dedup.includes(k)) dedup.push(k);
  }
  for (const k of defaultOrder) {
    if (!dedup.includes(k)) dedup.push(k);
  }
  return dedup;
}


// ─────────────────────────────────────────────────────────────
// Date/time helpers (native datetime-local picker)
// ─────────────────────────────────────────────────────────────
function _toDatetimeLocal(v) {
  const s0 = v === null || v === undefined ? "" : String(v).trim();
  if (!s0) return "";
  // Normalize common ISO variants to a datetime-local compatible value: YYYY-MM-DDTHH:MM
  let s = s0.replace(" ", "T");
  if (s.endsWith("Z")) s = s.slice(0, -1);

  // Strip timezone offsets (datetime-local does not accept them)
  const tzIdx = s.search(/[+-]\d\d:\d\d$/);
  if (tzIdx !== -1) s = s.slice(0, tzIdx);

  // Drop fractional seconds
  const fracIdx = s.indexOf(".");
  if (fracIdx !== -1) s = s.slice(0, fracIdx);

  // If date-only, default midnight
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s + "T00:00";

  // Trim to minutes if we have seconds
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(s)) return s.slice(0, 16);

  // Best-effort: if already minute precision, keep it
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(s)) return s;

  // Otherwise, return as-is (still editable)
  return s;
}

function _fromDatetimeLocal(local) {
  const s = String(local || "").trim();
  if (!s) return "";
  // Accept "YYYY-MM-DDTHH:MM" (seconds optional). Treat as local time.
  // Convert to ISO UTC ("...Z") for backend consistency.
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return "";
  const [_, yy, mm, dd, HH, MM, SS] = m;
  const dt = new Date(
    Number(yy),
    Number(mm) - 1,
    Number(dd),
    Number(HH),
    Number(MM),
    Number(SS || "0"),
    0
  );
  if (!Number.isFinite(dt.getTime())) return "";
  return dt.toISOString();
}

// NOTE: This MUST live at module scope (not inside LedgerWindow).
// If defined inside the parent component, it remounts on every keystroke,
// causing the input to lose focus after each character.
function DateTimeField({
  ui,
  value,
  onChange,
  placeholder = "YYYY-MM-DDTHH:MM:SSZ",
  ariaLabel = "Datetime",
}) {
  const pickRef = useRef(null);

  const stop = (e) => {
    if (!e) return;
    e.stopPropagation?.();
  };

  const onPointerDownCapture = (e) => {
    // Prevent window drag managers from stealing focus/click.
    // We do NOT preventDefault here (that can break the native picker),
    // we only stop propagation.
    stop(e);
  };

  const onPickClick = (e) => {
    stop(e);
    try {
      const el = pickRef.current;
      if (!el) return;

      // Seed picker with current value if possible
      const seed = _toDatetimeLocal(value);
      if (seed) el.value = seed;

      // showPicker is best when available; otherwise focus+click fallback.
      if (typeof el.showPicker === "function") {
        el.showPicker();
      } else {
        // Some browsers require focus before click to open the picker.
        el.focus?.();
        // Defer click to next tick to avoid being swallowed by ancestor handlers.
        setTimeout(() => el.click?.(), 0);
      }
    } catch {
      // ignore
    }
  };

  const applyPicked = (rawLocal) => {
    const nextLocal = String(rawLocal || "").trim();
    if (!nextLocal) return;
    const iso = _fromDatetimeLocal(nextLocal);
    // If conversion fails, fall back to rawLocal.
    onChange?.(iso || nextLocal);
  };

  const onPickChange = (e) => {
    stop(e);
    applyPicked(e?.target?.value);
  };

  return (
    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
      <input
        type="text"
        value={value || ""}
        placeholder={placeholder}
        aria-label={ariaLabel}
        style={ui.input}
        autoComplete="off"
        spellCheck={false}
        // Critical: stop propagation on pointer-down capture so drag handlers don't interfere,
        // while keeping the element mounted so focus persists.
        onPointerDownCapture={onPointerDownCapture}
        onPointerDown={stop}
        onMouseDown={stop}
        onClick={(e) => {
          stop(e);
          // Explicit focus helps if a parent handler messed with focus.
          e.currentTarget?.focus?.();
        }}
        onChange={(e) => onChange?.(e.target.value)}
      />

      <div style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
  {/* Visual button (click handled by the transparent native input overlay for reliability) */}
  <button
    type="button"
    style={{ ...ui.pickBtn, pointerEvents: "none" }}
    aria-hidden="true"
    tabIndex={-1}
    title="Pick date and time"
  >
    📅
  </button>

  {/* Native picker input overlay: captures the user gesture so the picker opens reliably */}
  <input
    ref={pickRef}
    type="datetime-local"
    value={_toDatetimeLocal(value) || ""}
    aria-label="Pick date and time"
    style={{
      position: "absolute",
      inset: 0,
      width: "100%",
      height: "100%",
      opacity: 0,
      cursor: "pointer",
      border: "none",
      background: "transparent",
      padding: 0,
      margin: 0,
    }}
    onPointerDownCapture={onPointerDownCapture}
    onPointerDown={stop}
    onMouseDown={stop}
    onClick={onPickClick}
    onChange={onPickChange}
    onInput={onPickChange}
    onBlur={(e) => applyPicked(e?.target?.value)}
  />
</div>
</div>
  );
}


export default function LedgerWindow({
  apiBase,
  hideTableData = false,
  onClose,
  height = 640,

  // NEW: if true, render as an in-app popup/modal (overlay + centered panel)
  popup = false,
  popupWidth = 1240,
}) {
  // Tab selection (persisted)
  const [tab, setTab] = useState(() => {
    const v = String(readStrLS(lsKey("tab"), "deposits") || "").trim().toLowerCase();
    return v === "withdrawals" ? "withdrawals" : "deposits";
  });

    // -------------------- Ledger Reconcile / Rebuild (lots + FIFO journal) --------------------
    // Mirrors the PowerShell paging loop, but runs from the UI.
    const [syncWalletId, setSyncWalletId] = useState('default');


  // Display-only toggle: when ON, list fetch omits wallet_id so we can display wallet-address ingest rows
  // (wallet_id="wallet_address", venue="self_custody") alongside venue wallets.
  const [viewAllWallets, setViewAllWallets] = useState(() => {
    try {
      return (localStorage.getItem("utt_ledger_view_all_wallets_v1") || "").trim() === "1";
    } catch {
      return false;
    }
  });

  useEffect(() => {
    try {
      localStorage.setItem("utt_ledger_view_all_wallets_v1", viewAllWallets ? "1" : "0");
    } catch {
      // ignore
    }
  }, [viewAllWallets]);

    const [syncLimit, setSyncLimit] = useState(500);
    const [syncRunning, setSyncRunning] = useState(false);
    const [syncLines, setSyncLines] = useState([]); // progress log lines
    const [syncLastResult, setSyncLastResult] = useState(null);
    const [syncApply, setSyncApply] = useState(false); // apply changes (dry_run=false)
    const [syncCanceled, setSyncCanceled] = useState(false);
    const [syncError, setSyncError] = useState(null);
    const [rebuildOpen, setRebuildOpen] = useState(false);
    const [resetPreview, setResetPreview] = useState(null);
    const [resetConfirmText, setResetConfirmText] = useState('');
    const [resetLoading, setResetLoading] = useState(false);
    const [resetError, setResetError] = useState('');

    const [ledgerStale, setLedgerStale] = useState(false);
    const syncAbortRef = useRef(null);

    function markLedgerStale(reason = '') {
      setLedgerStale(true);
      if (reason) {
        setSyncLines((prev) => (prev?.length ? prev : []).concat([`Ledger marked stale: ${reason}`]));
      }
    }

    function cancelLedgerSync() {
      try {
        // Abort the active reconcile/rebuild paging loop (if any).
        syncAbortRef.current?.abort?.();
      } catch {}
      // UI hint; the AbortError handler in runLedgerSync will append "Canceled."
      setSyncLines((prev) => (prev?.length ? prev : []).concat(["Cancel requested..."]));
    }

    function formatSyncLine(page, r) {
      const next = r?.next_cursor ? String(r.next_cursor) : '';
      return `${String(page).padStart(4, ' ')} rows=${String(r?.rows_fetched ?? '').padStart(4, ' ')} ` +
        `created=${String(r?.created_lots ?? '').padStart(4, ' ')} consumed=${String(r?.consumed_sells ?? '').padStart(4, ' ')} ` +
        `skipped=${String(r?.skipped ?? '').padStart(4, ' ')} already=${String(r?.skipped_already_applied ?? '').padStart(4, ' ')} ` +
        `missing=${String(r?.skipped_missing_data ?? '').padStart(4, ' ')} next=${next}`;
    }

    async function runLedgerSync({
      label,
      walletId,
      mode,
      limit,
      venue,
      symbolCanon,
      sinceIso,
      cursor,
      apply,
    }) {
      const base = trimApiBase(apiBase) || "";
      const _walletId = String(walletId ?? syncWalletId ?? "default").trim() || "default";

      // IMPORTANT: do not reference undefined globals (syncMode/syncVenue/etc).
      const _mode = String(mode ?? "ALL").trim() || "ALL";
      const _limit = Math.max(1, Math.min(5000, Number(limit ?? syncLimit ?? 500) || 500));
      const _venue = (venue ?? "").trim() || undefined;
      const _symbolCanon = (symbolCanon ?? "").trim() || undefined;
      const _sinceIso = (sinceIso ?? "").trim() || undefined;

      const _dryRun = !(apply ?? syncApply);

      setSyncCanceled(false);
      setSyncError(null);

      const params = new URLSearchParams();
      params.set("wallet_id", _walletId);
      params.set("mode", _mode);
      params.set("limit", String(_limit));
      params.set("dry_run", _dryRun ? "true" : "false");
      if (_venue) params.set("venue", _venue);
      if (_symbolCanon) params.set("symbol_canon", _symbolCanon);
      if (_sinceIso) params.set("since", _sinceIso);
      if (cursor) params.set("cursor", String(cursor));

      // Abort any previous run.
      try {
        syncAbortRef.current?.abort?.();
      } catch {}
      const controller = new AbortController();
      syncAbortRef.current = controller;

      setSyncRunning(true);

      // Buffer log lines locally and flush to React periodically to avoid UI starvation.
      let lines = [`Starting: ${label || "Sync"} (dry_run=${_dryRun ? "true" : "false"})`];
      setSyncLines((lines || []).slice(-400));
      setSyncLastResult(null);

      const seenCursors = new Set();
      let nextCursor = cursor ? String(cursor) : null;
      let page = 0;

      let lastFlushMs = 0;
      const flush = async (force = false) => {
        const now =
          typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
        if (!force && now - lastFlushMs < 200) return;
        lastFlushMs = now;
        setSyncLines((lines || []).slice(-400));
        // Yield to the browser so it can paint.
        await sleep(0);
      };

      try {
        while (true) {
          const pageParams = new URLSearchParams(params);
          if (nextCursor) pageParams.set("cursor", nextCursor);
          const url = `${base}/api/ledger/sync?${pageParams.toString()}`;

          const res = await rawFetchJSON(url, { method: "POST", signal: controller.signal, timeoutMs: 600000 });
          page += 1;

          setSyncLastResult(res);

          const line =
            typeof formatSyncLine === "function"
              ? formatSyncLine(page, res)
              : `Page ${page}: rows=${Number(res?.rows_fetched || 0)} created_lots=${Number(
                  res?.created_lots || 0
                )} consumed_sells=${Number(res?.consumed_sells || 0)} skipped=${Number(
                  res?.skipped || 0
                )}`;

          lines.push(line);

          const errs = Array.isArray(res?.errors) ? res.errors : [];
          if (errs.length) {
            for (const e of errs) lines.push(`ERROR: ${String(e)}`);
          }

          // Flush at least every 5 pages, otherwise every ~200ms.
          await flush(page % 5 === 0);

          const newCursor = res?.next_cursor ? String(res.next_cursor) : null;
          if (!newCursor) {
            lines.push("Done.");
            await flush(true);
            break;
          }

          if (seenCursors.has(newCursor)) {
            lines.push("STOP: next_cursor repeated (pagination would loop).");
            await flush(true);
            break;
          }
          seenCursors.add(newCursor);
          nextCursor = newCursor;
        }

        // After a run, refresh the visible tables so you don't need a manual reload.
        // If we actually wrote derived rows (dry_run=false), the ledger is no longer stale.
        if (!_dryRun) setLedgerStale(false);

        await Promise.allSettled([
          doRefresh({ forceFresh: true }),
          doWdRefresh({ forceFresh: true }),
        ]);

        return { ok: true };
      } catch (e) {
        if (String(e?.name || "").toLowerCase() === "aborterror") {
          setSyncCanceled(true);
          lines.push("Canceled.");
          setSyncLines((lines || []).slice(-400));
          return { ok: false, canceled: true };
        }
        const msg = String(e?.message || e);
        setSyncError(msg);
        lines.push(`ERROR: ${msg}`);
        setSyncLines((lines || []).slice(-400));
        return { ok: false, error: msg };
      } finally {
        syncAbortRef.current = null;
        setSyncRunning(false);
      }
    }

  // Confirm wrappers (you requested a warning for BOTH buttons)
  async function confirmAndRunReconcile() {
    if (syncRunning) return;

    const _walletId = String(syncWalletId || "default").trim() || "default";
    const willWrite = !!syncApply;
    const msg = willWrite
      ? `Reconcile will WRITE derived FIFO rows (dry_run=false) for wallet '${_walletId}'.

Proceed?`
      : `Reconcile is PREVIEW ONLY (dry_run=true) for wallet '${_walletId}'.

Proceed?`;

    if (!window.confirm(msg)) return;

    try {
      await runLedgerSync({ label: "Reconciling", walletId: _walletId, apply: syncApply });
    } catch (e) {
      const em = String(e?.message || e);
      setSyncError(em);
      setSyncLines((prev) => (prev ?? []).concat([`ERROR: ${em}`]));
    }
  }



  async function confirmAndOpenFullRebuild() {
    const _walletId = String(syncWalletId || "default").trim() || "default";
    const msg =
      `Full Rebuild will PREVIEW how many derived rows would be cleared for wallet '${_walletId}'.
` +
      `You will still need to type RESET to actually delete + rebuild.

Proceed?`;
    if (!window.confirm(msg)) return;
    await openRebuildModal({ walletId: _walletId });
  }

  async function openRebuildModal() {
      if (syncRunning) return;
      const walletId = (syncWalletId || 'default').trim() || 'default';

      setRebuildOpen(true);
      setResetConfirmText('');
      setResetPreview(null);
      setResetError('');
      setResetLoading(true);

      try {
        const r = await writeJSONWithFallback(
          `/api/ledger/reset?wallet_id=${encodeURIComponent(walletId)}&preview=true`,
          {},
          { methods: ['POST'] }
        );
        if (!r?.ok) throw new Error(r?.error || 'reset preview failed');
        setResetPreview({ journal_rows: r.journal_rows ?? 0, lot_rows: r.lot_rows ?? 0 });
      } catch (e) {
        setResetError(e?.message || String(e));
      } finally {
        setResetLoading(false);
      }
    }

    async function confirmFullRebuild() {
      if (syncRunning) return;

      const walletId = (syncWalletId || "default").trim() || "default";
      const t = String(resetConfirmText || "").trim().toUpperCase();
      if (t !== "RESET") {
        setResetError("Type RESET to confirm.");
        return;
      }

      const ok = window.confirm(
        `FINAL CONFIRM: Delete derived tables (basis_lots + lot_journal) for wallet_id=${walletId} and rebuild FIFO?`
      );
      if (!ok) return;

      setRebuildOpen(false);
      setResetError("");

      // During the reset call, support Cancel.
      const ac = new AbortController();
      syncAbortRef.current = ac;

      setSyncCanceled(false);
      setSyncError(null);
      setSyncRunning(true);
      setSyncLines([]);
      setSyncLastResult(null);

      try {
        setSyncLines((prev) =>
          (prev ?? []).concat([`Resetting derived tables for wallet_id=${walletId} ...`])
        );

        const rr = await writeJSONWithFallback(
          `/api/ledger/reset?wallet_id=${encodeURIComponent(walletId)}&preview=false&confirm=true&confirm_text=RESET`,
          {},
          { signal: ac.signal, methods: ["POST"] }
        );
        if (!rr?.ok) throw new Error(rr?.error || "reset failed");

        setSyncLines((prev) =>
          (prev ?? []).concat([
            `Reset deleted_journal_rows=${rr.deleted_journal_rows ?? "?"} deleted_lot_rows=${rr.deleted_lot_rows ?? "?"}`,
            "Starting full rebuild (dry_run=false) ...",
          ])
        );

        // Hand off to the same paged sync runner (batched logs + yield).
        // Full rebuild ALWAYS applies (dry_run=false).
        await runLedgerSync({
          label: "Full Rebuild",
          walletId,
          mode: "ALL",
          limit: syncLimit,
          apply: true,
        });

        setLedgerStale(false);
      } catch (e) {
        if (String(e?.name || "").toLowerCase() === "aborterror" || e?.message === "ABORTED") {
          setSyncLines((prev) => (prev ?? []).concat(["Canceled."]));
        } else {
          const msg = e?.message || String(e);
          setSyncLines((prev) => (prev ?? []).concat([`FAILED: ${msg}`]));
        }
      } finally {
        // runLedgerSync will clear syncAbortRef + setSyncRunning(false) when it's the active runner.
        // If we failed before entering runLedgerSync, clear flags here.
        if (syncAbortRef.current === ac) {
          syncAbortRef.current = null;
          setSyncRunning(false);
        }
      }
    }

// -------------------- Deposits state --------------------

  const [needsBasisMode, setNeedsBasisMode] = useState(() => {
    const savedMode = String(readStrLS(lsKey("needsBasisMode"), "") || "").trim().toLowerCase();
    if (savedMode === "needs" || savedMode === "has" || savedMode === "all") return savedMode;

    const legacyBool = lsGet(lsKey("needsBasisOnly"));
    if (legacyBool !== null && legacyBool !== undefined) {
      const s = String(legacyBool).trim().toLowerCase();
      const isNeeds = s === "1" || s === "true" || s === "yes" || s === "on";
      return isNeeds ? "needs" : "has";
    }
    return "all";
  });

  const [autoRefresh, setAutoRefresh] = useState(() => readBoolLS(lsKey("autoRefresh"), true));
  const [refreshSeconds, setRefreshSeconds] = useState(() =>
    clampSeconds(readIntLS(lsKey("refreshSeconds"), 300), 300)
  );

// Transfer ingest (deposits + withdrawals) controls (Sync Transfers)
const [xferVenue, setXferVenue] = useState(() => {
  const v = String(lsGet(lsKey("xferVenue")) || "gemini").trim();
  return v || "gemini";
});
const [xferLookbackDays, setXferLookbackDays] = useState(() => {
  const raw = lsGet(lsKey("xferLookbackDays"));
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 90;
});

  const [xferMode, setXferMode] = useState(() => {
    const v = String(lsGet(lsKey("xferMode")) || "days").trim().toLowerCase();
    return v === "all" ? "all" : "days";
  });


  const [xferVenueOptions, setXferVenueOptions] = useState(() => DEFAULT_XFER_VENUES.slice());

  // Populate the Sync Transfers venue dropdown from backend if possible.
  // This makes adding new venues a backend-only change as long as the backend exposes a venues list endpoint.
  useEffect(() => {
    let alive = true;

    (async () => {
      const base = trimApiBase(apiBase) || "";

      // Try a few likely endpoints used elsewhere in UTT. First one that returns a non-empty list wins.
      const candidates = [
        `${base}/api/market/venues`,
        `${base}/api/discovery/venues`,
        `${base}/api/venues`,
        // If you later add this optional endpoint, it will work automatically:
        `${base}/api/transfers/capabilities`,
      ];

      for (const url of candidates) {
        try {
          const r = await rawFetchJSON(url, { method: "GET" });
          const venues = extractVenueKeysFromPayload(r);
          if (venues && venues.length) {
            const cur = String(xferVenue || "").trim();
            const curKey = cur.toLowerCase();
            const hasCur = cur && venues.some((v) => String(v).trim().toLowerCase() === curKey);
            const next = hasCur ? venues.slice() : venues.concat([cur]).filter(Boolean);
            if (alive) setXferVenueOptions(next);
            return;
          }
        } catch {
          // try next
        }
      }

      // Fallback: keep defaults (but ensure current selection stays present)
      const cur = String(xferVenue || "").trim();
      const curKey = cur.toLowerCase();
      const baseList = DEFAULT_XFER_VENUES.slice();
      const hasCur = cur && baseList.some((v) => String(v).trim().toLowerCase() === curKey);
      const next = hasCur ? baseList : baseList.concat([cur]).filter(Boolean);
      if (alive) setXferVenueOptions(next);
    })();

    return () => {
      alive = false;
    };
  }, [apiBase, xferVenue]);

const [xferRunning, setXferRunning] = useState(false);
const [xferMsg, setXferMsg] = useState("");
const xferCooldownUntilRef = useRef(0);
  const xferStartedAtRef = useRef(0);


  const [limit, setLimit] = useState(() => Math.max(50, Math.min(500, readIntLS(lsKey("limit"), 200))));
  const [q, setQ] = useState(() => String(lsGet(lsKey("q")) || "").trim());

  const [rows, setRows] = useState([]);
  const [err, setErr] = useState(null);
  const [loading, setLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

  const [selectedId, setSelectedId] = useState(null);

  // lot editor state
  const [editBasisUsd, setEditBasisUsd] = useState("");
  const [editAcquiredAt, setEditAcquiredAt] = useState("");
  const [editMsg, setEditMsg] = useState("");
  const [saving, setSaving] = useState(false);

  // manual deposit entry state
  const [showNew, setShowNew] = useState(() => readBoolLS(lsKey("showNew"), false));
  const [newMsg, setNewMsg] = useState("");
  const [creating, setCreating] = useState(false);

  const [newVenue, setNewVenue] = useState(() => String(lsGet(lsKey("newVenue")) || "").trim());
  const [newWalletId, setNewWalletId] = useState(() => String(lsGet(lsKey("newWalletId")) || "default").trim());
  const [newAsset, setNewAsset] = useState(() => String(lsGet(lsKey("newAsset")) || "").trim());
  const [newQty, setNewQty] = useState(() => String(lsGet(lsKey("newQty")) || "").trim());
  const [newDepositTime, setNewDepositTime] = useState(() => String(lsGet(lsKey("newDepositTime")) || "").trim());
  const [newTxid, setNewTxid] = useState(() => String(lsGet(lsKey("newTxid")) || "").trim());
  const [newNetwork, setNewNetwork] = useState(() => String(lsGet(lsKey("newNetwork")) || "").trim());
  const [newNote, setNewNote] = useState(() => String(lsGet(lsKey("newNote")) || "").trim());

  const [newBasisTotalUsd, setNewBasisTotalUsd] = useState(() => String(lsGet(lsKey("newBasisTotalUsd")) || "").trim());
  const [newBasisUsdPerCoin, setNewBasisUsdPerCoin] = useState(() => String(lsGet(lsKey("newBasisUsdPerCoin")) || "").trim());
  const [newAcquiredAtOverride, setNewAcquiredAtOverride] = useState(() => String(lsGet(lsKey("newAcquiredAtOverride")) || "").trim());

  const [newTransferWithdrawalId, setNewTransferWithdrawalId] = useState(() =>
    String(lsGet(lsKey("newTransferWithdrawalId")) || "").trim()
  );

  // optional helper input for “Calc Basis”
  const [newUsdPrice, setNewUsdPrice] = useState(() => String(lsGet(lsKey("newUsdPrice")) || "").trim());

  // left table: sorting + column order (deposits)
  const [sortKey, setSortKey] = useState(() => String(lsGet(lsKey("sortKey")) || "receivedAt"));
  const [sortDir, setSortDir] = useState(() => String(lsGet(lsKey("sortDir")) || "desc")); // asc|desc
  const [colOrder, setColOrder] = useState(() => {
    try {
      const raw = lsGet(lsKey("colOrder"));
      if (!raw) return DEFAULT_DEP_COL_ORDER;
      const parsed = JSON.parse(raw);
      return normalizeColOrder(parsed, DEFAULT_DEP_COL_ORDER);
    } catch {
      return DEFAULT_DEP_COL_ORDER;
    }
  });
  const [showColEditor, setShowColEditor] = useState(() => readBoolLS(lsKey("showColEditor"), false));

  // deposit edit state (row-level edit)
  const [depEditId, setDepEditId] = useState(null);
  const [depEditMsg, setDepEditMsg] = useState("");
  const [depEditSaving, setDepEditSaving] = useState(false);
  const [depEditQty, setDepEditQty] = useState("");
  const [depEditTime, setDepEditTime] = useState("");
  const [depEditTxid, setDepEditTxid] = useState("");
  const [depEditNetwork, setDepEditNetwork] = useState("");
  const [depEditNote, setDepEditNote] = useState("");
  const [depEditTransferWithdrawalId, setDepEditTransferWithdrawalId] = useState("");

  // -------------------- Withdrawals state --------------------

  const [wdAutoRefresh, setWdAutoRefresh] = useState(() => readBoolLS(lsKey("wdAutoRefresh"), true));
  const [wdRefreshSeconds, setWdRefreshSeconds] = useState(() =>
    clampSeconds(readIntLS(lsKey("wdRefreshSeconds"), 300), 300)
  );
  const [wdLimit, setWdLimit] = useState(() => Math.max(50, Math.min(500, readIntLS(lsKey("wdLimit"), 200))));
  const [wdQ, setWdQ] = useState(() => String(lsGet(lsKey("wdQ")) || "").trim());

  const [wdRows, setWdRows] = useState([]);
  const [wdErr, setWdErr] = useState(null);
  const [wdLoading, setWdLoading] = useState(false);
  const [wdLastUpdated, setWdLastUpdated] = useState(null);

  const [wdSelectedId, setWdSelectedId] = useState(null);

  // manual withdrawal entry state
  const [wdShowNew, setWdShowNew] = useState(() => readBoolLS(lsKey("wdShowNew"), false));
  const [wdMsg, setWdMsg] = useState("");
  const [wdCreating, setWdCreating] = useState(false);

  const [wdVenue, setWdVenue] = useState(() => String(lsGet(lsKey("wdVenue")) || "").trim());
  const [wdWalletId, setWdWalletId] = useState(() => String(lsGet(lsKey("wdWalletId")) || "default").trim());
  const [wdAsset, setWdAsset] = useState(() => String(lsGet(lsKey("wdAsset")) || "").trim());
  const [wdQty, setWdQty] = useState(() => String(lsGet(lsKey("wdQty")) || "").trim());
  const [wdWithdrawalTime, setWdWithdrawalTime] = useState(() => String(lsGet(lsKey("wdWithdrawalTime")) || "").trim());
  const [wdTxid, setWdTxid] = useState(() => String(lsGet(lsKey("wdTxid")) || "").trim());
  const [wdNetwork, setWdNetwork] = useState(() => String(lsGet(lsKey("wdNetwork")) || "").trim());
  const [wdDestination, setWdDestination] = useState(() => String(lsGet(lsKey("wdDestination")) || "").trim());
const [wdApplyLotImpact, setWdApplyLotImpact] = useState(() => readBoolLS(lsKey("wdApplyLotImpact"), false));
  const [wdNote, setWdNote] = useState(() => String(lsGet(lsKey("wdNote")) || "").trim());

  const [wdTransferDepositId, setWdTransferDepositId] = useState(() =>
    String(lsGet(lsKey("wdTransferDepositId")) || "").trim()
  );

  // withdrawals table sort + cols
  const [wdSortKey, setWdSortKey] = useState(() => String(lsGet(lsKey("wdSortKey")) || "sentAt"));
  const [wdSortDir, setWdSortDir] = useState(() => String(lsGet(lsKey("wdSortDir")) || "desc"));
  const [wdColOrder, setWdColOrder] = useState(() => {
    try {
      const raw = lsGet(lsKey("wdColOrder"));
      if (!raw) return DEFAULT_WD_COL_ORDER;
      const parsed = JSON.parse(raw);
      return normalizeColOrder(parsed, DEFAULT_WD_COL_ORDER);
    } catch {
      return DEFAULT_WD_COL_ORDER;
    }
  });
  const [wdShowColEditor, setWdShowColEditor] = useState(() => readBoolLS(lsKey("wdShowColEditor"), false));

  // withdrawal edit state (row-level edit)
  const [wdEditId, setWdEditId] = useState(null);
  const [wdEditMsg, setWdEditMsg] = useState("");
  const [wdEditSaving, setWdEditSaving] = useState(false);
  const [wdEditQty, setWdEditQty] = useState("");
  const [wdEditTime, setWdEditTime] = useState("");
  const [wdEditTxid, setWdEditTxid] = useState("");
  const [wdEditNetwork, setWdEditNetwork] = useState("");
  const [wdEditDestination, setWdEditDestination] = useState("");
  const [wdEditNote, setWdEditNote] = useState("");
  const [wdEditTransferDepositId, setWdEditTransferDepositId] = useState("");

  // -------------------- Shared refs/timers --------------------

  const abortRef = useRef(null);
  const timerRef = useRef(null);
  const refreshSeqRef = useRef(0);
  const inFlightRef = useRef(false);

  const wdAbortRef = useRef(null);
  const wdTimerRef = useRef(null);
  const wdRefreshSeqRef = useRef(0);
  const wdInFlightRef = useRef(false);

  const ui = useMemo(() => {
    const lbl = { fontSize: 12, opacity: 0.85 };

    const inputBase = {
      width: "100%",
      minWidth: 0,
      height: 36,
      padding: "8px 10px",
      borderRadius: 10,
      border: "1px solid var(--utt-border-1, #2a2a2a)",
      background: "var(--utt-surface-2, #151515)",
      color: "var(--utt-text, #eaeaea)",
      fontSize: 13,
      outline: "none",
      lineHeight: "18px",
      boxSizing: "border-box",
    };

    const btnBase = {
      padding: "7px 12px",
      borderRadius: 10,
      border: "1px solid var(--utt-border-1, #2a2a2a)",
      background: "var(--utt-surface-2, #151515)",
      color: "var(--utt-text, #eaeaea)",
      cursor: "pointer",
      fontSize: 12,
      fontWeight: 800,
      whiteSpace: "nowrap",
    };

    return {
      // popup shell
      overlay: {
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.55)",
        zIndex: 9999,
        display: "flex",
        justifyContent: "center",
        alignItems: "flex-start",
        padding: 16,
        overflow: "auto",
      },
      popupCard: {
        marginTop: 54,
        width: `min(${Number(popupWidth) || 1240}px, calc(100vw - 24px))`,
        borderRadius: 16,
        border: "1px solid rgba(255,255,255,0.10)",
        background: "rgba(14,14,14,0.98)",
        boxShadow: "0 18px 60px rgba(0,0,0,0.55)",
        overflow: "hidden",
      },

      wrap: {
        height,
        width: "100%",
        minWidth: 0,
        display: "flex",
        flexDirection: "column",
        gap: 10,
        color: "var(--utt-text, #eaeaea)",
      },

      header: {
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "space-between",
        gap: 10,
        padding: "10px 12px",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 14,
        background: "var(--utt-surface-1, #121212)",
      },
      title: { fontWeight: 950, fontSize: 14, letterSpacing: 0.2 },
      sub: { fontSize: 12, opacity: 0.8, marginTop: 2 },
      right: {
        display: "flex",
        alignItems: "center",
        gap: 8,
        flexWrap: "wrap",
        justifyContent: "flex-end",
      },

      ctl: { ...inputBase, height: 32, padding: "6px 10px", fontSize: 12 },

      btn: btnBase,
      btnDisabled: {
        ...btnBase,
        opacity: 0.6,
        cursor: "not-allowed",
        filter: "grayscale(0.2)",
      },

      pickBtn: {
        ...btnBase,
        padding: "6px 10px",
        fontSize: 12,
        lineHeight: "18px",
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        gap: 6,
        minWidth: 40,
      },
      btnPrimary: {
        ...btnBase,
        border: "1px solid rgba(120,160,255,0.55)",
        background: "rgba(120,160,255,0.14)",
        fontWeight: 900,
      },
      btnDanger: {
        ...btnBase,
        border: "1px solid rgba(255,120,120,0.55)",
        background: "rgba(255,120,120,0.12)",
        fontWeight: 900,
      },

      btnGhost: {
        ...btnBase,
        background: "transparent",
      },
      btnGhostSmall: {
        ...btnBase,
        padding: "6px 10px",
        fontSize: 11,
        background: "transparent",
      },

      dtWrap: {
        display: "grid",
        gridTemplateColumns: "1fr auto",
        gap: 8,
        alignItems: "center",
        position: "relative",
      },

      // Rendered but visually hidden. Keep it in the DOM so showPicker() works reliably.
      hiddenPicker: {
        position: "absolute",
        opacity: 0,
        width: 1,
        height: 1,
        left: 0,
        top: 0,
        pointerEvents: "none",
      },

      smallBtn: { ...btnBase, padding: "6px 10px", fontSize: 11 },
      tinyBtn: { ...btnBase, padding: "5px 8px", fontSize: 11, borderRadius: 9 },

      tabStrip: {
        display: "inline-flex",
        gap: 6,
        padding: 4,
        borderRadius: 12,
        border: "1px solid rgba(255,255,255,0.08)",
        background: "rgba(255,255,255,0.03)",
        alignItems: "center",
      },
      tabBtn: (active) => ({
        padding: "6px 10px",
        borderRadius: 10,
        border: "1px solid rgba(255,255,255,0.08)",
        background: active ? "rgba(120,160,255,0.14)" : "transparent",
        cursor: "pointer",
        fontSize: 12,
        fontWeight: 900,
        color: "var(--utt-text, #eaeaea)",
      }),

      contentRow: {
        flex: "1 1 auto",
        minHeight: 0,
        minWidth: 0,
        display: "grid",
        gridTemplateColumns: "1.35fr 1fr",
        gap: 10,
      },

      panel: {
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 14,
        background: "var(--utt-surface-1, #121212)",
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        minWidth: 0,
      },
      panelHdr: {
        padding: "10px 12px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 10,
        minWidth: 0,
      },

      mono: { fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" },
      warn: {
        border: "1px solid rgba(255,90,90,0.35)",
        background: "rgba(255,40,40,0.08)",
        color: "#ffd2d2",
        borderRadius: 12,
        padding: 10,
        margin: 10,
      },
      ok: {
        border: "1px solid rgba(90,255,140,0.25)",
        background: "rgba(70,255,120,0.06)",
        color: "#c9ffdd",
        borderRadius: 12,
        padding: 10,
        margin: 10,
      },

      // Left table: one scroll area with BOTH axes
      tableWrap: { overflowY: "auto", overflowX: "auto", flex: "1 1 auto", minHeight: 0 },
      table: { width: "max-content", minWidth: "100%", borderCollapse: "collapse", tableLayout: "fixed" },
      th: {
        textAlign: "left",
        fontSize: 12,
        padding: "8px 10px",
        background: "var(--utt-surface-2, #151515)",
        borderBottom: "1px solid var(--utt-border-1, #2a2a2a)",
        position: "sticky",
        top: 0,
        zIndex: 1,
        whiteSpace: "nowrap",
        overflow: "hidden",
        textOverflow: "ellipsis",
        userSelect: "none",
        cursor: "pointer",
      },
      thNoSort: {
        cursor: "default",
      },
      thInner: { display: "inline-flex", alignItems: "center", gap: 6, maxWidth: "100%" },
      thCaret: { fontSize: 10, opacity: 0.9 },
      td: {
        fontSize: 12,
        padding: "8px 10px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        whiteSpace: "nowrap",
        overflow: "hidden",
        textOverflow: "ellipsis",
      },
      tdR: {
        fontSize: 12,
        padding: "8px 10px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        whiteSpace: "nowrap",
        textAlign: "right",
        fontVariantNumeric: "tabular-nums",
        overflow: "hidden",
        textOverflow: "ellipsis",
      },
      rowBtn: {
        appearance: "none",
        border: "none",
        background: "transparent",
        color: "inherit",
        cursor: "pointer",
        padding: 0,
        margin: 0,
        textAlign: "left",
        fontSize: 12,
        fontWeight: 850,
        overflow: "hidden",
        textOverflow: "ellipsis",
        maxWidth: "100%",
      },
      footerTip: { padding: "10px 12px", fontSize: 11, opacity: 0.7 },

      colEditor: {
        padding: 12,
        borderTop: "1px solid rgba(255,255,255,0.06)",
        background: "rgba(255,255,255,0.02)",
      },
      colRow: {
        display: "grid",
        gridTemplateColumns: "1fr auto auto",
        gap: 8,
        alignItems: "center",
        marginBottom: 8,
      },

      // Right side (single scrollbar for manual+editor)
      rightScroll: { overflowY: "auto", overflowX: "hidden", flex: "1 1 auto", minHeight: 0 },
      section: { padding: 12, borderBottom: "1px solid rgba(255,255,255,0.06)" },
      sectionTitleRow: {
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 10,
        marginBottom: 10,
      },
      help: { fontSize: 11, opacity: 0.7, lineHeight: 1.35 },

      // Manual entry: labels ABOVE inputs, 2 columns
      fieldGrid: {
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: 10,
        alignItems: "start",
      },
      field: { display: "flex", flexDirection: "column", gap: 6, minWidth: 0 },
      fieldLabel: lbl,
      input: inputBase,
      textarea: { ...inputBase, height: 84, resize: "vertical", paddingTop: 8, paddingBottom: 8 },

      // USD Price row with calc button
      priceRow: { display: "flex", gap: 8, alignItems: "center", minWidth: 0 },
      priceInput: { ...inputBase, flex: "1 1 auto" },

      // Editor form
      form: { display: "flex", flexDirection: "column", gap: 10 },
      formRow: { display: "grid", gridTemplateColumns: "160px 1fr", gap: 10, alignItems: "center" },
      formLabel: lbl,
      editorMsg: { fontSize: 12, opacity: 0.85, minHeight: 18 },

      actionRow: { display: "inline-flex", gap: 6, alignItems: "center" },
    };
  }, [height, popupWidth]);

  const mask = (s) => (hideTableData ? "••••" : String(s ?? "—"));

  const shortId = (s) => {
    if (s == null) return "";
    const str = String(s);
    if (hideTableData) return "••••";
    if (str.length <= 10) return str;
    return `${str.slice(0, 4)}…${str.slice(-4)}`;
  };

  const linkedBadge = (id) => {
    if (!id) return "—";
    return `✓ ${shortId(id)}`;
  };


  // ESC-to-close when popup mode
  useEffect(() => {
    if (!popup) return;
    const onKey = (e) => {
      if (e.key === "Escape") onClose?.();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [popup, onClose]);

  // -------------------- Deposits derived --------------------

  const filteredRows = useMemo(() => {
    const { venue, text } = parseVenueScopedQuery(q, xferVenueOptions);
    const qq = String(text || "").trim().toLowerCase();
    const vv = String(venue || "").trim().toLowerCase();

    if (!vv && !qq) return rows;

    return (rows || []).filter((r) => {
      if (vv && String(r.venue || "").trim().toLowerCase() !== vv) return false;
      if (!qq) return true;

      const hay = [r.id, r.venue, r.wallet, r.asset, r.txid, r.receivedAt, r.acquiredAt, r.transferWithdrawalId, r.lotId]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(qq);
    });
  }, [rows, q, xferVenueOptions]);

  const sortedRows = useMemo(() => {
    const dir = String(sortDir || "desc").toLowerCase() === "asc" ? 1 : -1;
    const key = String(sortKey || "").trim();

    const getVal = (r) => {
      switch (key) {
        case "asset":
          return String(r.asset || "");
        case "venue":
          return String(r.venue || "");
        case "wallet":
          return String(r.wallet || "");
        case "amount":
          return toNum(r.amount) ?? Number.NEGATIVE_INFINITY;
        case "receivedAt":
          return parseDateMs(r.receivedAt) ?? Number.NEGATIVE_INFINITY;
        case "lotId":
          return String(r.lotId || "");
        case "basisUsd":
          return toNum(r.basisUsd) ?? Number.NEGATIVE_INFINITY;
        default:
          return String(r[key] ?? "");
      }
    };

    const isNumLike = (v) => typeof v === "number";

    const arr = [...(filteredRows || [])];
    arr.sort((a, b) => {
      const va = getVal(a);
      const vb = getVal(b);

      if (isNumLike(va) && isNumLike(vb)) {
        if (va === vb) return 0;
        return va < vb ? -1 * dir : 1 * dir;
      }

      const sa = String(va ?? "");
      const sb = String(vb ?? "");
      const cmp = sa.localeCompare(sb, undefined, { numeric: true, sensitivity: "base" });
      return cmp * dir;
    });
    return arr;
  }, [filteredRows, sortKey, sortDir]);

  const selected = useMemo(() => {
    if (!selectedId) return null;
    return (rows || []).find((r) => String(r.id) === String(selectedId)) || null;
  }, [rows, selectedId]);

  function primeEditorFromRow(r) {
    if (!r) return;
    setEditMsg("");
    setEditBasisUsd(r.basisUsd === null || r.basisUsd === undefined ? "" : String(r.basisUsd));
    setEditAcquiredAt(String(r.acquiredAt || r.receivedAt || ""));
  }

  function primeDepositEditFromRow(r) {
    if (!r) return;
    setDepEditMsg("");
    setDepEditId(String(r.id));
    setDepEditQty(r.amount === null || r.amount === undefined ? "" : String(r.amount));
    setDepEditTime(String(r.receivedAt || ""));
    setDepEditTxid(String(r.txid || ""));
    setDepEditNetwork(String(r.network || ""));
    setDepEditNote(String(r.note || ""));
    setDepEditTransferWithdrawalId(String(r.transferWithdrawalId || ""));
  }

  function clearDepositEdit() {
    setDepEditId(null);
    setDepEditMsg("");
    setDepEditSaving(false);
    setDepEditQty("");
    setDepEditTime("");
    setDepEditTxid("");
    setDepEditNetwork("");
    setDepEditNote("");
    setDepEditTransferWithdrawalId("");
  }

  function toggleSort(nextKey) {
    const k = String(nextKey || "");
    if (!k) return;
    if (k === "actions") return;

    setSortKey((prevKey) => {
      const prev = String(prevKey || "");
      if (prev !== k) {
        setSortDir("asc");
        return k;
      }
      setSortDir((prevDir) => (String(prevDir || "desc").toLowerCase() === "asc" ? "desc" : "asc"));
      return prev;
    });
  }

  function sortCaretFor(k) {
    if (k === "actions") return "";
    if (String(sortKey || "") !== String(k || "")) return "";
    return String(sortDir || "").toLowerCase() === "asc" ? "▲" : "▼";
  }

  function moveCol(idx, delta) {
    setColOrder((prev) => {
      const cur = normalizeColOrder(prev, DEFAULT_DEP_COL_ORDER);
      const i = Number(idx);
      const j = i + Number(delta);
      if (!Number.isFinite(i) || !Number.isFinite(j)) return cur;
      if (i < 0 || i >= cur.length) return cur;
      if (j < 0 || j >= cur.length) return cur;
      const next = [...cur];
      const tmp = next[i];
      next[i] = next[j];
      next[j] = tmp;
      return next;
    });
  }

  function resetCols() {
    setColOrder(DEFAULT_DEP_COL_ORDER);
  }

  async function doRefresh({ preferSelectId, forceFresh } = {}) {
    const base = trimApiBase(apiBase) || "";
if (inFlightRef.current) return;
    inFlightRef.current = true;

    const seq = ++refreshSeqRef.current;

    try {
      setLoading(true);
      setErr(null);

      try {
        abortRef.current?.abort?.();
      } catch {}

      const controller = new AbortController();
      abortRef.current = controller;

      const { venue: venueScope } = parseVenueScopedQuery(q, xferVenueOptions);
      let items;
      try {
        items = await fetchDeposits(
          base,
          { needsBasisMode, limit, walletId: (viewAllWallets ? null : syncWalletId), venue: venueScope, ttlMs: (forceFresh ? 0 : 1200) },
          controller.signal,
        );
      } catch (e) {
        // Back-compat: if backend doesn't recognize venue=, retry without it (avoid 422 regressions)
        const msg = String(e?.message || e || "");
        if (venueScope && (e?.status === 422 || msg.includes("422") || msg.includes("Unprocessable Entity"))) {
          items = await fetchDeposits(
            base,
            { needsBasisMode, limit, walletId: (viewAllWallets ? null : syncWalletId), ttlMs: (forceFresh ? 0 : 1200) },
            controller.signal,
          );
        } else {
          throw e;
        }
      }
      if (seq !== refreshSeqRef.current) return;

      setLastUpdated(new Date().toISOString());
      setRows(items);


      const want = preferSelectId ? String(preferSelectId) : null;
      if (want) {
        const hit = items.find((r) => String(r.id) === want);
        if (hit) {
              primeEditorFromRow(hit);
          return items;
        }
      }

      if (selectedId) {
        const still = items.find((r) => String(r.id) === String(selectedId));
        if (!still) setSelectedId(null);
      }

      if (!selectedId && items.length) {
        setSelectedId(String(items[0].id));
        primeEditorFromRow(items[0]);
      }

      return items;
    } catch (e) {
      if (String(e?.name || "").toLowerCase() === "aborterror") return;
      const msg = String(e?.message || "Failed to load deposits.");
      if (seq === refreshSeqRef.current) {
        setRows([]);
        setErr(msg || "Failed to load deposits.");
        setLastUpdated(null);
      }
    } finally {
      if (seq === refreshSeqRef.current) setLoading(false);
      inFlightRef.current = false;
    }
  }

  // -------------------- Withdrawals derived --------------------

  const wdFilteredRows = useMemo(() => {
    const { venue: venueScope, q: qqRaw } = parseVenueScopedQuery(wdQ, xferVenueOptions);
    const qq = String(qqRaw || "").trim().toLowerCase();

    return (wdRows || []).filter((r) => {
      if (venueScope && String(r.venue || "").toLowerCase() !== String(venueScope).toLowerCase()) return false;
      if (!qq) return true;
      const hay = [r.id, r.venue, r.wallet, r.asset, r.txid, r.sentAt, r.network, r.transferDepositId, r.note]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(qq);
    });
  }, [wdRows, wdQ, xferVenueOptions]);

  const wdSortedRows = useMemo(() => {
    const dir = String(wdSortDir || "desc").toLowerCase() === "asc" ? 1 : -1;
    const key = String(wdSortKey || "").trim();

    const getVal = (r) => {
      switch (key) {
        case "asset":
          return String(r.asset || "");
        case "venue":
          return String(r.venue || "");
        case "wallet":
          return String(r.wallet || "");
        case "amount":
          return toNum(r.amount) ?? Number.NEGATIVE_INFINITY;
        case "sentAt":
          return parseDateMs(r.sentAt) ?? Number.NEGATIVE_INFINITY;
        case "txid":
          return String(r.txid || "");
        case "transferDepositId":
          return String(r.transferDepositId || "");
        default:
          return String(r[key] ?? "");
      }
    };

    const isNumLike = (v) => typeof v === "number";

    const arr = [...(wdFilteredRows || [])];
    arr.sort((a, b) => {
      const va = getVal(a);
      const vb = getVal(b);

      if (isNumLike(va) && isNumLike(vb)) {
        if (va === vb) return 0;
        return va < vb ? -1 * dir : 1 * dir;
      }

      const sa = String(va ?? "");
      const sb = String(vb ?? "");
      const cmp = sa.localeCompare(sb, undefined, { numeric: true, sensitivity: "base" });
      return cmp * dir;
    });
    return arr;
  }, [wdFilteredRows, wdSortKey, wdSortDir]);

  const wdSelected = useMemo(() => {
    if (!wdSelectedId) return null;
    return (wdRows || []).find((r) => String(r.id) === String(wdSelectedId)) || null;
  }, [wdRows, wdSelectedId]);

  function primeWithdrawalEditFromRow(r) {
    if (!r) return;
    setWdEditMsg("");
    setWdEditId(String(r.id));
    setWdEditQty(r.amount === null || r.amount === undefined ? "" : String(r.amount));
    setWdEditTime(String(r.sentAt || ""));
    setWdEditTxid(String(r.txid || ""));
    setWdEditNetwork(String(r.network || ""));
    setWdEditDestination(String(r.destination || ""));
    setWdEditNote(String(r.note || ""));
    setWdEditTransferDepositId(String(r.transferDepositId || ""));
  }

  function clearWithdrawalEdit() {
    setWdEditId(null);
    setWdEditMsg("");
    setWdEditSaving(false);
    setWdEditQty("");
    setWdEditTime("");
    setWdEditTxid("");
    setWdEditNetwork("");
    setWdEditDestination("");
    setWdEditNote("");
    setWdEditTransferDepositId("");
  }

  function wdToggleSort(nextKey) {
    const k = String(nextKey || "");
    if (!k) return;
    if (k === "actions") return;

    setWdSortKey((prevKey) => {
      const prev = String(prevKey || "");
      if (prev !== k) {
        setWdSortDir("asc");
        return k;
      }
      setWdSortDir((prevDir) => (String(prevDir || "desc").toLowerCase() === "asc" ? "desc" : "asc"));
      return prev;
    });
  }

  function wdSortCaretFor(k) {
    if (k === "actions") return "";
    if (String(wdSortKey || "") !== String(k || "")) return "";
    return String(wdSortDir || "").toLowerCase() === "asc" ? "▲" : "▼";
  }

  function wdMoveCol(idx, delta) {
    setWdColOrder((prev) => {
      const cur = normalizeColOrder(prev, DEFAULT_WD_COL_ORDER);
      const i = Number(idx);
      const j = i + Number(delta);
      if (!Number.isFinite(i) || !Number.isFinite(j)) return cur;
      if (i < 0 || i >= cur.length) return cur;
      if (j < 0 || j >= cur.length) return cur;
      const next = [...cur];
      const tmp = next[i];
      next[i] = next[j];
      next[j] = tmp;
      return next;
    });
  }

  function wdResetCols() {
    setWdColOrder(DEFAULT_WD_COL_ORDER);
  }

  async function doWdRefresh({ preferSelectId, forceFresh } = {}) {
    const base = trimApiBase(apiBase) || "";
if (wdInFlightRef.current) return;
    wdInFlightRef.current = true;

    const seq = ++wdRefreshSeqRef.current;

    try {
      setWdLoading(true);
      setWdErr(null);

      try {
        wdAbortRef.current?.abort?.();
      } catch {}

      const controller = new AbortController();
      wdAbortRef.current = controller;

      const { venue: venueScope } = parseVenueScopedQuery(wdQ, xferVenueOptions);
      let items;
      try {
        items = await fetchWithdrawals(base, { limit: wdLimit, walletId: (viewAllWallets ? null : syncWalletId), venue: venueScope, ttlMs: (forceFresh ? 0 : 1200) }, controller.signal);
      } catch (e) {
        const msg = String(e?.message || e || "");
        const is422 = msg.includes("422") || msg.toLowerCase().includes("unprocessable");
        if (!venueScope || !is422) throw e;
        items = await fetchWithdrawals(base, { limit: wdLimit, walletId: (viewAllWallets ? null : syncWalletId), ttlMs: (forceFresh ? 0 : 1200) }, controller.signal);
      }
      if (seq !== wdRefreshSeqRef.current) return;

      setWdRows(items);
      setWdLastUpdated(new Date().toISOString());

      const want = preferSelectId ? String(preferSelectId) : null;
      if (want) {
        const hit = items.find((r) => String(r.id) === want);
        if (hit) {
          setWdSelectedId(String(hit.id));
          return items;
        }
      }

      if (wdSelectedId) {
        const still = items.find((r) => String(r.id) === String(wdSelectedId));
        if (!still) setWdSelectedId(null);
      }

      if (!wdSelectedId && items.length) setWdSelectedId(String(items[0].id));

      return items;
    } catch (e) {
      if (String(e?.name || "").toLowerCase() === "aborterror") return;
      const msg = String(e?.message || "Failed to load withdrawals.");
      if (seq === wdRefreshSeqRef.current) {
        setWdRows([]);
        setWdErr(msg || "Failed to load withdrawals.");
        setWdLastUpdated(null);
      }
    } finally {
      if (seq === wdRefreshSeqRef.current) setWdLoading(false);
      wdInFlightRef.current = false;
    }
  }

  
// -------------------- Transfers ingest (venue -> deposits + withdrawals) --------------------
async function runTransferIngest(ev) {
    ev?.preventDefault?.();

    // Defensive: if we ever get "stuck" in running=true (exception/unmount/etc), allow a retry after a grace window.
    const nowMs = Date.now();
    const startedAt = xferStartedAtRef.current || 0;
    if (xferRunning && startedAt && nowMs - startedAt < 15000) {
      // Too soon: treat as in-flight.
      setXferMsg("Transfer ingest already running…");
      return;
    }
    if (xferRunning && startedAt && nowMs - startedAt >= 15000) {
      // Stale: clear and proceed.
      setXferRunning(false);
    }

    const walletId = String(syncWalletId || "").trim();

    if (!xferVenue || !walletId) {
      setXferMsg("Pick a venue + wallet before syncing transfers.");
      return;
    }

    xferStartedAtRef.current = nowMs;
    setXferRunning(true);

    try {
      // So the user can tell the click registered even before the first network row appears.
      setXferMsg("Starting transfer ingest…");
      console.debug?.("[LedgerWindow] Sync Transfers clicked", {
        venue: xferVenue,
        wallet_id: walletId,
        mode: xferMode,
        lookback_days: xferLookbackDays,
      });

      // Optional cooldown to avoid hammering Gemini.
      const cooldownUntil = xferCooldownUntilRef.current || 0;
      if (cooldownUntil && nowMs < cooldownUntil) {
        const waitMs = cooldownUntil - nowMs;
        setXferMsg(`Please wait ${(waitMs / 1000).toFixed(1)}s before syncing again (cooldown).`);
        return;
      }

      const qsBase = new URLSearchParams({
        venue: xferVenue,
        wallet_id: walletId,
        mode: xferMode === "all" ? "all" : "days",
      });

      if (xferMode !== "all") {
        const sinceIso = isoDaysAgo(xferLookbackDays);
        qsBase.set("since", sinceIso);
      }

      const base = trimApiBase(apiBase) || "";
      const depositURL = `${base}/api/deposits/ingest?${qsBase.toString()}`;
      const withdrawalURL = `${base}/api/withdrawals/ingest?${qsBase.toString()}`;

      let depMsg = null;
      try {
        const dep = await rawFetchJSON(depositURL, { method: "POST", timeoutMs: 600000 });
        depMsg = `Deposits ingest OK: seen=${dep?.seen_deposits ?? "?"}, inserted=${dep?.inserted ?? "?"}, updated=${dep?.updated ?? "?"}`;
      } catch (e) {
        depMsg = `Deposits ingest error: ${String(e?.message || e)}`;
      }

      // Pace between the two POSTs (Gemini is sensitive to rapid bursts).
      await sleep(5600);

      let wMsg = null;
      try {
        const w = await rawFetchJSON(withdrawalURL, { method: "POST", timeoutMs: 600000 });
        wMsg = `Withdrawals ingest OK: seen=${w?.seen_withdrawals ?? "?"}, inserted=${w?.inserted ?? "?"}, updated=${w?.updated ?? "?"}`;
      } catch (e) {
        wMsg = `Withdrawals ingest error: ${String(e?.message || e)}`;
      }

      setXferMsg(`${depMsg} • ${wMsg}`);

      // Give Gemini a small cooldown after a run.
      xferCooldownUntilRef.current = Date.now() + 3500;

      // Refresh both lists (don’t let one refresh failure suppress the other).
    await Promise.allSettled([
      doRefresh({ forceFresh: true }),
      doWdRefresh({ forceFresh: true }),
    ]);
    } finally {
      xferStartedAtRef.current = 0;
      setXferRunning(false);
    }
  }

// -------------------- Persist settings --------------------

// Transfer ingest persistence
useEffect(() => {
  lsSet(lsKey("xferVenue"), xferVenue);
}, [xferVenue]);

useEffect(() => {
  const n = Math.max(1, Math.floor(Number(xferLookbackDays) || 90));
  lsSet(lsKey("xferLookbackDays"), String(n));
}, [xferLookbackDays]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    lsSet(lsKey("tab"), tab);
  }, [tab]);

  // Deposits persistence
  useEffect(() => {
    if (typeof window === "undefined") return;
    lsSet(lsKey("needsBasisMode"), String(needsBasisMode));
    try {
      window?.localStorage?.removeItem?.(lsKey("needsBasisOnly"));
    } catch {}
  }, [needsBasisMode]);

  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("autoRefresh"), autoRefresh ? "1" : "0"); }, [autoRefresh]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("refreshSeconds"), String(clampSeconds(refreshSeconds, 300))); }, [refreshSeconds]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("limit"), String(Math.max(50, Math.min(500, Number(limit) || 200)))); }, [limit]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("q"), String(q || "")); }, [q]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("showNew"), showNew ? "1" : "0"); }, [showNew]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdDestination"), wdDestination); }, [wdDestination]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdApplyLotImpact"), wdApplyLotImpact ? "1" : "0"); }, [wdApplyLotImpact]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("sortKey"), String(sortKey || "")); }, [sortKey]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("sortDir"), String(sortDir || "")); }, [sortDir]);
  useEffect(() => {
    if (typeof window === "undefined") return;
    try { lsSet(lsKey("colOrder"), JSON.stringify(normalizeColOrder(colOrder, DEFAULT_DEP_COL_ORDER))); } catch {}
  }, [colOrder]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("showColEditor"), showColEditor ? "1" : "0"); }, [showColEditor]);

  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newVenue"), newVenue); }, [newVenue]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newWalletId"), newWalletId); }, [newWalletId]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newAsset"), newAsset); }, [newAsset]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newQty"), newQty); }, [newQty]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newDepositTime"), newDepositTime); }, [newDepositTime]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newTxid"), newTxid); }, [newTxid]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newNetwork"), newNetwork); }, [newNetwork]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newNote"), newNote); }, [newNote]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newBasisTotalUsd"), newBasisTotalUsd); }, [newBasisTotalUsd]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newBasisUsdPerCoin"), newBasisUsdPerCoin); }, [newBasisUsdPerCoin]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newAcquiredAtOverride"), newAcquiredAtOverride); }, [newAcquiredAtOverride]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newTransferWithdrawalId"), newTransferWithdrawalId); }, [newTransferWithdrawalId]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("newUsdPrice"), newUsdPrice); }, [newUsdPrice]);

  // Withdrawals persistence
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdAutoRefresh"), wdAutoRefresh ? "1" : "0"); }, [wdAutoRefresh]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdRefreshSeconds"), String(clampSeconds(wdRefreshSeconds, 300))); }, [wdRefreshSeconds]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdLimit"), String(Math.max(50, Math.min(500, Number(wdLimit) || 200)))); }, [wdLimit]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdQ"), String(wdQ || "")); }, [wdQ]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdShowNew"), wdShowNew ? "1" : "0"); }, [wdShowNew]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdSortKey"), String(wdSortKey || "")); }, [wdSortKey]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdSortDir"), String(wdSortDir || "")); }, [wdSortDir]);
  useEffect(() => {
    if (typeof window === "undefined") return;
    try { lsSet(lsKey("wdColOrder"), JSON.stringify(normalizeColOrder(wdColOrder, DEFAULT_WD_COL_ORDER))); } catch {}
  }, [wdColOrder]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdShowColEditor"), wdShowColEditor ? "1" : "0"); }, [wdShowColEditor]);

  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdVenue"), wdVenue); }, [wdVenue]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdWalletId"), wdWalletId); }, [wdWalletId]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdAsset"), wdAsset); }, [wdAsset]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdQty"), wdQty); }, [wdQty]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdWithdrawalTime"), wdWithdrawalTime); }, [wdWithdrawalTime]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdTxid"), wdTxid); }, [wdTxid]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdNetwork"), wdNetwork); }, [wdNetwork]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdDestination"), wdDestination); }, [wdDestination]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdNote"), wdNote); }, [wdNote]);
  useEffect(() => { if (typeof window !== "undefined") lsSet(lsKey("wdTransferDepositId"), wdTransferDepositId); }, [wdTransferDepositId]);

  // -------------------- Refresh triggers + loops (tab-aware) --------------------

  // Initial refresh when key deps change (per-tab)
  useEffect(() => {
    if (tab !== "deposits") return;
    doRefresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, apiBase, needsBasisMode, limit, viewAllWallets, syncWalletId]);

  useEffect(() => {
    if (tab !== "withdrawals") return;
    doWdRefresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, apiBase, wdLimit, viewAllWallets, syncWalletId]);

  // Auto-refresh loop (deposits)
  useEffect(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    if (tab !== "deposits") return;
    if (!autoRefresh) return;

    let canceled = false;

    const loop = async () => {
      if (canceled) return;
      const ms = clampSeconds(refreshSeconds, 300) * 1000;
      await doRefresh({ forceFresh: true });
      if (canceled) return;
      timerRef.current = setTimeout(loop, ms);
    };

    const jitterMs = Math.floor(Math.random() * 800);
    timerRef.current = setTimeout(loop, jitterMs);

    return () => {
      canceled = true;
      if (timerRef.current) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
      try { abortRef.current?.abort?.(); } catch {}
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, autoRefresh, refreshSeconds, apiBase, needsBasisMode, limit]);

  // Auto-refresh loop (withdrawals)
  useEffect(() => {
    if (wdTimerRef.current) {
      clearTimeout(wdTimerRef.current);
      wdTimerRef.current = null;
    }
    if (tab !== "withdrawals") return;
    if (!wdAutoRefresh) return;

    let canceled = false;

    const loop = async () => {
      if (canceled) return;
      const ms = clampSeconds(wdRefreshSeconds, 300) * 1000;
      await doWdRefresh({ forceFresh: true });
      if (canceled) return;
      wdTimerRef.current = setTimeout(loop, ms);
    };

    const jitterMs = Math.floor(Math.random() * 800);
    wdTimerRef.current = setTimeout(loop, jitterMs);

    return () => {
      canceled = true;
      if (wdTimerRef.current) {
        clearTimeout(wdTimerRef.current);
        wdTimerRef.current = null;
      }
      try { wdAbortRef.current?.abort?.(); } catch {}
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, wdAutoRefresh, wdRefreshSeconds, apiBase, wdLimit]);

  // -------------------- Deposits actions --------------------

  async function onSaveBasis() {
    const base = trimApiBase(apiBase) || "";
if (!selected) { setEditMsg("No deposit selected."); return; }
    if (!selected.lotId) { setEditMsg("Selected deposit has no lot_id to edit."); return; }

    const totalBasis = toNum(editBasisUsd);
    if (totalBasis === null || totalBasis < 0) { setEditMsg("Enter a valid total_basis_usd (>= 0)."); return; }

    const acquiredAt = isoOrEmpty(editAcquiredAt);
    const payload = { total_basis_usd: totalBasis };
    if (acquiredAt) payload.acquired_at = acquiredAt;

    setSaving(true);
    setEditMsg("");

    try {
      const controller = new AbortController();
      await patchLot(base, selected.lotId, payload, controller.signal);
      await doRefresh({ preferSelectId: selected.id });
      setEditMsg("Saved.");
    } catch (e) {
      setEditMsg(String(e?.message || "Failed to save lot basis."));
    } finally {
      setSaving(false);
    }
  }

    async function onSaveDepositEdit() {
    const base = trimApiBase(apiBase) || "";
const qtyN = toNum(depEditQty);
    if (qtyN === null || qtyN <= 0) { setDepEditMsg("Qty must be a number > 0."); return; }

    const orig = (rows || []).find((r) => String(r.id) === String(depEditId)) || null;

    const deposit_time = isoOrEmpty(depEditTime);
    const txid = String(depEditTxid || "").trim();
    const network = String(depEditNetwork || "").trim();
    const note = String(depEditNote || "").trim();

    const newTransferWithdrawalId = String(depEditTransferWithdrawalId || "").trim();
    const origTransferWithdrawalId = String(orig?.transferWithdrawalId || "").trim();

    // Primary edit payload: fields the deposits PATCH/PUT/POST endpoint is known to accept.
    // Transfer-link is handled via the dedicated link endpoint (see below).
    const payload = {
      qty: qtyN,
      txid: blankToNull(txid),
      network: blankToNull(network),
      note, // allow empty string
    };

    // Allow clearing time explicitly (only send null if we are clearing an existing value).
    if (deposit_time) payload.deposit_time = deposit_time;
    else if (orig?.receivedAt) payload.deposit_time = null;

    setDepEditSaving(true);
    setDepEditMsg("");

    try {
      const controller = new AbortController();

      // 1) Save the editable fields.
      const { methodUsed, json: res } = await patchDeposit(base, depEditId, payload, controller.signal);
      const mu = methodUsed || "PATCH";
      let updated = canonicalizeDepositRow(unwrapApiRow(res));

      // If write doesn't echo a usable row, fall back to an API-like object derived from the edit payload.
      if (!updated || !updated.id) {
        const apiLike = {
          id: depEditId,
          venue: orig?.venue,
          wallet_id: orig?.wallet,
          asset: orig?.asset,
          qty: payload.qty,
          txid: payload.txid,
          network: payload.network,
          note: payload.note,
          deposit_time: payload.deposit_time !== undefined ? payload.deposit_time : (orig?.receivedAt || null),
        };
        updated = canonicalizeDepositRow(apiLike);
      }

      // Optimistically merge so UI doesn't wait on refresh.
      setRows((prev) => prev.map((r) => (r.id === depEditId ? { ...r, ...updated } : r)));
      setSelectedId(depEditId);
      primeDepositEditFromRow(updated);

      // 2) Persist transfer link via dedicated endpoint (deposit ↔ withdrawal).
      // Deposits PATCH may ignore transfer_withdrawal_id (schema filtering), so the link endpoint is the source of truth.
      let linkMsg = "";
      let linkErr = "";
      const linkChanged = newTransferWithdrawalId !== origTransferWithdrawalId;
      if (linkChanged) {
        if (newTransferWithdrawalId) linkMsg = " + linked";
        else if (origTransferWithdrawalId) linkMsg = " + unlinked";

        if (newTransferWithdrawalId) {
          try {
            await linkDepositWithdrawal(base, depEditId, newTransferWithdrawalId, controller.signal);
          } catch (le) {
            linkErr = String(le?.message || le || "Link failed.");
          }
        } else if (origTransferWithdrawalId) {
          // Best-effort unlink: no dedicated unlink route today, so attempt to clear via PATCH.
          // If backend ignores it, refresh validation below will surface that.
          try {
            await patchDeposit(base, depEditId, { transfer_withdrawal_id: null }, controller.signal);
          } catch (_) {}
        }
      }

      // 3) Force fresh list refresh (source of truth).
      const freshItems = await doRefresh({ preferSelectId: depEditId, forceFresh: true });
      const freshRow = (freshItems || []).find((r) => String(r.id) === String(depEditId)) || null;

      const freshNote = String(freshRow?.note ?? "");
      const freshLink = String(freshRow?.transferWithdrawalId ?? "");

      // Validate: note must match, and if we attempted link it must now match.
      const noteOk = !freshRow ? true : freshNote === note;
      const linkOk = !linkChanged ? true : (freshLink === (newTransferWithdrawalId || ""));

      if (freshRow && noteOk && linkOk) {
        setDepEditMsg(`Saved.${linkMsg}${linkErr ? ` (link call error: ${linkErr})` : ""}`);
      } else if (freshRow && !noteOk) {
        setDepEditMsg(`Save returned OK (${mu}), but refresh shows note="${freshNote}". Backend did not apply note edit.`);
      } else if (freshRow && !linkOk) {
        setDepEditMsg(`Saved. (${mu}) but transfer link did not persist (wanted="${newTransferWithdrawalId}" got="${freshLink}").${linkErr ? ` (link call error: ${linkErr})` : ""}`);
      } else {
        setDepEditMsg(`Saved.${linkMsg}${linkErr ? ` (link call error: ${linkErr})` : ""}`);
      }
    markLedgerStale("deposit edited");

    } catch (e) {
      setDepEditMsg(normalizeMethodInError(String(e?.message || "Failed to edit deposit."), "PATCH"));
    } finally {
      setDepEditSaving(false);
    }
  }

async function onUnlinkDepositRow(depRow) {
    const base = trimApiBase(apiBase) || "";
    const rid = String(depRow?.id || "").trim();
    const wid = String(depRow?.transferWithdrawalId || "").trim();
    if (!rid || !wid) return;

    const ok = window.confirm(
      `Unlink deposit ${rid} from withdrawal ${wid}?\n\nThis will clear the transfer link on BOTH sides.`
    );
    if (!ok) return;

    try {
      await unlinkDepositWithdrawal(base, rid, wid);
      // refresh + keep UI consistent
      await refreshDeposits();
      await refreshWithdrawals();
    } catch (e) {
      setErr(String(e?.message || e || "Unlink failed"));
    }
  }

  async function onDeleteDepositRow(rowOrId) {
    const base = trimApiBase(apiBase) || "";
    const row = typeof rowOrId === "object" && rowOrId ? rowOrId : null;
    const rid = String(row?.id || rowOrId || "").trim();
    if (!rid) return;

    const wid = String(row?.transferWithdrawalId || "").trim();
    const msg = wid
      ? `Delete deposit ${rid}?\n\nThis deposit is linked to withdrawal ${wid}.\nWe will UNLINK both sides first.\n\nIf unlink fails, delete will be aborted to prevent orphan links.`
      : `Delete deposit ${rid}?\n\nThis cannot be undone.`;

    const ok = window.confirm(msg);
    if (!ok) return;

    if (wid) {
      try {
        await unlinkDepositWithdrawal(base, rid, wid);
      } catch (e) {
        setErr(
          `Unlink failed — aborting delete to prevent orphans. (${String(
            e?.message || e || "error"
          )})`
        );
        return;
      }
    }

    try {
      await deleteDeposit(base, rid);
      await refreshDeposits();
      // withdrawals only need refresh if we unlinked
      if (wid) await refreshWithdrawals();
    } catch (e) {
      setErr(String(e?.message || e || "Delete failed"));
    }
  }

function clearNewForm({ keepVenueWallet = true } = {}) {
    setNewMsg("");
    setNewAsset("");
    setNewQty("");
    setNewDepositTime("");
    setNewTxid("");
    setNewNetwork("");
    setNewNote("");
    setNewBasisTotalUsd("");
    setNewBasisUsdPerCoin("");
    setNewAcquiredAtOverride("");
    setNewTransferWithdrawalId("");
    setNewUsdPrice("");
    if (!keepVenueWallet) {
      setNewVenue("");
      setNewWalletId("default");
    }
  }

  function onCalcBasisFromPrice() {
    setNewMsg("");
    const qtyN = toNum(newQty);
    const pxN = toNum(newUsdPrice);
    if (qtyN === null || qtyN <= 0) { setNewMsg("Calc Basis: enter a valid Qty first."); return; }
    if (pxN === null || pxN <= 0) { setNewMsg("Calc Basis: enter a valid USD Price first."); return; }
    const total = qtyN * pxN;
    setNewBasisTotalUsd(String(total));
    setNewBasisUsdPerCoin("");
    setNewMsg(`Basis set to qty * price = ${total.toFixed(2)} USD.`);
  }

  async function onCreateDeposit() {
    const base = trimApiBase(apiBase) || "";

    // Validate required fields from the Manual Deposit Entry form.
    setNewMsg("");

    const qty = toNum(newQty);
    if (qty === null || qty <= 0) { setNewMsg("Qty is required."); return; }
    if (!String(newVenue || "").trim()) { setNewMsg("Venue is required."); return; }
    if (!String(newWalletId || "").trim()) { setNewMsg("Wallet ID is required."); return; }
    if (!String(newAsset || "").trim()) { setNewMsg("Asset is required."); return; }
    if (!String(newDepositTime || "").trim()) { setNewMsg("Deposit time is required."); return; }

    const basisTotalUsd = toNum(newBasisTotalUsd);
    const basisUsdPerCoin = toNum(newBasisUsdPerCoin);

    // If both basis fields are supplied, prefer total (backend can ignore per-coin).
    const payload = {
      venue: String(newVenue).trim(),
      wallet_id: String(newWalletId).trim(),
      asset: String(newAsset).trim(),
      qty,
      deposit_time: String(newDepositTime).trim(),
      txid: blankToNull(newTxid),
      network: blankToNull(newNetwork),
      note: blankToNull(newNote),
    };

    if (String(newAcquiredAtOverride || "").trim()) {
      payload.acquired_at = String(newAcquiredAtOverride).trim();
    }

    if (basisTotalUsd !== null && basisTotalUsd >= 0) {
      payload.basis_total_usd = basisTotalUsd;
    } else if (basisUsdPerCoin !== null && basisUsdPerCoin >= 0) {
      payload.basis_usd_per_coin = basisUsdPerCoin;
    }

    // Optional: allow linking on create if backend accepts it (otherwise it will be ignored).
    if (String(newTransferWithdrawalId || "").trim()) {
      payload.transfer_withdrawal_id = String(newTransferWithdrawalId).trim();
    }

    setCreating(true);

    try {
      const res = await createDeposit(base, payload);
      const created = canonicalizeDepositRow(unwrapApiRow(res)) || canonicalizeDepositRow(res);

      if (created && created.id) {
        // Make the new row visible immediately.
        setRows((prev) => {
          const cur = Array.isArray(prev) ? prev : [];
          const idx = cur.findIndex((r) => r && String(r.id) === String(created.id));
          if (idx >= 0) {
            const next = cur.slice();
            next[idx] = { ...next[idx], ...created };
            return next;
          }
          return [created, ...cur];
        });

        setSelectedId(String(created.id));
        primeEditorFromRow(created);
      }

      setNewMsg("Created.");

      // Best-effort refresh (force fresh; may be skipped if one is in-flight).
      void doRefresh({ preferSelectId: created?.id, forceFresh: true });
    markLedgerStale("deposit created");

    } catch (e) {
      // Surface the error in the UI.
      setNewMsg(String(e?.message || e || "Create failed."));
    } finally {
      setCreating(false);
    }
  }

  // -------------------- Withdrawals actions --------------------

  function wdClearNewForm({ keepVenueWallet = true } = {}) {
    setWdMsg("");
    setWdAsset("");
    setWdQty("");
    setWdWithdrawalTime("");
    setWdTxid("");
    setWdNetwork("");
    setWdDestination("");
    setWdApplyLotImpact(false);
    setWdNote("");
    setWdTransferDepositId("");
    if (!keepVenueWallet) {
      setWdVenue("");
      setWdWalletId("default");
    }
  }

  async function onCreateWithdrawal() {
    const base = trimApiBase(apiBase) || "";
const qty = toNum(wdQty);
    if (qty === null || qty <= 0) { setWdMsg("Qty is required."); return; }
    if (!wdVenue) { setWdMsg("Venue is required."); return; }
    if (!wdWalletId) { setWdMsg("Wallet ID is required."); return; }
    if (!wdAsset) { setWdMsg("Asset is required."); return; }
    if (!wdWithdrawalTime) { setWdMsg("Withdrawal time is required."); return; }

    const payload = {
      venue: String(wdVenue).trim(),
      wallet_id: String(wdWalletId).trim(),
      asset: String(wdAsset).trim(),
      qty,
      withdraw_time: wdWithdrawalTime,
      txid: wdTxid ? String(wdTxid).trim() : null,
      network: wdNetwork ? String(wdNetwork).trim() : null,
      destination: wdDestination ? String(wdDestination) : null,
      transfer_deposit_id: blankToNull(ensureString(wdTransferDepositId)),
      note: wdNote ? String(wdNote) : null,
    };

    setWdCreating(true);
    setWdMsg("");
    try {
      const created = await createWithdrawal(base, payload, wdApplyLotImpact);

      // Make the new row visible immediately, even if an auto-refresh is in-flight.
      if (created && created.id) {
        setWdRows((prev) => {
          const idx = (prev || []).findIndex((r) => r && r.id === created.id);
          if (idx >= 0) {
            const next = prev.slice();
            next[idx] = created;
            return next;
          }
          return [created, ...(prev || [])];
        });
        setSelectedId(created.id);
      }

      setWdMsg("Created.");
      // Best-effort refresh (may be skipped if a refresh is already in-flight).
      void doWdRefresh(created?.id);
    markLedgerStale("withdrawal created");

    } catch (e) {
      setWdMsg(String(e?.message || e || "Create failed"));
    } finally {
      setWdCreating(false);
    }
}

    async function onSaveWithdrawalEdit() {
    const base = trimApiBase(apiBase) || "";
const qtyN = toNum(wdEditQty);
    if (qtyN === null || qtyN <= 0) { setWdEditMsg("Qty must be a number > 0."); return; }

    const orig = (wdRows || []).find((r) => String(r.id) === String(wdEditId)) || null;

    const withdraw_time = isoOrEmpty(wdEditTime);
    const txid = String(wdEditTxid || "").trim();
    const network = String(wdEditNetwork || "").trim();
    const destination = String(wdEditDestination || "").trim();
    const note = String(wdEditNote || "").trim();

    const newTransferDepositId = String(wdEditTransferDepositId || "").trim();
    const origTransferDepositId = String(orig?.transferDepositId || "").trim();

    // Primary edit payload: fields the withdrawals PATCH endpoint is known to accept.
    // Transfer-link is handled via the dedicated deposits link endpoint (see below).
    const payload = {
      qty: qtyN,
      txid: blankToNull(txid),
      network: blankToNull(network),
      transfer_deposit_id: blankToNull(newTransferDepositId),
      destination, // allow empty string
      note, // allow empty string
    };

    if (withdraw_time) payload.withdraw_time = withdraw_time;
    else if (orig?.sentAt) payload.withdraw_time = null;

    setWdEditSaving(true);
    setWdEditMsg("");

    try {
      const controller = new AbortController();

      const { methodUsed, json: res } = await patchWithdrawal(base, wdEditId, payload, controller.signal);
      const mu = methodUsed || "PATCH";
      let updated = canonicalizeWithdrawalRow(unwrapApiRow(res));

      if (!updated || !updated.id) {
        const apiLike = {
          id: wdEditId,
          venue: orig?.venue,
          wallet_id: orig?.wallet,
          asset: orig?.asset,
          qty: payload.qty,
          txid: payload.txid,
          network: payload.network,
          destination: payload.destination,
          note: payload.note,
      transfer_deposit_id: blankToNull(newTransferDepositId),
          withdraw_time: payload.withdraw_time !== undefined ? payload.withdraw_time : (orig?.sentAt || null),
        };
        updated = canonicalizeWithdrawalRow(apiLike);
      }

      setWdRows((prev) => prev.map((r) => (r.id === wdEditId ? { ...r, ...updated } : r)));
      setWdSelectedId(wdEditId);
      primeWithdrawalEditFromRow(updated);

      // If user supplied a transfer deposit id and it changed, link using:
      // Transfer link fields are persisted via PATCH (no lot-impact requirement)
      let linkMsg = "";
      const linkChanged = newTransferDepositId !== origTransferDepositId;
      if (linkChanged) {
        if (newTransferDepositId) linkMsg = " + linked";
        else if (origTransferDepositId) linkMsg = " + unlinked";
      }

      const freshItems = await doWdRefresh({ preferSelectId: wdEditId, forceFresh: true });
      const freshRow = (freshItems || []).find((r) => String(r.id) === String(wdEditId)) || null;

      const freshNote = String(freshRow?.note ?? "");
      const freshLink = String(freshRow?.transferDepositId ?? "");

      const noteOk = !freshRow ? true : freshNote === note;
      const linkOk = !linkChanged ? true : (freshLink === (newTransferDepositId || ""));

      if (freshRow && noteOk && linkOk) {
        setWdEditMsg(`Saved.${linkMsg}`);
      } else if (freshRow && !noteOk) {
        setWdEditMsg(`Save returned OK (${mu}), but refresh shows note="${freshNote}". Backend did not apply note edit.`);
      } else if (freshRow && !linkOk) {
        setWdEditMsg(`Saved. (${mu}) but transfer link did not persist (wanted="${newTransferDepositId}" got="${freshLink}").`);
      } else {
        setWdEditMsg(`Saved.${linkMsg}`);
      }
    markLedgerStale("withdrawal edited");

    } catch (e) {
      setWdEditMsg(normalizeMethodInError(String(e?.message || "Failed to edit withdrawal."), "PATCH"));
    } finally {
      setWdEditSaving(false);
    }
  }

async function onUnlinkWithdrawalRow(wdwRow) {
    const base = trimApiBase(apiBase) || "";
    const rid = String(wdwRow?.id || "").trim();
    const did = String(wdwRow?.transferDepositId || "").trim();
    if (!rid || !did) return;

    const ok = window.confirm(
      `Unlink withdrawal ${rid} from deposit ${did}?\n\nThis will clear the transfer link on BOTH sides.`
    );
    if (!ok) return;

    try {
      // bidirectional unlink (deposit + withdrawal)
      await unlinkDepositWithdrawal(base, did, rid);
      await refreshWithdrawals();
      await refreshDeposits();
    } catch (e) {
      setErr(String(e?.message || e || "Unlink failed"));
    }
  }

  async function onDeleteWithdrawalRow(rowOrId) {
    const base = trimApiBase(apiBase) || "";
    const row = typeof rowOrId === "object" && rowOrId ? rowOrId : null;
    const rid = String(row?.id || rowOrId || "").trim();
    if (!rid) return;

    const did = String(row?.transferDepositId || "").trim();
    const msg = did
      ? `Delete withdrawal ${rid}?\n\nThis withdrawal is linked to deposit ${did}.\nWe will UNLINK both sides first.\n\nIf unlink fails, delete will be aborted to prevent orphan links.`
      : `Delete withdrawal ${rid}?\n\nThis cannot be undone.`;

    const ok = window.confirm(msg);
    if (!ok) return;

    if (did) {
      try {
        await unlinkDepositWithdrawal(base, did, rid);
      } catch (e) {
        setErr(
          `Unlink failed — aborting delete to prevent orphans. (${String(
            e?.message || e || "error"
          )})`
        );
        return;
      }
    }

    try {
      await deleteWithdrawal(base, rid);
      await refreshWithdrawals();
      if (did) await refreshDeposits();
    } catch (e) {
      setErr(String(e?.message || e || "Delete failed"));
    }
  }

  const filterLabel =
    needsBasisMode === "needs" ? "Needs basis" : needsBasisMode === "has" ? "Has basis" : "All";

  const DEP_COL_META = useMemo(() => {
    return {
      asset: { label: "Asset", width: 80, align: "l" },
      venue: { label: "Venue", width: 110, align: "l" },
      wallet: { label: "Wallet", width: 120, align: "l" },
      amount: { label: "Amount", width: 110, align: "r" },
      receivedAt: { label: "Deposit Time", width: 170, align: "l" },
      lotId: { label: "Lot", width: 190, align: "l" },
      basisUsd: { label: "Basis USD", width: 110, align: "r" },
      transferWithdrawalId: { label: "Transfer Withdrawal ID", width: 220, align: "l" },
      actions: { label: "", width: 140, align: "l" },
    };
  }, []);

  const WD_COL_META = useMemo(() => {
    return {
      asset: { label: "Asset", width: 80, align: "l" },
      venue: { label: "Venue", width: 110, align: "l" },
      wallet: { label: "Wallet", width: 120, align: "l" },
      amount: { label: "Amount", width: 110, align: "r" },
      sentAt: { label: "Withdrawal Time", width: 180, align: "l" },
      destination: { label: "Destination", width: 220, align: "l" },
      txid: { label: "TxID", width: 240, align: "l" },
      transferDepositId: { label: "Transfer Deposit ID", width: 220, align: "l" },
      actions: { label: "", width: 140, align: "l" },
    };
  }, []);

  function renderDepCell(colKey, r) {
    switch (colKey) {
      case "asset":
        return (
          <td style={{ ...ui.td, fontWeight: 900 }}>
            <button style={ui.rowBtn}>{mask(r.asset || "—")}</button>
          </td>
        );
      case "venue":
        return <td style={ui.td}>{mask(r.venue || "—")}</td>;
      case "wallet":
        return <td style={{ ...ui.td, ...ui.mono }}>{mask(r.wallet || "—")}</td>;
      case "amount":
        return <td style={ui.tdR}>{hideTableData ? "••••" : fmtQty(r.amount)}</td>;
      case "receivedAt":
        return <td style={{ ...ui.td, ...ui.mono }}>{mask(r.receivedAt || "—")}</td>;
      case "lotId":
        return (
          <td style={{ ...ui.td, ...ui.mono }} title={hideTableData ? undefined : String(r.lotId || "")}>
            {mask(r.lotId || "—")}
          </td>
        );
      case "basisUsd":
        return <td style={ui.tdR}>{hideTableData ? "••••" : fmtUsd(r.basisUsd)}</td>;
      case "transferWithdrawalId":
        return (
          <td style={{ ...ui.td, ...ui.mono }} title={hideTableData ? undefined : String(r.transferWithdrawalId || "")}>
            {r.transferWithdrawalId ? linkedBadge(r.transferWithdrawalId) : "—"}
          </td>
        );
      case "actions":
        return (
          <td style={ui.td}>
            <span style={ui.actionRow} onClick={(e) => e.stopPropagation()}>
              <button
                style={ui.tinyBtn}
                onClick={() => primeDepositEditFromRow(r)}
                title="Edit this deposit (PATCH /api/deposits/{id})"
              >
                Edit
              </button>
              <button
                style={ui.tinyBtn}
                onClick={() => onDeleteDepositRow(r)}
                title="Delete this deposit (DELETE /api/deposits/{id})"
              >
                Delete
              </button>
            </span>
          </td>
        );
      default:
        return <td style={ui.td}>{mask(r[colKey])}</td>;
    }
  }

  function renderWdCell(colKey, r) {
    switch (colKey) {
      case "asset":
        return (
          <td style={{ ...ui.td, fontWeight: 900 }}>
            <button style={ui.rowBtn}>{mask(r.asset || "—")}</button>
          </td>
        );
      case "venue":
        return <td style={ui.td}>{mask(r.venue || "—")}</td>;
      case "wallet":
        return <td style={{ ...ui.td, ...ui.mono }}>{mask(r.wallet || "—")}</td>;
      case "amount":
        return <td style={ui.tdR}>{hideTableData ? "••••" : fmtQty(r.amount)}</td>;
      case "sentAt":
        return <td style={{ ...ui.td, ...ui.mono }}>{mask(r.sentAt || "—")}</td>;
      case "destination":
        return <td style={ui.td}>{mask(r.destination || "—")}</td>;
      case "txid":
        return (
          <td style={{ ...ui.td, ...ui.mono }} title={hideTableData ? undefined : String(r.txid || "")}>
            {mask(r.txid || "—")}
          </td>
        );
      case "transferDepositId":
        return (
          <td style={{ ...ui.td, ...ui.mono }} title={hideTableData ? undefined : String(r.transferDepositId || "")}>
            {r.transferDepositId ? linkedBadge(r.transferDepositId) : "—"}
          </td>
        );
      case "actions":
        return (
          <td style={ui.td}>
            <span style={ui.actionRow} onClick={(e) => e.stopPropagation()}>
              <button
                style={ui.tinyBtn}
                onClick={() => primeWithdrawalEditFromRow(r)}
                title="Edit this withdrawal (PATCH /api/withdrawals/{id})"
              >
                Edit
              </button>
              <button
                style={ui.tinyBtn}
                onClick={() => onDeleteWithdrawalRow(r)}
                title="Delete this withdrawal (DELETE /api/withdrawals/{id})"
              >
                Delete
              </button>
            </span>
          </td>
        );
      default:
        return <td style={ui.td}>{mask(r[colKey])}</td>;
    }
  }

  function headerStyleFor(colKey, metaMap) {
    const meta = metaMap[colKey] || {};
    const w = meta.width ? { width: meta.width } : null;
    const align = meta.align === "r" ? { textAlign: "right" } : null;
    const base = colKey === "actions" ? { ...ui.th, ...ui.thNoSort } : ui.th;
    return { ...base, ...(w || {}), ...(align || {}) };
  }

  const headerTitle = "Ledger";
  const headerSub =
    tab === "withdrawals"
      ? `Withdrawals list + manual entry ${wdLoading ? "(Loading…)" : ""}`
      : `Needs-basis queue + lot basis editor ${loading ? "(Loading…)" : ""}`;

  const body = (
    <div style={ui.wrap}>
      <div style={ui.header}>
        <div>
          <div style={ui.title}>{headerTitle}</div>
          <div style={ui.sub}>{headerSub}</div>
        </div>

        <div style={ui.right}>
          <div style={ui.tabStrip} title="Switch view">
            <button style={ui.tabBtn(tab === "deposits")} onClick={() => setTab("deposits")}>
              Deposits
            </button>
            <button style={ui.tabBtn(tab === "withdrawals")} onClick={() => setTab("withdrawals")}>
              Withdrawals
            </button>
          </div>

{/* Ledger reconcile / rebuild controls */}
<div style={{ display: 'flex', alignItems: 'center', gap: 8, marginLeft: 10, flexWrap: 'wrap' }}>
  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
    <span style={{ fontSize: 12, opacity: 0.85 }}>Ledger</span>
    <input
      value={syncWalletId}
      onChange={(e) => setSyncWalletId(e.target.value)}
      title="wallet_id for ledger sync"
      style={{ ...ui.input, width: 110, padding: '6px 8px', fontSize: 12 }}
    />

            <label
              style={{ display: "flex", alignItems: "center", gap: 6, marginLeft: 8 }}
              title="Display-only: when enabled, Deposits/Withdrawals lists fetch across all wallet_ids (includes wallet-address ingest rows where wallet_id='wallet_address')."
            >
              <input
                type="checkbox"
                checked={!!viewAllWallets}
                onChange={(e) => setViewAllWallets(!!e.target.checked)}
              />
              <span>All wallets</span>
            </label>

    <input
      value={String(syncLimit)}
      onChange={(e) => setSyncLimit(e.target.value)}
      title="page size (limit)"
      style={{ ...ui.input, width: 70, padding: '6px 8px', fontSize: 12 }}
    />
              <label style={{ display: 'flex', alignItems: 'center', gap: 6 }} title="When unchecked, runs dry_run=true (no DB writes)">
                <input type="checkbox" checked={!!syncApply} onChange={(e) => setSyncApply(!!e.target.checked)} />
                <span style={{ fontSize: 12, opacity: 0.85 }}>Apply</span>
              </label>
  </div>

  <button
    style={syncRunning ? ui.btnDisabled : ui.btn}
    disabled={syncRunning}
    onClick={confirmAndRunReconcile}
    title="Runs /api/ledger/sync in a loop until next_cursor is empty (does not clear derived tables)."
  >
    Reconcile
  </button>

  <button
    style={syncRunning ? ui.btnDisabled : ui.btnDanger}
    disabled={syncRunning}
    onClick={confirmAndOpenFullRebuild}
    title="Clears derived tables for this wallet then runs a full rebuild (requires backend /api/ledger/reset)."
  >
    Full Rebuild
  </button>

  {syncRunning ? (
    <button style={ui.btn} onClick={cancelLedgerSync} title="Cancel the running reconcile/rebuild">
      Cancel
    </button>
  ) : null}

  <span style={{ fontSize: 12, opacity: ledgerStale ? 1 : 0.75, color: ledgerStale ? '#ffcc66' : undefined }}>
    {ledgerStale ? 'STALE' : 'OK'}
  </span>
</div>


          {tab === "deposits" ? (
            <>
              <select
                style={{ ...ui.ctl, minWidth: 140 }}
                value={needsBasisMode}
                onChange={(e) => setNeedsBasisMode(String(e.target.value))}
                title="Needs-basis filter"
              >
                <option value="needs">Needs basis</option>
                <option value="has">Has basis</option>
                <option value="all">All</option>
              </select>

              <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, opacity: 0.9 }}>
                <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(!!e.target.checked)} />
                Auto
              </label>

              <input
                style={{ ...ui.ctl, width: 92 }}
                value={String(refreshSeconds)}
                onChange={(e) => setRefreshSeconds(e.target.value)}
                onBlur={() => setRefreshSeconds((v) => clampSeconds(v, 300))}
                inputMode="numeric"
                placeholder="sec"
                title="Auto-refresh interval (seconds)"
              />

              <input
                style={{ ...ui.ctl, width: 92 }}
                value={String(limit)}
                onChange={(e) => setLimit(e.target.value)}
                onBlur={() => setLimit((v) => Math.max(50, Math.min(500, Math.floor(Number(v) || 200))))}
                inputMode="numeric"
                placeholder="rows"
                title="Row limit (max 500)"
              />

              <button style={ui.btn} onClick={() => doRefresh()}>
                Refresh
              </button>

              <button style={ui.btnPrimary} onClick={() => setShowNew((v) => !v)} title="Manual deposit entry">
                {showNew ? "Hide New" : "+ New"}
              </button>

<select
  value={xferVenue}
  onChange={(e) => setXferVenue(e.target.value)}
  style={{ ...ui.ctl, width: 140, marginLeft: 10 }}
  title="Venue for transfer ingest"
  aria-disabled={xferRunning}
>
  {xferVenueOptions.map((v) => (
    <option key={v} value={v}>
      {v}
    </option>
  ))}
</select>

<input
  value={String(xferLookbackDays)}
  disabled={xferMode === "all"}
  onChange={(e) => {
    const n = Math.max(1, Math.floor(Number(e.target.value || 0) || 1));
    setXferLookbackDays(n);
    lsSet(lsKey("xferLookbackDays"), n);
  }}
  style={{ ...ui.ctl, width: 70, marginLeft: 10 }}
  title="Lookback days for transfer ingest"
  inputMode="numeric"
/>
<label
                        style={{ display: "inline-flex", alignItems: "center", gap: 8, marginLeft: 10, userSelect: "none", flex: "0 0 auto" }}
                        title="Fetch all available transfer history (bounded by max_pages on the backend)"
                        onPointerDownCapture={(e) => e.stopPropagation()}
                        onPointerDown={(e) => e.stopPropagation()}
                        onMouseDown={(e) => e.stopPropagation()}
                        onClick={(e) => e.stopPropagation()}
                      >
  <input
    type="checkbox"
    checked={xferMode === "all"}
    onPointerDownCapture={(e) => e.stopPropagation()}
    onPointerDown={(e) => e.stopPropagation()}
    onMouseDown={(e) => e.stopPropagation()}
    onClick={(e) => e.stopPropagation()}
    onChange={(e) => {
      const next = e.target.checked ? "all" : "days";
      setXferMode(next);
      lsSet(lsKey("xferMode"), next);
    }}
  />
  All
</label>

<button
  type="button"
  style={{ ...ui.btn, position: "relative", zIndex: 100, opacity: xferRunning ? 0.7 : 1, marginLeft: 10 }}
          onPointerDownCapture={(e) => e.stopPropagation?.()}
          onPointerDown={(e) => e.stopPropagation?.()}
          onMouseDown={(e) => e.stopPropagation?.()}
          onClick={(e) => {
            e.stopPropagation?.();
            runTransferIngest();
          }}
  aria-disabled={xferRunning}
  title="POST /api/deposits/ingest then (paced) POST /api/withdrawals/ingest"
>
  {xferRunning ? "Syncing…" : "Sync Transfers"}
</button>

{xferMsg ? (
  <div
    style={{
      ...ui.mono,
      marginLeft: 10,
      maxWidth: 520,
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
      color:
        xferMsg.toLowerCase().includes("failed") || xferMsg.toLowerCase().includes("error")
          ? "#ff6b6b"
          : ui.fg,
    }}
    title={xferMsg}
  >
    {xferMsg}
  </div>
) : null}
            </>
          ) : (
            <>
              <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, opacity: 0.9 }}>
                <input type="checkbox" checked={wdAutoRefresh} onChange={(e) => setWdAutoRefresh(!!e.target.checked)} />
                Auto
              </label>

              <input
                style={{ ...ui.ctl, width: 92 }}
                value={String(wdRefreshSeconds)}
                onChange={(e) => setWdRefreshSeconds(e.target.value)}
                onBlur={() => setWdRefreshSeconds((v) => clampSeconds(v, 300))}
                inputMode="numeric"
                placeholder="sec"
                title="Auto-refresh interval (seconds)"
              />

              <input
                style={{ ...ui.ctl, width: 92 }}
                value={String(wdLimit)}
                onChange={(e) => setWdLimit(e.target.value)}
                onBlur={() => setWdLimit((v) => Math.max(50, Math.min(500, Math.floor(Number(v) || 200))))}
                inputMode="numeric"
                placeholder="rows"
                title="Row limit (max 500)"
              />

              <button style={ui.btn} onClick={() => doWdRefresh()}>
                Refresh
              </button>

              <button style={ui.btnPrimary} onClick={() => setWdShowNew((v) => !v)} title="Manual withdrawal entry">
                {wdShowNew ? "Hide New" : "+ New"}
              </button>

<select
  value={xferVenue}
  onChange={(e) => setXferVenue(e.target.value)}
  style={{ ...ui.ctl, width: 140, marginLeft: 10 }}
  title="Venue for transfer ingest"
  aria-disabled={xferRunning}
>
  {xferVenueOptions.map((v) => (
    <option key={v} value={v}>
      {v}
    </option>
  ))}
</select>

<input
  value={String(xferLookbackDays)}
  onChange={(e) => {
    const n = Math.max(1, Math.floor(Number(e.target.value || 0) || 1));
    setXferLookbackDays(n);
  }}
  style={{ ...ui.ctl, width: 70, marginLeft: 10 }}
  title="Lookback days for transfer ingest"
  inputMode="numeric"
/>

<button
  type="button"
  style={{ ...ui.btn, position: "relative", zIndex: 100, opacity: xferRunning ? 0.7 : 1, marginLeft: 10 }}
  onPointerDownCapture={(e) => e.stopPropagation()}
                      onPointerDown={(e) => e.stopPropagation()}
                      onMouseDown={(e) => e.stopPropagation()}
                      onClick={(e) => {
                        e.stopPropagation();
                        runTransferIngest();
                      }}
  aria-disabled={xferRunning}
  title="POST /api/deposits/ingest then (paced) POST /api/withdrawals/ingest"
>
  {xferRunning ? "Syncing…" : "Sync Transfers"}
</button>

{xferMsg ? (
  <div
    style={{
      ...ui.mono,
      marginLeft: 10,
      maxWidth: 520,
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
      color:
        xferMsg.toLowerCase().includes("failed") || xferMsg.toLowerCase().includes("error")
          ? "#ff6b6b"
          : ui.fg,
    }}
    title={xferMsg}
  >
    {xferMsg}
  </div>
) : null}
            </>
          )}

          <button style={ui.btn} onClick={() => onClose?.()} title="Close">
            Close
          </button>
        </div>
      </div>

      {/* Ledger sync progress (shows only when there is output) */}
      {((Array.isArray(syncLines) && syncLines.length) || syncRunning || resetLoading || rebuildOpen) ? (
        <div style={{ ...ui.panel, marginTop: 10, padding: 12 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10 }}>
            <div style={{ fontSize: 12, opacity: 0.85 }}>
              Sync log {syncRunning ? '(running...)' : ''} {syncLastResult?.rows_fetched === 0 ? '(done)' : ''}
            </div>
            <button style={ui.btnSmall} onClick={() => setSyncLines([])} disabled={syncRunning}>
              Clear
            </button>
          </div>
          <pre style={{ marginTop: 8, marginBottom: 0, maxHeight: 180, overflow: 'auto', fontSize: 12, lineHeight: 1.35 }}>
            {syncLines.join("\\n")}
          </pre>
            {rebuildOpen ? (
              <div
                style={{
                  position: 'fixed',
                  inset: 0,
                  zIndex: 9999,
                  background: 'rgba(0,0,0,0.55)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  padding: 16,
                }}
                onClick={() => { if (!resetLoading && !syncRunning) setRebuildOpen(false); }}
              >
                <div
                  style={{
                    width: 'min(760px, 96vw)',
                    borderRadius: 12,
                    background: 'var(--utt-surface-2, #14181f)',
                    border: '1px solid var(--utt-border, rgba(255,255,255,0.15))',
                    padding: 16,
                    boxShadow: '0 12px 40px rgba(0,0,0,0.55)',
                  }}
                  onClick={(e) => e.stopPropagation()}
                >
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
                    <div style={{ fontSize: 16, fontWeight: 700 }}>Full Rebuild — Confirm</div>
                    <button type="button" style={ui.btn} onClick={() => setRebuildOpen(false)} disabled={resetLoading || syncRunning}>
                      Close
                    </button>
                  </div>

                  <div style={{ marginTop: 10, fontSize: 13, lineHeight: 1.4 }}>
                    This will <b>delete</b> derived tables (<code>basis_lots</code> and <code>lot_journal</code>) for wallet <b>{syncWalletId}</b>,
                    then rebuild them by re-running FIFO with <code>dry_run=false</code>.
                  </div>

                  <div style={{ marginTop: 12, fontSize: 13 }}>
                    {resetLoading ? (
                      <div>Loading preview counts…</div>
                    ) : resetPreview ? (
                      <div>
                        Preview: journal_rows=<b>{resetPreview.journal_rows}</b>, lot_rows=<b>{resetPreview.lot_rows}</b>
                      </div>
                    ) : (
                      <div>Preview unavailable.</div>
                    )}
                  </div>

                  {!!resetError ? (
                    <div style={{ marginTop: 10, color: '#ff9a9a', fontSize: 12 }}>
                      {resetError}
                    </div>
                  ) : null}

                  <div style={{ marginTop: 12 }}>
                    <div style={{ fontSize: 12, opacity: 0.85, marginBottom: 6 }}>Type <b>RESET</b> to confirm</div>
                    <input
                      value={resetConfirmText}
                      onChange={(e) => setResetConfirmText(e.target.value)}
                      placeholder="RESET"
                      style={{ ...ui.ctl, width: '100%', padding: '8px 10px', fontSize: 13, height: 36 }}
                      autoFocus
                    />
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 14 }}>
                    <button type="button" style={ui.btn} onClick={() => setRebuildOpen(false)} disabled={resetLoading || syncRunning}>
                      Cancel
                    </button>
                    <button
                      type="button"
                      style={ui.btnDanger}
                      onClick={() => confirmFullRebuild()}
                      disabled={resetLoading || syncRunning || String(resetConfirmText || '').trim().toUpperCase() !== 'RESET'}
                      title="Deletes derived tables, then rebuilds FIFO"
                    >
                      Delete + Rebuild
                    </button>
                  </div>
                </div>
              </div>
            ) : null}

        </div>
      ) : null}


      {tab === "deposits" ? (
        err ? (
          <div style={ui.warn}>
            <div style={{ fontSize: 12, fontWeight: 900, marginBottom: 6 }}>Deposits error</div>
            <div style={{ fontSize: 12, opacity: 0.9 }}>{err}</div>
            <div style={{ marginTop: 10, fontSize: 12, opacity: 0.85, ...ui.mono }}>
              - API base: {String(apiBase || "")}
              <br />- Endpoint: /api/deposits (mode: {filterLabel}) &amp; limit={String(limit)}
            </div>
          </div>
        ) : null
      ) : (
        wdErr ? (
          <div style={ui.warn}>
            <div style={{ fontSize: 12, fontWeight: 900, marginBottom: 6 }}>Withdrawals error</div>
            <div style={{ fontSize: 12, opacity: 0.9 }}>{wdErr}</div>
            <div style={{ marginTop: 10, fontSize: 12, opacity: 0.85, ...ui.mono }}>
              - API base: {String(apiBase || "")}
              <br />- Endpoint: /api/withdrawals &amp; limit={String(wdLimit)}
            </div>
          </div>
        ) : null
      )}

      <div style={ui.contentRow}>
        {/* Left: list */}
        <div style={ui.panel}>
          <div style={ui.panelHdr}>
            {tab === "deposits" ? (
              <>
                <div style={{ fontSize: 12, fontWeight: 900, minWidth: 0 }}>
                  Deposits ({hideTableData ? "••••" : String(sortedRows.length)})
                  <span style={{ marginLeft: 10, fontSize: 11, opacity: 0.7 }}>
                    Last: <span style={ui.mono}>{mask(lastUpdated || "—")}</span>
                  </span>
                  <span style={{ marginLeft: 10, fontSize: 11, opacity: 0.65 }}>
                    Sort: <span style={ui.mono}>{String(sortKey || "—")}</span>{" "}
                    {String(sortDir || "").toLowerCase() === "asc" ? "↑" : "↓"}
                  </span>
                </div>

                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <button
                    style={ui.btnGhost}
                    onClick={() => setShowColEditor((v) => !v)}
                    title="Show/hide column order controls"
                  >
                    Cols
                  </button>

                  <input
                    style={{ ...ui.ctl, width: 260 }}
                    value={q}
                    onChange={(e) => setQ(e.target.value)}
                    placeholder="search (asset/venue/wallet/txid/transfer/lot)…"
                    title="Filter rows"
                  />
                </div>
              </>
            ) : (
              <>
                <div style={{ fontSize: 12, fontWeight: 900, minWidth: 0 }}>
                  Withdrawals ({hideTableData ? "••••" : String(wdSortedRows.length)})
                  <span style={{ marginLeft: 10, fontSize: 11, opacity: 0.7 }}>
                    Last: <span style={ui.mono}>{mask(wdLastUpdated || "—")}</span>
                  </span>
                  <span style={{ marginLeft: 10, fontSize: 11, opacity: 0.65 }}>
                    Sort: <span style={ui.mono}>{String(wdSortKey || "—")}</span>{" "}
                    {String(wdSortDir || "").toLowerCase() === "asc" ? "↑" : "↓"}
                  </span>
                </div>

                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <button
                    style={ui.btnGhost}
                    onClick={() => setWdShowColEditor((v) => !v)}
                    title="Show/hide column order controls"
                  >
                    Cols
                  </button>

                  <input
                    style={{ ...ui.ctl, width: 260 }}
                    value={wdQ}
                    onChange={(e) => setWdQ(e.target.value)}
                    placeholder="search (asset/venue/wallet/txid/transfer)…"
                    title="Filter rows"
                  />
                </div>
              </>
            )}
          </div>

          <div style={ui.tableWrap}>
            <table style={ui.table}>
              <thead>
                <tr>
                  {(tab === "deposits" ? colOrder : wdColOrder).map((colKey) => {
                    const metaMap = tab === "deposits" ? DEP_COL_META : WD_COL_META;
                    const meta = metaMap[colKey] || { label: colKey };
                    const caret = tab === "deposits" ? sortCaretFor(colKey) : wdSortCaretFor(colKey);
                    const thStyle = headerStyleFor(colKey, metaMap);

                    return (
                      <th
                        key={colKey}
                        style={thStyle}
                        onClick={colKey === "actions" ? undefined : () => (tab === "deposits" ? toggleSort(colKey) : wdToggleSort(colKey))}
                        title={colKey === "actions" ? "" : "Click to sort"}
                      >
                        <span style={ui.thInner}>
                          <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>{meta.label}</span>
                          {caret ? <span style={ui.thCaret}>{caret}</span> : null}
                        </span>
                      </th>
                    );
                  })}
                </tr>
              </thead>

              <tbody>
                {tab === "deposits" ? (
                  sortedRows.length === 0 ? (
                    <tr>
                      <td style={{ ...ui.td, opacity: 0.7 }} colSpan={colOrder.length}>
                        No rows.
                      </td>
                    </tr>
                  ) : (
                    sortedRows.map((r) => {
                      const isSel = String(r.id) === String(selectedId);
                      return (
                        <tr
                          key={String(r.id)}
                          style={{ background: isSel ? "rgba(120,160,255,0.10)" : "transparent" }}
                          onClick={() => {
                            setSelectedId(String(r.id));
                            primeEditorFromRow(r);
                          }}
                          title={hideTableData ? undefined : `txid: ${r.txid || "—"}`}
                        >
                          {colOrder.map((colKey) => renderDepCell(colKey, r))}
                        </tr>
                      );
                    })
                  )
                ) : (
                  wdSortedRows.length === 0 ? (
                    <tr>
                      <td style={{ ...ui.td, opacity: 0.7 }} colSpan={wdColOrder.length}>
                        No rows.
                      </td>
                    </tr>
                  ) : (
                    wdSortedRows.map((r) => {
                      const isSel = String(r.id) === String(wdSelectedId);
                      return (
                        <tr
                          key={String(r.id)}
                          style={{ background: isSel ? "rgba(120,160,255,0.10)" : "transparent" }}
                          onClick={() => setWdSelectedId(String(r.id))}
                          title={hideTableData ? undefined : `txid: ${r.txid || "—"}`}
                        >
                          {wdColOrder.map((colKey) => renderWdCell(colKey, r))}
                        </tr>
                      );
                    })
                  )
                )}
              </tbody>
            </table>
          </div>

          {tab === "deposits" ? (
            showColEditor ? (
              <div style={ui.colEditor}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10, marginBottom: 10 }}>
                  <div style={{ fontSize: 12, fontWeight: 900 }}>Column order</div>
                  <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                    <button style={ui.smallBtn} onClick={resetCols} title="Reset to default order">
                      Reset
                    </button>
                    <button style={ui.smallBtn} onClick={() => setShowColEditor(false)} title="Hide">
                      Done
                    </button>
                  </div>
                </div>

                {(colOrder || []).map((k, idx) => {
                  const meta = DEP_COL_META[k] || { label: k };
                  return (
                    <div key={k} style={ui.colRow}>
                      <div style={{ fontSize: 12, opacity: 0.9 }}>{meta.label || k}</div>
                      <button style={ui.smallBtn} onClick={() => moveCol(idx, -1)} disabled={idx === 0} title="Move left">
                        ◀
                      </button>
                      <button
                        style={ui.smallBtn}
                        onClick={() => moveCol(idx, +1)}
                        disabled={idx === colOrder.length - 1}
                        title="Move right"
                      >
                        ▶
                      </button>
                    </div>
                  );
                })}

                <div style={{ ...ui.help, marginTop: 10 }}>
                  Tip: the table supports horizontal scrolling; if your window is narrow, scroll left/right to see all columns.
                </div>
              </div>
            ) : (
              <div style={ui.footerTip}>
                Tip: selection primes the editor from total_basis_usd and acquired_at (falls back to deposit_time).
                <span style={{ marginLeft: 10, opacity: 0.7 }}>
                  Use the mouse wheel + Shift (or trackpad) to scroll horizontally if desired.
                </span>
              </div>
            )
          ) : (
            wdShowColEditor ? (
              <div style={ui.colEditor}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10, marginBottom: 10 }}>
                  <div style={{ fontSize: 12, fontWeight: 900 }}>Column order</div>
                  <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                    <button style={ui.smallBtn} onClick={wdResetCols} title="Reset to default order">
                      Reset
                    </button>
                    <button style={ui.smallBtn} onClick={() => setWdShowColEditor(false)} title="Hide">
                      Done
                    </button>
                  </div>
                </div>

                {(wdColOrder || []).map((k, idx) => {
                  const meta = WD_COL_META[k] || { label: k };
                  return (
                    <div key={k} style={ui.colRow}>
                      <div style={{ fontSize: 12, opacity: 0.9 }}>{meta.label || k}</div>
                      <button style={ui.smallBtn} onClick={() => wdMoveCol(idx, -1)} disabled={idx === 0} title="Move left">
                        ◀
                      </button>
                      <button
                        style={ui.smallBtn}
                        onClick={() => wdMoveCol(idx, +1)}
                        disabled={idx === wdColOrder.length - 1}
                        title="Move right"
                      >
                        ▶
                      </button>
                    </div>
                  );
                })}

                <div style={{ ...ui.help, marginTop: 10 }}>
                  Tip: the table supports horizontal scrolling; if your window is narrow, scroll left/right to see all columns.
                </div>
              </div>
            ) : (
              <div style={ui.footerTip}>
                Tip: create withdrawals manually here; later we’ll add transfer matching + reconciliation workflows.
              </div>
            )
          )}
        </div>

        {/* Right: editor / manual entry */}
        <div style={ui.panel}>
          <div style={ui.panelHdr}>
            <div style={{ fontSize: 12, fontWeight: 900 }}>
              {tab === "deposits"
                ? (showNew ? "Manual Entry + Editors" : "Editors")
                : (wdShowNew ? "Manual Entry + Editor" : "Withdrawal Details")}
            </div>
            <div style={{ fontSize: 11, opacity: 0.7 }}>
              {tab === "deposits" ? (
                selected ? (
                  <>
                    Deposit: <span style={ui.mono}>{mask(selected.id)}</span>
                  </>
                ) : (
                  "Select a deposit row"
                )
              ) : (
                wdSelected ? (
                  <>
                    Withdrawal: <span style={ui.mono}>{mask(wdSelected.id)}</span>
                  </>
                ) : (
                  "Select a withdrawal row"
                )
              )}
            </div>
          </div>

          <div style={ui.rightScroll}>
            {tab === "deposits" ? (
              <>
                {/* Deposit row edit section */}
                {depEditId ? (
                  <div style={ui.section}>
                    <div style={ui.sectionTitleRow}>
                      <div style={{ fontSize: 12, fontWeight: 900 }}>Edit Deposit</div>
                      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                        <button style={ui.btn} onClick={clearDepositEdit} disabled={depEditSaving}>
                          Cancel
                        </button>
                        <button style={ui.btnPrimary} onClick={onSaveDepositEdit} disabled={depEditSaving}>
                          {depEditSaving ? "Saving…" : "Save"}
                        </button>
                      </div>
                    </div>

                    {depEditMsg ? (
                      <div style={depEditMsg.toLowerCase().includes("saved") ? ui.ok : ui.warn}>
                        <div style={{ fontSize: 12, opacity: 0.95 }}>{depEditMsg}</div>
                      </div>
                    ) : null}

                    <div style={ui.fieldGrid}>
                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Qty *</div>
                        <input style={ui.input} value={depEditQty} onChange={(e) => setDepEditQty(e.target.value)} inputMode="decimal" type="number" step="any" min="0" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Deposit Time</div>
                        <DateTimeField ui={ui} value={depEditTime} onChange={(v) => setDepEditTime(v)} placeholder="YYYY-MM-DDTHH:MM" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>TxID</div>
                        <input style={ui.input} value={depEditTxid} onChange={(e) => setDepEditTxid(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Network</div>
                        <input style={ui.input} value={depEditNetwork} onChange={(e) => setDepEditNetwork(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Transfer Withdrawal ID</div>
                        <input
                          style={ui.input}
                          value={depEditTransferWithdrawalId}
                          onChange={(e) => setDepEditTransferWithdrawalId(e.target.value)}
                          placeholder="optional"
                        />
                      </div>

                      <div style={{ ...ui.field, gridColumn: "1 / -1" }}>
                        <div style={ui.fieldLabel}>Note</div>
                        <textarea style={ui.textarea} value={depEditNote} onChange={(e) => setDepEditNote(e.target.value)} placeholder="optional" />
                      </div>
                    </div>

                    <div style={{ marginTop: 10, ...ui.help }}>
                      This uses <span style={ui.mono}>PATCH /api/deposits/&lt;id&gt;</span>. If your backend doesn’t expose that route yet,
                      you’ll get an error; we’ll add it server-side next.
                    </div>
                  </div>
                ) : null}

                {/* Manual deposit entry */}
                {showNew ? (
                  <div style={ui.section}>
                    <div style={ui.sectionTitleRow}>
                      <div style={{ fontSize: 12, fontWeight: 900 }}>Manual Deposit Entry</div>
                      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                        <button type="button"
                          style={ui.btn}
                          onClick={() => clearNewForm({ keepVenueWallet: true })}
                          disabled={creating}
                          title="Clear (keeps venue + wallet)"
                        >
                          Clear
                        </button>
                        <button
                          style={ui.btnPrimary}
                          onClick={onCreateDeposit}
                          disabled={creating}
                          title="POST /api/deposits"
                        >
                          {creating ? "Creating…" : "Create"}
                        </button>
                      </div>
                    </div>

                    {newMsg ? (
                      <div
                        style={
                          newMsg.toLowerCase().includes("created") || newMsg.toLowerCase().includes("basis set")
                            ? ui.ok
                            : ui.warn
                        }
                      >
                        <div style={{ fontSize: 12, opacity: 0.95 }}>{newMsg}</div>
                      </div>
                    ) : null}

                    <div style={ui.fieldGrid}>
                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Venue *</div>
                        <input style={ui.input} value={newVenue} onChange={(e) => setNewVenue(e.target.value)} placeholder="e.g., coinbase" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Wallet ID</div>
                        <input style={ui.input} value={newWalletId} onChange={(e) => setNewWalletId(e.target.value)} placeholder="default" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Asset *</div>
                        <input style={ui.input} value={newAsset} onChange={(e) => setNewAsset(e.target.value)} placeholder="e.g., BTC" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Qty *</div>
                        <input style={ui.input} value={newQty} onChange={(e) => setNewQty(e.target.value)} inputMode="decimal" type="number" step="any" min="0" placeholder="e.g., 0.1234" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Deposit Time</div>
                        <DateTimeField ui={ui} value={newDepositTime} onChange={(v) => setNewDepositTime(v)} placeholder="YYYY-MM-DDTHH:MM" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Acquired Override</div>
                        <DateTimeField ui={ui} value={newAcquiredAtOverride} onChange={(v) => setNewAcquiredAtOverride(v)} placeholder="YYYY-MM-DDTHH:MM" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>TxID</div>
                        <input style={ui.input} value={newTxid} onChange={(e) => setNewTxid(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Network</div>
                        <input style={ui.input} value={newNetwork} onChange={(e) => setNewNetwork(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Basis Total USD</div>
                        <input style={ui.input} value={newBasisTotalUsd} onChange={(e) => setNewBasisTotalUsd(e.target.value)} inputMode="decimal" type="number" step="any" min="0" placeholder="optional (>=0)" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Basis USD/Coin</div>
                        <input style={ui.input} value={newBasisUsdPerCoin} onChange={(e) => setNewBasisUsdPerCoin(e.target.value)} inputMode="decimal" type="number" step="any" min="0" placeholder="optional (>=0)" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>USD Price (opt)</div>
                        <div style={ui.priceRow}>
                          <input style={ui.priceInput} value={newUsdPrice} onChange={(e) => setNewUsdPrice(e.target.value)} inputMode="decimal" type="number" step="any" min="0" placeholder="e.g., 45000" />
                          <button style={ui.smallBtn} onClick={onCalcBasisFromPrice} disabled={creating} title="Set Basis Total USD = Qty * USD Price">
                            Calc Basis
                          </button>
                        </div>
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Transfer Withdrawal ID</div>
                        <input style={ui.input} value={newTransferWithdrawalId} onChange={(e) => setNewTransferWithdrawalId(e.target.value)} placeholder="optional (creates transfer-in)" />
                      </div>

                      <div style={{ ...ui.field, gridColumn: "1 / -1" }}>
                        <div style={ui.fieldLabel}>Note</div>
                        <textarea style={ui.textarea} value={newNote} onChange={(e) => setNewNote(e.target.value)} placeholder="optional" />
                      </div>
                    </div>
                  </div>
                ) : null}

                {/* Lot basis editor */}
                <div style={{ padding: 12 }}>
                  {!selected ? (
                    <div style={{ ...ui.help, padding: 2 }}>
                      Select a deposit on the left to edit the linked lot basis.
                    </div>
                  ) : (
                    <div style={ui.form}>
                      <div style={ui.help}>
                        <div>
                          Asset: <b>{mask(selected.asset || "—")}</b> | Venue: <b>{mask(selected.venue || "—")}</b>
                        </div>
                        <div style={{ marginTop: 4 }}>
                          Amount: <span style={ui.mono}>{hideTableData ? "••••" : fmtQty(selected.amount)}</span> | Lot ID:{" "}
                          <span style={ui.mono}>{mask(selected.lotId || "—")}</span>
                        </div>
                        <div style={{ marginTop: 4 }}>
                          Tx: <span style={ui.mono}>{mask(selected.txid || "—")}</span>
                        </div>
                      </div>

                      <div style={ui.formRow}>
                        <div style={ui.formLabel}>Total Basis USD</div>
                        <input
                          style={ui.input}
                          value={editBasisUsd}
                          onChange={(e) => setEditBasisUsd(e.target.value)}
                          inputMode="decimal"
                          type="number"
                          step="any"
                          min="0"
                          placeholder="e.g., 123.45"
                          disabled={saving}
                        />
                      </div>

                      <div style={ui.formRow}>
                        <div style={ui.formLabel}>Acquired At</div>
                        <input
                          style={ui.input}
                          value={editAcquiredAt}
                          onChange={(e) => setEditAcquiredAt(e.target.value)}
                          placeholder="ISO timestamp or YYYY-MM-DD"
                          disabled={saving}
                        />
                      </div>

                      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                        <button style={ui.btnPrimary} onClick={onSaveBasis} disabled={saving || !selected?.lotId}>
                          {saving ? "Saving…" : "Save Basis"}
                        </button>

                        <button
                          style={ui.btn}
                          onClick={() => {
                            if (!selected) return;
                            primeEditorFromRow(selected);
                            setEditMsg("");
                          }}
                          disabled={saving || !selected}
                          title="Reset editor inputs to current row values"
                        >
                          Reset
                        </button>

                        <div style={{ marginLeft: "auto" }} />
                        <div style={ui.editorMsg}>{editMsg ? <span>{editMsg}</span> : null}</div>
                      </div>

                      <div style={{ ...ui.help, marginTop: 4 }}>
                        Save calls <span style={ui.mono}>PATCH /api/deposits/lots/&lt;lot_id&gt;</span>.
                      </div>
                    </div>
                  )}
                </div>
              </>
            ) : (
              <>
                {/* Withdrawal row edit section */}
                {wdEditId ? (
                  <div style={ui.section}>
                    <div style={ui.sectionTitleRow}>
                      <div style={{ fontSize: 12, fontWeight: 900 }}>Edit Withdrawal</div>
                      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                        <button style={ui.btn} onClick={clearWithdrawalEdit} disabled={wdEditSaving}>
                          Cancel
                        </button>
                        <button style={ui.btnPrimary} onClick={onSaveWithdrawalEdit} disabled={wdEditSaving}>
                          {wdEditSaving ? "Saving…" : "Save"}
                        </button>
                      </div>
                    </div>

                    {wdEditMsg ? (
                      <div style={wdEditMsg.toLowerCase().includes("saved") ? ui.ok : ui.warn}>
                        <div style={{ fontSize: 12, opacity: 0.95 }}>{wdEditMsg}</div>
                      </div>
                    ) : null}

                    <div style={ui.fieldGrid}>
                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Qty *</div>
                        <input style={ui.input} value={wdEditQty} onChange={(e) => setWdEditQty(e.target.value)} inputMode="decimal" type="number" step="any" min="0" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Withdrawal Time</div>
                        <DateTimeField ui={ui} value={wdEditTime} onChange={(v) => setWdEditTime(v)} placeholder="YYYY-MM-DDTHH:MM" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>TxID</div>
                        <input style={ui.input} value={wdEditTxid} onChange={(e) => setWdEditTxid(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Network</div>
                        <input style={ui.input} value={wdEditNetwork} onChange={(e) => setWdEditNetwork(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Destination</div>
                        <input style={ui.input} value={wdEditDestination} onChange={(e) => setWdEditDestination(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Transfer Deposit ID</div>
                        <input
                          style={ui.input}
                          value={wdEditTransferDepositId}
                          onChange={(e) => setWdEditTransferDepositId(e.target.value)}
                          placeholder="optional"
                        />
                      </div>

                      <div style={{ ...ui.field, gridColumn: "1 / -1" }}>
                        <div style={ui.fieldLabel}>Note</div>
                        <textarea style={ui.textarea} value={wdEditNote} onChange={(e) => setWdEditNote(e.target.value)} placeholder="optional" />
                      </div>
                    </div>

                    <div style={{ marginTop: 10, ...ui.help }}>
                      This uses <span style={ui.mono}>PATCH /api/withdrawals/&lt;id&gt;</span>. If your backend doesn’t expose that route yet,
                      you’ll get an error; we’ll add it server-side next.
                    </div>
                  </div>
                ) : null}

                {/* Manual withdrawal entry */}
                {wdShowNew ? (
                  <div style={ui.section}>
                    <div style={ui.sectionTitleRow}>
                      <div style={{ fontSize: 12, fontWeight: 900 }}>Manual Withdrawal Entry</div>
                      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                        <button type="button"
                          style={ui.btn}
                          onClick={() => wdClearNewForm({ keepVenueWallet: true })}
                          disabled={wdCreating}
                          title="Clear (keeps venue + wallet)"
                        >
                          Clear
                        </button>
                        <button
                          style={ui.btnPrimary}
                          onClick={onCreateWithdrawal}
                          disabled={wdCreating}
                          title="POST /api/withdrawals"
                        >
                          {wdCreating ? "Creating…" : "Create"}
                        </button>
                      </div>
                    </div>

                    {wdMsg ? (
                      <div style={wdMsg.toLowerCase().includes("created") ? ui.ok : ui.warn}>
                        <div style={{ fontSize: 12, opacity: 0.95 }}>{wdMsg}</div>
                      </div>
                    ) : null}

                    <label style={{ ...ui.row, gap: 10, fontSize: 12, opacity: 0.95 }}>
                      <input
                        type="checkbox"
                        checked={wdApplyLotImpact}
                        onChange={(e) => setWdApplyLotImpact(!!e.target.checked)}
                      />
                      Apply lot impact (consume lots / FIFO)
                    </label>

                    <div style={ui.fieldGrid}>
                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Venue *</div>
                        <input style={ui.input} value={wdVenue} onChange={(e) => setWdVenue(e.target.value)} placeholder="e.g., coinbase" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Wallet ID</div>
                        <input style={ui.input} value={wdWalletId} onChange={(e) => setWdWalletId(e.target.value)} placeholder="default" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Asset *</div>
                        <input style={ui.input} value={wdAsset} onChange={(e) => setWdAsset(e.target.value)} placeholder="e.g., BTC" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Qty *</div>
                        <input style={ui.input} value={wdQty} onChange={(e) => setWdQty(e.target.value)} inputMode="decimal" type="number" step="any" min="0" placeholder="e.g., 0.1234" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Withdrawal Time</div>
                        <DateTimeField ui={ui} value={wdWithdrawalTime} onChange={(v) => setWdWithdrawalTime(v)} placeholder="YYYY-MM-DDTHH:MM" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>TxID</div>
                        <input style={ui.input} value={wdTxid} onChange={(e) => setWdTxid(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Network</div>
                        <input style={ui.input} value={wdNetwork} onChange={(e) => setWdNetwork(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Destination</div>
                        <input style={ui.input} value={wdDestination} onChange={(e) => setWdDestination(e.target.value)} placeholder="optional" />
                      </div>

                      <div style={ui.field}>
                        <div style={ui.fieldLabel}>Transfer Deposit ID</div>
                        <input style={ui.input} value={wdTransferDepositId} onChange={(e) => setWdTransferDepositId(e.target.value)} placeholder="optional (links transfer)" />
                      </div>

                      <div style={{ ...ui.field, gridColumn: "1 / -1" }}>
                        <div style={ui.fieldLabel}>Note</div>
                        <textarea style={ui.textarea} value={wdNote} onChange={(e) => setWdNote(e.target.value)} placeholder="optional" />
                      </div>
                    </div>
                  </div>
                ) : null}

                {/* Details */}
                <div style={{ padding: 12 }}>
                  {!wdSelected ? (
                    <div style={{ ...ui.help, padding: 2 }}>
                      Select a withdrawal on the left to view its details.
                    </div>
                  ) : (
                    <div style={ui.form}>
                      <div style={ui.help}>
                        <div>
                          Asset: <b>{mask(wdSelected.asset || "—")}</b> | Venue: <b>{mask(wdSelected.venue || "—")}</b>
                        </div>
                        <div style={{ marginTop: 4 }}>
                          Amount: <span style={ui.mono}>{hideTableData ? "••••" : fmtQty(wdSelected.amount)}</span> | Wallet:{" "}
                          <span style={ui.mono}>{mask(wdSelected.wallet || "—")}</span>
                        </div>
                        <div style={{ marginTop: 4 }}>
                          Tx: <span style={ui.mono}>{mask(wdSelected.txid || "—")}</span>
                        </div>
                        <div style={{ marginTop: 4 }}>
                          Time: <span style={ui.mono}>{mask(wdSelected.sentAt || "—")}</span>
                        </div>
                        <div style={{ marginTop: 4 }}>
                          Destination: <span style={ui.mono}>{mask(wdSelected.destination || "—")}</span>
                        </div>
                        <div style={{ marginTop: 4 }}>
                          Transfer Deposit: <span style={ui.mono}>{mask(wdSelected.transferDepositId || "—")}</span>
                        </div>
                      </div>

                      <div style={{ ...ui.help, marginTop: 6 }}>
                        Use the row “Edit” button to patch fields (if your backend supports <span style={ui.mono}>PATCH /api/withdrawals/&lt;id&gt;</span>).
                      </div>
                    </div>
                  )}
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );

  if (!popup) return body;

  return (
    <div
      style={ui.overlay}
      onMouseDown={(e) => {
        // click outside closes
        if (e.target === e.currentTarget) onClose?.();
      }}
    >
      <div style={ui.popupCard} onMouseDown={(e) => e.stopPropagation()}>
        {body}
      </div>
    </div>
  );
}