// frontend/src/lib/api.js

import axios from "axios";

/**
 * API base handling:
 * - Prefer VITE_API_BASE if set (e.g. http://127.0.0.1:8000)
 * - Otherwise default to backend on 127.0.0.1:8000
 */
export const API_BASE = (import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000").replace(/\/$/, "");

export const http = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
});

function cleanParams(params) {
  const out = {};
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v === undefined || v === null) return;
    if (typeof v === "string" && v.trim() === "") return;
    out[k] = v;
  });
  return out;
}

// ─────────────────────────────────────────────────────────────
// Single-flight / de-dupe helpers (front-end only)
// ─────────────────────────────────────────────────────────────

/**
 * Ensures at most one in-flight request per key.
 * Optional minIntervalMs throttles how often a NEW request can start per key.
 *
 * Behavior:
 * - If a request is already in flight for key -> return it.
 * - Else if minIntervalMs not elapsed since last start:
 *    - if in-flight exists -> await it
 *    - else -> no-op, resolve null
 * - Else start a new request and track it until it settles.
 */
const _inFlight = new Map(); // key -> Promise
const _lastStart = new Map(); // key -> number (ms epoch)

function singleFlight(key, fn, { minIntervalMs = 0 } = {}) {
  const now = Date.now();

  if (minIntervalMs > 0) {
    const last = _lastStart.get(key) || 0;
    if (now - last < minIntervalMs) {
      if (_inFlight.has(key)) return _inFlight.get(key);
      return Promise.resolve(null);
    }
  }

  if (_inFlight.has(key)) return _inFlight.get(key);

  _lastStart.set(key, now);

  const p = (async () => fn())().finally(() => {
    _inFlight.delete(key);
  });

  _inFlight.set(key, p);
  return p;
}

// ─────────────────────────────────────────────────────────────
// Startup retry + cross-tab heavy-work coordination
// ─────────────────────────────────────────────────────────────

const DEFAULT_RETRY_DELAYS_MS = Object.freeze([1000, 2000, 5000]);
const HEAVY_TASK_CHANNEL_NAME = "utt:heavy-task-coordinator:v1";
const HEAVY_TASK_LEASE_PREFIX = "utt_heavy_task_lease_v1:";

function delayMs(ms) {
  const wait = Math.max(0, Number(ms) || 0);
  return new Promise((resolve) => setTimeout(resolve, wait));
}

function defaultRetryableRequestError(error) {
  const status = Number(error?.response?.status ?? error?.status);
  if (!Number.isFinite(status)) return true;
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

/**
 * Run a request with bounded retries. The default schedule is 1s, 2s, then 5s.
 * Client/validation errors fail immediately; network, 408/425/429, and 5xx errors retry.
 */
export async function withBoundedRetry(
  fn,
  { delaysMs = DEFAULT_RETRY_DELAYS_MS, shouldRetry = defaultRetryableRequestError } = {}
) {
  if (typeof fn !== "function") throw new Error("withBoundedRetry requires a function");
  const delays = Array.isArray(delaysMs) ? delaysMs : DEFAULT_RETRY_DELAYS_MS;
  let lastError = null;

  for (let attempt = 0; attempt <= delays.length; attempt += 1) {
    try {
      return await fn({ attempt, maxAttempts: delays.length + 1 });
    } catch (error) {
      lastError = error;
      const canRetry = attempt < delays.length && (typeof shouldRetry !== "function" || shouldRetry(error, attempt));
      if (!canRetry) throw error;
      await delayMs(delays[attempt]);
    }
  }

  throw lastError || new Error("bounded retry failed");
}

const HEAVY_TASK_TAB_ID = (() => {
  const fallback = `tab-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  try {
    if (typeof window === "undefined" || !window.sessionStorage) return fallback;
    const key = "utt_heavy_task_tab_id_v1";
    const current = String(window.sessionStorage.getItem(key) || "").trim();
    if (current) return current;
    const next = typeof globalThis.crypto?.randomUUID === "function" ? globalThis.crypto.randomUUID() : fallback;
    window.sessionStorage.setItem(key, next);
    return next;
  } catch {
    return fallback;
  }
})();

let _heavyTaskChannel = null;
function getHeavyTaskChannel() {
  try {
    if (typeof BroadcastChannel === "undefined") return null;
    if (!_heavyTaskChannel) _heavyTaskChannel = new BroadcastChannel(HEAVY_TASK_CHANNEL_NAME);
    return _heavyTaskChannel;
  } catch {
    return null;
  }
}

function broadcastHeavyTask(message) {
  try {
    getHeavyTaskChannel()?.postMessage?.({ ...message, tabId: HEAVY_TASK_TAB_ID, at: Date.now() });
  } catch {
    // coordination is best-effort; localStorage/navigator.locks remain available
  }
}

function heavyTaskLeaseKey(taskKey) {
  return `${HEAVY_TASK_LEASE_PREFIX}${String(taskKey || "default").trim().toLowerCase()}`;
}

function readHeavyTaskLease(taskKey) {
  try {
    if (typeof window === "undefined" || !window.localStorage) return null;
    const raw = window.localStorage.getItem(heavyTaskLeaseKey(taskKey));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const expiresAt = Number(parsed?.expiresAt || 0);
    if (!parsed?.owner || !Number.isFinite(expiresAt)) return null;
    return { owner: String(parsed.owner), expiresAt };
  } catch {
    return null;
  }
}

function tryAcquireHeavyTaskLease(taskKey, leaseMs) {
  try {
    if (typeof window === "undefined" || !window.localStorage) return false;
    const now = Date.now();
    const current = readHeavyTaskLease(taskKey);
    if (current && current.owner !== HEAVY_TASK_TAB_ID && current.expiresAt > now) return false;
    const candidate = { owner: HEAVY_TASK_TAB_ID, expiresAt: now + Math.max(5000, Number(leaseMs) || 120000) };
    window.localStorage.setItem(heavyTaskLeaseKey(taskKey), JSON.stringify(candidate));
    const confirmed = readHeavyTaskLease(taskKey);
    return confirmed?.owner === HEAVY_TASK_TAB_ID;
  } catch {
    return false;
  }
}

function releaseHeavyTaskLease(taskKey) {
  try {
    if (typeof window === "undefined" || !window.localStorage) return;
    const current = readHeavyTaskLease(taskKey);
    if (current?.owner === HEAVY_TASK_TAB_ID) window.localStorage.removeItem(heavyTaskLeaseKey(taskKey));
  } catch {
    // ignore lease cleanup failures; expiry remains the final guard
  }
}

async function waitForHeavyTaskCompletion(taskKey, waitMs) {
  const normalized = String(taskKey || "default").trim().toLowerCase();
  const timeoutMs = Math.max(0, Number(waitMs) || 0);
  if (timeoutMs <= 0) return;

  await new Promise((resolve) => {
    let settled = false;
    let timer = null;
    let poll = null;
    const channel = getHeavyTaskChannel();

    const done = () => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      if (poll) clearInterval(poll);
      try { channel?.removeEventListener?.("message", onMessage); } catch {}
      try { if (typeof window !== "undefined") window.removeEventListener("storage", onStorage); } catch {}
      resolve();
    };

    const onMessage = (event) => {
      const data = event?.data || {};
      if (String(data?.taskKey || "").trim().toLowerCase() !== normalized) return;
      if (data?.state === "complete" || data?.state === "failed") done();
    };

    const onStorage = (event) => {
      if (event?.key === heavyTaskLeaseKey(normalized) && !event?.newValue) done();
    };

    try { channel?.addEventListener?.("message", onMessage); } catch {}
    try { if (typeof window !== "undefined") window.addEventListener("storage", onStorage); } catch {}
    poll = setInterval(() => {
      const lease = readHeavyTaskLease(normalized);
      if (!lease || lease.expiresAt <= Date.now()) done();
    }, 250);
    timer = setTimeout(done, timeoutMs);
  });
}

/**
 * Run expensive refresh work in only one browser tab at a time.
 * Followers wait for the leader's completion signal, then continue by reading snapshots.
 */
export async function runCrossTabHeavyTask(
  taskKey,
  fn,
  { leaseMs = 180000, waitMs = 180000 } = {}
) {
  if (typeof fn !== "function") throw new Error("runCrossTabHeavyTask requires a function");
  const normalized = String(taskKey || "default").trim().toLowerCase();
  const startedAt = Date.now();
  const lockName = `utt-heavy:${normalized}`;

  try {
    if (typeof navigator !== "undefined" && navigator?.locks?.request) {
      let acquired = false;
      let value = null;
      await navigator.locks.request(lockName, { mode: "exclusive", ifAvailable: true }, async (lock) => {
        if (!lock) return;
        acquired = true;
        tryAcquireHeavyTaskLease(normalized, leaseMs);
        const renewEveryMs = Math.max(2000, Math.floor(Math.max(5000, Number(leaseMs) || 180000) / 3));
        const renew = setInterval(() => tryAcquireHeavyTaskLease(normalized, leaseMs), renewEveryMs);
        broadcastHeavyTask({ taskKey: normalized, state: "started" });
        try {
          value = await fn();
          broadcastHeavyTask({ taskKey: normalized, state: "complete" });
        } catch (error) {
          broadcastHeavyTask({ taskKey: normalized, state: "failed" });
          throw error;
        } finally {
          clearInterval(renew);
          releaseHeavyTaskLease(normalized);
        }
      });
      if (acquired) return { acquired: true, value, waitedMs: Date.now() - startedAt };
      await waitForHeavyTaskCompletion(normalized, waitMs);
      return { acquired: false, value: null, waitedMs: Date.now() - startedAt };
    }
  } catch (error) {
    // A task error from the leader must remain visible. Lock API availability errors fall through.
    if (error && !String(error?.name || "").toLowerCase().includes("notsupported")) throw error;
  }

  const acquired = tryAcquireHeavyTaskLease(normalized, leaseMs);
  if (!acquired) {
    await waitForHeavyTaskCompletion(normalized, waitMs);
    return { acquired: false, value: null, waitedMs: Date.now() - startedAt };
  }

  const renewEveryMs = Math.max(2000, Math.floor(Math.max(5000, Number(leaseMs) || 180000) / 3));
  const renew = setInterval(() => {
    tryAcquireHeavyTaskLease(normalized, leaseMs);
  }, renewEveryMs);
  broadcastHeavyTask({ taskKey: normalized, state: "started" });

  try {
    const value = await fn();
    broadcastHeavyTask({ taskKey: normalized, state: "complete" });
    return { acquired: true, value, waitedMs: Date.now() - startedAt };
  } catch (error) {
    broadcastHeavyTask({ taskKey: normalized, state: "failed" });
    throw error;
  } finally {
    clearInterval(renew);
    releaseHeavyTaskLease(normalized);
  }
}

function normVenueId(v) {
  return String(v ?? "")
    .trim()
    .toLowerCase();
}

// ─────────────────────────────────────────────────────────────
// UI helpers (front-end only)
// ─────────────────────────────────────────────────────────────

/**
 * Calculate qty from total USD and limit price.
 * Floors to qty_decimals to avoid accidental overspend due to rounding up.
 *
 * Returns:
 * - "" if inputs are invalid or non-positive
 * - Otherwise a string formatted without grouping
 */
export function calcQtyFromTotalUsd({ total_usd, limit_price, qty_decimals = 8 } = {}) {
  const t = Number(total_usd);
  const p = Number(limit_price);
  const d = Number(qty_decimals);

  if (!Number.isFinite(t) || !Number.isFinite(p) || t <= 0 || p <= 0) return "";

  const dd = Number.isFinite(d) && d >= 0 && d <= 18 ? d : 8;
  const factor = 10 ** dd;

  const raw = t / p;
  const floored = Math.floor(raw * factor) / factor;

  if (!Number.isFinite(floored) || floored <= 0) return "";

  // avoid scientific notation; keep it stable for inputs
  return floored.toLocaleString(undefined, { useGrouping: false, maximumFractionDigits: 18 });
}

// ─────────────────────────────────────────────────────────────
// Balances
// ─────────────────────────────────────────────────────────────

const DEFAULT_BALANCES_TIMEOUT_MS = 60000;

// Internal implementation (so we can wrap with singleFlight without changing signature)
async function _refreshBalancesImpl(venue, { timeout_ms = DEFAULT_BALANCES_TIMEOUT_MS } = {}) {
  const res = await http.post(`/api/balances/refresh`, { venue }, { timeout: timeout_ms });
  return res.data;
}

/**
 * De-duped balances refresh.
 * - One in-flight refresh per venue key.
 * - Minimum interval guard to prevent rapid multi-window refresh bursts.
 */
export async function refreshBalances(venue, { timeout_ms = DEFAULT_BALANCES_TIMEOUT_MS } = {}) {
  const v = normVenueId(venue);
  const key = `refreshBalances:${v || "all"}`;

  return singleFlight(
    key,
    async () => _refreshBalancesImpl(venue, { timeout_ms }),
    { minIntervalMs: 10_000 } // start at most once per 10s per venue key
  );
}

export async function getLatestBalances({
  venue,
  sort = "asset:asc",
  with_prices = true,
  timeout_ms = DEFAULT_BALANCES_TIMEOUT_MS,
} = {}) {
  const res = await http.get(`/api/balances/latest`, {
    params: cleanParams({ venue, sort, with_prices }),
    timeout: timeout_ms,
  });
  return res.data;
}

// Optional helper: callers can use this to avoid throwing (handy for UI “keep last snapshot” logic)
export async function getLatestBalancesSafe(opts = {}) {
  try {
    const data = await getLatestBalances(opts);
    return { ok: true, data, error: null };
  } catch (e) {
    const msg =
      e?.response?.data?.detail ||
      e?.response?.data?.message ||
      e?.message ||
      "balances request failed";
    return { ok: false, data: null, error: msg };
  }
}

/**
 * Read the newest Wallet Addresses balance snapshot rows.
 * Optional network / wallet_id / owner_scope filters keep on-chain portfolio
 * views bounded without routing them through the CEX adapter balance endpoint.
 */
export async function getWalletAddressBalancesLatest({
  with_prices = true,
  limit = 5000,
  network,
  wallet_id,
  owner_scope,
  timeout_ms = DEFAULT_BALANCES_TIMEOUT_MS,
} = {}) {
  const res = await http.get(`/api/wallet_addresses/balances/latest`, {
    params: cleanParams({
      with_prices: with_prices ? 1 : 0,
      limit,
      network,
      wallet_id,
      owner_scope,
    }),
    timeout: timeout_ms,
  });
  return res.data;
}

export async function listWalletAddresses({ asset, network, wallet_id, limit = 500 } = {}) {
  const res = await http.get(`/api/wallet_addresses`, {
    params: cleanParams({ asset, network, wallet_id, limit }),
  });
  return res.data;
}

export async function refreshWalletAddressBalances({
  ids,
  owner_scope = "user",
  timeout_ms = DEFAULT_BALANCES_TIMEOUT_MS,
} = {}) {
  const body = { owner_scope };
  if (Array.isArray(ids) && ids.length) body.ids = ids;
  const res = await http.post(`/api/wallet_addresses/balances/refresh`, body, { timeout: timeout_ms });
  return res.data;
}

export async function getTokenRegistryRows({
  chain,
  venue,
  include_global = true,
} = {}) {
  const res = await http.get(`/api/token_registry`, {
    params: cleanParams({
      chain,
      venue,
      include_global: include_global ? 1 : 0,
    }),
  });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Market
// ─────────────────────────────────────────────────────────────

/**
 * Orderbook fetch.
 * - Default: cached server-side behavior (no force).
 * - If force=true: caller is explicitly requesting a live fetch server-side.
 *
 * Note: _ts is only attached on force to defeat any proxy/browser caching.
 */
export async function getOrderbook({ venue, symbol, depth = 25, force = false } = {}) {
  const params = force
    ? cleanParams({ venue, symbol, depth, force: true, _ts: Date.now() })
    : cleanParams({ venue, symbol, depth });

  const res = await http.get(`/api/market/orderbook`, { params });
  return res.data;
}

/**
 * assets: array like ["USD","USDT","BTC","ETH","ALI"]
 * sent as CSV to keep query simple
 */
export async function getRobinhoodChainQuoteStatus({ apiBase, timeout_ms = 30000 } = {}) {
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.get(`/api/robinhood_chain/quotes/status`, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.get(`${base}/api/robinhood_chain/quotes/status`, { timeout: timeout_ms });
  return res.data;
}

export async function getRobinhoodChainSyntheticOrderbook({
  symbol = "ETH-USDG",
  depth = 5,
  force_refresh = false,
  timeout_ms = 45000,
} = {}) {
  const res = await http.get(`/api/robinhood_chain/orderbook`, {
    params: cleanParams({ symbol, depth, force_refresh }),
    timeout: timeout_ms,
  });
  return res.data;
}

export async function getRobinhoodChainIndicativeQuote(payload = {}, { apiBase, timeout_ms = 30000 } = {}) {
  const body = {
    provider: "0x",
    symbol: "ETH-USDG",
    ...payload,
  };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/quotes/indicative`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/quotes/indicative`, body, { timeout: timeout_ms });
  return res.data;
}

export async function getRobinhoodChainFirmQuotePlan(payload = {}, { apiBase, timeout_ms = 30000 } = {}) {
  const body = {
    provider: "0x",
    symbol: "ETH-USDG",
    slippage_bps: 100,
    ...payload,
  };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/quotes/firm-plan`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/quotes/firm-plan`, body, { timeout: timeout_ms });
  return res.data;
}

export async function getRobinhoodChainExecutionStatus({ apiBase, timeout_ms = 30000 } = {}) {
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.get(`/api/robinhood_chain/execution/status`, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.get(`${base}/api/robinhood_chain/execution/status`, { timeout: timeout_ms });
  return res.data;
}

export async function prepareRobinhoodChainExecution(payload = {}, { apiBase, timeout_ms = 45000 } = {}) {
  const body = {
    symbol: "ETH-USDG",
    side: "sell",
    quantity: "0.002",
    confirm_prepare: true,
    ...payload,
  };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/execution/prepare`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/execution/prepare`, body, { timeout: timeout_ms });
  return res.data;
}

export async function claimRobinhoodChainExecutionSend(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_send_claim: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/execution/${id}/claim-send`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/execution/${id}/claim-send`, body, { timeout: timeout_ms });
  return res.data;
}

export async function recordRobinhoodChainExecutionSubmission(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_record: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/execution/${id}/submission`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/execution/${id}/submission`, body, { timeout: timeout_ms });
  return res.data;
}

export async function recordRobinhoodChainExecutionSubmissionFailure(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_failure: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/execution/${id}/submission-failure`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/execution/${id}/submission-failure`, body, { timeout: timeout_ms });
  return res.data;
}

export async function refreshRobinhoodChainExecution(
  executionId,
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/execution/${id}/refresh`, null, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/execution/${id}/refresh`, null, { timeout: timeout_ms });
  return res.data;
}

export async function getRobinhoodChainExecution(
  executionId,
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.get(`/api/robinhood_chain/execution/${id}`, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.get(`${base}/api/robinhood_chain/execution/${id}`, { timeout: timeout_ms });
  return res.data;
}
export async function getRobinhoodChainSwapExecutionStatus({ apiBase, timeout_ms = 30000 } = {}) {
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.get(`/api/robinhood_chain/swap-execution/status`, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.get(`${base}/api/robinhood_chain/swap-execution/status`, { timeout: timeout_ms });
  return res.data;
}

export async function prepareRobinhoodChainSwapExecution(payload = {}, { apiBase, timeout_ms = 60000 } = {}) {
  const body = {
    from_asset: "USDG",
    to_asset: "ETH",
    amount_mode: "exact_spend",
    exact_input_amount: "2",
    slippage_bps: 100,
    confirm_prepare: true,
    ...payload,
  };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/swap-execution/prepare`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/swap-execution/prepare`, body, { timeout: timeout_ms });
  return res.data;
}

export async function getRobinhoodChainSwapExecution(
  executionId,
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.get(`/api/robinhood_chain/swap-execution/${id}`, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.get(`${base}/api/robinhood_chain/swap-execution/${id}`, { timeout: timeout_ms });
  return res.data;
}

export async function getRobinhoodChainBuyExecutionStatus({ apiBase, timeout_ms = 30000 } = {}) {
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.get(`/api/robinhood_chain/buy-execution/status`, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.get(`${base}/api/robinhood_chain/buy-execution/status`, { timeout: timeout_ms });
  return res.data;
}

export async function prepareRobinhoodChainBuyApproval(payload = {}, { apiBase, timeout_ms = 60000 } = {}) {
  const body = {
    symbol: "ETH-USDG",
    side: "buy",
    exact_output_quantity: "0.001",
    maximum_total_quote: "2",
    approval_amount: "2",
    slippage_bps: 100,
    confirm_prepare: true,
    ...payload,
  };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/prepare-approval`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/prepare-approval`, body, { timeout: timeout_ms });
  return res.data;
}

export async function getRobinhoodChainBuyExecution(
  executionId,
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.get(`/api/robinhood_chain/buy-execution/${id}`, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.get(`${base}/api/robinhood_chain/buy-execution/${id}`, { timeout: timeout_ms });
  return res.data;
}

export async function claimRobinhoodChainBuyApprovalSend(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_send_claim: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/approval/claim-send`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/approval/claim-send`, body, { timeout: timeout_ms });
  return res.data;
}

export async function recordRobinhoodChainBuyApprovalSubmission(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_record: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/approval/submission`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/approval/submission`, body, { timeout: timeout_ms });
  return res.data;
}

export async function recordRobinhoodChainBuyApprovalSubmissionFailure(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_failure: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/approval/submission-failure`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/approval/submission-failure`, body, { timeout: timeout_ms });
  return res.data;
}

export async function refreshRobinhoodChainBuyApproval(
  executionId,
  { apiBase, timeout_ms = 45000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/approval/refresh`, null, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/approval/refresh`, null, { timeout: timeout_ms });
  return res.data;
}

export async function prepareRobinhoodChainBuySwap(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 60000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_prepare: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/prepare-swap`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/prepare-swap`, body, { timeout: timeout_ms });
  return res.data;
}

export async function claimRobinhoodChainBuySwapSend(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_send_claim: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/swap/claim-send`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/swap/claim-send`, body, { timeout: timeout_ms });
  return res.data;
}

export async function recordRobinhoodChainBuySwapSubmission(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_record: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/swap/submission`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/swap/submission`, body, { timeout: timeout_ms });
  return res.data;
}

export async function recordRobinhoodChainBuySwapSubmissionFailure(
  executionId,
  payload = {},
  { apiBase, timeout_ms = 30000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const body = { confirm_failure: true, ...payload };
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/swap/submission-failure`, body, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/swap/submission-failure`, body, { timeout: timeout_ms });
  return res.data;
}

export async function refreshRobinhoodChainBuySwap(
  executionId,
  { apiBase, timeout_ms = 45000 } = {}
) {
  const id = encodeURIComponent(String(executionId || "").trim());
  if (!id) throw new Error("executionId is required");
  const base = String(apiBase || API_BASE).replace(/\/$/, "");
  if (base === API_BASE) {
    const res = await http.post(`/api/robinhood_chain/buy-execution/${id}/swap/refresh`, null, { timeout: timeout_ms });
    return res.data;
  }
  const res = await axios.post(`${base}/api/robinhood_chain/buy-execution/${id}/swap/refresh`, null, { timeout: timeout_ms });
  return res.data;
}

export async function getPricesUSD({ venue, assets } = {}) {
  const assetsCsv = Array.isArray(assets) ? assets.filter(Boolean).join(",") : assets;
  const res = await http.get(`/api/market/prices_usd`, {
    params: cleanParams({ venue, assets: assetsCsv }),
  });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Venue Rules (OrderTicket guardrails)
// ─────────────────────────────────────────────────────────────

/**
 * Fetch order rules for {venue, symbol} (canonical symbol).
 * GET /api/rules/order?venue=...&symbol=...
 */
export async function getOrderRules({ venue, symbol } = {}) {
  const v = String(venue || "").trim().toLowerCase();
  const s = String(symbol || "").trim();
  if (!v) throw new Error("venue is required");
  if (!s) throw new Error("symbol is required");

  const res = await http.get(`/api/rules/order`, {
    params: cleanParams({ venue: v, symbol: s }),
  });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Discover (Symbols)
// ─────────────────────────────────────────────────────────────

/**
 * Get discovery-capable venues enabled on backend.
 * GET /api/symbols/venues
 */
export async function getDiscoveryVenues() {
  const res = await http.get(`/api/symbols/venues`);
  return res.data; // {venues:[...]} (or possibly raw array in older code)
}

/**
 * Canonical venue registry (enabled/supported venues).
 * GET /api/venues
 *
 * Current canonical wire shape (treat as v1):
 * - [
 *     {
 *       venue: "coinbase",
 *       display_name: "Coinbase",
 *       enabled: true,
 *       supports: { trading:true, balances:true, orderbook:true, markets:true }
 *     },
 *     ...
 *   ]
 *
 * Compatibility (tolerated):
 * - { venues: [...] } (wrapper)
 * - ["gemini","kraken", ...] (legacy)
 */
export function normalizeVenue(v) {
  const venue = String(v?.venue ?? "").trim();
  const display_name = String(v?.display_name ?? v?.displayName ?? v?.venue ?? "").trim();

  return {
    venue,
    display_name: display_name || venue,
    enabled: Boolean(v?.enabled),
    supports: {
      trading: Boolean(v?.supports?.trading),
      balances: Boolean(v?.supports?.balances),
      orderbook: Boolean(v?.supports?.orderbook),
      markets: Boolean(v?.supports?.markets),
    },
  };
}

export function normalizeVenues(payload) {
  // Preferred: raw array of venue objects
  let arr = payload;

  // Optional wrapper: { venues: [...] }
  if (arr && typeof arr === "object" && !Array.isArray(arr) && Array.isArray(arr.venues)) {
    arr = arr.venues;
  }

  // Legacy: ["gemini","kraken", ...]
  if (Array.isArray(arr) && arr.length > 0 && typeof arr[0] === "string") {
    const uniq = [...new Set(arr.map((s) => String(s || "").trim()).filter(Boolean))];
    return uniq.map((venue) => ({
      venue,
      display_name: venue,
      enabled: true,
      // Legacy response had no capability info; treat as “supported” to avoid breaking older servers.
      supports: { trading: true, balances: true, orderbook: true, markets: true },
    }));
  }

  const list = Array.isArray(arr) ? arr : [];
  return list
    .map(normalizeVenue)
    .map((v) => ({
      ...v,
      venue: String(v.venue || "").trim().toLowerCase(), // normalize venue ids for internal usage
      display_name: String(v.display_name || "").trim(),
      supports: {
        trading: Boolean(v.supports?.trading),
        balances: Boolean(v.supports?.balances),
        orderbook: Boolean(v.supports?.orderbook),
        markets: Boolean(v.supports?.markets),
      },
    }))
    .filter((v) => v.venue.length > 0);
}

// ─────────────────────────────────────────────────────────────
// Venues (Registry)
// ─────────────────────────────────────────────────────────────

/**
 * Authoritative venue list (raw wire shape).
 * GET /api/venues?include_disabled=...
 *
 * Returns backend list response (do NOT normalize here):
 * - [
 *     { venue, display_name, enabled, supports: {...} },
 *     ...
 *   ]
 */
export async function getVenuesRaw({ include_disabled = true } = {}) {
  const res = await http.get(`/api/venues`, {
    params: cleanParams({ include_disabled }),
  });
  return res.data; // raw list (or wrapper, depending on backend)
}

/**
 * SAFE raw venues fetch used by App.jsx.
 *
 * Requirements:
 * - Calls GET /api/venues
 * - Passes include_disabled=true as query param (when requested)
 * - Returns parsed JSON WITHOUT coercing its shape (could be array or wrapper object)
 */
export async function getVenuesRawSafe({ include_disabled = true } = {}) {
  try {
    const qs = new URLSearchParams();
    if (include_disabled) qs.set("include_disabled", "true");

    const url = `${API_BASE}/api/venues${qs.toString() ? `?${qs.toString()}` : ""}`;

    const r = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });

    if (!r.ok) return [];
    return await r.json(); // IMPORTANT: do not coerce shape here
  } catch {
    return [];
  }
}

/**
 * Normalized venue list (current behavior, preserved).
 * Note: no params historically; left intact to avoid breaking callers.
 */
export async function getVenues() {
  const res = await http.get(`/api/venues`);
  return normalizeVenues(res.data);
}

/** Optional helper: never throw (useful for UI fallback logic) */
export async function getVenuesSafe() {
  try {
    const data = await getVenues();
    return { ok: true, data, error: null };
  } catch (e) {
    const msg =
      e?.response?.data?.detail ||
      e?.response?.data?.message ||
      e?.message ||
      "venues request failed";
    return { ok: false, data: null, error: msg };
  }
}

/**
 * Create a new snapshot of tradable symbols for a venue.
 * POST /api/symbols/refresh
 *
 * Compatibility:
 * - preferred: POST /api/symbols/refresh?venue=gemini
 * - also supports: POST /api/symbols/refresh {venue:"gemini"}
 * - legacy fallback: POST /api/symbol_discovery/refresh {venue:"gemini"}
 */
async function _refreshSymbolsImpl(venue) {
  const v = String(venue || "").trim().toLowerCase();
  if (!v) throw new Error("venue is required");

  // 1) query-param style (matches your frontend usage historically)
  try {
    const res = await http.post(`/api/symbols/refresh`, null, { params: cleanParams({ venue: v }) });
    return res.data;
  } catch (e1) {
    // 2) JSON body style
    try {
      const res = await http.post(`/api/symbols/refresh`, { venue: v });
      return res.data;
    } catch (e2) {
      // 3) legacy fallback
      try {
        const res = await http.post(`/api/symbol_discovery/refresh`, { venue: v });
        return res.data;
      } catch {
        // rethrow the "most informative" error
        throw e2?.response ? e2 : e1;
      }
    }
  }
}

export async function refreshSymbols(venue) {
  const v = normVenueId(venue);
  const key = `refreshSymbols:${v || "all"}`;

  return singleFlight(
    key,
    async () => _refreshSymbolsImpl(venue),
    { minIntervalMs: 30_000 } // discovery refresh can be heavier; avoid bursts
  );
}

/**
 * Get latest symbol snapshot for a venue.
 * GET /api/symbols/latest?venue=...
 */
export async function getLatestSymbols({ venue } = {}) {
  const res = await http.get(`/api/symbols/latest`, { params: cleanParams({ venue }) });
  return res.data;
}

/**
 * Get new listings since baseline snapshot for a venue.
 * GET /api/symbols/new?venue=...&days=...
 *
 * days:
 * - optional integer
 * - backend should interpret as "first_seen_at >= now - days"
 */
export async function getNewSymbols({ venue, days } = {}) {
  const res = await http.get(`/api/symbols/new`, { params: cleanParams({ venue, days }) });
  return res.data;
}

/**
 * Get new listings that are NOT held above EPS.
 * GET /api/symbols/unheld_new?venue=...&eps=...&days=...
 *
 * days:
 * - optional integer
 * - backend should interpret as "first_seen_at >= now - days"
 */
export async function getUnheldNewSymbols({ venue, eps, days } = {}) {
  const res = await http.get(`/api/symbols/unheld_new`, {
    params: cleanParams({ venue, eps, days }),
  });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Symbol Views (Discovery “viewed/confirmed”)
// ─────────────────────────────────────────────────────────────

/**
 * GET /api/symbols/views?venue=...
 * Compatibility: if backend returns {items:[...]} or {views:[...]} or raw array, caller normalizes.
 */
export async function listSymbolViews({ venue } = {}) {
  const v = String(venue || "").trim().toLowerCase();
  if (!v) throw new Error("venue is required");
  const res = await http.get(`/api/symbols/views`, { params: cleanParams({ venue: v }) });
  return res.data;
}

/**
 * POST /api/symbols/view
 * body: { view_key, viewed_confirmed }
 */
export async function confirmSymbolView({ view_key, viewed_confirmed } = {}) {
  const key = String(view_key || "").trim();
  if (!key) throw new Error("view_key is required");
  const res = await http.post(`/api/symbols/view`, { view_key: key, viewed_confirmed: !!viewed_confirmed });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Trade
// (Helper wrappers so UI can migrate off raw fetch() when ready)
// ─────────────────────────────────────────────────────────────
export async function submitOrder(payload = {}) {
  const res = await http.post(`/api/trade/order`, payload);
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Safety (ARM / DRY_RUN status)
// ─────────────────────────────────────────────────────────────
export async function getSafetyStatus() {
  const res = await http.get(`/api/arm`);
  return res.data; // { dry_run: bool, armed: bool }
}

export async function setArmed(armed) {
  const res = await http.post(`/api/arm`, { armed: !!armed });
  return res.data; // { dry_run: bool, armed: bool }
}

// Limit-only convenience wrapper
export async function submitLimitOrder({
  venue,
  symbol,
  side,
  qty,
  limit_price,
  tif = "gtc",
  post_only = false,
  client_order_id,
} = {}) {
  const res = await http.post(`/api/trade/order`, {
    venue,
    symbol,
    side,
    type: "limit",
    qty,
    limit_price,
    tif,
    post_only,
    client_order_id,
  });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Local Orders
// ─────────────────────────────────────────────────────────────
export async function getOrders({
  venue,
  sort = "created_at:desc",
  status,
  page = 1,
  page_size = 100,
} = {}) {
  const res = await http.get(`/api/orders`, {
    params: cleanParams({ venue, sort, status, page, page_size }),
  });
  return res.data;
}

export async function cancelOrder(orderId) {
  const res = await http.delete(`/api/orders/${orderId}`);
  return res.data;
}

// Unified cancel-by-ref (UI All Orders cancel button)
export async function cancelOrderByRef(cancel_ref) {
  const ref = typeof cancel_ref === "object" ? String(cancel_ref?.cancel_ref || "") : String(cancel_ref || "");
  const r = ref.trim();
  if (!r) throw new Error("cancel_ref is required");
  const res = await http.post(`/api/orders/cancel_by_ref`, { cancel_ref: r });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Venue Orders (Ingestion)
// ─────────────────────────────────────────────────────────────

// IMPORTANT: omit `venue` from the POST body unless it's non-empty.
// Also: tolerate existing call patterns: refreshVenueOrders(v), refreshVenueOrders(v, true),
//       refreshVenueOrders(v, { force: true })

async function _refreshVenueOrdersImpl(venue = "", forceOrOpts = false) {
  const force = typeof forceOrOpts === "boolean" ? forceOrOpts : !!forceOrOpts?.force;

  const qs = new URLSearchParams();
  qs.set("force", force ? "true" : "false");

  const v = String(venue ?? "").trim();

  // Only include venue if non-empty
  const bodyObj = {};
  if (v) bodyObj.venue = v;

  // If body is empty, send no payload at all (ideal)
  const hasBody = Object.keys(bodyObj).length > 0;

  const url = `${API_BASE}/api/venue_orders/refresh?${qs.toString()}`;

  const res = await fetch(url, {
    method: "POST",
    headers: hasBody ? { "Content-Type": "application/json" } : undefined,
    body: hasBody ? JSON.stringify(bodyObj) : undefined,
  });

  if (!res.ok) {
    let detail = "";
    try {
      const j = await res.json();
      detail = j?.detail ? `: ${j.detail}` : "";
    } catch {
      // ignore JSON parse errors
    }
    throw new Error(`refreshVenueOrders failed (${res.status})${detail}`);
  }

  // return JSON if present, otherwise return a minimal ok object
  try {
    return await res.json();
  } catch {
    return { ok: true };
  }
}

export async function refreshVenueOrders(venue = "", forceOrOpts = false) {
  const force = typeof forceOrOpts === "boolean" ? forceOrOpts : !!forceOrOpts?.force;
  const v = normVenueId(venue);
  const key = `refreshVenueOrders:${v || "all"}:${force ? "force" : "noforce"}`;

  return singleFlight(
    key,
    async () => _refreshVenueOrdersImpl(venue, forceOrOpts),
    { minIntervalMs: 10_000 } // prevent multi-window bursts
  );
}

// ─────────────────────────────────────────────────────────────
// All Orders (Unified)
// ─────────────────────────────────────────────────────────────
export async function getAllOrders({
  // Backward/forward compat:
  // - older backend: expects "source"
  // - newer design A: expects "scope"
  scope,
  source,

  venue,
  symbol,
  status,
  status_bucket,
  from,
  to,
  sort = "created_at:desc",
  page = 1,
  page_size = 50,
} = {}) {
  // If caller uses scope, also populate source for compatibility.
  const effectiveSource = source ?? scope;

  const res = await http.get(`/api/all_orders`, {
    params: cleanParams({
      scope,
      source: effectiveSource,
      venue,
      symbol,
      status,
      status_bucket,
      from,
      to,
      sort,
      page,
      page_size,
    }),
  });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Order Views (viewed/confirmed flag) — Orders (not Symbols)
// ─────────────────────────────────────────────────────────────

/**
 * POST /api/order_views/confirm
 * body: { view_key, viewed_confirmed }
 */
export async function confirmOrderView({ view_key, viewed_confirmed } = {}) {
  const res = await http.post(`/api/order_views/confirm`, { view_key, viewed_confirmed: !!viewed_confirmed });
  return res.data;
}

/**
 * GET /api/order_views
 */
export async function listOrderViews({
  view_key,
  confirmed,
  sort = "updated_at:desc",
  page = 1,
  page_size = 50,
} = {}) {
  const res = await http.get(`/api/order_views`, {
    params: cleanParams({ view_key, confirmed, sort, page, page_size }),
  });
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// Kraken: enforce min order sizes + price decimal precision
// ─────────────────────────────────────────────────────────────
const KRAKEN_PUBLIC_BASE = "https://api.kraken.com";

const krakenPublic = axios.create({
  baseURL: KRAKEN_PUBLIC_BASE,
  timeout: 20000,
});

function normalizeKrakenPairKey(sym) {
  return String(sym || "").trim().toUpperCase().replace(/\s+/g, "");
}

function normalizeWsname(sym) {
  const s = String(sym || "").trim().toUpperCase();
  if (!s) return "";
  if (s.includes("/")) return s;
  if (s.includes("-")) return s.replace("-", "/");
  return s;
}

export async function getKrakenAssetPairsRaw({ pair } = {}) {
  const res = await krakenPublic.get(`/0/public/AssetPairs`, {
    params: cleanParams({ pair }),
  });
  return res.data;
}

export async function getKrakenPairConstraints({ symbol } = {}) {
  const sym = String(symbol || "").trim();
  if (!sym) return null;

  const keyWanted = normalizeKrakenPairKey(sym);
  const wsWanted = normalizeWsname(sym);

  const raw = await getKrakenAssetPairsRaw();
  const result = raw?.result && typeof raw.result === "object" ? raw.result : null;
  if (!result) return null;

  let foundKey = null;
  let foundVal = null;

  for (const [k, v] of Object.entries(result)) {
    if (!v || typeof v !== "object") continue;

    const ws = String(v.wsname || "").toUpperCase();
    const alt = String(v.altname || "").toUpperCase();
    const kk = String(k || "").toUpperCase();

    if (kk === keyWanted) {
      foundKey = k;
      foundVal = v;
      break;
    }

    if (ws && wsWanted && ws === wsWanted) {
      foundKey = k;
      foundVal = v;
      break;
    }

    const altWanted = keyWanted.replace("-", "").replace("/", "");
    if (alt && altWanted && alt === altWanted) {
      foundKey = k;
      foundVal = v;
      break;
    }
  }

  if (!foundVal) return null;

  const pair_decimals = Number(foundVal.pair_decimals);
  const lot_decimals = Number(foundVal.lot_decimals);
  const ordermin = foundVal.ordermin !== undefined ? Number(foundVal.ordermin) : null;

  return {
    venue: "kraken",
    symbol,
    pair_key: foundKey,
    wsname: foundVal.wsname || null,
    altname: foundVal.altname || null,
    pair_decimals: Number.isFinite(pair_decimals) ? pair_decimals : null,
    lot_decimals: Number.isFinite(lot_decimals) ? lot_decimals : null,
    ordermin: Number.isFinite(ordermin) ? ordermin : null,
  };
}

export function truncateToDecimals(value, decimals) {
  const x = Number(value);
  const d = Number(decimals);
  if (!Number.isFinite(x) || !Number.isFinite(d) || d < 0) return value;
  const factor = 10 ** d;
  return Math.trunc(x * factor) / factor;
}

// ─────────────────────────────────────────────────────────────
// Robinhood (planned)
// ─────────────────────────────────────────────────────────────
export async function robinhoodGetPositions() {
  const res = await http.get(`/api/robinhood/positions`);
  return res.data;
}

export async function robinhoodSubmitOrder(payload = {}) {
  const res = await http.post(`/api/robinhood/order`, payload);
  return res.data;
}

// ─────────────────────────────────────────────────────────────
// DEX trade (planned; after Robinhood)
// ─────────────────────────────────────────────────────────────
export async function dexGetQuote(params = {}) {
  const res = await http.get(`/api/dex/quote`, { params: cleanParams(params) });
  return res.data;
}

export async function dexSubmitSwap(payload = {}) {
  const res = await http.post(`/api/dex/swap`, payload);
  return res.data;
}

// --- Arb helpers (append to frontend/src/lib/api.js) ---

function parseTopOfBookSide(side) {
  // side can be:
  // - array of arrays: [[price, size], ...]
  // - array of objects: [{ price, size }, ...]
  // Return best price as Number or null
  if (!Array.isArray(side) || side.length === 0) return null;

  const first = side[0];
  if (Array.isArray(first)) {
    const p = Number(first[0]);
    return Number.isFinite(p) ? p : null;
  }
  if (first && typeof first === "object") {
    const p = Number(first.price ?? first.px ?? first[0]);
    return Number.isFinite(p) ? p : null;
  }
  return null;
}

export async function getTopOfBook({ apiBase, venue, symbol, force = false } = {}) {
  if (!apiBase) throw new Error("apiBase not set");
  const v = String(venue || "").trim().toLowerCase();
  const s = String(symbol || "").trim();
  if (!v || !s) throw new Error("venue/symbol required");

  const url = new URL(`${apiBase}/api/market/orderbook`);
  url.searchParams.set("venue", v);
  url.searchParams.set("symbol", s);
  url.searchParams.set("depth", "1");

  if (force) url.searchParams.set("force", "true");

  url.searchParams.set("_ts", String(Date.now()));

  const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
  if (!r.ok) {
    const txt = await r.text();
    throw new Error(txt || `HTTP ${r.status}`);
  }
  const j = await r.json();

  const ask = parseTopOfBookSide(j?.asks);
  const bid = parseTopOfBookSide(j?.bids);

  return { venue: v, ask, bid, raw: j };
}

export async function getArbSnapshot({ apiBase, symbol, venues }) {
  const vList = Array.isArray(venues) ? venues : [];
  const results = await Promise.allSettled(vList.map((v) => getTopOfBook({ apiBase, venue: v, symbol })));

  const perVenue = [];
  for (const res of results) {
    if (res.status === "fulfilled") {
      perVenue.push({ venue: res.value.venue, ask: res.value.ask, bid: res.value.bid });
    } else {
      const msg = res.reason?.message || "error";
      perVenue.push({ venue: "unknown", ask: null, bid: null, error: msg });
    }
  }

  let bestAsk = null;
  let bestBid = null;

  for (const r of perVenue) {
    if (Number.isFinite(r.ask)) {
      if (!bestAsk || r.ask < bestAsk.price) bestAsk = { venue: r.venue, price: r.ask };
    }
    if (Number.isFinite(r.bid)) {
      if (!bestBid || r.bid > bestBid.price) bestBid = { venue: r.venue, price: r.bid };
    }
  }

  return {
    symbol,
    bestAsk,
    bestBid,
    perVenue,
    ts: Date.now(),
  };
}
