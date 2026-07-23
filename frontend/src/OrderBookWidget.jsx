import { useEffect, useMemo, useRef, useState } from "react";

const LS_OB_BOX = "utt_ob_box_v2";
const LS_OB_LOCK = "utt_ob_lock_v2";

// persist auto refresh + interval
const LS_OB_AUTO = "utt_ob_auto_v1";
const LS_OB_SEC = "utt_ob_sec_v1";
const LS_OB_SOL_ROUTER = "utt_ob_sol_router_v1";
const LS_OB_HYDRATION_ROUTE = "utt_ob_hydration_route_mode_v1";
const LS_OB_COUNTERPARTY_LIQUIDITY_FILTER = "utt_ob_counterparty_liquidity_filter_v1";
const LS_OT_COUNTERPARTY_EXECUTION_MODE = "utt_counterparty_execution_mode_v1";
const COUNTERPARTY_ORDERBOOK_PICK_EVENT = "utt:counterparty-orderbook-pick";
const COUNTERPARTY_EXECUTION_MODE_EVENT = "utt:counterparty-execution-mode";
const ROBINHOOD_CHAIN_ORDERBOOK_PICK_EVENT = "utt:robinhood-chain-orderbook-pick";
const MARKET_METRICS_BROWSER_CACHE_KEY = "utt.market_metrics.summary.v10";
const MARKET_METRICS_BROWSER_CACHE_EVENT = "utt:market-metrics-summary-v10";
const ORDERBOOK_QUOTE_USD_STALE_MS = 15 * 60 * 1000;
const COUNTERPARTY_RATE_LIMIT_BASE_BACKOFF_MS = 15 * 1000;
const COUNTERPARTY_RATE_LIMIT_MAX_BACKOFF_MS = 5 * 60 * 1000;
const COUNTERPARTY_RATE_LIMIT_JITTER_RATIO = 0.20;

const USD_VALUE_QUOTES = new Set([
  "USD", "USDT", "USDC", "DAI", "FDUSD", "PYUSD", "GUSD", "TUSD",
  "USDP", "USD1", "USDG", "HOLLAR", "BUSD", "USDD", "USDE", "RLUSD",
]);

function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function counterpartyRetryAfterMs(rawValue, nowMs = Date.now()) {
  const raw = String(rawValue ?? "").trim();
  if (!raw) return null;
  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds * 1000);
  const retryAt = Date.parse(raw);
  if (Number.isFinite(retryAt)) return Math.max(0, retryAt - nowMs);
  return null;
}

function counterpartyRetryAtMs(value) {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return n;
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function counterpartyCooldownText(ms) {
  const remaining = Math.max(0, Number(ms) || 0);
  const seconds = Math.ceil(remaining / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const extra = seconds % 60;
  return extra ? `${minutes}m ${extra}s` : `${minutes}m`;
}

function counterpartyOrderbookSnapshotKey(venue, symbol, depth) {
  return `${String(venue || "counterparty").trim().toLowerCase()}|${String(symbol || "").trim().toUpperCase()}|${Number(depth) || 25}`;
}

function normalizeOrderBookAsset(value) {
  const a = String(value || "").trim().toUpperCase();
  if (a === "XBT") return "BTC";
  if (a === "BCY" || a === "BITCRYSTAL") return "BITCRYSTALS";
  if (a === "WETH") return "ETH";
  if (a === "WSOL") return "SOL";
  return a;
}

function orderBookPairParts(symbol) {
  const raw = String(symbol || "").trim().toUpperCase().replace(/[\\/_]/g, "-");
  const parts = raw.split("-").map((x) => x.trim()).filter(Boolean);
  if (parts.length !== 2) return { base: "", quote: "", symbol: raw };
  const base = normalizeOrderBookAsset(parts[0]);
  const quote = normalizeOrderBookAsset(parts[1]);
  return { base, quote, symbol: base && quote ? `${base}-${quote}` : raw };
}

function isUsdValueQuote(asset) {
  return USD_VALUE_QUOTES.has(normalizeOrderBookAsset(asset));
}

function metricRowAsset(row) {
  const raw = String(row?.asset || row?.symbol || row?.pair || "").trim().toUpperCase();
  if (!raw) return "";
  const parts = raw.replace(/[\\/_]/g, "-").split("-").filter(Boolean);
  return normalizeOrderBookAsset(parts[0] || raw);
}

function metricRowIsUsdValue(row) {
  if (!row || typeof row !== "object") return false;
  if (row?.is_usd_quote === true || row?.is_stablecoin === true || row?.stablecoin === true) return true;
  const peg = String(row?.peg_currency || row?.peg || row?.stablecoin_peg || "").trim().toUpperCase();
  return peg === "USD";
}

function metricRowQuoteUsdContext(row, snapshot, quoteAsset) {
  if (!row || typeof row !== "object") return null;
  const quote = normalizeOrderBookAsset(quoteAsset);
  if (metricRowIsUsdValue(row)) {
    return {
      status: "native_usd",
      quoteAsset: quote,
      priceUsd: 1,
      source: String(row?.price_source || row?.market_data_source || "token_registry").trim(),
      updatedAt: row?.market_data_updated_at || row?.price_updated_at || row?.updated_at || snapshot?.lastUpdated || null,
      stale: false,
    };
  }
  const priceUsd = safeNum(row?.price_usd ?? row?.usd_price ?? row?.priceUsd);
  if (priceUsd === null || priceUsd <= 0) return null;
  const updatedAt = row?.market_data_updated_at || row?.price_updated_at || row?.updated_at || snapshot?.lastUpdated || null;
  const updatedMs = updatedAt ? Date.parse(updatedAt) : NaN;
  const stale = Number.isFinite(updatedMs) ? Date.now() - updatedMs > ORDERBOOK_QUOTE_USD_STALE_MS : false;
  return {
    status: "available",
    quoteAsset: normalizeOrderBookAsset(quoteAsset),
    priceUsd,
    source: String(
      row?.price_source ||
      row?.market_data_source ||
      row?.source ||
      row?.price_source_id ||
      "market_metrics"
    ).trim(),
    updatedAt,
    stale,
  };
}

function readSharedQuoteUsdContext(quoteAsset) {
  const quote = normalizeOrderBookAsset(quoteAsset);
  if (!quote) return null;
  if (isUsdValueQuote(quote)) {
    return { status: "native_usd", quoteAsset: quote, priceUsd: 1, source: "USD-valued quote", updatedAt: null, stale: false };
  }
  try {
    if (typeof window === "undefined" || !window.localStorage) return null;
    const raw = window.localStorage.getItem(MARKET_METRICS_BROWSER_CACHE_KEY);
    if (!raw) return null;
    const snapshot = JSON.parse(raw);
    const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
    const row = rows.find((item) => metricRowAsset(item) === quote);
    return metricRowQuoteUsdContext(row, snapshot, quote);
  } catch {
    return null;
  }
}

function formatOrderBookUsd(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "USD —";
  const abs = Math.abs(n);
  if (abs > 0 && abs < 0.00000001) return "<$0.00000001";
  if (abs >= 1000) return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  if (abs >= 1) return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 })}`;
  if (abs >= 0.01) return `$${n.toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 6 })}`;
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 8 })}`;
}

function normalizeCounterpartyLiquidityFilter(value) {
  const v = String(value || "all").trim().toLowerCase().replace(/-/g, "_");
  if (v === "dispenser" || v === "dispensers" || v === "disp") return "dispensers";
  if (v === "limit" || v === "limits" || v === "orders" || v === "limit_order") return "limit_orders";
  return "all";
}

function readCounterpartyLiquidityFilter() {
  try {
    return normalizeCounterpartyLiquidityFilter(localStorage.getItem(LS_OB_COUNTERPARTY_LIQUIDITY_FILTER) || "all");
  } catch {
    return "all";
  }
}

function counterpartyLiquidityFilterLabel(value) {
  const filter = normalizeCounterpartyLiquidityFilter(value);
  if (filter === "dispensers") return "Dispensers";
  if (filter === "limit_orders") return "Limit Orders";
  return "All Liquidity";
}

function normalizeCounterpartyExecutionMode(value) {
  const v = String(value || "dispenser").trim().toLowerCase().replace(/-/g, "_");
  if (v === "limit" || v === "order" || v === "protocol_order") return "limit_order";
  return v === "limit_order" ? "limit_order" : "dispenser";
}

function readCounterpartyExecutionMode() {
  try {
    return normalizeCounterpartyExecutionMode(localStorage.getItem(LS_OT_COUNTERPARTY_EXECUTION_MODE) || "dispenser");
  } catch {
    return "dispenser";
  }
}

function counterpartyLiquidityType(row) {
  const explicit = String(row?.liquidity_type || "").trim().toLowerCase();
  if (explicit === "dispenser" || explicit === "limit_order") return explicit;
  const sourceType = String(row?.source_type || "").trim().toLowerCase();
  if (sourceType === "counterparty_dispenser" || row?.raw_dispenser) return "dispenser";
  if (sourceType === "counterparty_order" || row?.raw_order) return "limit_order";
  return "unknown";
}

function counterpartyLiquidityLabel(row) {
  const type = counterpartyLiquidityType(row);
  if (type === "dispenser") return "DISP";
  if (type === "limit_order") return "LIMIT";
  return "UNKNOWN";
}

function counterpartyRowMatchesFilter(row, filter) {
  const f = normalizeCounterpartyLiquidityFilter(filter);
  if (f === "all") return true;
  const type = counterpartyLiquidityType(row);
  return f === "dispensers" ? type === "dispenser" : type === "limit_order";
}

function counterpartyLotSize(row) {
  const candidates = [
    row?.lot_size,
    row?.unit_size,
    row?.raw_dispenser?.give_quantity,
    row?.raw_dispenser?.dispense_quantity,
    row?.raw_dispenser?.unit_size,
  ];
  for (const value of candidates) {
    const n = safeNum(value);
    if (n !== null && n > 0) return n;
  }
  return null;
}

function counterpartyPlainDecimalText(value) {
  const raw = String(value ?? "").trim().replace(/^\+/, "");
  if (!raw) return "";

  const normalizePlain = (plain) => {
    const cleaned = String(plain || "").replace(/^0+(?=\d)/, "");
    const safe = cleaned.startsWith(".") ? `0${cleaned}` : cleaned || "0";
    if (!/^\d+(?:\.\d+)?$/.test(safe)) return "";
    if (!safe.includes(".")) return safe;
    const trimmed = safe.replace(/0+$/, "").replace(/\.$/, "");
    return trimmed || "0";
  };

  if (/^\d+(?:\.\d+)?$/.test(raw)) return normalizePlain(raw);

  const sci = /^(\d+)(?:\.(\d*))?[eE]([+-]?\d+)$/.exec(raw);
  if (sci) {
    const whole = sci[1] || "0";
    const fraction = sci[2] || "";
    const exponent = Number(sci[3]);
    if (!Number.isFinite(exponent)) return "";
    const digits = `${whole}${fraction}`;
    const decimalIndex = whole.length + exponent;
    const expanded = decimalIndex <= 0
      ? `0.${"0".repeat(Math.abs(decimalIndex))}${digits}`
      : decimalIndex >= digits.length
        ? `${digits}${"0".repeat(decimalIndex - digits.length)}`
        : `${digits.slice(0, decimalIndex)}.${digits.slice(decimalIndex)}`;
    return normalizePlain(expanded);
  }

  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return "";
  return normalizePlain(
    n.toLocaleString("en-US", {
      useGrouping: false,
      maximumFractionDigits: 18,
    })
  );
}

function counterpartyRowExactPriceText(row) {
  if (!row || typeof row !== "object") return "";
  const rawDispenser = row?.raw_dispenser && typeof row.raw_dispenser === "object"
    ? row.raw_dispenser
    : {};
  const rawOrder = row?.raw_order && typeof row.raw_order === "object"
    ? row.raw_order
    : {};
  for (const value of [
    row?.price_exact,
    row?.priceExact,
    row?.price_btc_per_unit_exact,
    rawDispenser?.price_btc_per_unit_exact,
    rawOrder?.price_exact,
    row?.price,
    row?.displayPrice,
    row?.limitPrice,
    row?.rate,
  ]) {
    const text = counterpartyPlainDecimalText(value);
    if (text && Number(text) > 0) return text;
  }
  return "";
}

function counterpartyPricePrecisionDecimals(value) {
  const text = counterpartyPlainDecimalText(value);
  if (!text || !text.includes(".")) return text ? 0 : null;
  return text.split(".", 2)[1].length;
}

// ─────────────────────────────────────────────────────────────
// Rules helpers (price precision / tick normalization)
// ─────────────────────────────────────────────────────────────
function decimalsFromIncrement(inc) {
  const s = String(inc ?? "").trim();
  if (!s) return null;

  const m = s.toLowerCase().match(/e-(\d+)/);
  if (m) return Number(m[1]);

  const dot = s.indexOf(".");
  if (dot === -1) return 0;
  return s.length - dot - 1;
}

function clamp(n, lo, hi) {
  if (!Number.isFinite(n)) return lo;
  return Math.max(lo, Math.min(hi, n));
}

function isPolkadotHydrationVenueKey(v) {
  const key = String(v || "").toLowerCase().trim();
  return key === "polkadot_hydration" || key === "hydration" || key === "polkadot_dex" || key.startsWith("polkadot_");
}

function isCounterpartyVenueKey(v) {
  const key = String(v || "").toLowerCase().trim();
  return key === "counterparty" || key === "counterparty_unisat" || key === "bitcoin_counterparty";
}

function isRobinhoodChainVenueKey(v) {
  return String(v || "").toLowerCase().trim() === "robinhood_chain";
}

function robinhoodChainPairParts(symbol) {
  const raw = String(symbol || "").trim().toUpperCase().replace(/[\/_]/g, "-");
  const parts = raw.split("-").map((item) => item.trim()).filter(Boolean);
  if (parts.length !== 2) return { base: "", quote: "", symbol: raw };
  return { base: parts[0], quote: parts[1], symbol: `${parts[0]}-${parts[1]}` };
}

function robinhoodChainMarketStatusLabel(market) {
  const mechanism = String(market?.mechanism || "").trim().toLowerCase();
  const state = String(market?.indicative_state || "not_tested").trim().toLowerCase();
  if (market?.execution_enabled === true) return "LIVE VERIFIED";
  if (market?.orderbook_enabled === true) return "SYNTH BOOK AVAILABLE";
  if (mechanism === "wrap_unwrap" && market?.mechanism_configured === true) return "WRAP / UNWRAP CONFIGURED";
  if (state === "provider_error") return "PROVIDER ERROR";
  return state.replaceAll("_", " ").toUpperCase();
}

function counterpartyPairParts(symbol) {
  const raw = String(symbol || "").trim().toUpperCase().replace(/[/_]/g, "-");
  const parts = raw.split("-").map((x) => x.trim()).filter(Boolean);
  if (parts.length !== 2) return { base: "", quote: "", symbol: raw };
  const alias = (v) => {
    const a = String(v || "").trim().toUpperCase();
    if (a === "BCY" || a === "BITCRYSTAL") return "BITCRYSTALS";
    if (a === "XBT") return "BTC";
    return a;
  };
  const base = alias(parts[0]);
  const quote = alias(parts[1]);
  return { base, quote, symbol: base && quote ? `${base}-${quote}` : raw };
}

function normalizeHydrationRouteMode(v) {
  const raw = String(v || "auto").toLowerCase().trim();
  if (raw === "managed" || raw === "managed_sdk" || raw === "sdk_router" || raw === "sidecar") return "sdk";
  if (raw === "isolated" || raw === "helper") return "isolated_helper";
  if (raw === "manual" || raw === "xyk") return "manual_xyk";
  return raw === "sdk" || raw === "isolated_helper" || raw === "manual_xyk" ? raw : "auto";
}

function hydrationRouteModeLabel(v) {
  const mode = normalizeHydrationRouteMode(v);
  if (mode === "sdk") return "SDK";
  if (mode === "isolated_helper") return "Isolated";
  if (mode === "manual_xyk") return "Manual XYK";
  return "Auto";
}

const HYDRATION_LOW_TVL_USD = 10000;

function firstFiniteNumber(...values) {
  for (const v of values) {
    if (v === null || v === undefined || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function formatUsdCompact(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1000) return `$${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  if (Math.abs(n) >= 1) return `$${n.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  return `$${n.toLocaleString(undefined, { maximumFractionDigits: 4 })}`;
}

function buildHydrationLowLiquidityWarning(payload) {
  const p = payload && typeof payload === "object" ? payload : {};
  const pool = (p.pool && typeof p.pool === "object") ? p.pool : p;
  const routerText = String(p.router || p.routeModeEffective || p.route_mode_effective || pool.router || "").toLowerCase();
  const sourceText = String(pool.source || p.source || "").toLowerCase();
  const tvlUsd = firstFiniteNumber(
    pool.tvlUsd,
    pool.tvl_usd,
    pool.liquidityUsd,
    pool.liquidity_usd,
    pool.totalUsd,
    pool.total_usd,
    p.tvlUsd,
    p.tvl_usd,
    p.liquidityUsd,
    p.liquidity_usd
  );
  const explicitLow =
    pool.lowLiquidity === true ||
    pool.low_liquidity === true ||
    p.lowLiquidity === true ||
    p.low_liquidity === true;
  const manualOrIsolated =
    sourceText.includes("live_pool_account") ||
    sourceText.includes("route_registry") ||
    sourceText.includes("manual") ||
    sourceText.includes("isolated") ||
    routerText.includes("manual_xyk") ||
    routerText.includes("fallback") ||
    routerText.includes("isolated");
  const belowThreshold = tvlUsd !== null && tvlUsd < HYDRATION_LOW_TVL_USD;
  if (!explicitLow && !belowThreshold && !manualOrIsolated) return null;
  const label = belowThreshold
    ? `Low TVL ${formatUsdCompact(tvlUsd)} < $10k`
    : "Low-liquidity isolated pool";
  return {
    label,
    tvlUsd,
    thresholdUsd: HYDRATION_LOW_TVL_USD,
    source: String(pool.source || p.source || "").trim(),
    manualOrIsolated,
    belowThreshold,
    message: belowThreshold
      ? "Hydration spot quotes may be unavailable below $10k TVL; UTT manual XYK routing can still trade when enabled."
      : "Manual/live isolated pool: monitor TVL and price impact; Hydration SDK spot quotes may be unavailable below $10k TVL.",
  };
}

function isHydrationQuoteAvailable(statusPayload) {
  const qs = statusPayload?.quoteStatus || statusPayload?.detail?.quoteStatus || null;
  return qs?.available === true && (statusPayload?.liveQuotesEnabled === true || qs?.enabled === true);
}

function formatHydrationQuoteStatus(statusPayload) {
  if (isHydrationQuoteAvailable(statusPayload)) {
    return "Hydration live quotes are enabled for controlled testing. Live swaps remain disabled.";
  }
  return "Hydration quotes/swaps are temporarily disabled. Asset resolution is available. Waiting on a non-router quote source before live trading is enabled.";
}

function formatHydrationQuoteStatusDetail(statusPayload) {
  const qs = statusPayload?.quoteStatus || statusPayload?.detail?.quoteStatus || null;
  return String(qs?.reason || statusPayload?.message || "").trim();
}

const HYDRATION_PRICE_STATUS_DEFAULT_ASSETS = ["HDX", "DOT", "USDT", "USDC", "UTTT", "HOLLAR"];

function hydrationPriceStatusAssetsForSymbol(sym) {
  const out = new Set(HYDRATION_PRICE_STATUS_DEFAULT_ASSETS);
  try {
    const s = String(sym || "").trim().toUpperCase();
    const parts = s.includes("-") ? s.split("-") : s.includes("/") ? s.split("/") : [];
    for (const p of parts) {
      const v = String(p || "").trim().toUpperCase();
      if (v) out.add(v);
    }
  } catch {
    // ignore
  }
  return Array.from(out);
}

function hydrationPriceStatusView(payload, err) {
  if (!payload && !err) {
    return {
      label: "Price status loading…",
      title: "Waiting for Hydration price/status endpoint. This status-only request does not refresh prices, start the sidecar, or call SDK router quotes.",
      tone: "warn",
    };
  }

  if (err) {
    return {
      label: "Price status unavailable",
      title: String(err || "Hydration price status unavailable."),
      tone: "warn",
    };
  }

  const p = payload && typeof payload === "object" ? payload : {};
  const d = (p.statusDetail && typeof p.statusDetail === "object") ? p.statusDetail : {};
  const c = (p.cache && typeof p.cache === "object") ? p.cache : {};
  const classification = String(d.classification || c.classification || p.status || "unknown").trim();
  const sourceState = String(d.source_state || c.source_state || p.status || "status_only").trim();
  const missingRaw = Array.isArray(d.missing_prices)
    ? d.missing_prices
    : Array.isArray(c.missing_prices)
      ? c.missing_prices
      : Array.isArray(p.missingPrices)
        ? p.missingPrices
        : [];
  const missing = missingRaw.map((x) => String(x || "").trim()).filter(Boolean);
  const stale = d.stale === true || c.stale === true;
  const inBackoff = d.in_error_backoff === true || c.in_error_backoff === true;
  const hasAll = d.has_all_requested === true || c.has_all_requested === true || (missing.length === 0 && !!payload);
  const ttl = Number(d.seconds_until_expiry ?? c.seconds_until_expiry ?? 0);
  const retry = Number(d.seconds_until_retry ?? c.seconds_until_retry ?? 0);

  let label = classification || "status_only";
  if (classification === "status_only" && hasAll && !stale) label = "price cache fresh";
  else if (classification === "cache_only_fresh") label = "price cache fresh";
  else if (classification === "cache_only_partial_stale") label = "price cache partial/stale";
  else if (classification === "cache_only_stale") label = "price cache stale";
  else if (classification === "cache_only_partial") label = "price cache partial";
  else if (classification === "live_fresh") label = "prices live/fresh";
  else if (classification === "partial_stale") label = "prices partial/stale";
  else if (classification === "error_backoff") label = "price refresh backoff";
  else if (classification === "refresh_failed_stale") label = "refresh failed; stale cache";

  const pieces = [`Hydration ${label}`];
  if (missing.length) pieces.push(`missing ${missing.join(", ")}`);
  if (Number.isFinite(ttl) && ttl > 0 && !stale) pieces.push(`${Math.floor(ttl)}s TTL`);
  if (Number.isFinite(retry) && retry > 0) pieces.push(`${Math.floor(retry)}s retry`);

  const tone = inBackoff || stale || missing.length ? "warn" : "ok";
  const title = [
    "Hydration price/status endpoint. This is status-only UI polish; it does not refresh prices, start the sidecar, or call SDK router quotes.",
    `classification=${classification || "unknown"}`,
    `source_state=${sourceState || "unknown"}`,
    missing.length ? `missing=${missing.join(",")}` : "missing=none",
  ].join(" ");

  return { label: pieces.join(" • "), title, tone };
}

/**
 * Best-effort floor to increment using integer math at inferred decimals.
 * Example: value=0.15130102, inc=0.000001 -> 0.151301
 */
function floorToIncrement(value, inc) {
  const v = Number(value);
  const step = Number(inc);
  if (!Number.isFinite(v) || !Number.isFinite(step) || !(step > 0)) return v;

  const d = decimalsFromIncrement(String(inc));
  if (d === null) return v;

  // Support higher-precision venues/pairs (BTC-quoted pairs often need 9+)
  const scale = Math.pow(10, clamp(d, 0, 18));
  const stepInt = Math.round(step * scale);
  if (!Number.isFinite(stepInt) || stepInt <= 0) return v;

  const vInt = Math.floor(v * scale + 1e-9);
  const floored = Math.floor(vInt / stepInt) * stepInt;
  return floored / scale;
}

/**
 * If we only have decimals, floor by decimals.
 * Example: value=0.15130102, decimals=6 -> 0.151301
 */
function floorToDecimals(value, decimals) {
  const v = Number(value);
  const d = Number(decimals);
  if (!Number.isFinite(v) || !Number.isFinite(d)) return v;
  const scale = Math.pow(10, clamp(d, 0, 18));
  return Math.floor(v * scale + 1e-9) / scale;
}

/**
 * Convert backend error payloads / thrown error strings into a user-friendly message.
 * Primary goal: when venue does not list the pair, show:
 *   "Pair Not Found at <venue>"
 */
function formatOrderBookError(rawMsg, venueLabel) {
  const venue = String(venueLabel || "").trim() || "venue";
  const msg = String(rawMsg || "").trim();
  const low = msg.toLowerCase();

  // FastAPI commonly returns { detail: <string|object> }
  let detailStr = "";
  let detailObj = null;
  if (msg.startsWith("{") && msg.endsWith("}")) {
    try {
      const parsed = JSON.parse(msg);
      if (parsed && typeof parsed === "object" && parsed.detail !== undefined) {
        if (typeof parsed.detail === "string") detailStr = parsed.detail;
        else if (parsed.detail && typeof parsed.detail === "object") detailObj = parsed.detail;
      }
    } catch {
      // ignore
    }
  }

  // If the backend sent structured detail, prefer a precise UX message.
  if (detailObj && typeof detailObj === "object") {
    const err = String(detailObj.error || "").toLowerCase();
    if (err === "unknown_symbol") {
      const sym = String(detailObj.symbol || "").trim() || "(unknown)";
      return `Token “${sym}” isn’t in your Token Registry for ${venue}. Add it in Tokens → Token/Symbol Registry (Solana) (preferred), or use mint:<ADDRESS> in the pair (e.g. mint:<UTTT_MINT>-SOL).`;
    }
    if (err === "symbol_ambiguous") {
      const sym = String(detailObj.symbol || "").trim() || "(unknown)";
      return `Token symbol “${sym}” maps to multiple mints on ${venue}. Pick the right one by adding the token in Tokens → Token/Symbol Registry (Solana), or use mint:<ADDRESS> in the pair.`;
    }
    if (err === "missing_decimals") {
      const sym = String(detailObj.symbol || "").trim() || "(unknown)";
      return `Missing decimals for ${sym} on ${venue} — set UTT_SOLANA_DECIMALS_JSON (and matching mint) in backend env`;
    }
    if (err === "unknown_mint_decimals") {
      const mint = String(detailObj.mint || "").trim() || "(unknown)";
      return `Unknown decimals for mint ${mint} on ${venue} — ensure the mint is valid / discoverable or set UTT_SOLANA_DECIMALS_JSON override`;

    if (err === "no_quote_levels") {
      const msg2 = String(detailObj.message || "").trim();
      const sampleErrors = Array.isArray(detailObj.sampleErrors) ? detailObj.sampleErrors : [];
      const first = sampleErrors[0]?.detail?.error ? String(sampleErrors[0].detail.error) : "";
      const hint = first ? ` (first error: ${first})` : "";
      return (msg2 || `No routable Jupiter quotes for sampled sizes`) + hint + ` — check liquidity/routes` + (detailObj.usedApiKey ? "" : ` or set UTT_JUP_API_KEY if required`);
    }
    }
  }

  const d = String(detailStr || "").trim();
  const dlow = d.toLowerCase();

  const looksLikePairNotFound =
    dlow.includes("pair not found") ||
    dlow.includes("symbol not found") ||
    dlow.includes("unknown symbol") ||
    dlow.includes("invalid symbol") ||
    dlow.includes("invalidsymbol") ||
    dlow.includes("unknown request") ||
    (dlow.includes("400") && dlow.includes("/v1/book/")) ||
    low.includes("pair not found") ||
    low.includes("symbol not found") ||
    low.includes("unknown symbol") ||
    low.includes("invalid symbol") ||
    low.includes("invalidsymbol") ||
    (low.includes("400 bad request") && low.includes("/v1/book/")) ||
    (low.includes("failed to fetch orderbook") && low.includes("400 bad request"));

  if (looksLikePairNotFound) return `Pair Not Found at ${venue}`;

  return d || msg || "Failed to load order book";
}

export default function OrderBookWidget({
  apiBase,
  effectiveVenue,
  fmtNum,
  styles,
  marketSymbol,
  obDepth,
  setObDepth,
  appContainerRef,
  hideVenueNames = false,
  onPickPrice,
  onPickQty,
}) {
  // RH-UI.MKT.1: App owns the committed market. This widget may refresh it,
  // but it must not substitute or mutate it.
  const obSymbol = String(marketSymbol || "").trim();
  const [obBids, setObBids] = useState([]);
  const [obAsks, setObAsks] = useState([]);
  const [obLoading, setObLoading] = useState(false);
  const [obError, setObError] = useState(null);
  const [orderBookMeta, setOrderBookMeta] = useState(null);
  const [quoteUsdContext, setQuoteUsdContext] = useState(null);
  const [robinhoodChainMarkets, setRobinhoodChainMarkets] = useState([]);
  const [robinhoodChainMarketsLoading, setRobinhoodChainMarketsLoading] = useState(false);
  const [robinhoodChainMarketsError, setRobinhoodChainMarketsError] = useState("");
  const robinhoodChainMarketsReqRef = useRef(0);
  const [counterpartyLiquidityFilter, setCounterpartyLiquidityFilter] = useState(() => readCounterpartyLiquidityFilter());
  const [counterpartyExecutionMode, setCounterpartyExecutionMode] = useState(() => readCounterpartyExecutionMode());
  const quoteUsdReqRef = useRef(0);

  // Order rules (price display + click-to-ticket normalization) for ANY venue
  const [priceDecimals, setPriceDecimals] = useState(null);
  const [sizeDecimals, setSizeDecimals] = useState(null);
  const [priceIncrement, setPriceIncrement] = useState(null);

  // Separate display precision from click/ticket precision:
  // - display: keep compact/readable (cap 8)
  // - click/ticket: preserve up to 9 decimals for low-priced USDC pairs
  const ORDERBOOK_PRICE_DISPLAY_CAP = 8;
  const ORDERBOOK_PRICE_CLICK_CAP = 9;

  // Defaults:
  // - Auto refresh: ON
  // - Interval: 30 seconds
  const [obAutoRefresh, setObAutoRefresh] = useState(() => {
    const raw = localStorage.getItem(LS_OB_AUTO);
    if (raw === null || raw === undefined) return true;
    return raw === "1" || raw === "true";
  });

  const [obAutoSeconds, setObAutoSeconds] = useState(() => {
    const raw = localStorage.getItem(LS_OB_SEC);
    const n = safeNum(raw);
    if (n === null) return 30;
    return Math.max(1, Math.min(300, Math.floor(n)));
  });
  const [obSolanaRouterMode, setObSolanaRouterMode] = useState(() => {
    try {
      const v = String(localStorage.getItem(LS_OB_SOL_ROUTER) || "auto").toLowerCase().trim();
      return v === "ultra" || v === "jupiter" || v === "raydium" || v === "metis" ? v : "auto";
    } catch {
      return "auto";
    }
  });
  const [obHydrationRouteMode, setObHydrationRouteMode] = useState(() => {
    try {
      return normalizeHydrationRouteMode(localStorage.getItem(LS_OB_HYDRATION_ROUTE) || "auto");
    } catch {
      return "auto";
    }
  });
  const [obSettingsOpen, setObSettingsOpen] = useState(false);
  const [obActiveRouter, setObActiveRouter] = useState(null);
  const [hydrationStatus, setHydrationStatus] = useState(null);
  const [hydrationLiquidityWarning, setHydrationLiquidityWarning] = useState(null);
  const [hydrationPriceStatus, setHydrationPriceStatus] = useState(null);
  const [hydrationPriceStatusError, setHydrationPriceStatusError] = useState(null);
  const hydrationPriceStatusReqRef = useRef(0);

  const inFlightRef = useRef(false);
  const abortRef = useRef(null);

  // NEW: error gating to stop hammering known-bad pairs
  const pairNotFoundRef = useRef(false);

  // NEW: 429 backoff/cooldown
  const cooldownUntilRef = useRef(0);
  const cooldownPowRef = useRef(0);
  const [counterpartyResilience, setCounterpartyResilience] = useState(null);
  const [cooldownClock, setCooldownClock] = useState(() => Date.now());
  const counterpartyLastGoodRef = useRef(new Map());

  const [inlineMode, setInlineMode] = useState(true);

  // Right-rail tile containment mode: keep this widget fully contained inside the App rail tile.
  const forceTileMode = true;

  const DEFAULT_W = 460;
  const DEFAULT_H = 520;

  const MIN_W = 320;
  const MIN_H = 260;
  const MAX_W = 900;
  const MAX_H = Math.max(260, Math.floor(window?.innerHeight ? window.innerHeight * 0.9 : 800));

  // The dedicated Order Book lock control was removed in 8.5B.
  // Keep the widget unlocked so old localStorage state cannot leave it stuck.
  const [locked] = useState(false);

  const [box, setBox] = useState(() => {
    const saved = (() => {
      try {
        return JSON.parse(localStorage.getItem(LS_OB_BOX) || "null");
      } catch {
        return null;
      }
    })();

    return saved && typeof saved === "object"
      ? { x: saved.x ?? 0, y: saved.y ?? 16, w: saved.w ?? DEFAULT_W, h: saved.h ?? DEFAULT_H, right: saved.right, bottom: saved.bottom }
      : { x: 0, y: 16, w: DEFAULT_W, h: DEFAULT_H };
  });

  useEffect(() => {
    try {
      localStorage.setItem(LS_OB_LOCK, "0");
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    localStorage.setItem(LS_OB_BOX, JSON.stringify(box));
  }, [box]);

  useEffect(() => {
    localStorage.setItem(LS_OB_AUTO, obAutoRefresh ? "1" : "0");
  }, [obAutoRefresh]);

  useEffect(() => {
    const n = Math.max(1, Math.min(300, Number(obAutoSeconds) || 30));
    localStorage.setItem(LS_OB_SEC, String(Math.floor(n)));
  }, [obAutoSeconds]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_OB_SOL_ROUTER, String(obSolanaRouterMode || "auto"));
    } catch {
      // ignore
    }
  }, [obSolanaRouterMode]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_OB_HYDRATION_ROUTE, normalizeHydrationRouteMode(obHydrationRouteMode));
    } catch {
      // ignore
    }
  }, [obHydrationRouteMode]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_OB_COUNTERPARTY_LIQUIDITY_FILTER, normalizeCounterpartyLiquidityFilter(counterpartyLiquidityFilter));
    } catch {
      // ignore
    }
  }, [counterpartyLiquidityFilter]);

  useEffect(() => {
    if (typeof window === "undefined") return undefined;
    const onMode = (event) => {
      setCounterpartyExecutionMode(normalizeCounterpartyExecutionMode(event?.detail?.mode));
    };
    const onStorage = (event) => {
      if (event?.key === LS_OT_COUNTERPARTY_EXECUTION_MODE) {
        setCounterpartyExecutionMode(readCounterpartyExecutionMode());
      }
    };
    window.addEventListener(COUNTERPARTY_EXECUTION_MODE_EVENT, onMode);
    window.addEventListener("storage", onStorage);
    return () => {
      window.removeEventListener(COUNTERPARTY_EXECUTION_MODE_EVENT, onMode);
      window.removeEventListener("storage", onStorage);
    };
  }, []);

  // Reset stale book state whenever the App-level venue/market/depth changes.
  useEffect(() => {
    try {
      if (abortRef.current) abortRef.current.abort();
    } catch {
      // ignore
    }
    inFlightRef.current = false;
    setObLoading(false);
    setObError(null);
    // Reset pair gating when symbol changes externally. Preserve an active
    // Counterparty service-wide cooldown so symbol changes cannot bypass 429.
    pairNotFoundRef.current = false;
    const preserveCounterpartyCooldown = isCounterpartyVenueKey(effectiveVenue) && Date.now() < (cooldownUntilRef.current || 0);
    if (preserveCounterpartyCooldown) {
      const canon = counterpartyPairParts(obSymbol).symbol || String(obSymbol || "").trim();
      const key = counterpartyOrderbookSnapshotKey(effectiveVenue, canon, obDepth);
      const cached = counterpartyLastGoodRef.current.get(key) || null;
      if (cached) {
        setObAsks(cached.asks || []);
        setObBids(cached.bids || []);
        setOrderBookMeta({ ...(cached.meta || {}), stale: true, snapshotSource: cached.snapshotSource || "frontend_last_good" });
        setCounterpartyResilience((prev) => prev ? { ...prev, stale: true } : prev);
      } else {
        setObAsks([]);
        setObBids([]);
        setOrderBookMeta(null);
        setCounterpartyResilience((prev) => prev ? { ...prev, stale: false } : prev);
      }
    } else {
      cooldownUntilRef.current = 0;
      cooldownPowRef.current = 0;
      setCounterpartyResilience(null);
      setObAsks([]);
      setObBids([]);
      setOrderBookMeta(null);
    }
    setHydrationStatus(null);
    setHydrationLiquidityWarning(null);
    setHydrationPriceStatus(null);
    setHydrationPriceStatusError(null);
    setQuoteUsdContext(null);
  }, [obSymbol, obDepth, effectiveVenue]);

  const isSolJupVenue = useMemo(() => {
    return String(effectiveVenue || "").toLowerCase().trim() === "solana_jupiter";
  }, [effectiveVenue]);

  const isPolkadotDexVenue = useMemo(() => isPolkadotHydrationVenueKey(effectiveVenue), [effectiveVenue]);
  const isCounterpartyVenue = useMemo(() => isCounterpartyVenueKey(effectiveVenue), [effectiveVenue]);
  const isRobinhoodChainVenue = useMemo(() => isRobinhoodChainVenueKey(effectiveVenue), [effectiveVenue]);
  const selectedRobinhoodChainMarket = useMemo(() => {
    const wanted = robinhoodChainPairParts(obSymbol).symbol;
    return (robinhoodChainMarkets || []).find((item) => (
      robinhoodChainPairParts(item?.symbol).symbol === wanted
    )) || null;
  }, [robinhoodChainMarkets, obSymbol]);

  function robinhoodChainMarketForSymbol(value) {
    const wanted = robinhoodChainPairParts(value).symbol;
    return (robinhoodChainMarkets || []).find((item) => (
      robinhoodChainPairParts(item?.symbol).symbol === wanted
    )) || null;
  }

  useEffect(() => {
    if (!isRobinhoodChainVenue) {
      setRobinhoodChainMarkets([]);
      setRobinhoodChainMarketsLoading(false);
      setRobinhoodChainMarketsError("");
      return undefined;
    }

    const reqId = ++robinhoodChainMarketsReqRef.current;
    const controller = new AbortController();
    let cancelled = false;

    (async () => {
      try {
        setRobinhoodChainMarketsLoading(true);
        setRobinhoodChainMarketsError("");
        const response = await fetch(
          `${apiBase}/api/robinhood_chain/registry-discovery/markets?_ts=${Date.now()}`,
          { signal: controller.signal, cache: "no-store" }
        );
        if (!response.ok) {
          const text = await response.text().catch(() => "");
          throw new Error(text || `Robinhood Chain market catalog HTTP ${response.status}`);
        }
        const payload = await response.json();
        if (payload?.ok !== true) throw new Error(payload?.error || "Robinhood Chain market catalog returned ok=false.");
        const items = Array.isArray(payload?.items) ? payload.items : [];
        if (cancelled || robinhoodChainMarketsReqRef.current !== reqId) return;
        setRobinhoodChainMarkets(items);

      } catch (error) {
        if (cancelled || String(error?.name || "") === "AbortError") return;
        setRobinhoodChainMarkets([]);
        setRobinhoodChainMarketsError(String(error?.message || error || "Robinhood Chain market catalog failed."));
      } finally {
        if (!cancelled && robinhoodChainMarketsReqRef.current === reqId) {
          setRobinhoodChainMarketsLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [isRobinhoodChainVenue, apiBase]);

  // Reset gating when venue changes
  useEffect(() => {
    pairNotFoundRef.current = false;
    cooldownUntilRef.current = 0;
    cooldownPowRef.current = 0;
    setCounterpartyResilience(null);
    setCooldownClock(Date.now());
    setObActiveRouter(null);
    setHydrationStatus(null);
    setHydrationLiquidityWarning(null);
    setHydrationPriceStatus(null);
    setHydrationPriceStatusError(null);
    setOrderBookMeta(null);
    setQuoteUsdContext(null);

    // Prevent DEX-specific decimals from leaking into regular CEX venues.
    if (!isSolJupVenue && !isPolkadotDexVenue && !isCounterpartyVenue && !isRobinhoodChainVenue) setSizeDecimals(null);
  }, [effectiveVenue, isSolJupVenue, isPolkadotDexVenue, isCounterpartyVenue, isRobinhoodChainVenue]);

  useEffect(() => {
    if (!isSolJupVenue) return;
    pairNotFoundRef.current = false;
    cooldownUntilRef.current = 0;
    cooldownPowRef.current = 0;
    setObActiveRouter(null);
  }, [obSolanaRouterMode, isSolJupVenue]);

  useEffect(() => {
    if (!isPolkadotDexVenue) return;
    pairNotFoundRef.current = false;
    cooldownUntilRef.current = 0;
    cooldownPowRef.current = 0;
    setObActiveRouter(null);
    setHydrationLiquidityWarning(null);
  }, [obHydrationRouteMode, isPolkadotDexVenue]);

  useEffect(() => {
    if (!isCounterpartyVenue) return;
    pairNotFoundRef.current = false;
    setObActiveRouter(null);
  }, [isCounterpartyVenue, obSymbol]);

  useEffect(() => {
    if (!counterpartyResilience?.active) return undefined;
    const tick = () => {
      const now = Date.now();
      setCooldownClock(now);
      if (now >= Number(counterpartyResilience?.retryAtMs || 0)) {
        setCounterpartyResilience((prev) => prev ? { ...prev, active: false } : prev);
      }
    };
    tick();
    const timer = window.setInterval(tick, 1000);
    return () => window.clearInterval(timer);
  }, [counterpartyResilience?.active, counterpartyResilience?.retryAtMs]);


  useEffect(() => {
    const parts = isRobinhoodChainVenue
      ? robinhoodChainPairParts(obSymbol)
      : orderBookPairParts(obSymbol);
    const quote = isRobinhoodChainVenue
      ? String(parts.quote || "").trim().toUpperCase()
      : normalizeOrderBookAsset(parts.quote);
    const reqId = ++quoteUsdReqRef.current;
    let cancelled = false;

    if (!quote) {
      setQuoteUsdContext(null);
      return undefined;
    }
    if (isUsdValueQuote(quote)) {
      setQuoteUsdContext({
        status: "native_usd",
        quoteAsset: quote,
        priceUsd: 1,
        source: "USD-valued quote",
        updatedAt: null,
        stale: false,
      });
      return undefined;
    }

    const applyShared = () => {
      if (cancelled || quoteUsdReqRef.current !== reqId) return null;
      const shared = readSharedQuoteUsdContext(quote);
      if (shared) setQuoteUsdContext(shared);
      return shared;
    };

    const shared = applyShared();
    const onSharedSnapshot = () => applyShared();
    const onStorage = (event) => {
      if (event?.key === MARKET_METRICS_BROWSER_CACHE_KEY) applyShared();
    };

    if (typeof window !== "undefined") {
      window.addEventListener(MARKET_METRICS_BROWSER_CACHE_EVENT, onSharedSnapshot);
      window.addEventListener("storage", onStorage);
    }

    const shouldRefresh = !shared || shared.stale === true;
    const timer = shouldRefresh && apiBase
      ? window.setTimeout(async () => {
          try {
            const base = String(apiBase || "").replace(/\/+$/, "");
            const url = new URL(`${base}/api/market_metrics/summary`);
            url.searchParams.set("assets", quote);
            url.searchParams.set("limit", "25");
            url.searchParams.set("ttl_s", "300");
            const response = await fetch(url.toString(), { method: "GET", cache: "no-store" });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const payload = await response.json();
            const rows = Array.isArray(payload?.items) ? payload.items : [];
            const row = rows.find((item) => metricRowAsset(item) === quote);
            const next = metricRowQuoteUsdContext(row, { lastUpdated: payload?.updated_at || null }, quote);
            if (cancelled || quoteUsdReqRef.current !== reqId) return;
            if (next) setQuoteUsdContext(next);
            else if (!shared) {
              setQuoteUsdContext({
                status: "unavailable",
                quoteAsset: quote,
                priceUsd: null,
                source: "",
                updatedAt: null,
                stale: false,
              });
            }
          } catch {
            if (cancelled || quoteUsdReqRef.current !== reqId || shared) return;
            setQuoteUsdContext({
              status: "unavailable",
              quoteAsset: quote,
              priceUsd: null,
              source: "",
              updatedAt: null,
              stale: false,
            });
          }
        }, 250)
      : null;

    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
      if (typeof window !== "undefined") {
        window.removeEventListener(MARKET_METRICS_BROWSER_CACHE_EVENT, onSharedSnapshot);
        window.removeEventListener("storage", onStorage);
      }
    };
  }, [apiBase, effectiveVenue, obSymbol]);


  useEffect(() => {
    const sym = String(obSymbol || "").trim().toUpperCase();
    if (!isPolkadotDexVenue || !apiBase || !sym || !sym.includes("-")) {
      setHydrationPriceStatus(null);
      setHydrationPriceStatusError(null);
      return;
    }

    const reqId = ++hydrationPriceStatusReqRef.current;
    let cancelled = false;

    const t = setTimeout(async () => {
      try {
        const url = new URL(`${apiBase}/api/polkadot_dex/hydration/prices/status`);
        url.searchParams.set("assets", hydrationPriceStatusAssetsForSymbol(sym).join(","));
        url.searchParams.set("symbol", sym);
        url.searchParams.set("_ts", String(Date.now()));

        const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
        if (!r.ok) {
          const txt = await r.text().catch(() => "");
          throw new Error(txt || `Hydration price status HTTP ${r.status}`);
        }

        const data = await r.json();
        if (cancelled || hydrationPriceStatusReqRef.current !== reqId) return;
        setHydrationPriceStatus(data || null);
        setHydrationPriceStatusError(null);
      } catch (e) {
        if (cancelled || hydrationPriceStatusReqRef.current !== reqId) return;
        setHydrationPriceStatus(null);
        setHydrationPriceStatusError(e?.message || "Failed to load Hydration price status.");
      }
    }, 350);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [isPolkadotDexVenue, apiBase, obSymbol]);


  const lockedRef = useRef(locked);
  const boxRef = useRef(box);
  useEffect(() => { lockedRef.current = locked; }, [locked]);
  useEffect(() => { boxRef.current = box; }, [box]);

  const dragStateRef = useRef(null);
  const resizeStateRef = useRef(null);

  const asksWrapRef = useRef(null);
  const bidsWrapRef = useRef(null);

  const venueLabel = hideVenueNames ? "••••" : String(effectiveVenue || "");

  function getViewport() {
  const vv = typeof window !== "undefined" ? window.visualViewport : null;
  const vw = Math.round(vv?.width ?? window.innerWidth);
  const vh = Math.round(vv?.height ?? window.innerHeight);
  const ox = Math.round(vv?.offsetLeft ?? 0);
  const oy = Math.round(vv?.offsetTop ?? 0);
  return { vw, vh, ox, oy };
}

function getGutterBounds() {
  const { vw, vh, ox, oy } = getViewport();

  const el = appContainerRef?.current;
  const rect = el?.getBoundingClientRect?.();

  const margin = 0;

  if (!rect) {
    return {
      minX: ox + margin,
      maxX: ox + vw - margin,
      minY: oy + margin,
      maxY: oy + vh - margin,
      gutterLeft: ox + margin,
      gutterWidth: vw - margin * 2,
      vw,
      vh,
      ox,
      oy,
    };
  }

  // rect.* are relative to the current visual viewport; convert to absolute page coords via (ox, oy)
  const containerRight = ox + rect.right;

  const gutterLeft = Math.ceil(containerRight + margin);
  const gutterWidth = Math.max(0, Math.floor((ox + vw) - gutterLeft - margin));

  return {
    minX: gutterLeft,
    maxX: ox + vw - margin,
    minY: oy + margin,
    maxY: oy + vh - margin,
    gutterLeft,
    gutterWidth,
    vw,
    vh,
    ox,
    oy,
  };
}

function clampBox(next) {
    const b = getGutterBounds();
    const w = clamp(next.w, MIN_W, Math.min(MAX_W, b.maxX - b.minX));
    const h = clamp(next.h, MIN_H, Math.min(MAX_H, b.maxY - b.minY));
    const x = clamp(next.x, b.minX, b.maxX - w);
    const y = clamp(next.y, b.minY, b.maxY - h);
    return { x, y, w, h };
  }

  useEffect(() => {
    if (forceTileMode) {
      setInlineMode(true);
      return;
    }

    const recompute = () => {
      const b = getGutterBounds();
      const canGutter = Number.isFinite(b.gutterWidth) ? b.gutterWidth >= MIN_W + 4 : false;
      setInlineMode(!canGutter);

      if (canGutter) {
        setBox((prev) => {
          // When locked, keep the widget anchored to the *same* side(s) of the gutter/viewport.
          // This prevents a temporary viewport reduction (DevTools, vertical tabs) from
          // permanently shoving the widget to a different docking edge.
          if (lockedRef.current) {

            const w = clamp(prev.w || DEFAULT_W, MIN_W, MAX_W);

            const h = clamp(prev.h || DEFAULT_H, MIN_H, MAX_H);

          

            const curX = Number.isFinite(prev.x) ? prev.x : b.minX;

            const curY = Number.isFinite(prev.y) ? prev.y : b.minY;

          

            // Freeze locked position (do not clamp/re-anchor under overlays).

            return { ...prev, x: curX, y: curY, w, h };

          }

// Unlocked: keep within gutter bounds and allow size clamp to gutter width/viewport height.
          const w = clamp(prev.w || DEFAULT_W, MIN_W, Math.min(MAX_W, b.gutterWidth));
          const h = clamp(prev.h || DEFAULT_H, MIN_H, Math.min(MAX_H, window.innerHeight));
          const x = clamp(prev.x ?? b.minX, b.minX, b.maxX - w);
          const y = clamp(prev.y ?? 0, b.minY, b.maxY - h);
          return clampBox({ x, y, w, h });
        });
      }
    };

    recompute();
    window.addEventListener("resize", recompute);
    return () => window.removeEventListener("resize", recompute);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [forceTileMode]);

  useEffect(() => {
    if (inlineMode) return;
    setBox((prev) => clampBox(prev));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [inlineMode, effectiveVenue]);

  function snapToCenterAnchors() {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        try {
          const asksEl = asksWrapRef.current;
          const bidsEl = bidsWrapRef.current;
          if (asksEl) asksEl.scrollTop = asksEl.scrollHeight;
          if (bidsEl) bidsEl.scrollTop = 0;
        } catch {
          // ignore
        }
      });
    });
  }

  function normalizeSide(arr) {
    const out = [];
    for (const x of arr || []) {
      if (Array.isArray(x)) {
        const px = Number(x?.[0]);
        const sz = Number(x?.[1]);
        if (Number.isFinite(px) && Number.isFinite(sz)) out.push({ price: px, size: sz });
      } else {
        const px = Number(x?.price);
        const sz = Number(x?.size ?? x?.qty ?? x?.amount);
        if (Number.isFinite(px) && Number.isFinite(sz)) {
          out.push({ ...x, price: px, size: sz });
        }
      }
    }
    return out;
  }

  const asksSorted = useMemo(() => [...(obAsks || [])].sort((a, b) => b.price - a.price), [obAsks]);
  const bidsSorted = useMemo(() => [...(obBids || [])].sort((a, b) => Number(b.price) - Number(a.price)), [obBids]);

  const counterpartyLiquidityCounts = useMemo(() => {
    const counts = {
      bidLimitOrders: 0,
      askLimitOrders: 0,
      askDispensers: 0,
      unknown: 0,
    };
    for (const row of obBids || []) {
      const type = counterpartyLiquidityType(row);
      if (type === "limit_order") counts.bidLimitOrders += 1;
      else if (type === "dispenser") counts.askDispensers += 1;
      else counts.unknown += 1;
    }
    for (const row of obAsks || []) {
      const type = counterpartyLiquidityType(row);
      if (type === "limit_order") counts.askLimitOrders += 1;
      else if (type === "dispenser") counts.askDispensers += 1;
      else counts.unknown += 1;
    }
    return counts;
  }, [obAsks, obBids]);

  // ─────────────────────────────────────────────────────────────
  // Rules fetch (any venue; used for BTC-quoted pairs that need >8 decimals)
  // ─────────────────────────────────────────────────────────────
  async function fetchOrderRules(symbolOverride) {
    const v = String(effectiveVenue || "").toLowerCase().trim();
    const sym = String(symbolOverride ?? obSymbol ?? "").trim();
    if (!v || !sym) {
      setPriceDecimals(null);
      setPriceIncrement(null);
      return;
    }

    if (isPolkadotHydrationVenueKey(v)) {
      setPriceDecimals(null);
      setPriceIncrement(null);
      return;
    }

    if (isCounterpartyVenueKey(v)) {
      const parts = counterpartyPairParts(sym);
      const quote = String(parts.quote || "").toUpperCase();
      const base = String(parts.base || "").toUpperCase();
      setPriceDecimals(quote === "BTC" || quote === "XCP" ? 8 : 8);
      setPriceIncrement(null);
      setSizeDecimals(base.endsWith("CARD") || base.endsWith("CD") ? 0 : 8);
      return;
    }

    if (isRobinhoodChainVenueKey(v)) {
      const market = robinhoodChainMarketForSymbol(sym);
      const quoteDecimals = Number(market?.quote?.decimals);
      const baseDecimals = Number(market?.base?.decimals);
      const displayDecimals = Number.isFinite(quoteDecimals) ? Math.max(6, Math.min(12, quoteDecimals)) : 8;
      setPriceDecimals(displayDecimals);
      setPriceIncrement(10 ** -displayDecimals);
      setSizeDecimals(Number.isFinite(baseDecimals) ? Math.max(0, Math.min(18, baseDecimals)) : 8);
      return;
    }

    try {
      const url = `${apiBase}/api/rules/order?venue=${encodeURIComponent(v)}&symbol=${encodeURIComponent(
        sym
      )}&type=limit&_ts=${Date.now()}`;

      const r = await fetch(url);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);

      const data = await r.json();

      const dec = data?.price_decimals;
      const inc = data?.price_increment;

      const inferred = dec ?? decimalsFromIncrement(inc);
      setPriceDecimals(Number.isFinite(Number(inferred)) ? Number(inferred) : null);

      const incNum = Number(inc);
      setPriceIncrement(Number.isFinite(incNum) && incNum > 0 ? incNum : null);
    } catch {
      // If a venue doesn't support rules for a symbol, we gracefully fall back to fmtNum()
      setPriceDecimals(null);
      setPriceIncrement(null);
    }
  }

  useEffect(() => {
    // Only refetch rules when venue/symbol changes (committed symbol, not draft)
    fetchOrderRules();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [effectiveVenue, obSymbol, apiBase]);

  function normPriceByRules(p) {
    const px = Number(p);
    if (!Number.isFinite(px)) return null;

    if (priceIncrement) return floorToIncrement(px, priceIncrement);
    if (Number.isFinite(Number(priceDecimals))) return floorToDecimals(px, Number(priceDecimals));

    return px;
  }

  function fmtPriceCell(p, row = null) {
    if (isCounterpartyVenue) {
      const exact = counterpartyRowExactPriceText(row || { price: p });
      if (exact) return exact;
    }

    // If we have rules-derived decimals (common for BTC-quoted pairs), use them.
    const np = normPriceByRules(p);
    if (np === null) return "—";

    if (Number.isFinite(Number(priceDecimals))) {
      const d = clamp(Number(priceDecimals), 0, ORDERBOOK_PRICE_DISPLAY_CAP);
      return Number(np).toFixed(d);
    }

    // Fall back to app-wide formatter (may be 8 decimals).
    return fmtNum(np);
  }

  function orderBookQuoteAsset() {
    if (isRobinhoodChainVenue) {
      const fromMeta = String(orderBookMeta?.quoteAsset || "").trim().toUpperCase();
      if (fromMeta) return fromMeta;
      return robinhoodChainPairParts(obSymbol).quote;
    }
    const fromMeta = normalizeOrderBookAsset(orderBookMeta?.quoteAsset);
    if (fromMeta) return fromMeta;
    return orderBookPairParts(obSymbol).quote;
  }

  function orderBookPriceUsd(p) {
    const quote = orderBookQuoteAsset();
    if (!quote || isUsdValueQuote(quote)) return null;
    const px = Number(p);
    const quoteUsd = Number(quoteUsdContext?.priceUsd);
    if (!Number.isFinite(px) || px < 0 || !Number.isFinite(quoteUsd) || quoteUsd <= 0) return null;
    return px * quoteUsd;
  }

  function orderBookPriceTitle(p, row = null) {
    const quote = orderBookQuoteAsset();
    const usd = orderBookPriceUsd(p);
    const exact = isCounterpartyVenue ? counterpartyRowExactPriceText(row || { price: p }) : "";
    const lines = [`Native execution price: ${exact || fmtPriceCell(p, row)} ${quote || ""}`.trim()];
    if (isCounterpartyVenue && exact) {
      const source = String(row?.price_source || row?.raw_dispenser?.price_source || "").trim();
      lines.push(`Exact audit precision: ${counterpartyPricePrecisionDecimals(exact) ?? "unknown"} decimal place(s)`);
      if (source) lines.push(`Price provenance: ${source}`);
    }
    if (usd !== null) {
      lines.push(`Derived USD value: ${formatOrderBookUsd(usd)} per ${orderBookPairParts(obSymbol).base || "base unit"}`);
      if (quoteUsdContext?.source) lines.push(`Source: ${quoteUsdContext.source}`);
      if (quoteUsdContext?.updatedAt) lines.push(`Updated: ${quoteUsdContext.updatedAt}`);
      if (quoteUsdContext?.stale) lines.push("Status: stale");
      lines.push("Informational only; the native quote price is used for execution.");
    } else if (quote && !isUsdValueQuote(quote)) {
      lines.push("USD conversion unavailable; native price remains authoritative.");
    }
    return lines.join("\n");
  }

  function fmtSizeCell(sz) {
    const v = Number(sz);
    if (!Number.isFinite(v)) return "—";
    // IMPORTANT: sizeDecimals is a DEX-only hint. Never apply it to regular CEX venues.
    if ((isSolJupVenue || isPolkadotDexVenue || isCounterpartyVenue || isRobinhoodChainVenue) && Number.isFinite(Number(sizeDecimals))) {
      const d = clamp(Number(sizeDecimals), 0, 18);
      return v.toFixed(d);
    }
    return fmtNum(v);
  }

  function enterCounterpartyCooldown(details = {}) {
    const now = Date.now();
    const pow = clamp(Number(cooldownPowRef.current) || 0, 0, 6);
    const localBaseMs = Math.min(
      COUNTERPARTY_RATE_LIMIT_MAX_BACKOFF_MS,
      COUNTERPARTY_RATE_LIMIT_BASE_BACKOFF_MS * Math.pow(2, pow)
    );
    const jitterMs = Math.floor(localBaseMs * COUNTERPARTY_RATE_LIMIT_JITTER_RATIO * Math.random());
    let retryAtMs = now + localBaseMs + jitterMs;

    const advertisedDelayMs = Number(details?.retryAfterMs);
    if (Number.isFinite(advertisedDelayMs) && advertisedDelayMs >= 0) {
      retryAtMs = Math.max(retryAtMs, now + advertisedDelayMs);
    }
    const advertisedRetryAtMs = counterpartyRetryAtMs(details?.retryAtMs);
    if (advertisedRetryAtMs !== null) retryAtMs = Math.max(retryAtMs, advertisedRetryAtMs);

    cooldownPowRef.current = clamp(pow + 1, 0, 6);
    cooldownUntilRef.current = retryAtMs;
    setCooldownClock(now);
    setCounterpartyResilience({
      active: true,
      stale: details?.stale === true,
      retryAtMs,
      retryAfterMs: Math.max(0, retryAtMs - now),
      source: String(details?.source || "counterparty_upstream").trim(),
      reason: String(details?.reason || "HTTP 429 Too Many Requests").trim(),
      snapshotAgeS: Number.isFinite(Number(details?.snapshotAgeS)) ? Number(details.snapshotAgeS) : null,
      snapshotSource: String(details?.snapshotSource || "").trim(),
    });
    return retryAtMs;
  }

  async function fetchOrderBook(opts = {}) {
    const v = String(opts.venueOverride ?? effectiveVenue ?? "").toLowerCase().trim();
    const sym = String(opts.symbolOverride ?? obSymbol ?? "").trim();
    const depth = Math.max(1, Math.min(200, Number(opts.depthOverride ?? obDepth) || 25));

    if (!v || !sym) return;

    if (isRobinhoodChainVenueKey(v)) {
      if (robinhoodChainMarketsLoading) return;
      const market = robinhoodChainMarketForSymbol(sym);
      if (!market) {
        setObAsks([]);
        setObBids([]);
        setOrderBookMeta(null);
        setObError(robinhoodChainMarketsError || "This market is not present in the Robinhood Chain database catalog.");
        return;
      }
      if (market?.orderbook_enabled !== true) {
        const parts = robinhoodChainPairParts(market?.symbol);
        setObAsks([]);
        setObBids([]);
        setObError(null);
        setObActiveRouter(String(market?.mechanism || "").trim().toLowerCase() === "wrap_unwrap" ? "native_wrap" : null);
        setOrderBookMeta({
          venue: v,
          symbol: parts.symbol,
          baseAsset: String(market?.base?.symbol || parts.base).trim().toUpperCase(),
          quoteAsset: String(market?.quote?.symbol || parts.quote).trim().toUpperCase(),
          mechanism: market?.mechanism || null,
          marketStatus: market?.indicative_state || "not_tested",
          orderbookEnabled: false,
          orderbookReason: market?.orderbook_reason || null,
          provider: Array.isArray(market?.providers) ? market.providers.join(", ") : null,
          synthetic: false,
          quoteOnly: true,
          restingOrder: false,
          warningCount: Number(market?.provider_error_direction_count || 0),
          fetchedAt: market?.last_verified_at || null,
          stale: false,
          snapshotSource: "database_market_catalog",
        });
        return;
      }
    }

    const counterpartyRequest = isCounterpartyVenueKey(v);
    const counterpartyRequestSymbol = counterpartyPairParts(sym).symbol || sym;
    const counterpartyCacheKey = counterpartyOrderbookSnapshotKey(v, counterpartyRequestSymbol, depth);

    // gating: known-bad pair
    if (!opts.force && pairNotFoundRef.current) return;

    // Counterparty 429 is service-wide: force/manual refresh must not bypass it.
    const now = Date.now();
    if (now < (cooldownUntilRef.current || 0) && (counterpartyRequest || !opts.force)) {
      setCooldownClock(now);
      return;
    }

    if (inFlightRef.current) return;
    inFlightRef.current = true;

    try {
      setObLoading(true);
      setObError(null);

      // cancel any prior request
      try {
        if (abortRef.current) abortRef.current.abort();
      } catch {
        // ignore
      }
      const ac = new AbortController();
      abortRef.current = ac;
      let requestTimedOut = false;
      let requestTimeoutId = null;

      // IMPORTANT:
      // - _ts busts browser/proxy caches
      // - force=true (when opts.force) requests a live fetch server-side
            const forceQ = opts.force ? "&force=true" : "";

      const isSolJup = v === "solana_jupiter";
      const isSolRay = v === "solana_raydium";
      const isPolkadotHydration = isPolkadotHydrationVenueKey(v);
      const routerModeRaw = isSolJup ? String(obSolanaRouterMode || "auto").toLowerCase().trim() : "auto";
      const routerMode = routerModeRaw === "metis" ? "jupiter" : routerModeRaw;
      const hydrationRouteMode = isPolkadotHydration ? normalizeHydrationRouteMode(obHydrationRouteMode) : "auto";
      const ultraUrl = `${apiBase}/api/solana_dex/jupiter/ultra_orderbook?symbol=${encodeURIComponent(sym)}&depth=${encodeURIComponent(
        String(depth)
      )}${forceQ}&_ts=${Date.now()}`;
      const jupUrl = `${apiBase}/api/solana_dex/jupiter/orderbook?symbol=${encodeURIComponent(sym)}&depth=${encodeURIComponent(
        String(depth)
      )}${forceQ}&_ts=${Date.now()}`;
      const rayUrl = `${apiBase}/api/solana_dex/raydium/orderbook?symbol=${encodeURIComponent(sym)}&depth=${encodeURIComponent(
        String(depth)
      )}${forceQ}&_ts=${Date.now()}`;
      const hydrationStatusUrl = `${apiBase}/api/polkadot_dex/hydration/status?symbol=${encodeURIComponent(sym)}&_ts=${Date.now()}`;
      const hydrationOrderbookUrl = `${apiBase}/api/polkadot_dex/hydration/orderbook?symbol=${encodeURIComponent(sym)}&depth=${encodeURIComponent(
        String(depth)
      )}&route_mode=${encodeURIComponent(hydrationRouteMode)}${forceQ}&_ts=${Date.now()}`;
      const counterpartyParts = counterpartyPairParts(sym);
      const counterpartySymbol = counterpartyRequestSymbol;
      const counterpartyOrderbookUrl = `${apiBase}/api/counterparty/orderbook?symbol=${encodeURIComponent(counterpartySymbol)}&depth=${encodeURIComponent(
        String(depth)
      )}&open_only=true${forceQ}&_ts=${Date.now()}`;
      const robinhoodChainOrderbookUrl = `${apiBase}/api/robinhood_chain/orderbook?symbol=${encodeURIComponent(sym)}&depth=${encodeURIComponent(
        String(Math.min(depth, 5))
      )}&force_refresh=${opts.force ? "true" : "false"}&_ts=${Date.now()}`;

      if (isPolkadotHydration) {
        const sr = await fetch(hydrationStatusUrl, { signal: ac.signal, cache: "no-store" });
        if (!sr.ok) {
          const txt = await sr.text().catch(() => "");
          throw new Error(txt || `Hydration status HTTP ${sr.status}`);
        }
        const statusData = await sr.json();
        setHydrationStatus(statusData || null);

        const quoteAvailable = isHydrationQuoteAvailable(statusData);

        // Do not block the orderbook on broad Hydration quote status alone.
        // UTTT-HDX can be served by the backend manual XYK/live-pool route even when
        // generic SDK router quotes are disabled to protect RPC quota.
        // Unsupported generic pairs will still be rejected by the orderbook endpoint below.
        if (!quoteAvailable) {
          setObActiveRouter(null);
        }
      }

      const url = isSolJup
        ? (routerMode === "raydium" ? rayUrl : (routerMode === "jupiter" ? jupUrl : (routerMode === "ultra" ? ultraUrl : ultraUrl)))
        : isSolRay
          ? rayUrl
          : isPolkadotHydration
            ? hydrationOrderbookUrl
            : isCounterpartyVenueKey(v)
              ? counterpartyOrderbookUrl
              : isRobinhoodChainVenueKey(v)
                ? robinhoodChainOrderbookUrl
                : `${apiBase}/api/market/orderbook?venue=${encodeURIComponent(v)}&symbol=${encodeURIComponent(
                  sym
                )}&depth=${encodeURIComponent(String(depth))}${forceQ}&_ts=${Date.now()}`;

      const shouldFallbackToRaydium = (txt) => {
        const low = String(txt || "").toLowerCase();
        return isSolJup && (
          low.includes("token_not_tradable") ||
          low.includes("not tradable") ||
          low.includes("no_quote_levels") ||
          low.includes("no routable jupiter quotes") ||
          low.includes("jupiter_quote_http_error") ||
          low.includes("jupiter_ultra_order_http_error") ||
          low.includes("failed to get quotes") ||
          low.includes("jupiterultra")
        );
      };

      const throwRateLimit = async (response) => {
        const txt = await response.text().catch(() => "");
        if (counterpartyRequest) {
          const retryAfterRaw = response.headers?.get?.("retry-after") || "";
          const retryAfterMs = counterpartyRetryAfterMs(retryAfterRaw);
          const err = new Error(txt || "HTTP 429 Too Many Requests");
          err.counterpartyRateLimited = true;
          err.retryAfterMs = retryAfterMs;
          err.counterpartyRateLimitSource = "counterparty_http_response";
          throw err;
        }
        cooldownPowRef.current = clamp((cooldownPowRef.current || 0) + 1, 0, 6);
        const backoffMs = Math.min(300000, 15000 * Math.pow(2, cooldownPowRef.current));
        cooldownUntilRef.current = Date.now() + backoffMs;
        throw new Error(txt || "HTTP 429 Too Many Requests");
      };

      if (isPolkadotHydration || isRobinhoodChainVenueKey(v)) {
        requestTimeoutId = window.setTimeout(() => {
          requestTimedOut = true;
          try { ac.abort(); } catch { /* ignore */ }
        }, isRobinhoodChainVenueKey(v) ? 30000 : 45000);
      }

      let r = await fetch(url, { signal: ac.signal });
      if (requestTimeoutId) {
        window.clearTimeout(requestTimeoutId);
        requestTimeoutId = null;
      }
      let usedVenue = v;
      let usedRouter = isSolJup
        ? (routerMode === "raydium" ? "raydium" : (routerMode === "jupiter" ? "jupiter" : "ultra"))
        : isCounterpartyVenueKey(v)
          ? "counterparty"
          : isRobinhoodChainVenueKey(v)
            ? "0x"
            : (isSolRay ? "raydium" : v || null);

      // handle 429 explicitly (cooldown)
      if (r.status === 429) await throwRateLimit(r);

      if (!r.ok && isSolJup) {
        const txt = await r.text().catch(() => "");
        if (routerMode === "auto") {
          if (usedRouter === "ultra" && shouldFallbackToRaydium(txt)) {
            r = await fetch(rayUrl, { signal: ac.signal });
            usedVenue = "solana_raydium";
            usedRouter = "raydium";
          } else {
            throw new Error(txt || `HTTP ${r.status}`);
          }
        } else if (routerMode !== "jupiter" && routerMode !== "ultra" && shouldFallbackToRaydium(txt)) {
          r = await fetch(rayUrl, { signal: ac.signal });
          usedVenue = "solana_raydium";
          usedRouter = "raydium";
        } else {
          throw new Error(txt || `HTTP ${r.status}`);
        }
      }

      if (r.status === 429) await throwRateLimit(r);

      if (!r.ok) {
        const txt = await r.text().catch(() => "");
        throw new Error(txt || `HTTP ${r.status}`);
      }

      const data = await r.json();

      if (counterpartyRequest && (data?.rate_limited === true || data?.rate_limit?.active === true)) {
        const retryAfterMs = Number.isFinite(Number(data?.rate_limit?.retry_after_s))
          ? Math.max(0, Number(data.rate_limit.retry_after_s) * 1000)
          : counterpartyRetryAfterMs(data?.rate_limit?.retry_after_raw);
        const retryAtMs = counterpartyRetryAtMs(data?.rate_limit?.retry_at);
        const backendHasSnapshot = data?.stale === true && Array.isArray(data?.asks) && Array.isArray(data?.bids);
        const localSnapshot = counterpartyLastGoodRef.current.get(counterpartyCacheKey) || null;
        const staleSnapshot = backendHasSnapshot
          ? {
              asks: normalizeSide(data?.asks || []),
              bids: normalizeSide(data?.bids || []),
              meta: {
                venue: v,
                symbol: data?.symbol || counterpartyRequestSymbol,
                baseAsset: data?.base_asset || data?.baseAsset || counterpartyParts.base,
                quoteAsset: data?.quote_asset || data?.quoteAsset || counterpartyParts.quote,
                liquidityCounts: data?.liquidity_counts || data?.counts || null,
                sources: data?.sources || null,
                stale: true,
                snapshotAgeS: Number.isFinite(Number(data?.snapshot_age_s)) ? Number(data.snapshot_age_s) : null,
                snapshotSource: String(data?.snapshot_source || "last_good_memory_cache"),
              },
            }
          : localSnapshot;

        enterCounterpartyCooldown({
          retryAfterMs,
          retryAtMs,
          stale: !!staleSnapshot,
          source: data?.rate_limit?.source || "counterparty_upstream",
          reason: data?.stale_reason || "Counterparty upstream returned HTTP 429.",
          snapshotAgeS: backendHasSnapshot ? data?.snapshot_age_s : localSnapshot?.snapshotAgeS,
          snapshotSource: backendHasSnapshot ? data?.snapshot_source : localSnapshot?.snapshotSource,
        });

        if (staleSnapshot) {
          setObAsks(staleSnapshot.asks || []);
          setObBids(staleSnapshot.bids || []);
          setOrderBookMeta({ ...(staleSnapshot.meta || {}), stale: true });
          setObActiveRouter("counterparty");
          setObError(null);
          snapToCenterAnchors();
        } else {
          setObError("Counterparty API cooldown active. No last successful snapshot is available for this pair yet.");
        }
        return;
      }

      // Live success: clear cooldown and reset the exponential sequence.
      cooldownPowRef.current = 0;
      cooldownUntilRef.current = 0;
      setCounterpartyResilience(null);
      setCooldownClock(Date.now());

      setHydrationLiquidityWarning(isPolkadotHydration ? buildHydrationLowLiquidityWarning(data) : null);
      const responseRouter = String(data?.router || "").toLowerCase().trim();
      if (usedRouter === "ultra") {
        setObActiveRouter("ultra");
      } else if (responseRouter === "ultra" || responseRouter === "jupiter" || responseRouter === "metis" || responseRouter === "raydium") {
        setObActiveRouter(responseRouter);
      } else if (isPolkadotHydration && responseRouter) {
        setObActiveRouter(responseRouter);
      } else {
        setObActiveRouter(usedRouter);
      }

      // DEX-only formatting hints (opt-in by venue)
      if (isSolJup || usedVenue === "solana_raydium" || isSolRay || isPolkadotHydration || isCounterpartyVenueKey(usedVenue) || isRobinhoodChainVenueKey(usedVenue)) {
        const inferLevelDecimals = (levels) => {
          try {
            let best = 0;
            for (const lvl of Array.isArray(levels) ? levels : []) {
              const px = Number(lvl?.price);
              if (!Number.isFinite(px) || px <= 0) continue;
              const s = px.toFixed(12).replace(/0+$/g, "").replace(/\.$/g, "");
              const i = s.indexOf(".");
              const d = i >= 0 ? (s.length - i - 1) : 0;
              if (d > best) best = d;
            }
            return Math.min(Math.max(best, 0), ORDERBOOK_PRICE_CLICK_CAP);
          } catch {
            return 0;
          }
        };

        const pdApi = Number(data?.priceDecimals);
        const pdLevels = inferLevelDecimals([...(data?.asks || []), ...(data?.bids || [])]);
        const pd = Math.max(Number.isFinite(pdApi) ? pdApi : 0, pdLevels);
        if (Number.isFinite(pd) && pd > 0) setPriceDecimals(clamp(pd, 0, ORDERBOOK_PRICE_CLICK_CAP));

        const sd = Number(data?.sizeDecimals);
        if (Number.isFinite(sd)) setSizeDecimals(sd);
      }

      const nextAsks = normalizeSide(data?.asks || []);
      const nextBids = normalizeSide(data?.bids || []);
      const nextMeta = {
        venue: usedVenue,
        symbol: data?.symbol || data?.resolvedSymbol || sym,
        baseAsset: data?.base_asset || data?.baseAsset || (isRobinhoodChainVenueKey(usedVenue) ? robinhoodChainPairParts(sym).base : orderBookPairParts(sym).base),
        quoteAsset: data?.quote_asset || data?.quoteAsset || (isRobinhoodChainVenueKey(usedVenue) ? robinhoodChainPairParts(sym).quote : orderBookPairParts(sym).quote),
        mechanism: data?.market?.mechanism || data?.mechanism || null,
        marketStatus: data?.market_status || null,
        orderbookEnabled: true,
        liquidityCounts: data?.liquidity_counts || data?.counts || null,
        sources: data?.sources || data?.route_sources || null,
        synthetic: data?.synthetic === true,
        quoteOnly: data?.quote_only === true,
        restingOrder: data?.resting_order === true,
        provider: data?.provider || null,
        cached: data?.cached === true,
        cacheMixed: data?.cache_mixed === true,
        fetchedAt: data?.fetched_at || null,
        bestBid: data?.best_bid ?? null,
        bestAsk: data?.best_ask ?? null,
        spreadBps: data?.spread_bps ?? null,
        warningCount: Number(data?.warning_count || 0),
        stale: data?.stale === true,
        snapshotAgeS: 0,
        snapshotSource: data?.snapshot_source || "live",
      };
      setOrderBookMeta(nextMeta);
      setObAsks(nextAsks);
      setObBids(nextBids);

      if (counterpartyRequest) {
        const cache = counterpartyLastGoodRef.current;
        if (cache.size >= 20 && !cache.has(counterpartyCacheKey)) {
          const oldestKey = cache.keys().next().value;
          if (oldestKey) cache.delete(oldestKey);
        }
        cache.set(counterpartyCacheKey, {
          asks: nextAsks,
          bids: nextBids,
          meta: nextMeta,
          ts: Date.now(),
          snapshotAgeS: 0,
          snapshotSource: "frontend_last_good",
        });
      }

      snapToCenterAnchors();
    } catch (e) {
      // ignore abort errors
      if (requestTimeoutId) {
        window.clearTimeout(requestTimeoutId);
        requestTimeoutId = null;
      }
      const msg = String(e?.message || "");
      if (msg.toLowerCase().includes("aborted") && !requestTimedOut) {
        return;
      }

      const counterpartyRateLimited = counterpartyRequest && (e?.counterpartyRateLimited === true || msg.toLowerCase().includes("429"));
      if (counterpartyRateLimited) {
        const cached = counterpartyLastGoodRef.current.get(counterpartyCacheKey) || null;
        if (!counterpartyResilience?.active) {
          enterCounterpartyCooldown({
            retryAfterMs: e?.retryAfterMs,
            stale: !!cached,
            source: e?.counterpartyRateLimitSource || "counterparty_fetch_error",
            reason: e?.message || "HTTP 429 Too Many Requests",
            snapshotSource: cached?.snapshotSource,
          });
        }
        if (cached) {
          setObAsks(cached.asks || []);
          setObBids(cached.bids || []);
          setOrderBookMeta({ ...(cached.meta || {}), stale: true, snapshotSource: cached.snapshotSource || "frontend_last_good" });
          setObActiveRouter("counterparty");
          setObError(null);
        } else {
          setObError("Counterparty API cooldown active. No last successful snapshot is available for this pair yet.");
        }
        return;
      }

      setObAsks([]);
      setObBids([]);
      setOrderBookMeta(null);
      setHydrationLiquidityWarning(null);

      setObActiveRouter(null);
      const raw = requestTimedOut
        ? (isRobinhoodChainVenueKey(v)
            ? "Robinhood Chain synthetic quote sampling timed out after 30s. Retry once after the provider backoff window."
            : "Hydration orderbook request timed out after 45s. The backend may still be probing slow quote samples; try Refresh once, then test depth=1 from PowerShell if this repeats.")
        : (e?.message || "Failed to load order book");
      const pretty = formatOrderBookError(raw, venueLabel || effectiveVenue);
      setObError(pretty);

      // If we detected a "pair not found", stop auto-refresh hammering until symbol/venue changes or user forces refresh
      const plow = String(pretty).toLowerCase();
      if (plow.startsWith("pair not found at") || plow.startsWith("unknown token symbol") || plow.startsWith("ambiguous token symbol")) {
        pairNotFoundRef.current = true;
      }
    } finally {
      setObLoading(false);
      inFlightRef.current = false;
    }
  }

  useEffect(() => {
    if (!obLoading && !obError) snapToCenterAnchors();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [asksSorted.length, bidsSorted.length]);

  // Auto-refresh:
  // - Debounce the initial fetch (prevents cascades when venue/symbol is switching)
  // - Do NOT refresh on draft typing (only committed obSymbol)
  useEffect(() => {
    if (!obAutoRefresh) return;

    const v = String(effectiveVenue || "").toLowerCase().trim();
    const sym = String(obSymbol || "").trim();
    if (!v || !sym) return;

    const sec = clamp(Number(obAutoSeconds) || 30, 1, 300);
    const ms = sec * 1000;

    let cancelled = false;

    const initial = setTimeout(() => {
      if (cancelled) return;
      if (document.hidden) return;
      void fetchOrderBook();
    }, 300);

    const t = setInterval(() => {
      if (cancelled) return;
      if (document.hidden) return;
      void fetchOrderBook();
    }, ms);

    return () => {
      cancelled = true;
      clearTimeout(initial);
      clearInterval(t);
      try {
        if (abortRef.current) abortRef.current.abort();
      } catch {
        // ignore
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [obAutoRefresh, obAutoSeconds, effectiveVenue, obSymbol, obDepth, apiBase, obSolanaRouterMode, obHydrationRouteMode, robinhoodChainMarkets.length]);

  function onDragMouseDown(e) {
    if (inlineMode || locked) return;
    e.preventDefault();

    dragStateRef.current = { startX: e.clientX, startY: e.clientY, startBox: { ...box } };

    const onMove = (ev) => {
      const st = dragStateRef.current;
      if (!st) return;
      const dx = ev.clientX - st.startX;
      const dy = ev.clientY - st.startY;
      setBox(clampBox({ ...st.startBox, x: st.startBox.x + dx, y: st.startBox.y + dy }));
    };

    const onUp = () => {
      dragStateRef.current = null;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }

  function onResizeMouseDown(e) {
    if (inlineMode || locked) return;
    e.preventDefault();
    e.stopPropagation();

    const start = { ...box };
    const startRight = start.x + start.w;

    resizeStateRef.current = { startX: e.clientX, startY: e.clientY, startBox: start, startRight };

    const onMove = (ev) => {
      const st = resizeStateRef.current;
      if (!st) return;

      const dx = ev.clientX - st.startX;
      const dy = ev.clientY - st.startY;

      const rawW = st.startBox.w - dx;
      const rawH = st.startBox.h + dy;

      const b = getGutterBounds();
      const w = clamp(rawW, MIN_W, Math.min(MAX_W, b.maxX - b.minX));
      const h = clamp(rawH, MIN_H, Math.min(MAX_H, b.maxY - b.minY));

      const x = st.startRight - w;
      const y = st.startBox.y;

      setBox(clampBox({ x, y, w, h }));
      snapToCenterAnchors();
    };

    const onUp = () => {
      resizeStateRef.current = null;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }

  function dispatchCounterpartyBookPick(row, pick) {
    if (!isCounterpartyVenue || typeof window === "undefined" || !row || typeof row !== "object") return;
    try {
      window.dispatchEvent(new CustomEvent(COUNTERPARTY_ORDERBOOK_PICK_EVENT, {
        detail: {
          symbol: orderBookMeta?.symbol || obSymbol,
          row,
          pick: String(pick || "price"),
          liquidity_type: counterpartyLiquidityType(row),
          execution_mode: counterpartyExecutionMode,
        },
      }));
    } catch {
      // Event bridge is best-effort; regular App.jsx callbacks still receive price/qty.
    }
  }

  function dispatchRobinhoodChainBookPick(row, pick) {
    if (!isRobinhoodChainVenue || typeof window === "undefined" || !row || typeof row !== "object") return;
    try {
      window.dispatchEvent(new CustomEvent(ROBINHOOD_CHAIN_ORDERBOOK_PICK_EVENT, {
        detail: {
          symbol: orderBookMeta?.symbol || obSymbol,
          row,
          pick: String(pick || "price"),
          book_side: String(row?.side || "").trim().toLowerCase(),
          synthetic: row?.synthetic === true,
          quote_only: row?.quote_only === true,
        },
      }));
    } catch {
      // The regular App callbacks still receive price/qty if the event bridge is unavailable.
    }
  }

  function handlePickPrice(px, row = null) {
    if (isCounterpartyVenue) {
      const exact = counterpartyRowExactPriceText(row || { price: px });
      if (!exact || !Number.isFinite(Number(exact)) || Number(exact) <= 0) return;
      const d = counterpartyPricePrecisionDecimals(exact);
      if (typeof onPickPrice === "function") {
        onPickPrice(exact, exact, {
          priceDecimals: d,
          priceIncrement: null,
          exactCounterpartyPrice: true,
          priceSource: String(row?.price_source || "").trim() || null,
        });
      }
      dispatchCounterpartyBookPick(row, "price");
      return;
    }

    // Normalize clicked price when rules are known; otherwise pass through.
    const outPx = normPriceByRules(px);
    const n = Number(outPx);
    if (!Number.isFinite(n)) return;

    // Preserve decimals for venues where order rules may be unknown.
    // Some ticket implementations format clicked prices using "known" decimals
    // (which may default to 0), causing whole-number rounding.
    const d = Number.isFinite(Number(priceDecimals))
      ? clamp(Number(priceDecimals), 0, ORDERBOOK_PRICE_CLICK_CAP)
      : null;
    const pxStr = d !== null ? n.toFixed(clamp(d, 0, 18)) : String(outPx);

    if (typeof onPickPrice === "function") {
      onPickPrice(n, pxStr, { priceDecimals: d, priceIncrement });
    }
    dispatchCounterpartyBookPick(row, "price");
    dispatchRobinhoodChainBookPick(row, "price");
  }

  function handlePickQty(q, row = null, pick = "size") {
    if (typeof onPickQty === "function" && Number.isFinite(Number(q))) onPickQty(Number(q));
    dispatchCounterpartyBookPick(row, pick);
    dispatchRobinhoodChainBookPick(row, pick);
  }

  const depthN = Math.max(1, Math.min(200, Number(obDepth) || 25));
  const asksFiltered = isCounterpartyVenue
    ? asksSorted.filter((row) => counterpartyRowMatchesFilter(row, counterpartyLiquidityFilter))
    : asksSorted;
  const bidsFiltered = isCounterpartyVenue
    ? bidsSorted.filter((row) => counterpartyRowMatchesFilter(row, counterpartyLiquidityFilter))
    : bidsSorted;
  const asksView = asksFiltered.slice(0, depthN);
  const bidsView = bidsFiltered.slice(0, depthN);
  const hasLiveBookRows = asksView.length > 0 || bidsView.length > 0;
  const liveBookWrapMinHeight = hasLiveBookRows ? 96 : 44;

  const BOTTOM_SPACER = 0;
  const SHELL_PAD = 8;
  const SHELL_PAD_BOTTOM = 10;

  const obShellStyle = inlineMode
    ? {
        ...styles.orderBookDock,
        width: "100%",
        maxWidth: "100%",
        height: "100%",
        maxHeight: "100%",
        resize: "none",
        overflow: "hidden",
        marginTop: 0,
        padding: SHELL_PAD,
        paddingBottom: SHELL_PAD_BOTTOM,
        display: "flex",
        flexDirection: "column",
        flex: "1 1 auto",
        minHeight: 0,
        minWidth: 0,
        boxSizing: "border-box",
        position: "relative",
      }
    : {
        ...styles.orderBookDock,
        width: box.w,
        height: box.h,
        resize: "none",
        overflow: "hidden",
        padding: SHELL_PAD,
        paddingBottom: SHELL_PAD_BOTTOM,
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        boxSizing: "border-box",
        position: "relative",
      };

  const fixedWrapperStyle = inlineMode
    ? {
        width: "100%",
        height: "100%",
        minHeight: 0,
        minWidth: 0,
        display: "flex",
        flexDirection: "column",
      }
    : { position: "fixed", left: box.x, top: box.y, zIndex: 60, userSelect: "none" };

  const approxChrome = 145;
  const remaining = Math.max(200, (inlineMode ? DEFAULT_H : box.h) - approxChrome);
  const half = Math.max(110, Math.floor(remaining / 2));

  const ASK = { border: "rgba(53, 208, 127, 0.55)", bg: "rgba(53, 208, 127, 0.06)" };
  const BID = { border: "rgba(224, 79, 79, 0.55)", bg: "rgba(224, 79, 79, 0.06)" };
  const OB_TEXT = "#ffffff";

  const BIDS_INNER_PAD_BOTTOM = 0;

  const asksWrapStyle = {
    ...styles.obTableWrap,
    maxHeight: "none",
    height: "100%",
    minHeight: liveBookWrapMinHeight,
    marginTop: 3,
    border: `1px solid ${ASK.border}`,
    background: ASK.bg,
    borderRadius: 10,
    boxSizing: "border-box",
    overflow: "auto",
    flex: hasLiveBookRows ? "1 0 96px" : "1 1 0",
  };

  const bidsWrapStyle = {
    ...styles.obTableWrap,
    maxHeight: "none",
    height: "100%",
    minHeight: liveBookWrapMinHeight,
    marginTop: 3,
    border: `1px solid ${BID.border}`,
    background: BID.bg,
    borderRadius: 10,
    paddingBottom: BIDS_INNER_PAD_BOTTOM,
    boxSizing: "border-box",
    overflow: "auto",
    flex: hasLiveBookRows ? "1 0 96px" : "1 1 0",
  };

  const obBodyWrapStyle = {
    display: "flex",
    flexDirection: "column",
    flex: "1 1 auto",
    minHeight: 0,
    overflow: "auto",
  };

  const obDepthStackStyle = {
    display: "grid",
    gridTemplateRows: hasLiveBookRows ? "minmax(96px, 1fr) minmax(96px, 1fr)" : "minmax(0, 1fr) minmax(0, 1fr)",
    gap: 6,
    flex: "1 1 auto",
    minHeight: hasLiveBookRows ? 210 : 0,
    overflow: "visible",
    paddingBottom: 0,
  };

  const obDepthPaneStyle = {
    display: "flex",
    flexDirection: "column",
    minHeight: hasLiveBookRows ? 100 : 0,
    overflow: "visible",
  };

  const GAP = 6;
  const rowStyle = { display: "flex", gap: GAP, flexWrap: "wrap", alignItems: "center" };
  const topControlsStyle = {
    display: "grid",
    gridTemplateColumns: "minmax(0, 1fr) auto auto",
    gap: 6,
    alignItems: "center",
    width: "100%",
    minWidth: 0,
  };

  const settingsBackdropStyle = {
    position: "absolute",
    inset: 0,
    zIndex: 8,
    background: "rgba(0,0,0,0.18)",
    borderRadius: 12,
    display: "flex",
    alignItems: "flex-start",
    justifyContent: "flex-end",
    padding: 8,
    boxSizing: "border-box",
  };

  const settingsPanelStyle = {
    width: "min(340px, calc(100% - 16px))",
    maxHeight: "calc(100% - 16px)",
    overflow: "auto",
    border: "1px solid rgba(255,255,255,0.16)",
    background: "rgba(14,17,22,0.98)",
    borderRadius: 12,
    boxShadow: "0 14px 40px rgba(0,0,0,0.55)",
    padding: 10,
    boxSizing: "border-box",
  };

  const settingsGridStyle = {
    display: "grid",
    gridTemplateColumns: "1fr",
    gap: 8,
    marginTop: 8,
  };

  const settingsFieldStyle = {
    display: "grid",
    gridTemplateColumns: "88px minmax(0, 1fr)",
    gap: 8,
    alignItems: "center",
    fontSize: 12,
  };

  const pillCompact = (extra = {}) => ({
    ...styles.pill,
    padding: "5px 7px",
    gap: 6,
    borderRadius: 10,
    ...extra,
  });

  const inputCompact = (extra = {}) => ({
    ...styles.input,
    padding: "4px 6px",
    ...extra,
  });

  const topPillCompact = (extra = {}) => pillCompact({
    width: "100%",
    minWidth: 0,
    boxSizing: "border-box",
    flexWrap: "nowrap",
    whiteSpace: "nowrap",
    overflow: "hidden",
    padding: "5px 7px",
    fontSize: 12,
    ...extra,
  });

  const btnCompact = (extra = {}) => ({
    ...styles.button,
    padding: "6px 8px",
    borderRadius: 10,
    ...extra,
  });
  const darkSelectStyle = {
    ...styles.select,
    minWidth: 0,
    padding: "4px 5px",
    fontSize: 12,
    background: "#101010",
    backgroundColor: "#101010",
    color: "#eaeaea",
    border: "1px solid rgba(255,255,255,0.14)",
  };
  const darkOptionStyle = { backgroundColor: "#101010", color: "#eaeaea" };

  const thCompact = { ...styles.obTh, padding: "6px 8px", fontSize: 12, color: OB_TEXT };
  const tdCompact = { ...styles.obTd, padding: "6px 8px", fontSize: 12, color: OB_TEXT };

  const asksTh = { ...thCompact };
  const bidsTh = { ...thCompact };

  const asksTd = (extra = {}) => ({ ...tdCompact, ...extra });
  const bidsTd = (extra = {}) => ({ ...tdCompact, ...extra });

  const asksTitleStyle = { ...styles.obSectionTitle, marginTop: 8, fontSize: 11, color: OB_TEXT };
  const bidsTitleStyle = { ...styles.obSectionTitle, marginTop: 8, fontSize: 11, color: OB_TEXT };

  const hydrationPriceStatusDisplay = hydrationPriceStatusView(hydrationPriceStatus, hydrationPriceStatusError);
  const hydrationPriceStatusStyle = hydrationPriceStatusDisplay.tone === "ok"
    ? { border: "1px solid rgba(46,204,113,0.20)", background: "rgba(46,204,113,0.06)", color: "#c9f7d7" }
    : { border: "1px solid rgba(241,196,15,0.20)", background: "rgba(241,196,15,0.06)", color: "#f7e8b0" };

  const displayedQuoteAsset = orderBookQuoteAsset();
  const displayedBaseAsset = isRobinhoodChainVenue
    ? String(orderBookMeta?.baseAsset || robinhoodChainPairParts(obSymbol).base).trim().toUpperCase()
    : normalizeOrderBookAsset(orderBookMeta?.baseAsset) || orderBookPairParts(obSymbol).base;
  const showDerivedUsd = Boolean(
    displayedQuoteAsset &&
    !isUsdValueQuote(displayedQuoteAsset) &&
    quoteUsdContext?.status !== "native_usd"
  );
  const counterpartyColumnCount = isCounterpartyVenue ? 4 : 2;

  function renderOrderBookPrice(row) {
    const usd = orderBookPriceUsd(row?.price);
    const usdText = usd !== null
      ? `${formatOrderBookUsd(usd)}${quoteUsdContext?.stale ? "*" : ""}`
      : "USD —";
    return (
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: 4,
          lineHeight: 1,
          minWidth: 0,
          whiteSpace: "nowrap",
          overflow: "hidden",
        }}
      >
        <span style={{ flex: "0 0 auto" }}>
          {fmtPriceCell(row?.price, row)}
          {displayedQuoteAsset ? <span style={{ opacity: 0.72, marginLeft: 4 }}>{displayedQuoteAsset}</span> : null}
          {isRobinhoodChainVenue && row?.synthetic ? (
            <span
              title="Synthetic 0x indicative quote sample; not a resting order"
              style={{ marginLeft: 5, padding: "1px 4px", borderRadius: 999, border: "1px solid rgba(34,211,238,0.55)", background: "rgba(34,211,238,0.10)", color: "#a5f3fc", fontSize: 8, fontWeight: 900, letterSpacing: 0.5 }}
            >
              SYNTH
            </span>
          ) : null}
        </span>
        {showDerivedUsd ? (
          <span
            style={{
              flex: "1 1 auto",
              minWidth: 0,
              overflow: "hidden",
              textOverflow: "ellipsis",
              fontSize: 10,
              opacity: quoteUsdContext?.stale ? 0.62 : 0.78,
            }}
          >
            ({usdText})
          </span>
        ) : null}
      </div>
    );
  }

  function counterpartyRowTitle(row) {
    const type = counterpartyLiquidityType(row);
    const unit = counterpartyLotSize(row);
    const lines = [
      `Type: ${type === "dispenser" ? "Counterparty dispenser" : type === "limit_order" ? "Counterparty protocol limit order" : "Unknown Counterparty liquidity"}`,
      `Price: ${counterpartyRowExactPriceText(row) || fmtPriceCell(row?.price, row)} ${displayedQuoteAsset || ""}`.trim(),
      `Remaining: ${fmtSizeCell(row?.size)} ${displayedBaseAsset || ""}`.trim(),
    ];
    if (unit !== null) lines.push(`Lot size: ${fmtSizeCell(unit)} ${displayedBaseAsset || ""}`.trim());
    const exactPrice = counterpartyRowExactPriceText(row);
    if (exactPrice) lines.push(`Audit precision: ${counterpartyPricePrecisionDecimals(exactPrice) ?? "unknown"} decimal place(s)`);
    if (row?.price_source) lines.push(`Price provenance: ${row.price_source}`);
    if (row?.source) lines.push(`Source: ${row.source}`);
    if (row?.status) lines.push(`Status: ${row.status}`);
    if (type === "unknown") lines.push("Unknown rows are display-only and are not executable as dispenser selections.");
    else if (type === "dispenser") lines.push("Execution: immediate purchase in complete lots. Exact payment is lot count × satoshirate; the rounded displayed price is informational only.");
    else lines.push("Execution: protocol limit-order context; clicking copies price for a new limit order.");
    return lines.join("\n");
  }

  function robinhoodChainRowTitle(row) {
    const routes = Array.isArray(row?.route_sources) ? row.route_sources.filter(Boolean).join(", ") : String(row?.route_source || "0x");
    const lines = [
      "Synthetic 0x indicative quote sample — not a resting order.",
      `Price: ${fmtPriceCell(row?.price, row)} ${displayedQuoteAsset || "USDG"}`.trim(),
      `Sample size: ${fmtSizeCell(row?.size)} ${displayedBaseAsset || "ETH"}`.trim(),
      `Direction: ${String(row?.input_amount || "—")} ${String(row?.input_asset || "—")} → ${String(row?.output_amount || "—")} ${String(row?.output_asset || "—")}`,
      `Route: ${routes || "0x"}`,
      `Cache: ${row?.cached ? "cached" : "fresh"}`,
      `Allowance: ${row?.allowance_required ? "required for later execution planning" : "not reported as required"}`,
      "RH-CHAIN.10D.0 remains review-only: no signing, submission, or order recording.",
    ];
    return lines.join("\n");
  }

  function counterpartyTypeBadge(row) {
    const type = counterpartyLiquidityType(row);
    const label = counterpartyLiquidityLabel(row);
    const style = type === "dispenser"
      ? { border: "1px solid rgba(64,196,255,0.40)", background: "rgba(64,196,255,0.10)", color: "#bcecff" }
      : type === "limit_order"
        ? { border: "1px solid rgba(178,132,255,0.40)", background: "rgba(178,132,255,0.10)", color: "#ddc8ff" }
        : { border: "1px solid rgba(241,196,15,0.35)", background: "rgba(241,196,15,0.08)", color: "#f7e8b0" };
    return (
      <span style={{ ...style, display: "inline-flex", padding: "2px 6px", borderRadius: 999, fontSize: 9, fontWeight: 900 }}>
        {label}
      </span>
    );
  }

  function refreshSelectedMarket(force = false) {
    const selectedMarket = String(obSymbol || "").trim();
    if (!selectedMarket) return;

    const now = Date.now();
    if (isCounterpartyVenue && now < (cooldownUntilRef.current || 0)) {
      setCooldownClock(now);
      return;
    }

    // Reset pair gating; Counterparty cooldown power resets only after success.
    pairNotFoundRef.current = false;
    if (!isCounterpartyVenue) {
      cooldownUntilRef.current = 0;
      cooldownPowRef.current = 0;
    }

    void fetchOrderBook({ force: !!force });
    void fetchOrderRules(selectedMarket);
  }

  const counterpartyCooldownRemainingMs = isCounterpartyVenue
    ? Math.max(0, Number(cooldownUntilRef.current || 0) - Number(cooldownClock || Date.now()))
    : 0;
  const counterpartyCooldownActive = isCounterpartyVenue && counterpartyCooldownRemainingMs > 0;
  const counterpartyCooldownLabel = counterpartyCooldownText(counterpartyCooldownRemainingMs);

  return (
    <div style={fixedWrapperStyle}>
      <div style={obShellStyle}>
        <div
          style={{
            ...styles.widgetTitleRow,
            cursor: inlineMode || locked ? "default" : "move",
            paddingBottom: 2,
            borderBottom: "1px solid #2a2a2a",
            marginBottom: 4,
          }}
          onMouseDown={onDragMouseDown}
          title={inlineMode ? "" : locked ? "Locked" : "Drag to move (snug gutter, no margins)"}
        >
          <h3 style={{ ...styles.widgetTitle, fontSize: 16, lineHeight: "18px" }}>Order Book</h3>
          <span
            style={{
              ...styles.widgetSub,
              fontSize: 11,
              flex: "1 1 auto",
              minWidth: 0,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={isCounterpartyVenue
              ? `Venue used: ${venueLabel || "—"} • Source: orders / dispensers • Asks: ${counterpartyLiquidityCounts.askDispensers} DISP / ${counterpartyLiquidityCounts.askLimitOrders} LIMIT • Bids: ${counterpartyLiquidityCounts.bidLimitOrders} LIMIT`
              : undefined}
          >
            Venue used: <b>{venueLabel || "—"}</b>
            {isSolJupVenue ? (
              <>
                {" "}• API router: <b>{
                  obActiveRouter
                    ? (String(obActiveRouter) === "ultra"
                        ? "Jupiter Ultra"
                        : (String(obActiveRouter) === "jupiter" || String(obActiveRouter) === "metis")
                            ? "Jupiter Metis"
                            : String(obActiveRouter).replace(/^./, (m) => m.toUpperCase()))
                    : (String(obSolanaRouterMode || "auto") === "auto"
                        ? "Auto"
                        : (String(obSolanaRouterMode || "auto") === "ultra"
                            ? "Jupiter Ultra"
                            : ((String(obSolanaRouterMode || "auto") === "jupiter" || String(obSolanaRouterMode || "auto") === "metis")
                                ? "Jupiter Metis"
                                : String(obSolanaRouterMode || "auto").replace(/^./, (m) => m.toUpperCase()))))
                }</b>
              </>
            ) : isPolkadotDexVenue ? (
              <>
                {" "}• Route: <b>{
                  obActiveRouter
                    ? String(obActiveRouter).replace(/_/g, " ")
                    : hydrationRouteModeLabel(obHydrationRouteMode)
                }</b>
              </>
            ) : isCounterpartyVenue ? (
              <>
                {" "}• Source: <b>orders / dispensers</b>
                {" "}• Asks: <b>{counterpartyLiquidityCounts.askDispensers} DISP / {counterpartyLiquidityCounts.askLimitOrders} LIMIT</b>
                {" "}• Bids: <b>{counterpartyLiquidityCounts.bidLimitOrders} LIMIT</b>
              </>
            ) : isRobinhoodChainVenue ? (
              <>
                {" "}• Provider: <b>{Array.isArray(selectedRobinhoodChainMarket?.providers) && selectedRobinhoodChainMarket.providers.length ? selectedRobinhoodChainMarket.providers.join(", ") : "database"}</b>
                {" "}• Market: <b>{selectedRobinhoodChainMarket?.orderbook_enabled ? "quote-only synthetic" : String(selectedRobinhoodChainMarket?.mechanism || "catalog").replaceAll("_", " ")}</b>
              </>
            ) : null}
          </span>
        </div>

        <div style={topControlsStyle}>
          <div
            style={topPillCompact()}
            title="Market follows the top-level MARKET selector. Change it in the main UTT header."
          >
            <span>Market</span>
            <b style={{ minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {obSymbol || "—"}
            </b>
          </div>

          <button
            type="button"
            style={btnCompact({ whiteSpace: "nowrap" })}
            onClick={() => setObSettingsOpen(true)}
            title="Order Book settings"
          >
            ⚙ Settings
          </button>

          <button
            style={{ ...btnCompact(), ...((obLoading || counterpartyCooldownActive) ? styles.buttonDisabled : {}) }}
            disabled={obLoading || counterpartyCooldownActive}
            title={counterpartyCooldownActive ? `Counterparty cooldown active. Refresh available in ${counterpartyCooldownLabel}.` : "Refresh OrderBook"}
            onClick={() => refreshSelectedMarket(true)}
          >
            {obLoading ? "Loading…" : counterpartyCooldownActive ? `Retry ${counterpartyCooldownLabel}` : "Refresh"}
          </button>
        </div>

        <div style={{ ...rowStyle, marginTop: 6, gap: 8, flexWrap: "nowrap", minWidth: 0, overflow: "hidden" }}>
          <span
            style={{ ...styles.muted, fontSize: 11, flex: "1 1 auto", minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
            title={isCounterpartyVenue
              ? `Counterparty liquidity filter: ${counterpartyLiquidityFilterLabel(counterpartyLiquidityFilter)}. DISP = immediate dispenser liquidity. LIMIT = open protocol order. Ticket mode: ${counterpartyExecutionMode === "limit_order" ? "Limit Order" : "Dispenser Purchase"}.`
              : isRobinhoodChainVenue
                ? `${selectedRobinhoodChainMarket?.symbol || robinhoodChainPairParts(obSymbol).symbol || "Robinhood Chain market"} · ${String(selectedRobinhoodChainMarket?.mechanism || "catalog").replaceAll("_", " ").toUpperCase()} · ${robinhoodChainMarketStatusLabel(selectedRobinhoodChainMarket)} · execution ${selectedRobinhoodChainMarket?.execution_enabled ? "LIVE VERIFIED" : "NO"} · identity TokenRegistry · capabilities database`
                : undefined}
          >
            Depth <b>{obDepth}</b> • Auto <b>{obAutoRefresh ? `${obAutoSeconds}s` : "off"}</b>
            {isSolJupVenue ? <> • Router <b>{String(obSolanaRouterMode || "auto")}</b></> : null}
            {isPolkadotDexVenue ? <> • Route <b>{hydrationRouteModeLabel(obHydrationRouteMode)}</b></> : null}
            {isCounterpartyVenue ? (
              <>
                {" "}• Filter <b>{counterpartyLiquidityFilterLabel(counterpartyLiquidityFilter)}</b>
                {" "}• Mode <b>{counterpartyExecutionMode === "limit_order" ? "Limit Order" : "Dispenser Purchase"}</b>
                {counterpartyResilience?.stale || orderBookMeta?.stale ? <> • Book <b>STALE</b></> : null}
                {counterpartyCooldownActive ? <> • Retry <b>{counterpartyCooldownLabel}</b></> : null}
                {!counterpartyCooldownActive && (counterpartyResilience?.stale || orderBookMeta?.stale) ? <> • <b>Refresh available</b></> : null}
              </>
            ) : null}
            {isRobinhoodChainVenue ? (
              <>
                {" "}• <b>RH-EVM</b>
                {" "}• <b>{selectedRobinhoodChainMarket?.symbol || robinhoodChainPairParts(obSymbol).symbol || "—"}</b>
                {" "}• <b>{String(selectedRobinhoodChainMarket?.mechanism || "catalog").replaceAll("_", " ").toUpperCase()}</b>
                {" "}• <b>{robinhoodChainMarketStatusLabel(selectedRobinhoodChainMarket)}</b>
                {" "}• Execution <b>{selectedRobinhoodChainMarket?.execution_enabled ? "LIVE VERIFIED" : "NO"}</b>
                {selectedRobinhoodChainMarket?.orderbook_enabled ? (
                  <>
                    {" "}• <b>0x</b>
                    {" "}• <b>SYNTH</b>
                    {" "}• <b>QUOTE ONLY</b>
                    {" "}• <b>NOT RESTING</b>
                    {" "}• {orderBookMeta?.cached ? <b>CACHED</b> : orderBookMeta?.cacheMixed ? <b>MIXED CACHE</b> : <b>FRESH</b>}
                    {Array.isArray(orderBookMeta?.sources) && orderBookMeta.sources.length ? <> • Route <b>{orderBookMeta.sources.join(", ")}</b></> : null}
                    {Number(orderBookMeta?.warningCount || 0) > 0 ? <> • Warnings <b>{Number(orderBookMeta.warningCount)}</b></> : null}
                    {orderBookMeta?.spreadBps !== null && orderBookMeta?.spreadBps !== undefined ? <> • Spread <b>{Number(orderBookMeta.spreadBps).toFixed(2)} bps</b></> : null}
                  </>
                ) : (
                  <>
                    {" "}• <b>NO PROVIDER FETCH</b>
                  </>
                )}
              </>
            ) : null}
            {showDerivedUsd ? (
              <>
                {" "}• {displayedQuoteAsset}/USD <b>{
                  Number.isFinite(Number(quoteUsdContext?.priceUsd)) && Number(quoteUsdContext?.priceUsd) > 0
                    ? `${formatOrderBookUsd(quoteUsdContext.priceUsd)}${quoteUsdContext?.stale ? " stale" : ""}`
                    : "unavailable"
                }</b>
              </>
            ) : null}
          </span>
          {isPolkadotDexVenue ? (
            <span
              title={hydrationPriceStatusDisplay.title}
              style={{
                ...hydrationPriceStatusStyle,
                flex: "0 1 auto",
                minWidth: 0,
                maxWidth: "54%",
                padding: "2px 8px",
                borderRadius: 999,
                fontSize: 10,
                lineHeight: 1.15,
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
              }}
            >
              <b>Prices</b> {hydrationPriceStatusDisplay.label}
            </span>
          ) : null}
        </div>

        {obSettingsOpen ? (
          <div style={settingsBackdropStyle} onMouseDown={(e) => { if (e.target === e.currentTarget) setObSettingsOpen(false); }}>
            <div style={settingsPanelStyle} onMouseDown={(e) => e.stopPropagation()}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
                <b>Order Book Settings</b>
                <button type="button" style={btnCompact()} onClick={() => setObSettingsOpen(false)}>Close</button>
              </div>

              <div style={settingsGridStyle}>
                <label style={settingsFieldStyle}>
                  <span>Depth</span>
                  <input
                    style={inputCompact({ width: "100%" })}
                    type="number"
                    min="1"
                    max="200"
                    value={obDepth}
                    onChange={(e) => setObDepth(e.target.value)}
                  />
                </label>

                <label style={settingsFieldStyle}>
                  <span>Auto refresh</span>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
                    <input type="checkbox" checked={obAutoRefresh} onChange={(e) => setObAutoRefresh(e.target.checked)} />
                    <span>{obAutoRefresh ? "Enabled" : "Disabled"}</span>
                  </span>
                </label>

                <label style={settingsFieldStyle}>
                  <span>Every</span>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                    <input
                      style={inputCompact({ width: 84 })}
                      type="number"
                      min="30"
                      max="300"
                      value={obAutoSeconds}
                      disabled={!obAutoRefresh}
                      onChange={(e) => setObAutoSeconds(e.target.value)}
                    />
                    <span style={styles.muted}>sec</span>
                  </span>
                </label>

                {isCounterpartyVenue ? (
                  <>
                    <label style={settingsFieldStyle}>
                      <span>Liquidity</span>
                      <select
                        style={{ ...darkSelectStyle, width: "100%" }}
                        value={counterpartyLiquidityFilter}
                        onChange={(e) => setCounterpartyLiquidityFilter(normalizeCounterpartyLiquidityFilter(e.target.value))}
                        title="Filter the combined Counterparty book by execution mechanism"
                      >
                        <option value="all" style={darkOptionStyle}>All Liquidity</option>
                        <option value="dispensers" style={darkOptionStyle}>Dispensers</option>
                        <option value="limit_orders" style={darkOptionStyle}>Limit Orders</option>
                      </select>
                    </label>
                    <div
                      style={{
                        fontSize: 10,
                        lineHeight: 1.3,
                        color: "rgba(255,255,255,0.62)",
                        padding: "0 2px",
                      }}
                    >
                      <b>DISP</b> immediate dispenser liquidity • <b>LIMIT</b> open protocol order
                    </div>
                  </>
                ) : null}

                {isSolJupVenue ? (
                  <label style={settingsFieldStyle}>
                    <span>Router</span>
                    <select
                      style={{ ...darkSelectStyle, width: "100%" }}
                      value={obSolanaRouterMode}
                      onChange={(e) => setObSolanaRouterMode(e.target.value)}
                      title="Order book quote source"
                    >
                      <option value="auto" style={darkOptionStyle}>Auto</option>
                      <option value="ultra" style={darkOptionStyle}>Jupiter Ultra</option>
                      <option value="jupiter" style={darkOptionStyle}>Jupiter Metis</option>
                      <option value="raydium" style={darkOptionStyle}>Raydium</option>
                    </select>
                  </label>
                ) : null}

                {isPolkadotDexVenue ? (
                  <label style={settingsFieldStyle}>
                    <span>Route</span>
                    <select
                      style={{ ...darkSelectStyle, width: "100%" }}
                      value={obHydrationRouteMode}
                      onChange={(e) => setObHydrationRouteMode(normalizeHydrationRouteMode(e.target.value))}
                      title="Hydration route source. Auto uses manual XYK for configured custom pairs; generic SDK pairs stay blocked unless the backend explicitly enables router quotes."
                    >
                      <option value="auto" style={darkOptionStyle}>Auto</option>
                      <option value="sdk" style={darkOptionStyle}>SDK</option>
                      <option value="isolated_helper" style={darkOptionStyle}>Isolated</option>
                      <option value="manual_xyk" style={darkOptionStyle}>Manual XYK</option>
                    </select>
                  </label>
                ) : null}
              </div>
            </div>
          </div>
        ) : null}



        {obError && <div style={{ ...styles.codeError, marginTop: 6, padding: 8 }}>{obError}</div>}

        <div style={obBodyWrapStyle}>
          <div style={obDepthStackStyle}>
            <div style={obDepthPaneStyle}>
              <div style={asksTitleStyle}>
                Asks
                {isCounterpartyVenue ? (
                  <span style={{ opacity: 0.72, marginLeft: 6 }}>
                    {asksView.filter((row) => counterpartyLiquidityType(row) === "dispenser").length} DISP • {asksView.filter((row) => counterpartyLiquidityType(row) === "limit_order").length} LIMIT
                  </span>
                ) : null}
              </div>
              <div ref={asksWrapRef} style={asksWrapStyle}>
                <table style={styles.obInnerTable}>
                  <thead>
                    <tr>
                      <th style={asksTh}>Price{displayedQuoteAsset ? ` (${displayedQuoteAsset})` : ""}</th>
                      <th style={asksTh}>{isCounterpartyVenue ? "Remaining" : "Size"}</th>
                      {isCounterpartyVenue ? <th style={asksTh} title="Asset quantity dispensed per satoshirate payment increment. Purchases must use complete lots.">Lot</th> : null}
                      {isCounterpartyVenue ? <th style={asksTh}>Type</th> : null}
                    </tr>
                  </thead>
                  <tbody>
                    {asksView.map((x, idx) => {
                      const liquidityType = counterpartyLiquidityType(x);
                      const lotSize = counterpartyLotSize(x);
                      const rowTitle = isCounterpartyVenue ? counterpartyRowTitle(x) : isRobinhoodChainVenue ? robinhoodChainRowTitle(x) : "";
                      const remainingClickable = !isCounterpartyVenue || liquidityType === "limit_order";
                      return (
                        <tr key={`a-${idx}-${x?.tx_hash || x?.source || ""}`} title={rowTitle || undefined}>
                          <td
                            style={{ ...asksTd(), cursor: "pointer", userSelect: "none" }}
                            title={orderBookPriceTitle(x.price, x)}
                            onClick={() => handlePickPrice(x.price, x)}
                          >
                            {renderOrderBookPrice(x)}
                          </td>
                          <td
                            style={{
                              ...asksTd(),
                              cursor: remainingClickable ? "pointer" : "default",
                              userSelect: "none",
                            }}
                            title={remainingClickable ? "Click to set ticket Qty" : "Remaining dispenser inventory; use Lot for purchase quantity"}
                            onClick={() => {
                              if (remainingClickable) handlePickQty(x.size, x, "size");
                            }}
                          >
                            {fmtSizeCell(x.size)}
                          </td>
                          {isCounterpartyVenue ? (
                            <td
                              style={{
                                ...asksTd(),
                                cursor: liquidityType === "dispenser" && lotSize !== null ? "pointer" : "default",
                                userSelect: "none",
                              }}
                              title={liquidityType === "dispenser" && lotSize !== null ? "Click to set one complete dispenser lot as Qty" : "Not applicable to protocol limit orders"}
                              onClick={() => {
                                if (liquidityType === "dispenser" && lotSize !== null) handlePickQty(lotSize, x, "lot");
                              }}
                            >
                              {lotSize !== null ? fmtSizeCell(lotSize) : "—"}
                            </td>
                          ) : null}
                          {isCounterpartyVenue ? <td style={asksTd()}>{counterpartyTypeBadge(x)}</td> : null}
                        </tr>
                      );
                    })}
                    {asksView.length === 0 && (
                      <tr>
                        <td style={tdCompact} colSpan={counterpartyColumnCount}>
                          <span style={styles.muted}>
                            {isCounterpartyVenue && counterpartyLiquidityFilter === "dispensers"
                              ? "No dispenser asks loaded."
                              : isCounterpartyVenue && counterpartyLiquidityFilter === "limit_orders"
                                ? "No protocol limit-order asks loaded."
                                : "No asks loaded."}
                          </span>
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>


            <div style={obDepthPaneStyle}>
              <div style={bidsTitleStyle}>
                Bids
                {isCounterpartyVenue ? (
                  <span style={{ opacity: 0.72, marginLeft: 6 }}>
                    {bidsView.filter((row) => counterpartyLiquidityType(row) === "limit_order").length} LIMIT
                  </span>
                ) : null}
              </div>
              <div ref={bidsWrapRef} style={bidsWrapStyle}>
                <table style={styles.obInnerTable}>
                  <thead>
                    <tr>
                      <th style={bidsTh}>Price{displayedQuoteAsset ? ` (${displayedQuoteAsset})` : ""}</th>
                      <th style={bidsTh}>{isCounterpartyVenue ? "Remaining" : "Size"}</th>
                      {isCounterpartyVenue ? <th style={bidsTh} title="Dispenser lot size; not applicable to protocol limit orders.">Lot</th> : null}
                      {isCounterpartyVenue ? <th style={bidsTh}>Type</th> : null}
                    </tr>
                  </thead>
                  <tbody>
                    {bidsView.map((x, idx) => {
                      const rowTitle = isCounterpartyVenue ? counterpartyRowTitle(x) : isRobinhoodChainVenue ? robinhoodChainRowTitle(x) : "";
                      return (
                        <tr key={`b-${idx}-${x?.tx_hash || x?.source || ""}`} title={rowTitle || undefined}>
                          <td
                            style={{ ...bidsTd(), cursor: "pointer", userSelect: "none" }}
                            title={orderBookPriceTitle(x.price, x)}
                            onClick={() => handlePickPrice(x.price, x)}
                          >
                            {renderOrderBookPrice(x)}
                          </td>
                          <td
                            style={{ ...bidsTd(), cursor: "pointer", userSelect: "none" }}
                            title="Click to set ticket Qty"
                            onClick={() => handlePickQty(x.size, x, "size")}
                          >
                            {fmtSizeCell(x.size)}
                          </td>
                          {isCounterpartyVenue ? <td style={bidsTd()}>—</td> : null}
                          {isCounterpartyVenue ? <td style={bidsTd()}>{counterpartyTypeBadge(x)}</td> : null}
                        </tr>
                      );
                    })}
                    {bidsView.length === 0 && (
                      <tr>
                        <td style={tdCompact} colSpan={counterpartyColumnCount}>
                          <span style={styles.muted}>
                            {isCounterpartyVenue && counterpartyLiquidityFilter === "dispensers"
                              ? "Dispensers do not create bids."
                              : isCounterpartyVenue && counterpartyLiquidityFilter === "limit_orders"
                                ? "No protocol limit-order bids loaded."
                                : "No bids loaded."}
                          </span>
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>


          <div style={{ height: BOTTOM_SPACER, flex: "0 0 auto" }} />
        </div>

        {!inlineMode && (
          <div
            style={{ ...styles.obResizeHandle, left: 0, bottom: 0, borderRadius: 8 }}
            onMouseDown={onResizeMouseDown}
            title={locked ? "Locked" : "Resize from bottom-left"}
          />
        )}
      </div>
    </div>
  );
}
