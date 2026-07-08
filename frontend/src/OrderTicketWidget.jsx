// frontend/src/OrderTicketWidget.jsx

import { useEffect, useMemo, useRef, useState } from "react";
import { Connection, clusterApiUrl } from "@solana/web3.js";
import { useWallet } from "@solana/wallet-adapter-react";
import { UnifiedWalletButton } from "@jup-ag/wallet-adapter";
import { getOrderRules } from "./lib/api";
import { expandExponential } from "./lib/format";

// Auth (local token) — used to gate funds actions.
const UTT_AUTH_TOKEN_KEY = 'utt_auth_token_v1';
function getAuthToken() {
  try { return localStorage.getItem(UTT_AUTH_TOKEN_KEY) || ''; } catch { return ''; }
}

const LS_OT_SOL_WALLET = "utt_ot_sol_wallet_v1";
const LS_OT_SOL_ROUTER = "utt_ot_sol_router_v1";
const LS_OT_DOT_WALLET = "utt_ot_dot_wallet_v1";
const LS_OT_HYDRATION_ROUTE = "utt_ot_hydration_route_mode_v1";
const POLKADOT_APP_NAME = "UTT Unified Trading Terminal";
const HYDRATION_ROUTER_BOOK_SIDE_TOLERANCE_BPS = 2;

function getPreferredSolanaWalletKey() {
  try { return localStorage.getItem(LS_OT_SOL_WALLET) || "solflare"; } catch { return "solflare"; }
}
function setPreferredSolanaWalletKey(v) {
  try { localStorage.setItem(LS_OT_SOL_WALLET, String(v || "solflare")); } catch {}
}
function getPreferredSolanaRouterMode() {
  try {
    const raw = String(localStorage.getItem(LS_OT_SOL_ROUTER) || "auto").toLowerCase().trim();
    const v = raw === "jupiter" ? "metis" : raw; // back-compat
    return v === "ultra" || v === "metis" || v === "raydium" ? v : "auto";
  } catch {
    return "auto";
  }
}
function setPreferredSolanaRouterMode(v) {
  try {
    const next = String(v || "auto").toLowerCase().trim();
    localStorage.setItem(LS_OT_SOL_ROUTER, next === "ultra" || next === "metis" || next === "raydium" ? next : "auto");
  } catch {}
}

function normalizeHydrationRouteMode(v) {
  const raw = String(v || "auto").toLowerCase().trim();
  if (raw === "managed" || raw === "managed_sdk" || raw === "sdk_router" || raw === "sidecar") return "sdk";
  if (raw === "isolated" || raw === "helper") return "isolated_helper";
  if (raw === "manual" || raw === "xyk") return "manual_xyk";
  return raw === "sdk" || raw === "isolated_helper" || raw === "manual_xyk" ? raw : "auto";
}

function getPreferredHydrationRouteMode() {
  try { return normalizeHydrationRouteMode(localStorage.getItem(LS_OT_HYDRATION_ROUTE) || "auto"); } catch { return "auto"; }
}
function setPreferredHydrationRouteMode(v) {
  try { localStorage.setItem(LS_OT_HYDRATION_ROUTE, normalizeHydrationRouteMode(v)); } catch {}
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
  const syntheticFallback =
    p.syntheticFallback === true ||
    p.synthetic_fallback === true ||
    routerText.includes("synthetic_spot_fallback");
  const manualOrIsolated = !syntheticFallback && (
    sourceText.includes("live_pool_account") ||
    sourceText.includes("route_registry") ||
    sourceText.includes("manual") ||
    sourceText.includes("isolated") ||
    routerText.includes("manual_xyk") ||
    routerText.includes("manual_papi") ||
    routerText.includes("isolated")
  );
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

function isHydrationManualRoutePayload(payload) {
  const p = payload && typeof payload === "object" ? payload : {};
  const pool = (p.pool && typeof p.pool === "object") ? p.pool : {};
  const cfg = (p.orderbookConfig && typeof p.orderbookConfig === "object") ? p.orderbookConfig : {};
  const cfgSnake = (p.orderbook_config && typeof p.orderbook_config === "object") ? p.orderbook_config : {};
  const routerText = String(p.router || p.routeModeEffective || p.route_mode_effective || cfg.routeModeEffective || cfgSnake.route_mode_effective || pool.router || "").toLowerCase();
  const routeModeText = String(p.routeModeEffective || p.route_mode_effective || cfg.routeModeEffective || cfgSnake.route_mode_effective || "").toLowerCase();
  const sourceText = String(pool.source || p.source || cfg.source || cfgSnake.source || "").toLowerCase();
  const syntheticFallback =
    p.syntheticFallback === true ||
    p.synthetic_fallback === true ||
    routerText.includes("synthetic_spot_fallback") ||
    routeModeText.includes("synthetic_spot_fallback");
  if (syntheticFallback && !isHydrationManualRouterFallbackPayload(p)) return false;
  return (
    isHydrationManualRouterFallbackPayload(p) ||
    p.manualFallback === true ||
    p.manual_fallback === true ||
    routeModeText === "manual_xyk" ||
    routerText.includes("manual_xyk") ||
    routerText.includes("manual_papi_router") ||
    sourceText.includes("live_pool_account") ||
    sourceText.includes("route_registry") ||
    sourceText.includes("manual")
  );
}


function isHydrationManualRouterFallbackBuildablePayload(payload) {
  const p = payload && typeof payload === "object" ? payload : {};
  const manual = (p.manualCustomSwap && typeof p.manualCustomSwap === "object") ? p.manualCustomSwap : {};
  const tx = (p.tx && typeof p.tx === "object") ? p.tx : {};
  const txManual = (tx.manualCustomSwap && typeof tx.manualCustomSwap === "object") ? tx.manualCustomSwap : {};
  const routeModeEffective = String(
    p.routeModeEffective ||
    p.route_mode_effective ||
    manual.routeModeEffective ||
    manual.route_mode_effective ||
    tx.routeModeEffective ||
    tx.route_mode_effective ||
    txManual.routeModeEffective ||
    txManual.route_mode_effective ||
    ""
  ).trim().toLowerCase();
  const provider = String(p.provider || manual.provider || tx.provider || "").trim().toLowerCase();
  return Boolean(
    p.manualRouterFallback === true ||
    p.manual_router_fallback === true ||
    manual.manualRouterFallback === true ||
    manual.manual_router_fallback === true ||
    tx.manualRouterFallback === true ||
    tx.manual_router_fallback === true ||
    txManual.manualRouterFallback === true ||
    txManual.manual_router_fallback === true ||
    routeModeEffective === "manual_router" ||
    (provider === "manual_papi_router" && (manual.enabled === true || p.manualCustomSwap === true || tx.manualCustomSwap === true))
  );
}

function isHydrationExecutionConfirmedPayload(payload) {
  const p = payload && typeof payload === "object" ? payload : {};
  const manual = (p.manualCustomSwap && typeof p.manualCustomSwap === "object") ? p.manualCustomSwap : {};
  const tx = (p.tx && typeof p.tx === "object") ? p.tx : {};
  const txManual = (tx.manualCustomSwap && typeof tx.manualCustomSwap === "object") ? tx.manualCustomSwap : {};
  return Boolean(
    p.executionConfirmed === true ||
    p.execution_confirmed === true ||
    manual.executionConfirmed === true ||
    manual.execution_confirmed === true ||
    tx.executionConfirmed === true ||
    tx.execution_confirmed === true ||
    txManual.executionConfirmed === true ||
    txManual.execution_confirmed === true
  );
}

function isHydrationManualRouterFallbackPayload(payload) {
  return Boolean(
    isHydrationManualRouterFallbackBuildablePayload(payload) &&
    isHydrationExecutionConfirmedPayload(payload)
  );
}

function hydrationRouteProbeView(payload, symbol) {
  const p = payload && typeof payload === "object" ? payload : {};
  const pool = (p.pool && typeof p.pool === "object") ? p.pool : {};
  const cfg = (p.orderbookConfig && typeof p.orderbookConfig === "object") ? p.orderbookConfig : {};
  const cfgSnake = (p.orderbook_config && typeof p.orderbook_config === "object") ? p.orderbook_config : {};
  const resolvedSymbol = String(p.resolvedSymbol || p.resolved_symbol || symbol || "").trim().toUpperCase();
  const routerText = String(p.router || p.routeModeEffective || p.route_mode_effective || cfg.routeModeEffective || cfgSnake.route_mode_effective || pool.router || "").toLowerCase();
  const routeModeEffective = String(p.routeModeEffective || p.route_mode_effective || cfg.routeModeEffective || cfgSnake.route_mode_effective || "").trim();
  const syntheticOnly = Boolean(
    p.syntheticFallback === true ||
    p.synthetic_fallback === true ||
    routerText.includes("synthetic_spot_fallback") ||
    String(routeModeEffective || "").toLowerCase().includes("synthetic_spot_fallback") ||
    p.tradeRequiresRealRouterQuote === true ||
    cfg.tradeRequiresRealRouterQuote === true ||
    cfgSnake.trade_requires_real_router_quote === true
  );
  const manualRouterFallbackBuildable = isHydrationManualRouterFallbackBuildablePayload(p);
  const executionConfirmed = isHydrationExecutionConfirmedPayload(p);
  const manualRouterFallbackAvailable = Boolean(manualRouterFallbackBuildable && executionConfirmed);
  const manualRouteAvailable = isHydrationManualRoutePayload(p);
  const tradable = Boolean(
    p.tradable === true ||
    cfg.tradable === true ||
    manualRouteAvailable
  ) && (!syntheticOnly || manualRouterFallbackAvailable);
  const reason = syntheticOnly && !manualRouteAvailable
    ? `Synthetic price only — no executable manual route registered for ${resolvedSymbol || "this Hydration pair"}. Orderbook prices are external/cached context only.`
    : manualRouterFallbackAvailable
      ? `Manual Router/Omnipool fallback available for ${resolvedSymbol || "this Hydration pair"}; SDK router quotes remain disabled.`
      : manualRouteAvailable
        ? `Manual XYK/live-pool route available for ${resolvedSymbol || "this Hydration pair"}; generic SDK router quotes remain disabled.`
        : String(p.syntheticFallbackReason || p.synthetic_fallback_reason || cfg.fallbackReason || cfgSnake.fallback_reason || "").trim();
  return {
    symbol: resolvedSymbol,
    router: String(p.router || "").trim(),
    routeModeEffective,
    manualRouteAvailable,
    manualRouterFallbackAvailable,
    syntheticOnly,
    tradable,
    tradeRequiresRealRouterQuote: Boolean(p.tradeRequiresRealRouterQuote === true || cfg.tradeRequiresRealRouterQuote === true || cfgSnake.trade_requires_real_router_quote === true),
    reason,
  };
}



function hydrationExtractOrderbookPrice(row) {
  try {
    if (Array.isArray(row)) {
      const nums = row.map((v) => Number(v)).filter((n) => Number.isFinite(n) && n > 0);
      if (!nums.length) return null;
      if (nums.length === 1) return nums[0];
      // Hydration pseudo-orderbook rows can arrive as [price, size] or [size, price].
      // For these router guard levels, sizes are usually whole-token amounts while prices
      // are fractional. Prefer the smallest positive numeric value so the guard mirrors
      // the displayed lowest ask / highest bid instead of accidentally reading the size.
      const fractional = nums.filter((n) => n > 0 && n < 1);
      if (fractional.length) return Math.min(...fractional);
      return nums[0];
    }
    if (!row || typeof row !== "object") return null;
    const candidates = [
      // Prefer UI/display fields first. Some manual-router payloads also carry
      // raw/inverse/internal price fields; using those can make the guard appear
      // to require the highest ask or lowest bid instead of the visible best side.
      row.displayPrice,
      row.display_price,
      row.uiPrice,
      row.ui_price,
      row.priceUi,
      row.price_ui,
      row.levelPrice,
      row.level_price,
      row.limitPrice,
      row.limit_price,
      row.price,
      row.px,
      row.rate,
    ];
    for (const v of candidates) {
      const n = Number(v);
      if (Number.isFinite(n) && n > 0) return n;
    }
  } catch {
    // ignore malformed orderbook row
  }
  return null;
}


function hydrationOrderbookSideGuardView(payload, symbol) {
  const p = payload && typeof payload === "object" ? payload : {};
  const cfg = (p.orderbookConfig && typeof p.orderbookConfig === "object") ? p.orderbookConfig : {};
  const cfgSnake = (p.orderbook_config && typeof p.orderbook_config === "object") ? p.orderbook_config : {};
  const rawAsks =
    Array.isArray(p.asks) ? p.asks :
    Array.isArray(p.askLevels) ? p.askLevels :
    Array.isArray(p.ask_levels) ? p.ask_levels :
    Array.isArray(p.sell) ? p.sell :
    Array.isArray(p.sells) ? p.sells :
    [];
  const rawBids =
    Array.isArray(p.bids) ? p.bids :
    Array.isArray(p.bidLevels) ? p.bidLevels :
    Array.isArray(p.bid_levels) ? p.bid_levels :
    Array.isArray(p.buy) ? p.buy :
    Array.isArray(p.buys) ? p.buys :
    [];
  const asks = rawAsks.map(hydrationExtractOrderbookPrice).filter((n) => Number.isFinite(n) && n > 0);
  const bids = rawBids.map(hydrationExtractOrderbookPrice).filter((n) => Number.isFinite(n) && n > 0);
  const bestAsk = asks.length ? Math.min(...asks) : null;
  const bestBid = bids.length ? Math.max(...bids) : null;
  return {
    symbol: String(p.resolvedSymbol || p.resolved_symbol || symbol || "").trim().toUpperCase(),
    bestAsk,
    bestBid,
    askCount: asks.length,
    bidCount: bids.length,
    router: String(p.router || cfg.router || cfgSnake.router || "").trim(),
    routeModeEffective: String(p.routeModeEffective || p.route_mode_effective || cfg.routeModeEffective || cfgSnake.route_mode_effective || "").trim(),
    source: String(p.source || cfg.source || cfgSnake.source || "").trim(),
  };
}


function hydrationSwapTxProbeView(payload, symbol) {
  const p = payload && typeof payload === "object" ? payload : {};
  const manual = (p.manualCustomSwap && typeof p.manualCustomSwap === "object") ? p.manualCustomSwap : {};
  const tx = (p.tx && typeof p.tx === "object") ? p.tx : {};
  const resolvedSymbol = String(p.resolvedSymbol || p.resolved_symbol || symbol || "").trim().toUpperCase();
  const manualRouterFallbackBuildable = isHydrationManualRouterFallbackBuildablePayload(p);
  const executionConfirmed = isHydrationExecutionConfirmedPayload(p);
  const manualRouterFallbackAvailable = Boolean(manualRouterFallbackBuildable && executionConfirmed);
  const routeModeEffective = String(
    p.routeModeEffective ||
    p.route_mode_effective ||
    manual.routeModeEffective ||
    manual.route_mode_effective ||
    tx.routeMode ||
    ""
  ).trim();
  const provider = String(p.provider || manual.provider || tx.provider || "").trim();
  const estimatedOut = firstFiniteNumber(
    manual.estimatedAmountOutUi,
    manual.estimated_amount_out_ui,
    tx.estimatedAmountOutUi,
    tx.estimated_amount_out_ui
  );
  const minOut = firstFiniteNumber(
    manual.minAmountOutUi,
    manual.min_amount_out_ui,
    tx.minAmountOutUi,
    tx.min_amount_out_ui
  );
  const reason = manualRouterFallbackAvailable
    ? `Controlled manual Router fallback is execution-confirmed for ${resolvedSymbol || "this Hydration pair"}; SDK router quotes remain disabled.`
    : manualRouterFallbackBuildable
      ? `Manual Router fallback is buildable for ${resolvedSymbol || "this Hydration pair"}, but it is not execution-confirmed yet. Signing is blocked until a real executable route is confirmed.`
      : String(p.message || p.reason || "").trim();
  return {
    symbol: resolvedSymbol,
    manualRouterFallbackAvailable,
    manualRouterFallbackBuildable,
    executionConfirmed,
    routeModeEffective,
    provider,
    reason,
    estimatedOut,
    minOut,
    payload: p,
  };
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

function getPreferredPolkadotWalletKey() {
  try { return localStorage.getItem(LS_OT_DOT_WALLET) || "subwallet-js"; } catch { return "subwallet-js"; }
}
function setPreferredPolkadotWalletKey(v) {
  try { localStorage.setItem(LS_OT_DOT_WALLET, String(v || "subwallet-js")); } catch {}
}

function normalizePolkadotExtensionKey(keyLike) {
  try {
    const s = String(keyLike || "").toLowerCase().trim();
    if (!s) return null;
    if (s === "subwallet" || s === "subwallet-js" || s.includes("subwallet")) return "subwallet-js";
    if (s === "polkadot" || s === "polkadot-js" || s.includes("polkadot")) return "polkadot-js";
    if (s === "talisman" || s.includes("talisman")) return "talisman";
    return s;
  } catch {
    return null;
  }
}

function getPolkadotWalletLabel(keyLike) {
  const k = normalizePolkadotExtensionKey(keyLike);
  if (k === "subwallet-js") return "SubWallet";
  if (k === "talisman") return "Talisman";
  if (k === "polkadot-js") return "Polkadot.js";
  return keyLike ? String(keyLike) : "Polkadot Wallet";
}

const OT_POLKADOT_BALANCE_CACHE_TTL_MS = 2500;
const OT_POLKADOT_BALANCE_INFLIGHT = new Map();
const OT_POLKADOT_BALANCE_CACHE = new Map();

function polkadotBalanceCacheKey(apiBase, venue, address) {
  return [
    String(apiBase || "").replace(/\/+$/, ""),
    String(venue || "").toLowerCase().trim(),
    String(address || "").trim(),
  ].join("|");
}

async function fetchPolkadotDexBalancesCached({ apiBase, venue, address, force = false }) {
  const base = String(apiBase || "").replace(/\/+$/, "");
  const v = String(venue || "").toLowerCase().trim();
  const addr = String(address || "").trim();
  if (!base) throw new Error("apiBase not set");
  if (!v) throw new Error("Hydration venue missing for balance request.");
  if (!addr) throw new Error("Connect SubWallet to load Polkadot balances.");

  const key = polkadotBalanceCacheKey(base, v, addr);
  const now = Date.now();
  const cached = OT_POLKADOT_BALANCE_CACHE.get(key);
  if (!force && cached && now - Number(cached.ts || 0) < OT_POLKADOT_BALANCE_CACHE_TTL_MS) {
    return cached.data;
  }

  const pending = OT_POLKADOT_BALANCE_INFLIGHT.get(key);
  if (!force && pending) return pending;

  const promise = (async () => {
    const url = new URL(`${base}/api/polkadot_dex/balances`);
    url.searchParams.set("venue", v);
    url.searchParams.set("address", addr);
    url.searchParams.set("_ts", String(Date.now()));

    const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `HTTP ${r.status}`);
    }

    const data = await r.json();
    OT_POLKADOT_BALANCE_CACHE.set(key, { ts: Date.now(), data });
    return data;
  })();

  OT_POLKADOT_BALANCE_INFLIGHT.set(key, promise);
  try {
    return await promise;
  } finally {
    const cur = OT_POLKADOT_BALANCE_INFLIGHT.get(key);
    if (cur === promise) OT_POLKADOT_BALANCE_INFLIGHT.delete(key);
  }
}

function getInjectedPolkadotWalletOptions() {
  try {
    const w = typeof window !== "undefined" ? window : null;
    const injected = w?.injectedWeb3 && typeof w.injectedWeb3 === "object" ? w.injectedWeb3 : {};
    const priority = ["subwallet-js", "talisman", "polkadot-js"];
    const seen = new Set();
    const out = [];

    for (const key of priority) {
      if (!injected?.[key]) continue;
      seen.add(key);
      out.push({ key, label: getPolkadotWalletLabel(key), installed: true });
    }

    for (const rawKey of Object.keys(injected || {})) {
      const key = normalizePolkadotExtensionKey(rawKey) || rawKey;
      if (seen.has(key)) continue;
      const ext = injected?.[rawKey];
      if (!ext || typeof ext.enable !== "function") continue;
      seen.add(key);
      out.push({ key: rawKey, label: getPolkadotWalletLabel(rawKey), installed: true });
    }

    return out;
  } catch {
    return [];
  }
}

async function connectInjectedPolkadotWallet(preferredKey) {
  const w = typeof window !== "undefined" ? window : null;
  const injected = w?.injectedWeb3 && typeof w.injectedWeb3 === "object" ? w.injectedWeb3 : {};
  const installed = getInjectedPolkadotWalletOptions();
  const preferred = normalizePolkadotExtensionKey(preferredKey) || "subwallet-js";
  const selected = installed.find((x) => normalizePolkadotExtensionKey(x?.key) === preferred) || installed[0] || null;
  const key = selected?.key || preferred;
  const provider = injected?.[key] || injected?.[preferred];

  if (!provider || typeof provider.enable !== "function") {
    throw new Error("Install or unlock SubWallet, then refresh/rescan Polkadot wallets.");
  }

  const extension = await provider.enable(POLKADOT_APP_NAME);
  const rawAccounts =
    typeof extension?.accounts?.get === "function"
      ? await extension.accounts.get()
      : Array.isArray(extension?.accounts)
        ? extension.accounts
        : [];

  const accounts = (Array.isArray(rawAccounts) ? rawAccounts : [])
    .map((acc) => ({
      ...acc,
      address: String(acc?.address || "").trim(),
      name: String(acc?.meta?.name || acc?.name || "").trim(),
      source: String(acc?.meta?.source || normalizePolkadotExtensionKey(key) || key || "").trim(),
    }))
    .filter((acc) => !!acc.address);

  const sourceKey = normalizePolkadotExtensionKey(key);
  const account = accounts.find((acc) => normalizePolkadotExtensionKey(acc?.source) === sourceKey) || accounts[0] || null;
  if (!account?.address) {
    throw new Error("No Polkadot accounts were shared by the selected wallet.");
  }

  return {
    key: sourceKey || key,
    label: getPolkadotWalletLabel(sourceKey || key),
    extension,
    accounts,
    address: account.address,
    accountName: account.name || "",
    source: account.source || sourceKey || key,
  };
}


const POLKADOT_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const POLKADOT_BASE58_MAP = (() => {
  const m = new Map();
  for (let i = 0; i < POLKADOT_BASE58_ALPHABET.length; i += 1) m.set(POLKADOT_BASE58_ALPHABET[i], i);
  return m;
})();

function hexToU8a(hexLike) {
  let h = String(hexLike || "").trim();
  if (!h) return new Uint8Array();
  if (h.startsWith("0x")) h = h.slice(2);
  if (h.length % 2) h = `0${h}`;
  const out = new Uint8Array(h.length / 2);
  for (let i = 0; i < out.length; i += 1) out[i] = Number.parseInt(h.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function u8aToHex(u8a) {
  try {
    const arr = u8a instanceof Uint8Array ? u8a : new Uint8Array(u8a || []);
    return `0x${Array.from(arr).map((b) => b.toString(16).padStart(2, "0")).join("")}`;
  } catch {
    return "0x";
  }
}

function normalizeHexValue(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  return s.startsWith("0x") ? s : `0x${s}`;
}

function polkadotBase58DecodeRaw(address) {
  const s = String(address || "").trim();
  if (!s) throw new Error("Empty Substrate address.");
  let n = 0n;
  for (const ch of s) {
    const v = POLKADOT_BASE58_MAP.get(ch);
    if (v == null) throw new Error(`Invalid SS58 address character: ${ch}`);
    n = n * 58n + BigInt(v);
  }
  const bytes = [];
  while (n > 0n) {
    bytes.push(Number(n & 0xffn));
    n >>= 8n;
  }
  bytes.reverse();
  let leading = 0;
  for (const ch of s) {
    if (ch === "1") leading += 1;
    else break;
  }
  return new Uint8Array([...new Array(leading).fill(0), ...bytes]);
}

function ss58PublicKeyFromAddress(address) {
  const raw = polkadotBase58DecodeRaw(address);
  if (![35, 36, 37, 38].includes(raw.length)) {
    throw new Error(`Unsupported SS58 address length: ${raw.length}`);
  }
  const prefixLen = raw[0] < 64 ? 1 : 2;
  const account = raw.slice(prefixLen, prefixLen + 32);
  if (account.length !== 32) throw new Error("Could not extract 32-byte public key from SS58 address.");
  return account;
}

function normalizePolkadotKeyType(rawType) {
  const t = String(rawType || "").toLowerCase().trim();
  if (t === "ecdsa") return "Ecdsa";
  if (t === "ed25519") return "Ed25519";
  return "Sr25519";
}

function toPlainJson(value, seen = new WeakSet()) {
  if (value == null) return value;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
  if (value instanceof Uint8Array) return u8aToHex(value);
  if (Array.isArray(value)) return value.slice(0, 50).map((x) => toPlainJson(x, seen));
  if (typeof value === "object") {
    if (seen.has(value)) return "[Circular]";
    seen.add(value);
    if (typeof value.toString === "function") {
      try {
        const s = value.toString();
        if (s && s !== "[object Object]") return s;
      } catch {}
    }
    const out = {};
    for (const [k, v] of Object.entries(value).slice(0, 80)) {
      if (typeof v === "function") continue;
      out[k] = toPlainJson(v, seen);
    }
    return out;
  }
  return String(value);
}

function extractPolkadotTxHash(result) {
  try {
    const candidates = [
      result?.txHash,
      result?.hash,
      result?.transactionHash,
      result?.value?.txHash,
      result?.value?.hash,
      result?.events?.txHash,
    ];
    for (const c of candidates) {
      const s = String(c || "").trim();
      if (s.startsWith("0x")) return s;
    }
  } catch {}
  return null;
}


function extractPolkadotDispatchFailure(result) {
  try {
    const events = Array.isArray(result?.events) ? result.events : [];
    for (const ev of events) {
      const event = ev?.event || ev;
      const section = String(event?.type || event?.section || "").trim();
      const variant = String(event?.value?.type || event?.method || "").trim();
      if (section === "System" && variant === "ExtrinsicFailed") {
        const value = event?.value?.value || {};
        const dispatchError = value?.dispatch_error || value?.dispatchError || result?.dispatchError || null;
        const dispatchInfo = value?.dispatch_info || value?.dispatchInfo || null;
        return {
          event: "System.ExtrinsicFailed",
          dispatchError: toPlainJson(dispatchError),
          dispatchInfo: toPlainJson(dispatchInfo),
          phase: toPlainJson(ev?.phase || null),
        };
      }
    }

    if (result?.dispatchError && String(result.dispatchError) !== "[Circular]") {
      return {
        event: "dispatchError",
        dispatchError: toPlainJson(result.dispatchError),
        dispatchInfo: null,
        phase: null,
      };
    }
  } catch {}
  return null;
}

function summarizePolkadotDispatchFailure(failure) {
  try {
    const err = failure?.dispatchError;
    const parts = [];
    const walk = (value, depth = 0) => {
      if (value == null || depth > 6) return;
      if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
        const s = String(value).trim();
        if (s && s !== "[Circular]" && !parts.includes(s)) parts.push(s);
        return;
      }
      if (Array.isArray(value)) {
        value.slice(0, 10).forEach((x) => walk(x, depth + 1));
        return;
      }
      if (typeof value === "object") {
        for (const key of ["type", "section", "name", "method", "error", "value"]) {
          if (Object.prototype.hasOwnProperty.call(value, key)) walk(value[key], depth + 1);
        }
      }
    };
    walk(err);
    const joined = parts.filter(Boolean).join(".");
    return joined || failure?.event || "ExtrinsicFailed";
  } catch {
    return "ExtrinsicFailed";
  }
}

async function importPolkadotPapiRuntime() {
  try {
    const papi = await import("polkadot-api");
    const pjsSignerPkg = await import("polkadot-api/pjs-signer");

    // Vite cannot pre-bundle a bare package subpath when the subpath is
    // hidden behind import(spec). In that case the browser receives the raw
    // bare specifier and throws: Failed to resolve module specifier
    // "polkadot-api/ws". Use a literal dynamic import so Vite can analyze
    // and rewrite it during dependency optimization.
    const wsPkg = await import("polkadot-api/ws");
    const wsProviderImport = "polkadot-api/ws";

    if (typeof wsPkg?.getWsProvider !== "function") {
      throw new Error("polkadot-api/ws did not export getWsProvider.");
    }
    if (typeof pjsSignerPkg?.connectInjectedExtension !== "function") {
      throw new Error("polkadot-api/pjs-signer did not export connectInjectedExtension.");
    }
    return { papi, pjsSignerPkg, wsPkg, wsProviderImport };
  } catch (e) {
    const msg = e?.message || String(e);
    throw new Error(`Polkadot signing dependencies are missing or failed to load. From the frontend folder run: npm install polkadot-api. Detail: ${msg}`);
  }
}

async function resolvePapiInjectedPolkadotSigner({ pjsSignerPkg, walletKey, address, accounts }) {
  const wantedAddress = String(address || "").trim();
  if (!wantedAddress) throw new Error("Missing SubWallet address for PAPI extension signer lookup.");

  const installed = typeof pjsSignerPkg?.getInjectedExtensions === "function"
    ? pjsSignerPkg.getInjectedExtensions()
    : [];
  const installedList = Array.isArray(installed) ? installed.map((x) => String(x || "").trim()).filter(Boolean) : [];
  const preferred = normalizePolkadotExtensionKey(walletKey) || normalizePolkadotExtensionKey(accounts?.[0]?.source) || "subwallet-js";
  const preferredCandidates = [
    preferred,
    walletKey,
    "subwallet-js",
    "subwallet",
    ...installedList,
  ].map((x) => String(x || "").trim()).filter(Boolean);

  const seen = new Set();
  const candidates = preferredCandidates.filter((x) => {
    const k = x.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  const errors = [];
  for (const extName of candidates) {
    const installedMatch = installedList.find((name) => normalizePolkadotExtensionKey(name) === normalizePolkadotExtensionKey(extName));
    const connectName = installedMatch || extName;
    try {
      const injected = await pjsSignerPkg.connectInjectedExtension(connectName);
      const accountListRaw = typeof injected?.getAccounts === "function" ? await injected.getAccounts() : [];
      const accountList = Array.isArray(accountListRaw) ? accountListRaw : [];
      const account = accountList.find((a) => String(a?.address || "") === wantedAddress) || null;
      if (account?.polkadotSigner) {
        return {
          signer: account.polkadotSigner,
          extensionName: connectName,
          account: toPlainJson({
            address: account.address,
            name: account.name,
            type: account.type,
            genesisHash: account.genesisHash,
          }),
        };
      }
      errors.push(`${connectName}: selected address not shared by extension`);
      try { injected?.disconnect?.(); } catch {}
    } catch (e) {
      errors.push(`${connectName}: ${e?.message || String(e)}`);
    }
  }

  throw new Error(`Could not get a PAPI PolkadotSigner for ${shortenWalletAddress(wantedAddress)}. Reconnect SubWallet and make sure this account is shared. Tried: ${candidates.join(", ")}. ${errors.slice(0, 5).join(" | ")}`);
}

function hydrationFrontendWsUrl(status) {
  const fromStatus = String(status?.sidecar?.wsUrl || "").trim();
  if (fromStatus && !fromStatus.includes("***")) return fromStatus;
  return "wss://hydration-rpc.n.dwellir.com";
}

async function signAndSubmitHydrationCallData({ encodedCallData, address, walletKey, accounts, wsUrl, onProgress }) {
  const encodedHex = normalizeHexValue(encodedCallData);
  if (!encodedHex) throw new Error("Missing Hydration encoded call data.");
  if (!address) throw new Error("Missing SubWallet address for signing.");

  const progress = (stage) => {
    try {
      if (typeof onProgress === "function") onProgress(stage);
    } catch {}
  };

  progress("wallet");
  const { papi, pjsSignerPkg, wsPkg, wsProviderImport } = await importPolkadotPapiRuntime();
  progress("wallet");
  const { signer, extensionName, account } = await resolvePapiInjectedPolkadotSigner({
    pjsSignerPkg,
    walletKey,
    address,
    accounts,
  });

  const client = papi.createClient(wsPkg.getWsProvider(wsUrl));
  try {
    const unsafeApi = client.getUnsafeApi();
    const callBinary = papi.Binary.fromHex(encodedHex);
    const tx = await unsafeApi.txFromCallData(callBinary);
    progress("finality");
    const result = await tx.signAndSubmit(signer);
    const plainResult = toPlainJson(result);
    const dispatchFailure = extractPolkadotDispatchFailure(plainResult);
    const chainOk = plainResult?.ok !== false && !dispatchFailure;
    return {
      ok: chainOk,
      signed: true,
      submitted: true,
      finalized: true,
      wsUrl,
      wsProviderImport,
      signerSource: "polkadot-api/pjs-signer",
      signerExtension: extensionName,
      signerAccount: account,
      address,
      encodedCallData: encodedHex,
      txHash: extractPolkadotTxHash(plainResult),
      submitResult: plainResult,
      dispatchFailure: dispatchFailure || null,
      dispatchErrorSummary: dispatchFailure ? summarizePolkadotDispatchFailure(dispatchFailure) : null,
    };
  } finally {
    try { client?.destroy?.(); } catch {}
  }
}

function solanaProviderPubkeyBase58(provider) {
  try {
    const pk = provider?.publicKey;
    if (!pk) return null;
    if (typeof pk?.toBase58 === "function") return pk.toBase58();
    if (typeof pk?.toString === "function") return pk.toString();
    if (typeof pk === "string") return pk;
    return null;
  } catch {
    return null;
  }
}

function isSolanaProviderLike(provider) {
  try {
    return !!provider && (
      typeof provider?.connect === "function" ||
      typeof provider?.signTransaction === "function" ||
      typeof provider?.signAndSendTransaction === "function" ||
      !!provider?.publicKey
    );
  } catch {
    return false;
  }
}

function unwrapSolanaProvider(candidate) {
  try {
    if (!candidate) return null;
    if (isSolanaProviderLike(candidate)) return candidate;
    if (isSolanaProviderLike(candidate?.solana)) return candidate.solana;
    if (isSolanaProviderLike(candidate?.provider)) return candidate.provider;
    return null;
  } catch {
    return null;
  }
}

function isJupiterLikeProvider(provider) {
  try {
    if (!provider) return false;
    return !!(
      provider?.isJupiter ||
      provider?.isJupiterWallet ||
      provider?.isJup ||
      provider?.isJupWallet ||
      String(provider?.name || provider?.walletName || provider?.providerName || "").toLowerCase().includes("jupiter")
    );
  } catch {
    return false;
  }
}

const WALLET_STANDARD_REGISTER_EVENT = "register-wallet";
const WALLET_STANDARD_APP_READY_EVENT = "app-ready";

function getWalletStandardState() {
  try {
    const w = typeof window !== "undefined" ? window : null;
    if (!w) return null;
    if (!w.__uttWalletStandardState) {
      w.__uttWalletStandardState = {
        initialized: false,
        primed: false,
        wallets: [],
        seen: new Set(),
        listeners: new Set(),
        onRegister: null,
      };
    }
    return w.__uttWalletStandardState;
  } catch {
    return null;
  }
}

function walletStandardWalletId(wallet) {
  try {
    const name = String(wallet?.name || "").trim().toLowerCase();
    const version = String(wallet?.version || "").trim().toLowerCase();
    const chains = Array.isArray(wallet?.chains) ? wallet.chains.map((x) => String(x || "").trim().toLowerCase()).sort().join(",") : "";
    const features = wallet?.features && typeof wallet.features === "object"
      ? Object.keys(wallet.features).map((x) => String(x || "").trim().toLowerCase()).sort().join(",")
      : "";
    const icon = String(wallet?.icon || "").trim().toLowerCase();
    return [name, version, chains, features, icon].join("|");
  } catch {
    return "";
  }
}

function isWalletStandardSolanaWallet(wallet) {
  try {
    if (!wallet || typeof wallet !== "object") return false;
    const name = String(wallet?.name || "").trim();
    if (!name) return false;

    const chains = Array.isArray(wallet?.chains) ? wallet.chains.map((x) => String(x || "").toLowerCase()) : [];
    const featureKeys = wallet?.features && typeof wallet.features === "object"
      ? Object.keys(wallet.features).map((x) => String(x || "").toLowerCase())
      : [];

    return (
      chains.some((c) => c.includes("solana")) ||
      featureKeys.some((k) => k.includes("solana:")) ||
      isJupiterLikeProvider(wallet)
    );
  } catch {
    return false;
  }
}

function isWalletStandardJupiterWallet(wallet) {
  try {
    return isJupiterLikeProvider(wallet) || String(wallet?.name || "").toLowerCase().includes("jupiter");
  } catch {
    return false;
  }
}

function notifyWalletStandardListeners() {
  try {
    const st = getWalletStandardState();
    if (!st) return;
    const snapshot = Array.isArray(st.wallets) ? st.wallets.slice() : [];
    for (const cb of st.listeners || []) {
      try { cb(snapshot); } catch {}
    }
  } catch {
    // ignore
  }
}

function addWalletStandardWalletCandidate(candidate) {
  try {
    const wallet =
      candidate?.wallet ||
      candidate?.adapter ||
      candidate ||
      null;

    if (!isWalletStandardSolanaWallet(wallet)) return;

    const st = getWalletStandardState();
    if (!st) return;

    const id = walletStandardWalletId(wallet);
    if (!id) return;
    if (st.seen.has(id)) return;

    st.seen.add(id);
    st.wallets = [...(Array.isArray(st.wallets) ? st.wallets : []), wallet];
    notifyWalletStandardListeners();
  } catch {
    // ignore
  }
}

function handleWalletStandardRegisterEvent(event) {
  try {
    const detail = event?.detail;

    if (typeof detail?.register === "function") {
      detail.register((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    if (typeof detail === "function") {
      detail((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    if (Array.isArray(detail?.wallets)) {
      detail.wallets.forEach((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    if (Array.isArray(detail)) {
      detail.forEach((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    addWalletStandardWalletCandidate(detail?.wallet || detail);
  } catch {
    // ignore
  }
}

function ensureWalletStandardBridge() {
  try {
    const st = getWalletStandardState();
    const w = typeof window !== "undefined" ? window : null;
    if (!st || !w || st.initialized) return;

    st.initialized = true;
    st.onRegister = (event) => handleWalletStandardRegisterEvent(event);

    w.addEventListener(WALLET_STANDARD_REGISTER_EVENT, st.onRegister);

    try {
      w.dispatchEvent(new Event(WALLET_STANDARD_APP_READY_EVENT));
    } catch {
      // ignore
    }
  } catch {
    // ignore
  }
}

async function primeWalletStandardWallets() {
  try {
    const st = getWalletStandardState();
    if (!st || st.primed) return;
    st.primed = true;
    ensureWalletStandardBridge();

    try {
      // Keep this package-free so Vite does not hard-fail when @wallet-standard/app
      // is not installed in the frontend. Wallet-standard extensions can still
      // register through the browser event bridge above.
      const navWallets = typeof navigator !== "undefined" ? navigator?.wallets : null;
      const wallets =
        Array.isArray(navWallets) ? navWallets :
        Array.isArray(navWallets?.wallets) ? navWallets.wallets :
        typeof navWallets?.get === "function" ? navWallets.get() :
        [];
      if (Array.isArray(wallets)) {
        wallets.forEach((wallet) => addWalletStandardWalletCandidate(wallet));
      }
    } catch {
      // Event bridge still works without any helper package.
    }
  } catch {
    // ignore
  }
}

function getWalletStandardWallets() {
  try {
    ensureWalletStandardBridge();
    const st = getWalletStandardState();
    return Array.isArray(st?.wallets) ? st.wallets.slice() : [];
  } catch {
    return [];
  }
}

function subscribeWalletStandardWallets(callback) {
  try {
    ensureWalletStandardBridge();
    const st = getWalletStandardState();
    if (!st || typeof callback !== "function") return () => {};

    st.listeners.add(callback);
    callback(getWalletStandardWallets());
    return () => {
      try { st.listeners.delete(callback); } catch {}
    };
  } catch {
    return () => {};
  }
}

const B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

function base58EncodeBytes(bytesLike) {
  try {
    const bytes = bytesLike instanceof Uint8Array ? bytesLike : Uint8Array.from(bytesLike || []);
    if (!bytes.length) return "";
    let zeros = 0;
    while (zeros < bytes.length && bytes[zeros] === 0) zeros += 1;

    let digits = [0];
    for (let i = zeros; i < bytes.length; i += 1) {
      let carry = bytes[i];
      for (let j = 0; j < digits.length; j += 1) {
        const x = digits[j] * 256 + carry;
        digits[j] = x % 58;
        carry = Math.floor(x / 58);
      }
      while (carry > 0) {
        digits.push(carry % 58);
        carry = Math.floor(carry / 58);
      }
    }

    let out = "1".repeat(zeros);
    for (let i = digits.length - 1; i >= 0; i -= 1) out += B58_ALPHABET[digits[i]];
    return out;
  } catch {
    return "";
  }
}

function coerceWalletStandardSignature(value) {
  try {
    if (!value) return null;
    if (typeof value === "string") return value;
    if (value instanceof Uint8Array) return base58EncodeBytes(value);
    if (ArrayBuffer.isView(value)) return base58EncodeBytes(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    if (value instanceof ArrayBuffer) return base58EncodeBytes(new Uint8Array(value));
    if (Array.isArray(value)) return base58EncodeBytes(Uint8Array.from(value));
    return null;
  } catch {
    return null;
  }
}

function walletStandardAccountAddress(account) {
  try {
    return String(account?.address || account?.publicKey || "").trim() || null;
  } catch {
    return null;
  }
}

function walletStandardPublicKeyShim(address) {
  if (!address) return null;
  return {
    toBase58: () => String(address),
    toString: () => String(address),
  };
}

function walletStandardSolanaChain(wallet) {
  try {
    const chains = Array.isArray(wallet?.chains) ? wallet.chains.map((x) => String(x || "")) : [];
    return chains.find((c) => c.toLowerCase().includes("solana")) || "solana:mainnet";
  } catch {
    return "solana:mainnet";
  }
}

async function callWalletStandardFeatureMethod(featureObj, methodName, input) {
  if (!featureObj || typeof featureObj?.[methodName] !== "function") {
    throw new Error(`Wallet missing ${methodName}`);
  }
  try {
    return await featureObj[methodName](input);
  } catch (e1) {
    try {
      return await featureObj[methodName]([input]);
    } catch {
      throw e1;
    }
  }
}

function createWalletStandardSolanaProvider(wallet) {
  const provider = {
    __walletStandard: true,
    __walletStandardWallet: wallet,
    __walletStandardAccount: (Array.isArray(wallet?.accounts) ? wallet.accounts[0] : null) || null,
    get publicKey() {
      const account = this.__walletStandardAccount || (Array.isArray(this.__walletStandardWallet?.accounts) ? this.__walletStandardWallet.accounts[0] : null);
      return walletStandardPublicKeyShim(walletStandardAccountAddress(account));
    },
    async connect() {
      const feature = this.__walletStandardWallet?.features?.["standard:connect"];
      if (feature && typeof feature.connect === "function") {
        const out = await feature.connect();
        const accounts =
          Array.isArray(out?.accounts) ? out.accounts :
          Array.isArray(this.__walletStandardWallet?.accounts) ? this.__walletStandardWallet.accounts :
          [];
        this.__walletStandardAccount = accounts[0] || this.__walletStandardAccount || null;
      } else if (!this.__walletStandardAccount && Array.isArray(this.__walletStandardWallet?.accounts)) {
        this.__walletStandardAccount = this.__walletStandardWallet.accounts[0] || null;
      }

      return this.publicKey ? { publicKey: this.publicKey } : null;
    },
    async signTransaction(transaction) {
      const wallet = this.__walletStandardWallet;
      const account = this.__walletStandardAccount || (await this.connect(), this.__walletStandardAccount);
      if (!account) throw new Error("Wallet account unavailable.");

      const feature = wallet?.features?.["solana:signTransaction"];
      const out = await callWalletStandardFeatureMethod(feature, "signTransaction", {
        account,
        chain: walletStandardSolanaChain(wallet),
        transaction,
      });

      const first = Array.isArray(out) ? out[0] : out;
      return first?.signedTransaction || first?.transaction || first || null;
    },
    async signAndSendTransaction(transaction) {
      const wallet = this.__walletStandardWallet;
      const account = this.__walletStandardAccount || (await this.connect(), this.__walletStandardAccount);
      if (!account) throw new Error("Wallet account unavailable.");

      const feature = wallet?.features?.["solana:signAndSendTransaction"];
      const out = await callWalletStandardFeatureMethod(feature, "signAndSendTransaction", {
        account,
        chain: walletStandardSolanaChain(wallet),
        transaction,
      });

      const first = Array.isArray(out) ? out[0] : out;
      const signature =
        coerceWalletStandardSignature(first?.signature) ||
        coerceWalletStandardSignature(Array.isArray(first?.signatures) ? first.signatures[0] : null);

      if (!signature) throw new Error("Wallet did not return a signature.");
      return { signature };
    },
  };

  return provider;
}

function collectSolanaProviderCandidates(root) {
  const out = [];
  const push = (candidate) => {
    try {
      if (!candidate) return;
      out.push(candidate);
      if (Array.isArray(candidate?.providers)) {
        for (const p of candidate.providers) out.push(p);
      }
      if (Array.isArray(candidate?.wallets)) {
        for (const p of candidate.wallets) out.push(p);
      }
    } catch {
      // ignore
    }
  };

  try {
    push(root?.solana);
    push(root?.phantom);
    push(root?.phantom?.solana);
    push(root?.solflare);
    push(root?.solflare?.solana);
    push(root?.backpack);
    push(root?.backpack?.solana);
    push(root?.jupiterWallet);
    push(root?.jupiterWallet?.solana);
    push(root?.jupiter);
    push(root?.jupiter?.solana);
    push(root?.jup);
    push(root?.jup?.solana);
    push(root?.xnft?.solana);
  } catch {
    // ignore
  }

  return out;
}

function classifyInjectedSolanaProvider(candidate) {
  try {
    const provider = unwrapSolanaProvider(candidate) || candidate || null;
    if (!provider) return { key: null, provider: null };

    if (isJupiterLikeProvider(candidate) || isJupiterLikeProvider(provider)) {
      return { key: "jupiter", provider };
    }
    if (provider?.isPhantom || candidate?.isPhantom) {
      return { key: "phantom", provider };
    }
    if (provider?.isBackpack || candidate?.isBackpack) {
      return { key: "backpack", provider };
    }
    if (provider?.isSolflare || candidate?.isSolflare) {
      return { key: "solflare", provider };
    }

    const nameBlob = [
      candidate?.name,
      candidate?.walletName,
      candidate?.providerName,
      provider?.name,
      provider?.walletName,
      provider?.providerName,
    ].map((x) => String(x || "").toLowerCase()).join(" ");

    if (nameBlob.includes("jupiter")) return { key: "jupiter", provider };
    if (nameBlob.includes("phantom")) return { key: "phantom", provider };
    if (nameBlob.includes("backpack")) return { key: "backpack", provider };
    if (nameBlob.includes("solflare")) return { key: "solflare", provider };

    return { key: null, provider: null };
  } catch {
    return { key: null, provider: null };
  }
}

function getInjectedSolanaProviders(walletStandardWallets = null) {
  try {
    ensureWalletStandardBridge();
    const w = typeof window !== "undefined" ? window : null;
    if (!w) return {};

    const providers = {};
    const candidates = collectSolanaProviderCandidates(w);
    for (const candidate of candidates) {
      const { key, provider } = classifyInjectedSolanaProvider(candidate);
      if (!key || !provider || providers[key]) continue;
      providers[key] = provider;
    }

    const walletStandardList = Array.isArray(walletStandardWallets) ? walletStandardWallets : getWalletStandardWallets();
    const wsJupiter = walletStandardList.find((wallet) => isWalletStandardJupiterWallet(wallet));
    if (!providers.jupiter && wsJupiter) {
      providers.jupiter = createWalletStandardSolanaProvider(wsJupiter);
    }

    return providers;
  } catch {
    return {};
  }
}

function getInjectedSolanaProvider(preferred = "solflare", walletStandardWallets = null) {
  const providers = getInjectedSolanaProviders(walletStandardWallets);
  const order = ["jupiter", "solflare", "phantom", "backpack"];

  const pref = String(preferred || "solflare").toLowerCase().trim();
  if (providers[pref]) return { key: pref, provider: providers[pref] };

  for (const key of order) {
    const p = providers[key];
    if (p && solanaProviderPubkeyBase58(p)) return { key, provider: p };
  }
  for (const key of order) {
    const p = providers[key];
    if (p) return { key, provider: p };
  }
  return { key: null, provider: null };
}

function getInstalledSolanaWalletOptions(walletStandardWallets = null) {
  const providers = getInjectedSolanaProviders(walletStandardWallets);
  const labels = { jupiter: "Jupiter", solflare: "Solflare", phantom: "Phantom", backpack: "Backpack" };
  const order = ["jupiter", "solflare", "phantom", "backpack"];
  return order.filter((k) => !!providers[k]).map((k) => ({ key: k, label: labels[k] || k }));
}


const LS_OT_BOX = "utt_ot_box_v2";
const LS_OT_LOCK = "utt_ot_lock_v2";

// Back-compat storage keys (Total was originally "USD sizing")
const LS_OT_TOTAL_USD = "utt_ot_total_usd_v1";
const LS_OT_AUTOQTY = "utt_ot_autoqty_v1";

// ─────────────────────────────────────────────────────────────
// Safe environment helpers (prevents “blank UI” from storage/window issues)
// ─────────────────────────────────────────────────────────────
const HAS_WINDOW = typeof window !== "undefined";
function lsGet(key, fallback = null) {
  try {
    if (typeof localStorage === "undefined") return fallback;
    const v = localStorage.getItem(key);
    return v === null || v === undefined ? fallback : v;
  } catch {
    return fallback;
  }
}
function lsSet(key, value) {
  try {
    if (typeof localStorage === "undefined") return;
    localStorage.setItem(key, value);
  } catch {
    // ignore
  }
}
function safeJsonParse(s, fallback = null) {
  try {
    return JSON.parse(s);
  } catch {
    return fallback;
  }
}

function extractRulesError(e) {
  // Axios shape
  const status = e?.response?.status;
  const data = e?.response?.data;

  const detail =
    typeof data === "string"
      ? data
      : data?.detail
        ? typeof data.detail === "string"
          ? data.detail
          : JSON.stringify(data.detail)
        : null;

  if (status && detail) return `HTTP ${status}: ${detail}`;
  if (status) return `HTTP ${status}`;
  if (e?.message) return e.message;

  try {
    return JSON.stringify(e);
  } catch {
    return "Failed loading rules";
  }
}


function classifyWalletAdapterNameToKey(nameLike) {
  try {
    const s = String(nameLike || "").toLowerCase().trim();
    if (!s) return null;
    if (s.includes("jupiter") || s.includes("jup.ag") || s === "jup" || s.includes(" jup ")) return "jupiter";
    if (s.includes("solflare")) return "solflare";
    if (s.includes("phantom")) return "phantom";
    if (s.includes("backpack")) return "backpack";
    return null;
  } catch {
    return null;
  }
}

function createWalletAdapterBridgeProvider(walletApi, connection) {
  if (!walletApi) return null;

  return {
    __walletAdapterBridge: true,
    get publicKey() {
      return walletApi?.publicKey || null;
    },
    async connect() {
      if (walletApi?.connected && walletApi?.publicKey) {
        return { publicKey: walletApi.publicKey };
      }
      if (typeof walletApi?.connect === "function") {
        await walletApi.connect();
      }
      return walletApi?.publicKey ? { publicKey: walletApi.publicKey } : null;
    },
    async signTransaction(transaction) {
      if (typeof walletApi?.signTransaction !== "function") {
        throw new Error("Selected wallet does not support signTransaction.");
      }
      return await walletApi.signTransaction(transaction);
    },
    async signAndSendTransaction(transaction) {
      if (typeof walletApi?.sendTransaction === "function") {
        const signature = await walletApi.sendTransaction(transaction, connection);
        if (!signature) throw new Error("Wallet did not return a signature.");
        return { signature: String(signature) };
      }
      if (typeof walletApi?.signTransaction === "function") {
        const signed = await walletApi.signTransaction(transaction);
        const raw = typeof signed?.serialize === "function" ? signed.serialize() : null;
        if (!raw) throw new Error("Wallet did not return a serializable signed transaction.");
        const signature = await connection.sendRawTransaction(raw);
        if (!signature) throw new Error("RPC did not return a signature.");
        return { signature: String(signature) };
      }
      throw new Error("Selected wallet does not support sendTransaction or signTransaction.");
    },
  };
}

function shortenWalletAddress(addr, left = 6, right = 4) {
  try {
    const s = String(addr || "").trim();
    if (!s) return "";
    if (s.length <= left + right + 1) return s;
    return `${s.slice(0, left)}…${s.slice(-right)}`;
  } catch {
    return "";
  }
}

function getSolanaWalletVisualMeta(key, nameLike, iconLike) {
  try {
    const k = String(key || "").toLowerCase().trim();
    const name = String(nameLike || "").trim();
    const icon = String(iconLike || "").trim();

    const presets = {
      jupiter: {
        label: "Jupiter",
        color: "#43d3c5",
        border: "rgba(67, 211, 197, 0.35)",
        glow: "rgba(67, 211, 197, 0.18)",
        fallbackBg: "linear-gradient(135deg, #36cfc9, #2f7cf6)",
        fallbackFg: "#071014",
        fallbackText: "J",
      },
      solflare: {
        label: "Solflare",
        color: "#f7d34a",
        border: "rgba(247, 211, 74, 0.35)",
        glow: "rgba(247, 211, 74, 0.18)",
        fallbackBg: "#f7d34a",
        fallbackFg: "#101010",
        fallbackText: "S",
      },
      phantom: {
        label: "Phantom",
        color: "#a78bfa",
        border: "rgba(167, 139, 250, 0.35)",
        glow: "rgba(167, 139, 250, 0.18)",
        fallbackBg: "#8b5cf6",
        fallbackFg: "#ffffff",
        fallbackText: "P",
      },
      backpack: {
        label: "Backpack",
        color: "#ef4444",
        border: "rgba(239, 68, 68, 0.35)",
        glow: "rgba(239, 68, 68, 0.18)",
        fallbackBg: "#ef4444",
        fallbackFg: "#ffffff",
        fallbackText: "B",
      },
    };

    const byKey = presets[k];
    if (byKey) {
      return {
        ...byKey,
        icon,
        label: byKey.label || name || "Wallet",
      };
    }

    return {
      label: name || "Wallet",
      color: "#7dd3fc",
      border: "rgba(125, 211, 252, 0.30)",
      glow: "rgba(125, 211, 252, 0.16)",
      fallbackBg: "#0f172a",
      fallbackFg: "#e5f3ff",
      fallbackText: String((name || "W").slice(0, 1) || "W").toUpperCase(),
      icon,
    };
  } catch {
    return {
      label: "Wallet",
      color: "#7dd3fc",
      border: "rgba(125, 211, 252, 0.30)",
      glow: "rgba(125, 211, 252, 0.16)",
      fallbackBg: "#0f172a",
      fallbackFg: "#e5f3ff",
      fallbackText: "W",
      icon: "",
    };
  }
}

export default function OrderTicketWidget({
  apiBase,
  effectiveVenue,
  fmtNum,
  styles,
  otSymbol,
  setOtSymbol,
  appContainerRef,
  hideVenueNames = false,

  // driven by App.jsx "Hide table data" checkbox
  hideTableData = false,

  qty: qtyProp,
  setQty: setQtyProp,
  limitPrice: limitPriceProp,
  setLimitPrice: setLimitPriceProp,
}) {
  // Optional toast emitter (some app shells inject this; keep safe/no-op if absent)
  const onToast = (typeof window !== "undefined" && (window.__uttOnToast || window.uttOnToast))
    ? (window.__uttOnToast || window.uttOnToast)
    : undefined;

  const walletKit = useWallet();
  const walletKitButtonHostRef = useRef(null);
  const solanaRpcConnection = useMemo(() => new Connection(clusterApiUrl("mainnet-beta"), "confirmed"), []);

  const [side, setSide] = useState("buy");
  const [solanaOrderMode, setSolanaOrderMode] = useState("swap"); // solana_jupiter only: "swap" | "limit"
  const JUPITER_LIMIT_MIN_USD = 10.10;

  const [qtyLocal, setQtyLocal] = useState("");
  const [limitPriceLocal, setLimitPriceLocal] = useState("");

  const qty = qtyProp !== undefined ? qtyProp : qtyLocal;
  const setQty = typeof setQtyProp === "function" ? setQtyProp : setQtyLocal;

  const limitPrice = limitPriceProp !== undefined ? limitPriceProp : limitPriceLocal;
  const setLimitPrice = typeof setLimitPriceProp === "function" ? setLimitPriceProp : setLimitPriceLocal;

  // NEW: prevents auto-normalization from fighting the user's typing
  const limitEditingRef = useRef(false);
  const limitSourceRef = useRef("init"); // "user" | "blur" | "auto" | "sci" | "init"

  const [postOnly, setPostOnly] = useState(false);
  const [tif, setTif] = useState("gtc");
  const [solanaExpiryPreset, setSolanaExpiryPreset] = useState("never"); // solana_jupiter limit only
  const [solanaExpiryCustom, setSolanaExpiryCustom] = useState("");
  const [clientOid, setClientOid] = useState("");

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState(null);
  const [submitOk, setSubmitOk] = useState(null);

  const [showConfirm, setShowConfirm] = useState(false);

  // NEW: submission result modal (instead of inline JSON block)
  const [showSubmitResult, setShowSubmitResult] = useState(false);
  const [submitResultKind, setSubmitResultKind] = useState(null); // "ok" | "error"
  const [submitResultPayload, setSubmitResultPayload] = useState(null); // object|string
  const [submitResultText, setSubmitResultText] = useState(""); // preformatted string for display/copy
  const [submitResultTitle, setSubmitResultTitle] = useState(""); // heading for modal

  const venueLabel = hideVenueNames ? "••••" : String(effectiveVenue || "");

  const isSolanaDexVenue = useMemo(() => {
    const v = String(effectiveVenue || "").toLowerCase().trim();
    return v === "solana_jupiter" || v === "solana_dex" || v.startsWith("solana_");
  }, [effectiveVenue]);
  const isSolanaJupiterVenue = useMemo(() => {
    const v = String(effectiveVenue || "").toLowerCase().trim();
    return v === "solana_jupiter";
  }, [effectiveVenue]);
  const isPolkadotDexVenue = useMemo(() => {
    const v = String(effectiveVenue || "").toLowerCase().trim();
    return v === "polkadot_hydration" || v === "hydration" || v === "polkadot_dex" || v.startsWith("polkadot_");
  }, [effectiveVenue]);
  const isDexSwapVenue = isSolanaDexVenue || isPolkadotDexVenue;
  const isSolanaLimitMode = isSolanaJupiterVenue && solanaOrderMode === "limit";
  const [preferredSolanaWallet, setPreferredSolanaWallet] = useState(() => getPreferredSolanaWalletKey());
  const [preferredSolanaRouterMode, setPreferredSolanaRouterModeState] = useState(() => getPreferredSolanaRouterMode());
  const [walletStandardWallets, setWalletStandardWallets] = useState(() => getWalletStandardWallets());
  const [walletKitPendingConnectName, setWalletKitPendingConnectName] = useState("");
  const [preferredHydrationRouteMode, setPreferredHydrationRouteModeState] = useState(() => getPreferredHydrationRouteMode());
  const [polkadotSettingsOpen, setPolkadotSettingsOpen] = useState(false);
  const [preferredPolkadotWallet, setPreferredPolkadotWallet] = useState(() => getPreferredPolkadotWalletKey());
  const [polkadotWalletScanNonce, setPolkadotWalletScanNonce] = useState(0);
  const [polkadotWalletState, setPolkadotWalletState] = useState(() => ({
    key: getPreferredPolkadotWalletKey(),
    label: getPolkadotWalletLabel(getPreferredPolkadotWalletKey()),
    connected: false,
    address: null,
    accountName: "",
    accounts: [],
    extension: null,
    error: null,
  }));
  const [polkadotHydrationStatus, setPolkadotHydrationStatus] = useState(null);
  const [polkadotHydrationStatusLoading, setPolkadotHydrationStatusLoading] = useState(false);
  const [polkadotHydrationStatusError, setPolkadotHydrationStatusError] = useState(null);
  const [polkadotPriceStatus, setPolkadotPriceStatus] = useState(null);
  const [polkadotPriceStatusError, setPolkadotPriceStatusError] = useState(null);
  const [polkadotLiquidityWarning, setPolkadotLiquidityWarning] = useState(null);
  const [polkadotManualRouteAvailable, setPolkadotManualRouteAvailable] = useState(false);
  const [polkadotHydrationRouteProbe, setPolkadotHydrationRouteProbe] = useState(null);
  const [polkadotHydrationSwapTxProbe, setPolkadotHydrationSwapTxProbe] = useState(null);
  const [polkadotOrderbookSideGuard, setPolkadotOrderbookSideGuard] = useState(null);
  const polkadotHydrationStatusReqRef = useRef(0);
  const polkadotPriceStatusReqRef = useRef(0);
  const polkadotLiquidityReqRef = useRef(0);
  const polkadotSwapTxProbeReqRef = useRef(0);
  useEffect(() => { setPreferredSolanaWalletKey(preferredSolanaWallet); }, [preferredSolanaWallet]);
  useEffect(() => { setPreferredSolanaRouterMode(preferredSolanaRouterMode); }, [preferredSolanaRouterMode]);
  useEffect(() => { setPreferredHydrationRouteMode(preferredHydrationRouteMode); }, [preferredHydrationRouteMode]);
  useEffect(() => { setPreferredPolkadotWalletKey(preferredPolkadotWallet); }, [preferredPolkadotWallet]);
  useEffect(() => {
    if (!isPolkadotDexVenue) return;
    const bump = () => setPolkadotWalletScanNonce((x) => x + 1);
    bump();
    if (typeof window === "undefined") return undefined;
    window.addEventListener("focus", bump);
    const t = window.setTimeout(bump, 650);
    return () => {
      window.removeEventListener("focus", bump);
      window.clearTimeout(t);
    };
  }, [isPolkadotDexVenue]);
  useEffect(() => {
    if (!isSolanaDexVenue) return;
    const unsub = subscribeWalletStandardWallets((wallets) => setWalletStandardWallets(Array.isArray(wallets) ? wallets : []));
    void primeWalletStandardWallets().then(() => {
      try { setWalletStandardWallets(getWalletStandardWallets()); } catch {}
    });
    return () => {
      try { unsub?.(); } catch {}
    };
  }, [isSolanaDexVenue]);

  const walletKitRawAdapterName = useMemo(() => {
    return String(
      walletKit?.wallet?.adapter?.name ||
      walletKit?.wallet?.name ||
      walletKit?.wallet?.adapter?.url ||
      ""
    ).trim();
  }, [walletKit?.wallet]);

  const walletKitSelectedKey = useMemo(() => {
    const selectedName =
      walletKit?.wallet?.adapter?.name ||
      walletKit?.wallet?.adapter?.url ||
      walletKit?.wallet?.adapter?.icon ||
      walletKit?.wallet?.adapter?.publicKey ||
      walletKit?.wallet?.adapter?.toString?.() ||
      walletKit?.wallet?.name ||
      "";
    return classifyWalletAdapterNameToKey(selectedName);
  }, [walletKit?.wallet]);

  const walletKitConnected = useMemo(() => {
    return !!walletKit?.connected && !!walletKit?.publicKey;
  }, [walletKit?.connected, walletKit?.publicKey]);

  const walletKitBridgeProvider = useMemo(() => {
    if (!isSolanaDexVenue) return null;
    if (!walletKitSelectedKey) return null;
    return createWalletAdapterBridgeProvider(walletKit, solanaRpcConnection);
  }, [isSolanaDexVenue, walletKitSelectedKey, walletKit, solanaRpcConnection]);

  useEffect(() => {
    if (!isSolanaDexVenue) return;
    if (!walletKitConnected) return;
    if (!walletKitSelectedKey) return;
    if (preferredSolanaWallet === walletKitSelectedKey) return;
    setPreferredSolanaWallet(walletKitSelectedKey);
  }, [isSolanaDexVenue, walletKitConnected, walletKitSelectedKey, preferredSolanaWallet]);

  const resolveInjectedSolanaProvider = useMemo(() => {
    return (preferred) => {
      const baseProviders = getInjectedSolanaProviders(walletStandardWallets);
      const merged = { ...baseProviders };
      if (walletKitSelectedKey && walletKitBridgeProvider) {
        merged[walletKitSelectedKey] = walletKitBridgeProvider;
      }

      const order = ["jupiter", "solflare", "phantom", "backpack"];

      if (walletKitConnected && walletKitSelectedKey && merged[walletKitSelectedKey]) {
        return { key: walletKitSelectedKey, provider: merged[walletKitSelectedKey] };
      }

      const pref = String(preferred || "solflare").toLowerCase().trim();
      if (merged[pref]) return { key: pref, provider: merged[pref] };

      for (const key of order) {
        const p = merged[key];
        if (p && solanaProviderPubkeyBase58(p)) return { key, provider: p };
      }
      for (const key of order) {
        const p = merged[key];
        if (p) return { key, provider: p };
      }
      return { key: null, provider: null };
    };
  }, [walletStandardWallets, walletKitSelectedKey, walletKitBridgeProvider, walletKitConnected]);

  const installedSolanaWallets = useMemo(() => {
    if (!isSolanaDexVenue) return [];
    const base = getInstalledSolanaWalletOptions(walletStandardWallets);
    const labels = { jupiter: "Jupiter", solflare: "Solflare", phantom: "Phantom", backpack: "Backpack" };
    if (walletKitSelectedKey && !base.some((x) => x?.key === walletKitSelectedKey)) {
      return [{ key: walletKitSelectedKey, label: labels[walletKitSelectedKey] || walletKitSelectedKey }, ...base];
    }
    return base;
  }, [isSolanaDexVenue, walletStandardWallets, walletKitSelectedKey]);
  const solanaWalletState = useMemo(() => {
    if (!isSolanaDexVenue) return { key: null, label: null, connected: false, address: null };
    const { key, provider } = resolveInjectedSolanaProvider(preferredSolanaWallet);
    const labels = { jupiter: "Jupiter", solflare: "Solflare", phantom: "Phantom", backpack: "Backpack" };
    const address = solanaProviderPubkeyBase58(provider);
    return {
      key,
      label: labels[key] || "Wallet",
      connected: !!address,
      address: address || null,
    };
  }, [isSolanaDexVenue, preferredSolanaWallet, resolveInjectedSolanaProvider]);
  const solanaWalletLabel = solanaWalletState.label;
  const solanaWalletConnected = solanaWalletState.connected;

  const installedPolkadotWallets = useMemo(() => {
    if (!isPolkadotDexVenue) return [];
    return getInjectedPolkadotWalletOptions();
  }, [isPolkadotDexVenue, polkadotWalletScanNonce]);

  useEffect(() => {
    if (!isPolkadotDexVenue) return;
    const preferredNorm = normalizePolkadotExtensionKey(preferredPolkadotWallet);
    const selected = installedPolkadotWallets.find((x) => normalizePolkadotExtensionKey(x?.key) === preferredNorm) || installedPolkadotWallets[0] || null;
    if (selected?.key && normalizePolkadotExtensionKey(selected.key) !== preferredNorm) {
      setPreferredPolkadotWallet(selected.key);
    }
    setPolkadotWalletState((prev) => {
      if (prev?.connected && prev?.address) return prev;
      const key = selected?.key || preferredPolkadotWallet || "subwallet-js";
      return {
        ...prev,
        key,
        label: getPolkadotWalletLabel(key),
        connected: false,
        address: null,
        accountName: "",
        accounts: [],
        extension: null,
        error: installedPolkadotWallets.length ? null : "SubWallet not detected",
      };
    });
  }, [isPolkadotDexVenue, installedPolkadotWallets, preferredPolkadotWallet]);

  const polkadotWalletConnected = !!polkadotWalletState?.connected && !!polkadotWalletState?.address;
  const polkadotWalletLabel = polkadotWalletState?.label || getPolkadotWalletLabel(preferredPolkadotWallet);


  const [inlineMode, setInlineMode] = useState(true);

  // Right-rail tile containment mode: keep this widget fully contained inside the App rail tile.
  const forceTileMode = true;

  const DEFAULT_W = 420;
  const DEFAULT_H = 330;

  const MIN_W = 320;
  const MIN_H = 250;
  const MAX_W = 900;

  const MAX_H = useMemo(() => {
    const vh = HAS_WINDOW && Number.isFinite(window.innerHeight) ? window.innerHeight : 700;
    return Math.max(250, Math.floor(vh * 0.85));
  }, []);

  // 8.5C: Order Ticket lock control removed. Keep the widget explicitly unlocked so
  // older localStorage values cannot leave the tile invisibly locked.
  const locked = false;

  const [box, setBox] = useState(() => {
    const saved = safeJsonParse(lsGet(LS_OT_BOX, "null"), null);
    return saved && typeof saved === "object"
      ? { x: saved.x ?? 0, y: saved.y ?? 0, w: saved.w ?? DEFAULT_W, h: saved.h ?? DEFAULT_H }
      : { x: 0, y: 0, w: DEFAULT_W, h: DEFAULT_H };
  });

  // ─────────────────────────────────────────────────────────────
  // Total (Quote) ↔ Qty (Bidirectional auto-calc)
  // ─────────────────────────────────────────────────────────────
  const [totalQuote, setTotalQuote] = useState(() => lsGet(LS_OT_TOTAL_USD, "") || "");
  const [autoCalc, setAutoCalc] = useState(() => lsGet(LS_OT_AUTOQTY, "1") !== "0");

  const lastEditedRef = useRef("total"); // "total" | "qty"
  const autoCalcWriteGuardRef = useRef({ qty: null, total: null });

  useEffect(() => lsSet(LS_OT_TOTAL_USD, String(totalQuote ?? "")), [totalQuote]);
  useEffect(() => lsSet(LS_OT_AUTOQTY, autoCalc ? "1" : "0"), [autoCalc]);

  useEffect(() => lsSet(LS_OT_LOCK, "0"), []);
  useEffect(() => lsSet(LS_OT_BOX, JSON.stringify(box)), [box]);


  const lockedRef = useRef(locked);
  const boxRef = useRef(box);
  useEffect(() => { lockedRef.current = locked; }, [locked]);
  useEffect(() => { boxRef.current = box; }, [box]);

  const dragStateRef = useRef(null);
  const resizeStateRef = useRef(null);

  // NOTE: Use the same coordinate-space model as OrderBookWidget.
  // We store and clamp x/y in *page* coords (visualViewport offsets applied), because
  // Brave vertical tabs / docked DevTools shift visualViewport and can otherwise cause
  // x to be permanently clamped to a boundary (making horizontal drag feel "stuck").
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

    // If we have an app container, treat its right edge as the gutter split.
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
        gutterWidth: Math.max(0, vw - margin * 2),
        vw,
        vh,
        ox,
        oy,
      };
    }

    // rect.* are relative to the current visual viewport; convert to absolute page coords.
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


  function clamp(n, lo, hi) {
    if (!Number.isFinite(n)) return lo;
    return Math.max(lo, Math.min(hi, n));
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
          // b.vw/vh are visualViewport dimensions; b.ox/oy are offsets (page coords).
          const vwAbs = (Number.isFinite(b.ox) ? b.ox : 0) + (Number.isFinite(b.vw) ? b.vw : (HAS_WINDOW ? window.innerWidth : 1200));
          const vhAbs = (Number.isFinite(b.oy) ? b.oy : 0) + (Number.isFinite(b.vh) ? b.vh : (HAS_WINDOW ? window.innerHeight : 800));
          const w = clamp(prev.w || DEFAULT_W, MIN_W, Math.min(MAX_W, b.gutterWidth));
          const h = clamp(prev.h || DEFAULT_H, MIN_H, Math.min(MAX_H, b.maxY - b.minY));

          if (lockedRef.current) {
            const curX = Number.isFinite(prev.x) ? prev.x : b.minX;
            const curY = Number.isFinite(prev.y) ? prev.y : b.minY;

            const left = Number.isFinite(prev.left) ? prev.left : (curX - b.minX);
            const top = Number.isFinite(prev.top) ? prev.top : (curY - b.minY);
            const right = Number.isFinite(prev.right) ? prev.right : (vwAbs - (curX + w));
            const bottom = Number.isFinite(prev.bottom) ? prev.bottom : (vhAbs - (curY + h));

            const anchorX = prev.anchorX === "right" || prev.anchorX === "left"
              ? prev.anchorX
              : (left <= right ? "left" : "right");
            const anchorY = prev.anchorY === "bottom" || prev.anchorY === "top"
              ? prev.anchorY
              : (top <= bottom ? "top" : "bottom");

            const rawX = anchorX === "right" ? (vwAbs - w - right) : (b.minX + left);
            const rawY = anchorY === "bottom" ? (vhAbs - h - bottom) : (b.minY + top);

            const x = clamp(rawX, b.minX, b.maxX - w);
            const y = clamp(rawY, b.minY, b.maxY - h);
            const clamped = clampBox({ x, y, w, h });
            // Preserve lock metadata so we don't "re-decide" anchors on overlay/resize.
            return { ...prev, ...clamped, left, top, right, bottom, anchorX, anchorY };
          }

          const x = clamp(prev.x ?? b.minX, b.minX, b.maxX - w);
          const y = clamp(prev.y ?? b.maxY - h, b.minY, b.maxY - h);
          return clampBox({ x, y, w, h });
        });
      }
    };

    recompute();
    if (HAS_WINDOW) window.addEventListener("resize", recompute);
    return () => {
      if (HAS_WINDOW) window.removeEventListener("resize", recompute);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [forceTileMode]);

  useEffect(() => {
    if (inlineMode) return;
    setBox((prev) => clampBox(prev));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [inlineMode, effectiveVenue]);

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
    // Resize from bottom-right corner (keep x/y fixed; change w/h).
    resizeStateRef.current = { startX: e.clientX, startY: e.clientY, startBox: start };

    const onMove = (ev) => {
      const st = resizeStateRef.current;
      if (!st) return;

      const dx = ev.clientX - st.startX;
      const dy = ev.clientY - st.startY;

      const rawW = st.startBox.w + dx;
      const rawH = st.startBox.h + dy;

      const b = getGutterBounds();
      const w = clamp(rawW, MIN_W, Math.min(MAX_W, b.maxX - b.minX));
      const h = clamp(rawH, MIN_H, Math.min(MAX_H, b.maxY - b.minY));

      const x = st.startBox.x;
      const y = st.startBox.y;

      setBox(clampBox({ x, y, w, h }));
    };

    const onUp = () => {
      resizeStateRef.current = null;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }

  // ─────────────────────────────────────────────────────────────
  // Symbol parsing (base/quote) + balances available
  // ─────────────────────────────────────────────────────────────
  function parseBaseQuote(sym) {
    const s = String(sym || "").trim().toUpperCase();
    if (!s) return { base: null, quote: null };

    let base = null;
    let quote = null;

    if (s.includes("-")) {
      const [a, b] = s.split("-");
      base = (a || "").trim() || null;
      quote = (b || "").trim() || null;
    } else if (s.includes("/")) {
      const [a, b] = s.split("/");
      base = (a || "").trim() || null;
      quote = (b || "").trim() || null;
    } else {
      base = s || null;
      quote = null;
    }

    return { base, quote };
  }

  const { base: baseAsset, quote: quoteAsset } = useMemo(() => parseBaseQuote(otSymbol), [otSymbol]);

  const quoteIsUsdLike = useMemo(() => {
    const q = String(quoteAsset || "").toUpperCase().trim();
    return q === "USD" || q === "USDT" || q === "USDC";
  }, [quoteAsset]);

  const totalQuoteDecimals = useMemo(() => (quoteIsUsdLike ? 2 : 8), [quoteIsUsdLike]);

  const polkadotQuoteStatus = polkadotHydrationStatus?.quoteStatus || null;
  const polkadotQuotesAvailable = polkadotQuoteStatus?.available === true && polkadotHydrationStatus?.liveQuotesEnabled === true;
  const polkadotSwapTxAvailable = polkadotQuotesAvailable && (
    polkadotQuoteStatus?.swapTxEnabled === true ||
    polkadotQuoteStatus?.liveSwapsRecommended === true ||
    polkadotHydrationStatus?.swapTxEnabled === true ||
    polkadotHydrationStatus?.liveSwapsRecommended === true
  );
  const polkadotLiveSwapsRecommended = polkadotSwapTxAvailable;
  const polkadotExactBuyEnabled = Boolean(
    polkadotQuoteStatus?.exactBuyEnabled === true ||
    polkadotHydrationStatus?.exactBuyEnabled === true ||
    polkadotQuoteStatus?.liveExactBuyRecommended === true ||
    polkadotHydrationStatus?.liveExactBuyRecommended === true
  );
  const polkadotStatusDetail = String(
    polkadotQuoteStatus?.reason ||
    polkadotHydrationStatusError ||
    "Hydration live quotes/swaps are disabled until a non-router quote source is selected."
  ).trim();
  const polkadotStatusReason = polkadotQuotesAvailable
    ? polkadotLiveSwapsRecommended
      ? polkadotExactBuyEnabled
        ? "Hydration live quotes and SubWallet swap signing/submission are enabled for controlled BUY and SELL testing."
        : "Hydration live quotes and SELL swap signing/submission are enabled. BUY remains disabled until UTT_HYDRATION_ENABLE_EXACT_BUY=1."
      : "Hydration live quotes are enabled. Unsigned swap transaction building remains disabled until slippage handling and SubWallet signing are verified."
    : "Hydration quotes/swaps are temporarily disabled. Asset resolution and balances are available. Waiting on a non-router quote source before live trading is enabled.";
  const polkadotManualRouterFallbackAvailable = Boolean(
    isPolkadotDexVenue &&
    polkadotHydrationSwapTxProbe?.manualRouterFallbackAvailable === true
  );
  const polkadotManualSwapAvailable = isPolkadotDexVenue && (polkadotManualRouteAvailable || polkadotManualRouterFallbackAvailable);
  const polkadotSyntheticPriceOnly = Boolean(
    isPolkadotDexVenue &&
    !polkadotManualSwapAvailable &&
    polkadotHydrationRouteProbe?.syntheticOnly === true
  );
  const polkadotSyntheticPriceOnlyReason = String(
    polkadotHydrationRouteProbe?.reason ||
    `Synthetic price only — no executable manual route registered for ${String(otSymbol || "this Hydration pair").trim().toUpperCase()}. Orderbook prices are external/cached context only.`
  ).trim();
  const polkadotEffectiveQuotesAvailable = polkadotQuotesAvailable || polkadotManualSwapAvailable;
  const polkadotEffectiveLiveSwapsRecommended = polkadotLiveSwapsRecommended || polkadotManualSwapAvailable;
  const polkadotEffectiveExactBuyEnabled = polkadotExactBuyEnabled || polkadotManualSwapAvailable;
  const polkadotEffectiveStatusReason = polkadotManualRouterFallbackAvailable && !polkadotQuotesAvailable
    ? "Hydration generic SDK quotes are disabled, but this pair has a controlled manual Router fallback available."
    : polkadotManualRouteAvailable && !polkadotQuotesAvailable
      ? "Hydration generic SDK quotes are disabled, but this pair has a backend manual XYK/live-pool route available."
      : polkadotSyntheticPriceOnly
        ? polkadotSyntheticPriceOnlyReason
        : polkadotStatusReason;
  const polkadotPriceStatusDisplay = hydrationPriceStatusView(polkadotPriceStatus, polkadotPriceStatusError);

  useEffect(() => {
    const sym = String(otSymbol || "").trim().toUpperCase();
    if (!isPolkadotDexVenue || !apiBase || !sym || !sym.includes("-")) {
      setPolkadotHydrationStatus(null);
      setPolkadotHydrationStatusError(null);
      setPolkadotHydrationStatusLoading(false);
      return;
    }

    const reqId = ++polkadotHydrationStatusReqRef.current;
    let cancelled = false;

    const t = setTimeout(async () => {
      try {
        setPolkadotHydrationStatusLoading(true);
        setPolkadotHydrationStatusError(null);

        const url = new URL(`${apiBase}/api/polkadot_dex/hydration/status`);
        url.searchParams.set("symbol", sym);
        url.searchParams.set("_ts", String(Date.now()));

        const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
        if (!r.ok) {
          const txt = await r.text().catch(() => "");
          throw new Error(txt || `Hydration status HTTP ${r.status}`);
        }

        const data = await r.json();
        if (cancelled || polkadotHydrationStatusReqRef.current !== reqId) return;
        setPolkadotHydrationStatus(data || null);
      } catch (e) {
        if (cancelled || polkadotHydrationStatusReqRef.current !== reqId) return;
        setPolkadotHydrationStatus(null);
        setPolkadotHydrationStatusError(e?.message || "Failed to load Hydration status.");
      } finally {
        if (!cancelled && polkadotHydrationStatusReqRef.current === reqId) setPolkadotHydrationStatusLoading(false);
      }
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [isPolkadotDexVenue, apiBase, otSymbol]);

  useEffect(() => {
    const sym = String(otSymbol || "").trim().toUpperCase();
    if (!isPolkadotDexVenue || !apiBase || !sym || !sym.includes("-")) {
      setPolkadotPriceStatus(null);
      setPolkadotPriceStatusError(null);
      return;
    }

    const reqId = ++polkadotPriceStatusReqRef.current;
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
        if (cancelled || polkadotPriceStatusReqRef.current !== reqId) return;
        setPolkadotPriceStatus(data || null);
        setPolkadotPriceStatusError(null);
      } catch (e) {
        if (cancelled || polkadotPriceStatusReqRef.current !== reqId) return;
        setPolkadotPriceStatus(null);
        setPolkadotPriceStatusError(e?.message || "Failed to load Hydration price status.");
      }
    }, 350);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [isPolkadotDexVenue, apiBase, otSymbol]);

  useEffect(() => {
    const sym = String(otSymbol || "").trim().toUpperCase();
    if (!isPolkadotDexVenue || !apiBase || !sym || !sym.includes("-")) {
      setPolkadotLiquidityWarning(null);
      setPolkadotManualRouteAvailable(false);
      setPolkadotHydrationRouteProbe(null);
      setPolkadotHydrationSwapTxProbe(null);
      setPolkadotOrderbookSideGuard(null);
      return;
    }

    const reqId = ++polkadotLiquidityReqRef.current;
    let cancelled = false;

    const t = setTimeout(async () => {
      const preferredMode = normalizeHydrationRouteMode(preferredHydrationRouteMode);
      const probeModes = preferredMode === "auto" ? ["auto"] : [preferredMode, "auto"];
      let lastPayload = null;

      try {
        for (const mode of probeModes) {
          const url = new URL(`${apiBase}/api/polkadot_dex/hydration/orderbook`);
          url.searchParams.set("symbol", sym);
          url.searchParams.set("depth", "3");
          url.searchParams.set("route_mode", mode);
          url.searchParams.set("force", "true");
          url.searchParams.set("_ts", String(Date.now()));

          const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
          if (!r.ok) {
            lastPayload = await r.json().catch(() => null);
            continue;
          }

          const data = await r.json().catch(() => null);
          if (!data || typeof data !== "object") continue;
          lastPayload = data;
          break;
        }

        if (cancelled || polkadotLiquidityReqRef.current !== reqId) return;

        if (!lastPayload || lastPayload?.ok === false || lastPayload?.detail?.error) {
          setPolkadotLiquidityWarning(null);
          setPolkadotManualRouteAvailable(false);
          setPolkadotHydrationRouteProbe(null);
          setPolkadotHydrationSwapTxProbe(null);
          setPolkadotOrderbookSideGuard(null);
          return;
        }

        const routeProbe = hydrationRouteProbeView(lastPayload, sym);
        setPolkadotLiquidityWarning(buildHydrationLowLiquidityWarning(lastPayload));
        setPolkadotManualRouteAvailable(routeProbe.manualRouteAvailable === true);
        setPolkadotHydrationRouteProbe(routeProbe);
        setPolkadotOrderbookSideGuard(hydrationOrderbookSideGuardView(lastPayload, sym));
      } catch {
        if (!cancelled && polkadotLiquidityReqRef.current === reqId) {
          setPolkadotLiquidityWarning(null);
          setPolkadotManualRouteAvailable(false);
          setPolkadotHydrationRouteProbe(null);
          setPolkadotHydrationSwapTxProbe(null);
          setPolkadotOrderbookSideGuard(null);
        }
      }
    }, 450);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [isPolkadotDexVenue, apiBase, otSymbol, preferredHydrationRouteMode]);

  useEffect(() => {
    const sym = String(otSymbol || "").trim().toUpperCase();
    // Keep this probe above the derived qtyNum declaration without tripping
    // React's temporal-dead-zone rules. qtyNum is declared later in the file,
    // so this early probe must parse the raw Qty field directly.
    const amount = Number(qty);
    const address = String(
      polkadotWalletState?.address ||
      polkadotWalletState?.accounts?.[0]?.address ||
      ""
    ).trim();

    if (
      !isPolkadotDexVenue ||
      !apiBase ||
      !sym ||
      !sym.includes("-") ||
      !(side === "buy" || side === "sell") ||
      !Number.isFinite(amount) ||
      amount <= 0 ||
      !address
    ) {
      setPolkadotHydrationSwapTxProbe(null);
      return;
    }

    const reqId = ++polkadotSwapTxProbeReqRef.current;
    let cancelled = false;

    const t = setTimeout(async () => {
      try {
        const base = String(apiBase || "").replace(/\/+$/, "");
        const r = await fetch(`${base}/api/polkadot_dex/hydration/swap_tx`, {
          method: "POST",
          cache: "no-store",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            symbol: sym,
            side,
            amount,
            amount_mode: side === "buy" ? "exact_out" : "exact_in",
            route_mode: "auto",
            slippage_bps: 100,
            user_pubkey: address,
          }),
        });

        const data = await r.json().catch(() => null);
        if (cancelled || polkadotSwapTxProbeReqRef.current !== reqId) return;

        if (!r.ok || !data || data?.ok === false) {
          setPolkadotHydrationSwapTxProbe(null);
          return;
        }

        const probe = hydrationSwapTxProbeView(data, sym);
        setPolkadotHydrationSwapTxProbe(probe.manualRouterFallbackAvailable ? probe : null);
      } catch {
        if (!cancelled && polkadotSwapTxProbeReqRef.current === reqId) {
          setPolkadotHydrationSwapTxProbe(null);
        }
      }
    }, 650);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [isPolkadotDexVenue, apiBase, otSymbol, side, qty, polkadotWalletState?.address, polkadotWalletState?.accounts, preferredHydrationRouteMode]);

  // Null-safe number parsing (prevents null → 0 bugs via Number(null))
  const toFiniteOrNull = (x) => {
    if (x === null || x === undefined) return null;
    const n = Number(x);
    return Number.isFinite(n) ? n : null;
  };

  // ─────────────────────────────────────────────────────────────
  // Order Rules
  // ─────────────────────────────────────────────────────────────
  const [rulesLoading, setRulesLoading] = useState(false);
  const [rulesErr, setRulesErr] = useState(null);
  const [rules, setRules] = useState(null);
  const rulesReqIdRef = useRef(0);

  useEffect(() => {
    const v = String(effectiveVenue || "").trim().toLowerCase();
    const s = String(otSymbol || "").trim();

    if (!v || !s) {
      setRules(null);
      setRulesErr(null);
      setRulesLoading(false);
      return;
    }

    const reqId = ++rulesReqIdRef.current;
    let cancelled = false;

    const t = setTimeout(async () => {
      try {
        setRulesLoading(true);
        setRulesErr(null);

        const data = await getOrderRules(
          { venue: v, symbol: s, side, type: "limit", tif, post_only: postOnly },
          { apiBase }
        );

        if (cancelled || rulesReqIdRef.current !== reqId) return;

        // Solana-Jupiter is swap-style; if backend returns "unknown constraints" (often with 0 decimals),
        // override with sane defaults so ticket math + validation behaves like CEX precision-wise.
        const vLower = String(v || "").trim().toLowerCase();
        const isSol = vLower === "solana_jupiter" || vLower === "solana-dex" || vLower.startsWith("solana_");
        const isDot = vLower === "polkadot_hydration" || vLower === "hydration" || vLower === "polkadot_dex" || vLower.startsWith("polkadot_");
        const warns = Array.isArray(data?.warnings) ? data.warnings.map((x) => String(x || "")) : [];
        const warnText = warns.join(" ").toLowerCase();

        if (
          isSol &&
          (
            data == null ||
            Number(data?.price_decimals ?? 0) <= 0 ||
            Number(data?.qty_decimals ?? 0) <= 0 ||
            warnText.includes("does not implement get_order_rules") ||
            warnText.includes("constraints unknown")
          )
        ) {
          setRules({
            ...(data || {}),
            venue: vLower,
            symbol: s,
            type: "swap",
            price_decimals: 9,
            qty_decimals: 6,
            price_increment: 0.000000001,
            qty_increment: 0.000001,
            min_qty: Number(data?.min_qty ?? 0) || 0,
            min_notional: Number(data?.min_notional ?? 0) || 0,
            errors: [],
            warnings: [],
          });
          setRulesErr(null);
        } else if (
          isDot &&
          (
            data == null ||
            Number(data?.price_decimals ?? 0) <= 0 ||
            Number(data?.qty_decimals ?? 0) <= 0 ||
            warnText.includes("does not implement get_order_rules") ||
            warnText.includes("constraints unknown")
          )
        ) {
          setRules({
            ...(data || {}),
            venue: vLower,
            symbol: s,
            type: "swap",
            price_decimals: 12,
            qty_decimals: 12,
            price_increment: 0.000000000001,
            qty_increment: 0.000000000001,
            min_qty: Number(data?.min_qty ?? 0) || 0,
            min_notional: Number(data?.min_notional ?? 0) || 0,
            errors: [],
            warnings: [],
          });
          setRulesErr(null);
        } else {
          // Solana pairs quoted in SOL often require 9dp pricing; ensure we never clamp below that.
          if (isSol && data && typeof data === "object") {
            const symU = String(s || "").toUpperCase();
            const isSolQuoted = symU.endsWith("-SOL") || symU.startsWith("SOL-");
            if (isSolQuoted) {
              const pd = Math.max(Number(data?.price_decimals ?? 0) || 0, 9);
              const pi = 1 / Math.pow(10, pd);
              setRules({
                ...data,
                price_decimals: pd,
                price_increment: Number.isFinite(pi) ? pi : data?.price_increment,
              });
              return;
            }
          }
          setRules(data || null);
        }
      } catch (e) {
        if (cancelled || rulesReqIdRef.current !== reqId) return;

        const vLower = String(v || "").trim().toLowerCase();
        const errMsg = extractRulesError(e);

        // Solana-Jupiter is swap-style; if backend doesn't implement get_order_rules yet,
        // fall back to sane decimals so UI doesn't clamp to 0 and block.
        if (
          (vLower === "solana_jupiter" || vLower === "solana-dex" || vLower.startsWith("solana_")) &&
          typeof errMsg === "string" &&
          (
            errMsg.toLowerCase().includes("does not implement get_order_rules") ||
            errMsg.toLowerCase().includes("constraints unknown")
          )
        ) {
          setRules({
            venue: vLower,
            symbol: s,
            type: "swap",
            // Conservative defaults for USDC/SOL style quoting
            price_decimals: 9,
            qty_decimals: 6,
            price_increment: 0.000000001,
            qty_increment: 0.000001,
            min_qty: 0,
            min_notional: 0,
            errors: [],
          });
          setRulesErr(null);
        } else if (
          (vLower === "polkadot_hydration" || vLower === "hydration" || vLower === "polkadot_dex" || vLower.startsWith("polkadot_")) &&
          typeof errMsg === "string" &&
          (
            errMsg.toLowerCase().includes("does not implement get_order_rules") ||
            errMsg.toLowerCase().includes("constraints unknown") ||
            errMsg.toLowerCase().includes("404")
          )
        ) {
          setRules({
            venue: vLower,
            symbol: s,
            type: "swap",
            // Conservative defaults for DOT/Asset Hub/Hydration-style precision.
            price_decimals: 12,
            qty_decimals: 12,
            price_increment: 0.000000000001,
            qty_increment: 0.000000000001,
            min_qty: 0,
            min_notional: 0,
            errors: [],
          });
          setRulesErr(null);
        } else {
          setRules(null);
          setRulesErr(errMsg);
        }
      } finally {
        if (cancelled || rulesReqIdRef.current !== reqId) return;
        setRulesLoading(false);
      }
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [effectiveVenue, otSymbol, side, tif, postOnly, apiBase]);

  // ─────────────────────────────────────────────────────────────
  // Helpers: formatting + increment math
  // ─────────────────────────────────────────────────────────────
  function trimFixedStr(s) {
    const x = String(s ?? "");
    if (!x) return "";
    if (!x.includes(".")) return x;
    return x.replace(/0+$/g, "").replace(/\.$/g, "");
  }

  function fmtStepValue(v, decimalsHint) {
    if (v === null || v === undefined) return null;

    const raw = String(v).trim();
    if (!raw) return null;

    const n = Number(raw);
    if (!Number.isFinite(n)) return raw;

    const dh = Number(decimalsHint);
    if (Number.isFinite(dh) && dh >= 0) {
      const cap = Math.min(Math.max(Math.trunc(dh), 0), 18);
      return trimFixedStr(n.toFixed(cap));
    }

    return n.toLocaleString(undefined, { useGrouping: false, maximumFractionDigits: 18 });
  }

  function countDecimalsFromString(x) {
    const s = String(x ?? "").trim();
    if (!s) return 0;
    if (s.includes("e") || s.includes("E")) return null;
    const i = s.indexOf(".");
    if (i < 0) return 0;
    return Math.max(0, s.length - i - 1);
  }

  function decimalsFromIncrement(x) {
    const s = String(x ?? "").trim();
    if (!s) return null;

    if (s.includes("e") || s.includes("E")) {
      const n = Number(s);
      if (!Number.isFinite(n) || n <= 0) return null;
      const p = Math.round(-Math.log10(n));
      return Number.isFinite(p) && p >= 0 && p <= 18 ? p : null;
    }

    const i = s.indexOf(".");
    if (i < 0) return 0;
    return Math.max(0, s.length - i - 1);
  }

  function parseDecimalToScaledInt(str, scaleDec) {
    const s0 = String(str ?? "").trim();
    if (!s0) return null;
    if (s0.includes("e") || s0.includes("E")) return null;

    const neg = s0.startsWith("-");
    const s = neg ? s0.slice(1) : s0;

    const parts = s.split(".");
    const whole = parts[0] || "0";
    const frac = parts[1] || "";

    if (!/^\d+$/.test(whole) || (frac && !/^\d+$/.test(frac))) return null;

    const fracPadded = (frac + "0".repeat(scaleDec)).slice(0, scaleDec);
    const combined = (whole.replace(/^0+(?=\d)/, "") || "0") + fracPadded;

    const combinedNorm = combined.replace(/^0+(?=\d)/, "") || "0";
    try {
      const bi = BigInt(combinedNorm);
      return neg ? -bi : bi;
    } catch {
      return null;
    }
  }

  function isMultipleOfStep(valueStr, stepStr, decimalsHint) {
    const dec = Number.isFinite(Number(decimalsHint))
      ? Math.min(Math.max(Math.trunc(Number(decimalsHint)), 0), 18)
      : decimalsFromIncrement(stepStr);
    if (dec === null || dec === undefined) return null;

    const vInt = parseDecimalToScaledInt(valueStr, dec);
    const sInt = parseDecimalToScaledInt(stepStr, dec);
    if (vInt === null || sInt === null) return null;
    if (sInt === 0n) return null;

    return vInt % sInt === 0n;
  }

  function floorToStepNumber(rawNum, stepStr, decimalsHint) {
    const stepNum = Number(stepStr);
    if (!Number.isFinite(rawNum) || rawNum <= 0) return null;
    if (!Number.isFinite(stepNum) || stepNum <= 0) return rawNum;

    const dec = Number.isFinite(Number(decimalsHint))
      ? Math.min(Math.max(Math.trunc(Number(decimalsHint)), 0), 18)
      : decimalsFromIncrement(stepStr);

    if (!Number.isFinite(dec) || dec === null || dec === undefined) {
      const k = Math.floor(rawNum / stepNum);
      const q = k * stepNum;
      return Number.isFinite(q) && q > 0 ? q : null;
    }

    const scale = 10 ** dec;
    if (!Number.isFinite(scale) || scale <= 0) return null;

    const rawScaled = Math.floor(rawNum * scale + 1e-9);
    const stepScaled = Math.round(stepNum * scale);
    if (!Number.isFinite(rawScaled) || !Number.isFinite(stepScaled) || stepScaled <= 0) return null;

    const flooredScaled = Math.floor(rawScaled / stepScaled) * stepScaled;
    const q = flooredScaled / scale;
    return Number.isFinite(q) && q > 0 ? q : null;
  }

  // NEW: ceil rounding to step (used for SELL limit safety)
  function ceilToStepNumber(rawNum, stepStr, decimalsHint) {
    const stepNum = Number(stepStr);
    if (!Number.isFinite(rawNum) || rawNum <= 0) return null;
    if (!Number.isFinite(stepNum) || stepNum <= 0) return rawNum;

    const dec = Number.isFinite(Number(decimalsHint))
      ? Math.min(Math.max(Math.trunc(Number(decimalsHint)), 0), 18)
      : decimalsFromIncrement(stepStr);

    if (!Number.isFinite(dec) || dec === null || dec === undefined) {
      const k = Math.ceil(rawNum / stepNum);
      const q = k * stepNum;
      return Number.isFinite(q) && q > 0 ? q : null;
    }

    const scale = 10 ** dec;
    if (!Number.isFinite(scale) || scale <= 0) return null;

    // Ceil in scaled space; a tiny epsilon avoids accidental bump from float noise
    const rawScaled = Math.ceil(rawNum * scale - 1e-9);
    const stepScaled = Math.round(stepNum * scale);
    if (!Number.isFinite(rawScaled) || !Number.isFinite(stepScaled) || stepScaled <= 0) return null;

    const ceiledScaled = Math.ceil(rawScaled / stepScaled) * stepScaled;
    const q = ceiledScaled / scale;
    return Number.isFinite(q) && q > 0 ? q : null;
  }

  // ─────────────────────────────────────────────────────────────
  // NEW: sanitize numeric input as a string (prevents “e-” from persisting)
  // ─────────────────────────────────────────────────────────────
  function sanitizeDecimalInput(raw, { allowLeadingDot = true } = {}) {
    let s = String(raw ?? "");
    if (!s) return "";

    // Expand scientific notation if user pasted/auto-set it.
    s = expandExponential(s);

    // Normalize separators/spaces
    s = s.replace(/,/g, "").trim();

    // Keep digits + dot only
    s = s.replace(/[^\d.]/g, "");

    // Only one dot
    const firstDot = s.indexOf(".");
    if (firstDot >= 0) {
      const left = s.slice(0, firstDot + 1);
      const right = s.slice(firstDot + 1).replace(/\./g, "");
      s = left + right;
    }

    if (allowLeadingDot && s.startsWith(".")) s = `0${s}`;

    return s;
  }

  // ─────────────────────────────────────────────────────────────
  // NEW: normalize limit price string to venue tick/decimals (UI-side)
  //      Directional rounding:
  //        - BUY: floor (do not exceed user's max)
  //        - SELL: ceil  (do not go below user's min)
  // ─────────────────────────────────────────────────────────────
  function normalizeLimitPriceStr(rawStr, rulesObj, sideForRounding) {
    const cleaned = sanitizeDecimalInput(expandExponential(rawStr));
    if (!cleaned) return "";

    // If rules are unavailable or contain errors, do not mutate user entry.
    if (!rulesObj) return cleaned;
    const errs = Array.isArray(rulesObj?.errors) ? rulesObj.errors : [];
    if (errs.length > 0) return cleaned;

    const pi = rulesObj?.price_increment;
    const pxDec = rulesObj?.price_decimals;

    const n = Number(cleaned);
    if (!Number.isFinite(n) || n <= 0) return cleaned;

    const roundingSide = String(sideForRounding || "").toLowerCase().trim();
    const wantCeil = roundingSide === "sell";

    // Prefer tick quantization when available.
    if (pi !== null && pi !== undefined && String(pi).trim() && Number(pi) > 0) {
      const piStr = String(pi).trim();
      const qNum = wantCeil ? ceilToStepNumber(n, piStr, pxDec) : floorToStepNumber(n, piStr, pxDec);
      if (qNum === null) return cleaned;

      const dec =
        Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0
          ? Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18)
          : decimalsFromIncrement(piStr);

      if (dec === null || dec === undefined || !Number.isFinite(dec)) {
        return String(qNum);
      }

      // For prices, keep fixed decimals (cents) instead of trimming.
      return Number(qNum).toFixed(dec);
    }

    // Fallback: clamp by decimals if we have them.
    if (Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0) {
      const dec = Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18);
      return Number(n).toFixed(dec);
    }

    return cleaned;
  }

  // If upstream provides sci notation (number or "5.6e-8" string), normalize it once.
  // Do not fight the user while they are typing.
  useEffect(() => {
    if (limitEditingRef.current) return;

    const s = String(limitPrice ?? "").trim();
    if (!s) return;
    if (/[eE]/.test(s)) {
      const expanded = expandExponential(s);
      const cleaned = sanitizeDecimalInput(expanded);
      if (cleaned && cleaned !== s) {
        limitSourceRef.current = "sci";
        setLimitPrice(cleaned);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [limitPrice]);

  // NEW: auto-normalize limit price when rules/side/limitPrice change,
  // but only when the user is not actively editing the Limit field.
  useEffect(() => {
    if (limitEditingRef.current) return;

    // DEX (Solana) venues: limit price is informational for swap-style flows.
    // Do NOT clamp/round it using CEX-style venue rules (which may be unknown and default to 0 decimals).
    if (isDexSwapVenue) return;

    const lp = String(limitPrice ?? "");
    if (!lp) return;

    const normalized = normalizeLimitPriceStr(lp, rules, side);
    if (!normalized) return;

    // Avoid loops and avoid pointless writes.
    if (String(normalized) !== String(lp)) {
      limitSourceRef.current = "auto";
      setLimitPrice(normalized);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rules, side, limitPrice]);

  // ─────────────────────────────────────────────────────────────
  // Effective min qty (gated to Crypto.com only)
  // ─────────────────────────────────────────────────────────────
  const isCryptoCom = useMemo(
    () => String(effectiveVenue || "").toLowerCase().trim() === "cryptocom",
    [effectiveVenue]
  );

  const limitPxNumForMin = useMemo(() => {
    const s = String(limitPrice ?? "").trim();
    if (!s) return null;
    const n = Number(expandExponential(s));
    if (!Number.isFinite(n) || n <= 0) return null;
    return n;
  }, [limitPrice]);

  const uiMinQty = useMemo(() => {
    if (!rules) return null;
    if (!isCryptoCom) return null; // critical: do not affect other venues

    const minNotional = toFiniteOrNull(rules?.min_notional);
    const qtyStep = toFiniteOrNull(rules?.base_increment);

    // Default display when we cannot compute from entered price
    const fallback = toFiniteOrNull(rules?.min_qty) ?? qtyStep;

    const px = limitPxNumForMin;

    // If we have min_notional and a user-entered price, compute required qty from that price.
    if (minNotional !== null && minNotional > 0 && px !== null && px > 0) {
      const rawReq = minNotional / px;

      if (qtyStep !== null && qtyStep > 0) {
        const rounded = ceilToStepNumber(rawReq, String(qtyStep), rules?.qty_decimals);
        if (rounded !== null && rounded > 0) return Math.max(qtyStep, rounded);
        return Math.max(qtyStep, rawReq);
      }

      return rawReq;
    }

    return fallback;
  }, [rules, limitPxNumForMin, isCryptoCom]);

  const rulesBanner = useMemo(() => {
    if (isPolkadotDexVenue) return null;
    if (rulesLoading) return { kind: "info", lines: ["Rules: loading…"] };

    if (rulesErr) {
      if (hideTableData) return { kind: "warn", lines: ["Rules: unavailable."] };
      return { kind: "warn", lines: [`Rules: ${rulesErr}`] };
    }

    if (!rules) return null;

    const errs = Array.isArray(rules?.errors) ? rules.errors : [];
    const warns = Array.isArray(rules?.warnings) ? rules.warnings : [];
    const suggested = rules?.suggested_symbol ? String(rules.suggested_symbol) : null;

    const lines = [];
    for (const e of errs) lines.push(hideTableData ? "Order rule error." : String(e));
    for (const w of warns) lines.push(hideTableData ? "Order rule warning." : String(w));

    if (lines.length === 0 && !hideTableData) {
      const bi = rules?.base_increment ?? null;
      const pi = rules?.price_increment ?? null;
      const mq = uiMinQty ?? (rules?.min_qty ?? null);
      const mn = rules?.min_notional ?? null;

      const biStr = fmtStepValue(bi, rules?.qty_decimals);
      const piStr = fmtStepValue(pi, rules?.price_decimals);

      const parts = [];
      if (biStr) parts.push(`qty step: ${biStr}`);
      if (piStr) parts.push(`price step: ${piStr}`);
      if (mq !== null && mq !== undefined) parts.push(`min qty: ${mq}`);
      if (mn !== null && mn !== undefined) parts.push(`min notional: ${mn}`);

      lines.push(parts.length ? `Rules: ok (${parts.join(" • ")})` : "Rules: ok");
    }

    if (suggested && !hideTableData) lines.push(`Suggested: ${suggested}`);

    if (lines.length === 0) return null;
    return { kind: errs.length > 0 ? "error" : "warn", lines };
  }, [rulesLoading, rulesErr, rules, hideTableData, uiMinQty, isPolkadotDexVenue]);

  const rulesBannerStyle = useMemo(() => {
    if (!rulesBanner) return null;
    if (rulesBanner.kind === "error") return { border: "1px solid #4a1f1f", background: "#160b0b", color: "#ffd2d2" };
    if (rulesBanner.kind === "info") return { border: "1px solid #2a2a2a", background: "#101010", color: "#cfcfcf" };
    return { border: "1px solid #3b3413", background: "#151208", color: "#f2e6b7" };
  }, [rulesBanner]);

  // ─────────────────────────────────────────────────────────────
  // Balances: available
  // ─────────────────────────────────────────────────────────────
  const [balAvail, setBalAvail] = useState({});
  const [balLoading, setBalLoading] = useState(false);
  const [balErr, setBalErr] = useState(null);
  const [balNotice, setBalNotice] = useState(null);

  useEffect(() => {
    if (!isDexSwapVenue) {
      setBalErr(null);
      setBalNotice(null);
    }
  }, [isDexSwapVenue]);

  useEffect(() => {
    if (!isSolanaJupiterVenue) setSolanaOrderMode("swap");
  }, [isSolanaJupiterVenue]);

  function normalizeBalItems(items, venueKey) {
    const out = {};
    for (const b of items || []) {
      const a = String(b?.asset || "").toUpperCase().trim();
      if (!a) continue;

      if (venueKey) {
        const vv = String(b?.venue || "").toLowerCase().trim();
        if (vv && vv !== venueKey) continue;
      }

      const available = Number(b?.available);
      const total = Number(b?.total);
      const hold = Number(b?.hold);

      out[a] = {
        available: Number.isFinite(available) ? available : null,
        total: Number.isFinite(total) ? total : null,
        hold: Number.isFinite(hold) ? hold : null,
      };
    }
    return out;
  }

  function computeBalHash(obj) {
    try {
      const entries = Object.entries(obj || {}).sort(([a], [b]) => String(a).localeCompare(String(b)));
      return JSON.stringify(entries);
    } catch {
      return "";
    }
  }

  // NEW: focus hashing for base/quote only (so we can wait for the pair we traded)
  function computeFocusHash(availObj, focusAssets) {
    try {
      const fa = Array.isArray(focusAssets) ? focusAssets.filter(Boolean) : [];
      const norm = fa.map((x) => String(x).toUpperCase().trim()).filter(Boolean);
      const parts = [];
      for (const a of norm) {
        const v = availObj?.[a] ?? null;
        parts.push([a, v?.available ?? null, v?.total ?? null, v?.hold ?? null]);
      }
      return JSON.stringify(parts);
    } catch {
      return "";
    }
  }


  // ─────────────────────────────────────────────────────────────
  // Solana DEX balances support (DEX-only, opt-in by venue)
  // Uses injected Solana wallet public key (Jupiter / Solflare / Phantom / Backpack) + backend /api/solana_dex endpoints.
  // ─────────────────────────────────────────────────────────────
  const solanaResolveCacheRef = useRef({}); // assetKey -> { mint, decimals }

  function getInjectedSolanaPubkeyBase58() {
    try {
      const { provider } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      return solanaProviderPubkeyBase58(provider);
    } catch {
      return null;
    }
  }

  function getInjectedSolanaWalletLabel() {
    try {
      const { key } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (key === "jupiter") return "Jupiter";
      if (key === "solflare") return "Solflare";
      if (key === "phantom") return "Phantom";
      if (key === "backpack") return "Backpack";
      return "Wallet";
    } catch {
      return "Wallet";
    }
  }

  function getInjectedSolanaWalletConnected() {
    try {
      return !!getInjectedSolanaPubkeyBase58();
    } catch {
      return false;
    }
  }

  function isBlockedJupiterTokenError(msg) {
    const s = String(msg || "").toLowerCase();
    if (!s) return false;
    return (
      s.includes("not tradable on jupiter") ||
      s.includes("not supported on jupiter") ||
      s.includes("token not tradable") ||
      s.includes("token_not_tradable") ||
      s.includes("not supported") ||
      s.includes("could not find any route") ||
      s.includes("no route") ||
      s.includes("route not found") ||
      s.includes("jupiter_swap_failed")
    );
  }

  async function fetchSolanaSwapTx({ provider, symbol, side, amount, address, slippageBps, tok }) {
    const base = String(apiBase || "").replace(/\/+$/, "");
    const route =
      provider === "raydium"
        ? `${base}/api/solana_dex/raydium/swap_tx`
        : provider === "jupiter_ultra"
          ? `${base}/api/solana_dex/jupiter/ultra_order`
          : `${base}/api/solana_dex/jupiter/swap_tx`;

    const headers = { "Content-Type": "application/json" };
    if (tok) headers.Authorization = `Bearer ${tok}`;

    const payload = {
      symbol,
      side,
      amount,
      slippage_bps: slippageBps,
      user_pubkey: address,
    };

    const r = await fetch(route, { method: "POST", headers, body: JSON.stringify(payload) });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `HTTP ${r.status}`);
    }

    const j = await r.json();
    const txB64 =
      j?.swapTransaction ||
      j?.transaction ||
      (Array.isArray(j?.transactions) && j.transactions.length ? j.transactions[0] : null);

    if (!txB64) {
      throw new Error(`Missing swap transaction in ${provider} response`);
    }

    return { provider: provider || "jupiter_metis", data: j, txB64: String(txB64) };
  }

  async function executeSolanaUltraSwap({ signedTransaction, requestId, tok }) {
    const base = String(apiBase || "").replace(/\/+$/, "");
    const route = `${base}/api/solana_dex/jupiter/ultra_execute`;
    const headers = { "Content-Type": "application/json" };
    if (tok) headers.Authorization = `Bearer ${tok}`;

    const r = await fetch(route, {
      method: "POST",
      headers,
      body: JSON.stringify({ signedTransaction, requestId }),
    });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `HTTP ${r.status}`);
    }
    return await r.json();
  }


  async function ensureSolanaWalletConnected() {
    try {
      const { provider } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (!provider) return null;

      const existing = solanaProviderPubkeyBase58(provider);
      if (existing) return existing;

      if (typeof provider.connect === "function") {
        await provider.connect();
      }

      return solanaProviderPubkeyBase58(provider);
    } catch {
      return null;
    }
  }

  async function ensurePolkadotWalletConnected() {
    try {
      if (polkadotWalletState?.connected && polkadotWalletState?.address) return polkadotWalletState.address;
      const next = await connectInjectedPolkadotWallet(preferredPolkadotWallet);
      setPreferredPolkadotWallet(next.key || "subwallet-js");
      setPolkadotWalletState({
        key: next.key || "subwallet-js",
        label: next.label || getPolkadotWalletLabel(next.key),
        connected: true,
        address: next.address,
        accountName: next.accountName || "",
        accounts: Array.isArray(next.accounts) ? next.accounts : [],
        extension: next.extension || null,
        error: null,
      });
      return next.address || null;
    } catch (e) {
      const msg = e?.message || "Failed to connect Polkadot wallet.";
      setPolkadotWalletState((prev) => ({
        ...(prev || {}),
        key: preferredPolkadotWallet || "subwallet-js",
        label: getPolkadotWalletLabel(preferredPolkadotWallet),
        connected: false,
        address: null,
        error: msg,
      }));
      throw e;
    }
  }

  async function solanaResolveAsset(asset) {
    const a = String(asset || "").trim();
    if (!a) return null;

    const key = a.toUpperCase();
    const cached = solanaResolveCacheRef.current?.[key];
    if (cached?.mint && cached?.decimals !== null && cached?.decimals !== undefined) return cached;

    if (!apiBase) return null;

    const url = new URL(`${apiBase}/api/solana_dex/resolve`);
    url.searchParams.set("asset", a);
    url.searchParams.set("_ts", String(Date.now()));

    const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `solana resolve HTTP ${r.status}`);
    }
    const j = await r.json();
    const mint = j?.mint ? String(j.mint) : null;
    const decimals = Number.isFinite(Number(j?.decimals)) ? Math.trunc(Number(j.decimals)) : null;

    const out = mint ? { mint, decimals } : null;
    if (out) {
      solanaResolveCacheRef.current = { ...(solanaResolveCacheRef.current || {}), [key]: out };
    }
    return out;
  }

  async function loadAvailBalances(opts = {}) {
    const { silent = false, venueOverride = null, focusAssets = null, force = false } = opts;

    const v = String(venueOverride || effectiveVenue || "").toLowerCase().trim();
    if (!v) return { avail: balAvail, hash: computeBalHash(balAvail), focusHash: "" };

    if (!silent) {
      setBalLoading(true);
      setBalErr(null);
      setBalNotice(null);
    }

    try {
      if (!apiBase) throw new Error("apiBase not set");
      // DEX-only: Solana venues do not have adapter-backed /api/balances/latest.
      if (isSolanaDexVenue) {
        let address = getInjectedSolanaPubkeyBase58();
        if (!address) {
          const addr2 = await ensureSolanaWalletConnected();
          if (!addr2) throw new Error("Connect a supported Solana wallet (Jupiter / Solflare / Phantom / Backpack) to load balances.");
          address = addr2;
        }

        const url = new URL(`${apiBase}/api/solana_dex/balances`);
        url.searchParams.set("address", address);
        url.searchParams.set("_ts", String(Date.now()));

        const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
        if (!r.ok) {
          const txt = await r.text();
          throw new Error(txt || `HTTP ${r.status}`);
        }

        const j = await r.json();
        const nextAvail = {};

        const balanceStale = !!(j?.balanceStale || j?.balanceStatus === "stale_fallback");
        const providerLabel = String(j?.rpcProviderLabel || j?.balanceSource || "Solana RPC");
        const balanceErr = String(j?.balanceError || "").trim();
        if (balanceStale) {
          setBalNotice(
            hideTableData
              ? "Solana balances are stale; verify before sizing a live trade."
              : `Solana balances are stale from ${providerLabel}; verify before sizing a live trade.${balanceErr ? ` Last live error: ${balanceErr}` : ""}`
          );
        } else {
          setBalNotice(null);
        }

        const sol = Number(j?.sol);
        if (Number.isFinite(sol)) {
          nextAvail["SOL"] = { available: sol, total: sol, hold: null };
          nextAvail["WSOL"] = { available: sol, total: sol, hold: null };
        }

        const toks = Array.isArray(j?.tokens) ? j.tokens : [];
        const mintToUi = {};
        const symbolToUi = {};
        const uiAmtFromToken = (t) => {
          const uiRaw = t?.uiAmount ?? t?.ui_amount ?? t?.uiAmountString ?? t?.ui_amount_string ?? t?.uiAmountStr;
          let ui = typeof uiRaw === "number" ? uiRaw : parseFloat(String(uiRaw ?? ""));
          if (Number.isFinite(ui)) return ui;

          const amtRaw = t?.amount ?? t?.rawAmount ?? t?.raw_amount ?? t?.tokenAmount ?? t?.token_amount;
          const decRaw = t?.decimals ?? t?.decimal ?? t?.dec ?? t?.precision;
          const amt = typeof amtRaw === "number" ? amtRaw : parseFloat(String(amtRaw ?? ""));
          const dec = typeof decRaw === "number" ? decRaw : parseInt(String(decRaw ?? ""), 10);
          if (Number.isFinite(amt) && Number.isFinite(dec) && dec >= 0) return amt / Math.pow(10, dec);

          return null;
        };

        for (const t of toks) {
          const mint = String(t?.mint || t?.address || t?.tokenMint || t?.token_mint || "").trim();
          if (!mint) continue;

          const uiAmt = uiAmtFromToken(t);
          mintToUi[mint] = uiAmt;

          const sym = String(t?.symbol || t?.asset || "").trim();
          if (sym) symbolToUi[sym.toUpperCase()] = uiAmt;
        }

        // Resolve only what we need for this ticket (base/quote + common aliases).
        const want = new Set(
          [baseAsset, quoteAsset, "USD", "USDC", "USDT", "PYUSD"]
            .map((x) => String(x || "").trim().toUpperCase())
            .filter(Boolean)
        );

        for (const a of want) {
          if (a === "SOL" || a === "WSOL") continue;
          const symUi = symbolToUi[String(a).toUpperCase()];
          if (Number.isFinite(symUi)) {
            nextAvail[a] = { available: symUi, total: symUi, hold: null };
            continue;
          }
          try {
            const res = await solanaResolveAsset(a);
            const mint = res?.mint;
            if (!mint) continue;
            const ui = mintToUi[mint];
            if (!Number.isFinite(ui)) continue;
            nextAvail[a] = { available: ui, total: ui, hold: null };

            // Convenience: treat USD as USDC for Solana venues (keep both keys filled if present).
            if (a === "USDC" && !nextAvail["USD"]) nextAvail["USD"] = { available: ui, total: ui, hold: null };
            if (a === "USD" && !nextAvail["USDC"]) nextAvail["USDC"] = { available: ui, total: ui, hold: null };
          } catch {
            // ignore resolve failures; balances will simply be missing for that asset
          }
        }

        const nextHash = computeBalHash(nextAvail);
        const nextFocusHash = focusAssets ? computeFocusHash(nextAvail, focusAssets) : "";

        setBalAvail(nextAvail);
        return { avail: nextAvail, hash: nextHash, focusHash: nextFocusHash };
      }

      if (isPolkadotDexVenue) {
        let address = polkadotWalletState?.address || null;
        if (!address) address = await ensurePolkadotWalletConnected();
        if (!address) throw new Error("Connect SubWallet to load Polkadot balances.");

        const j = await fetchPolkadotDexBalancesCached({
          apiBase,
          venue: v,
          address,
          force: !!force,
        });
        const rawItems = Array.isArray(j?.items) ? j.items : Array.isArray(j?.balances) ? j.balances : Array.isArray(j?.tokens) ? j.tokens : [];
        const items = rawItems.map((b) => ({
          ...b,
          asset: b?.asset || b?.symbol || b?.token || b?.ticker,
        }));
        const nextAvail = normalizeBalItems(items, v);

        const native = j?.native && typeof j.native === "object" ? j.native : null;
        const nativeSym = String(native?.symbol || "HDX").trim().toUpperCase();
        const nativeFreeRaw = toFiniteOrNull(native?.free ?? native?.free_ui ?? native?.freeUi);
        const nativeTotal = toFiniteOrNull(native?.total ?? native?.total_ui ?? native?.totalUi);
        const nativeFrozen = toFiniteOrNull(native?.frozen ?? native?.hold ?? native?.locked);
        const nativeBackendAvailable = toFiniteOrNull(
          native?.available ??
          native?.transferable ??
          native?.spendable ??
          native?.available_ui ??
          native?.availableUi ??
          native?.transferable_ui ??
          native?.transferableUi ??
          native?.spendable_ui ??
          native?.spendableUi
        );
        const nativeComputedAvailable =
          nativeFreeRaw !== null && nativeFrozen !== null
            ? Math.max(nativeFreeRaw - nativeFrozen, 0)
            : nativeFreeRaw;
        const nativeAvailable = nativeBackendAvailable !== null ? nativeBackendAvailable : nativeComputedAvailable;
        if (nativeSym && nativeAvailable !== null) {
          nextAvail[nativeSym] = {
            available: nativeAvailable,
            total: nativeTotal !== null ? nativeTotal : nativeFreeRaw !== null ? nativeFreeRaw : nativeAvailable,
            hold: nativeFrozen,
          };
        }

        const nextHash = computeBalHash(nextAvail);
        const nextFocusHash = focusAssets ? computeFocusHash(nextAvail, focusAssets) : "";

        setBalAvail(nextAvail);
        return { avail: nextAvail, hash: nextHash, focusHash: nextFocusHash };
      }

      const url = new URL(`${apiBase}/api/balances/latest`);
      url.searchParams.set("venue", v);
      url.searchParams.set("sort", "asset:asc");
      url.searchParams.set("_ts", String(Date.now()));

      const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
      if (!r.ok) {
        const txt = await r.text();
        throw new Error(txt || `HTTP ${r.status}`);
      }

      const j = await r.json();
      const items = Array.isArray(j?.items) ? j.items : [];
      const nextAvail = normalizeBalItems(items, v);

      const nextHash = computeBalHash(nextAvail);
      const nextFocusHash = focusAssets ? computeFocusHash(nextAvail, focusAssets) : "";

      setBalAvail(nextAvail);
      return { avail: nextAvail, hash: nextHash, focusHash: nextFocusHash };
    } catch (e) {
      setBalAvail({});
      setBalErr(e?.message || "Failed loading balances");
      return { avail: {}, hash: "", focusHash: "" };
    } finally {
      if (!silent) setBalLoading(false);
    }
  }

  // UPDATED: refresh can be tuned (force, polling window, focusAssets)
  async function refreshAvailBalances(opts = {}) {
    const {
      venueOverride = null,
      force = false,
      focusAssets = null,

      // new defaults: "a few tries is enough"
      maxPolls = 5, // hard cap on GETs after refresh
      initialDelayMs = 900, // let venue settle before first GET
      pollBackoffMs = [600, 900, 1300, 1800, 2200], // per-attempt delays
    } = opts;

    const v = String(venueOverride || effectiveVenue || "").toLowerCase().trim();
    if (!v) return false;

    // DEX-only: Solana/Polkadot venues don't have a CEX adapter refresh path; just re-load wallet balances.
    if (isDexSwapVenue) {
      const beforeFullHash = computeBalHash(balAvail);
      const beforeFocusHash = focusAssets ? computeFocusHash(balAvail, focusAssets) : "";

      setBalLoading(true);
      setBalErr(null);
      try {
        const { hash: afterFullHash, focusHash: afterFocusHash } = await loadAvailBalances({
          silent: true,
          venueOverride: v,
          focusAssets,
          force: !!force,
        });

        if (focusAssets) return !!afterFocusHash && afterFocusHash !== beforeFocusHash;
        return !!afterFullHash && afterFullHash !== beforeFullHash;
      } catch (e) {
        setBalErr(e?.message || (isPolkadotDexVenue ? "Failed loading Polkadot balances" : "Failed loading Solana balances"));
        return false;
      } finally {
        setBalLoading(false);
      }
    }

    setBalLoading(true);
    setBalErr(null);

    // compute BEFORE snapshots from current state once
    const beforeFullHash = computeBalHash(balAvail);
    const beforeFocusHash = focusAssets ? computeFocusHash(balAvail, focusAssets) : "";

    let changed = false;

    try {
      if (!apiBase) throw new Error("apiBase not set");

      const postUrl = `${apiBase}/api/balances/refresh`;

      let rr = await fetch(postUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ venue: v, force: !!force }),
      });

      // compatibility fallback for older schema
      if (rr.status === 422) {
        rr = await fetch(postUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ input: { venue: v, force: !!force } }),
        });
      }

      // fallback if refresh endpoint was GET-only in older versions
      if (rr.status === 405 || rr.status === 404) {
        const getUrl = new URL(`${apiBase}/api/balances/refresh`);
        getUrl.searchParams.set("venue", v);
        getUrl.searchParams.set("force", force ? "true" : "false");
        getUrl.searchParams.set("_ts", String(Date.now()));
        rr = await fetch(getUrl.toString(), { method: "GET", cache: "no-store" });
      }

      if (!rr.ok) {
        const txt = await rr.text();
        throw new Error(txt || `balances refresh HTTP ${rr.status}`);
      }

      // settle delay
      await new Promise((r) => setTimeout(r, Math.max(0, Number(initialDelayMs) || 0)));

      // poll a few times at most
      const polls = Math.max(1, Math.min(10, Math.floor(Number(maxPolls) || 5)));
      for (let i = 0; i < polls; i++) {
        const { hash: afterFullHash, focusHash: afterFocusHash } = await loadAvailBalances({
          silent: true,
          venueOverride: v,
          focusAssets,
        });

        if (focusAssets) {
          if (afterFocusHash && afterFocusHash !== beforeFocusHash) {
            changed = true;
            break;
          }
        } else {
          if (afterFullHash && afterFullHash !== beforeFullHash) {
            changed = true;
            break;
          }
        }

        const delay =
          Array.isArray(pollBackoffMs) && pollBackoffMs[i] != null
            ? Math.max(150, Math.floor(Number(pollBackoffMs[i]) || 0))
            : 900;

        await new Promise((r) => setTimeout(r, delay));
      }
    } catch (e) {
      setBalErr(e?.message || "Failed refreshing balances");
    } finally {
      setBalLoading(false);
    }

    return changed;
  }

  useEffect(() => {
    if (isDexSwapVenue) {
      setBalAvail({});
      setBalErr(null);
    }
    loadAvailBalances();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [effectiveVenue, apiBase, baseAsset, quoteAsset, isSolanaDexVenue, isPolkadotDexVenue, isDexSwapVenue, walletKitConnected, walletKitSelectedKey, solanaWalletState?.address, polkadotWalletState?.address]);

  const baseBal = useMemo(() => (baseAsset ? (balAvail?.[baseAsset] ?? null) : null), [balAvail, baseAsset]);
  const quoteBal = useMemo(() => (quoteAsset ? (balAvail?.[quoteAsset] ?? null) : null), [balAvail, quoteAsset]);

  const baseAvail = useMemo(() => toFiniteOrNull(baseBal?.available), [baseBal]);
  const quoteAvail = useMemo(() => toFiniteOrNull(quoteBal?.available), [quoteBal]);

  const relevantAvailLabel = useMemo(() => {
    if (side === "sell") return baseAsset ? `${baseAsset} available` : "Base available";
    return quoteAsset ? `${quoteAsset} available` : "Quote available";
  }, [side, baseAsset, quoteAsset]);

  const relevantAvailValue = useMemo(() => (side === "sell" ? baseAvail : quoteAvail), [side, baseAvail, quoteAvail]);

  // ─────────────────────────────────────────────────────────────
  // Numbers + derived calcs
  // ─────────────────────────────────────────────────────────────
  const qtyNum = useMemo(() => {
    const x = Number(qty);
    return Number.isFinite(x) && x > 0 ? x : null;
  }, [qty]);

  const pxNum = useMemo(() => {
    // IMPORTANT: limitPrice is treated as a string in UI, but we convert here for math.
    const x = Number(expandExponential(limitPrice));
    return Number.isFinite(x) && x > 0 ? x : null;
  }, [limitPrice]);

  const totalQuoteNum = useMemo(() => {
    const x = Number(totalQuote);
    return Number.isFinite(x) && x > 0 ? x : null;
  }, [totalQuote]);

  function fmtPlain(n, { maxFrac }) {
    if (n === null || n === undefined) return "";
    const x = Number(n);
    if (!Number.isFinite(x)) return "";
    return x.toLocaleString(undefined, { useGrouping: false, maximumFractionDigits: maxFrac });
  }

  const notional = useMemo(() => (qtyNum === null || pxNum === null ? null : qtyNum * pxNum), [qtyNum, pxNum]);

  const qtyFromTotal = useMemo(() => {
    if (pxNum === null || totalQuoteNum === null) return null;

    const raw = totalQuoteNum / pxNum;
    if (!Number.isFinite(raw) || raw <= 0) return null;

    const bi = rules?.base_increment ?? null;
    const biStr = bi === null || bi === undefined ? null : String(bi).trim();
    const decHint = rules?.qty_decimals;

    if (biStr && Number(biStr) > 0) return floorToStepNumber(raw, biStr, decHint);

    const factor = 10 ** 8;
    const floored = Math.floor(raw * factor) / factor;
    return Number.isFinite(floored) && floored > 0 ? floored : null;
  }, [pxNum, totalQuoteNum, rules]);

  const totalFromQty = useMemo(() => {
    if (qtyNum === null || pxNum === null) return null;
    const raw = qtyNum * pxNum;
    return Number.isFinite(raw) && raw > 0 ? raw : null;
  }, [qtyNum, pxNum]);

  const jupiterFrontendInputUsdValue = useMemo(() => {
    if (!isSolanaLimitMode) return null;
    const q = String(quoteAsset || "").toUpperCase().trim();
    const stableQuote = q === "USD" || q === "USDC" || q === "USDT" || q === "PYUSD";
    if (!stableQuote) return null;

    if (side === "buy") {
      return totalQuoteNum !== null && totalQuoteNum > 0 ? totalQuoteNum : null;
    }
    return notional !== null && notional > 0 ? notional : null;
  }, [isSolanaLimitMode, quoteAsset, side, totalQuoteNum, notional]);

  const jupiterMinFrontendEnforceable = useMemo(() => {
    return isSolanaLimitMode && jupiterFrontendInputUsdValue !== null;
  }, [isSolanaLimitMode, jupiterFrontendInputUsdValue]);
  const solanaExpiredAt = useMemo(() => {
    if (!isSolanaLimitMode) return undefined;

    const nowSec = Math.floor(Date.now() / 1000);
    const preset = String(solanaExpiryPreset || "never").toLowerCase().trim();

    if (preset === "never") return undefined;
    if (preset === "10m") return nowSec + 10 * 60;
    if (preset === "1h") return nowSec + 60 * 60;
    if (preset === "1d") return nowSec + 24 * 60 * 60;
    if (preset === "7d") return nowSec + 7 * 24 * 60 * 60;
    if (preset === "custom") {
      const raw = String(solanaExpiryCustom || "").trim();
      if (!raw) return null;
      const ms = Date.parse(raw);
      if (!Number.isFinite(ms)) return null;
      const sec = Math.floor(ms / 1000);
      if (sec <= nowSec) return null;
      return sec;
    }
    return undefined;
  }, [isSolanaLimitMode, solanaExpiryPreset, solanaExpiryCustom]);

  const solanaExpiryLabel = useMemo(() => {
    const preset = String(solanaExpiryPreset || "never").toLowerCase().trim();
    if (!isSolanaLimitMode) return "—";
    if (preset === "never") return "Never";
    if (preset === "10m") return "10m";
    if (preset === "1h") return "1h";
    if (preset === "1d") return "1d";
    if (preset === "7d") return "7d";
    if (preset === "custom") return solanaExpiryCustom ? String(solanaExpiryCustom) : "Custom";
    return "Never";
  }, [isSolanaLimitMode, solanaExpiryPreset, solanaExpiryCustom]);

  useEffect(() => {
    if (!autoCalc) return;

    if (lastEditedRef.current !== "qty" && lastEditedRef.current !== "total") lastEditedRef.current = "total";
    const mode = lastEditedRef.current;

    if (mode === "total") {
      if (qtyFromTotal === null) return;
      const maxFrac = Number.isFinite(Number(rules?.qty_decimals))
        ? Math.min(Math.max(Math.trunc(Number(rules.qty_decimals)), 0), 18)
        : 18;
      const nextQty = fmtPlain(qtyFromTotal, { maxFrac });
      if (!nextQty) return;

      if (String(nextQty) !== String(qty ?? "")) {
        if (autoCalcWriteGuardRef.current.qty !== nextQty) {
          autoCalcWriteGuardRef.current.qty = nextQty;
          setQty(nextQty);
        }
      }
      return;
    }

    if (mode === "qty") {
      if (totalFromQty === null) return;

      const nextTotal = fmtPlain(totalFromQty, { maxFrac: totalQuoteDecimals });
      if (!nextTotal) return;

      if (String(nextTotal) !== String(totalQuote ?? "")) {
        if (autoCalcWriteGuardRef.current.total !== nextTotal) {
          autoCalcWriteGuardRef.current.total = nextTotal;
          setTotalQuote(nextTotal);
        }
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoCalc, qtyFromTotal, totalFromQty, totalQuoteDecimals, pxNum, rules]);

  useEffect(() => {
    autoCalcWriteGuardRef.current.qty = null;
  }, [qty]);

  useEffect(() => {
    autoCalcWriteGuardRef.current.total = null;
  }, [totalQuote]);

  const hydrationManualRouterPriceGuard = useMemo(() => {
    if (!isPolkadotDexVenue || !polkadotManualRouterFallbackAvailable) return null;
    const bestAsk = Number(polkadotOrderbookSideGuard?.bestAsk);
    const bestBid = Number(polkadotOrderbookSideGuard?.bestBid);
    const hasAsk = Number.isFinite(bestAsk) && bestAsk > 0;
    const hasBid = Number.isFinite(bestBid) && bestBid > 0;
    if (!hasAsk && !hasBid) return null;

    const impliedPx =
      pxNum !== null
        ? pxNum
        : (qtyNum !== null && totalQuoteNum !== null && qtyNum > 0)
          ? totalQuoteNum / qtyNum
          : null;
    const recommendedPrice = side === "buy" ? (hasAsk ? bestAsk : null) : (hasBid ? bestBid : null);
    const recommendedLabel = side === "buy" ? "best ask / lowest sell" : "best bid / highest buy";
    const toleranceRatio = HYDRATION_ROUTER_BOOK_SIDE_TOLERANCE_BPS / 10_000;
    const buyMinAllowed = recommendedPrice !== null ? recommendedPrice * (1 - toleranceRatio) : null;
    const sellMaxAllowed = recommendedPrice !== null ? recommendedPrice * (1 + toleranceRatio) : null;
    const mismatch =
      impliedPx !== null &&
      recommendedPrice !== null &&
      (
        side === "buy"
          ? impliedPx + 1e-18 < buyMinAllowed
          : impliedPx - 1e-18 > sellMaxAllowed
      );

    return {
      show: true,
      side,
      bestAsk: hasAsk ? bestAsk : null,
      bestBid: hasBid ? bestBid : null,
      impliedPrice: impliedPx,
      recommendedPrice,
      recommendedLabel,
      toleranceBps: HYDRATION_ROUTER_BOOK_SIDE_TOLERANCE_BPS,
      mismatch,
      symbol: polkadotOrderbookSideGuard?.symbol || String(otSymbol || "").trim().toUpperCase(),
    };
  }, [isPolkadotDexVenue, polkadotManualRouterFallbackAvailable, polkadotOrderbookSideGuard, side, pxNum, qtyNum, totalQuoteNum, otSymbol]);

  function applyHydrationBookSideLimit() {
    const px = hydrationManualRouterPriceGuard?.recommendedPrice;
    if (!Number.isFinite(Number(px)) || Number(px) <= 0) return;
    const next = fmtPlain(Number(px), { maxFrac: 18 });
    if (!next) return;
    limitSourceRef.current = "hydration_book_side_guard";
    setLimitPrice(next);
  }

  // ─────────────────────────────────────────────────────────────
  // Pre-trade checks
  // ─────────────────────────────────────────────────────────────
  const preTrade = useMemo(() => {
    const lines = [];
    const fails = [];

    if (rulesLoading) return { status: "neutral", title: "Pre-trade checks: loading…", lines: [], block: false };


    if (isSolanaLimitMode) {
      if (qtyNum === null) {
        lines.push("Qty missing/invalid.");
        fails.push("qty_missing");
      }
      if (pxNum === null) {
        lines.push("Limit price missing/invalid.");
        fails.push("px_missing");
      }

      if (jupiterMinFrontendEnforceable) {
        if (jupiterFrontendInputUsdValue + 1e-12 < JUPITER_LIMIT_MIN_USD) {
          lines.push(
            hideTableData
              ? "Jupiter limit minimum not met."
              : `Jupiter limit minimum: need current input-token value ≥ $${JUPITER_LIMIT_MIN_USD.toFixed(2)}.`
          );
          fails.push("jupiter_min_usd");
        }
      } else {
        lines.push(
          hideTableData
            ? "Jupiter limit minimum will be checked on submit."
            : `Jupiter limit minimum (${JUPITER_LIMIT_MIN_USD.toFixed(2)} USD current input value) will be enforced by backend on submit.`
        );
      }

      if (String(solanaExpiryPreset || "never").toLowerCase().trim() === "custom") {
        if (solanaExpiredAt === null) {
          lines.push(
            hideTableData
              ? "Custom expiry invalid."
              : "Custom expiry must be a valid future date/time."
          );
          fails.push("solana_expiry_invalid");
        }
      }

      if (fails.length === 0) return { status: "ok", title: "Pre-trade checks: OK", lines, block: false };
      return { status: "fail", title: "Pre-trade checks: FAIL (blocked)", lines, block: true };
    }

    if (isPolkadotDexVenue) {
      if (side === "buy") {
        if (qtyNum === null) {
          lines.push(`Qty ${baseAsset || "base"} missing/invalid. BUY uses Qty as the exact base amount to receive.`);
          fails.push("qty_missing");
        }
      } else if (qtyNum === null) {
        lines.push("Qty missing/invalid.");
        fails.push("qty_missing");
      }

      if (polkadotHydrationStatusLoading) {
        lines.push("Hydration status loading…");
        fails.push("hydration_status_loading");
      } else if (polkadotHydrationStatusError) {
        lines.push(hideTableData ? "Hydration status unavailable." : `Hydration status unavailable: ${polkadotHydrationStatusError}`);
        fails.push("hydration_status_error");
      } else if (!polkadotHydrationStatus) {
        lines.push("Hydration status unavailable.");
        fails.push("hydration_status_missing");
      } else if (polkadotSyntheticPriceOnly) {
        lines.push(hideTableData ? "Synthetic Hydration price only; swap route unavailable." : polkadotSyntheticPriceOnlyReason);
        fails.push("hydration_synthetic_price_only");
      } else if (!polkadotEffectiveQuotesAvailable) {
        lines.push(hideTableData ? "Live Hydration quotes/swaps are disabled." : polkadotEffectiveStatusReason);
        fails.push("hydration_quotes_disabled");
      } else if (!polkadotEffectiveLiveSwapsRecommended) {
        lines.push(hideTableData ? "Live Hydration swaps are disabled." : polkadotEffectiveStatusReason);
        fails.push("hydration_swaps_disabled");
      } else if (polkadotManualRouterFallbackAvailable && !polkadotQuotesAvailable) {
        lines.push(`Controlled manual Router fallback available for ${side === "buy" ? "BUY exact-out" : "SELL exact-in"} while generic SDK router quotes remain disabled.`);
      } else if (polkadotManualRouteAvailable && !polkadotQuotesAvailable) {
        lines.push("Manual XYK/live-pool route available for this pair while generic SDK router quotes remain disabled.");
      }

      if (
        side === "buy" &&
        !polkadotEffectiveExactBuyEnabled &&
        !polkadotSyntheticPriceOnly &&
        polkadotEffectiveQuotesAvailable &&
        polkadotEffectiveLiveSwapsRecommended
      ) {
        lines.push("Hydration BUY swaps are disabled until UTT_HYDRATION_ENABLE_EXACT_BUY=1. SELL swaps remain available.");
        fails.push("hydration_buy_swaps_disabled");
      }

      if (hydrationManualRouterPriceGuard?.mismatch) {
        const rec = hydrationManualRouterPriceGuard.recommendedPrice;
        const recStr = Number.isFinite(Number(rec)) ? fmtPlain(Number(rec), { maxFrac: 18 }) : "current book price";
        if (side === "buy") {
          lines.push(
            hideTableData
              ? "BUY limit is below best ask."
              : `BUY exact-out should use the best ask / lowest sell price. Current limit is meaningfully below ${recStr}; this can fail on-chain with Router.TradingLimitReached.`
          );
          fails.push("hydration_buy_below_best_ask");
        } else {
          lines.push(
            hideTableData
              ? "SELL limit is above best bid."
              : `SELL exact-in should use the best bid / highest buy price. Current limit is meaningfully above ${recStr}; this can fail on-chain with Router.TradingLimitReached.`
          );
          fails.push("hydration_sell_above_best_bid");
        }
      }

      if (polkadotLiquidityWarning) {
        lines.push(hideTableData ? "Low-liquidity isolated pool warning." : `${polkadotLiquidityWarning.label}. ${polkadotLiquidityWarning.message}`);
      }

      if (fails.length === 0) {
        if (polkadotLiquidityWarning) {
          return { status: "warn", title: "Pre-trade checks: WARNING · Low-liquidity isolated pool", lines, block: false };
        }
        return { status: "ok", title: "Pre-trade checks: OK", lines, block: false };
      }
      return { status: "fail", title: "Pre-trade checks: Hydration swap blocked", lines, block: true, message: lines.join(" ") };
    }

    if (rulesErr || !rules) {
      return {
        status: "neutral",
        title: hideTableData ? "Pre-trade checks: unavailable." : "Pre-trade checks: unavailable.",
        lines: hideTableData ? [] : rulesErr ? [String(rulesErr)] : [],
        block: false,
      };
    }

    const errs = Array.isArray(rules?.errors) ? rules.errors : [];
    if (errs.length > 0) {
      return {
        status: "neutral",
        title: "Pre-trade checks: unavailable.",
        lines: hideTableData ? [] : errs.map((x) => String(x)),
        block: false,
      };
    }

    const minQty = uiMinQty ?? rules?.min_qty;
    const minNotional = rules?.min_notional;
    const baseInc = rules?.base_increment;
    const priceInc = rules?.price_increment;

    const qtyDec = rules?.qty_decimals;
    const pxDec = rules?.price_decimals;

    const limitStrExpanded = expandExponential(limitPrice);

    if (qtyNum === null) {
      lines.push("Qty missing/invalid.");
      fails.push("qty_missing");
    }
    if (pxNum === null) {
      lines.push("Limit price missing/invalid.");
      fails.push("px_missing");
    }

    if (qtyNum !== null && minQty !== null && minQty !== undefined && Number.isFinite(Number(minQty))) {
      const mq = Number(minQty);
      if (qtyNum < mq) {
        lines.push(hideTableData ? "Qty below min." : `Qty min: need ≥ ${mq}.`);
        fails.push("qty_min");
      }
    }

    if (notional !== null && minNotional !== null && minNotional !== undefined && Number.isFinite(Number(minNotional))) {
      const mn = Number(minNotional);
      if (notional < mn) {
        lines.push(hideTableData ? "Notional below min." : `Notional min: need ≥ ${mn}.`);
        fails.push("notional_min");
      }
    }

    if (qtyNum !== null) {
      const dCount = countDecimalsFromString(qty);
      if (dCount !== null && Number.isFinite(Number(qtyDec)) && Number(qtyDec) >= 0) {
        const allowed = Math.min(Math.max(Math.trunc(Number(qtyDec)), 0), 18);
        if (dCount > allowed) {
          lines.push(hideTableData ? "Qty precision too high." : `Qty precision: ${dCount} decimals → allowed ${allowed}.`);
          fails.push("qty_precision");
        }
      }
    }

    if (pxNum !== null) {
      const dCount = countDecimalsFromString(limitStrExpanded);
      if (dCount !== null && Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0) {
        const allowed = Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18);
        if (dCount > allowed) {
          lines.push(hideTableData ? "Price precision too high." : `Price precision: ${dCount} decimals → allowed ${allowed}.`);
          fails.push("px_precision");
        }
      }
    }

    if (qtyNum !== null && baseInc !== null && baseInc !== undefined && String(baseInc).trim()) {
      const biStr = String(baseInc).trim();
      const ok = isMultipleOfStep(String(qty), biStr, qtyDec);
      if (ok === false) {
        lines.push(
          hideTableData
            ? "Qty step invalid."
            : `Qty step: qty must be a multiple of ${fmtStepValue(biStr, qtyDec) ?? biStr}.`
        );
        fails.push("qty_step");
      }
    }

    if (pxNum !== null && priceInc !== null && priceInc !== undefined && String(priceInc).trim()) {
      const piStr = String(priceInc).trim();
      const ok = isMultipleOfStep(String(limitStrExpanded), piStr, pxDec);
      if (ok === false) {
        lines.push(
          hideTableData
            ? "Price tick invalid."
            : `Price tick: price must be a multiple of ${fmtStepValue(piStr, pxDec) ?? piStr}.`
        );
        fails.push("px_tick");
      }
    }

    if (fails.length === 0) return { status: "ok", title: "Pre-trade checks: OK", lines: [], block: false };
    return { status: "fail", title: "Pre-trade checks: FAIL (blocked)", lines, block: true };
  }, [
    rulesLoading,
    rulesErr,
    rules,
    uiMinQty,
    qty,
    limitPrice,
    qtyNum,
    pxNum,
    notional,
    hideTableData,
    isSolanaLimitMode,
    isPolkadotDexVenue,
    totalQuoteNum,
    quoteAsset,
    jupiterFrontendInputUsdValue,
    jupiterMinFrontendEnforceable,
    side,
    solanaExpiryPreset,
    solanaExpiredAt,
    polkadotHydrationStatus,
    polkadotHydrationStatusLoading,
    polkadotHydrationStatusError,
    polkadotQuotesAvailable,
    polkadotLiveSwapsRecommended,
    polkadotExactBuyEnabled,
    polkadotEffectiveQuotesAvailable,
    polkadotEffectiveLiveSwapsRecommended,
    polkadotEffectiveExactBuyEnabled,
    polkadotManualSwapAvailable,
    polkadotManualRouteAvailable,
    polkadotManualRouterFallbackAvailable,
    polkadotSyntheticPriceOnly,
    polkadotSyntheticPriceOnlyReason,
    polkadotEffectiveStatusReason,
    polkadotStatusReason,
    polkadotLiquidityWarning,
    hydrationManualRouterPriceGuard,
  ]);

  const preTradeStyle = useMemo(() => {
    if (!preTrade) return null;
    if (preTrade.status === "ok") return { border: "1px solid #203a20", background: "#0f1a0f", color: "#cdeccd" };
    if (preTrade.status === "warn") return { border: "1px solid rgba(245, 158, 11, 0.55)", background: "rgba(120, 72, 16, 0.18)", color: "#ffe2a6" };
    if (preTrade.status === "fail") return { border: "1px solid #4a1f1f", background: "#160b0b", color: "#ffd2d2" };
    return { border: "1px solid #2a2a2a", background: "#101010", color: "#cfcfcf" };
  }, [preTrade]);

  const canSubmitBase = useMemo(() => {
    const v = String(effectiveVenue || "").trim();
    const s = String(otSymbol || "").trim();
    if (!v || !s) return false;
    if (!(side === "buy" || side === "sell")) return false;

    // Solana DEX venues are swap-style:
    // - BUY uses Total (quote spend)
    // - SELL uses Qty (base spend)
    // Limit price is not required.
    if (isDexSwapVenue) {
      if (isSolanaLimitMode) return qtyNum !== null && pxNum !== null;
      if (side === "buy") return totalQuoteNum !== null;
      return qtyNum !== null;
    }

    // CEX-style limit order
    return qtyNum !== null && pxNum !== null;
  }, [effectiveVenue, otSymbol, side, isDexSwapVenue, isSolanaLimitMode, qtyNum, pxNum, totalQuoteNum]);

  const canSubmit = useMemo(() => {
    if (!canSubmitBase) return false;
    if (preTrade?.block) return false;
    return true;
  }, [canSubmitBase, preTrade]);

  const buySpendQuote = useMemo(() => {
    if (side !== "buy") return null;

    // Solana DEX BUY spends quote = Total field
    if (isDexSwapVenue) {
      return totalQuoteNum === null ? null : totalQuoteNum;
    }

    // CEX BUY spends quote = qty * limit
    if (qtyNum === null || pxNum === null) return null;
    const spend = qtyNum * pxNum;
    return Number.isFinite(spend) ? spend : null;
  }, [side, isDexSwapVenue, qtyNum, pxNum, totalQuoteNum]);

  const buySpendCapacityQuote = useMemo(() => {
    if (side !== "buy") return null;
    const qAvail = toFiniteOrNull(quoteAvail);
    if (qAvail === null || qAvail < 0) return null;
    return qAvail;
  }, [side, quoteAvail]);

  const sellCapacity = useMemo(() => {
    if (side !== "sell") return null;
    const bAvail = toFiniteOrNull(baseAvail);
    return bAvail === null ? null : bAvail;
  }, [side, baseAvail]);

  const balanceWarning = useMemo(() => {
    if (side === "buy") {
      if (!quoteAsset) return null;
      if (buySpendQuote === null || buySpendCapacityQuote === null) return null;

      if (buySpendQuote > buySpendCapacityQuote + 1e-12) {
        return hideTableData
          ? "Insufficient available balance for this buy."
          : `Insufficient ${quoteAsset} available: need ${buySpendQuote.toLocaleString(undefined, {
              maximumFractionDigits: 12,
            })}, have ${buySpendCapacityQuote.toLocaleString(undefined, { maximumFractionDigits: 12 })}.`;
      }
      return null;
    }

    if (side === "sell") {
      if (!baseAsset) return null;
      if (qtyNum === null || sellCapacity === null) return null;

      if (qtyNum > sellCapacity + 1e-12) {
        return hideTableData
          ? "Insufficient available balance for this sell."
          : `Insufficient ${baseAsset} available: need ${qtyNum.toLocaleString(undefined, {
              maximumFractionDigits: 12,
            })}, have ${sellCapacity.toLocaleString(undefined, { maximumFractionDigits: 12 })}.`;
      }
      return null;
    }
    return null;
  }, [side, hideTableData, quoteAsset, baseAsset, buySpendQuote, buySpendCapacityQuote, qtyNum, sellCapacity]);

  // NEW: helper to open the submission result modal deterministically
  function openSubmitResultModal(kind, payload, title) {
    const t = String(title || (kind === "error" ? "Order Submit Failed" : "Order Submit Result"));
    setSubmitResultKind(kind);
    setSubmitResultPayload(payload);
    setSubmitResultTitle(t);

    if (hideTableData) {
      setSubmitResultText(
        kind === "error"
          ? "Result hidden (Hide table data is enabled). Disable Hide table data to view error details."
          : "Result hidden (Hide table data is enabled). Disable Hide table data to view order details."
      );
    } else {
      try {
        if (typeof payload === "string") setSubmitResultText(payload);
        else setSubmitResultText(JSON.stringify(payload, null, 2));
      } catch {
        setSubmitResultText(String(payload ?? ""));
      }
    }

    setShowSubmitResult(true);
  }

  // Hydration progress modal helper. BUY exact-out can be slower because it
  // intentionally uses an isolated helper process before the SubWallet signing
  // step, so keep the modal alive and update it at each visible milestone.
  function hydrationSubmitProgressText(stage, tradeSide = side) {
    const s = String(stage || "build").trim().toLowerCase();
    const isBuy = String(tradeSide || side || "").trim().toLowerCase() === "buy";
    const steps = [
      { key: "build", label: isBuy ? "Building exact BUY route..." : "Building exact SELL route..." },
      { key: "wallet", label: "Waiting for SubWallet..." },
      { key: "finality", label: "Waiting for finality..." },
      { key: "record", label: "Recording swap..." },
    ];
    const idx = Math.max(0, steps.findIndex((x) => x.key === s));
    const lines = steps.map((x, i) => {
      const prefix = i < idx ? "✓" : i === idx ? "→" : "•";
      return `${prefix} ${x.label}`;
    });
    if (isBuy) {
      lines.push("", "Exact BUY uses an isolated helper first, so this path can take longer than SELL.");
    }
    return lines.join("\n");
  }

  function hydrationSubmitProgressTitle(stage, tradeSide = side) {
    const s = String(stage || "build").trim().toLowerCase();
    const isBuy = String(tradeSide || side || "").trim().toLowerCase() === "buy";
    if (s === "wallet") return "Waiting for SubWallet";
    if (s === "finality") return "Waiting for Hydration Finality";
    if (s === "record") return "Recording Hydration Swap";
    return isBuy ? "Building exact BUY route" : "Building Hydration SELL route";
  }

  function openHydrationSubmitProgress(stage, tradeSide = side) {
    const safeStage = String(stage || "build").trim().toLowerCase() || "build";
    setSubmitResultKind("info");
    setSubmitResultPayload({ ok: true, venue: "polkadot_hydration", stage: safeStage, side: tradeSide });
    setSubmitResultTitle(hydrationSubmitProgressTitle(safeStage, tradeSide));
    setSubmitResultText(hydrationSubmitProgressText(safeStage, tradeSide));
    setShowSubmitResult(true);
  }

  // UPDATED: refresh balances AFTER submit returns OK (longer window + focus base/quote)
  async function refreshBalancesAfterSubmit({ venueKey, focusBase, focusQuote } = {}) {
    try {
      const v = String(venueKey || "").toLowerCase().trim();
      if (!v) return;

      const focus = [focusBase, focusQuote].filter(Boolean);

      // Let backend/venue settle, especially for holds/reserved amounts.
      await new Promise((r) => setTimeout(r, 900));

      const changed = await refreshAvailBalances({
        venueOverride: v,
        force: true, // post-submit should be strict
        focusAssets: focus.length ? focus : null,
        maxPolls: 5,
        initialDelayMs: 0, // already waited above
        pollBackoffMs: [600, 900, 1300, 1800, 2200],
      });

      // If nothing changed yet, schedule one follow-up pass (non-spammy).
      if (!changed) {
        setTimeout(() => {
          refreshAvailBalances({
            venueOverride: v,
            force: false,
            focusAssets: focus.length ? focus : null,
            maxPolls: 3,
            initialDelayMs: 0,
            pollBackoffMs: [900, 1400, 2000],
          });
        }, 4000);
      }
    } catch {
      // Any errors are surfaced by refreshAvailBalances via balErr.
    }
  }

  
  async function submitSolanaSwapOrder() {
    const tok = getAuthToken();

    if (!tok) {
      const msg = "Login required to place orders.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    // Never silently no-op.
    if (!canSubmit) {
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Swap is not currently submittable — check Qty/Total and venue rules.";
      onToast?.({ kind: "warn", msg: reason });
      openSubmitResultModal("error", reason, "Swap Not Submitted");
      return;
    }

    if (!apiBase) {
      const msg = "apiBase not set";
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    const v = String(effectiveVenue || "").toLowerCase().trim();
    const sym = String(otSymbol || "").trim();
    let address = getInjectedSolanaPubkeyBase58();

    if (!address) {
      address = await ensureSolanaWalletConnected();
    }
    if (!address) {
      const msg = "Connect a supported Solana wallet (Jupiter / Solflare / Phantom / Backpack) to submit swaps.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    // Amount in HUMAN units of the INPUT token:
    // - BUY  => QUOTE spend ("Total")
    // - SELL => BASE qty ("Qty")
    const amount = side === "buy" ? Number(totalQuoteNum) : Number(qtyNum);
    if (!Number.isFinite(amount) || amount <= 0) {
      const msg = side === "buy" ? "Enter a valid Total amount." : "Enter a valid Qty amount.";
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    setSubmitting(true);
    setSubmitError(null);
    setSubmitOk(null);

    try {
      const slippageBps = 100;
      let swapResp = null;

      const routerMode = String(preferredSolanaRouterMode || "auto").toLowerCase().trim();
      const preferRaydium = v === "solana_raydium" || routerMode === "raydium";
      const ultraOnly = routerMode === "ultra";
      const metisOnly = routerMode === "metis";

      if (preferRaydium) {
        swapResp = await fetchSolanaSwapTx({
          provider: "raydium",
          symbol: sym,
          side,
          amount,
          address,
          slippageBps,
          tok,
        });
      } else if (ultraOnly) {
        swapResp = await fetchSolanaSwapTx({
          provider: "jupiter_ultra",
          symbol: sym,
          side,
          amount,
          address,
          slippageBps,
          tok,
        });
      } else if (metisOnly) {
        swapResp = await fetchSolanaSwapTx({
          provider: "jupiter_metis",
          symbol: sym,
          side,
          amount,
          address,
          slippageBps,
          tok,
        });
      } else {
        try {
          swapResp = await fetchSolanaSwapTx({
            provider: "jupiter_ultra",
            symbol: sym,
            side,
            amount,
            address,
            slippageBps,
            tok,
          });
        } catch (eUltra) {
          const msgUltra = eUltra?.message || "Failed to build Jupiter Ultra swap";
          if (!isBlockedJupiterTokenError(msgUltra)) throw eUltra;

          try {
            swapResp = await fetchSolanaSwapTx({
              provider: "jupiter_metis",
              symbol: sym,
              side,
              amount,
              address,
              slippageBps,
              tok,
            });
          } catch (eMetis) {
            const msgMetis = eMetis?.message || "Failed to build Jupiter Metis swap";
            if (!isBlockedJupiterTokenError(msgMetis)) throw eMetis;

            onToast?.({
              kind: "warn",
              msg: "Jupiter Ultra/Metis blocked or could not route this token — retrying through Raydium.",
            });

            swapResp = await fetchSolanaSwapTx({
              provider: "raydium",
              symbol: sym,
              side,
              amount,
              address,
              slippageBps,
              tok,
            });
          }
        }
      }

      const { provider, data: j, txB64: b64 } = swapResp || {};
      if (!b64) throw new Error("Missing swap transaction in response");

      // Deserialize VersionedTransaction
      const { VersionedTransaction } = await import("@solana/web3.js");
      const bytes = Uint8Array.from(atob(String(b64)), (c) => c.charCodeAt(0));
      const tx = VersionedTransaction.deserialize(bytes);

      const { provider: providerWallet } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (!providerWallet) throw new Error("No supported Solana wallet provider found (Jupiter / Solflare / Phantom / Backpack).");

      let signature = null;

      if (provider === "jupiter_ultra") {
        if (typeof providerWallet.signTransaction !== "function") {
          throw new Error("Wallet provider missing signTransaction (required for Jupiter Ultra).");
        }
        const signedTx = await providerWallet.signTransaction(tx);
        if (!signedTx || typeof signedTx.serialize !== "function") {
          throw new Error("Wallet did not return a signed transaction for Jupiter Ultra.");
        }
        const signedBytes = signedTx.serialize();
        const signedB64 = btoa(String.fromCharCode(...Array.from(signedBytes)));
        const requestId = j?.requestId || j?.order?.requestId;
        if (!requestId) throw new Error("Missing requestId from Jupiter Ultra order response.");
        const execResp = await executeSolanaUltraSwap({ signedTransaction: signedB64, requestId, tok });
        signature = execResp?.signature || "";
      } else {
        if (typeof providerWallet.signAndSendTransaction === "function") {
          const res = await providerWallet.signAndSendTransaction(tx);
          signature = res?.signature || res?.sig || res;
        } else if (typeof providerWallet.signTransaction === "function") {
          throw new Error("Wallet does not support signAndSendTransaction (required).");
        } else {
          throw new Error("Wallet provider missing signAndSendTransaction.");
        }
      }

      signature = signature ? String(signature) : "";
      if (!signature) throw new Error("Missing signature from wallet response");

      if (provider === "jupiter" || provider === "jupiter_metis") {
        const base = String(apiBase || "").replace(/\/+$/, "");
        const recUrl = `${base}/api/solana_dex/jupiter/record_submit`;

        try {
          const headers = { "Content-Type": "application/json" };
          if (tok) headers.Authorization = `Bearer ${tok}`;

          const recPayload = {
            signature,
            chain: "solana",
            venue: v || "solana_jupiter",
            ts: Math.floor(Date.now() / 1000),
            wallet_address: address,
            raw_symbol: sym,
            resolved_symbol: null,
            side,
            base_qty: side === "sell" ? Number(qtyNum) : null,
            quote_qty: side === "buy" ? Number(totalQuoteNum) : null,
            price: null,
            fee_quote: null,
            status: "submitted",
            raw: { quote: j?.quote ?? null, last_valid_block_height: j?.last_valid_block_height ?? null },
          };
          await fetch(recUrl, { method: "POST", headers, body: JSON.stringify(recPayload) });
        } catch {
          // ignore
        }
      }

      const okPayload = { ok: true, provider: provider || "jupiter_metis", signature };
      setSubmitOk(okPayload);
      openSubmitResultModal("ok", okPayload, `${String(provider || "jupiter_metis").replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase())} Swap Submitted`);

      refreshBalancesAfterSubmit({ venueKey: provider === "raydium" ? "solana_raydium" : v, focusBase: baseAsset, focusQuote: quoteAsset });
    } catch (e) {
      const msg = e?.message || "Failed to submit swap";
      setSubmitError(msg);
      openSubmitResultModal("error", msg, "Swap Submit Failed");
    } finally {
      setSubmitting(false);
    }
  }


async function submitSolanaTriggerLimitOrder() {
    const tok = getAuthToken();

    if (!tok) {
      const msg = "Login required to place orders.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
      return;
    }

    if (!canSubmit) {
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Jupiter limit order is not currently submittable — check Qty/Price and minimum rules.";
      onToast?.({ kind: "warn", msg: reason });
      openSubmitResultModal("error", reason, "Jupiter Limit Not Submitted");
      return;
    }

    if (!apiBase) {
      const msg = "apiBase not set";
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
      return;
    }

    let address = getInjectedSolanaPubkeyBase58();
    if (!address) address = await ensureSolanaWalletConnected();
    if (!address) {
      const msg = "Connect a supported Solana wallet (Jupiter / Solflare / Phantom / Backpack) to submit Jupiter limit orders.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
      return;
    }

    const quantity = Number(qtyNum);
    const limit_price = Number(pxNum);
    if (!Number.isFinite(quantity) || quantity <= 0) {
      openSubmitResultModal("error", "Enter a valid Qty amount.", "Jupiter Limit Submit Failed");
      return;
    }
    if (!Number.isFinite(limit_price) || limit_price <= 0) {
      openSubmitResultModal("error", "Enter a valid Limit price.", "Jupiter Limit Submit Failed");
      return;
    }

    setSubmitting(true);
    setSubmitError(null);
    setSubmitOk(null);

    try {
      const base = String(apiBase || "").replace(/\/+$/, "");
      const url = `${base}/api/solana_dex/jupiter/trigger/create_order`;
      const headers = { "Content-Type": "application/json" };
      if (tok) headers.Authorization = `Bearer ${tok}`;

      const expired_at = solanaExpiredAt === undefined ? undefined : String(solanaExpiredAt);

      const payload = {
        symbol: String(otSymbol || "").trim(),
        side,
        quantity,
        limit_price,
        user_pubkey: address,
        payer: address,
        expired_at,
        slippage_bps: 0,
        wrap_and_unwrap_sol: true,
      };

      const r = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });

      if (!r.ok) {
        const txt = await r.text();
        throw new Error(txt || `HTTP ${r.status}`);
      }

      const j = await r.json();
      const txB64 = j?.transaction;
      if (!txB64) throw new Error("Missing transaction in Jupiter Trigger response");

      const { VersionedTransaction } = await import("@solana/web3.js");
      const bytes = Uint8Array.from(atob(String(txB64)), (c) => c.charCodeAt(0));
      const tx = VersionedTransaction.deserialize(bytes);

      const w = typeof window !== "undefined" ? window : null;
      const { provider, key: providerKey } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (!provider) throw new Error("No supported Solana wallet provider found (Jupiter / Solflare / Phantom / Backpack).");

      let signature = null;
      if (typeof provider.signAndSendTransaction === "function") {
        const res = await provider.signAndSendTransaction(tx);
        signature = res?.signature || res?.sig || res;
      } else {
        throw new Error("Wallet provider missing signAndSendTransaction.");
      }

      signature = signature ? String(signature) : "";

      try {
        await fetch(`${base}/api/solana_dex/jupiter/trigger/register_open_order`, {
          method: "POST",
          headers,
          body: JSON.stringify({
            symbol: String(otSymbol || "").trim(),
            side,
            quantity,
            limit_price,
            user_pubkey: address,
            signature: signature || "",
            request_id: j?.requestId ?? null,
            order: j?.order ?? "",
            expired_at,
          }),
        });
      } catch {}

      const okPayload = {
        ok: true,
        mode: "limit",
        signature: signature || null,
        requestId: j?.requestId ?? null,
        order: j?.order ?? null,
      };

      setSubmitOk(okPayload);
      openSubmitResultModal("ok", okPayload, "Jupiter Limit Submitted");
      refreshBalancesAfterSubmit({ venueKey: "solana_jupiter", focusBase: baseAsset, focusQuote: quoteAsset });
    } catch (e) {
      const msg = e?.message || "Failed to submit Jupiter limit order";
      setSubmitError(msg);
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
    } finally {
      setSubmitting(false);
    }
  }

async function submitPolkadotSwapOrder() {
  if (!apiBase) {
    const msg = "apiBase not set";
    openSubmitResultModal("error", msg, "Hydration Swap Submit Failed");
    return;
  }

  if (polkadotSyntheticPriceOnly) {
    const reason = polkadotSyntheticPriceOnlyReason || "Synthetic Hydration price only; swap route unavailable.";
    onToast?.({ kind: "warn", msg: reason });
    openSubmitResultModal("error", {
      ok: false,
      venue: String(effectiveVenue || "polkadot_hydration").toLowerCase().trim(),
      status_endpoint: "/api/polkadot_dex/hydration/orderbook",
      next_endpoint: "/api/polkadot_dex/hydration/swap_tx",
      quoteStatus: polkadotHydrationStatus?.quoteStatus || null,
      quoteStatusDetail: polkadotStatusDetail,
      swapTxBuildAvailable: false,
      manualRouteAvailable: !!polkadotManualSwapAvailable,
      manualRouterFallbackAvailable: !!polkadotManualRouterFallbackAvailable,
      syntheticPriceOnly: true,
      hydrationRouteProbe: polkadotHydrationRouteProbe || null,
      message: reason,
    }, "Synthetic Price Only");
    return;
  }

  if (!polkadotEffectiveQuotesAvailable || !polkadotEffectiveLiveSwapsRecommended) {
    const reason = polkadotEffectiveStatusReason || "Hydration swap transaction building is disabled.";
    onToast?.({ kind: "warn", msg: reason });
    openSubmitResultModal("error", {
      ok: false,
      venue: String(effectiveVenue || "polkadot_hydration").toLowerCase().trim(),
      status_endpoint: "/api/polkadot_dex/hydration/status",
      next_endpoint: "/api/polkadot_dex/hydration/swap_tx",
      quoteStatus: polkadotHydrationStatus?.quoteStatus || null,
      quoteStatusDetail: polkadotStatusDetail,
      swapTxBuildAvailable: !!polkadotEffectiveLiveSwapsRecommended,
      manualRouteAvailable: !!polkadotManualSwapAvailable,
      manualRouterFallbackAvailable: !!polkadotManualRouterFallbackAvailable,
      syntheticPriceOnly: !!polkadotSyntheticPriceOnly,
      hydrationRouteProbe: polkadotHydrationRouteProbe || null,
      message: reason,
    }, "Hydration Swap Submit Disabled");
    return;
  }

  if (side === "buy" && !polkadotEffectiveExactBuyEnabled) {
    const reason = "Hydration BUY swaps are disabled until UTT_HYDRATION_ENABLE_EXACT_BUY=1. SELL swaps remain available.";
    onToast?.({ kind: "warn", msg: reason });
    openSubmitResultModal("error", {
      ok: false,
      venue: String(effectiveVenue || "polkadot_hydration").toLowerCase().trim(),
      status_endpoint: "/api/polkadot_dex/hydration/status",
      next_endpoint: "/api/polkadot_dex/hydration/swap_tx",
      quoteStatus: polkadotHydrationStatus?.quoteStatus || null,
      quoteStatusDetail: polkadotStatusDetail,
      swapTxBuildAvailable: !!polkadotEffectiveLiveSwapsRecommended,
      manualRouteAvailable: !!polkadotManualSwapAvailable,
      manualRouterFallbackAvailable: !!polkadotManualRouterFallbackAvailable,
      exactBuyEnabled: !!polkadotEffectiveExactBuyEnabled,
      side,
      message: reason,
    }, "Hydration BUY Swap Disabled");
    return;
  }

  let address = polkadotWalletState?.address || null;
  if (!address) address = await ensurePolkadotWalletConnected();
  if (!address) {
    const msg = "Connect SubWallet to sign and submit a Hydration swap.";
    onToast?.({ kind: "warn", msg });
    openSubmitResultModal("error", msg, "Hydration Wallet Required");
    return;
  }

  // Hydration SELL remains exact-in. Hydration BUY is exact-out and uses Qty
  // as the exact BASE amount to receive, gated by UTT_HYDRATION_ENABLE_EXACT_BUY=1.
  const amountMode = side === "buy" ? "exact_out" : "exact_in";
  const amount = Number(qtyNum);
  const quoteSpendEstimate = Number(totalQuoteNum);
  if (!Number.isFinite(amount) || amount <= 0) {
    const msg = side === "buy" ? "Enter a valid Qty amount to buy." : "Enter a valid Qty amount to sell.";
    openSubmitResultModal("error", msg, "Hydration Swap Submit Failed");
    return;
  }

  setSubmitting(true);
  setSubmitError(null);
  setSubmitOk(null);
  openHydrationSubmitProgress("build", side);

  try {
    const base = String(apiBase || "").replace(/\/+$/, "");
    const url = `${base}/api/polkadot_dex/hydration/swap_tx`;
    const tok = getAuthToken();
    const headers = { "Content-Type": "application/json" };
    if (tok) headers.Authorization = `Bearer ${tok}`;

    const payload = {
      symbol: String(otSymbol || "").trim(),
      side,
      amount,
      amount_mode: amountMode,
      quote_spend_estimate: Number.isFinite(quoteSpendEstimate) ? quoteSpendEstimate : null,
      route_mode: polkadotManualRouterFallbackAvailable ? "auto" : normalizeHydrationRouteMode(preferredHydrationRouteMode),
      slippage_bps: 100,
      user_pubkey: address,
    };

    const r = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `HTTP ${r.status}`);
    }

    const j = await r.json();
    const encoded = j?.tx?.encodedCallData || j?.tx?.transactionData || j?.encodedCallData || j?.transactionData || null;
    if (!encoded) throw new Error("Hydration swap builder did not return encoded transaction data.");

    openHydrationSubmitProgress("wallet", side);

    let walletSnapshot = polkadotWalletState || {};
    if (!walletSnapshot?.extension?.signer) {
      const next = await connectInjectedPolkadotWallet(preferredPolkadotWallet);
      setPreferredPolkadotWallet(next.key || "subwallet-js");
      walletSnapshot = {
        key: next.key || "subwallet-js",
        label: next.label || getPolkadotWalletLabel(next.key),
        connected: true,
        address: next.address,
        accountName: next.accountName || "",
        accounts: Array.isArray(next.accounts) ? next.accounts : [],
        extension: next.extension || null,
        error: null,
      };
      setPolkadotWalletState(walletSnapshot);
      address = walletSnapshot.address || address;
    }

    const wsUrl = hydrationFrontendWsUrl(polkadotHydrationStatus);

    onToast?.({ kind: "warn", msg: "SubWallet signing prompt opening… review the transaction before approving." });

    const submit = await signAndSubmitHydrationCallData({
      encodedCallData: encoded,
      address,
      walletKey: walletSnapshot?.key || preferredPolkadotWallet || "subwallet-js",
      accounts: walletSnapshot?.accounts || [],
      wsUrl,
      onProgress: (stage) => openHydrationSubmitProgress(stage, side),
    });

    if (submit?.ok === false) {
      const failSummary = submit?.dispatchErrorSummary || "ExtrinsicFailed";
      const failedPayload = {
        ...j,
        signed: true,
        submitted: true,
        finalized: !!submit?.finalized,
        txHash: submit?.txHash || null,
        submit,
        onChainOk: false,
        message: submit?.txHash
          ? `Hydration swap finalized but failed on-chain (${failSummary}): ${submit.txHash}`
          : `Hydration swap finalized but failed on-chain (${failSummary}).`,
      };
      setSubmitError(failedPayload.message);
      openSubmitResultModal("error", failedPayload, "Hydration Swap Failed On-Chain");
      onToast?.({ kind: "warn", msg: failedPayload.message });
      refreshBalancesAfterSubmit({ venueKey: effectiveVenue, focusBase: baseAsset, focusQuote: quoteAsset });
      return;
    }

    const okPayload = {
      ...j,
      signed: true,
      submitted: true,
      finalized: !!submit?.finalized,
      txHash: submit?.txHash || null,
      submit,
      onChainOk: true,
      wallet_address: address,
      user_pubkey: address,
      message: submit?.txHash
        ? `Hydration swap submitted/finalized: ${submit.txHash}`
        : "Hydration swap signed and submitted/finalized.",
    };

    try {
      openHydrationSubmitProgress("record", side);
      const recUrl = `${base}/api/polkadot_dex/hydration/record_submit`;
      const recPayload = {
        ...okPayload,
        symbol: String(otSymbol || "").trim(),
        rawSymbol: okPayload?.rawSymbol || String(otSymbol || "").trim(),
        resolvedSymbol: okPayload?.resolvedSymbol || String(otSymbol || "").trim(),
        side,
        wallet_address: address,
        user_pubkey: address,
      };
      const recHeaders = { "Content-Type": "application/json" };
      if (tok) recHeaders.Authorization = `Bearer ${tok}`;
      const recResp = await fetch(recUrl, {
        method: "POST",
        headers: recHeaders,
        body: JSON.stringify(recPayload),
      });
      const recText = await recResp.text();
      let recJson = null;
      try { recJson = recText ? JSON.parse(recText) : null; } catch { recJson = { raw: recText }; }
      okPayload.recordSubmit = recJson || { ok: recResp.ok };
      okPayload.recorded = !!recResp.ok && !!(recJson?.ok ?? true);
      if (!okPayload.recorded) {
        onToast?.({ kind: "warn", msg: "Hydration swap submitted, but local All Orders recording failed." });
      }
    } catch (recErr) {
      okPayload.recordSubmit = { ok: false, error: recErr?.message || String(recErr) };
      okPayload.recorded = false;
      onToast?.({ kind: "warn", msg: "Hydration swap submitted, but local All Orders recording failed." });
    }

    setSubmitOk(okPayload);
    openSubmitResultModal("ok", okPayload, okPayload.recorded === false ? "Hydration Swap Submitted — Record Failed" : "Hydration Swap Submitted");
    onToast?.({
      kind: "ok",
      msg: submit?.txHash
        ? `Hydration swap submitted${okPayload.recorded === false ? " (record failed)" : " + recorded"}: ${submit.txHash}`
        : `Hydration swap submitted${okPayload.recorded === false ? " (record failed)" : " + recorded"}.`,
    });

    // Refresh spendable balances after on-chain submission/finalization.
    refreshBalancesAfterSubmit({ venueKey: effectiveVenue, focusBase: baseAsset, focusQuote: quoteAsset });
  } catch (e) {
    const msg = e?.message || "Failed to sign/submit Hydration swap";
    setSubmitError(msg);
    openSubmitResultModal("error", msg, "Hydration Swap Submit Failed");
  } finally {
    setSubmitting(false);
  }
}


async function submitLimitOrder() {
  const tok = getAuthToken();

  // Never silently no-op.
  // If something changed after the confirm modal opened, surface why.
  if (!canSubmit) {
    const reason =
      preTrade?.message ||
      (preTrade?.status ? String(preTrade.status) : "") ||
      "Order is not currently submittable — check Qty/Price and venue rules.";
    onToast?.({ kind: "warn", msg: reason });
    openSubmitResultModal("error", reason, "Order Not Submitted");
    return;
  }

  // Do not silently no-op when logged out.
  // Attempt the request without Authorization so we always get a network response (401/403).
  if (!tok) {
    onToast?.({ kind: "warn", msg: "Login required to place orders." });
  }

  setSubmitting(true);
    setSubmitError(null);
    setSubmitOk(null);

    try {
      const v = String(effectiveVenue || "").toLowerCase().trim();
      const sym = String(otSymbol || "").trim();

      const payload = {
        venue: v,
        symbol: sym,
        side,
        type: "limit",
        qty: Number(qtyNum),
        limit_price: Number(pxNum),
        tif,
        post_only: !!postOnly,
        client_order_id: clientOid ? String(clientOid).trim() : undefined,
      };

      const headers = { "Content-Type": "application/json" };
      if (tok) headers.Authorization = `Bearer ${tok}`;

      const base = String(apiBase || "").replace(/\/+$/, "");
      const url = `${base}/api/trade/order`;

      const r = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });

      if (!r.ok) {
        const txt = await r.text();
        const errMsg = txt || `HTTP ${r.status}`;
        throw new Error(errMsg);
      }

      const j = await r.json();
      setSubmitOk(j);

      // Show modal instead of inline printing below the widget
      openSubmitResultModal("ok", j, "Order Submitted");

      // UPDATED: capture venue + base/quote at submit time and refresh deterministically.
      refreshBalancesAfterSubmit({ venueKey: v, focusBase: baseAsset, focusQuote: quoteAsset });
    } catch (e) {
      const msg = e?.message || "Failed to submit order";
      setSubmitError(msg);

      // Show modal for error too (same UX pattern)
      openSubmitResultModal("error", msg, "Order Submit Failed");
    } finally {
      setSubmitting(false);
    }
  }

  function openConfirm() {
    if (submitting) return;
    if (!canSubmit) {
      // Restore prior UX expectation: give an explicit reason instead of "nothing happens".
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Order is not currently submittable — check Qty/Price and venue rules.";
      onToast?.({ kind: "warn", msg: reason });
      return;
    }
    setShowConfirm(true);
  }

  function confirmAndSubmit() {
    if (submitting) return;
    if (!canSubmit) {
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Order is not currently submittable — check Qty/Price and venue rules.";
      onToast?.({ kind: "warn", msg: reason });
      setShowConfirm(false);
      return;
    }
    setShowConfirm(false);
    // Surface immediate feedback and never allow a silent no-op.
    if (isPolkadotDexVenue) {
      openHydrationSubmitProgress("build", side);
    } else {
      openSubmitResultModal("info", "Submitting…", "Submitting");
    }
    void (
      isSolanaLimitMode
        ? submitSolanaTriggerLimitOrder()
        : isSolanaDexVenue
          ? submitSolanaSwapOrder()
          : isPolkadotDexVenue
            ? submitPolkadotSwapOrder()
            : submitLimitOrder()
    ).catch((e) => {
      const msg = e?.message || String(e);
      openSubmitResultModal(
        "error",
        msg,
        isSolanaLimitMode ? "Jupiter Limit Submit Failed" : isSolanaDexVenue ? "Swap Submit Failed" : isPolkadotDexVenue ? "Polkadot Swap Submit Failed" : "Order Submit Failed"
      );
    });
  }

  // Defensive: if styles is missing, do not crash the entire UI.
  const safeStyles = styles || {};
  const safeDock = safeStyles.orderBookDock || {};
  const safeButton = safeStyles.button || {};
  const safeButtonDisabled = safeStyles.buttonDisabled || {};
  const safeInput = safeStyles.input || {};
  const safeSelect = safeStyles.select || {};
  const safePill = safeStyles.pill || {};
  const darkSelectStyle = {
    ...safeSelect,
    minWidth: 110,
    padding: "4px 6px",
    background: "#101010",
    backgroundColor: "#101010",
    color: "#eaeaea",
    border: "1px solid rgba(255,255,255,0.14)",
  };
  const darkOptionStyle = { backgroundColor: "#101010", color: "#eaeaea" };
  const safeMuted = safeStyles.muted || {};
  const safeWidgetTitleRow = safeStyles.widgetTitleRow || {};
  const safeWidgetSub = safeStyles.widgetSub || {};
  const safeCodeError = safeStyles.codeError || {};

  const shellStyleBase = inlineMode
    ? {
        ...safeDock,
        width: "100%",
        maxWidth: "100%",
        height: "100%",
        maxHeight: "100%",
        resize: "none",
        overflow: "hidden",
        marginTop: 0,
        display: "flex",
        flexDirection: "column",
        flex: "1 1 auto",
        minHeight: 0,
        minWidth: 0,
        boxSizing: "border-box",
      }
    : {
        ...safeDock,
        width: box.w,
        height: box.h,
        resize: "none",
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        boxSizing: "border-box",
      };

  const sideAccent = side === "buy" ? "#1f6f3a" : "#7a2b2b";
  const sideBg = side === "buy" ? "rgba(31, 111, 58, 0.07)" : "rgba(122, 43, 43, 0.07)";

  const shellStyle = {
    ...shellStyleBase,
    boxShadow: `0 0 0 1px ${sideAccent} inset`,
    background: shellStyleBase?.background ? shellStyleBase.background : undefined,
    backgroundImage: `linear-gradient(${sideBg}, ${sideBg})`,
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
    : { position: "fixed", left: box.x, top: box.y, zIndex: 61, userSelect: "none" };

  const rowStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" };
  const rowTightStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginTop: 6 };
  const sectionGap = 6;


  const ticketScrollBodyStyle = {
    display: "flex",
    flexDirection: "column",
    flex: "1 1 auto",
    minHeight: 0,
    minWidth: 0,
    overflowY: "auto",
    overflowX: "hidden",
    paddingRight: 6,
    scrollbarWidth: "thin",
    scrollbarColor: "rgba(255,255,255,0.22) transparent",
  };

  const sideBtnBase = { ...safeButton, padding: "6px 10px", borderRadius: 10, fontWeight: 800, lineHeight: 1.1 };
  const sideBtnActive = { background: "#151515", border: "1px solid #3a3a3a" };

  const fmtAvail = (n) => {
    if (n === null || n === undefined) return "—";
    const x = Number(n);
    if (!Number.isFinite(x)) return "—";
    return x.toLocaleString(undefined, { maximumFractionDigits: 12 });
  };

  const maskIfHidden = (s) => (hideTableData ? "••••" : s);

  const totalLabel = useMemo(() => {
    const q = String(quoteAsset || "").trim();
    return q ? q : "Quote";
  }, [quoteAsset]);

  const walletKitButtonLabel = useMemo(() => {
    if (!walletKitConnected) return "Connect Wallet";

    const address =
      solanaWalletState?.address ||
      solanaProviderPubkeyBase58(walletKitBridgeProvider) ||
      (typeof walletKit?.publicKey?.toBase58 === "function"
        ? walletKit.publicKey.toBase58()
        : typeof walletKit?.publicKey?.toString === "function"
          ? walletKit.publicKey.toString()
          : "");

    if (!address) return "Wallet Connected";
    return hideTableData ? "••••" : shortenWalletAddress(address);
  }, [walletKitConnected, solanaWalletState?.address, walletKitBridgeProvider, walletKit?.publicKey, hideTableData]);

  const walletKitButtonTitle = useMemo(() => {
    if (!walletKitConnected) return "Open the Jupiter Wallet Kit connect dialog";
    const address = solanaWalletState?.address || "";
    const label = solanaWalletLabel || walletKitRawAdapterName || "Wallet";
    if (address && !hideTableData) return `${label}: ${address}`;
    return "Open the Jupiter Wallet Kit wallet manager";
  }, [walletKitConnected, solanaWalletState?.address, solanaWalletLabel, walletKitRawAdapterName, hideTableData]);

  const walletButtonVisualKey = useMemo(() => {
    return solanaWalletState?.key || walletKitSelectedKey || classifyWalletAdapterNameToKey(walletKitRawAdapterName) || null;
  }, [solanaWalletState?.key, walletKitSelectedKey, walletKitRawAdapterName]);

  const walletButtonVisualAddress = useMemo(() => {
    return (
      solanaWalletState?.address ||
      solanaProviderPubkeyBase58(walletKitBridgeProvider) ||
      (typeof walletKit?.publicKey?.toBase58 === "function"
        ? walletKit.publicKey.toBase58()
        : typeof walletKit?.publicKey?.toString === "function"
          ? walletKit.publicKey.toString()
          : "")
    );
  }, [solanaWalletState?.address, walletKitBridgeProvider, walletKit?.publicKey]);

  const walletButtonVisualMeta = useMemo(() => {
    return getSolanaWalletVisualMeta(
      walletButtonVisualKey,
      solanaWalletLabel || walletKitRawAdapterName || "Wallet",
      walletKit?.wallet?.adapter?.icon || walletKit?.wallet?.icon || ""
    );
  }, [walletButtonVisualKey, solanaWalletLabel, walletKitRawAdapterName, walletKit?.wallet]);

  const walletKitSelectableWallets = useMemo(() => {
    const raw = Array.isArray(walletKit?.wallets) ? walletKit.wallets : [];
    const seen = new Set();
    const out = [];

    for (const entry of raw) {
      const adapterName = String(entry?.adapter?.name || entry?.name || "").trim();
      const key = classifyWalletAdapterNameToKey(adapterName);
      if (!key || !adapterName) continue;
      const uniq = `${key}::${adapterName}`;
      if (seen.has(uniq)) continue;
      seen.add(uniq);
      out.push({
        key,
        name: adapterName,
        label: getSolanaWalletVisualMeta(key, adapterName, entry?.adapter?.icon || entry?.icon || "").label || adapterName,
      });
    }

    const order = { jupiter: 0, solflare: 1, phantom: 2, backpack: 3 };
    out.sort((a, b) => {
      const oa = Number.isFinite(order[a.key]) ? order[a.key] : 99;
      const ob = Number.isFinite(order[b.key]) ? order[b.key] : 99;
      if (oa !== ob) return oa - ob;
      return String(a.label || a.name || "").localeCompare(String(b.label || b.name || ""));
    });
    return out;
  }, [walletKit?.wallets]);

  const walletKitSelectedAdapterName = useMemo(() => {
    return String(walletKit?.wallet?.adapter?.name || walletKit?.wallet?.name || "").trim();
  }, [walletKit?.wallet]);

  async function handleWalletKitSelectChange(nextName) {
    const targetName = String(nextName || "").trim();
    if (!targetName) return;

    try {
      const currentName = String(walletKit?.wallet?.adapter?.name || walletKit?.wallet?.name || "").trim();
      const sameWallet = currentName && currentName === targetName;

      setBalAvail({});
      setBalErr(null);
      setWalletKitPendingConnectName(targetName);

      const nextKey = classifyWalletAdapterNameToKey(targetName);
      if (nextKey) setPreferredSolanaWallet(nextKey);

      if (sameWallet) {
        if (!walletKit?.connected && typeof walletKit?.connect === "function") {
          try { await walletKit.connect(); } finally { setWalletKitPendingConnectName(""); }
        } else {
          setWalletKitPendingConnectName("");
        }
        return;
      }

      // Let Wallet Kit switch adapters first; keep the manager button usable in case
      // the target wallet requires an explicit modal step/approval.
      if (typeof walletKit?.select === "function") {
        walletKit.select(targetName);
      }

      if (typeof window !== "undefined") {
        window.setTimeout(() => {
          try { openWalletKitManager(); } catch {}
        }, 40);
      }
    } catch (e) {
      const msg = e?.message || `Failed to switch wallet to ${targetName}.`;
      setWalletKitPendingConnectName("");
      setSubmitError(msg);
      openSubmitResultModal("error", msg, "Wallet Switch Failed");
    }
  }

  useEffect(() => {
    if (!isSolanaDexVenue) return;
    const targetName = String(walletKitPendingConnectName || "").trim();
    if (!targetName) return;

    const selectedName = String(walletKit?.wallet?.adapter?.name || walletKit?.wallet?.name || "").trim();
    if (!selectedName || selectedName !== targetName) return;

    if (walletKit?.connected && walletKit?.publicKey) {
      setWalletKitPendingConnectName("");
      return;
    }

    if (typeof walletKit?.connect !== "function") {
      setWalletKitPendingConnectName("");
      return;
    }

    let cancelled = false;
    const t = setTimeout(() => {
      void (async () => {
        try {
          await walletKit.connect();
        } catch (e) {
          if (cancelled) return;
          const msg = e?.message || `Failed to connect ${targetName}.`;
          setSubmitError(msg);
          openSubmitResultModal("error", msg, "Wallet Connect Failed");
        } finally {
          if (!cancelled) setWalletKitPendingConnectName("");
        }
      })();
    }, 160);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [isSolanaDexVenue, walletKitPendingConnectName, walletKit?.wallet, walletKit?.connected, walletKit?.publicKey]);

  useEffect(() => {
    const targetName = String(walletKitPendingConnectName || "").trim();
    if (!targetName) return;

    const t = setTimeout(() => {
      setWalletKitPendingConnectName((cur) => (String(cur || "").trim() === targetName ? "" : cur));
    }, 2500);

    return () => clearTimeout(t);
  }, [walletKitPendingConnectName]);



  // NEW: allow re-opening the last submit result without re-submitting
  const hasLastSubmitResult = useMemo(
    () => submitResultPayload !== null && submitResultKind !== null,
    [submitResultPayload, submitResultKind]
  );

  const submitEndpointLabel = useMemo(() => {
    if (isPolkadotDexVenue) {
      if (polkadotManualRouterFallbackAvailable) return `/api/polkadot_dex/hydration/swap_tx (manual Router ${side === "buy" ? "BUY" : "SELL"} fallback) → SubWallet sign/send`;
      if (polkadotSyntheticPriceOnly) return "/api/polkadot_dex/hydration/orderbook (synthetic price only; no executable route)";
      if (!polkadotEffectiveQuotesAvailable) return "/api/polkadot_dex/hydration/status (quotes disabled)";
      if (!polkadotEffectiveLiveSwapsRecommended) return "/api/polkadot_dex/hydration/swap_tx (build disabled)";
      if (side === "buy" && !polkadotEffectiveExactBuyEnabled) return "/api/polkadot_dex/hydration/swap_tx (BUY disabled; SELL enabled)";
      return `/api/polkadot_dex/hydration/swap_tx (${hydrationRouteModeLabel(preferredHydrationRouteMode)}) → SubWallet sign/send`;
    }
    if (!isSolanaDexVenue) return "/api/trade/order";
    if (isSolanaLimitMode) return "/api/solana_dex/jupiter/trigger/create_order";
    const v = String(effectiveVenue || "").toLowerCase().trim();
    const routerMode = String(preferredSolanaRouterMode || "auto").toLowerCase().trim();
    if (v === "solana_raydium" || routerMode === "raydium") return "/api/solana_dex/raydium/swap_tx";
    if (routerMode === "ultra") return "/api/solana_dex/jupiter/ultra_order → /api/solana_dex/jupiter/ultra_execute";
    if (routerMode === "metis") return "/api/solana_dex/jupiter/swap_tx";
    return "/api/solana_dex/jupiter/ultra_order → /api/solana_dex/jupiter/ultra_execute → fallback /api/solana_dex/jupiter/swap_tx → fallback /api/solana_dex/raydium/swap_tx";
  }, [isSolanaDexVenue, isSolanaLimitMode, isPolkadotDexVenue, effectiveVenue, preferredSolanaRouterMode, preferredHydrationRouteMode, polkadotManualRouterFallbackAvailable, polkadotSyntheticPriceOnly, polkadotEffectiveQuotesAvailable, polkadotEffectiveLiveSwapsRecommended, side, polkadotEffectiveExactBuyEnabled]);

  async function copySubmitResultToClipboard() {
    try {
      if (!HAS_WINDOW || !navigator?.clipboard?.writeText) return;
      await navigator.clipboard.writeText(String(submitResultText || ""));
    } catch {
      // ignore
    }
  }

  // Input display value: expand exponent without mutating what user is typing.
  const limitDisplayValue = useMemo(() => {
    const s = String(limitPrice ?? "");
    if (!s) return "";
    return expandExponential(s);
  }, [limitPrice]);

  const confirmLines = useMemo(() => {
    const v = venueLabel || "—";
    const sym = String(otSymbol || "").trim() || "—";
    const qStr = qtyNum === null ? "—" : qtyNum.toLocaleString(undefined, { maximumFractionDigits: 18 });

    // IMPORTANT: never show sci in confirmation UI
    const pxStr = !limitPrice || pxNum === null ? "—" : expandExponential(limitPrice).toString();

    const totStr =
      notional === null ? "—" : notional.toLocaleString(undefined, { maximumFractionDigits: totalQuoteDecimals });
    const reqTotStr =
      totalQuoteNum === null ? "—" : totalQuoteNum.toLocaleString(undefined, { maximumFractionDigits: totalQuoteDecimals });

    return [
      { k: "Venue", v: hideVenueNames ? "••••" : v },
      { k: "Symbol", v: hideTableData ? "••••" : sym },
      { k: "Side", v: side.toUpperCase() },
      { k: "Type", v: isSolanaJupiterVenue ? (solanaOrderMode === "limit" ? "LIMIT" : "SWAP") : isPolkadotDexVenue ? "SWAP" : "LIMIT" },
      ...(isPolkadotDexVenue ? [{ k: "Route", v: polkadotManualRouterFallbackAvailable ? "Manual Router fallback" : hydrationRouteModeLabel(preferredHydrationRouteMode) }] : []),
      { k: "Qty", v: hideTableData ? "••••" : qStr },
      { k: "Limit", v: hideTableData ? "••••" : pxStr },
      { k: `Total (${totalLabel})`, v: hideTableData ? "••••" : totStr },
      ...(autoCalc ? [{ k: `Requested Total (${totalLabel})`, v: hideTableData ? "••••" : reqTotStr }] : []),
      ...(isSolanaLimitMode
        ? [{ k: "Expiry", v: hideTableData ? "••••" : solanaExpiryLabel }]
        : [{ k: "TIF", v: String(tif || "gtc").toUpperCase() }]),
      ...(!isSolanaLimitMode ? [{ k: "Post-only", v: postOnly ? "YES" : "NO" }] : []),
      ...(!isSolanaLimitMode && clientOid ? [{ k: "Client OID", v: hideTableData ? "••••" : String(clientOid) }] : []),
    ];
  }, [
    venueLabel,
    otSymbol,
    side,
    qtyNum,
    pxNum,
    limitPrice,
    notional,
    totalQuoteDecimals,
    totalQuoteNum,
    totalLabel,
    tif,
    postOnly,
    clientOid,
    hideTableData,
    hideVenueNames,
    autoCalc,
    isSolanaJupiterVenue,
    isPolkadotDexVenue,
    preferredHydrationRouteMode,
    isSolanaLimitMode,
    solanaOrderMode,
    solanaExpiryLabel,
  ]);

  function openWalletKitManager() {
    try {
      const host = walletKitButtonHostRef.current;
      if (!host) return;
      const btn = host.querySelector('button, [role="button"], a');
      if (btn && typeof btn.click === "function") {
        btn.click();
      }
    } catch {
      // ignore
    }
  }


  return (
    <div style={fixedWrapperStyle}>
      <div style={shellStyle}>
        <div
          style={{
            ...safeWidgetTitleRow,
            cursor: inlineMode || locked ? "default" : "move",
            paddingBottom: 4,
            borderBottom: "1px solid #2a2a2a",
            marginBottom: 8,
          }}
          onMouseDown={onDragMouseDown}
          title={inlineMode ? "" : locked ? "Locked" : "Drag to move (snug gutter, no margins)"}
        >
          <h3 style={{ ...styles.widgetTitle, fontSize: 16, lineHeight: "18px" }}>Order Ticket</h3>
          <span style={safeWidgetSub}>
            Venue used: <b>{venueLabel || "—"}</b>
          </span>
        </div>


        {(forceTileMode || !inlineMode) && (
          <style>{`
            .utt-order-ticket-scroll::-webkit-scrollbar { width: 10px; }
            .utt-order-ticket-scroll::-webkit-scrollbar-track { background: transparent; }
            .utt-order-ticket-scroll::-webkit-scrollbar-thumb {
              background: rgba(255,255,255,0.18);
              border-radius: 999px;
              border: 2px solid transparent;
              background-clip: padding-box;
            }
            .utt-order-ticket-scroll::-webkit-scrollbar-thumb:hover {
              background: rgba(255,255,255,0.28);
              border: 2px solid transparent;
              background-clip: padding-box;
            }
          `}</style>
        )}

        <div
          style={ticketScrollBodyStyle}
          className="utt-order-ticket-scroll"
        >
          <div style={rowStyle}>
          <div style={safePill}>
            <span>Symbol</span>
            <input
              style={{ ...safeInput, width: 150 }}
              value={otSymbol}
              placeholder="e.g. BTC-USD"
              onChange={(e) => setOtSymbol(e.target.value)}
            />
          </div>

          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <button
              style={{
                ...sideBtnBase,
                ...(side === "buy" ? sideBtnActive : null),
                boxShadow: side === "buy" ? `0 0 0 1px ${sideAccent} inset` : undefined,
              }}
              onClick={() => setSide("buy")}
              type="button"
            >
              Buy
            </button>
            <button
              style={{
                ...sideBtnBase,
                ...(side === "sell" ? sideBtnActive : null),
                boxShadow: side === "sell" ? `0 0 0 1px ${sideAccent} inset` : undefined,
              }}
              onClick={() => setSide("sell")}
              type="button"
            >
              Sell
            </button>
          </div>

          {isSolanaJupiterVenue && (
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <button
                style={{
                  ...sideBtnBase,
                  ...(solanaOrderMode === "swap" ? sideBtnActive : null),
                  boxShadow: solanaOrderMode === "swap" ? "0 0 0 1px #2f4f8f inset" : undefined,
                }}
                onClick={() => setSolanaOrderMode("swap")}
                type="button"
                title="Use the existing Jupiter swap flow"
              >
                Swap
              </button>
              <button
                style={{
                  ...sideBtnBase,
                  ...(solanaOrderMode === "limit" ? sideBtnActive : null),
                  boxShadow: solanaOrderMode === "limit" ? "0 0 0 1px #8f6a2f inset" : undefined,
                }}
                onClick={() => setSolanaOrderMode("limit")}
                type="button"
                title="Use Jupiter Trigger limit orders"
              >
                Limit
              </button>
            </div>
          )}

        </div>

        {rulesBanner && (
          <div
            style={{
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 10,
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
              ...rulesBannerStyle,
            }}
            title="Policy/rules checks are advisory; backend/venue may still accept/reject."
          >
            {rulesBanner.lines.map((ln, i) => (
              <div key={i}>{ln}</div>
            ))}
          </div>
        )}

        {preTrade && (
          <div
            style={{
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 10,
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
              ...preTradeStyle,
            }}
            title="Pre-trade checks use venue constraints (min + increments). When checks fail and rules are known, submit is blocked."
          >
            <div style={{ fontWeight: 900, marginBottom: preTrade.lines?.length ? 4 : 0 }}>{preTrade.title}</div>
            {Array.isArray(preTrade.lines) &&
              preTrade.lines.map((ln, i) => (
                <div key={i}>• {ln}</div>
              ))}
          </div>
        )}

        {hydrationManualRouterPriceGuard?.show && (
          <div
            style={{
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 10,
              fontSize: 11,
              lineHeight: 1.18,
              whiteSpace: "pre-wrap",
              border: hydrationManualRouterPriceGuard.mismatch
                ? "1px solid rgba(245, 158, 11, 0.55)"
                : "1px solid rgba(59, 130, 246, 0.35)",
              background: hydrationManualRouterPriceGuard.mismatch
                ? "rgba(120, 72, 16, 0.18)"
                : "rgba(30, 64, 175, 0.10)",
              color: hydrationManualRouterPriceGuard.mismatch ? "#ffe2a6" : "#bfdbfe",
            }}
            title="Hydration manual Router uses synthetic/reference orderbook levels. BUY should use asks; SELL should use bids."
          >
            <div style={{ fontWeight: 900, marginBottom: 4 }}>
              Hydration Router price guard
            </div>
            <div>
              {side === "buy"
                ? "BUY exact-out should use the lowest sell / best ask."
                : "SELL exact-in should use the highest buy / best bid."}
            </div>
            <div>
              Best ask: <b>{hydrationManualRouterPriceGuard.bestAsk === null ? "—" : fmtPlain(hydrationManualRouterPriceGuard.bestAsk, { maxFrac: 18 })}</b>
              {" "}• Best bid: <b>{hydrationManualRouterPriceGuard.bestBid === null ? "—" : fmtPlain(hydrationManualRouterPriceGuard.bestBid, { maxFrac: 18 })}</b>
              {" "}• tolerance: <b>{hydrationManualRouterPriceGuard.toleranceBps} bps</b>
            </div>
            {hydrationManualRouterPriceGuard.mismatch && (
              <div style={{ marginTop: 4, fontWeight: 800 }}>
                Current limit is meaningfully on the wrong side of the book and may fail with Router.TradingLimitReached.
              </div>
            )}
            {hydrationManualRouterPriceGuard.recommendedPrice !== null && (
              <button
                type="button"
                style={{ ...safeButton, padding: "5px 8px", marginTop: 6 }}
                onClick={applyHydrationBookSideLimit}
                title={`Set limit to ${hydrationManualRouterPriceGuard.recommendedLabel}`}
              >
                Use {side === "buy" ? "best ask" : "best bid"}
              </button>
            )}
          </div>
        )}

        {isSolanaLimitMode && (
          <div
            style={{
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 10,
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
              border: "1px solid #3b3413",
              background: "#151208",
              color: "#f2e6b7",
            }}
            title="Jupiter requires a minimum current input-token value for Trigger limit orders."
          >
            Jupiter limit minimum: <b>${JUPITER_LIMIT_MIN_USD.toFixed(2)}</b>
            {jupiterMinFrontendEnforceable && jupiterFrontendInputUsdValue !== null ? (
              <> • Current frontend-estimated input value: <b>${jupiterFrontendInputUsdValue.toFixed(4)}</b></>
            ) : (
              <> • Backend will enforce current USD input-value minimum on submit.</>
            )}
          </div>
        )}

        <div style={{ ...rowTightStyle, marginTop: sectionGap }}>
          <div style={safePill}>
            <span>Qty</span>
            <input
              style={{ ...safeInput, width: 125 }}
              value={qty}
              placeholder="Amount"
              onChange={(e) => {
                lastEditedRef.current = "qty";
                setQty(e.target.value);
              }}
              inputMode="decimal"
            />
          </div>

          <div style={safePill}>
            <span>Limit</span>
            <input
              style={{ ...safeInput, width: 140 }}
              type="text"
              inputMode="decimal"
              pattern="^[0-9]*[.]?[0-9]*$"
              value={limitDisplayValue}
              placeholder="Limit price"
              onFocus={() => {
                limitEditingRef.current = true;
                limitSourceRef.current = "user";
              }}
              onChange={(e) => {
                limitEditingRef.current = true;
                limitSourceRef.current = "user";

                const cleaned = sanitizeDecimalInput(e.target.value);

                // Solana DEX venues: keep the user-picked decimal price as-is; do not CEX-normalize.
                if (isDexSwapVenue) {
                  setLimitPrice(cleaned);
                  return;
                }

                // If user pasted/entered too many decimals, normalize immediately (prevents “stuck disabled button”).
                const d = countDecimalsFromString(expandExponential(cleaned));
                const pxDec = rules?.price_decimals;
                const allowed =
                  Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0
                    ? Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18)
                    : null;

                if (d !== null && allowed !== null && d > allowed) {
                  const normalized = normalizeLimitPriceStr(cleaned, rules, side);
                  setLimitPrice(normalized);
                  return;
                }

                setLimitPrice(cleaned);
              }}
              onBlur={() => {
                limitEditingRef.current = false;
                limitSourceRef.current = "blur";

                if (!limitPrice) return;

                // Solana DEX venues: do not clamp/round limit price on blur.
                if (isDexSwapVenue) return;

                const normalized = normalizeLimitPriceStr(limitPrice, rules, side);
                if (normalized && normalized !== String(limitPrice)) setLimitPrice(normalized);
              }}
            />
          </div>

          <div style={safePill} title={`Total (${totalLabel}) to spend/receive.`}>
            <span>Total</span>
            <input
              style={{ ...safeInput, width: 120 }}
              value={totalQuote}
              placeholder={totalLabel}
              onChange={(e) => {
                lastEditedRef.current = "total";
                const cleaned = sanitizeDecimalInput(e.target.value);
                setTotalQuote(cleaned);

                // DEX-only: ensure Total→Qty auto-calc works even when venue rules are unavailable.
                if (isDexSwapVenue && autoCalc) {
                  const t = Number(cleaned);
                  const p = Number(expandExponential(limitPrice));
                  if (Number.isFinite(t) && t > 0 && Number.isFinite(p) && p > 0) {
                    const raw = t / p;
                    if (Number.isFinite(raw) && raw > 0) {
                      const nextQty = fmtPlain(raw, { maxFrac: 18 });
                      if (nextQty) setQty(nextQty);
                    }
                  }
                }
              }}
              inputMode="decimal"
            />
          </div>

          <label style={safePill} title="When enabled, Qty and Total stay in sync.">
            <input type="checkbox" checked={autoCalc} onChange={(e) => setAutoCalc(e.target.checked)} />
            <span>Auto-calc</span>
          </label>
        </div>

        <div style={{ ...rowTightStyle, marginTop: sectionGap }}>
          <div style={{ ...safePill, gap: 8 }}>
            <span style={{ opacity: 0.85 }}>Avail</span>

            <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
              {baseAsset ? (
                <>
                  <b>{baseAsset}</b>: {maskIfHidden(fmtAvail(baseAvail))}
                </>
              ) : (
                <>Base: —</>
              )}
            </span>

            <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
              {quoteAsset ? (
                <>
                  <b>{quoteAsset}</b>: {maskIfHidden(fmtAvail(quoteAvail))}
                </>
              ) : (
                <>Quote: —</>
              )}
            </span>

            <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
              Focus({side}):{" "}
              <b>
                {relevantAvailLabel}: {maskIfHidden(fmtAvail(relevantAvailValue))}
              </b>
            </span>

            <button
              style={{ ...safeButton, padding: "5px 8px", lineHeight: 1.05 }}
              onClick={() => refreshAvailBalances({ force: true, maxPolls: 2, pollBackoffMs: [800, 1200] })}
              disabled={balLoading}
              title="Refresh balances from venue"
            >
              {balLoading ? "…" : "Refresh"}
            </button>
          </div>

          {isSolanaDexVenue && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                flexWrap: "wrap",
                marginTop: 6,
                padding: "6px 10px",
                borderRadius: 10,
                border: "1px solid rgba(255,255,255,0.08)",
                background: "rgba(255,255,255,0.03)",
                fontSize: 12,
                opacity: 0.95,
              }}
            >
              <span style={{ display: "inline-flex", alignItems: "center", gap: 8, minWidth: 0 }}>
                <span
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: 999,
                    background: solanaWalletConnected ? "rgba(46, 204, 113, 0.95)" : "rgba(231, 76, 60, 0.95)",
                    boxShadow: "0 0 0 2px rgba(0,0,0,0.35)",
                  }}
                />
                {solanaWalletConnected ? (
                  <span>
                    Connected w/<b style={{ marginLeft: 4 }}>{solanaWalletLabel || "Wallet"}</b>
                  </span>
                ) : (
                  <span>
                    Disconnected <span style={{ opacity: 0.75 }}>({solanaWalletLabel || "Wallet"})</span>
                  </span>
                )}
              </span>

              <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1, opacity: 0.9 }}>
                {walletKitRawAdapterName
                  ? `Managed by Wallet Kit: ${hideTableData ? "••••" : walletKitRawAdapterName}`
                  : solanaWalletLabel
                    ? `Resolved wallet: ${hideTableData ? "••••" : solanaWalletLabel}`
                    : "Use Wallet Kit to connect a supported Solana wallet."}
              </span>

              {isSolanaJupiterVenue ? (
                <label style={{ display: "inline-flex", alignItems: "center", gap: 6, marginLeft: "auto", opacity: 0.92, flexWrap: "nowrap" }}>
                  <span>Router</span>
                  <select
                    style={{ ...darkSelectStyle, minWidth: 104 }}
                    value={preferredSolanaRouterMode}
                    onChange={(e) => setPreferredSolanaRouterModeState(e.target.value)}
                    title="Swap routing source"
                  >
                    <option value="auto" style={darkOptionStyle}>Auto</option>
                    <option value="ultra" style={darkOptionStyle}>Jupiter Ultra</option>
                    <option value="metis" style={darkOptionStyle}>Jupiter Metis</option>
                    <option value="raydium" style={darkOptionStyle}>Raydium</option>
                  </select>
                </label>
              ) : null}
            </div>
          )}

          {isSolanaDexVenue && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                flexWrap: "wrap",
                marginTop: 6,
                padding: "6px 10px",
                borderRadius: 10,
                border: "1px solid rgba(255,255,255,0.08)",
                background: "rgba(255,255,255,0.02)",
                fontSize: 12,
                opacity: 0.95,
                position: "relative",
              }}
            >
              <span style={{ opacity: 0.9, fontWeight: 700 }}>Wallet Kit</span>
              <label style={{ display: "inline-flex", alignItems: "center", gap: 6, flexWrap: "nowrap" }}>
                <span style={{ opacity: 0.82 }}>Wallet</span>
                <select
                  style={{ ...darkSelectStyle, minWidth: 112 }}
                  value={walletKitSelectedAdapterName || ""}
                  onChange={(e) => { void handleWalletKitSelectChange(e.target.value); }}
                  title="Switch Wallet Kit wallet"
                  
                >
                  {!walletKitSelectedAdapterName && <option value="" style={darkOptionStyle}>Select wallet</option>}
                  {walletKitSelectableWallets.map((opt) => (
                    <option key={opt.name} value={opt.name} style={darkOptionStyle}>
                      {opt.label}
                    </option>
                  ))}
                </select>
              </label>
              <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
                {walletKitPendingConnectName
                  ? `Switching to: ${hideTableData ? "••••" : walletKitPendingConnectName}`
                  : walletKitRawAdapterName
                    ? `${walletKitConnected ? "Selected" : "Last selected"}: ${hideTableData ? "••••" : walletKitRawAdapterName}`
                    : "Open wallet manager"}
              </span>
              <button
                type="button"
                style={{
                  ...safeButton,
                  padding: walletKitConnected ? "6px 8px" : "7px 10px",
                  marginLeft: "auto",
                  minWidth: walletKitConnected ? 132 : 120,
                  maxWidth: walletKitConnected ? 176 : 144,
                  flex: "0 1 auto",
                  fontWeight: 800,
                  borderColor: walletKitConnected ? walletButtonVisualMeta?.border : safeButton?.borderColor,
                  boxShadow: walletKitConnected ? `0 0 0 1px ${walletButtonVisualMeta?.border || "rgba(255,255,255,0.10)"} inset, 0 0 14px ${walletButtonVisualMeta?.glow || "transparent"}` : undefined,
                }}
                onClick={openWalletKitManager}
                title={walletKitButtonTitle}
                
              >
                {walletKitConnected ? (
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 5, minWidth: 0, maxWidth: "100%" }}>
                    {walletButtonVisualMeta?.icon ? (
                      <img
                        src={walletButtonVisualMeta.icon}
                        alt={walletButtonVisualMeta.label || "Wallet"}
                        style={{
                          width: 18,
                          height: 18,
                          borderRadius: 6,
                          objectFit: "cover",
                          boxShadow: "0 0 0 1px rgba(255,255,255,0.12) inset",
                          flex: "0 0 auto",
                        }}
                      />
                    ) : (
                      <span
                        aria-hidden="true"
                        style={{
                          width: 18,
                          height: 18,
                          borderRadius: 6,
                          display: "inline-flex",
                          alignItems: "center",
                          justifyContent: "center",
                          background: walletButtonVisualMeta?.fallbackBg || "#0f172a",
                          color: walletButtonVisualMeta?.fallbackFg || "#e5f3ff",
                          fontSize: 11,
                          fontWeight: 900,
                          lineHeight: 1,
                          flex: "0 0 auto",
                        }}
                      >
                        {walletButtonVisualMeta?.fallbackText || "W"}
                      </span>
                    )}

                    <span style={{ display: "inline-flex", alignItems: "baseline", gap: 4, minWidth: 0, maxWidth: "100%", whiteSpace: "nowrap", overflow: "hidden" }}>
                      <span style={{ color: walletButtonVisualMeta?.color || "#eaeaea", fontWeight: 900, fontSize: 11, flex: "0 0 auto" }}>
                        {hideTableData ? "••••" : (walletButtonVisualMeta?.label || "Wallet")}
                      </span>
                      <span
                        style={{
                          color: walletButtonVisualMeta?.color || "#eaeaea",
                          opacity: 0.98,
                          fontWeight: 800,
                          fontSize: 11,
                          minWidth: 0,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                        }}
                      >
                        {hideTableData ? "••••" : shortenWalletAddress(walletButtonVisualAddress)}
                      </span>
                    </span>
                  </span>
                ) : (
                  walletKitButtonLabel
                )}
              </button>
              <div
                ref={walletKitButtonHostRef}
                aria-hidden="true"
                style={{
                  position: "absolute",
                  right: 0,
                  bottom: 0,
                  width: 1,
                  height: 1,
                  overflow: "hidden",
                  opacity: 0,
                  pointerEvents: "none",
                }}
              >
                <UnifiedWalletButton />
              </div>
            </div>
          )}

          {isPolkadotDexVenue && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                flexWrap: "wrap",
                marginTop: 6,
                padding: "6px 10px",
                borderRadius: 10,
                border: "1px solid rgba(255,255,255,0.08)",
                background: "rgba(255,255,255,0.02)",
                fontSize: 12,
                opacity: 0.95,
              }}
            >
              <span style={{ opacity: 0.9, fontWeight: 700 }}>Polkadot</span>
              <label style={{ display: "inline-flex", alignItems: "center", gap: 6, flexWrap: "nowrap" }}>
                <span style={{ opacity: 0.82 }}>Wallet</span>
                <select
                  style={{ ...darkSelectStyle, minWidth: 118 }}
                  value={preferredPolkadotWallet || "subwallet-js"}
                  onChange={(e) => {
                    setPreferredPolkadotWallet(e.target.value);
                    setPolkadotWalletState((prev) => ({
                      ...(prev || {}),
                      key: e.target.value,
                      label: getPolkadotWalletLabel(e.target.value),
                      connected: false,
                      address: null,
                      accountName: "",
                      extension: null,
                      error: null,
                    }));
                  }}
                  title="Select Polkadot injected wallet"
                >
                  {installedPolkadotWallets.length === 0 ? (
                    <option value="subwallet-js" style={darkOptionStyle}>SubWallet</option>
                  ) : null}
                  {installedPolkadotWallets.map((opt) => (
                    <option key={opt.key} value={opt.key} style={darkOptionStyle}>
                      {opt.label}
                    </option>
                  ))}
                </select>
              </label>
              <button
                type="button"
                style={{ ...safeButton, padding: "6px 8px", fontWeight: 800 }}
                onClick={() => setPolkadotSettingsOpen(true)}
                title={`Polkadot DEX settings. Route: ${hydrationRouteModeLabel(preferredHydrationRouteMode)}`}
              >
                ⚙ Settings
              </button>
              <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
                {polkadotWalletConnected
                  ? `${hideTableData ? "••••" : (polkadotWalletLabel || "Wallet")}: ${hideTableData ? "••••" : shortenWalletAddress(polkadotWalletState.address)}`
                  : polkadotWalletState?.error
                    ? (hideTableData ? "Wallet not connected" : polkadotWalletState.error)
                    : "Connect SubWallet for Polkadot DEX"}
              </span>
              <span
                title={polkadotPriceStatusDisplay.title}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 6,
                  padding: "4px 7px",
                  borderRadius: 999,
                  border: polkadotPriceStatusDisplay.tone === "ok" ? "1px solid rgba(46,204,113,0.20)" : "1px solid rgba(241,196,15,0.20)",
                  background: polkadotPriceStatusDisplay.tone === "ok" ? "rgba(46,204,113,0.06)" : "rgba(241,196,15,0.06)",
                  color: polkadotPriceStatusDisplay.tone === "ok" ? "#c9f7d7" : "#f7e8b0",
                  fontSize: 11,
                  lineHeight: 1.1,
                  maxWidth: 280,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                <b>Prices</b>
                <span style={{ minWidth: 0, overflow: "hidden", textOverflow: "ellipsis" }}>
                  {hideTableData ? "status ready" : polkadotPriceStatusDisplay.label}
                </span>
              </span>
              <button
                type="button"
                style={{ ...safeButton, padding: "6px 8px", marginLeft: "auto", fontWeight: 800 }}
                onClick={() => {
                  void ensurePolkadotWalletConnected().catch((e) => {
                    const msg = e?.message || "Failed to connect Polkadot wallet.";
                    onToast?.({ kind: "warn", msg });
                    openSubmitResultModal("error", msg, "Polkadot Wallet Connect Failed");
                  });
                }}
                title={polkadotWalletConnected && polkadotWalletState?.address && !hideTableData ? polkadotWalletState.address : "Connect Polkadot wallet"}
              >
                {polkadotWalletConnected ? "Connected" : "Connect"}
              </button>
              <button
                type="button"
                style={{ ...safeButton, padding: "6px 8px" }}
                onClick={() => setPolkadotWalletScanNonce((x) => x + 1)}
                title="Rescan injected Polkadot wallets"
              >
                Rescan
              </button>
            </div>
          )}


          {isPolkadotDexVenue && polkadotSettingsOpen ? (
            <div
              style={{
                marginTop: 6,
                padding: "8px 10px",
                borderRadius: 10,
                border: "1px solid rgba(255,255,255,0.12)",
                background: "rgba(14,17,22,0.98)",
                boxShadow: "0 10px 26px rgba(0,0,0,0.35)",
                fontSize: 12,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, marginBottom: 8 }}>
                <b>Polkadot DEX Settings</b>
                <button type="button" style={{ ...safeButton, padding: "5px 8px" }} onClick={() => setPolkadotSettingsOpen(false)}>Close</button>
              </div>
              <label style={{ display: "grid", gridTemplateColumns: "88px minmax(0, 1fr)", alignItems: "center", gap: 8 }}>
                <span style={{ opacity: 0.82 }}>Route</span>
                <select
                  style={{ ...darkSelectStyle, width: "100%" }}
                  value={preferredHydrationRouteMode}
                  onChange={(e) => setPreferredHydrationRouteModeState(normalizeHydrationRouteMode(e.target.value))}
                  title="Hydration route source. Auto uses manual XYK for configured custom pairs and controlled manual Router fallbacks when available; generic SDK pairs stay blocked unless the backend explicitly enables router quotes."
                >
                  <option value="auto" style={darkOptionStyle}>Auto</option>
                  <option value="sdk" style={darkOptionStyle}>SDK</option>
                  <option value="isolated_helper" style={darkOptionStyle}>Isolated</option>
                  <option value="manual_xyk" style={darkOptionStyle}>Manual XYK</option>
                </select>
              </label>
              <div style={{ ...safeMuted, marginTop: 8, fontSize: 11, lineHeight: 1.25 }}>
                Current route: <b>{hydrationRouteModeLabel(preferredHydrationRouteMode)}</b>. Auto uses manual XYK for configured custom pairs and controlled manual Router fallbacks when the backend exposes one; generic SDK pairs stay blocked unless router quotes are explicitly enabled.
              </div>
            </div>
          ) : null}

          {balErr && (isDexSwapVenue || !String(balErr).includes("429")) && (
            <div style={{ ...safeMuted, fontSize: 11, color: "#ff6b6b", lineHeight: 1.1 }}>
              Bal: {hideTableData ? "Hidden" : balErr}
            </div>
          )}
          {balNotice && (
            <div style={{ ...safeMuted, fontSize: 11, color: "#f2e6b7", lineHeight: 1.15 }}>
              Bal notice: {hideTableData ? "Hidden" : balNotice}
            </div>
          )}
        </div>

        {balanceWarning && (
          <div
            style={{
              marginTop: 6,
              border: "1px solid #3b3413",
              background: "#151208",
              padding: "6px 8px",
              borderRadius: 10,
              color: "#f2e6b7",
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
            }}
          >
            {balanceWarning}
          </div>
        )}

        <div style={{ ...rowTightStyle, marginTop: sectionGap }}>
          {isSolanaLimitMode ? (
            <>
              <div style={safePill}>
                <span>Expiry</span>
                <select style={darkSelectStyle} value={solanaExpiryPreset} onChange={(e) => setSolanaExpiryPreset(e.target.value)}>
                  <option value="never" style={darkOptionStyle}>Never</option>
                  <option value="10m" style={darkOptionStyle}>10m</option>
                  <option value="1h" style={darkOptionStyle}>1h</option>
                  <option value="1d" style={darkOptionStyle}>1d</option>
                  <option value="7d" style={darkOptionStyle}>7d</option>
                  <option value="custom" style={darkOptionStyle}>Custom</option>
                </select>
              </div>

              {String(solanaExpiryPreset || "never").toLowerCase().trim() === "custom" && (
                <div style={safePill}>
                  <span>Custom</span>
                  <input
                    style={{ ...safeInput, width: 190 }}
                    type="datetime-local"
                    value={solanaExpiryCustom}
                    onChange={(e) => setSolanaExpiryCustom(e.target.value)}
                  />
                </div>
              )}
            </>
          ) : (
            <>
              <div style={safePill}>
                <span>TIF</span>
                <select style={darkSelectStyle} value={tif} onChange={(e) => setTif(e.target.value)}>
                  <option value="gtc" style={darkOptionStyle}>GTC</option>
                  <option value="ioc" style={darkOptionStyle}>IOC</option>
                  <option value="fok" style={darkOptionStyle}>FOK</option>
                </select>
              </div>

              <label style={safePill}>
                <input type="checkbox" checked={postOnly} onChange={(e) => setPostOnly(e.target.checked)} />
                <span>Post-only</span>
              </label>

              <div style={safePill}>
                <span>Client OID</span>
                <input
                  style={{ ...safeInput, width: 140 }}
                  value={clientOid}
                  placeholder="optional"
                  onChange={(e) => setClientOid(e.target.value)}
                />
              </div>
            </>
          )}
        </div>

        <div style={{ marginTop: 6, ...safeMuted, fontSize: 12, lineHeight: 1.15 }}>
          Type: <b>{isSolanaJupiterVenue ? (solanaOrderMode === "limit" ? "Limit" : "Swap") : isPolkadotDexVenue ? "Swap" : "Limit"}</b>
          {isSolanaLimitMode ? <> • Expiry: <b>{hideTableData ? "••••" : solanaExpiryLabel}</b></> : null}
          {" "}• Est. Total ({totalLabel}): <b>{notional === null ? "—" : fmtNum ? fmtNum(notional) : String(notional)}</b>
        </div>

        <div style={{ marginTop: 8, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <button
            style={{
              ...safeButton,
              ...(submitting || !canSubmit ? safeButtonDisabled : {}),
              padding: "9px 12px",
              fontWeight: 900,
            }}
            disabled={submitting || !canSubmit}
            onClick={openConfirm}
            title={
              !canSubmitBase
                ? (isSolanaLimitMode ? "Fill symbol, qty, and limit price" : isDexSwapVenue ? "Fill symbol and order amount" : "Fill symbol, qty, and limit price")
                : preTrade?.block
                  ? "Blocked by pre-trade checks"
                  : "Review and confirm order"
            }
          >
            {submitting
              ? "Submitting…"
              : isSolanaLimitMode
                ? side === "buy" ? "Place Buy Limit" : "Place Sell Limit"
                : isDexSwapVenue
                  ? isPolkadotDexVenue
                    ? side === "buy" ? "Sign Swap Buy" : "Sign Swap Sell"
                    : side === "buy" ? "Swap Buy" : "Swap Sell"
                  : side === "buy" ? "Place Buy Limit" : "Place Sell Limit"}
          </button>

          <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
            Endpoint: <code>{submitEndpointLabel}</code>
          </span>

          {hasLastSubmitResult && (
            <button
              type="button"
              style={{ ...safeButton, padding: "7px 10px", opacity: 0.95 }}
              onClick={() =>
                openSubmitResultModal(
                  submitResultKind,
                  submitResultPayload,
                  submitResultKind === "error" ? "Order Submit Failed" : "Order Submitted"
                )
              }
              title="View the last submit result"
            >
              View last result
            </button>
          )}
        </div>

        </div>

        {/* Confirm submit modal (existing) */}
        {showConfirm && (
          <div
            style={{
              position: "fixed",
              inset: 0,
              background: "rgba(0,0,0,0.55)",
              zIndex: 9999,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 16,
            }}
            onMouseDown={() => setShowConfirm(false)}
            role="dialog"
            aria-modal="true"
          >
            <div
              style={{
                width: "min(560px, 94vw)",
                borderRadius: 14,
                border: `1px solid ${sideAccent}`,
                background: "#101010",
                boxShadow: "0 12px 40px rgba(0,0,0,0.5)",
                padding: 14,
              }}
              onMouseDown={(e) => e.stopPropagation()}
            >
              <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 10 }}>
                <div style={{ fontSize: 14, fontWeight: 900 }}>
                  Confirm {side === "buy" ? "BUY" : "SELL"} {isSolanaLimitMode ? "Jupiter Limit Order" : isDexSwapVenue ? "Swap" : "Limit Order"}
                </div>
                <button
                  type="button"
                  onClick={() => setShowConfirm(false)}
                  style={{ ...safeButton, padding: "6px 10px", opacity: 0.9 }}
                >
                  Close
                </button>
              </div>

              {preTrade?.status === "fail" && (
                <div
                  style={{
                    marginTop: 10,
                    borderRadius: 10,
                    padding: "8px 10px",
                    border: "1px solid #4a1f1f",
                    background: "#160b0b",
                    color: "#ffd2d2",
                    fontSize: 11,
                    lineHeight: 1.2,
                  }}
                >
                  This order is blocked by pre-trade checks. Fix the Qty/Limit to match venue increments/minimums.
                </div>
              )}

              <div style={{ marginTop: 10, borderTop: "1px solid #2a2a2a", paddingTop: 10 }}>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "160px 1fr",
                    rowGap: 6,
                    columnGap: 10,
                    fontSize: 12,
                  }}
                >
                  {confirmLines.map((x) => (
                    <div key={x.k} style={{ display: "contents" }}>
                      <div style={{ color: "#a9a9a9" }}>{x.k}</div>
                      <div style={{ color: "#eaeaea", fontWeight: 700 }}>{x.v}</div>
                    </div>
                  ))}
                </div>
              </div>

              <div style={{ marginTop: 12, display: "flex", gap: 10, justifyContent: "flex-end", flexWrap: "wrap" }}>
                <button
                  type="button"
                  onClick={() => setShowConfirm(false)}
                  style={{ ...safeButton, padding: "8px 12px", opacity: 0.95 }}
                >
                  Cancel
                </button>

                <button
                  type="button"
                  onClick={confirmAndSubmit}
                  disabled={submitting || !canSubmit}
                  style={{
                    ...safeButton,
                    ...(submitting || !canSubmit ? safeButtonDisabled : {}),
                    padding: "8px 12px",
                    fontWeight: 900,
                    boxShadow: `0 0 0 1px ${sideAccent} inset`,
                  }}
                >
                  {submitting ? "Submitting…" : isPolkadotDexVenue ? "Confirm & Sign" : "Confirm & Submit"}
                </button>
              </div>

              <div style={{ marginTop: 10, fontSize: 11, color: "#a9a9a9", lineHeight: 1.25 }}>
                {isPolkadotDexVenue ? (
                  <>
                    Confirm builds the Hydration swap payload, opens SubWallet for signing, and submits/finalizes through{" "}
                    <code>{submitEndpointLabel}</code>.
                    {" "}Cancel returns you to the form without signing.
                  </>
                ) : (
                  <>
                    Confirm submits immediately via{" "}
                    <code>{submitEndpointLabel}</code>.
                    {" "}Cancel returns you to the form without submitting.
                  </>
                )}
              </div>
            </div>
          </div>
        )}

        {/* NEW: submission result modal */}
        {showSubmitResult && (
          <div
            style={{
              position: "fixed",
              inset: 0,
              background: "rgba(0,0,0,0.55)",
              zIndex: 10000,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 16,
            }}
            onMouseDown={() => setShowSubmitResult(false)}
            role="dialog"
            aria-modal="true"
          >
            <div
              style={{
                width: "min(720px, 96vw)",
                maxHeight: "min(78vh, 720px)",
                overflow: "hidden",
                borderRadius: 14,
                border: `1px solid ${submitResultKind === "error" ? "#7a2b2b" : "#1f6f3a"}`,
                background: "#101010",
                boxShadow: "0 12px 40px rgba(0,0,0,0.5)",
                padding: 14,
                display: "flex",
                flexDirection: "column",
                gap: 10,
              }}
              onMouseDown={(e) => e.stopPropagation()}
            >
              <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 10 }}>
                <div style={{ fontSize: 14, fontWeight: 900 }}>{submitResultTitle || "Order Submit Result"}</div>
                <button
                  type="button"
                  onClick={() => setShowSubmitResult(false)}
                  style={{ ...safeButton, padding: "6px 10px", opacity: 0.9 }}
                >
                  Close
                </button>
              </div>

              <div
                style={{
                  display: "flex",
                  gap: 10,
                  justifyContent: "space-between",
                  alignItems: "center",
                  flexWrap: "wrap",
                }}
              >
                <div style={{ fontSize: 12, color: submitResultKind === "error" ? "#ffd2d2" : "#cdeccd" }}>
                  {submitResultKind === "error" ? "Status: ERROR" : "Status: OK"}
                </div>

                <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                  <button
                    type="button"
                    onClick={() => {
                      setSubmitResultKind(submitResultKind || null);
                      setSubmitResultPayload(submitResultPayload);
                      copySubmitResultToClipboard();
                    }}
                    style={{ ...safeButton, padding: "7px 10px", opacity: 0.95 }}
                    title="Copy the result text to clipboard"
                    disabled={!HAS_WINDOW || !navigator?.clipboard?.writeText}
                  >
                    Copy
                  </button>

                  <button
                    type="button"
                    onClick={() => setShowSubmitResult(false)}
                    style={{ ...safeButton, padding: "7px 10px", opacity: 0.95 }}
                  >
                    OK
                  </button>
                </div>
              </div>

              <div
                style={{
                  borderTop: "1px solid #2a2a2a",
                  paddingTop: 10,
                  overflow: "auto",
                  flex: 1,
                }}
              >
                <pre
                  style={{
                    margin: 0,
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    fontSize: 11,
                    lineHeight: 1.2,
                    color: submitResultKind === "error" ? "#ffd2d2" : "#cdeccd",
                    background: submitResultKind === "error" ? "#160b0b" : "#0f1a0f",
                    border: submitResultKind === "error" ? "1px solid #4a1f1f" : "1px solid #203a20",
                    borderRadius: 12,
                    padding: 10,
                  }}
                >
                  {submitResultText || (hideTableData ? "Result hidden." : "—")}
                </pre>

                {!hideTableData && submitOk && submitError && (
                  <div style={{ marginTop: 8, ...safeMuted, fontSize: 11 }}>
                    Note: both submitOk and submitError are set. This should not happen; if it does, it indicates a UI state race.
                  </div>
                )}
              </div>

              <div style={{ fontSize: 11, color: "#a9a9a9", lineHeight: 1.25 }}>
                This modal replaces the inline JSON printout previously shown below the Order Ticket.
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}