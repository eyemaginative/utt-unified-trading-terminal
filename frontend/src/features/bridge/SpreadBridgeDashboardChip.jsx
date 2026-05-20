// frontend/src/features/bridge/SpreadBridgeDashboardChip.jsx
import { useEffect, useMemo, useRef, useState } from "react";
import { sharedFetchJSON } from "../../lib/sharedFetch";

const POP_MARGIN = 8;

const SPREAD_CACHE_KEY = "utt_cross_chain_spread_uttt_v1";
const BRIDGE_DASH_CACHE_KEY = "utt_bridge_transfer_dashboard_v1";
const BRIDGE_DASH_POS_KEY = "utt_bridge_transfer_dashboard_pos_v1";

const smallBtnStyle = {
  display: "inline-flex",
  alignItems: "center",
  gap: 8,
  padding: "7px 10px",
  borderRadius: 10,
  background: "var(--utt-hdr-btn-bg, rgba(255,255,255,0.04))",
  border: "1px solid var(--utt-hdr-btn-border, rgba(255,255,255,0.12))",
  color: "var(--utt-hdr-fg, #e8eef8)",
  cursor: "pointer",
  fontSize: 12,
  fontWeight: 800,
  userSelect: "none",
  whiteSpace: "nowrap",
};

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function BridgeToolChip({
  title,
  subLabel,
  isOpen,
  onClick,
  showStatus = true,
  showSubLabel = true,
  minWidth,
}) {
  const base = {
    display: "inline-flex",
    flexDirection: "column",
    alignItems: "flex-start",
    justifyContent: "center",
    gap: 2,
    padding: "8px 12px",
    borderRadius: 999,
    border: "1px solid var(--utt-hdr-pill-border, rgba(255,255,255,0.12))",
    background: "var(--utt-hdr-pill-bg, rgba(255,255,255,0.04))",
    color: "inherit",
    cursor: "pointer",
    userSelect: "none",
    minWidth: minWidth ?? (showStatus || showSubLabel ? 124 : 104),
    maxWidth: 164,
    flexShrink: 1,
  };

  const open = {
    ...base,
    border: "1px solid color-mix(in srgb, var(--utt-hdr-link, #9ad) 55%, transparent)",
    background: "color-mix(in srgb, var(--utt-hdr-link, #9ad) 12%, var(--utt-hdr-pill-bg, rgba(255,255,255,0.04)))",
    boxShadow: "0 0 0 1px color-mix(in srgb, var(--utt-hdr-link, #9ad) 22%, transparent) inset",
  };

  return (
    <button type="button" onClick={onClick} style={isOpen ? open : base} title={`${title} window`}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 8, lineHeight: 1.1 }}>
        <span style={{ fontWeight: 800, fontSize: 13, whiteSpace: "nowrap" }}>{title}</span>
        {showStatus ? (
          <span style={{ fontSize: 11, opacity: 0.75 }}>{isOpen ? "Open" : "Closed"}</span>
        ) : null}
      </div>
      {showSubLabel ? (
        <div style={{ fontSize: 11, opacity: 0.75, maxWidth: "100%", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
          {subLabel || "—"}
        </div>
      ) : null}
    </button>
  );
}

function spreadTrimApiBase(base) {
  return String(base || "").replace(/\/+$/, "");
}

function spreadNum(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function spreadFmtUsd(v, max = 8) {
  const n = spreadNum(v);
  if (n === null) return "—";
  if (n === 0) return "$0";
  if (Math.abs(n) < 0.0001) return `$${n.toFixed(max)}`;
  if (Math.abs(n) < 1) return `$${n.toFixed(6)}`;
  return `$${n.toLocaleString(undefined, { maximumFractionDigits: 4 })}`;
}

function spreadFmtPct(v) {
  const n = spreadNum(v);
  if (n === null) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
}

function spreadFmtQty(v, max = 6) {
  const n = spreadNum(v);
  if (n === null) return "—";
  if (n === 0) return "0";
  if (Math.abs(n) < 1) return n.toLocaleString(undefined, { maximumFractionDigits: max });
  return n.toLocaleString(undefined, { maximumFractionDigits: max });
}

function spreadReadCache() {
  try {
    const raw = localStorage.getItem(SPREAD_CACHE_KEY);
    if (!raw) return null;
    const v = JSON.parse(raw);
    return v && typeof v === "object" ? v : null;
  } catch {
    return null;
  }
}

function spreadWriteCache(v) {
  try {
    localStorage.setItem(SPREAD_CACHE_KEY, JSON.stringify(v || {}));
  } catch {
    // ignore
  }
}

function spreadIsBlockedGenericHydrationOrderbookPath(path) {
  try {
    const u = new URL(String(path || ""), "http://utt.local");
    if (!u.pathname.includes("/api/polkadot_dex/hydration/orderbook")) return false;
    const sym = String(u.searchParams.get("symbol") || "").trim().toUpperCase();
    const routeMode = String(u.searchParams.get("route_mode") || "").trim().toLowerCase();
    // Spread / Bridge may use only the safe UTTT-HDX manual/live route here.
    // Generic Hydration USD pairs are owned by /hydration/prices.
    return !(sym === "UTTT-HDX" && routeMode === "manual_xyk");
  } catch {
    return false;
  }
}

async function spreadFetchJson(base, path, signal, ttlMs = 2500) {
  if (spreadIsBlockedGenericHydrationOrderbookPath(path)) {
    return {
      ok: false,
      error: "blocked_generic_hydration_orderbook_frontend",
      message: "Spread / Bridge blocked a generic Hydration orderbook pricing call. Use /api/polkadot_dex/hydration/prices for USD pricing; only UTTT-HDX manual_xyk is allowed here.",
    };
  }
  const root = spreadTrimApiBase(base);
  const url = `${root}${path}`;
  return await sharedFetchJSON(url, { signal, ttlMs });
}

function spreadFirstLevelPrice(levels) {
  const arr = Array.isArray(levels) ? levels : [];
  for (const lvl of arr) {
    const px = Array.isArray(lvl)
      ? spreadNum(lvl?.[0] ?? lvl?.price)
      : spreadNum(lvl?.price ?? lvl?.px ?? lvl?.rate ?? lvl?.limit ?? lvl?.p);
    if (px !== null && px > 0) return px;
  }
  return null;
}

function spreadOrderbookMid(data) {
  const direct = spreadNum(
    data?.mid ??
      data?.midPrice ??
      data?.mid_price ??
      data?.price ??
      data?.markPrice ??
      data?.mark_price ??
      data?.spotPrice ??
      data?.spot_price ??
      data?.pool?.spotPrice ??
      data?.pool?.spot_price
  );
  if (direct !== null && direct > 0) return direct;

  const bid = spreadNum(data?.bestBid ?? data?.best_bid ?? data?.bid ?? data?.bids?.[0]?.price) ?? spreadFirstLevelPrice(data?.bids);
  const ask = spreadNum(data?.bestAsk ?? data?.best_ask ?? data?.ask ?? data?.asks?.[0]?.price) ?? spreadFirstLevelPrice(data?.asks);
  if (bid !== null && ask !== null && bid > 0 && ask > 0) return (bid + ask) / 2;
  if (bid !== null && bid > 0) return bid;
  if (ask !== null && ask > 0) return ask;
  return null;
}

function spreadPriceFromJupiterResponse(data, id) {
  const key = String(id || "").trim();
  const maps = [data?.items, data?.data, data?.prices, data?.results, data && typeof data === "object" ? data : null];
  for (const m of maps) {
    if (!m || typeof m !== "object") continue;
    const entry = m?.[key] || m?.[key.toLowerCase?.() || key];
    const val = spreadNum(
      typeof entry === "object"
        ? entry?.price ?? entry?.priceUsd ?? entry?.usdPrice ?? entry?.usd_price ?? entry?.value ?? entry?.usd
        : entry
    );
    if (val !== null && val > 0) return val;
  }
  return null;
}

async function spreadResolveSolanaUtttMint(base, signal) {
  try {
    const r = await spreadFetchJson(base, "/api/solana_dex/resolve?asset=UTTT", signal, 10_000);
    const mint = String(r?.mint || r?.address || r?.token || r?.tokenMint || "").trim();
    if (mint) return mint;
  } catch {
    // fall through
  }

  const urls = ["/api/token_registry?chain=solana", "/api/token_registry?network=solana", "/api/token_registry"];
  for (const u of urls) {
    try {
      const data = await spreadFetchJson(base, u, signal, 10_000);
      const items = Array.isArray(data) ? data : data?.items || data?.mappings || data?.tokens || [];
      for (const it of items || []) {
        const sym = String(it?.symbol || it?.asset || it?.ticker || "").trim().toUpperCase();
        const venue = String(it?.venue || it?.venue_override || it?.venueOverride || "").trim().toLowerCase();
        const chain = String(it?.chain || it?.network || "").trim().toLowerCase();
        if (sym !== "UTTT") continue;
        if (venue && !venue.startsWith("solana")) continue;
        if (chain && chain !== "solana") continue;
        const mint = String(it?.mint || it?.address || it?.mint_address || it?.mintAddress || it?.addr || "").trim();
        if (mint) return mint;
      }
    } catch {
      // keep trying
    }
  }
  return "";
}

async function spreadFetchSolanaUtttUsd(base, signal) {
  const mint = await spreadResolveSolanaUtttMint(base, signal);
  if (!mint) return { price: null, source: "solana:missing-mint", mint: "" };
  try {
    const data = await spreadFetchJson(base, `/api/solana_dex/jupiter/prices?ids=${encodeURIComponent(mint)}`, signal, 5000);
    const price = spreadPriceFromJupiterResponse(data, mint);
    return { price, source: price ? "solana:jupiter" : "solana:jupiter:no-price", mint };
  } catch (e) {
    return { price: null, source: "solana:jupiter:error", mint, error: String(e?.message || e) };
  }
}

async function spreadFetchHydrationMid(base, symbol, signal) {
  const sym = String(symbol || "").trim().toUpperCase();
  // Spread/Bridge only needs the safe manual UTTT-HDX pool route here. Do not
  // use this helper for generic Hydration USD pairs while SDK router quotes are
  // disabled.
  if (sym !== "UTTT-HDX") return { mid: null, data: null, skipped: "non-manual-hydration-pair" };
  try {
    const data = await spreadFetchJson(
      base,
      `/api/polkadot_dex/hydration/orderbook?symbol=${encodeURIComponent(sym)}&depth=5&route_mode=manual_xyk`,
      signal,
      5000
    );
    if (data?.ok === false) return { mid: null, data };
    return { mid: spreadOrderbookMid(data), data };
  } catch (e) {
    return { mid: null, data: null, error: String(e?.message || e) };
  }
}

async function spreadFetchHydrationUsdPrices(base, signal, refresh = true) {
  const suffix = refresh ? "&refresh=true" : "";
  try {
    const data = await spreadFetchJson(
      base,
      `/api/polkadot_dex/hydration/prices?assets=HDX,DOT,USDT,UTTT,HOLLAR${suffix}`,
      signal,
      9000
    );
    const rawPrices = data?.prices_usd || data?.usd_prices || data?.prices || data?.priceMap || data?.price_map || {};
    const rawSources = data?.priceSources || data?.price_sources || data?.sources || {};
    const prices = {};
    const sources = {};
    for (const [k, v] of Object.entries(rawPrices || {})) {
      const sym = String(k || "").trim().toUpperCase();
      const px = spreadNum(v && typeof v === "object" ? (v.px_usd ?? v.usd_price ?? v.priceUsd ?? v.usdPrice ?? v.price ?? v.usd) : v);
      if (sym && px !== null && px > 0) {
        prices[sym] = px;
        sources[sym] = rawSources?.[sym] || rawSources?.[k] || data?.status || "hydration:price-cache";
      }
    }
    return { prices, sources, data };
  } catch (e) {
    return { prices: {}, sources: {}, data: null, error: String(e?.message || e) };
  }
}

async function spreadFetchHydrationUtttUsd(base, signal) {
  // Do not call generic Hydration orderbook pairs such as HDX-USDT here.
  // The backend /hydration/prices endpoint owns controlled SDK pricing with TTL/backoff guards.
  const priceData = await spreadFetchHydrationUsdPrices(base, signal, true);
  const hdxUsd = spreadNum(priceData?.prices?.HDX);
  const cachedUtttUsd = spreadNum(priceData?.prices?.UTTT);
  const hdxSource = priceData?.sources?.HDX || "hydration:price-cache:HDX";

  const utttHdx = await spreadFetchHydrationMid(base, "UTTT-HDX", signal);
  const utttHdxPx = utttHdx.mid;
  const derivedUtttUsd = hdxUsd > 0 && utttHdxPx > 0 ? utttHdxPx * hdxUsd : null;
  const price = cachedUtttUsd > 0 ? cachedUtttUsd : derivedUtttUsd;

  const pool = utttHdx.data?.pool || utttHdx.data?.meta?.pool || {};
  const baseReserve = spreadNum(pool?.baseReserve ?? pool?.base_reserve ?? pool?.base?.reserve ?? pool?.reserves?.base);
  const quoteReserve = spreadNum(pool?.quoteReserve ?? pool?.quote_reserve ?? pool?.quote?.reserve ?? pool?.reserves?.quote);
  const poolSource = String(pool?.source || utttHdx.data?.source || "").trim();
  const poolAccount = String(pool?.poolAccount || pool?.pool_account || pool?.account || "").trim();
  const tvlUsd = price > 0 && hdxUsd > 0 && baseReserve !== null && quoteReserve !== null
    ? baseReserve * price + quoteReserve * hdxUsd
    : null;

  return {
    price,
    source: price ? (priceData?.sources?.UTTT || "hydration:UTTT-HDX×HDX-USD") : "hydration:missing-price",
    hdxUsd,
    hdxSource,
    utttHdx: utttHdxPx,
    priceCacheStatus: priceData?.data?.status || null,
    priceCacheError: priceData?.error || priceData?.data?.cache?.last_error || null,
    poolSource,
    poolAccount,
    baseReserve,
    quoteReserve,
    tvlUsd,
  };
}

async function spreadFetchSnapshot(apiBase, signal) {
  const [sol, hyd] = await Promise.all([
    spreadFetchSolanaUtttUsd(apiBase, signal),
    spreadFetchHydrationUtttUsd(apiBase, signal),
  ]);

  const solPrice = spreadNum(sol?.price);
  const hydPrice = spreadNum(hyd?.price);
  const spreadPct = solPrice > 0 && hydPrice > 0 ? ((hydPrice - solPrice) / solPrice) * 100 : null;
  const absSpreadUsd = solPrice !== null && hydPrice !== null ? hydPrice - solPrice : null;
  const lowTvl = hyd?.tvlUsd !== null && hyd?.tvlUsd !== undefined ? Number(hyd.tvlUsd) < 10_000 : false;

  return {
    ok: solPrice > 0 && hydPrice > 0,
    sol,
    hyd,
    solPrice,
    hydPrice,
    spreadPct,
    absSpreadUsd,
    lowTvl,
    at: new Date().toISOString(),
  };
}

function bridgeReadPanelPos() {
  try {
    const raw = localStorage.getItem(BRIDGE_DASH_POS_KEY);
    if (!raw) return null;
    const v = JSON.parse(raw);
    const x = Number(v?.x);
    const y = Number(v?.y);
    const w = Number(v?.w);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
    return { x, y, w: Number.isFinite(w) && w > 0 ? w : 620 };
  } catch {
    return null;
  }
}

function bridgeWritePanelPos(v) {
  try {
    if (!v || typeof v !== "object") return;
    localStorage.setItem(BRIDGE_DASH_POS_KEY, JSON.stringify({ x: v.x, y: v.y, w: v.w }));
  } catch {
    // ignore
  }
}

function bridgeShortAddress(v, left = 6, right = 5) {
  const s = String(v || "").trim();
  if (!s) return "—";
  if (s.length <= left + right + 3) return s;
  return `${s.slice(0, left)}…${s.slice(-right)}`;
}

function bridgeReadDraft() {
  try {
    const raw = localStorage.getItem(BRIDGE_DASH_CACHE_KEY);
    if (!raw) return null;
    const v = JSON.parse(raw);
    return v && typeof v === "object" ? v : null;
  } catch {
    return null;
  }
}

function bridgeWriteDraft(v) {
  try {
    localStorage.setItem(BRIDGE_DASH_CACHE_KEY, JSON.stringify(v || {}));
  } catch {
    // ignore
  }
}

async function bridgeFetchWalletAddresses(base, signal) {
  const tryUrls = [
    "/api/wallet_addresses?limit=500",
    "/api/wallet_addresses?asset=&network=&limit=500",
  ];

  for (const u of tryUrls) {
    try {
      const data = await spreadFetchJson(base, u, signal, 10_000);
      const items = Array.isArray(data)
        ? data
        : data?.items || data?.rows || data?.wallet_addresses || data?.addresses || data?.data || [];
      if (Array.isArray(items)) return items;
    } catch {
      // try next shape
    }
  }
  return [];
}

async function bridgeFetchUtttSupply(base, signal) {
  try {
    const data = await spreadFetchJson(base, "/api/bridge/uttt_supply?asset=UTTT", signal, 10_000);
    return data && typeof data === "object" ? data : { ok: false, error: "bridge_supply_unexpected_response" };
  } catch (e) {
    return { ok: false, error: String(e?.message || e || "UTTT supply unavailable") };
  }
}


async function bridgeFetchTransferRecordStatus(base, signal) {
  try {
    const data = await spreadFetchJson(base, "/api/bridge/transfer_records/status", signal, 10_000);
    return data && typeof data === "object" ? data : { ok: false, support: "missing", error: "bridge_transfer_status_unexpected_response" };
  } catch (e) {
    return { ok: false, support: "missing", error: String(e?.message || e || "Transfer record status unavailable") };
  }
}

function bridgeApiErrorMessage(data, fallback) {
  const detail = data?.detail ?? data?.error ?? data?.message;
  if (!detail) return fallback;
  if (typeof detail === "string") return detail;
  if (typeof detail?.message === "string") return detail.message;
  if (typeof detail?.error === "string") return detail.error;
  try {
    return JSON.stringify(detail);
  } catch {
    return fallback;
  }
}

async function bridgePostTransferRecordPreview(base, payload, signal) {
  const root = spreadTrimApiBase(base);
  if (!root) throw new Error("API base is not configured.");
  const res = await fetch(`${root}/api/bridge/transfer_records/preview`, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify(payload || {}),
    signal,
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  if (!res.ok) {
    throw new Error(bridgeApiErrorMessage(data, `Transfer-record preview failed (${res.status})`));
  }
  if (!data || typeof data !== "object") {
    throw new Error("Transfer-record preview returned an unexpected response.");
  }
  return data;
}

async function bridgePostTransferRecordCreate(base, payload, signal) {
  const root = spreadTrimApiBase(base);
  if (!root) throw new Error("API base is not configured.");
  const res = await fetch(`${root}/api/bridge/transfer_records`, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ ...(payload || {}), create_from_preview: true }),
    signal,
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  if (!res.ok) {
    throw new Error(bridgeApiErrorMessage(data, `Transfer-record create failed (${res.status})`));
  }
  if (!data || typeof data !== "object") {
    throw new Error("Transfer-record create returned an unexpected response.");
  }
  return data;
}

async function bridgePostTransferRecordLink(base, recordId, kind, payload, signal) {
  const root = spreadTrimApiBase(base);
  const rid = String(recordId || "").trim();
  if (!root) throw new Error("API base is not configured.");
  if (!rid) throw new Error("Create a planned transfer record before linking.");
  const suffix = kind === "source" ? "link_source" : kind === "destination" ? "link_destination" : "reconcile";
  const res = await fetch(`${root}/api/bridge/transfer_records/${encodeURIComponent(rid)}/${suffix}`, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify(payload || {}),
    signal,
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  if (!res.ok) {
    throw new Error(bridgeApiErrorMessage(data, `Transfer-record ${suffix} failed (${res.status})`));
  }
  if (!data || typeof data !== "object") {
    throw new Error(`Transfer-record ${suffix} returned an unexpected response.`);
  }
  return data;
}

async function bridgeGetTransferRecordBasisPreview(base, recordId, signal) {
  const root = spreadTrimApiBase(base);
  const rid = String(recordId || "").trim();
  if (!root) throw new Error("API base is not configured.");
  if (!rid) throw new Error("Create a planned transfer record before previewing basis treatment.");
  const res = await fetch(`${root}/api/bridge/transfer_records/${encodeURIComponent(rid)}/basis_preview`, {
    method: "GET",
    headers: { accept: "application/json" },
    signal,
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  if (!res.ok) {
    throw new Error(bridgeApiErrorMessage(data, `Transfer-record basis preview failed (${res.status})`));
  }
  if (!data || typeof data !== "object") {
    throw new Error("Transfer-record basis preview returned an unexpected response.");
  }
  return data;
}

async function bridgePostTransferRecordApplyBasisPreview(base, recordId, signal) {
  const root = spreadTrimApiBase(base);
  const rid = String(recordId || "").trim();
  if (!root) throw new Error("API base is not configured.");
  if (!rid) throw new Error("Create a planned transfer record before previewing basis-transfer apply.");
  const res = await fetch(`${root}/api/bridge/transfer_records/${encodeURIComponent(rid)}/apply_basis_transfer_preview`, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ note: "UI read-only apply basis transfer preview." }),
    signal,
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  if (!res.ok) {
    throw new Error(bridgeApiErrorMessage(data, `Apply-basis-transfer preview failed (${res.status})`));
  }
  if (!data || typeof data !== "object") {
    throw new Error("Apply-basis-transfer preview returned an unexpected response.");
  }
  return data;
}

function bridgeFirstCloseCandidateId(v) {
  const ids = Array.isArray(v?.closeAmountIds) ? v.closeAmountIds : [];
  return ids.find((id) => String(id || "").trim()) || "";
}

function bridgeCandidateSummary(v) {
  if (!v || typeof v !== "object") return "missing";
  const count = v.count;
  const table = v.table ? `${v.table}: ` : "";
  if (count === null || count === undefined) return `${table}unknown`;
  const close = Array.isArray(v.closeAmountIds) ? v.closeAmountIds.length : 0;
  return `${table}${count} cached${close ? ` · ${close} close amount` : ""}`;
}

function bridgeBasisStatusLabel(v) {
  const s = String(v || "").trim();
  if (!s) return "unknown";
  return s.replace(/_/g, " ");
}

function bridgeBasisQtySummary(fifo, asset) {
  if (!fifo || typeof fifo !== "object") return "not previewed";
  const selected = spreadNum(fifo.selectedQty);
  const required = spreadNum(fifo.requiredQty);
  const available = spreadNum(fifo.availableQty);
  const short = spreadNum(fifo.quantityShortfall);
  const a = String(asset || fifo.asset || "UTTT").trim().toUpperCase() || "UTTT";
  if (required === null) return "unknown";
  const parts = [];
  parts.push(`${spreadFmtQty(selected || 0)} / ${spreadFmtQty(required)} ${a}`);
  if (available !== null) parts.push(`${spreadFmtQty(available)} available`);
  if (short !== null && short > 0) parts.push(`${spreadFmtQty(short)} short`);
  return parts.join(" · ");
}

function bridgeBasisUsdSummary(v) {
  const n = spreadNum(v);
  if (n === null) return "missing / needs review";
  return spreadFmtUsd(n);
}

function bridgePreviewStatusLabel(status) {
  const s = String(status || "").trim();
  return s || "unknown";
}

function bridgeIsHydrationAddressRow(row) {
  const venue = String(row?.venue || row?.wallet_id || row?.walletId || row?.source || "").trim().toLowerCase();
  const network = String(row?.network || row?.chain || row?.chain_id || row?.chainId || "").trim().toLowerCase();
  return venue === "polkadot_hydration" || venue === "hydration" || network === "hydration";
}

function bridgeIsSolanaAddressRow(row) {
  const venue = String(row?.venue || row?.wallet_id || row?.walletId || row?.source || "").trim().toLowerCase();
  const network = String(row?.network || row?.chain || row?.chain_id || row?.chainId || "").trim().toLowerCase();
  return venue.startsWith("solana") || network === "solana";
}

function bridgePickAddress(rows, kind) {
  const arr = Array.isArray(rows) ? rows : [];
  const matches = arr.filter((row) => (kind === "hydration" ? bridgeIsHydrationAddressRow(row) : bridgeIsSolanaAddressRow(row)));
  const preferred = matches.find((row) => String(row?.asset || row?.symbol || "").trim().toUpperCase() === "ALL") || matches[0];
  const address = String(preferred?.address || preferred?.wallet_address || preferred?.pubkey || preferred?.owner || "").trim();
  return {
    row: preferred || null,
    address,
    label: String(preferred?.label || preferred?.name || "").trim(),
    count: matches.length,
  };
}

function bridgeSupplyRows(supply) {
  return Array.isArray(supply?.chains) ? supply.chains : [];
}

function bridgeFindSupplyRow(supply, aliases) {
  const keys = new Set((aliases || []).map((x) => String(x || "").trim().toLowerCase()).filter(Boolean));
  for (const row of bridgeSupplyRows(supply)) {
    const chain = String(row?.chain || "").trim().toLowerCase();
    const venue = String(row?.venue || "").trim().toLowerCase();
    const label = String(row?.label || "").trim().toLowerCase();
    if (keys.has(chain) || keys.has(venue) || keys.has(label)) return row;
  }
  return null;
}

function bridgeSupplyAmount(row) {
  return spreadNum(row?.supply ?? row?.amount ?? row?.qty ?? row?.quantity ?? row?.totalSupply ?? row?.total_supply);
}

function bridgeSupplySource(row) {
  return String(row?.source || row?.status || "").trim();
}

function bridgeSupplyStatus(supply) {
  if (!supply) return "missing";
  if (supply.ok) return "ready";
  return "partial";
}

function bridgeTransferSupportStatus(transferStatus) {
  if (!transferStatus) return "missing";
  if (transferStatus?.ok && transferStatus?.support) return String(transferStatus.support);
  return "missing";
}

function bridgeSupplyPct(row, supply) {
  const qty = bridgeSupplyAmount(row);
  const total = spreadNum(supply?.totalCanonicalSupply ?? supply?.totalSupply ?? supply?.totalConicalSupply);
  if (qty === null || !(total > 0)) return null;
  return (qty / total) * 100;
}

export default function SpreadBridgeDashboardChip({ apiBase, hideTableData = false }) {
  const draft = useMemo(() => (typeof window === "undefined" ? null : bridgeReadDraft()), []);
  const [open, setOpen] = useState(false);
  const [direction, setDirection] = useState(() => draft?.direction || "sol_to_hyd");
  const [asset, setAsset] = useState(() => draft?.asset || "UTTT");
  const [amount, setAmount] = useState(() => draft?.amount || "");
  const [snap, setSnap] = useState(() => (typeof window === "undefined" ? null : spreadReadCache()));
  const [supply, setSupply] = useState(null);
  const [transferStatus, setTransferStatus] = useState(null);
  const [transferPreview, setTransferPreview] = useState(null);
  const [transferPreviewBusy, setTransferPreviewBusy] = useState(false);
  const [transferPreviewErr, setTransferPreviewErr] = useState("");
  const [transferCreateResult, setTransferCreateResult] = useState(null);
  const [transferCreateBusy, setTransferCreateBusy] = useState(false);
  const [transferCreateErr, setTransferCreateErr] = useState("");
  const [transferLinkResult, setTransferLinkResult] = useState(null);
  const [transferLinkBusy, setTransferLinkBusy] = useState("");
  const [transferLinkErr, setTransferLinkErr] = useState("");
  const [basisPreview, setBasisPreview] = useState(null);
  const [basisPreviewBusy, setBasisPreviewBusy] = useState(false);
  const [basisPreviewErr, setBasisPreviewErr] = useState("");
  const [basisApplyPreview, setBasisApplyPreview] = useState(null);
  const [basisApplyPreviewBusy, setBasisApplyPreviewBusy] = useState(false);
  const [basisApplyPreviewErr, setBasisApplyPreviewErr] = useState("");
  const [transferSourceTxid, setTransferSourceTxid] = useState("");
  const [transferDestinationTxid, setTransferDestinationTxid] = useState("");
  const [addresses, setAddresses] = useState([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const abortRef = useRef(null);
  const previewAbortRef = useRef(null);
  const createAbortRef = useRef(null);
  const linkAbortRef = useRef(null);
  const basisAbortRef = useRef(null);
  const basisApplyAbortRef = useRef(null);
  const chipRef = useRef(null);
  const panelRef = useRef(null);
  const dragRef = useRef(null);
  const [panelPos, setPanelPos] = useState(() => (typeof window === "undefined" ? null : bridgeReadPanelPos()));

  useEffect(() => {
    bridgeWriteDraft({ direction, asset, amount });
  }, [direction, asset, amount]);

  useEffect(() => {
    setTransferPreview(null);
    setTransferPreviewErr("");
    setTransferCreateResult(null);
    setTransferCreateErr("");
    setTransferLinkResult(null);
    setTransferLinkErr("");
    setBasisPreview(null);
    setBasisPreviewErr("");
    setBasisApplyPreview(null);
    setBasisApplyPreviewErr("");
    setTransferSourceTxid("");
    setTransferDestinationTxid("");
  }, [direction, asset, amount]);

  const refresh = async () => {
    const base = spreadTrimApiBase(apiBase);
    if (!base || busy) return;
    try {
      abortRef.current?.abort?.();
    } catch {
      // ignore
    }
    const controller = new AbortController();
    abortRef.current = controller;
    setBusy(true);
    setErr("");
    try {
      const [nextSnap, rows, nextSupply, nextTransferStatus] = await Promise.all([
        spreadFetchSnapshot(base, controller.signal).catch((e) => ({ ok: false, error: String(e?.message || e) })),
        bridgeFetchWalletAddresses(base, controller.signal),
        bridgeFetchUtttSupply(base, controller.signal),
        bridgeFetchTransferRecordStatus(base, controller.signal),
      ]);
      setSnap(nextSnap);
      setAddresses(rows);
      setSupply(nextSupply);
      setTransferStatus(nextTransferStatus);
      if (nextSnap?.ok) spreadWriteCache(nextSnap);
      if (nextSnap?.error) setErr(nextSnap.error);
    } catch (e) {
      if (controller.signal?.aborted) return;
      setErr(String(e?.message || e || "Bridge dashboard refresh failed"));
    } finally {
      if (!controller.signal?.aborted) setBusy(false);
    }
  };

  useEffect(() => {
    if (!open) return undefined;
    refresh();
    return () => {
      try { abortRef.current?.abort?.(); } catch { /* ignore */ }
      try { previewAbortRef.current?.abort?.(); } catch { /* ignore */ }
      try { createAbortRef.current?.abort?.(); } catch { /* ignore */ }
      try { linkAbortRef.current?.abort?.(); } catch { /* ignore */ }
      try { basisAbortRef.current?.abort?.(); } catch { /* ignore */ }
      try { basisApplyAbortRef.current?.abort?.(); } catch { /* ignore */ }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, apiBase]);

  const bridgeClampPanelPos = (next) => {
    if (typeof window === "undefined") return next || { x: POP_MARGIN, y: 120, w: 660 };
    const vw = Math.max(320, window.innerWidth || 0);
    const vh = Math.max(320, window.innerHeight || 0);
    const w = clamp(Number(next?.w) || 660, 360, Math.max(360, vw - POP_MARGIN * 2));
    const maxX = Math.max(POP_MARGIN, vw - w - POP_MARGIN);
    const maxY = Math.max(POP_MARGIN, vh - 80);
    return {
      x: clamp(Number(next?.x) || POP_MARGIN, POP_MARGIN, maxX),
      y: clamp(Number(next?.y) || 120, POP_MARGIN, maxY),
      w,
    };
  };

  const placeBridgeNearChip = () => {
    if (typeof window === "undefined") return;
    const btn = chipRef.current;
    const vw = Math.max(320, window.innerWidth || 0);
    const w = Math.min(660, Math.max(360, vw - POP_MARGIN * 2));
    const rect = btn?.getBoundingClientRect?.();
    const x = rect ? rect.left : POP_MARGIN;
    const y = rect ? rect.bottom + 10 : 120;
    setPanelPos((prev) => {
      const next = bridgeClampPanelPos({ x, y, w: prev?.w || w });
      bridgeWritePanelPos(next);
      return next;
    });
  };

  useEffect(() => {
    if (!open) return undefined;
    if (!panelPos) placeBridgeNearChip();

    const onResize = () => {
      setPanelPos((prev) => {
        const next = bridgeClampPanelPos(prev || bridgeReadPanelPos() || { x: POP_MARGIN, y: 120, w: 660 });
        bridgeWritePanelPos(next);
        return next;
      });
    };
    const onKey = (e) => {
      if (e.key === "Escape") setOpen(false);
    };

    window.addEventListener("resize", onResize);
    document.addEventListener("keydown", onKey, true);
    return () => {
      window.removeEventListener("resize", onResize);
      document.removeEventListener("keydown", onKey, true);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, panelPos]);

  const startBridgeDrag = (e) => {
    if (e.button !== 0) return;
    const t = e.target;
    const interactive = t?.closest?.("button, a, input, select, textarea, label");
    if (interactive) return;

    const current = panelPos || bridgeClampPanelPos(bridgeReadPanelPos() || { x: POP_MARGIN, y: 120, w: 660 });
    dragRef.current = { mx: e.clientX, my: e.clientY, x: current.x, y: current.y, w: current.w };

    const prevUserSelect = document.body.style.userSelect;
    const prevCursor = document.body.style.cursor;
    document.body.style.userSelect = "none";
    document.body.style.cursor = "grabbing";

    const onMove = (ev) => {
      const d = dragRef.current;
      if (!d) return;
      const next = bridgeClampPanelPos({ x: d.x + ev.clientX - d.mx, y: d.y + ev.clientY - d.my, w: d.w });
      setPanelPos(next);
    };

    const onUp = () => {
      const d = dragRef.current;
      dragRef.current = null;
      document.body.style.userSelect = prevUserSelect;
      document.body.style.cursor = prevCursor;
      setPanelPos((prev) => {
        const next = bridgeClampPanelPos(prev || d || { x: POP_MARGIN, y: 120, w: 660 });
        bridgeWritePanelPos(next);
        return next;
      });
      window.removeEventListener("mousemove", onMove, true);
      window.removeEventListener("mouseup", onUp, true);
    };

    window.addEventListener("mousemove", onMove, true);
    window.addEventListener("mouseup", onUp, true);
  };

  const solWallet = useMemo(() => bridgePickAddress(addresses, "solana"), [addresses]);
  const hydWallet = useMemo(() => bridgePickAddress(addresses, "hydration"), [addresses]);
  const solSupply = useMemo(() => bridgeFindSupplyRow(supply, ["solana", "solana_jupiter"]), [supply]);
  const polkaSupply = useMemo(() => bridgeFindSupplyRow(supply, ["polkadot_asset_hub", "asset_hub", "polkadot / asset hub", "polkadot"]), [supply]);
  const hydSupply = useMemo(() => bridgeFindSupplyRow(supply, ["hydration", "polkadot_hydration", "hydration route asset"]), [supply]);

  const source = direction === "sol_to_hyd" ? solWallet : hydWallet;
  const dest = direction === "sol_to_hyd" ? hydWallet : solWallet;
  const sourceLabel = direction === "sol_to_hyd" ? "Solana" : "Hydration";
  const destLabel = direction === "sol_to_hyd" ? "Hydration" : "Solana";
  const sourceSupplyRow = direction === "sol_to_hyd" ? solSupply : polkaSupply;
  const destSupplyRow = direction === "sol_to_hyd" ? polkaSupply : solSupply;

  const qty = spreadNum(amount);
  const solUsd = snap?.solPrice;
  const hydUsd = snap?.hydPrice;
  const sourcePx = direction === "sol_to_hyd" ? solUsd : hydUsd;
  const destPx = direction === "sol_to_hyd" ? hydUsd : solUsd;
  const sourceValue = qty !== null && sourcePx > 0 ? qty * sourcePx : null;
  const destValue = qty !== null && destPx > 0 ? qty * destPx : null;
  const hasBothAddresses = !!source?.address && !!dest?.address;
  const totalSupply = spreadNum(supply?.totalCanonicalSupply ?? supply?.totalSupply ?? supply?.totalConicalSupply);
  const canPreviewTransfer = qty !== null && qty > 0 && !transferPreviewBusy;
  const canCreateTransferRecord = !!transferPreview?.ok && !transferPreviewBusy && !transferCreateBusy && !transferCreateResult?.item?.id;
  const previewPlanned = transferPreview?.plannedRecord || {};
  const previewReadiness = Array.isArray(transferPreview?.readiness) ? transferPreview.readiness : [];
  const previewWarnings = Array.isArray(transferPreview?.warnings) ? transferPreview.warnings : [];
  const createdTransferRecord = transferCreateResult?.item || null;
  const sourceCloseCandidateId = bridgeFirstCloseCandidateId(transferPreview?.candidateLinks?.source);
  const destinationCloseCandidateId = bridgeFirstCloseCandidateId(transferPreview?.candidateLinks?.destination);
  const sourceTxidClean = String(transferSourceTxid || "").trim();
  const destinationTxidClean = String(transferDestinationTxid || "").trim();
  const recordHasSourceLink = !!(createdTransferRecord?.source_withdrawal_id || createdTransferRecord?.source_txid);
  const recordHasDestinationLink = !!(createdTransferRecord?.destination_deposit_id || createdTransferRecord?.destination_txid);
  const canLinkSourceRecord = !!createdTransferRecord?.id && !transferLinkBusy && !!(sourceCloseCandidateId || sourceTxidClean);
  const canLinkDestinationRecord = !!createdTransferRecord?.id && !transferLinkBusy && !!(destinationCloseCandidateId || destinationTxidClean);
  const canReconcileTransferRecord = !!createdTransferRecord?.id && !transferLinkBusy && recordHasSourceLink && recordHasDestinationLink && createdTransferRecord?.status !== "RECONCILED";
  const canPreviewBasisTreatment = !!createdTransferRecord?.id && !basisPreviewBusy;
  const canPreviewBasisApply = !!createdTransferRecord?.id && !!basisPreview?.ok && !basisApplyPreviewBusy;
  const basisReadiness = Array.isArray(basisPreview?.readiness) ? basisPreview.readiness : [];
  const basisWarnings = Array.isArray(basisPreview?.warnings) ? basisPreview.warnings : [];
  const basisApplyReadiness = Array.isArray(basisApplyPreview?.readiness) ? basisApplyPreview.readiness : [];
  const basisApplyWarnings = Array.isArray(basisApplyPreview?.warnings) ? basisApplyPreview.warnings : [];

  const buildTransferRecordPayload = (nextQty) => {
    const sourceChain = direction === "sol_to_hyd" ? "solana" : "hydration";
    const destinationChain = direction === "sol_to_hyd" ? "hydration" : "solana";
    return {
      asset,
      amount: nextQty,
      source_chain: sourceChain,
      destination_chain: destinationChain,
      source_address: source?.address || null,
      destination_address: dest?.address || null,
      source_wallet_id: source?.row?.wallet_id || source?.row?.walletId || null,
      destination_wallet_id: dest?.row?.wallet_id || dest?.row?.walletId || null,
      bridge_mechanism: "manual",
    };
  };

  const handlePreviewTransferRecord = async () => {
    const base = spreadTrimApiBase(apiBase);
    const nextQty = spreadNum(amount);
    if (!base) {
      setTransferPreviewErr("API base is not configured.");
      return;
    }
    if (!(nextQty > 0)) {
      setTransferPreviewErr("Enter a positive UTTT amount before previewing a transfer record.");
      return;
    }

    try {
      previewAbortRef.current?.abort?.();
    } catch {
      // ignore
    }

    const controller = new AbortController();
    previewAbortRef.current = controller;
    setTransferPreviewBusy(true);
    setTransferPreviewErr("");
    try {
      const payload = buildTransferRecordPayload(nextQty);
      const data = await bridgePostTransferRecordPreview(base, payload, controller.signal);
      if (!controller.signal?.aborted) {
        setTransferPreview(data);
        setTransferCreateResult(null);
        setTransferCreateErr("");
        setBasisPreview(null);
        setBasisPreviewErr("");
        setBasisApplyPreview(null);
        setBasisApplyPreviewErr("");
      }
    } catch (e) {
      if (controller.signal?.aborted) return;
      setTransferPreviewErr(String(e?.message || e || "Transfer-record preview failed"));
    } finally {
      if (!controller.signal?.aborted) setTransferPreviewBusy(false);
    }
  };

  const handleCreateTransferRecord = async () => {
    const base = spreadTrimApiBase(apiBase);
    const nextQty = spreadNum(amount);
    if (!base) {
      setTransferCreateErr("API base is not configured.");
      return;
    }
    if (!transferPreview?.ok) {
      setTransferCreateErr("Preview the transfer record before creating a local planned record.");
      return;
    }
    if (!(nextQty > 0)) {
      setTransferCreateErr("Enter a positive UTTT amount before creating a transfer record.");
      return;
    }

    try {
      createAbortRef.current?.abort?.();
    } catch {
      // ignore
    }

    const controller = new AbortController();
    createAbortRef.current = controller;
    setTransferCreateBusy(true);
    setTransferCreateErr("");
    try {
      const payload = buildTransferRecordPayload(nextQty);
      const data = await bridgePostTransferRecordCreate(base, payload, controller.signal);
      if (!controller.signal?.aborted) {
        setTransferCreateResult(data);
        setTransferLinkResult(null);
        setTransferLinkErr("");
        setBasisPreview(null);
        setBasisPreviewErr("");
        setBasisApplyPreview(null);
        setBasisApplyPreviewErr("");
      }
    } catch (e) {
      if (controller.signal?.aborted) return;
      setTransferCreateErr(String(e?.message || e || "Transfer-record create failed"));
    } finally {
      if (!controller.signal?.aborted) setTransferCreateBusy(false);
    }
  };

  const handleLinkTransferRecord = async (kind) => {
    const base = spreadTrimApiBase(apiBase);
    const recordId = createdTransferRecord?.id;
    if (!base) {
      setTransferLinkErr("API base is not configured.");
      return;
    }
    if (!recordId) {
      setTransferLinkErr("Create a planned transfer record before linking.");
      return;
    }

    const isSource = kind === "source";
    const payload = isSource
      ? {
          source_withdrawal_id: sourceCloseCandidateId || null,
          source_txid: sourceTxidClean || null,
        }
      : {
          destination_deposit_id: destinationCloseCandidateId || null,
          destination_txid: destinationTxidClean || null,
        };

    if (isSource && !payload.source_withdrawal_id && !payload.source_txid) {
      setTransferLinkErr("Provide a source txid/signature or wait for a matching source withdrawal candidate.");
      return;
    }
    if (!isSource && !payload.destination_deposit_id && !payload.destination_txid) {
      setTransferLinkErr("Provide a destination txid/hash or wait for a matching destination deposit candidate.");
      return;
    }

    try {
      linkAbortRef.current?.abort?.();
    } catch {
      // ignore
    }

    const controller = new AbortController();
    linkAbortRef.current = controller;
    setTransferLinkBusy(isSource ? "source" : "destination");
    setTransferLinkErr("");
    try {
      const data = await bridgePostTransferRecordLink(base, recordId, kind, payload, controller.signal);
      if (!controller.signal?.aborted) {
        setTransferLinkResult(data);
        if (data?.item) {
          setTransferCreateResult((prev) => ({ ...(prev || {}), item: data.item, execution: data.execution || prev?.execution }));
        }
        setBasisPreview(null);
        setBasisPreviewErr("");
        setBasisApplyPreview(null);
        setBasisApplyPreviewErr("");
      }
    } catch (e) {
      if (controller.signal?.aborted) return;
      setTransferLinkErr(String(e?.message || e || "Transfer-record link failed"));
    } finally {
      if (!controller.signal?.aborted) setTransferLinkBusy("");
    }
  };

  const handleReconcileTransferRecord = async () => {
    const base = spreadTrimApiBase(apiBase);
    const recordId = createdTransferRecord?.id;
    if (!base) {
      setTransferLinkErr("API base is not configured.");
      return;
    }
    if (!recordId) {
      setTransferLinkErr("Create a planned transfer record before reconciling.");
      return;
    }
    if (!recordHasSourceLink || !recordHasDestinationLink) {
      setTransferLinkErr("Link both source and destination evidence before reconciling.");
      return;
    }

    try {
      linkAbortRef.current?.abort?.();
    } catch {
      // ignore
    }

    const controller = new AbortController();
    linkAbortRef.current = controller;
    setTransferLinkBusy("reconcile");
    setTransferLinkErr("");
    try {
      const data = await bridgePostTransferRecordLink(base, recordId, "reconcile", {}, controller.signal);
      if (!controller.signal?.aborted) {
        setTransferLinkResult(data);
        if (data?.item) {
          setTransferCreateResult((prev) => ({ ...(prev || {}), item: data.item, execution: data.execution || prev?.execution }));
        }
        setBasisPreview(null);
        setBasisPreviewErr("");
        setBasisApplyPreview(null);
        setBasisApplyPreviewErr("");
      }
    } catch (e) {
      if (controller.signal?.aborted) return;
      setTransferLinkErr(String(e?.message || e || "Transfer-record reconcile failed"));
    } finally {
      if (!controller.signal?.aborted) setTransferLinkBusy("");
    }
  };

  const handlePreviewBasisTreatment = async () => {
    const base = spreadTrimApiBase(apiBase);
    const recordId = createdTransferRecord?.id;
    if (!base) {
      setBasisPreviewErr("API base is not configured.");
      return;
    }
    if (!recordId) {
      setBasisPreviewErr("Create a planned transfer record before previewing basis treatment.");
      return;
    }

    try {
      basisAbortRef.current?.abort?.();
    } catch {
      // ignore
    }

    const controller = new AbortController();
    basisAbortRef.current = controller;
    setBasisPreviewBusy(true);
    setBasisPreviewErr("");
    try {
      const data = await bridgeGetTransferRecordBasisPreview(base, recordId, controller.signal);
      if (!controller.signal?.aborted) {
        setBasisPreview(data);
        setBasisApplyPreview(null);
        setBasisApplyPreviewErr("");
      }
    } catch (e) {
      if (controller.signal?.aborted) return;
      setBasisPreviewErr(String(e?.message || e || "Transfer-record basis preview failed"));
    } finally {
      if (!controller.signal?.aborted) setBasisPreviewBusy(false);
    }
  };

  const handlePreviewBasisApply = async () => {
    const base = spreadTrimApiBase(apiBase);
    const recordId = createdTransferRecord?.id;
    if (!base) {
      setBasisApplyPreviewErr("API base is not configured.");
      return;
    }
    if (!recordId) {
      setBasisApplyPreviewErr("Create a planned transfer record before previewing basis-transfer apply.");
      return;
    }
    if (!basisPreview?.ok) {
      setBasisApplyPreviewErr("Run Basis Preview before Apply Preview.");
      return;
    }

    try {
      basisApplyAbortRef.current?.abort?.();
    } catch {
      // ignore
    }

    const controller = new AbortController();
    basisApplyAbortRef.current = controller;
    setBasisApplyPreviewBusy(true);
    setBasisApplyPreviewErr("");
    try {
      const data = await bridgePostTransferRecordApplyBasisPreview(base, recordId, controller.signal);
      if (!controller.signal?.aborted) {
        setBasisApplyPreview(data);
      }
    } catch (e) {
      if (controller.signal?.aborted) return;
      setBasisApplyPreviewErr(String(e?.message || e || "Apply-basis-transfer preview failed"));
    } finally {
      if (!controller.signal?.aborted) setBasisApplyPreviewBusy(false);
    }
  };

  const sub = hideTableData
    ? "••••"
    : snap?.spreadPct !== null && snap?.spreadPct !== undefined
      ? `Hyd-Sol ${spreadFmtPct(snap.spreadPct)}`
      : hasBothAddresses
        ? `${sourceLabel} → ${destLabel}`
        : "Sol ↔ Hydration";

  const rowStyle = { display: "grid", gridTemplateColumns: "155px 1fr", gap: 10, alignItems: "baseline", fontSize: 12, lineHeight: 1.45 };
  const labelStyle = { opacity: 0.68 };
  const valueStyle = { fontWeight: 800, fontVariantNumeric: "tabular-nums" };
  const inputStyle = {
    width: "100%",
    padding: "8px 9px",
    borderRadius: 10,
    border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.14))",
    background: "rgba(0,0,0,0.42)",
    color: "var(--utt-hdr-fg, #e8eef8)",
    fontWeight: 800,
    outline: "none",
    colorScheme: "dark",
  };
  const optionStyle = {
    background: "var(--utt-hdr-bg, #0d1117)",
    color: "var(--utt-hdr-fg, #e8eef8)",
  };
  const panelCardStyle = {
    padding: 9,
    borderRadius: 10,
    border: "1px solid rgba(255,255,255,0.12)",
    background: "rgba(255,255,255,0.04)",
    fontSize: 12,
  };

  return (
    <div ref={chipRef} style={{ position: "relative", display: "inline-block" }}>
      <BridgeToolChip
        title="Spread / Bridge"
        subLabel={sub}
        isOpen={open}
        onClick={() => setOpen((v) => !v)}
        showStatus={true}
        showSubLabel={true}
        minWidth={150}
      />
      {open ? (
        <div
          ref={panelRef}
          style={{
            position: "fixed",
            top: panelPos?.y ?? 120,
            left: panelPos?.x ?? POP_MARGIN,
            width: panelPos?.w ?? 660,
            maxWidth: "calc(100vw - 16px)",
            maxHeight: "min(720px, calc(100vh - 32px))",
            zIndex: 9999,
            padding: 0,
            borderRadius: 14,
            border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.14))",
            background: "var(--utt-hdr-bg, rgba(10,14,20,0.98))",
            color: "var(--utt-hdr-fg, #e8eef8)",
            boxShadow: "0 18px 50px rgba(0,0,0,0.55)",
            overflow: "hidden",
          }}
        >
          <div
            onMouseDown={startBridgeDrag}
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 10,
              padding: "10px 12px",
              borderBottom: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
              cursor: "grab",
              userSelect: "none",
            }}
            title="Drag to move"
          >
            <div>
              <div style={{ fontSize: 13, fontWeight: 900 }}>Spread / Bridge Dashboard</div>
              <div style={{ fontSize: 11, opacity: 0.68 }}>Unified cross-chain spread context plus transfer planning — no automated bridge execution is wired yet.</div>
            </div>
            <div style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <button type="button" onClick={refresh} disabled={busy} style={smallBtnStyle}>
                {busy ? "Refreshing…" : "Refresh"}
              </button>
              <button type="button" onClick={() => setOpen(false)} style={smallBtnStyle}>
                Close
              </button>
            </div>
          </div>

          <div
            style={{
              padding: 12,
              maxHeight: "calc(min(720px, calc(100vh - 32px)) - 58px)",
              overflowY: "auto",
              overflowX: "hidden",
              overscrollBehavior: "contain",
              scrollbarWidth: "thin",
            }}
          >
            <div style={{ display: "grid", gridTemplateColumns: "1.2fr 0.8fr", gap: 8, marginBottom: 10 }}>
              <label style={{ display: "grid", gap: 4, fontSize: 11, opacity: 0.88 }}>
                Direction
                <select value={direction} onChange={(e) => setDirection(e.target.value)} style={inputStyle}>
                  <option value="sol_to_hyd" style={optionStyle}>Solana → Hydration</option>
                  <option value="hyd_to_sol" style={optionStyle}>Hydration → Solana</option>
                </select>
              </label>
              <label style={{ display: "grid", gap: 4, fontSize: 11, opacity: 0.88 }}>
                Asset
                <select value={asset} onChange={(e) => setAsset(e.target.value)} style={inputStyle}>
                  <option value="UTTT" style={optionStyle}>UTTT</option>
                </select>
              </label>
            </div>

            <label style={{ display: "grid", gap: 4, fontSize: 11, opacity: 0.88, marginBottom: 10 }}>
              Amount
              <input
                value={amount}
                onChange={(e) => setAmount(e.target.value)}
                placeholder="optional planning amount"
                inputMode="decimal"
                style={inputStyle}
              />
            </label>

            <div style={{ display: "grid", gap: 6 }}>
              <div style={rowStyle}>
                <span style={labelStyle}>Source</span>
                <span style={valueStyle}>{sourceLabel} · {hideTableData ? "••••" : bridgeShortAddress(source?.address)} {source?.label ? `· ${source.label}` : ""}</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Destination</span>
                <span style={valueStyle}>{destLabel} · {hideTableData ? "••••" : bridgeShortAddress(dest?.address)} {dest?.label ? `· ${dest.label}` : ""}</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Source UTTT/USD</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtUsd(sourcePx)}</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Destination UTTT/USD</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtUsd(destPx)}</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Spread</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtPct(snap?.spreadPct)}</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Source value</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtUsd(sourceValue)}</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Destination value</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtUsd(destValue)}</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Source supply</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtQty(bridgeSupplyAmount(sourceSupplyRow))} UTTT</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Destination supply</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtQty(bridgeSupplyAmount(destSupplyRow))} UTTT</span>
              </div>
            </div>

            <div style={{ ...panelCardStyle, marginTop: 10 }}>
              <div style={{ fontWeight: 900, marginBottom: 6 }}>UTTT Supply</div>
              <div style={rowStyle}>
                <span style={labelStyle}>Total Canonical Supply</span>
                <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtQty(totalSupply)} UTTT</span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Solana</span>
                <span style={valueStyle}>
                  {hideTableData ? "••••" : spreadFmtQty(bridgeSupplyAmount(solSupply))} UTTT
                  {bridgeSupplyPct(solSupply, supply) !== null ? ` · ${spreadFmtPct(bridgeSupplyPct(solSupply, supply))}` : ""}
                </span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Polkadot / Asset Hub</span>
                <span style={valueStyle}>
                  {hideTableData ? "••••" : spreadFmtQty(bridgeSupplyAmount(polkaSupply))} UTTT
                  {bridgeSupplyPct(polkaSupply, supply) !== null ? ` · ${spreadFmtPct(bridgeSupplyPct(polkaSupply, supply))}` : ""}
                </span>
              </div>
              <div style={rowStyle}>
                <span style={labelStyle}>Hydration route asset</span>
                <span style={valueStyle}>
                  {hydSupply?.counted ? (hideTableData ? "••••" : `${spreadFmtQty(bridgeSupplyAmount(hydSupply))} UTTT`) : "metadata / route only"}
                </span>
              </div>
              <div style={{ marginTop: 6, opacity: 0.68, lineHeight: 1.35 }}>
                Hydration is shown as route/liquidity context unless the backend marks it as separately counted, so the Polkadot-side supply is not double-counted.
              </div>
              {supply?.warnings?.length ? (
                <div style={{ marginTop: 6, color: "var(--utt-warn, #f7b955)" }}>
                  {supply.warnings.join(" ")}
                </div>
              ) : null}
            </div>

            <div style={{ marginTop: 10, padding: 9, borderRadius: 10, border: "1px solid rgba(247,185,85,0.35)", background: "rgba(247,185,85,0.10)", fontSize: 12 }}>
              Execution is intentionally disabled here. Next backend work should choose the mechanism: treasury-mediated burn/mint, lock/release, XCM-style transfer, or external bridge integration.
            </div>

            <div style={{ ...panelCardStyle, marginTop: 10 }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, marginBottom: 6 }}>
                <div style={{ fontWeight: 900 }}>Transfer Record Preview</div>
                <div style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                  {transferPreview ? (
                    <button type="button" onClick={() => { setTransferPreview(null); setTransferPreviewErr(""); setTransferCreateResult(null); setTransferCreateErr(""); setTransferLinkResult(null); setTransferLinkErr(""); setBasisPreview(null); setBasisPreviewErr(""); setTransferSourceTxid(""); setTransferDestinationTxid(""); }} style={{ ...smallBtnStyle, padding: "5px 8px", fontSize: 11 }}>
                      Clear
                    </button>
                  ) : null}
                  <button
                    type="button"
                    onClick={handlePreviewTransferRecord}
                    disabled={!canPreviewTransfer}
                    style={{
                      ...smallBtnStyle,
                      padding: "5px 8px",
                      fontSize: 11,
                      opacity: canPreviewTransfer ? 1 : 0.55,
                      cursor: canPreviewTransfer ? "pointer" : "not-allowed",
                    }}
                  >
                    {transferPreviewBusy ? "Previewing…" : "Preview Transfer Record"}
                  </button>
                  <button
                    type="button"
                    onClick={handleCreateTransferRecord}
                    disabled={!canCreateTransferRecord}
                    style={{
                      ...smallBtnStyle,
                      padding: "5px 8px",
                      fontSize: 11,
                      opacity: canCreateTransferRecord ? 1 : 0.55,
                      cursor: canCreateTransferRecord ? "pointer" : "not-allowed",
                    }}
                    title="Create a local PLANNED bridge transfer record only. This does not execute a bridge transaction."
                  >
                    {transferCreateBusy ? "Creating…" : createdTransferRecord?.id ? "Planned Record Created" : "Create Planned Record"}
                  </button>
                  <button
                    type="button"
                    onClick={() => handleLinkTransferRecord("source")}
                    disabled={!canLinkSourceRecord}
                    style={{
                      ...smallBtnStyle,
                      padding: "5px 8px",
                      fontSize: 11,
                      opacity: canLinkSourceRecord ? 1 : 0.55,
                      cursor: canLinkSourceRecord ? "pointer" : "not-allowed",
                    }}
                    title="Link source-side withdrawal/outflow evidence to the local bridge transfer record."
                  >
                    {transferLinkBusy === "source" ? "Linking…" : recordHasSourceLink ? "Source Linked" : "Link Source"}
                  </button>
                  <button
                    type="button"
                    onClick={() => handleLinkTransferRecord("destination")}
                    disabled={!canLinkDestinationRecord}
                    style={{
                      ...smallBtnStyle,
                      padding: "5px 8px",
                      fontSize: 11,
                      opacity: canLinkDestinationRecord ? 1 : 0.55,
                      cursor: canLinkDestinationRecord ? "pointer" : "not-allowed",
                    }}
                    title="Link destination-side deposit/inflow evidence to the local bridge transfer record."
                  >
                    {transferLinkBusy === "destination" ? "Linking…" : recordHasDestinationLink ? "Destination Linked" : "Link Destination"}
                  </button>
                  <button
                    type="button"
                    onClick={handleReconcileTransferRecord}
                    disabled={!canReconcileTransferRecord}
                    style={{
                      ...smallBtnStyle,
                      padding: "5px 8px",
                      fontSize: 11,
                      opacity: canReconcileTransferRecord ? 1 : 0.55,
                      cursor: canReconcileTransferRecord ? "pointer" : "not-allowed",
                    }}
                    title="Mark the local transfer record reconciled after both source and destination evidence are linked."
                  >
                    {transferLinkBusy === "reconcile" ? "Reconciling…" : createdTransferRecord?.status === "RECONCILED" ? "Reconciled" : "Reconcile"}
                  </button>
                  <button
                    type="button"
                    onClick={handlePreviewBasisTreatment}
                    disabled={!canPreviewBasisTreatment}
                    style={{
                      ...smallBtnStyle,
                      padding: "5px 8px",
                      fontSize: 11,
                      opacity: canPreviewBasisTreatment ? 1 : 0.55,
                      cursor: canPreviewBasisTreatment ? "pointer" : "not-allowed",
                    }}
                    title="Read-only basis/tax-treatment preview. This does not mutate ledger/FIFO state."
                  >
                    {basisPreviewBusy ? "Previewing Basis…" : basisPreview?.ok ? "Basis Previewed" : "Basis Preview"}
                  </button>
                  <button
                    type="button"
                    onClick={handlePreviewBasisApply}
                    disabled={!canPreviewBasisApply}
                    style={{
                      ...smallBtnStyle,
                      padding: "5px 8px",
                      fontSize: 11,
                      opacity: canPreviewBasisApply ? 1 : 0.55,
                      cursor: canPreviewBasisApply ? "pointer" : "not-allowed",
                    }}
                    title="Read-only apply-basis-transfer preview. This models future lot/journal changes but does not mutate anything."
                  >
                    {basisApplyPreviewBusy ? "Previewing Apply…" : basisApplyPreview?.ok ? "Apply Previewed" : "Apply Preview"}
                  </button>
                </div>
              </div>
              <div style={{ opacity: 0.68, lineHeight: 1.35, marginBottom: 8 }}>
                Preview is dry-run. Create stores a local PLANNED transfer record. Link/Reconcile update only that local record; no signing, submission, ledger/FIFO mutation, or bridge execution occurs.
              </div>

              {transferPreviewErr ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(255,107,107,0.35)", background: "rgba(255,107,107,0.10)" }}>
                  {transferPreviewErr}
                </div>
              ) : null}

              {transferCreateErr ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(255,107,107,0.35)", background: "rgba(255,107,107,0.10)" }}>
                  {transferCreateErr}
                </div>
              ) : null}

              {transferLinkErr ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(255,107,107,0.35)", background: "rgba(255,107,107,0.10)" }}>
                  {transferLinkErr}
                </div>
              ) : null}

              {basisPreviewErr ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(255,107,107,0.35)", background: "rgba(255,107,107,0.10)" }}>
                  {basisPreviewErr}
                </div>
              ) : null}

              {basisApplyPreviewErr ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(255,107,107,0.35)", background: "rgba(255,107,107,0.10)" }}>
                  {basisApplyPreviewErr}
                </div>
              ) : null}

              {transferLinkResult?.ok ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(53,208,127,0.35)", background: "rgba(53,208,127,0.10)", lineHeight: 1.4 }}>
                  <div style={{ fontWeight: 900 }}>
                    {transferLinkResult.mode === "reconcile" ? "Transfer record reconciled" : transferLinkResult.mode === "link_source" ? "Source evidence linked" : "Destination evidence linked"}
                  </div>
                  <div>Status: {transferLinkResult?.item?.status || "LINKED"}</div>
                  {transferLinkResult?.execution?.message ? <div style={{ opacity: 0.72 }}>{transferLinkResult.execution.message}</div> : null}
                </div>
              ) : null}

              {createdTransferRecord?.id ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(53,208,127,0.35)", background: "rgba(53,208,127,0.10)", lineHeight: 1.4 }}>
                  <div style={{ fontWeight: 900 }}>Local PLANNED transfer record created</div>
                  <div>ID: {hideTableData ? "••••" : bridgeShortAddress(createdTransferRecord.id, 8, 8)}</div>
                  <div>Status: {createdTransferRecord.status || "PLANNED"}</div>
                  <div>Source link: {createdTransferRecord.source_withdrawal_id || createdTransferRecord.source_txid ? "linked" : "missing"}</div>
                  <div>Destination link: {createdTransferRecord.destination_deposit_id || createdTransferRecord.destination_txid ? "linked" : "missing"}</div>
                  {transferCreateResult?.execution?.message ? <div style={{ opacity: 0.72 }}>{transferCreateResult.execution.message}</div> : null}
                </div>
              ) : null}

              {createdTransferRecord?.id ? (
                <div style={{ marginBottom: 8, display: "grid", gap: 6 }}>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                    <label style={{ display: "grid", gap: 4, fontSize: 11, opacity: 0.88 }}>
                      <span>Source txid/signature fallback</span>
                      <input
                        value={transferSourceTxid}
                        onChange={(e) => setTransferSourceTxid(e.target.value)}
                        placeholder={sourceCloseCandidateId ? `Candidate: ${bridgeShortAddress(sourceCloseCandidateId, 8, 8)}` : "optional source txid/signature"}
                        type={hideTableData ? "password" : "text"}
                        style={{ padding: "7px 8px", borderRadius: 8, border: "1px solid rgba(255,255,255,0.14)", background: "rgba(0,0,0,0.18)", color: "inherit", minWidth: 0 }}
                      />
                    </label>
                    <label style={{ display: "grid", gap: 4, fontSize: 11, opacity: 0.88 }}>
                      <span>Destination txid/hash fallback</span>
                      <input
                        value={transferDestinationTxid}
                        onChange={(e) => setTransferDestinationTxid(e.target.value)}
                        placeholder={destinationCloseCandidateId ? `Candidate: ${bridgeShortAddress(destinationCloseCandidateId, 8, 8)}` : "optional destination txid/hash"}
                        type={hideTableData ? "password" : "text"}
                        style={{ padding: "7px 8px", borderRadius: 8, border: "1px solid rgba(255,255,255,0.14)", background: "rgba(0,0,0,0.18)", color: "inherit", minWidth: 0 }}
                      />
                    </label>
                  </div>
                  <div style={{ opacity: 0.64, fontSize: 11, lineHeight: 1.35 }}>
                    Link buttons use the first close amount candidate when available, otherwise they use the txid/hash you enter here.
                  </div>
                </div>
              ) : null}

              {basisPreview?.ok ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(255,255,255,0.12)", background: "rgba(0,0,0,0.14)", display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 900 }}>Basis / Tax Treatment Preview</div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Preview status</span>
                    <span style={valueStyle}>{bridgeBasisStatusLabel(basisPreview?.basisTreatment?.status)} · will mutate: {basisPreview.willMutate ? "yes" : "no"}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Source treatment</span>
                    <span style={valueStyle}>{basisPreview?.source?.treatment || "TRANSFER_OUT_CANDIDATE"} · taxable sale: {basisPreview?.source?.taxableDisposition ? "yes" : "no"}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Destination treatment</span>
                    <span style={valueStyle}>{basisPreview?.destination?.treatment || "TRANSFER_IN_CANDIDATE"} · taxable acquisition: {basisPreview?.destination?.taxableAcquisition ? "yes" : "no"}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>FIFO quantity</span>
                    <span style={valueStyle}>{hideTableData ? "••••" : bridgeBasisQtySummary(basisPreview?.fifoPreview, basisPreview?.item?.asset || asset)}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Estimated carried basis</span>
                    <span style={valueStyle}>{hideTableData ? "••••" : bridgeBasisUsdSummary(basisPreview?.basisTreatment?.estimatedCarriedBasisUsd)}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Selected lots</span>
                    <span style={valueStyle}>{Array.isArray(basisPreview?.fifoPreview?.selectedLots) ? basisPreview.fifoPreview.selectedLots.length : 0} lot(s)</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Apply preview</span>
                    <span style={valueStyle}>{basisPreview?.basisTreatment?.applyPreviewEndpointWired ? "wired read-only" : "not wired"}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Actual apply endpoint</span>
                    <span style={valueStyle}>{basisPreview?.basisTreatment?.applyEndpointWired ? "wired" : "disabled / not wired yet"}</span>
                  </div>

                  {basisReadiness.length ? (
                    <div style={{ marginTop: 4 }}>
                      <div style={{ fontWeight: 900, marginBottom: 4 }}>Basis readiness</div>
                      {basisReadiness.map((item) => (
                        <div key={item.key || item.label} style={{ lineHeight: 1.35 }}>
                          • {item.label || item.key}: {item.status || "unknown"} — {item.message || ""}
                        </div>
                      ))}
                    </div>
                  ) : null}

                  {basisWarnings.length ? (
                    <div style={{ marginTop: 4, color: "#ffc857", lineHeight: 1.35 }}>
                      {basisWarnings.map((w, i) => <div key={`${w}-${i}`}>⚠ {w}</div>)}
                    </div>
                  ) : null}

                  {basisPreview?.execution?.message ? (
                    <div style={{ opacity: 0.72, lineHeight: 1.35 }}>{basisPreview.execution.message}</div>
                  ) : null}
                </div>
              ) : null}

              {basisApplyPreview?.ok ? (
                <div style={{ marginBottom: 8, padding: 8, borderRadius: 9, border: "1px solid rgba(88,166,255,0.26)", background: "rgba(88,166,255,0.08)", display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 900 }}>Apply Basis Transfer Preview</div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Preview status</span>
                    <span style={valueStyle}>{basisApplyPreview?.applyReadiness?.readyIfConfirmed ? "ready if confirmed later" : "blocked / needs review"} · will mutate: {basisApplyPreview.willMutate ? "yes" : "no"}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Source plan</span>
                    <span style={valueStyle}>{basisApplyPreview?.plan?.sourceTreatment || "TRANSFER_OUT"} · {basisApplyPreview?.plan?.sourceLotConsumptions?.length || 0} lot consumption(s)</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Destination plan</span>
                    <span style={valueStyle}>{basisApplyPreview?.plan?.destinationTreatment || "TRANSFER_IN"} · inherited lot preview</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Estimated carried basis</span>
                    <span style={valueStyle}>{hideTableData ? "••••" : bridgeBasisUsdSummary(basisApplyPreview?.plan?.estimatedCarriedBasisUsd)}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Journal preview</span>
                    <span style={valueStyle}>{basisApplyPreview?.plan?.lotJournalPreview?.length || 0} future audit row(s) · no rows written</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Actual apply endpoint</span>
                    <span style={valueStyle}>{basisApplyPreview?.applyReadiness?.actualApplyEndpointWired ? "wired" : "disabled / not wired yet"}</span>
                  </div>

                  {basisApplyReadiness.length ? (
                    <div style={{ marginTop: 4 }}>
                      <div style={{ fontWeight: 900, marginBottom: 4 }}>Apply readiness</div>
                      {basisApplyReadiness.map((item) => (
                        <div key={item.key || item.label} style={{ lineHeight: 1.35 }}>
                          • {item.label || item.key}: {item.status || "unknown"} — {item.message || ""}
                        </div>
                      ))}
                    </div>
                  ) : null}

                  {basisApplyPreview?.applyReadiness?.blockedReasons?.length ? (
                    <div style={{ marginTop: 4, color: "#ffc857", lineHeight: 1.35 }}>
                      {basisApplyPreview.applyReadiness.blockedReasons.map((w, i) => <div key={`${w}-${i}`}>⚠ {w}</div>)}
                    </div>
                  ) : null}

                  {basisApplyWarnings.length ? (
                    <div style={{ marginTop: 4, color: "#ffc857", lineHeight: 1.35 }}>
                      {basisApplyWarnings.map((w, i) => <div key={`apply-${w}-${i}`}>⚠ {w}</div>)}
                    </div>
                  ) : null}

                  {basisApplyPreview?.execution?.message ? (
                    <div style={{ opacity: 0.72, lineHeight: 1.35 }}>{basisApplyPreview.execution.message}</div>
                  ) : null}
                </div>
              ) : null}

              {transferPreview?.ok ? (
                <div style={{ display: "grid", gap: 6 }}>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Preview status</span>
                    <span style={valueStyle}>{bridgePreviewStatusLabel(transferPreview.mode)} · will mutate: {transferPreview.willMutate ? "yes" : "no"}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Planned route</span>
                    <span style={valueStyle}>{transferPreview.sourceLabel || sourceLabel} → {transferPreview.destinationLabel || destLabel}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Planned amount</span>
                    <span style={valueStyle}>{hideTableData ? "••••" : spreadFmtQty(transferPreview.amount)} {transferPreview.asset || asset}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Mechanism</span>
                    <span style={valueStyle}>{previewPlanned.bridge_mechanism || transferPreview.bridgeMechanism || "manual"}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Record status</span>
                    <span style={valueStyle}>{createdTransferRecord?.status || previewPlanned.status || "PLANNED"}</span>
                  </div>
                  {createdTransferRecord?.id ? (
                    <div style={rowStyle}>
                      <span style={labelStyle}>Created record ID</span>
                      <span style={valueStyle}>{hideTableData ? "••••" : bridgeShortAddress(createdTransferRecord.id, 8, 8)}</span>
                    </div>
                  ) : null}
                  {createdTransferRecord?.id ? (
                    <div style={rowStyle}>
                      <span style={labelStyle}>Source link</span>
                      <span style={valueStyle}>{createdTransferRecord.source_withdrawal_id ? `withdrawal ${hideTableData ? "••••" : bridgeShortAddress(createdTransferRecord.source_withdrawal_id, 8, 8)}` : createdTransferRecord.source_txid ? `tx ${hideTableData ? "••••" : bridgeShortAddress(createdTransferRecord.source_txid, 8, 8)}` : "missing"}</span>
                    </div>
                  ) : null}
                  {createdTransferRecord?.id ? (
                    <div style={rowStyle}>
                      <span style={labelStyle}>Destination link</span>
                      <span style={valueStyle}>{createdTransferRecord.destination_deposit_id ? `deposit ${hideTableData ? "••••" : bridgeShortAddress(createdTransferRecord.destination_deposit_id, 8, 8)}` : createdTransferRecord.destination_txid ? `tx ${hideTableData ? "••••" : bridgeShortAddress(createdTransferRecord.destination_txid, 8, 8)}` : "missing"}</span>
                    </div>
                  ) : null}
                  <div style={rowStyle}>
                    <span style={labelStyle}>Source address</span>
                    <span style={valueStyle}>{hideTableData ? "••••" : bridgeShortAddress(previewPlanned.source_address)}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Destination address</span>
                    <span style={valueStyle}>{hideTableData ? "••••" : bridgeShortAddress(previewPlanned.destination_address)}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Source candidates</span>
                    <span style={valueStyle}>{bridgeCandidateSummary(transferPreview.candidateLinks?.source)}</span>
                  </div>
                  <div style={rowStyle}>
                    <span style={labelStyle}>Destination candidates</span>
                    <span style={valueStyle}>{bridgeCandidateSummary(transferPreview.candidateLinks?.destination)}</span>
                  </div>

                  {previewReadiness.length ? (
                    <div style={{ marginTop: 4 }}>
                      <div style={{ fontWeight: 900, marginBottom: 4 }}>Preview readiness</div>
                      {previewReadiness.map((item) => (
                        <div key={item.key || item.label} style={{ opacity: 0.86, lineHeight: 1.4 }}>
                          • {item.label || item.key}: {item.status || "unknown"}{item.message ? ` — ${item.message}` : ""}
                        </div>
                      ))}
                    </div>
                  ) : null}

                  {previewWarnings.length ? (
                    <div style={{ marginTop: 4, color: "var(--utt-warn, #f7b955)", lineHeight: 1.4 }}>
                      {previewWarnings.map((w, idx) => (
                        <div key={`${idx}-${String(w).slice(0, 24)}`}>⚠ {String(w)}</div>
                      ))}
                    </div>
                  ) : null}

                  {transferPreview?.execution?.message ? (
                    <div style={{ marginTop: 4, opacity: 0.68, lineHeight: 1.35 }}>
                      {transferPreview.execution.message}
                    </div>
                  ) : null}
                </div>
              ) : (
                <div style={{ opacity: 0.68, lineHeight: 1.35 }}>
                  Enter a positive amount, then preview the transfer record before creating or linking anything.
                </div>
              )}
            </div>

            <div style={{ ...panelCardStyle, marginTop: 10 }}>
              <div style={{ fontWeight: 900, marginBottom: 6 }}>Readiness checklist</div>
              <div>• Source wallet registered: {source?.address ? "yes" : "missing"}</div>
              <div>• Destination wallet registered: {dest?.address ? "yes" : "missing"}</div>
              <div>• UTTT price context: {snap?.ok ? "ready" : "partial"}</div>
              <div>• UTTT supply context: {bridgeSupplyStatus(supply)}</div>
              <div>• Transfer record support: {bridgeTransferSupportStatus(transferStatus)}</div>
              <div>• Transfer execution: disabled</div>
            </div>

            {err ? (
              <div style={{ marginTop: 10, padding: 9, borderRadius: 10, border: "1px solid rgba(255,107,107,0.35)", background: "rgba(255,107,107,0.10)", fontSize: 12 }}>
                {err}
              </div>
            ) : null}

            {supply?.ok === false && supply?.error ? (
              <div style={{ marginTop: 10, padding: 9, borderRadius: 10, border: "1px solid rgba(247,185,85,0.35)", background: "rgba(247,185,85,0.10)", fontSize: 12 }}>
                Supply endpoint partial: {String(supply.error)}
              </div>
            ) : null}

            {transferStatus?.ok === false && transferStatus?.error ? (
              <div style={{ marginTop: 10, padding: 9, borderRadius: 10, border: "1px solid rgba(247,185,85,0.35)", background: "rgba(247,185,85,0.10)", fontSize: 12 }}>
                Transfer-record endpoint partial: {String(transferStatus.error)}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
