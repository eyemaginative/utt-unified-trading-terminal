// frontend/src/features/registry/TokenRegistryWindow.jsx
import React, { useCallback, useEffect, useMemo, useState } from "react";

const LS_SOLANA_DETECTED_TOKENS_KEY = "utt_solana_detected_tokens_v1";

const CHAIN_OPTIONS = ["solana", "polkadot", "hydration", "counterparty", "robinhood_chain"];
const GENERIC_ADDRESS_LABEL = "Address / Mint / Asset ID";
const GENERIC_ADDRESS_PLACEHOLDER = "Mint / contract address / asset ID";
const EXTERNAL_PRICE_SOURCE_OPTIONS = ["", "stable", "coingecko", "derived", "none"];

const ROBINHOOD_CHAIN_ID = 4663;
const ROBINHOOD_CHAIN_ID_HEX = "0x1237";
const ROBINHOOD_CHAIN_NATIVE_SYMBOL = "ETH";
const ROBINHOOD_CHAIN_NATIVE_DECIMALS = 18;
const EVM_CONTRACT_ADDRESS_RE = /^0x[0-9a-fA-F]{40}$/;

function isRobinhoodChain(value) {
  return String(value || "").trim().toLowerCase() === "robinhood_chain";
}

function chainDisplayName(value) {
  const c = String(value || "").trim().toLowerCase();
  if (c === "robinhood_chain") return "Robinhood Chain";
  if (c === "counterparty") return "Counterparty";
  if (c === "polkadot") return "Polkadot";
  if (c === "hydration") return "Hydration";
  if (c === "solana") return "Solana";
  return c || "Unknown chain";
}

function addressLabelForChain(value) {
  return isRobinhoodChain(value) ? "Contract address / native marker" : GENERIC_ADDRESS_LABEL;
}

function addressPlaceholderForChain(value, symbolValue = "") {
  const c = String(value || "").trim().toLowerCase();
  const s = String(symbolValue || "").trim().toUpperCase();
  if (c === "counterparty") return "optional for protocol/global asset";
  if (c === "robinhood_chain" && s === ROBINHOOD_CHAIN_NATIVE_SYMBOL) {
    return "blank for native ETH";
  }
  if (c === "robinhood_chain") return "0x + 40 hex contract address";
  return GENERIC_ADDRESS_PLACEHOLDER;
}

function validateTokenIdentityInput(chainValue, symbolValue, addressValue, decimalsValue) {
  const c = String(chainValue || "").trim().toLowerCase();
  const s = String(symbolValue || "").trim().toUpperCase();
  const a = String(addressValue || "").trim();
  const rawDecimals = String(decimalsValue ?? "").trim();

  if (!s) return { ok: false, message: "Symbol is required." };
  if (!rawDecimals) return { ok: false, message: "Decimals are required." };

  const d = Number(rawDecimals);
  if (!Number.isFinite(d) || d < 0 || d > 18) {
    return { ok: false, message: "Decimals must be between 0 and 18." };
  }

  if (c === "counterparty") {
    return { ok: true, native: false, message: "Counterparty metadata identity is valid." };
  }

  if (c === "robinhood_chain") {
    if (s === ROBINHOOD_CHAIN_NATIVE_SYMBOL) {
      if (a) {
        return {
          ok: false,
          native: true,
          message: "Native ETH must use a blank contract address. Use WETH or another distinct symbol for ERC-20 contracts.",
        };
      }
      if (d !== ROBINHOOD_CHAIN_NATIVE_DECIMALS) {
        return {
          ok: false,
          native: true,
          message: "Native Robinhood Chain ETH must use exactly 18 decimals.",
        };
      }
      return {
        ok: true,
        native: true,
        message: "Native ETH identity is valid: blank contract address and 18 decimals.",
      };
    }

    if (!a) {
      return {
        ok: false,
        native: false,
        message: "Robinhood Chain ERC-20 rows require a contract address.",
      };
    }
    if (!EVM_CONTRACT_ADDRESS_RE.test(a)) {
      return {
        ok: false,
        native: false,
        message: "Contract address must be 0x followed by exactly 40 hexadecimal characters.",
      };
    }
    return {
      ok: true,
      native: false,
      message: "Robinhood Chain ERC-20 identity is structurally valid.",
    };
  }

  if (!a) return { ok: false, message: "Address / mint / asset ID is required." };
  return { ok: true, native: false, message: "Token identity is valid." };
}

function chainIdentityProfile(value) {
  const c = String(value || "").trim().toLowerCase();
  if (c === "robinhood_chain") {
    return {
      name: "Robinhood Chain",
      code: "RH-EVM",
      accent: "cyan",
      badges: [
        `Mainnet ${ROBINHOOD_CHAIN_ID} / ${ROBINHOOD_CHAIN_ID_HEX}`,
        "Native ETH · 18 decimals",
        "ERC-20 contracts · strict EVM address",
        "Registry-only · no wallet prompt",
      ],
      detail: "Native ETH uses a blank contract address. All non-ETH rows are treated as future ERC-20 identities and require an exact 20-byte EVM contract address.",
    };
  }
  if (c === "hydration" || c === "polkadot") {
    return {
      name: chainDisplayName(c),
      code: "HYDRA-DOT",
      accent: "violet",
      badges: ["Asset IDs", "Route Registry", "Manual routing safeguards"],
      detail: "Hydration and Polkadot identities preserve asset IDs, route templates, and venue-scoped mappings.",
    };
  }
  if (c === "counterparty") {
    return {
      name: "Counterparty",
      code: "BTC-META",
      accent: "amber",
      badges: ["Protocol assets", "Optional identifier", "Price metadata"],
      detail: "Counterparty price metadata may use a blank identifier for protocol/global assets.",
    };
  }
  return {
    name: "Solana",
    code: "SOL-SPL",
    accent: "green",
    badges: ["SPL mint", "Detected suggestions", "Decimals required"],
    detail: "Solana mappings use SPL mint addresses and can be prefilled from detected order-row suggestions.",
  };
}

function externalPriceSourceLabel(value) {
  const v = String(value || "").trim().toLowerCase();
  if (!v) return "—";
  if (v === "coingecko") return "CoinGecko";
  if (v === "coingecko_simple") return "CoinGecko";
  if (v === "stable") return "Stable";
  if (v === "derived") return "Derived";
  if (v === "none") return "None";
  return v;
}

function defaultVenueForChain(chain) {
  const c = String(chain || "").trim().toLowerCase();
  if (c === "polkadot") return "polkadot_hydration";
  if (c === "hydration") return "hydration";
  return "";
}

function compactMiddle(value, head = 12, tail = 10) {
  const s = String(value || "").trim();
  if (!s) return "";
  if (s.length <= head + tail + 3) return s;
  return `${s.slice(0, head)}…${s.slice(-tail)}`;
}

function copyText(value) {
  const s = String(value || "").trim();
  if (!s) return;
  try {
    navigator?.clipboard?.writeText?.(s);
  } catch {
    // Clipboard is best-effort only; keep the full value available in the title tooltip.
  }
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


function safeJsonPretty(value, fallback = "[]") {
  try {
    return JSON.stringify(value ?? [], null, 2);
  } catch {
    return fallback;
  }
}

function parseRouteJsonText(text) {
  const raw = String(text || "").trim();
  if (!raw) return [];
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("route_json must be a JSON array of route legs.");
  }
  return parsed;
}

const HYDRATION_BUILTIN_ROUTE_TEMPLATES = [
  {
    id: "builtin:DOT-HDX:manual_router",
    source: "builtin",
    label: "DOT-HDX manual Router · DOT → aDOT → HDX",
    symbol: "DOT-HDX",
    routeMode: "manual_router",
    poolType: "Router",
    baseReserve: "",
    quoteReserve: "",
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "Aave" }, assetIn: 5, assetOut: 1001 },
      { pool: { type: "Omnipool" }, assetIn: 1001, assetOut: 0 },
    ],
    direction: { label: "DOT → aDOT → HDX" },
    note: "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
  },
  {
    id: "builtin:HDX-DOT:manual_router",
    source: "builtin",
    label: "HDX-DOT manual Router · HDX → aDOT → DOT",
    symbol: "HDX-DOT",
    routeMode: "manual_router",
    poolType: "Router",
    baseReserve: "",
    quoteReserve: "",
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "Omnipool" }, assetIn: 0, assetOut: 1001 },
      { pool: { type: "Aave" }, assetIn: 1001, assetOut: 5 },
    ],
    direction: { label: "HDX → aDOT → DOT" },
    note: "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
  },
  {
    id: "builtin:UTTT-HDX:manual_xyk",
    source: "builtin",
    label: "UTTT-HDX manual XYK · UTTT → HDX snapshot",
    symbol: "UTTT-HDX",
    routeMode: "manual_xyk",
    poolType: "XYK",
    baseReserve: 1000000,
    quoteReserve: 832.45,
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "XYK" }, assetIn: 1001331, assetOut: 0 },
    ],
    direction: { label: "UTTT → HDX" },
    note: "Built-in manual XYK snapshot template. Add or verify pool account before relying on live reserves; confirm only after a tiny live on-chain success.",
  },
  {
    id: "builtin:HDX-UTTT:manual_xyk",
    source: "builtin",
    label: "HDX-UTTT manual XYK · HDX → UTTT snapshot",
    symbol: "HDX-UTTT",
    routeMode: "manual_xyk",
    poolType: "XYK",
    baseReserve: 832.45,
    quoteReserve: 1000000,
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "XYK" }, assetIn: 0, assetOut: 1001331 },
    ],
    direction: { label: "HDX → UTTT" },
    note: "Built-in reverse manual XYK snapshot template. Add or verify pool account before relying on live reserves; confirm only after a tiny live on-chain success.",
  },
  {
    id: "builtin:HDX-USDT:manual_router",
    source: "builtin",
    label: "HDX-USDT manual Router · HDX → USDT",
    symbol: "HDX-USDT",
    routeMode: "manual_router",
    poolType: "Router",
    baseReserve: "",
    quoteReserve: "",
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "Omnipool" }, assetIn: 0, assetOut: 10 },
    ],
    direction: { label: "HDX → USDT" },
    liquidityClass: "high_liquidity_candidate",
    templateNotes: [
      "High-liquidity candidate template using Hydration Omnipool HDX/USDT routing.",
      "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
    ],
    note: "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
  },
  {
    id: "builtin:USDT-HDX:manual_router",
    source: "builtin",
    label: "USDT-HDX manual Router · USDT → HDX",
    symbol: "USDT-HDX",
    routeMode: "manual_router",
    poolType: "Router",
    baseReserve: "",
    quoteReserve: "",
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "Omnipool" }, assetIn: 10, assetOut: 0 },
    ],
    direction: { label: "USDT → HDX" },
    liquidityClass: "high_liquidity_candidate",
    templateNotes: [
      "High-liquidity candidate template using Hydration Omnipool USDT/HDX routing.",
      "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
    ],
    note: "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
  },
  {
    id: "builtin:DOT-USDT:manual_router",
    source: "builtin",
    label: "DOT-USDT manual Router · DOT → aDOT → USDT",
    symbol: "DOT-USDT",
    routeMode: "manual_router",
    poolType: "Router",
    baseReserve: "",
    quoteReserve: "",
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "Aave" }, assetIn: 5, assetOut: 1001 },
      { pool: { type: "Omnipool" }, assetIn: 1001, assetOut: 10 },
    ],
    direction: { label: "DOT → aDOT → USDT" },
    liquidityClass: "high_liquidity_candidate",
    templateNotes: [
      "High-liquidity candidate template using DOT/aDOT wrapping plus Omnipool routing into USDT.",
      "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
    ],
    note: "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
  },
  {
    id: "builtin:USDT-DOT:manual_router",
    source: "builtin",
    label: "USDT-DOT manual Router · USDT → aDOT → DOT",
    symbol: "USDT-DOT",
    routeMode: "manual_router",
    poolType: "Router",
    baseReserve: "",
    quoteReserve: "",
    feeBps: 30,
    poolAccount: "",
    routeJson: [
      { pool: { type: "Omnipool" }, assetIn: 10, assetOut: 1001 },
      { pool: { type: "Aave" }, assetIn: 1001, assetOut: 5 },
    ],
    direction: { label: "USDT → aDOT → DOT" },
    liquidityClass: "high_liquidity_candidate",
    templateNotes: [
      "High-liquidity candidate template using Omnipool routing into aDOT plus DOT unwrap through Aave.",
      "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
    ],
    note: "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
  },
];

function routeTemplateSourceLabel(sourceType) {
  const s = String(sourceType || "").trim().toLowerCase();
  if (s === "built_in" || s === "builtin") return "Built-in template";
  if (s === "saved_registry" || s === "saved_route_registry") return "Saved Route Registry row";
  if (s === "reverse_preview") return "Reverse preview";
  if (s === "active_registry_row") return "Active Route Registry row";
  if (s === "fallback") return "Local fallback template";
  return "Route template";
}

function routeTemplateSourceBadgeSeverity(tpl) {
  const s = String(tpl?.warningLevel || "").trim().toLowerCase();
  if (s === "info" || s === "ok") return "info";
  if (s === "danger" || s === "error") return "warn";
  if (tpl?.sourceExecutable) return "ok";
  return "warn";
}

function routeWarningMessage(raw) {
  if (raw && typeof raw === "object") {
    return String(raw.message || raw.warning || raw.error || raw.note || JSON.stringify(raw)).trim();
  }
  return String(raw || "").trim();
}

function routeWarningKind(raw, fallback = "template_source_warning") {
  if (raw && typeof raw === "object") {
    return String(raw.warning || raw.error || raw.kind || fallback).trim();
  }
  return fallback;
}

function normalizeRouteWarningList(rawWarnings) {
  if (!Array.isArray(rawWarnings)) return [];
  const seen = new Set();
  const out = [];
  for (const raw of rawWarnings) {
    const message = routeWarningMessage(raw);
    if (!message || seen.has(message)) continue;
    seen.add(message);
    out.push({ warning: routeWarningKind(raw), message });
  }
  return out;
}

function normalizeRouteNoteList(rawNotes) {
  if (!Array.isArray(rawNotes)) return [];
  const seen = new Set();
  const out = [];
  for (const raw of rawNotes) {
    const message = routeWarningMessage(raw);
    if (!message || seen.has(message)) continue;
    seen.add(message);
    out.push(message);
  }
  return out;
}

function normalizeHydrationRouteTemplate(raw, index = 0) {
  const t = raw && typeof raw === "object" ? raw : {};
  const symbol = String(t.symbol || "").trim().toUpperCase();
  const routeMode = String(t.routeMode || t.route_mode || "manual_xyk").trim().toLowerCase() === "manual_router"
    ? "manual_router"
    : "manual_xyk";
  const routeJson = Array.isArray(t.routeJson)
    ? t.routeJson
    : (Array.isArray(t.route_json) ? t.route_json : (Array.isArray(t.route) ? t.route : []));
  const source = String(t.source || "template").trim() || "template";
  const sourceTypeRaw = String(t.sourceType || t.source_type || source || "template").trim().toLowerCase();
  const sourceType = sourceTypeRaw === "builtin" || sourceTypeRaw === "built-in" ? "built_in" : sourceTypeRaw;
  const id = String(t.id || `${source}:${symbol || "route"}:${routeMode}:${index}`).trim();
  const label = String(t.label || [symbol, routeMode === "manual_router" ? "manual Router" : "manual XYK", t.routeDirection || t.direction?.label].filter(Boolean).join(" · ")).trim();
  const warnings = normalizeRouteWarningList(t.warnings);
  const templateNotes = normalizeRouteNoteList(t.templateNotes || t.template_notes || []);
  return {
    id,
    source,
    sourceType,
    sourceLabel: String(t.sourceLabel || t.source_label || routeTemplateSourceLabel(sourceType)).trim(),
    label,
    symbol,
    routeMode,
    sourceRouteMode: String(t.sourceRouteMode || t.source_route_mode || routeMode).trim().toLowerCase(),
    sourceSymbol: String(t.sourceSymbol || t.source_symbol || t.routeRegistrySymbol || symbol || "").trim().toUpperCase(),
    poolType: String(t.poolType || t.pool_type || (routeMode === "manual_router" ? "Router" : "XYK")).trim(),
    baseReserve: t.baseReserve ?? t.base_reserve ?? "",
    quoteReserve: t.quoteReserve ?? t.quote_reserve ?? "",
    feeBps: t.feeBps ?? t.fee_bps ?? 30,
    poolAccount: t.poolAccount ?? t.pool_account ?? "",
    routeJson,
    direction: t.direction || (t.routeDirection ? { label: t.routeDirection } : null),
    routeDirection: t.routeDirection || t.direction?.label || "",
    note: t.note || "",
    sourceConfirmed: !!(t.sourceConfirmed || t.source_confirmed),
    sourceExecutable: !!(t.sourceExecutable || t.source_executable),
    warningLevel: String(t.warningLevel || t.warning_level || (sourceType === "saved_registry" && (t.sourceConfirmed || t.source_confirmed) ? "info" : "warn")).trim().toLowerCase(),
    warnings,
    warningCount: Number(t.warningCount ?? t.warning_count ?? warnings.length) || warnings.length,
    templateNotes,
    routeHazards: t.routeHazards || t.route_hazards || null,
    recommendedNextAction: String(t.recommendedNextAction || t.recommended_next_action || "Validate before saving. Keep Confirmed unchecked until this exact direction is live-tested.").trim(),
    loadSafety: t.loadSafety || t.load_safety || {
      clearsConfirmed: true,
      writesDb: false,
      requiresValidateBeforeSave: true,
      requiresLiveTestBeforeConfirmed: true,
    },
    requiresConfirmation: t.requiresConfirmation !== false,
  };
}

function hydrationRouteTemplateForSymbol(symbol) {
  const s = String(symbol || "").trim().toUpperCase();
  const found = HYDRATION_BUILTIN_ROUTE_TEMPLATES.find((tpl) => String(tpl.symbol || "").toUpperCase() === s);
  if (found && Array.isArray(found.routeJson)) return found.routeJson;
  return [
    { pool: { type: "Omnipool" }, assetIn: 0, assetOut: 0 },
  ];
}

function hasKnownHydrationRouteTemplate(symbol) {
  const s = String(symbol || "").trim().toUpperCase();
  return HYDRATION_BUILTIN_ROUTE_TEMPLATES.some((tpl) => String(tpl.symbol || "").toUpperCase() === s);
}

function isKnownHydrationRouteTemplateText(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  try {
    const parsed = JSON.parse(raw);
    const json = JSON.stringify(parsed);
    return HYDRATION_BUILTIN_ROUTE_TEMPLATES.some((tpl) => json === JSON.stringify(tpl.routeJson || []));
  } catch {
    return false;
  }
}

function routeModeLabel(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "manual_router") return "Manual Router";
  if (v === "manual_xyk") return "Manual XYK";
  return v || "Manual XYK";
}

function assetIdForCompare(value) {
  const s = String(value ?? "").trim().toLowerCase();
  if (!s) return "";
  if (s === "native") return "0";
  return s;
}

function routeAssetLabel(assetId, row) {
  const id = assetIdForCompare(assetId);
  if (!id) return "asset:?";
  if (id === assetIdForCompare(row?.baseAssetId)) return String(row?.baseSymbol || row?.base || id).toUpperCase();
  if (id === assetIdForCompare(row?.quoteAssetId)) return String(row?.quoteSymbol || row?.quote || id).toUpperCase();
  if (id === "0") return "HDX";
  if (id === "1001") return "aDOT";
  if (id === "1001331") return "UTTT";
  return `asset:${id}`;
}

function routeLegAssetValue(leg, ...keys) {
  if (!leg || typeof leg !== "object") return null;
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(leg, key)) {
      const n = Number(String(leg[key]).trim());
      if (Number.isFinite(n)) return n;
      return null;
    }
  }
  return null;
}

function routeDirectionLabel(row) {
  const supplied = String(row?.routeDirection || row?.direction?.label || "").trim();
  if (supplied) return supplied;

  const route = Array.isArray(row?.route) ? row.route : [];
  const ids = [];
  if (route.length) {
    const firstIn = routeLegAssetValue(route[0], "assetIn", "asset_in");
    if (firstIn !== null) ids.push(firstIn);
    for (const leg of route) {
      const out = routeLegAssetValue(leg, "assetOut", "asset_out");
      if (out !== null) ids.push(out);
    }
  }
  if (ids.length < 2) {
    ids.push(assetIdForCompare(row?.baseAssetId) || row?.baseSymbol || "?");
    ids.push(assetIdForCompare(row?.quoteAssetId) || row?.quoteSymbol || "?");
  }
  return ids.map((id) => routeAssetLabel(id, row)).join(" → ");
}

function routeExecutionStatus(row) {
  const fromBackend = row?.executionStatus;
  if (fromBackend && typeof fromBackend === "object") {
    return {
      status: String(fromBackend.status || "").trim() || "unknown",
      label: String(fromBackend.label || "").trim() || "Unknown",
      severity: String(fromBackend.severity || "").trim() || "info",
      executable: !!fromBackend.executable,
    };
  }

  if (row?.enabled === false) {
    return { status: "disabled", label: "Disabled", severity: "muted", executable: false };
  }
  if (String(row?.routeMode || "").toLowerCase() === "manual_router") {
    return row?.confirmed
      ? { status: "confirmed_executable", label: "Confirmed executable", severity: "ok", executable: true }
      : { status: "unconfirmed_blocked", label: "Needs confirmation", severity: "warn", executable: false };
  }
  return { status: "manual_pool_available", label: "Manual pool route", severity: row?.confirmed ? "ok" : "info", executable: true };
}

function routeBadgeStyle(severity = "info") {
  const s = String(severity || "info").toLowerCase();
  if (s === "ok") {
    return {
      border: "1px solid rgba(74,222,128,0.35)",
      background: "rgba(22,101,52,0.18)",
      color: "#b8f7c7",
    };
  }
  if (s === "warn") {
    return {
      border: "1px solid rgba(245,158,11,0.40)",
      background: "rgba(120,72,16,0.18)",
      color: "#ffe2a6",
    };
  }
  if (s === "muted") {
    return {
      border: "1px solid rgba(148,163,184,0.22)",
      background: "rgba(148,163,184,0.08)",
      color: "rgba(226,232,240,0.70)",
    };
  }
  return {
    border: "1px solid rgba(96,165,250,0.30)",
    background: "rgba(30,64,175,0.16)",
    color: "#cfe4ff",
  };
}



export default function TokenRegistryWindow({ apiBase = "", onClose }) {
  const API_BASE = String(apiBase || "").trim() || "";

  const [chain, setChain] = useState("solana");
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState(null);
  const [suggestions, setSuggestions] = useState([]);
  const [dismissed, setDismissed] = useState(() => new Set());

  // Add form
  const [symbol, setSymbol] = useState("");
  const [address, setAddress] = useState("");
  const [decimals, setDecimals] = useState("");
  const [label, setLabel] = useState("");
  const [venue, setVenue] = useState(""); // optional override scope (blank = global)
  const [externalPriceSource, setExternalPriceSource] = useState("");
  const [externalPriceId, setExternalPriceId] = useState("");

  // Inline edit
  const [editId, setEditId] = useState(null);
  const [editRow, setEditRow] = useState({
    symbol: "",
    address: "",
    decimals: "",
    label: "",
    venue: "",
    external_price_source: "",
    external_price_id: "",
  });

  // Hydration manual route registry
  const [routes, setRoutes] = useState([]);
  const [routeLoading, setRouteLoading] = useState(false);
  const [routeSaving, setRouteSaving] = useState(false);
  const [routeValidating, setRouteValidating] = useState(false);
  const [routeReversing, setRouteReversing] = useState(false);
  const [routeErr, setRouteErr] = useState(null);
  const [routeSymbol, setRouteSymbol] = useState("UTTT-HDX");
  const [routeMode, setRouteMode] = useState("manual_xyk");
  const [routePoolType, setRoutePoolType] = useState("XYK");
  const [routeBaseReserve, setRouteBaseReserve] = useState("");
  const [routeQuoteReserve, setRouteQuoteReserve] = useState("");
  const [routeFeeBps, setRouteFeeBps] = useState("30");
  const [routePoolAccount, setRoutePoolAccount] = useState("");
  const [routeEnabled, setRouteEnabled] = useState(true);
  const [routeConfirmed, setRouteConfirmed] = useState(false);
  const [routeJsonText, setRouteJsonText] = useState("");
  const [routeNote, setRouteNote] = useState("");
  const [routeTestResult, setRouteTestResult] = useState(null);
  const [routeTemplates, setRouteTemplates] = useState([]);
  const [routeTemplateLoading, setRouteTemplateLoading] = useState(false);
  const [selectedRouteTemplateId, setSelectedRouteTemplateId] = useState("builtin:DOT-HDX:manual_router");

  const activeVenueFilter = useMemo(() => defaultVenueForChain(chain), [chain]);
  const activeChainProfile = useMemo(() => chainIdentityProfile(chain), [chain]);
  const tokenIdentityValidation = useMemo(
    () => validateTokenIdentityInput(chain, symbol, address, decimals),
    [chain, symbol, address, decimals]
  );
  const showHydrationRoutes = useMemo(() => {
    const c = String(chain || "").trim().toLowerCase();
    return c === "hydration" || c === "polkadot";
  }, [chain]);

  const routeTemplateOptions = useMemo(() => {
    const out = [];
    const seen = new Set();
    for (const raw of [...HYDRATION_BUILTIN_ROUTE_TEMPLATES, ...(routeTemplates || [])]) {
      const tpl = normalizeHydrationRouteTemplate(raw, out.length);
      const key = tpl.id || `${tpl.source}:${tpl.symbol}:${tpl.routeMode}:${out.length}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(tpl);
    }
    return out;
  }, [routeTemplates]);

  const selectedRouteTemplate = useMemo(
    () => routeTemplateOptions.find((tpl) => tpl.id === selectedRouteTemplateId) || null,
    [routeTemplateOptions, selectedRouteTemplateId]
  );

  const loadSuggestions = useCallback(() => {
    try {
      const raw = localStorage.getItem(LS_SOLANA_DETECTED_TOKENS_KEY) || "[]";
      const arr = JSON.parse(raw);
      setSuggestions(Array.isArray(arr) ? arr : []);
    } catch {
      setSuggestions([]);
    }
  }, []);

  const existingAddressSet = useMemo(() => {
    const s = new Set();
    for (const row of items || []) {
      const a = String(row?.address || "").trim();
      if (!a) continue;
      s.add(a);
      s.add(a.toLowerCase());
    }
    return s;
  }, [items]);

  const visibleSuggestions = useMemo(() => {
    if (chain !== "solana") return [];
    return (suggestions || []).filter((it) => {
      const a = String(it?.address || "").trim();
      if (!a) return false;
      if (dismissed.has(a)) return false;
      if (existingAddressSet.has(a) || existingAddressSet.has(a.toLowerCase())) return false;
      return true;
    });
  }, [chain, suggestions, dismissed, existingAddressSet]);

  const canAdd = useMemo(
    () => tokenIdentityValidation.ok,
    [tokenIdentityValidation]
  );

  const canUpsertRoute = useMemo(() => {
    const sym = String(routeSymbol || "").trim().toUpperCase();
    const mode = String(routeMode || "manual_xyk").trim().toLowerCase();
    const fee = Number(String(routeFeeBps || "").trim());
    if (!sym || !sym.includes("-") || !Number.isFinite(fee) || fee < 0) return false;
    if (mode === "manual_router") {
      try {
        const route = parseRouteJsonText(routeJsonText);
        return Array.isArray(route) && route.length > 0;
      } catch {
        return false;
      }
    }
    const base = Number(String(routeBaseReserve || "").trim());
    const quote = Number(String(routeQuoteReserve || "").trim());
    return Number.isFinite(base) && base > 0 && Number.isFinite(quote) && quote > 0;
  }, [routeSymbol, routeMode, routeBaseReserve, routeQuoteReserve, routeFeeBps, routeJsonText]);

  useEffect(() => {
    const mode = String(routeMode || "").trim().toLowerCase();
    const sym = String(routeSymbol || "").trim().toUpperCase();
    if (mode !== "manual_router" || !hasKnownHydrationRouteTemplate(sym)) return;

    const current = String(routeJsonText || "").trim();
    if (current && !isKnownHydrationRouteTemplateText(current)) return;

    const next = safeJsonPretty(hydrationRouteTemplateForSymbol(sym));
    if (current !== next) {
      setRouteJsonText(next);
    }
  }, [routeMode, routeSymbol, routeJsonText]);

  const load = useCallback(async () => {
    setLoading(true);
    setErr(null);
    try {
      let url = `${API_BASE}/api/token_registry?chain=${encodeURIComponent(chain)}`;
      if (activeVenueFilter) {
        url += `&venue=${encodeURIComponent(activeVenueFilter)}&include_global=1`;
      }
      const r = await fetch(url, { method: "GET", headers: { accept: "application/json" } });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      const arr = Array.isArray(j?.items) ? j.items : [];
      setItems(arr);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [API_BASE, chain, activeVenueFilter]);

  const loadRoutes = useCallback(async () => {
    if (!showHydrationRoutes) {
      setRoutes([]);
      setRouteErr(null);
      return;
    }
    setRouteLoading(true);
    setRouteErr(null);
    try {
      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry`, {
        method: "GET",
        headers: { accept: "application/json" },
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      setRoutes(Array.isArray(j?.items) ? j.items : []);
    } catch (e) {
      setRouteErr(String(e?.message || e));
    } finally {
      setRouteLoading(false);
    }
  }, [API_BASE, showHydrationRoutes]);

  const loadRouteTemplates = useCallback(async () => {
    if (!showHydrationRoutes) {
      setRouteTemplates([]);
      return;
    }
    setRouteTemplateLoading(true);
    try {
      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry/templates?include_builtin=1&include_saved=1`, {
        method: "GET",
        headers: { accept: "application/json" },
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      const items = Array.isArray(j?.templates) ? j.templates : (Array.isArray(j?.items) ? j.items : []);
      setRouteTemplates(items.map((item, idx) => normalizeHydrationRouteTemplate(item, idx)));
    } catch {
      // Keep built-in templates available even if the optional backend template
      // endpoint is temporarily unavailable during a rolling local restart.
      setRouteTemplates([]);
    } finally {
      setRouteTemplateLoading(false);
    }
  }, [API_BASE, showHydrationRoutes]);

  useEffect(() => {
    load();
    loadSuggestions();
  }, [load, loadSuggestions]);

  useEffect(() => {
    loadRoutes();
    loadRouteTemplates();
  }, [loadRoutes, loadRouteTemplates]);

  useEffect(() => {
    const onFocus = () => loadSuggestions();
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, [loadSuggestions]);

  const useSuggestion = useCallback((sug) => {
    setSymbol(String(sug?.symbol || "").trim());
    setAddress(String(sug?.address || "").trim());
    setDecimals(sug?.decimals == null ? "" : String(sug.decimals));
    setVenue(String(sug?.venue || "").trim());
  }, []);

  const useRobinhoodNativeEthPreset = useCallback(() => {
    setSymbol(ROBINHOOD_CHAIN_NATIVE_SYMBOL);
    setAddress("");
    setDecimals(String(ROBINHOOD_CHAIN_NATIVE_DECIMALS));
    setLabel("Robinhood Chain ETH");
    setVenue("");
    setExternalPriceSource("coingecko");
    setExternalPriceId("ethereum");
    setErr(null);
  }, []);

  const addSuggestion = useCallback(async (sug) => {
    const payload = {
      chain,
      symbol: String(sug?.symbol || "").trim(),
      address: String(sug?.address || "").trim(),
      decimals: Number(sug?.decimals),
    };
    const v = String(sug?.venue || "").trim();
    if (v) payload.venue = v;
    if (!payload.symbol || !payload.address || !Number.isFinite(payload.decimals)) {
      useSuggestion(sug);
      setErr("Suggestion is incomplete. Review and save manually.");
      return;
    }
    setSaving(true);
    setErr(null);
    try {
      const r = await fetch(`${API_BASE}/api/token_registry`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      setDismissed((prev) => { const n = new Set(prev); n.add(payload.address); return n; });
      await load();
      loadSuggestions();
    } catch (e) {
      setErr(String(e?.message || e));
      useSuggestion(sug);
    } finally {
      setSaving(false);
    }
  }, [API_BASE, chain, load, loadSuggestions, useSuggestion]);

  const onAdd = useCallback(async () => {
    if (!canAdd) return;
    setSaving(true);
    setErr(null);
    try {
      const payload = {
        chain,
        symbol: String(symbol || "").trim(),
        address: String(address || "").trim(),
        decimals: Number(String(decimals || "").trim()),
        label: String(label || "").trim(),
      };
      const v = String(venue || "").trim();
      if (v) payload.venue = v;
      const eps = String(externalPriceSource || "").trim();
      const epid = String(externalPriceId || "").trim();
      if (eps) payload.external_price_source = eps;
      if (epid) payload.external_price_id = epid;

      const r = await fetch(`${API_BASE}/api/token_registry`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);

      setSymbol("");
      setAddress("");
      setDecimals("");
      setLabel("");
      setVenue("");
      setExternalPriceSource("");
      setExternalPriceId("");
      await load();
      loadSuggestions();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }, [API_BASE, canAdd, chain, symbol, address, decimals, label, venue, externalPriceSource, externalPriceId, load, loadSuggestions]);

  const startEdit = useCallback((row) => {
    setEditId(row?.id || null);
    setEditRow({
      symbol: String(row?.symbol || ""),
      address: String(row?.address || ""),
      decimals: String(row?.decimals ?? ""),
      label: String(row?.label || ""),
      venue: String(row?.venue || ""),
      external_price_source: String(row?.external_price_source || ""),
      external_price_id: String(row?.external_price_id || ""),
    });
  }, []);

  const cancelEdit = useCallback(() => {
    setEditId(null);
    setEditRow({ symbol: "", address: "", decimals: "", label: "", venue: "", external_price_source: "", external_price_id: "" });
  }, []);

  const saveEdit = useCallback(async () => {
    const id = editId;
    if (!id) return;

    const s = String(editRow.symbol || "").trim();
    const a = String(editRow.address || "").trim();
    const d = Number(String(editRow.decimals || "").trim());
    const editValidation = validateTokenIdentityInput(chain, s, a, editRow.decimals);
    if (!editValidation.ok) {
      setErr(`Edit: ${editValidation.message}`);
      return;
    }

    setSaving(true);
    setErr(null);
    try {
      const payload = {
        chain,
        symbol: s,
        address: a,
        decimals: d,
        label: String(editRow.label || "").trim(),
      };
      const v = String(editRow.venue || "").trim();
      if (v) payload.venue = v;
      payload.external_price_source = String(editRow.external_price_source || "").trim();
      payload.external_price_id = String(editRow.external_price_id || "").trim();

      const r = await fetch(`${API_BASE}/api/token_registry/${encodeURIComponent(String(id))}`, {
        method: "PUT",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);

      cancelEdit();
      await load();
      loadSuggestions();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }, [API_BASE, chain, editId, editRow, load, cancelEdit]);

  const delRow = useCallback(
    async (row) => {
      const id = row?.id;
      if (!id) return;
      const ok = confirm(`Delete token mapping?\n\n${row?.symbol} (${row?.chain})\n${row?.address}`);
      if (!ok) return;

      setSaving(true);
      setErr(null);
      try {
        const r = await fetch(`${API_BASE}/api/token_registry/${encodeURIComponent(String(id))}`, {
          method: "DELETE",
          headers: { accept: "application/json" },
        });
        const j = await r.json().catch(() => null);
        if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
        await load();
        loadSuggestions();
      } catch (e) {
        setErr(String(e?.message || e));
      } finally {
        setSaving(false);
      }
    },
    [API_BASE, load]
  );

  const testResolve = useCallback(
    async (sym) => {
      const a = String(sym || "").trim();
      if (!a) return;
      setErr(null);
      try {
        const c = String(chain || "").trim().toLowerCase();
        if (c === "robinhood_chain") {
          const row = (items || []).find(
            (item) => String(item?.symbol || "").trim().toUpperCase() === a.toUpperCase()
          );
          if (!row) {
            throw new Error(`No Robinhood Chain registry row returned for ${a.toUpperCase()}.`);
          }
          const validation = validateTokenIdentityInput(
            c,
            row?.symbol,
            row?.address,
            row?.decimals
          );
          if (!validation.ok) {
            throw new Error(validation.message);
          }
          alert(`Registry identity valid:\n\nchain=robinhood_chain\nsymbol=${row?.symbol || a.toUpperCase()}\nkind=${validation.native ? "native ETH" : "ERC-20 contract"}\naddress=${row?.address || "(blank native address)"}\ndecimals=${row?.decimals}\nvenue=${row?.venue || "global"}\npriceSource=${row?.external_price_source || "—"}\npriceId=${row?.external_price_id || "—"}\n\nNo RPC balance read or wallet request was performed.`);
          return;
        }

        if (c === "counterparty") {
          const params = new URLSearchParams({
            assets: a.toUpperCase(),
            limit: "1",
            ttl_s: "300",
            force_refresh: "true",
          });
          const r = await fetch(`${API_BASE}/api/market_metrics/summary?${params.toString()}`, {
            method: "GET",
            headers: { accept: "application/json" },
          });
          const j = await r.json().catch(() => null);
          if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
          const row = Array.isArray(j?.items)
            ? j.items.find((item) => String(item?.asset || "").trim().toUpperCase() === a.toUpperCase())
            : null;
          if (!row) {
            throw new Error(`No Market Metrics row returned for ${a.toUpperCase()}.`);
          }
          alert(`Price mapping:\n\nchain=counterparty\nsymbol=${row?.asset || a.toUpperCase()}\npriceUsd=${row?.price_usd ?? "—"}\nsource=${row?.price_source || row?.source || "—"}\nstatus=${row?.price_status || "—"}\nwarnings=${Array.isArray(row?.warnings) && row.warnings.length ? row.warnings.join(" | ") : "none"}`);
          return;
        }

        const resolvePath = c === "polkadot" || c === "hydration"
          ? "/api/polkadot_dex/resolve"
          : "/api/solana_dex/resolve";
        const r = await fetch(`${API_BASE}${resolvePath}?asset=${encodeURIComponent(a)}`, {
          method: "GET",
          headers: { accept: "application/json" },
        });
        const j = await r.json().catch(() => null);
        if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
        if (resolvePath === "/api/polkadot_dex/resolve") {
          alert(`Resolved:\n\nchain=${c}\nsymbol=${j?.symbol}\nassetId=${j?.assetId}\ndecimals=${j?.decimals}\nnative=${j?.native}\nsource=${j?.source || "—"}`);
        } else {
          alert(`Resolved:\n\nasset=${j?.asset}\nmint=${j?.mint}\ndecimals=${j?.decimals}`);
        }
      } catch (e) {
        setErr(String(e?.message || e));
      }
    },
    [API_BASE, chain, items]
  );

  const clearRouteForm = useCallback(() => {
    setRouteSymbol("UTTT-HDX");
    setRouteMode("manual_xyk");
    setRoutePoolType("XYK");
    setRouteBaseReserve("");
    setRouteQuoteReserve("");
    setRouteFeeBps("30");
    setRoutePoolAccount("");
    setRouteEnabled(true);
    setRouteConfirmed(false);
    setRouteJsonText("");
    setRouteNote("");
    setRouteTestResult(null);
  }, []);

  const useRoute = useCallback((row) => {
    const mode = String(row?.routeMode || row?.route_mode || "manual_xyk").trim().toLowerCase() === "manual_router"
      ? "manual_router"
      : "manual_xyk";
    setRouteSymbol(String(row?.symbol || ""));
    setRouteMode(mode);
    setRoutePoolType(mode === "manual_router" ? "Router" : "XYK");
    setRouteBaseReserve(row?.baseReserve == null ? "" : String(row.baseReserve));
    setRouteQuoteReserve(row?.quoteReserve == null ? "" : String(row.quoteReserve));
    setRouteFeeBps(row?.feeBps == null ? "30" : String(row.feeBps));
    setRoutePoolAccount(String(row?.poolAccount || row?.pool_account || ""));
    setRouteEnabled(row?.enabled !== false);
    setRouteConfirmed(row?.confirmed === true);
    const normalizedRoute = Array.isArray(row?.route) ? row.route : [];
    setRouteJsonText(normalizedRoute.length ? safeJsonPretty(normalizedRoute) : "");
    setRouteNote(String(row?.note || ""));
    const status = routeExecutionStatus(row);
    setRouteTestResult({
      kind: "template",
      templateAction: "active_row",
      ok: true,
      symbol: String(row?.symbol || ""),
      routeMode: mode,
      direction: row?.direction || (row?.routeDirection ? { label: row.routeDirection } : null),
      normalizedRoute,
      validation: { legCount: normalizedRoute.length },
      template: {
        source: "active_registry_row",
        sourceType: "active_registry_row",
        sourceLabel: "Active Route Registry row",
        sourceConfirmed: row?.confirmed === true,
        sourceExecutable: !!status.executable,
        recommendedNextAction: row?.confirmed
          ? "Active confirmed row loaded. Edit carefully; saving will update this route row."
          : "Active unconfirmed row loaded. Validate and live-test before checking Confirmed.",
      },
      message: "Active Route Registry row loaded into the form. Saving will update this pair.",
    });
  }, []);

  const applyRouteTemplate = useCallback((rawTemplate, action = "load") => {
    const tpl = normalizeHydrationRouteTemplate(rawTemplate || selectedRouteTemplate || {}, 0);
    if (!tpl.symbol) {
      setRouteErr("No route template selected.");
      return;
    }
    setRouteSymbol(tpl.symbol);
    setRouteMode(tpl.routeMode);
    setRoutePoolType(tpl.routeMode === "manual_router" ? "Router" : "XYK");
    setRouteBaseReserve(tpl.routeMode === "manual_router" ? "" : (tpl.baseReserve == null ? "" : String(tpl.baseReserve)));
    setRouteQuoteReserve(tpl.routeMode === "manual_router" ? "" : (tpl.quoteReserve == null ? "" : String(tpl.quoteReserve)));
    setRouteFeeBps(tpl.feeBps == null ? "30" : String(tpl.feeBps));
    setRoutePoolAccount(String(tpl.poolAccount || ""));
    setRouteEnabled(true);
    // Templates are operator aids. Keep confirmation explicit even when the
    // source template came from an already-confirmed saved row.
    setRouteConfirmed(false);
    setRouteJsonText(Array.isArray(tpl.routeJson) && tpl.routeJson.length ? safeJsonPretty(tpl.routeJson) : "");
    setRouteNote(String(tpl.note || ""));
    setRouteErr(null);
    setRouteTestResult({
      kind: "template",
      templateAction: action === "clone" ? "clone" : "load",
      ok: true,
      symbol: tpl.symbol,
      routeMode: tpl.routeMode,
      direction: tpl.direction || (tpl.routeDirection ? { label: tpl.routeDirection } : null),
      normalizedRoute: Array.isArray(tpl.routeJson) ? tpl.routeJson : [],
      template: tpl,
      warnings: Array.isArray(tpl.warnings) ? tpl.warnings : [],
      message: action === "clone"
        ? "Template cloned into the form. No DB write happened. Validate before saving; Confirmed stays unchecked until explicitly restored after a live test."
        : "Template loaded. Validate before saving; Confirmed stays unchecked until explicitly restored after a live test.",
    });
  }, [selectedRouteTemplate]);

  const buildRouteRegistryPayload = useCallback(() => {
    const mode = String(routeMode || "manual_xyk").trim().toLowerCase() === "manual_router" ? "manual_router" : "manual_xyk";
    const route = parseRouteJsonText(routeJsonText);
    const payload = {
      symbol: String(routeSymbol || "").trim().toUpperCase(),
      route_mode: mode,
      fee_bps: Number(String(routeFeeBps || "").trim()),
      enabled: !!routeEnabled,
      confirmed: !!routeConfirmed,
      pool_type: mode === "manual_router" ? "Router" : "XYK",
    };
    if (mode === "manual_xyk") {
      payload.base_reserve = Number(String(routeBaseReserve || "").trim());
      payload.quote_reserve = Number(String(routeQuoteReserve || "").trim());
    }
    if (route.length) {
      payload.route_json = route;
    }
    const pool = String(routePoolAccount || "").trim();
    if (pool) payload.pool_account = pool;
    const n = String(routeNote || "").trim();
    if (n) payload.note = n;
    return payload;
  }, [routeSymbol, routeMode, routeBaseReserve, routeQuoteReserve, routeFeeBps, routePoolAccount, routeEnabled, routeConfirmed, routeJsonText, routeNote]);

  const validateRoute = useCallback(async () => {
    if (!canUpsertRoute) return;
    setRouteValidating(true);
    setRouteErr(null);
    setRouteTestResult(null);
    try {
      const payload = buildRouteRegistryPayload();
      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry/validate`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      const validation = j?.routeValidation || {};
      setRouteTestResult({
        kind: "validation",
        ok: !!(j?.ok && validation?.ok),
        symbol: j?.symbol || payload.symbol,
        routeMode: j?.routeMode || payload.route_mode,
        direction: j?.direction || null,
        validation,
        normalizedRoute: validation?.route || [],
        errors: Array.isArray(validation?.errors) ? validation.errors : [],
        warnings: Array.isArray(validation?.warnings) ? validation.warnings : [],
      });
    } catch (e) {
      const msg = String(e?.message || e);
      setRouteErr(msg);
      setRouteTestResult({ kind: "validation", ok: false, symbol: String(routeSymbol || "").trim().toUpperCase(), error: msg });
    } finally {
      setRouteValidating(false);
    }
  }, [API_BASE, canUpsertRoute, routeSymbol, buildRouteRegistryPayload]);

  const reverseRoute = useCallback(async () => {
    if (!canUpsertRoute) return;
    setRouteReversing(true);
    setRouteErr(null);
    setRouteTestResult(null);
    try {
      const payload = buildRouteRegistryPayload();
      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry/reverse_preview`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);

      const reversedPayload = j?.reversedPayload || {};
      const validation = j?.routeValidation || {};
      const reversedRoute = Array.isArray(reversedPayload?.route_json)
        ? reversedPayload.route_json
        : (Array.isArray(validation?.route) ? validation.route : []);

      setRouteSymbol(String(reversedPayload?.symbol || j?.symbol || "").trim().toUpperCase());
      setRouteMode(String(reversedPayload?.route_mode || j?.routeMode || routeMode || "manual_xyk").trim().toLowerCase() === "manual_router" ? "manual_router" : "manual_xyk");
      setRoutePoolType(String(reversedPayload?.route_mode || j?.routeMode || routeMode || "").trim().toLowerCase() === "manual_router" ? "Router" : "XYK");
      setRouteBaseReserve(reversedPayload?.base_reserve == null ? "" : String(reversedPayload.base_reserve));
      setRouteQuoteReserve(reversedPayload?.quote_reserve == null ? "" : String(reversedPayload.quote_reserve));
      setRouteFeeBps(reversedPayload?.fee_bps == null ? String(routeFeeBps || "30") : String(reversedPayload.fee_bps));
      setRoutePoolAccount(String(reversedPayload?.pool_account || ""));
      setRouteEnabled(reversedPayload?.enabled !== false);
      setRouteConfirmed(false);
      setRouteJsonText(reversedRoute.length ? safeJsonPretty(reversedRoute) : "");
      setRouteNote(String(reversedPayload?.note || routeNote || ""));

      setRouteTestResult({
        kind: "reverse_preview",
        ok: !!(j?.ok && validation?.ok),
        symbol: j?.symbol || reversedPayload?.symbol || payload.symbol,
        routeMode: j?.routeMode || reversedPayload?.route_mode || payload.route_mode,
        direction: j?.direction || null,
        validation,
        normalizedRoute: reversedRoute,
        errors: Array.isArray(validation?.errors) ? validation.errors : [],
        original: j?.original || null,
        template: {
          source: "reverse_preview",
          sourceType: "reverse_preview",
          sourceLabel: j?.sourceLabel || "Reverse preview",
          warningLevel: j?.warningLevel || "warn",
          warnings: Array.isArray(j?.warnings) ? j.warnings : [],
          recommendedNextAction: j?.recommendedNextAction || "Validate before saving. Keep Confirmed unchecked until this exact direction is live-tested.",
        },
        warnings: [
          ...(Array.isArray(j?.warnings) ? j.warnings.map((message) => ({ warning: "reverse_preview_warning", message })) : []),
          ...(Array.isArray(validation?.warnings) ? validation.warnings : []),
        ],
      });
    } catch (e) {
      const msg = String(e?.message || e);
      setRouteErr(msg);
      setRouteTestResult({ kind: "reverse_preview", ok: false, symbol: String(routeSymbol || "").trim().toUpperCase(), error: msg });
    } finally {
      setRouteReversing(false);
    }
  }, [API_BASE, canUpsertRoute, routeMode, routeFeeBps, routeNote, routeSymbol, buildRouteRegistryPayload]);


  const upsertRoute = useCallback(async () => {
    if (!canUpsertRoute) return;
    setRouteSaving(true);
    setRouteErr(null);
    try {
      const payload = buildRouteRegistryPayload();

      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry/upsert`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      setRouteTestResult(null);
      await loadRoutes();
      await loadRouteTemplates();
    } catch (e) {
      setRouteErr(String(e?.message || e));
    } finally {
      setRouteSaving(false);
    }
  }, [API_BASE, canUpsertRoute, buildRouteRegistryPayload, loadRoutes, loadRouteTemplates]);

  const deleteRoute = useCallback(async (row) => {
    const id = row?.id;
    if (!id) return;
    const ok = confirm(`Delete Hydration route?\n\n${row?.symbol || ""}`);
    if (!ok) return;
    setRouteSaving(true);
    setRouteErr(null);
    try {
      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry/${encodeURIComponent(String(id))}`, {
        method: "DELETE",
        headers: { accept: "application/json" },
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      setRouteTestResult(null);
      await loadRoutes();
      await loadRouteTemplates();
    } catch (e) {
      setRouteErr(String(e?.message || e));
    } finally {
      setRouteSaving(false);
    }
  }, [API_BASE, loadRoutes, loadRouteTemplates]);

  const testRouteOrderbook = useCallback(async (row) => {
    const sym = String(row?.symbol || routeSymbol || "").trim().toUpperCase();
    if (!sym) return;
    setRouteErr(null);
    setRouteTestResult(null);
    try {
      const testMode = String(row?.routeMode || routeMode || "manual_xyk").toLowerCase() === "manual_router" ? "auto" : "manual_xyk";
      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/orderbook?symbol=${encodeURIComponent(sym)}&depth=5&route_mode=${encodeURIComponent(testMode)}`, {
        method: "GET",
        headers: { accept: "application/json" },
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      const bid = Array.isArray(j?.bids) && j.bids[0] ? j.bids[0].price : null;
      const ask = Array.isArray(j?.asks) && j.asks[0] ? j.asks[0].price : null;
      setRouteTestResult({
        kind: "orderbook",
        ok: true,
        symbol: sym,
        router: j?.router || "—",
        effective: j?.routeModeEffective || "—",
        source: j?.pool?.source || "—",
        spot: j?.pool?.spotPrice ?? "—",
        inverse: j?.pool?.inversePrice ?? "—",
        bid: bid ?? "—",
        ask: ask ?? "—",
        liveReservesOk: j?.pool?.liveReserves?.ok,
        liquidityWarning: buildHydrationLowLiquidityWarning(j),
        poolAccount: j?.pool?.poolAccount || row?.poolAccount || row?.pool_account || routePoolAccount || "",
      });
    } catch (e) {
      const msg = String(e?.message || e);
      setRouteErr(msg);
      setRouteTestResult({ kind: "orderbook", ok: false, symbol: sym, error: msg });
    }
  }, [API_BASE, routeSymbol, routePoolAccount, routeMode]);

  const testRouteLiveReserves = useCallback(async (row) => {
    const sym = String(row?.symbol || routeSymbol || "").trim().toUpperCase();
    if (!sym) return;
    setRouteErr(null);
    setRouteTestResult(null);
    try {
      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry/${encodeURIComponent(sym)}/live_reserves`, {
        method: "GET",
        headers: { accept: "application/json" },
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      setRouteTestResult({
        kind: "live_reserves",
        ok: !!j?.ok,
        symbol: sym,
        source: j?.source || "—",
        baseReserve: j?.baseReserve ?? "—",
        quoteReserve: j?.quoteReserve ?? "—",
        spot: j?.spotPrice ?? "—",
        inverse: j?.inversePrice ?? "—",
        liquidityWarning: buildHydrationLowLiquidityWarning({
          source: j?.source,
          router: "manual_xyk",
          routeModeEffective: "manual_xyk",
          pool: {
            source: j?.source,
            poolAccount: j?.poolAccount || row?.poolAccount || row?.pool_account || routePoolAccount || "",
            baseReserve: j?.baseReserve,
            quoteReserve: j?.quoteReserve,
            spotPrice: j?.spotPrice,
            inversePrice: j?.inversePrice,
          },
        }),
        poolAccount: j?.poolAccount || row?.poolAccount || row?.pool_account || routePoolAccount || "",
      });
    } catch (e) {
      const msg = String(e?.message || e);
      setRouteErr(msg);
      setRouteTestResult({ kind: "live_reserves", ok: false, symbol: sym, error: msg });
    }
  }, [API_BASE, routeSymbol, routePoolAccount]);


  const onChainChange = useCallback((nextChain) => {
    setChain(nextChain);
    setEditId(null);
    setEditRow({ symbol: "", address: "", decimals: "", label: "", venue: "", external_price_source: "", external_price_id: "" });
    setErr(null);
    setRouteErr(null);
    setRouteTestResult(null);
  }, []);

  return (
    <div className="utt-token-registry-cyber" style={tokenRegistryRootStyle}>
      <style>{tokenRegistryCyberCss}</style>
      <div style={tokenRegistryHeaderStyle}>
        <div style={{ minWidth: 0 }}>
          <div style={terminalEyebrowStyle}>UTT // IDENTITY MATRIX</div>
          <div style={tokenRegistryTitleStyle}>Token / Symbol Registry</div>
          <div style={tokenRegistrySubtitleStyle}>Canonical asset metadata · operator-controlled · read-only identity layer</div>
        </div>
        <div style={tokenRegistryHeaderActionsStyle}>
          <select value={chain} onChange={(e) => onChainChange(e.target.value)} style={headerSelectStyle} aria-label="Registry chain">
            {CHAIN_OPTIONS.map((opt) => (
              <option key={opt} value={opt} style={selectOptionStyle}>{chainDisplayName(opt)}</option>
            ))}
          </select>
          <button type="button" onClick={load} style={headerBtnStyle} disabled={loading}>
            {loading ? "Loading…" : "Refresh"}
          </button>
          {onClose && (
            <button type="button" onClick={onClose} style={headerBtnStyle}>
              Close
            </button>
          )}
        </div>
      </div>

      <div style={chainIdentityPanelStyle}>
        <div style={chainIdentityHeaderStyle}>
          <div>
            <div style={chainIdentityCodeStyle}>{activeChainProfile.code}</div>
            <div style={chainIdentityNameStyle}>{activeChainProfile.name}</div>
          </div>
          <div style={readOnlyBadgeStyle}>IDENTITY ONLY</div>
        </div>
        <div style={chainBadgeWrapStyle}>
          {activeChainProfile.badges.map((badge) => (
            <span key={badge} style={chainBadgeStyle}>{badge}</span>
          ))}
        </div>
        <div style={chainIdentityDetailStyle}>{activeChainProfile.detail}</div>
      </div>

      {visibleSuggestions.length ? (
        <div style={panelStyle}>
          <div style={{ fontWeight: 700, marginBottom: 8 }}>Detected suggestions</div>
          <div style={{ fontSize: 12, opacity: 0.8, marginBottom: 8 }}>Detected from recent Solana order rows. One-click add when symbol + decimals are available, or prefill the form to review.</div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0 }}>
              <thead>
                <tr>
                  <th style={thStyle}>Symbol</th>
                  <th style={thStyle}>{GENERIC_ADDRESS_LABEL}</th>
                  <th style={thStyle}>Decimals</th>
                  <th style={thStyle}>Source</th>
                  <th style={{ ...thStyle, width: 240 }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {visibleSuggestions.slice(0, 50).map((sug) => {
                  const ready = !!String(sug?.symbol || "").trim() && Number.isFinite(Number(sug?.decimals));
                  return (
                    <tr key={String(sug?.address || Math.random())}>
                      <td style={tdStyle}><span style={{ fontWeight: 700, opacity: sug?.symbol ? 1 : 0.55 }}>{sug?.symbol || "(symbol unknown)"}</span></td>
                      <td style={tdStyle}><code style={codeStyle} title={String(sug?.address || "")}>{String(sug?.address || "")}</code></td>
                      <td style={tdStyle}><span style={{ opacity: Number.isFinite(Number(sug?.decimals)) ? 1 : 0.55 }}>{Number.isFinite(Number(sug?.decimals)) ? Number(sug.decimals) : "—"}</span></td>
                      <td style={tdStyle}><span style={{ opacity: 0.8 }}>{String(sug?.sourceSymbol || "") || "—"}</span></td>
                      <td style={tdStyle}>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <button type="button" style={btnStyle} onClick={() => useSuggestion(sug)}>Use</button>
                          <button type="button" style={btnStyle} onClick={() => addSuggestion(sug)} disabled={!ready || saving}>{saving ? "Saving…" : "Add"}</button>
                          <button type="button" style={btnStyle} onClick={() => setDismissed((prev) => { const n = new Set(prev); n.add(String(sug?.address || "")); return n; })}>Dismiss</button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}

      <div style={panelStyle}>
        <div style={panelHeadingRowStyle}>
          <div>
            <div style={panelEyebrowStyle}>REGISTRY WRITE PREVIEW</div>
            <div style={panelTitleStyle}>Add token identity</div>
          </div>
          {isRobinhoodChain(chain) ? (
            <button
              type="button"
              onClick={useRobinhoodNativeEthPreset}
              style={presetBtnStyle}
              disabled={saving}
              title="Fill the native ETH identity without saving it"
            >
              Load native ETH preset
            </button>
          ) : null}
        </div>
        <div style={addTokenGridStyle}>
          <input value={symbol} onChange={(e) => setSymbol(e.target.value)} placeholder="SYMBOL (e.g. UTTT)" style={inputStyle} />
          <input
            value={address}
            onChange={(e) => setAddress(e.target.value)}
            placeholder={addressPlaceholderForChain(chain, symbol)}
            aria-label={addressLabelForChain(chain)}
            style={inputStyle}
          />
          <input value={decimals} onChange={(e) => setDecimals(e.target.value)} placeholder="decimals" style={inputStyle} />
          <input value={label} onChange={(e) => setLabel(e.target.value)} placeholder="display label (optional)" style={inputStyle} />
          <input value={venue} onChange={(e) => setVenue(e.target.value)} placeholder="venue override" style={inputStyle} />
          <select value={externalPriceSource} onChange={(e) => setExternalPriceSource(e.target.value)} style={selectStyle} title="External price source">
            {EXTERNAL_PRICE_SOURCE_OPTIONS.map((opt) => (
              <option key={opt || "blank"} value={opt} style={selectOptionStyle}>{opt ? externalPriceSourceLabel(opt) : "Price source"}</option>
            ))}
          </select>
          <input value={externalPriceId} onChange={(e) => setExternalPriceId(e.target.value)} placeholder="price ID (hydradx)" style={inputStyle} />
          <button type="button" onClick={onAdd} style={btnStyle} disabled={!canAdd || saving}>
            {saving ? "Saving…" : "Add"}
          </button>
        </div>
        <div style={tokenValidationStyle(tokenIdentityValidation.ok)}>
          <span style={tokenValidationDotStyle(tokenIdentityValidation.ok)} />
          <span>{tokenIdentityValidation.message}</span>
        </div>
        <div style={{ marginTop: 8, fontSize: 12, opacity: 0.78, lineHeight: 1.5 }}>
          Tip: leave “venue override” blank for global entries. For Hydration, use polkadot_hydration and put the asset ID (or native for HDX) in Address / Mint / Asset ID. For Counterparty price metadata, the identifier may be blank; use XCP = CoinGecko / counterparty. Price source examples: HDX = CoinGecko / hydradx, DOT = CoinGecko / polkadot, USDT = Stable / stable, UTTT = Derived / UTTT-HDX×HDX-USD.
        </div>
        {isRobinhoodChain(chain) ? (
          <div style={robinhoodHelpStyle}>
            <b>Robinhood Chain rules:</b> native ETH must be <code style={codeStyle}>ETH</code>, blank contract address, and 18 decimals. Every other symbol is treated as an ERC-20 identity and requires an exact <code style={codeStyle}>0x</code> + 40-hex contract address. Preset loading never writes to the database.
          </div>
        ) : null}
        {chain !== "solana" && (
          <div style={{ marginTop: 6, fontSize: 12, opacity: 0.75 }}>
            Selected chain: <code style={codeStyle}>{chain}</code>{activeVenueFilter ? <> · default DEX venue filter: <code style={codeStyle}>{activeVenueFilter}</code></> : null}
          </div>
        )}
      </div>

      {showHydrationRoutes && (
        <div style={panelStyle}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center", marginBottom: 8 }}>
            <div>
              <div style={{ fontWeight: 700 }}>Hydration Route Registry</div>
              <div style={{ marginTop: 3, fontSize: 12, opacity: 0.72 }}>
                Manual XYK rows handle reserve-based pools. Manual Router rows handle confirmed multi-leg paths like DOT → aDOT → HDX.
              </div>
            </div>
            <button type="button" onClick={() => { loadRoutes(); loadRouteTemplates(); }} style={btnStyle} disabled={routeLoading || routeTemplateLoading}>
              {routeLoading || routeTemplateLoading ? "Loading…" : "Refresh routes"}
            </button>
          </div>

          <div style={routeFormGridStyle}>
            <input value={routeSymbol} onChange={(e) => setRouteSymbol(e.target.value)} placeholder="PAIR (UTTT-HDX)" style={inputStyle} />
            <select
              value={routeMode}
              onChange={(e) => {
                const mode = e.target.value;
                setRouteMode(mode);
                setRoutePoolType(mode === "manual_router" ? "Router" : "XYK");
                if (mode === "manual_router") {
                  setRouteBaseReserve("");
                  setRouteQuoteReserve("");
                  if (hasKnownHydrationRouteTemplate(routeSymbol)) {
                    setRouteJsonText(safeJsonPretty(hydrationRouteTemplateForSymbol(routeSymbol)));
                  }
                }
              }}
              style={selectStyle}
              title="Hydration route mode"
            >
              <option value="manual_xyk" style={selectOptionStyle}>Manual XYK</option>
              <option value="manual_router" style={selectOptionStyle}>Manual Router</option>
            </select>
            <select value={routePoolType} onChange={(e) => setRoutePoolType(e.target.value)} style={selectStyle} title="Pool type">
              <option value={routeMode === "manual_router" ? "Router" : "XYK"} style={selectOptionStyle}>{routeMode === "manual_router" ? "Router" : "XYK"}</option>
            </select>
            <input
              value={routeBaseReserve}
              onChange={(e) => setRouteBaseReserve(e.target.value)}
              placeholder={routeMode === "manual_router" ? "not used: route JSON below" : "base reserve"}
              title={routeMode === "manual_router" ? "Manual Router rows do not use reserve fields. Use the route JSON box below." : "Base reserve in human units for Manual XYK rows."}
              style={{ ...inputStyle, opacity: routeMode === "manual_router" ? 0.58 : 1 }}
              disabled={routeMode === "manual_router"}
            />
            <input
              value={routeQuoteReserve}
              onChange={(e) => setRouteQuoteReserve(e.target.value)}
              placeholder={routeMode === "manual_router" ? "not used: route JSON below" : "quote reserve"}
              title={routeMode === "manual_router" ? "Manual Router rows do not use reserve fields. Use the route JSON box below." : "Quote reserve in human units for Manual XYK rows."}
              style={{ ...inputStyle, opacity: routeMode === "manual_router" ? 0.58 : 1 }}
              disabled={routeMode === "manual_router"}
            />
            <input value={routeFeeBps} onChange={(e) => setRouteFeeBps(e.target.value)} placeholder="fee bps" style={inputStyle} />
            <input value={routePoolAccount} onChange={(e) => setRoutePoolAccount(e.target.value)} placeholder="pool account (optional live reserves)" title={routePoolAccount} style={{ ...inputStyle, fontFamily: codeStyle.fontFamily, fontSize: 11 }} />
            <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, whiteSpace: "nowrap" }}>
              <input type="checkbox" checked={!!routeEnabled} onChange={(e) => setRouteEnabled(e.target.checked)} /> Enabled
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, whiteSpace: "nowrap" }} title="Only mark confirmed after a tiny on-chain success for this exact route direction.">
              <input type="checkbox" checked={!!routeConfirmed} onChange={(e) => setRouteConfirmed(e.target.checked)} /> Confirmed
            </label>
            <input value={routeNote} onChange={(e) => setRouteNote(e.target.value)} placeholder="note (optional)" style={inputStyle} />
            <select
              value={selectedRouteTemplateId}
              onChange={(e) => setSelectedRouteTemplateId(e.target.value)}
              style={selectStyle}
              title="Built-in and saved Hydration route templates"
            >
              <option value="" style={selectOptionStyle}>Template for current pair / blank</option>
              {routeTemplateOptions.map((tpl) => (
                <option key={tpl.id} value={tpl.id} style={selectOptionStyle}>
                  {(tpl.warningCount || tpl.warnings?.length) ? "⚠ " : ""}{tpl.sourceType === "saved_registry" || tpl.source === "saved_route_registry" ? "Saved · " : (tpl.sourceType === "fallback" ? "Fallback · " : "Built-in · ")}{tpl.label}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => {
                if (selectedRouteTemplate) {
                  applyRouteTemplate(selectedRouteTemplate, "load");
                  return;
                }
                const sym = String(routeSymbol || "").trim().toUpperCase();
                const matchingTemplate = routeTemplateOptions.find((tpl) => String(tpl.symbol || "").toUpperCase() === sym);
                if (matchingTemplate) {
                  applyRouteTemplate(matchingTemplate, "load");
                  return;
                }
                const routeJson = hydrationRouteTemplateForSymbol(sym);
                const fallback = normalizeHydrationRouteTemplate({
                  id: `fallback:${sym || "route"}`,
                  source: "fallback",
                  sourceType: "fallback",
                  label: sym ? `${sym} fallback template` : "Fallback route template",
                  symbol: sym,
                  routeMode: "manual_router",
                  poolType: "Router",
                  routeJson,
                  note: "Fallback template loaded from current pair. Validate before saving.",
                });
                applyRouteTemplate(fallback, "load");
              }}
              style={btnStyle}
              disabled={routeSaving || routeValidating || routeReversing || routeTemplateLoading}
            >
              {routeTemplateLoading ? "Loading templates…" : "Load selected template"}
            </button>
            <button
              type="button"
              onClick={() => {
                if (selectedRouteTemplate) {
                  applyRouteTemplate(selectedRouteTemplate, "clone");
                }
              }}
              style={btnStyle}
              disabled={!selectedRouteTemplate || routeSaving || routeValidating || routeReversing || routeTemplateLoading}
              title="Clone the selected template into the form without saving. Confirmed remains unchecked."
            >
              Clone selected
            </button>
            <button type="button" onClick={validateRoute} style={btnStyle} disabled={!canUpsertRoute || routeSaving || routeValidating || routeReversing}>
              {routeValidating ? "Validating…" : "Validate route"}
            </button>
            <button type="button" onClick={reverseRoute} style={btnStyle} disabled={!canUpsertRoute || routeSaving || routeValidating || routeReversing}>
              {routeReversing ? "Reversing…" : "Reverse route"}
            </button>
            <button type="button" onClick={upsertRoute} style={btnStyle} disabled={!canUpsertRoute || routeSaving || routeValidating || routeReversing}>
              {routeSaving ? "Saving…" : "Save route"}
            </button>
          </div>
          {selectedRouteTemplate ? (
            <div style={{ marginTop: 8, padding: 8, borderRadius: 10, border: "1px solid rgba(96,165,250,0.22)", background: "rgba(15,23,42,0.55)", fontSize: 12 }}>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
                <span style={{ display: "inline-flex", alignItems: "center", padding: "2px 6px", borderRadius: 999, fontSize: 10, lineHeight: 1.3, ...routeBadgeStyle(routeTemplateSourceBadgeSeverity(selectedRouteTemplate)) }}>
                  {selectedRouteTemplate.sourceLabel || routeTemplateSourceLabel(selectedRouteTemplate.sourceType)}
                </span>
                <b>{selectedRouteTemplate.symbol || "—"}</b>
                {selectedRouteTemplate.routeDirection ? <span style={{ opacity: 0.8 }}>{selectedRouteTemplate.routeDirection}</span> : null}
                {selectedRouteTemplate.sourceConfirmed ? <span style={{ color: "#b8f7c7" }}>source confirmed</span> : <span style={{ color: "#ffe2a6" }}>source not confirmed</span>}
                {selectedRouteTemplate.liquidityClass === "high_liquidity_candidate" ? <span style={{ color: "#b8f7c7" }}>high-liquidity candidate</span> : null}
              </div>
              <div style={{ marginTop: 5, color: "#c7d2fe" }}>
                {selectedRouteTemplate.recommendedNextAction || "Load/clone, validate, then confirm only after a tiny live test."}
              </div>
              {selectedRouteTemplate.warnings?.length ? (
                <div style={{ marginTop: 6, display: "grid", gap: 4 }}>
                  {selectedRouteTemplate.warnings.slice(0, 5).map((warning, idx) => (
                    <div key={`${selectedRouteTemplate.id}-warning-${idx}`} style={{ padding: "4px 6px", borderRadius: 8, border: "1px solid rgba(245,158,11,0.40)", background: "rgba(120,72,16,0.16)", color: "#ffe2a6" }}>
                      ⚠ {routeWarningMessage(warning)}
                    </div>
                  ))}
                </div>
              ) : null}
              {selectedRouteTemplate.templateNotes?.length ? (
                <div style={{ marginTop: 6, display: "grid", gap: 3, color: "rgba(226,232,240,0.78)" }}>
                  {selectedRouteTemplate.templateNotes.slice(0, 4).map((note, idx) => (
                    <div key={`${selectedRouteTemplate.id}-note-${idx}`}>• {note}</div>
                  ))}
                </div>
              ) : null}
            </div>
          ) : null}
          {routeMode === "manual_router" && (
            <div style={{ marginTop: 8 }}>
              <textarea
                value={routeJsonText}
                onChange={(e) => setRouteJsonText(e.target.value)}
                placeholder={'Route JSON, e.g. [{"pool":{"type":"Aave"},"assetIn":5,"assetOut":1001},{"pool":{"type":"Omnipool"},"assetIn":1001,"assetOut":0}]'}
                style={routeJsonTextAreaStyle}
                spellCheck={false}
              />
            </div>
          )}
          <div style={{ marginTop: 6, fontSize: 12, opacity: 0.72 }}>
            Manual XYK reserves are human units. Manual Router rows do not use the reserve fields — they use the route JSON box instead. DOT routes use DOT(5) ↔ aDOT(1001) through Aave, then Omnipool; USDT templates use Hydration asset 10. Click “Load route template” after entering the pair, then mark Confirmed only after a tiny live on-chain success.
          </div>
          {routeErr && <div style={{ marginTop: 8, color: "#ffb3b3", fontSize: 12 }}>{routeErr}</div>}
          {routeTestResult && (
            <div style={{ marginTop: 8, padding: 8, borderRadius: 10, border: "1px solid rgba(255,255,255,0.10)", background: routeTestResult.ok ? "rgba(20,80,45,0.25)" : "rgba(120,30,30,0.25)", fontSize: 12 }}>
              <div style={{ fontWeight: 700, marginBottom: 6 }}>
                {routeTestResult.kind === "validation" ? "Route validation" : (routeTestResult.kind === "reverse_preview" ? "Route reverse preview" : (routeTestResult.kind === "template" ? (routeTestResult.templateAction === "clone" ? "Route template cloned" : "Route template loaded") : (routeTestResult.kind === "live_reserves" ? "Live reserve test" : "Manual route orderbook test")))}: {routeTestResult.symbol || "—"}
              </div>
              {routeTestResult.error ? (
                <div style={{ color: "#ffb3b3" }}>{routeTestResult.error}</div>
              ) : (routeTestResult.kind === "validation" || routeTestResult.kind === "reverse_preview" || routeTestResult.kind === "template") ? (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 6 }}>
                  <div>Validation: <code style={codeStyle}>{routeTestResult.ok ? "OK" : "Failed"}</code></div>
                  <div>Mode: <code style={codeStyle}>{routeTestResult.routeMode || "—"}</code></div>
                  <div>Legs: <code style={codeStyle}>{routeTestResult.validation?.legCount ?? "—"}</code></div>
                  <div style={{ gridColumn: "1 / -1" }}>Direction: <code style={codeStyle}>{routeTestResult.direction?.label || "—"}</code></div>
                  {routeTestResult.template?.sourceLabel || routeTestResult.template?.source ? (
                    <>
                      <div>Template source: <code style={codeStyle}>{routeTestResult.template.sourceLabel || routeTestResult.template.source}</code></div>
                      <div>Source confirmed: <code style={codeStyle}>{routeTestResult.template.sourceConfirmed ? "yes" : "no"}</code></div>
                      {routeTestResult.template.sourceExecutable != null ? (
                        <div>Source executable: <code style={codeStyle}>{routeTestResult.template.sourceExecutable ? "yes" : "no"}</code></div>
                      ) : null}
                      {routeTestResult.template.recommendedNextAction ? (
                        <div style={{ gridColumn: "1 / -1", color: "#c7d2fe" }}>Next: {routeTestResult.template.recommendedNextAction}</div>
                      ) : null}
                    </>
                  ) : null}
                  {routeTestResult.message ? (
                    <div style={{ gridColumn: "1 / -1", color: "#c7d2fe" }}>{routeTestResult.message}</div>
                  ) : null}
                  {routeTestResult.kind === "reverse_preview" && routeTestResult.original?.direction?.label ? (
                    <div style={{ gridColumn: "1 / -1" }}>Original: <code style={codeStyle}>{routeTestResult.original.direction.label}</code></div>
                  ) : null}
                  {routeTestResult.warnings?.length ? (
                    <div style={{ gridColumn: "1 / -1", padding: 6, borderRadius: 8, border: "1px solid rgba(245,158,11,0.45)", background: "rgba(120,72,16,0.18)", color: "#ffe2a6" }}>
                      <b>Warnings</b>
                      <div style={{ display: "grid", gap: 4, marginTop: 4 }}>
                        {routeTestResult.warnings.map((warning, idx) => (
                          <div key={`route-warning-${idx}`}>⚠ {routeWarningMessage(warning)}</div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {routeTestResult.errors?.length ? (
                    <div style={{ gridColumn: "1 / -1", padding: 6, borderRadius: 8, border: "1px solid rgba(248,113,113,0.45)", background: "rgba(120,30,30,0.18)", color: "#ffdddd" }}>
                      <b>Errors</b>
                      <pre style={routeValidationPreStyle}>{safeJsonPretty(routeTestResult.errors)}</pre>
                    </div>
                  ) : null}
                  <div style={{ gridColumn: "1 / -1" }}>
                    <b>Normalized route</b>
                    <pre style={routeValidationPreStyle}>{safeJsonPretty(routeTestResult.normalizedRoute)}</pre>
                  </div>
                </div>
              ) : (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 6 }}>
                  {routeTestResult.router ? <div>Router: <code style={codeStyle}>{routeTestResult.router}</code></div> : null}
                  {routeTestResult.effective ? <div>Effective: <code style={codeStyle}>{routeTestResult.effective}</code></div> : null}
                  <div>Source: <code style={codeStyle}>{routeTestResult.source || "—"}</code></div>
                  <div>Spot: <code style={codeStyle}>{routeTestResult.spot ?? "—"}</code></div>
                  <div>Inverse: <code style={codeStyle}>{routeTestResult.inverse ?? "—"}</code></div>
                  {routeTestResult.baseReserve != null ? <div>Base reserve: <code style={codeStyle}>{routeTestResult.baseReserve}</code></div> : null}
                  {routeTestResult.quoteReserve != null ? <div>Quote reserve: <code style={codeStyle}>{routeTestResult.quoteReserve}</code></div> : null}
                  {routeTestResult.bid != null ? <div>Bid: <code style={codeStyle}>{routeTestResult.bid}</code></div> : null}
                  {routeTestResult.ask != null ? <div>Ask: <code style={codeStyle}>{routeTestResult.ask}</code></div> : null}
                  {routeTestResult.liveReservesOk != null ? <div>Live reserves: <code style={codeStyle}>{routeTestResult.liveReservesOk ? "ok" : "not active"}</code></div> : null}
                  {routeTestResult.liquidityWarning ? (
                    <div style={{ gridColumn: "1 / -1", padding: 6, borderRadius: 8, border: "1px solid rgba(245,158,11,0.45)", background: "rgba(120,72,16,0.18)", color: "#ffe2a6" }}>
                      ⚠ <b>{routeTestResult.liquidityWarning.label}</b> · {routeTestResult.liquidityWarning.message}
                    </div>
                  ) : null}
                  {routeTestResult.poolAccount ? (
                    <div style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
                      <span>Pool:</span>
                      <code style={poolAccountCodeStyle} title={String(routeTestResult.poolAccount || "")}>{compactMiddle(routeTestResult.poolAccount)}</code>
                      <button type="button" style={miniBtnStyle} onClick={() => copyText(routeTestResult.poolAccount)}>Copy</button>
                    </div>
                  ) : null}
                </div>
              )}
            </div>
          )}

          <div style={{ marginTop: 10, overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0 }}>
              <thead>
                <tr>
                  <th style={thStyle}>Pair</th>
                  <th style={thStyle}>Assets</th>
                  <th style={thStyle}>Reserves</th>
                  <th style={thStyle}>Fee</th>
                  <th style={thStyle}>Source / Pool</th>
                  <th style={{ ...thStyle, width: 250 }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {(routes || []).map((row) => (
                  <tr key={row.id} style={{ borderTop: "1px solid rgba(255,255,255,0.08)", opacity: row.enabled === false ? 0.58 : 1 }}>
                    <td style={tdStyle}>
                      <div style={{ fontWeight: 700 }}>{row.symbol}</div>
                      <div style={{ fontSize: 11, opacity: 0.7 }}>
                          {row.enabled === false ? "disabled" : routeModeLabel(row.routeMode || "manual_xyk")}
                          {row.confirmed ? " · confirmed" : ""}
                        </div>
                      <div style={{ marginTop: 4, fontSize: 11, color: "#d6f5ff", opacity: 0.9 }} title="Saved route direction">
                        {routeDirectionLabel(row)}
                      </div>
                      <div style={{ marginTop: 5 }}>
                        <span style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "2px 6px", borderRadius: 999, fontSize: 10, lineHeight: 1.3, ...routeBadgeStyle(routeExecutionStatus(row).severity) }}>
                          {routeExecutionStatus(row).label}
                        </span>
                      </div>
                    </td>
                    <td style={tdStyle}>
                      <code style={codeStyle}>{row.baseAssetId}</code> → <code style={codeStyle}>{row.quoteAssetId}</code>
                    </td>
                    <td style={tdStyle}>
                      {String(row.routeMode || "").toLowerCase() === "manual_router" ? (
                        <div style={{ opacity: 0.75 }}>route JSON</div>
                      ) : (
                        <>
                          <div>{Number(row.baseReserve || 0).toLocaleString()} {row.baseSymbol}</div>
                          <div>{Number(row.quoteReserve || 0).toLocaleString()} {row.quoteSymbol}</div>
                        </>
                      )}
                    </td>
                    <td style={tdStyle}>{row.feeBps ?? "—"} bps</td>
                    <td style={tdStyle}>
                      <div>
                        <span style={{ opacity: 0.8 }}>{row.poolType || (String(row.routeMode || "").toLowerCase() === "manual_router" ? "Router" : "XYK")}</span>
                        {Array.isArray(row.route) && row.route.length ? <span style={{ opacity: 0.55 }}> · {row.route.length} leg{row.route.length === 1 ? "" : "s"}</span> : null}
                        {row.confirmed ? <span style={{ opacity: 0.65, color: "#b8f7c7" }}> · confirmed</span> : null}
                        {(row.poolAccount || row.pool_account) ? <span style={{ opacity: 0.55 }}> · live pool account</span> : null}
                      </div>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginTop: 5 }}>
                        <span style={{ display: "inline-flex", alignItems: "center", padding: "2px 6px", borderRadius: 999, fontSize: 10, lineHeight: 1.3, ...routeBadgeStyle(routeExecutionStatus(row).severity) }}>
                          {routeExecutionStatus(row).label}
                        </span>
                        <span style={{ display: "inline-flex", alignItems: "center", padding: "2px 6px", borderRadius: 999, fontSize: 10, lineHeight: 1.3, ...routeBadgeStyle("info") }} title="Route direction">
                          {routeDirectionLabel(row)}
                        </span>
                      </div>
                      {(row.poolAccount || row.pool_account) ? (
                        <>
                          <div style={poolAccountWrapStyle}>
                            <code style={poolAccountCodeStyle} title={String(row.poolAccount || row.pool_account)}>{compactMiddle(row.poolAccount || row.pool_account)}</code>
                            <button type="button" style={miniBtnStyle} onClick={() => copyText(row.poolAccount || row.pool_account)}>Copy</button>
                          </div>
                          <div style={{ marginTop: 4, color: "#ffe2a6", fontSize: 11 }}>⚠ monitor isolated-pool TVL</div>
                        </>
                      ) : (
                        <span style={{ opacity: 0.55 }}>{String(row.routeMode || "").toLowerCase() === "manual_router" ? "manual Router path" : "snapshot only"}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                        <button type="button" style={btnStyle} onClick={() => useRoute(row)}>Use</button>
                        <button type="button" style={btnStyle} onClick={() => testRouteOrderbook(row)}>Test orderbook</button>
                        <button type="button" style={btnStyle} onClick={() => testRouteLiveReserves(row)} disabled={String(row.routeMode || "").toLowerCase() === "manual_router"}>Live reserves</button>
                        <button type="button" style={dangerBtnStyle} onClick={() => deleteRoute(row)} disabled={routeSaving}>Delete</button>
                      </div>
                    </td>
                  </tr>
                ))}
                {!routes?.length && (
                  <tr>
                    <td colSpan={6} style={{ ...tdStyle, opacity: 0.7 }}>
                      No manual Hydration routes yet. Add manual XYK rows for custom pools or manual Router rows for confirmed multi-leg paths.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button type="button" style={btnStyle} onClick={() => testRouteOrderbook(null)} disabled={!String(routeSymbol || "").trim()}>
              Test form pair
            </button>
            <button type="button" style={btnStyle} onClick={() => testRouteLiveReserves(null)} disabled={!String(routeSymbol || "").trim()}>
              Test live reserves
            </button>
            <button type="button" style={btnStyle} onClick={clearRouteForm}>Clear route form</button>
          </div>
        </div>
      )}

      {err && <div style={{ ...panelStyle, borderColor: "rgba(255,120,120,0.35)", background: "rgba(40,10,10,0.45)" }}>{err}</div>}

      <div style={{ marginTop: 10 }}>
        <div style={panelHeadingRowStyle}>
          <div>
            <div style={panelEyebrowStyle}>CANONICAL RECORDS</div>
            <div style={panelTitleStyle}>Mappings</div>
          </div>
          <div style={mappingCountBadgeStyle}>{items?.length || 0} ROWS</div>
        </div>

        <div style={{ overflowX: "auto", marginTop: 8 }}>
          <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0 }}>
            <thead>
              <tr>
                <th style={thStyle}>Symbol</th>
                <th style={thStyle}>{addressLabelForChain(chain)}</th>
                <th style={thStyle}>Decimals</th>
                <th style={thStyle}>Label</th>
                <th style={thStyle}>Venue</th>
                <th style={thStyle}>Price Src</th>
                <th style={thStyle}>Price ID</th>
                <th style={{ ...thStyle, width: 240 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {(items || []).map((row) => {
                const isEdit = String(editId || "") === String(row?.id || "");
                return (
                  <tr key={row.id} style={{ borderTop: "1px solid rgba(255,255,255,0.08)" }}>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.symbol} onChange={(e) => setEditRow((p) => ({ ...p, symbol: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span style={{ fontWeight: 700 }}>{row.symbol}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.address} onChange={(e) => setEditRow((p) => ({ ...p, address: e.target.value }))} style={inputStyle} />
                      ) : (
                        <code style={codeStyle}>{row.address || (isRobinhoodChain(chain) && String(row.symbol || "").toUpperCase() === ROBINHOOD_CHAIN_NATIVE_SYMBOL ? "native · no contract" : "—")}</code>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.decimals} onChange={(e) => setEditRow((p) => ({ ...p, decimals: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span>{row.decimals}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.label} onChange={(e) => setEditRow((p) => ({ ...p, label: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span style={{ opacity: row.label ? 1 : 0.55 }}>{row.label || "—"}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.venue} onChange={(e) => setEditRow((p) => ({ ...p, venue: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span style={{ opacity: row.venue ? 1 : 0.55 }}>{row.venue || "—"}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <select value={editRow.external_price_source || ""} onChange={(e) => setEditRow((p) => ({ ...p, external_price_source: e.target.value }))} style={selectStyle}>
                          {EXTERNAL_PRICE_SOURCE_OPTIONS.map((opt) => (
                            <option key={opt || "blank"} value={opt} style={selectOptionStyle}>{opt ? externalPriceSourceLabel(opt) : "—"}</option>
                          ))}
                        </select>
                      ) : (
                        <span style={{ opacity: row.external_price_source ? 1 : 0.55 }}>{externalPriceSourceLabel(row.external_price_source)}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.external_price_id || ""} onChange={(e) => setEditRow((p) => ({ ...p, external_price_id: e.target.value }))} placeholder="hydradx" style={inputStyle} />
                      ) : (
                        <code style={codeStyle}>{row.external_price_id || "—"}</code>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {!isEdit ? (
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <button type="button" style={btnStyle} onClick={() => testResolve(row.symbol)}>
                            {chain === "counterparty" ? "Test price" : (isRobinhoodChain(chain) ? "Validate identity" : "Test resolve")}
                          </button>
                          <button type="button" style={btnStyle} onClick={() => startEdit(row)}>
                            Edit
                          </button>
                          <button type="button" style={dangerBtnStyle} onClick={() => delRow(row)}>
                            Delete
                          </button>
                        </div>
                      ) : (
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <button type="button" style={btnStyle} onClick={saveEdit} disabled={saving}>
                            {saving ? "Saving…" : "Save"}
                          </button>
                          <button type="button" style={btnStyle} onClick={cancelEdit} disabled={saving}>
                            Cancel
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
              {!items?.length && (
                <tr>
                  <td colSpan={8} style={{ ...tdStyle, opacity: 0.7 }}>
                    No mappings yet. Add a symbol + identifier + decimals above.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={{ marginTop: 8, fontSize: 12, opacity: 0.7 }}>
          Backend: <code style={codeStyle}>/api/token_registry</code>
        </div>
      </div>
    </div>
  );
}

const tokenRegistryRootStyle = {
  position: "relative",
  isolation: "isolate",
  minWidth: 0,
  maxWidth: "100%",
  color: "var(--utt-page-fg, var(--utt-text, #e9eef7))",
  padding: 12,
  borderRadius: 16,
  border: "1px solid rgba(34,211,238,0.22)",
  background: "linear-gradient(145deg, rgba(2,8,16,0.96), rgba(7,18,28,0.94) 48%, rgba(10,10,24,0.96))",
  boxShadow: "inset 0 0 0 1px rgba(125,211,252,0.035), 0 18px 44px rgba(0,0,0,0.34)",
  overflow: "hidden",
};

const panelStyle = {
  position: "relative",
  marginTop: 12,
  padding: 12,
  borderRadius: 12,
  border: "1px solid rgba(34,211,238,0.18)",
  background: "linear-gradient(180deg, rgba(8,23,35,0.82), rgba(3,12,22,0.82))",
  boxShadow: "inset 3px 0 0 rgba(34,211,238,0.12), inset 0 1px 0 rgba(255,255,255,0.025)",
};

const addTokenGridStyle = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(135px, 1fr))",
  gap: 8,
  alignItems: "center",
  maxWidth: "100%",
};

const routeFormGridStyle = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(130px, 1fr))",
  gap: 8,
  alignItems: "center",
  maxWidth: "100%",
};

const routeJsonTextAreaStyle = {
  width: "100%",
  minHeight: 92,
  minWidth: 0,
  boxSizing: "border-box",
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,0.12)",
  background: "rgba(0,0,0,0.25)",
  color: "var(--utt-text, #e9eef7)",
  outline: "none",
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
  fontSize: 11,
  resize: "vertical",
};

const routeValidationPreStyle = {
  margin: "6px 0 0",
  padding: 8,
  borderRadius: 8,
  background: "rgba(0,0,0,0.22)",
  color: "inherit",
  fontSize: 11,
  lineHeight: 1.35,
  overflowX: "auto",
  whiteSpace: "pre-wrap",
  wordBreak: "break-word",
};


const inputStyle = {
  width: "100%",
  minWidth: 0,
  padding: "8px 10px",
  borderRadius: 8,
  border: "1px solid rgba(56,189,248,0.20)",
  background: "rgba(1,8,15,0.78)",
  color: "var(--utt-page-fg, var(--utt-text, #e9eef7))",
  outline: "none",
  boxSizing: "border-box",
  boxShadow: "inset 0 0 18px rgba(14,116,144,0.05)",
};

const selectStyle = {
  width: "100%",
  minWidth: 0,
  boxSizing: "border-box",
  padding: "8px 10px",
  borderRadius: 8,
  border: "1px solid rgba(56,189,248,0.20)",
  background: "rgba(1,8,15,0.78)",
  backgroundColor: "var(--utt-control-bg, #07111b)",
  color: "var(--utt-page-fg, var(--utt-text, #e9eef7))",
  colorScheme: "dark",
  outline: "none",
};

const selectOptionStyle = {
  backgroundColor: "#111821",
  color: "var(--utt-text, #e9eef7)",
};

const btnStyle = {
  minWidth: 0,
  fontSize: 12,
  fontWeight: 700,
  letterSpacing: "0.02em",
  padding: "8px 10px",
  borderRadius: 8,
  border: "1px solid rgba(56,189,248,0.24)",
  background: "linear-gradient(180deg, rgba(14,116,144,0.20), rgba(8,47,73,0.16))",
  color: "var(--utt-page-fg, var(--utt-text, #e9eef7))",
  cursor: "pointer",
};


const tokenRegistryHeaderStyle = {
  position: "relative",
  zIndex: 1,
  display: "flex",
  justifyContent: "space-between",
  gap: 14,
  alignItems: "center",
  marginBottom: 12,
  padding: "10px 12px",
  flexWrap: "wrap",
  minWidth: 0,
  maxWidth: "100%",
  border: "1px solid rgba(34,211,238,0.24)",
  borderRadius: 12,
  background: "linear-gradient(90deg, rgba(8,47,73,0.50), rgba(30,27,75,0.28), rgba(2,8,23,0.70))",
  boxShadow: "inset 4px 0 0 rgba(34,211,238,0.55), 0 0 28px rgba(6,182,212,0.06)",
};

const tokenRegistryHeaderActionsStyle = {
  display: "flex",
  gap: 8,
  alignItems: "center",
  justifyContent: "flex-end",
  flexWrap: "wrap",
  minWidth: 0,
  maxWidth: "100%",
};

const headerSelectStyle = {
  ...selectStyle,
  width: 132,
  flex: "0 0 132px",
};

const headerBtnStyle = {
  ...btnStyle,
  flex: "0 0 auto",
  whiteSpace: "nowrap",
};

const dangerBtnStyle = {
  ...btnStyle,
  border: "1px solid rgba(255,120,120,0.35)",
  background: "rgba(120,30,30,0.25)",
};

const thStyle = {
  textAlign: "left",
  fontSize: 11,
  fontWeight: 800,
  letterSpacing: "0.06em",
  textTransform: "uppercase",
  padding: "9px 10px",
  borderBottom: "1px solid rgba(34,211,238,0.22)",
  background: "rgba(8,47,73,0.36)",
  color: "rgba(207,250,254,0.88)",
  whiteSpace: "nowrap",
};

const tdStyle = {
  fontSize: 12,
  padding: "10px",
  borderBottom: "1px solid rgba(56,189,248,0.08)",
  verticalAlign: "top",
  background: "rgba(1,8,15,0.18)",
};

const codeStyle = {
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
  fontSize: 11,
  opacity: 0.9,
};

const terminalEyebrowStyle = {
  fontSize: 10,
  fontWeight: 900,
  letterSpacing: "0.16em",
  color: "#67e8f9",
};

const tokenRegistryTitleStyle = {
  marginTop: 2,
  fontSize: 17,
  fontWeight: 900,
  letterSpacing: "0.02em",
  color: "var(--utt-page-fg, #e9eef7)",
};

const tokenRegistrySubtitleStyle = {
  marginTop: 3,
  fontSize: 11,
  color: "rgba(186,230,253,0.68)",
};

const chainIdentityPanelStyle = {
  position: "relative",
  zIndex: 1,
  padding: 12,
  border: "1px solid rgba(139,92,246,0.24)",
  borderRadius: 12,
  background: "linear-gradient(135deg, rgba(30,27,75,0.34), rgba(8,47,73,0.24), rgba(2,8,23,0.72))",
  boxShadow: "inset 3px 0 0 rgba(139,92,246,0.42)",
};

const chainIdentityHeaderStyle = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  gap: 10,
  flexWrap: "wrap",
};

const chainIdentityCodeStyle = {
  fontSize: 10,
  fontWeight: 900,
  letterSpacing: "0.18em",
  color: "#c4b5fd",
};

const chainIdentityNameStyle = {
  marginTop: 2,
  fontSize: 15,
  fontWeight: 850,
};

const readOnlyBadgeStyle = {
  padding: "3px 8px",
  borderRadius: 999,
  border: "1px solid rgba(34,211,238,0.34)",
  background: "rgba(8,145,178,0.12)",
  color: "#a5f3fc",
  fontSize: 10,
  fontWeight: 900,
  letterSpacing: "0.08em",
};

const chainBadgeWrapStyle = {
  display: "flex",
  gap: 6,
  flexWrap: "wrap",
  marginTop: 9,
};

const chainBadgeStyle = {
  display: "inline-flex",
  padding: "3px 7px",
  borderRadius: 999,
  border: "1px solid rgba(125,211,252,0.18)",
  background: "rgba(14,116,144,0.10)",
  color: "rgba(207,250,254,0.84)",
  fontSize: 10,
  fontWeight: 700,
};

const chainIdentityDetailStyle = {
  marginTop: 8,
  maxWidth: 980,
  fontSize: 12,
  lineHeight: 1.5,
  color: "rgba(226,232,240,0.74)",
};

const panelHeadingRowStyle = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  gap: 10,
  flexWrap: "wrap",
  marginBottom: 9,
};

const panelEyebrowStyle = {
  fontSize: 9,
  fontWeight: 900,
  letterSpacing: "0.14em",
  color: "rgba(103,232,249,0.72)",
};

const panelTitleStyle = {
  marginTop: 2,
  fontSize: 14,
  fontWeight: 850,
};

const presetBtnStyle = {
  ...btnStyle,
  border: "1px solid rgba(167,139,250,0.34)",
  background: "linear-gradient(180deg, rgba(109,40,217,0.22), rgba(49,46,129,0.18))",
  color: "#ddd6fe",
};

const mappingCountBadgeStyle = {
  padding: "3px 7px",
  borderRadius: 999,
  border: "1px solid rgba(34,211,238,0.24)",
  color: "#a5f3fc",
  fontSize: 10,
  fontWeight: 900,
  letterSpacing: "0.08em",
};

const tokenValidationStyle = (ok) => ({
  display: "flex",
  alignItems: "center",
  gap: 7,
  marginTop: 8,
  padding: "6px 8px",
  borderRadius: 8,
  border: ok ? "1px solid rgba(74,222,128,0.24)" : "1px solid rgba(245,158,11,0.28)",
  background: ok ? "rgba(22,101,52,0.12)" : "rgba(120,72,16,0.13)",
  color: ok ? "#bbf7d0" : "#fde68a",
  fontSize: 11,
});

const tokenValidationDotStyle = (ok) => ({
  width: 7,
  height: 7,
  flex: "0 0 7px",
  borderRadius: 999,
  background: ok ? "#4ade80" : "#f59e0b",
  boxShadow: ok ? "0 0 12px rgba(74,222,128,0.65)" : "0 0 12px rgba(245,158,11,0.55)",
});

const robinhoodHelpStyle = {
  marginTop: 8,
  padding: 9,
  borderRadius: 9,
  border: "1px solid rgba(167,139,250,0.22)",
  background: "rgba(49,46,129,0.14)",
  color: "rgba(237,233,254,0.84)",
  fontSize: 12,
  lineHeight: 1.5,
};

const tokenRegistryCyberCss = `
.utt-token-registry-cyber::before {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  z-index: 0;
  opacity: 0.22;
  background-image:
    linear-gradient(rgba(34,211,238,0.035) 1px, transparent 1px),
    linear-gradient(90deg, rgba(34,211,238,0.035) 1px, transparent 1px),
    repeating-linear-gradient(180deg, transparent 0, transparent 3px, rgba(255,255,255,0.012) 4px);
  background-size: 24px 24px, 24px 24px, 100% 4px;
}
.utt-token-registry-cyber > * {
  position: relative;
  z-index: 1;
}
.utt-token-registry-cyber input,
.utt-token-registry-cyber select,
.utt-token-registry-cyber textarea,
.utt-token-registry-cyber button {
  transition: border-color 120ms ease, box-shadow 120ms ease, transform 120ms ease, background 120ms ease;
}
.utt-token-registry-cyber input:focus-visible,
.utt-token-registry-cyber select:focus-visible,
.utt-token-registry-cyber textarea:focus-visible,
.utt-token-registry-cyber button:focus-visible {
  outline: 2px solid rgba(34,211,238,0.72);
  outline-offset: 2px;
  border-color: rgba(34,211,238,0.62) !important;
  box-shadow: 0 0 0 3px rgba(6,182,212,0.12), 0 0 22px rgba(6,182,212,0.10);
}
.utt-token-registry-cyber button:not(:disabled):hover {
  transform: translateY(-1px);
  border-color: rgba(34,211,238,0.58) !important;
  box-shadow: 0 0 18px rgba(6,182,212,0.10);
}
.utt-token-registry-cyber button:disabled {
  cursor: not-allowed !important;
  opacity: 0.48;
}
.utt-token-registry-cyber table tbody tr:hover td {
  background: rgba(8,47,73,0.22) !important;
}
`;

const poolAccountWrapStyle = {
  display: "flex",
  alignItems: "center",
  gap: 6,
  minWidth: 0,
  maxWidth: "100%",
  marginTop: 2,
};

const poolAccountCodeStyle = {
  ...codeStyle,
  display: "inline-block",
  minWidth: 0,
  maxWidth: 180,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  verticalAlign: "bottom",
};

const miniBtnStyle = {
  fontSize: 10,
  padding: "3px 6px",
  borderRadius: 7,
  border: "1px solid rgba(255,255,255,0.14)",
  background: "rgba(255,255,255,0.05)",
  color: "var(--utt-text, #e9eef7)",
  cursor: "pointer",
};
