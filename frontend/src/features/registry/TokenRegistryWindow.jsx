// frontend/src/features/registry/TokenRegistryWindow.jsx
import React, { useCallback, useEffect, useMemo, useState } from "react";

const LS_SOLANA_DETECTED_TOKENS_KEY = "utt_solana_detected_tokens_v1";

const CHAIN_OPTIONS = ["solana", "polkadot", "hydration"];
const GENERIC_ADDRESS_LABEL = "Address / Mint / Asset ID";
const GENERIC_ADDRESS_PLACEHOLDER = "Mint / contract address / asset ID";
const EXTERNAL_PRICE_SOURCE_OPTIONS = ["", "stable", "coingecko", "derived", "none"];

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

function hydrationRouteTemplateForSymbol(symbol) {
  const s = String(symbol || "").trim().toUpperCase();
  if (s === "DOT-HDX") {
    return [
      { pool: { type: "Aave" }, assetIn: 5, assetOut: 1001 },
      { pool: { type: "Omnipool" }, assetIn: 1001, assetOut: 0 },
    ];
  }
  if (s === "HDX-DOT") {
    return [
      { pool: { type: "Omnipool" }, assetIn: 0, assetOut: 1001 },
      { pool: { type: "Aave" }, assetIn: 1001, assetOut: 5 },
    ];
  }
  return [
    { pool: { type: "Omnipool" }, assetIn: 0, assetOut: 0 },
  ];
}

function hasKnownHydrationRouteTemplate(symbol) {
  const s = String(symbol || "").trim().toUpperCase();
  return s === "DOT-HDX" || s === "HDX-DOT";
}

function isKnownHydrationRouteTemplateText(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  try {
    const parsed = JSON.parse(raw);
    const json = JSON.stringify(parsed);
    return (
      json === JSON.stringify(hydrationRouteTemplateForSymbol("DOT-HDX")) ||
      json === JSON.stringify(hydrationRouteTemplateForSymbol("HDX-DOT"))
    );
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
  const [venue, setVenue] = useState(""); // optional override scope (blank = global)
  const [externalPriceSource, setExternalPriceSource] = useState("");
  const [externalPriceId, setExternalPriceId] = useState("");

  // Inline edit
  const [editId, setEditId] = useState(null);
  const [editRow, setEditRow] = useState({
    symbol: "",
    address: "",
    decimals: "",
    venue: "",
    external_price_source: "",
    external_price_id: "",
  });

  // Hydration manual route registry
  const [routes, setRoutes] = useState([]);
  const [routeLoading, setRouteLoading] = useState(false);
  const [routeSaving, setRouteSaving] = useState(false);
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

  const activeVenueFilter = useMemo(() => defaultVenueForChain(chain), [chain]);
  const showHydrationRoutes = useMemo(() => {
    const c = String(chain || "").trim().toLowerCase();
    return c === "hydration" || c === "polkadot";
  }, [chain]);

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

  const canAdd = useMemo(() => {
    const s = String(symbol || "").trim();
    const a = String(address || "").trim();
    const d = String(decimals || "").trim();
    if (!s || !a || !d) return false;
    const di = Number(d);
    if (!Number.isFinite(di) || di < 0 || di > 18) return false;
    return true;
  }, [symbol, address, decimals]);

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

  useEffect(() => {
    load();
    loadSuggestions();
  }, [load, loadSuggestions]);

  useEffect(() => {
    loadRoutes();
  }, [loadRoutes]);

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
  }, [API_BASE, canAdd, chain, symbol, address, decimals, venue, externalPriceSource, externalPriceId, load]);

  const startEdit = useCallback((row) => {
    setEditId(row?.id || null);
    setEditRow({
      symbol: String(row?.symbol || ""),
      address: String(row?.address || ""),
      decimals: String(row?.decimals ?? ""),
      venue: String(row?.venue || ""),
      external_price_source: String(row?.external_price_source || ""),
      external_price_id: String(row?.external_price_id || ""),
    });
  }, []);

  const cancelEdit = useCallback(() => {
    setEditId(null);
    setEditRow({ symbol: "", address: "", decimals: "", venue: "", external_price_source: "", external_price_id: "" });
  }, []);

  const saveEdit = useCallback(async () => {
    const id = editId;
    if (!id) return;

    const s = String(editRow.symbol || "").trim();
    const a = String(editRow.address || "").trim();
    const d = Number(String(editRow.decimals || "").trim());
    if (!s || !a || !Number.isFinite(d) || d < 0 || d > 18) {
      setErr("Edit: symbol/identifier/decimals invalid.");
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
    [API_BASE, chain]
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
    setRouteJsonText(Array.isArray(row?.route) ? safeJsonPretty(row.route) : "");
    setRouteNote(String(row?.note || ""));
    setRouteTestResult(null);
  }, []);

  const upsertRoute = useCallback(async () => {
    if (!canUpsertRoute) return;
    setRouteSaving(true);
    setRouteErr(null);
    try {
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

      const r = await fetch(`${API_BASE}/api/polkadot_dex/hydration/route_registry/upsert`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      setRouteTestResult(null);
      await loadRoutes();
    } catch (e) {
      setRouteErr(String(e?.message || e));
    } finally {
      setRouteSaving(false);
    }
  }, [API_BASE, canUpsertRoute, routeSymbol, routeMode, routeBaseReserve, routeQuoteReserve, routeFeeBps, routePoolAccount, routeEnabled, routeConfirmed, routeJsonText, routeNote, loadRoutes]);

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
    } catch (e) {
      setRouteErr(String(e?.message || e));
    } finally {
      setRouteSaving(false);
    }
  }, [API_BASE, loadRoutes]);

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
    setEditRow({ symbol: "", address: "", decimals: "", venue: "", external_price_source: "", external_price_id: "" });
    setErr(null);
    setRouteErr(null);
    setRouteTestResult(null);
  }, []);

  return (
    <div style={{ color: "var(--utt-text, #e9eef7)", minWidth: 0, maxWidth: "100%" }}>
      <div style={tokenRegistryHeaderStyle}>
        <div style={{ fontWeight: 800, fontSize: 14, minWidth: 0 }}>Token / Symbol Registry</div>
        <div style={tokenRegistryHeaderActionsStyle}>
          <select value={chain} onChange={(e) => onChainChange(e.target.value)} style={headerSelectStyle}>
            {CHAIN_OPTIONS.map((opt) => (
              <option key={opt} value={opt}>{opt}</option>
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
        <div style={{ fontWeight: 700, marginBottom: 8 }}>Add token</div>
        <div style={addTokenGridStyle}>
          <input value={symbol} onChange={(e) => setSymbol(e.target.value)} placeholder="SYMBOL (e.g. UTTT)" style={inputStyle} />
          <input value={address} onChange={(e) => setAddress(e.target.value)} placeholder={GENERIC_ADDRESS_PLACEHOLDER} style={inputStyle} />
          <input value={decimals} onChange={(e) => setDecimals(e.target.value)} placeholder="decimals" style={inputStyle} />
          <input value={venue} onChange={(e) => setVenue(e.target.value)} placeholder="venue override" style={inputStyle} />
          <select value={externalPriceSource} onChange={(e) => setExternalPriceSource(e.target.value)} style={selectStyle} title="External price source">
            {EXTERNAL_PRICE_SOURCE_OPTIONS.map((opt) => (
              <option key={opt || "blank"} value={opt}>{opt ? externalPriceSourceLabel(opt) : "Price source"}</option>
            ))}
          </select>
          <input value={externalPriceId} onChange={(e) => setExternalPriceId(e.target.value)} placeholder="price ID (hydradx)" style={inputStyle} />
          <button type="button" onClick={onAdd} style={btnStyle} disabled={!canAdd || saving}>
            {saving ? "Saving…" : "Add"}
          </button>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, opacity: 0.75 }}>
          Tip: leave “venue override” blank for global entries. For Hydration, use polkadot_hydration and put the asset ID (or native for HDX) in Address / Mint / Asset ID. Price source examples: HDX = CoinGecko / hydradx, DOT = CoinGecko / polkadot, USDT = Stable / stable, UTTT = Derived / UTTT-HDX×HDX-USD.
        </div>
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
            <button type="button" onClick={loadRoutes} style={btnStyle} disabled={routeLoading}>
              {routeLoading ? "Loading…" : "Refresh routes"}
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
              <option value="manual_xyk">Manual XYK</option>
              <option value="manual_router">Manual Router</option>
            </select>
            <select value={routePoolType} onChange={(e) => setRoutePoolType(e.target.value)} style={selectStyle} title="Pool type">
              <option value={routeMode === "manual_router" ? "Router" : "XYK"}>{routeMode === "manual_router" ? "Router" : "XYK"}</option>
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
            <button
              type="button"
              onClick={() => {
                setRouteMode("manual_router");
                setRoutePoolType("Router");
                setRouteBaseReserve("");
                setRouteQuoteReserve("");
                setRouteJsonText(safeJsonPretty(hydrationRouteTemplateForSymbol(routeSymbol)));
              }}
              style={btnStyle}
              disabled={routeMode !== "manual_router"}
            >
              Load route template
            </button>
            <button type="button" onClick={upsertRoute} style={btnStyle} disabled={!canUpsertRoute || routeSaving}>
              {routeSaving ? "Saving…" : "Save route"}
            </button>
          </div>
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
            Manual XYK reserves are human units. Manual Router rows do not use the reserve fields — they use the route JSON box instead. For DOT-HDX use DOT(5) → Aave → aDOT(1001) → Omnipool → HDX(0); for HDX-DOT use the reverse route. Click “Load route template” after entering the pair, then mark Confirmed only after a tiny live on-chain success.
          </div>
          {routeErr && <div style={{ marginTop: 8, color: "#ffb3b3", fontSize: 12 }}>{routeErr}</div>}
          {routeTestResult && (
            <div style={{ marginTop: 8, padding: 8, borderRadius: 10, border: "1px solid rgba(255,255,255,0.10)", background: routeTestResult.ok ? "rgba(20,80,45,0.25)" : "rgba(120,30,30,0.25)", fontSize: 12 }}>
              <div style={{ fontWeight: 700, marginBottom: 6 }}>
                {routeTestResult.kind === "live_reserves" ? "Live reserve test" : "Manual route orderbook test"}: {routeTestResult.symbol || "—"}
              </div>
              {routeTestResult.error ? (
                <div style={{ color: "#ffb3b3" }}>{routeTestResult.error}</div>
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
        <div style={{ fontWeight: 700, marginBottom: 8 }}>Mappings</div>

        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0 }}>
            <thead>
              <tr>
                <th style={thStyle}>Symbol</th>
                <th style={thStyle}>{GENERIC_ADDRESS_LABEL}</th>
                <th style={thStyle}>Decimals</th>
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
                        <code style={codeStyle}>{row.address}</code>
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
                        <input value={editRow.venue} onChange={(e) => setEditRow((p) => ({ ...p, venue: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span style={{ opacity: row.venue ? 1 : 0.55 }}>{row.venue || "—"}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <select value={editRow.external_price_source || ""} onChange={(e) => setEditRow((p) => ({ ...p, external_price_source: e.target.value }))} style={selectStyle}>
                          {EXTERNAL_PRICE_SOURCE_OPTIONS.map((opt) => (
                            <option key={opt || "blank"} value={opt}>{opt ? externalPriceSourceLabel(opt) : "—"}</option>
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
                            Test resolve
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
                  <td colSpan={7} style={{ ...tdStyle, opacity: 0.7 }}>
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

const panelStyle = {
  marginTop: 10,
  padding: 10,
  borderRadius: 12,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "rgba(255,255,255,0.04)",
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

const inputStyle = {
  width: "100%",
  minWidth: 0,
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,0.12)",
  background: "rgba(0,0,0,0.25)",
  color: "var(--utt-text, #e9eef7)",
  outline: "none",
  boxSizing: "border-box",
};

const selectStyle = {
  width: "100%",
  minWidth: 0,
  boxSizing: "border-box",
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,0.12)",
  background: "rgba(0,0,0,0.25)",
  color: "var(--utt-text, #e9eef7)",
  outline: "none",
};

const btnStyle = {
  minWidth: 0,
  fontSize: 12,
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,0.16)",
  background: "rgba(255,255,255,0.06)",
  color: "var(--utt-text, #e9eef7)",
  cursor: "pointer",
};


const tokenRegistryHeaderStyle = {
  display: "flex",
  justifyContent: "space-between",
  gap: 10,
  alignItems: "center",
  marginBottom: 10,
  flexWrap: "wrap",
  minWidth: 0,
  maxWidth: "100%",
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
  fontSize: 12,
  padding: "8px 10px",
  borderBottom: "1px solid rgba(255,255,255,0.10)",
  opacity: 0.85,
  whiteSpace: "nowrap",
};

const tdStyle = {
  fontSize: 12,
  padding: "10px",
  borderBottom: "1px solid rgba(255,255,255,0.06)",
  verticalAlign: "top",
};

const codeStyle = {
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
  fontSize: 11,
  opacity: 0.9,
};

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
