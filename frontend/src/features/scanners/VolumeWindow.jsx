// frontend/src/features/scanners/VolumeWindow.jsx
import { useEffect, useMemo, useRef, useState } from "react";


function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

function trimApiBase(base) {
  return String(base || "").replace(/\/+$/, "");
}

const MARKET_METRICS_REQUEST_LIMIT = 1000;

const MARKET_METRICS_BROWSER_CACHE_KEY = "utt.market_metrics.volume.summary.v9";
const MARKET_METRICS_BROWSER_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;

function readBrowserMetricSnapshot() {
  try {
    if (typeof window === "undefined" || !window.localStorage) return null;
    const raw = window.localStorage.getItem(MARKET_METRICS_BROWSER_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const cachedAt = Number(parsed?.cachedAt || 0);
    if (!Number.isFinite(cachedAt) || Date.now() - cachedAt > MARKET_METRICS_BROWSER_CACHE_MAX_AGE_MS) return null;
    if (!Array.isArray(parsed?.rows) || !parsed.rows.length) return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeBrowserMetricSnapshot(snapshot) {
  try {
    if (typeof window === "undefined" || !window.localStorage) return;
    if (!snapshot || !Array.isArray(snapshot.rows) || !snapshot.rows.length) return;
    window.localStorage.setItem(
      MARKET_METRICS_BROWSER_CACHE_KEY,
      JSON.stringify({ ...snapshot, cachedAt: Date.now() })
    );
  } catch {
    // Browser cache is best-effort only.
  }
}


// PORT-METRICS.1 v5:
// assets=db asks the backend for the local known/tracked universe.  Do not
// append a broad CoinGecko fallback list from the frontend; that caused duplicate
// Market Cap / Volume requests, rate limits, and long refresh timeouts.
function normalizeAssetList(value) {
  const input = Array.isArray(value) ? value.join(",") : String(value || "");
  return input
    .split(/[\s,;]+/)
    .map((x) => assetFromSymbol(x))
    .filter(Boolean);
}

function mergeAssetLists(...values) {
  const out = [];
  const seen = new Set();
  values.forEach((value) => {
    normalizeAssetList(value).forEach((asset) => {
      if (seen.has(asset)) return;
      seen.add(asset);
      out.push(asset);
    });
  });
  return out;
}

function metricRowKey(row) {
  const asset = rowAssetKey(row);
  if (asset) return asset;
  return String(row?.asset || row?.symbol || row?.pair || "").trim().toUpperCase();
}

function mergeMetricRows(...rowLists) {
  const merged = new Map();
  rowLists.forEach((rowList) => {
    (Array.isArray(rowList) ? rowList : []).forEach((row) => {
      const key = metricRowKey(row);
      if (!key) return;
      if (!merged.has(key)) {
        merged.set(key, row);
        return;
      }
      const prev = merged.get(key) || {};
      merged.set(key, { ...prev, ...row });
    });
  });
  return Array.from(merged.values());
}

function isGenericSourceValue(v) {
  const s = String(v || "").trim().toLowerCase();
  return !s || s === "global" || s === "unknown" || s === "all" || s === "none" || s === "n/a" || s === "na";
}

function normalizeSourceKey(v) {
  const s = String(v || "").trim().toLowerCase();
  if (!s || isGenericSourceValue(s)) return "";
  if (s === "polkadot_hydration" || s === "hydration_dex") return "hydration";
  if (s === "solana_jupiter" || s === "jupiter" || s === "raydium" || s === "solana_dex") return "solana";
  if (s === "self-custody" || s === "selfcustody") return "self_custody";
  if (s === "crypto_com" || s === "crypto.com" || s === "crypto-com") return "cryptocom";
  if (s === "dex-trade") return "dex_trade";
  return s;
}

function addNormalizedSourceKey(out, value) {
  const values = Array.isArray(value) ? value : [value];
  values.forEach((v) => {
    const s = normalizeSourceKey(v);
    if (!s || out.includes(s)) return;
    out.push(s);
  });
}

function assetFromSymbol(symbolLike) {
  const raw = String(symbolLike || "").trim().toUpperCase();
  if (!raw) return "";
  const clean = raw.replace(/\s+/g, "").replace(/[\/_]/g, "-");
  if (clean.includes("-")) return clean.split("-").filter(Boolean)[0] || "";
  for (const suffix of ["USDT", "USDC", "USD", "HDX", "SOL", "DOT", "BTC", "ETH", "DOGE"]) {
    if (clean.endsWith(suffix) && clean.length > suffix.length) return clean.slice(0, -suffix.length);
  }
  return clean;
}

function isAbortLikeError(e) {
  const name = String(e?.name || "").toLowerCase();
  const msg = String(e?.message || e || "").toLowerCase();
  return name === "aborterror" || msg.includes("aborted") || msg.includes("abort");
}

async function fetchJson(url, timeoutMs = 25000) {
  let timer = null;
  try {
    const timeoutPromise = new Promise((_, reject) => {
      timer = window.setTimeout(() => reject(new Error("Market metrics refresh timed out")), timeoutMs);
    });
    const res = await Promise.race([
      fetch(url, { cache: "no-store" }),
      timeoutPromise,
    ]);
    if (!res.ok) {
      let msg = `HTTP ${res.status}`;
      try {
        const body = await res.json();
        msg = String(body?.detail || body?.message || msg);
      } catch {
        // keep HTTP status message
      }
      throw new Error(msg);
    }
    return res.json();
  } finally {
    if (timer) window.clearTimeout(timer);
  }
}

function toNum(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function fmtMoney(v, hide = false) {
  if (hide) return "••••";
  const n = toNum(v);
  if (n === null) return "—";
  if (Math.abs(n) >= 1_000_000_000_000) return `$${(n / 1_000_000_000_000).toFixed(2)}T`;
  if (Math.abs(n) >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(2)}B`;
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000) return `$${(n / 1_000).toFixed(2)}K`;
  if (Math.abs(n) >= 1) return `$${n.toFixed(2)}`;
  if (Math.abs(n) >= 0.000001) return `$${n.toFixed(8)}`;
  return `$${n.toPrecision(4)}`;
}

function fmtNum(v, hide = false) {
  if (hide) return "••••";
  const n = toNum(v);
  if (n === null) return "—";
  if (Math.abs(n) >= 1_000_000_000_000) return `${(n / 1_000_000_000_000).toFixed(2)}T`;
  if (Math.abs(n) >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B`;
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(2)}K`;
  return n.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

function fmtTime(v, hide = false) {
  if (hide) return "••••";
  if (!v) return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString();
}

function cellTitle(v) {
  if (v === null || v === undefined || v === "") return "";
  return String(v);
}

function volumeRowHasData(row) {
  return [row?.volume_24h_usd, row?.volume_24h_base, row?.liquidity_usd].some((v) => toNum(v) !== null);
}

function metricMessageText(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  return String(v?.message || v?.error || v?.detail || "");
}

function volumeRefreshLooksFailed(rows, errors) {
  const rowList = Array.isArray(rows) ? rows : [];
  const errorList = Array.isArray(errors) ? errors : [];
  if (!rowList.length) return false;
  const anyData = rowList.some(volumeRowHasData);
  if (anyData) return false;
  const text = [
    ...errorList.map(metricMessageText),
    ...rowList.flatMap((r) => (Array.isArray(r?.warnings) ? r.warnings : [])),
  ]
    .join(" ")
    .toLowerCase();
  return Boolean(text.match(/ssl|certificate|urlopen|coingecko|unavailable|failed|timeout|rate/));
}


function rowAssetKey(row) {
  return assetFromSymbol(row?.asset || row?.symbol || row?.pair || "");
}

function rowOwnedSourceKeys(row) {
  const out = [];
  addNormalizedSourceKey(out, row?.owned_venues);
  return out;
}

function rowTrackedSourceKeys(row) {
  const out = [];
  addNormalizedSourceKey(out, row?.venue_filter_keys);
  addNormalizedSourceKey(out, row?.owned_venues);
  addNormalizedSourceKey(out, row?.tracked_venues);
  addNormalizedSourceKey(out, row?.dex);
  addNormalizedSourceKey(out, row?.venue);
  return out;
}

function rowSourceKeys(row) {
  // Prefer backend-provided venue/dex coverage. Do not fall back to CoinGecko
  // IDs or asset chains here; those made the dropdown look like an asset/source
  // list instead of a venue/DEX filter.
  return rowTrackedSourceKeys(row);
}

function rowSourceKey(row) {
  const keys = rowSourceKeys(row);
  return keys[0] || "";
}

function rowMatchesSource(row, sourceKey) {
  const key = normalizeSourceKey(sourceKey);
  if (!key || key === "all") return true;
  return rowSourceKeys(row).includes(key);
}

function rowSourceLabel(sourceKey) {
  const s = String(sourceKey || "").trim();
  if (!s || s === "all") return "All Venues / Sources";
  if (s === "self_custody") return "Self-custody";
  if (s === "polkadot_hydration") return "Hydration";
  return s.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function ownedKeysFromPayload(...payloads) {
  const out = [];
  const seen = new Set();
  const add = (value) => {
    normalizeAssetList(value).forEach((asset) => {
      if (!asset || seen.has(asset)) return;
      seen.add(asset);
      out.push(asset);
    });
  };

  payloads.forEach((payload) => {
    if (!payload) return;
    add(payload.owned_assets);
    (Array.isArray(payload.items) ? payload.items : []).forEach((row) => {
      if (row?.is_owned === true || row?.owned === true || row?.has_balance === true) {
        add(rowAssetKey(row));
      }
    });
  });

  return out;
}


function sourceKeysFromPayload(...payloads) {
  const out = [];
  const add = (value) => addNormalizedSourceKey(out, value);

  payloads.forEach((payload) => {
    if (!payload) return;
    add(payload.venue_filter_options);
    const ctx = payload.asset_context && typeof payload.asset_context === "object" ? payload.asset_context : {};
    Object.values(ctx).forEach((row) => {
      if (!row || typeof row !== "object") return;
      add(row.owned_venues);
      add(row.tracked_venues);
    });
  });

  return out;
}

function rowIsOwned(row, ownedAssetKeys) {
  const explicit = row?.is_owned ?? row?.owned ?? row?.has_balance;
  if (typeof explicit === "boolean") return explicit;
  const key = rowAssetKey(row);
  return Boolean(key && Array.isArray(ownedAssetKeys) && ownedAssetKeys.includes(key));
}

function rowIsOwnedForSource(row, ownedAssetKeys, sourceKey = "all") {
  const key = normalizeSourceKey(sourceKey);
  if (!key || key === "all") return rowIsOwned(row, ownedAssetKeys);

  // Venue-specific view: owned means owned at that venue/source, not merely
  // owned somewhere else in the global portfolio.
  return rowOwnedSourceKeys(row).includes(key);
}

function scopeLabel(row, ownedAssetKeys, sourceKey = "all") {
  return rowIsOwnedForSource(row, ownedAssetKeys, sourceKey) ? "Owned" : "Unowned";
}

function isSoftMetricError(e) {
  const text = metricMessageText(e).toLowerCase();
  return Boolean(text.match(/429|too many requests|rate_limited|rate limited|backoff|using cached|stale/));
}

function metricSortValue(row, key) {
  if (key === "asset") return rowAssetKey(row) || String(row?.asset || row?.pair || "").toUpperCase();
  if (key === "volume_24h_usd") return toNum(row?.volume_24h_usd ?? row?.volume_usd ?? row?.volume_24h);
  if (key === "market_cap_usd") return toNum(row?.market_cap_usd ?? row?.fdv_usd);
  return toNum(row?.[key]);
}

function compareMetricRows(a, b, sortKey, sortDir) {
  const dir = sortDir === "asc" ? 1 : -1;
  const av = metricSortValue(a, sortKey);
  const bv = metricSortValue(b, sortKey);

  if (sortKey === "asset") {
    return String(av || "").localeCompare(String(bv || "")) * dir;
  }

  if (av === null && bv === null) return String(rowAssetKey(a) || "").localeCompare(String(rowAssetKey(b) || ""));
  if (av === null) return 1;
  if (bv === null) return -1;
  return (av - bv) * dir;
}

export default function VolumeWindow({
  apiBase,
  selectedSymbol = "",
  hideTableData = false,
  onClose,
  height,
  onDragHandleMouseDown,
}) {
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshSeconds, setRefreshSeconds] = useState(300);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [rows, setRows] = useState([]);
  const [payloadCounts, setPayloadCounts] = useState({ asset: 0, known: 0, owned: 0, ids: 0, symbolMatched: 0, pageRows: 0 });
  const [ownedAssetKeys, setOwnedAssetKeys] = useState([]);
  const [payloadSourceKeys, setPayloadSourceKeys] = useState([]);
  const [sourceFilter, setSourceFilter] = useState("all");
  const sortKey = "volume_24h_usd";
  const [sortDir, setSortDir] = useState("desc");
  const [errors, setErrors] = useState([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const mountedRef = useRef(false);
  const requestSeqRef = useRef(0);

  const selectedAsset = useMemo(() => assetFromSymbol(selectedSymbol), [selectedSymbol]);
  // PORT-METRICS.1 v9:
  // Do not append the selected symbol to broad assets=db window requests. The
  // backend already returns the full known universe, and changing include_assets
  // fragments backend/browser caches and can make symbol selection trigger a
  // 700+ row rebuild.
  const marketUniverseKey = "";

  const ui = useMemo(
    () => ({
      wrap: {
        height: height || "100%",
        minHeight: 320,
        display: "flex",
        flexDirection: "column",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-surface-1, #121212)",
        borderRadius: 14,
        overflow: "hidden",
      },
      header: {
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 10,
        padding: "10px 10px",
        borderBottom: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-surface-2, #151515)",
        cursor: onDragHandleMouseDown ? "grab" : "default",
        userSelect: onDragHandleMouseDown ? "none" : "auto",
      },
      title: { fontSize: 14, fontWeight: 900, margin: 0 },
      sub: { fontSize: 12, opacity: 0.75 },
      right: { display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" },
      ctl: {
        background: "var(--utt-control-bg, #0f0f0f)",
        color: "var(--utt-page-fg, #eee)",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 10,
        padding: "6px 8px",
      },
      btn: {
        background: "var(--utt-button-bg, #1b1b1b)",
        color: "var(--utt-page-fg, #eee)",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 10,
        padding: "6px 10px",
        cursor: "pointer",
        whiteSpace: "nowrap",
        fontWeight: 800,
      },
      body: { padding: 10, overflow: "auto", flex: 1 },
      card: {
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-surface-1, #121212)",
        borderRadius: 12,
        padding: 10,
      },
      mono: { fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" },
      table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
      th: {
        textAlign: "left",
        position: "sticky",
        top: 0,
        zIndex: 1,
        background: "var(--utt-surface-2, #151515)",
        borderBottom: "1px solid var(--utt-border-1, #2a2a2a)",
        padding: "8px 7px",
        whiteSpace: "nowrap",
      },
      td: {
        borderBottom: "1px solid color-mix(in srgb, var(--utt-border-1, #2a2a2a) 70%, transparent)",
        padding: "8px 7px",
        verticalAlign: "top",
      },
      pill: {
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        padding: "2px 7px",
        borderRadius: 999,
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-control-bg, #0f0f0f)",
        maxWidth: 220,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      },
    }),
    [height, onDragHandleMouseDown]
  );

  async function doRefresh(options = {}) {
    const base = trimApiBase(apiBase);
    if (!base || busy) return;

    const forceLive = Boolean(options?.forceLive);
    const requestTimeoutMs = forceLive ? 120000 : 60000;

    const seq = requestSeqRef.current + 1;
    requestSeqRef.current = seq;

    const buildParams = (assetMode, includeAssets = "") => {
      const p = new URLSearchParams();
      p.set("assets", assetMode);
      if (includeAssets) p.set("include_assets", includeAssets);
      if (forceLive) p.set("force_refresh", "true");
      p.set("ttl_s", String(clampSeconds(refreshSeconds, 300)));
      p.set("limit", String(MARKET_METRICS_REQUEST_LIMIT));
      return p;
    };

    setBusy(true);
    setErr("");

    try {
      let dbJson = null;
      let ownedJson = null;
      let dbError = null;
      let ownedError = null;

      try {
        dbJson = await fetchJson(`${base}/api/market_metrics/summary?${buildParams("db", marketUniverseKey).toString()}`, requestTimeoutMs);
      } catch (e) {
        dbError = e;
      }

      if (!mountedRef.current || requestSeqRef.current !== seq) return;

      // New backend payloads include owned_assets on the db/known request. If
      // running against an older backend, fall back to the older owned request.
      if (!ownedKeysFromPayload(dbJson).length) {
        try {
          ownedJson = await fetchJson(`${base}/api/market_metrics/summary?${buildParams("owned", "").toString()}`, requestTimeoutMs);
        } catch (e) {
          ownedError = e;
        }
      }

      if (!mountedRef.current || requestSeqRef.current !== seq) return;

      if (!dbJson && !ownedJson) {
        const msg = String(dbError?.message || ownedError?.message || "Market metrics refresh failed");
        throw new Error(msg);
      }

      const nextOwnedRows = Array.isArray(ownedJson?.items) ? ownedJson.items : [];
      const nextDbRows = Array.isArray(dbJson?.items) ? dbJson.items : [];
      const nextRows = mergeMetricRows(nextDbRows, nextOwnedRows);
      const nextOwnedKeys = ownedKeysFromPayload(dbJson, ownedJson);
      const nextSourceKeys = sourceKeysFromPayload(dbJson, ownedJson);
      const nextErrors = [
        ...(Array.isArray(ownedJson?.errors) ? ownedJson.errors : []),
        ...(Array.isArray(dbJson?.errors) ? dbJson.errors : []),
      ];
      if (ownedError) {
        nextErrors.push({ message: `Owned market metrics unavailable: ${String(ownedError?.message || ownedError || "request failed")}` });
      }
      if (dbError) {
        nextErrors.push({ message: `Tracked/unowned market metrics unavailable: ${String(dbError?.message || dbError || "request failed")}` });
      }
      if (rows.length && volumeRefreshLooksFailed(nextRows, nextErrors)) {
        setErr("Volume refresh failed; keeping last good snapshot.");
        setErrors([
          { message: "Last good volume snapshot retained because the refresh returned no usable metric data." },
          ...nextErrors,
        ]);
        return;
      }
      const nextPayloadCounts = {
        asset: Number(dbJson?.asset_count ?? nextRows.length) || nextRows.length,
        known: Number(dbJson?.known_asset_count ?? nextRows.length) || nextRows.length,
        owned: Number(dbJson?.owned_asset_count ?? nextOwnedKeys.length) || nextOwnedKeys.length,
        mode: String(dbJson?.refresh_mode || (forceLive ? "live_refresh" : "cache_snapshot")),
        ids: Number(dbJson?.market_data_id_count ?? 0) || 0,
        live: Number(dbJson?.market_data_live_fetch_id_count ?? 0) || 0,
        cached: Number(dbJson?.market_data_cached_id_count ?? 0) || 0,
        skipped: Number(dbJson?.market_data_skipped_live_id_count ?? 0) || 0,
        symbolMatched: Number(dbJson?.market_data_symbol_match_count ?? 0) || 0,
        symbolCached: Number(dbJson?.market_data_symbol_cache_match_count ?? 0) || 0,
        pageRows: Number(dbJson?.market_data_market_page_live_fetch_count ?? 0) || 0,
        snapshot: Boolean(dbJson?.summary_snapshot),
      };
      const nextLastUpdated = dbJson?.updated_at || ownedJson?.updated_at || new Date().toISOString();

      setOwnedAssetKeys(nextOwnedKeys);
      setPayloadSourceKeys(nextSourceKeys);
      setPayloadCounts(nextPayloadCounts);
      setRows(nextRows);
      setErrors(nextErrors);
      setLastUpdated(nextLastUpdated);
      writeBrowserMetricSnapshot({
        rows: nextRows,
        ownedAssetKeys: nextOwnedKeys,
        payloadSourceKeys: nextSourceKeys,
        payloadCounts: nextPayloadCounts,
        lastUpdated: nextLastUpdated,
      });
    } catch (e) {
      if (!mountedRef.current || requestSeqRef.current !== seq || isAbortLikeError(e)) return;
      const priorNote = rows.length ? "; keeping last good snapshot" : "";
      const msg = String(e?.message || e || "Volume refresh failed");
      if (rows.length && isSoftMetricError({ message: msg })) {
        setErrors((prev) => [{ message: `${msg}; keeping last good snapshot` }, ...(Array.isArray(prev) ? prev : [])]);
        return;
      }
      const cachedSnapshot = readBrowserMetricSnapshot();
      if (!rows.length && cachedSnapshot?.rows?.length) {
        setRows(cachedSnapshot.rows);
        setOwnedAssetKeys(Array.isArray(cachedSnapshot.ownedAssetKeys) ? cachedSnapshot.ownedAssetKeys : []);
        setPayloadSourceKeys(Array.isArray(cachedSnapshot.payloadSourceKeys) ? cachedSnapshot.payloadSourceKeys : []);
        setPayloadCounts(cachedSnapshot.payloadCounts || {});
        setLastUpdated(cachedSnapshot.lastUpdated || null);
        setErrors([{ message: `${msg}; showing last browser-cached market metrics snapshot.` }]);
        return;
      }
      setErr(`${msg}${priorNote}`);
    } finally {
      if (mountedRef.current && requestSeqRef.current === seq) setBusy(false);
    }
  }

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      requestSeqRef.current += 1;
    };
  }, []);

  useEffect(() => {
    const cachedSnapshot = readBrowserMetricSnapshot();
    if (!cachedSnapshot?.rows?.length) return;
    setRows(cachedSnapshot.rows);
    setOwnedAssetKeys(Array.isArray(cachedSnapshot.ownedAssetKeys) ? cachedSnapshot.ownedAssetKeys : []);
    setPayloadSourceKeys(Array.isArray(cachedSnapshot.payloadSourceKeys) ? cachedSnapshot.payloadSourceKeys : []);
    setPayloadCounts(cachedSnapshot.payloadCounts || {});
    setLastUpdated(cachedSnapshot.lastUpdated || null);
  }, []);

  useEffect(() => {
    doRefresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiBase]);

  useEffect(() => {
    if (!autoRefresh) return;
    const ms = clampSeconds(refreshSeconds, 300) * 1000;
    const t = setInterval(() => doRefresh({ forceLive: false }), ms);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, refreshSeconds, apiBase]);

  const sourceOptions = useMemo(() => {
    const keys = new Set();
    (Array.isArray(payloadSourceKeys) ? payloadSourceKeys : []).forEach((key) => {
      const k = normalizeSourceKey(key);
      if (k) keys.add(k);
    });
    (Array.isArray(rows) ? rows : []).forEach((r) => {
      rowSourceKeys(r).forEach((key) => keys.add(key));
    });
    return Array.from(keys).filter(Boolean).sort((a, b) => rowSourceLabel(a).localeCompare(rowSourceLabel(b)));
  }, [rows, payloadSourceKeys]);

  const rowCounts = useMemo(() => {
    const sourceRows = (Array.isArray(rows) ? rows : []).filter((r) => rowMatchesSource(r, sourceFilter));
    let owned = 0;
    let unowned = 0;
    sourceRows.forEach((r) => {
      if (rowIsOwnedForSource(r, ownedAssetKeys, sourceFilter)) owned += 1;
      else unowned += 1;
    });
    return { all: sourceRows.length, owned, unowned };
  }, [rows, ownedAssetKeys, sourceFilter]);

  const sortedRows = useMemo(() => {
    const arr = (Array.isArray(rows) ? rows : [])
      .filter((r) => rowMatchesSource(r, sourceFilter))
      .slice();

    return arr.sort((a, b) => compareMetricRows(a, b, sortKey, sortDir));
  }, [rows, sourceFilter, sortDir]);

  const visibleErrors = useMemo(() => {
    const hasUsableRows = (Array.isArray(sortedRows) ? sortedRows : []).some(volumeRowHasData);
    return (Array.isArray(errors) ? errors : []).filter((e) => !hasUsableRows || !isSoftMetricError(e));
  }, [errors, sortedRows]);

  const selectedRow = useMemo(() => {
    const a = String(selectedAsset || "").trim().toUpperCase();
    if (!a) return null;
    return sortedRows.find((r) => rowAssetKey(r) === a) || null;
  }, [sortedRows, selectedAsset]);

  const topLine = useMemo(() => {
    const preferred = selectedRow || sortedRows.find((r) => toNum(r?.volume_24h_usd) !== null);
    const knownNote = sourceFilter === "all" && payloadCounts?.known
      ? ` • ${payloadCounts.asset || rowCounts.all}/${payloadCounts.known} known loaded`
      : "";
    const modeNote = payloadCounts?.mode === "summary_snapshot" || payloadCounts?.snapshot
      ? " • summary-cache"
      : (payloadCounts?.mode === "cache_snapshot"
        ? " • cache-first"
        : (payloadCounts?.live ? ` • live ${payloadCounts.live}` : ""));
    const dataNote = payloadCounts?.symbolMatched
      ? ` • market ${payloadCounts.ids || 0} id / ${payloadCounts.symbolMatched} symbol`
      : (payloadCounts?.ids ? ` • market ${payloadCounts.ids} id` : "");
    if (!preferred) return selectedAsset ? `${selectedAsset} vol —${knownNote}${dataNote}` : `24h volume data (${rowCounts.all} rows • ${rowCounts.owned} owned / ${rowCounts.unowned} unowned${knownNote}${modeNote}${dataNote})`;
    const asset = String(preferred.asset || selectedAsset || "—").toUpperCase();
    return `${asset} vol ${fmtMoney(preferred.volume_24h_usd, hideTableData)} • ${rowCounts.all} rows • ${rowCounts.owned} owned / ${rowCounts.unowned} unowned${knownNote}${modeNote}${dataNote}`;
  }, [selectedRow, selectedAsset, sortedRows, hideTableData, rowCounts, payloadCounts, sourceFilter]);

  return (
    <div style={ui.wrap}>
      <div style={ui.header} onMouseDown={onDragHandleMouseDown} title={onDragHandleMouseDown ? "Drag to move" : undefined}>
        <div>
          <div style={ui.title}>Volume</div>
          <div style={ui.sub}>{hideTableData ? "••••" : topLine}</div>
        </div>

        <div style={ui.right}>
          <div
            style={{
              ...ui.ctl,
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              fontSize: 12,
              whiteSpace: "nowrap",
              cursor: "default",
            }}
            title="Rows are not hidden by ownership here; use the Scope column to distinguish owned and unowned assets. Backend request limit is 1000; open/auto refresh uses cache-first mode."
          >
            <span>All {rowCounts.all}</span>
            <span style={{ opacity: 0.65 }}>•</span>
            <span>Owned {rowCounts.owned}</span>
            <span style={{ opacity: 0.65 }}>•</span>
            <span>Unowned {rowCounts.unowned}</span>
          </div>

          <select
            style={{ ...ui.ctl, maxWidth: 190 }}
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value)}
            title="Filter by venue / DEX when backend rows provide venue coverage"
          >
            <option value="all">All Venues / Sources</option>
            {sourceOptions.map((src) => (
              <option key={src} value={src}>{rowSourceLabel(src)}</option>
            ))}
          </select>


          <select
            style={ui.ctl}
            value={sortDir}
            onChange={(e) => setSortDir(e.target.value)}
            title="Sort direction"
          >
            <option value="desc">Desc</option>
            <option value="asc">Asc</option>
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
            placeholder="seconds"
          />

          <button
            style={ui.btn}
            onClick={() => doRefresh({ forceLive: true })}
            disabled={busy}
            title="Refresh live market data and update the backend cache. Window open/auto refresh use the cache-first path."
          >
            {busy ? "Refreshing…" : "Refresh"}
          </button>

          {onClose ? (
            <button style={ui.btn} onClick={onClose}>
              Close
            </button>
          ) : null}
        </div>
      </div>

      <div style={ui.body}>
        {err ? (
          <div style={{ ...ui.card, marginBottom: 10, borderColor: "rgba(255,100,100,0.45)", color: "var(--utt-danger, #ff7b7b)" }}>
            {err}
          </div>
        ) : null}

        {visibleErrors.length ? (
          <div style={{ ...ui.card, marginBottom: 10, fontSize: 12, opacity: 0.85 }}>
            {visibleErrors.slice(0, 3).map((e, idx) => (
              <div key={idx}>{String(e?.message || e?.error || e)}</div>
            ))}
          </div>
        ) : null}

        <div style={{ overflow: "auto", border: "1px solid var(--utt-border-1, #2a2a2a)", borderRadius: 12 }}>
          <table style={ui.table}>
            <thead>
              <tr>
                <th style={ui.th}>Asset / Pair</th>
                <th style={ui.th}>Scope</th>
                <th style={ui.th}>Venue</th>
                <th style={ui.th}>Chain</th>
                <th style={ui.th}>24h Volume USD</th>
                <th style={ui.th}>24h Base Vol</th>
                <th style={ui.th}>Liquidity USD</th>
                <th style={ui.th}>Source</th>
                <th style={ui.th}>Updated</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.length ? (
                sortedRows.map((r, idx) => {
                  const asset = String(r?.pair || r?.asset || "—").toUpperCase();
                  const warnings = Array.isArray(r?.warnings) ? r.warnings.filter(Boolean) : [];
                  return (
                    <tr key={`${asset}:${r?.venue || ""}:${r?.source || ""}:${idx}`}>
                      <td style={{ ...ui.td, fontWeight: 900 }}>
                        {hideTableData ? "••••" : asset}
                        {warnings.length ? (
                          <div style={{ fontSize: 10, opacity: 0.62, marginTop: 2 }} title={warnings.join("; ")}>
                            {warnings.length} warning{warnings.length === 1 ? "" : "s"}
                          </div>
                        ) : null}
                      </td>
                      <td style={ui.td}>
                        <span style={ui.pill}>{hideTableData ? "••••" : scopeLabel(r, ownedAssetKeys, sourceFilter)}</span>
                      </td>
                      <td style={ui.td}>{hideTableData ? "••••" : r?.venue || "global"}</td>
                      <td style={ui.td}>{hideTableData ? "••••" : r?.chain || "—"}</td>
                      <td style={{ ...ui.td, fontWeight: 800 }}>{fmtMoney(r?.volume_24h_usd, hideTableData)}</td>
                      <td style={ui.td}>{fmtNum(r?.volume_24h_base, hideTableData)}</td>
                      <td style={ui.td}>{fmtMoney(r?.liquidity_usd, hideTableData)}</td>
                      <td style={ui.td} title={cellTitle(r?.source)}>
                        <span style={ui.pill}>{hideTableData ? "••••" : r?.source || "—"}</span>
                      </td>
                      <td style={{ ...ui.td, ...ui.mono, fontSize: 11 }}>{fmtTime(r?.updated_at || lastUpdated, hideTableData)}</td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td style={ui.td} colSpan={9}>
                    {busy ? "Loading volume metrics…" : "No volume rows returned yet."}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={{ marginTop: 10, fontSize: 12, opacity: 0.78 }}>
          Last updated: <span style={ui.mono}>{hideTableData ? "••••" : lastUpdated || "—"}</span>
        </div>
      </div>
    </div>
  );
}
