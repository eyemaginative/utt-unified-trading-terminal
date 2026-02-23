// frontend/src/features/scanners/LosersWindow.jsx
import { useEffect, useMemo, useRef, useState } from "react";

function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

function trimApiBase(apiBase) {
  const s = String(apiBase || "").trim();
  return s.replace(/\/+$/, "");
}

function toNum(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function fmt2(n) {
  const x = toNum(n);
  if (x === null) return "—";
  return x.toFixed(2);
}

function fmtQty(n) {
  const x = toNum(n);
  if (x === null) return "—";
  const s = x.toFixed(8).replace(/0+$/, "").replace(/\.$/, "");
  return s || "0";
}

function fmtUsd(n) {
  const x = toNum(n);
  if (x === null) return "—";
  return x.toFixed(2);
}

function normalizeVenue(v) {
  return String(v || "").trim().toLowerCase();
}

function normalizeVenueFilterValue(v) {
  const s = normalizeVenue(v);
  if (!s) return "";
  if (s === "all" || s === "all venues" || s === "all enabled venues") return "";
  return s;
}

function safeKey(s) {
  return String(s || "").trim().toLowerCase();
}

/**
 * Some backends may return camelCase keys (totalUsd, pxUsd, change1d).
 * We support both snake_case and camelCase in sorting/formatting.
 */
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

function pickStr(r, keys, fallback = "") {
  if (!r || typeof r !== "object") return fallback;
  for (const k of keys) {
    const v = r[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") return String(v);
  }
  return fallback;
}

const NUM_FIELD_KEYS = {
  total: ["total", "qty", "quantity", "amount", "base_qty", "baseQty"],
  px_usd: ["px_usd", "pxUsd", "price_usd", "priceUsd", "usd_price", "usdPrice"],
  total_usd: ["total_usd", "totalUsd", "usd_total", "usdTotal", "value_usd", "valueUsd"],

  // IMPORTANT:
  // Your /api/scanners/top_gainers is currently emitting duplicate variants like:
  // - change_1h AND 1h_change
  // - change_1d AND 1d_change (or 24h variants)
  // - change_1w AND 1w_change (or 7d variants)
  // Robinhood/Kraken appear to be filling the alternate keys, so we accept both.
  change_1h: [
    "change_1h",
    "change1h",
    "1h_change",
    "1hChange",
    "pct_1h",
    "pct1h",
    "pct_change_1h",
    "pctChange1h",
    "percent_change_1h",
    "percentChange1h",
    "price_change_1h",
    "priceChange1h",
  ],
  change_1d: [
    "change_1d",
    "change1d",
    "1d_change",
    "1dChange",
    "pct_1d",
    "pct1d",
    "pct_change_1d",
    "pctChange1d",
    "percent_change_1d",
    "percentChange1d",

    // very common “1 day” as “24h”
    "change_24h",
    "change24h",
    "pct_change_24h",
    "pctChange24h",
    "percent_change_24h",
    "percentChange24h",
    "price_change_24h",
    "priceChange24h",
  ],
  change_1w: [
    "change_1w",
    "change1w",
    "1w_change",
    "1wChange",
    "pct_1w",
    "pct1w",
    "pct_change_1w",
    "pctChange1w",
    "percent_change_1w",
    "percentChange1w",

    // very common “1 week” as “7d”
    "change_7d",
    "change7d",
    "pct_change_7d",
    "pctChange7d",
    "percent_change_7d",
    "percentChange7d",
    "price_change_7d",
    "priceChange7d",
  ],
};

function rowVenue(r) {
  const v = normalizeVenue(r?.venue);
  return v || "";
}

function venuesTextForRow(r) {
  const v = rowVenue(r);
  return v ? v : "—";
}

function symbolFromAsset(asset) {
  const a = String(asset || "").trim().toUpperCase();
  if (!a) return "";
  // You can extend this later (e.g., fiat pairs), but for now your system is asset-USD.
  return `${a}-USD`;
}

/**
 * Canonicalize various symbol/pair formats into "BASE-QUOTE" (e.g. BTC-USD).
 * Handles: BTCUSD, btcusd, BTC/USD, btc_usd, BTC-USD, etc.
 */
function canonicalizeSymbol(symRaw) {
  const s0 = String(symRaw || "").trim();
  if (!s0) return "";
  const up = s0.toUpperCase();

  // Normalize separators to "-"
  let s = up.replace(/\s+/g, "").replace(/[\/_]/g, "-");

  // If already BASE-QUOTE, keep it
  if (s.includes("-")) {
    // If multiple dashes, keep first two segments as BASE-QUOTE (best-effort)
    const parts = s.split("-").filter(Boolean);
    if (parts.length >= 2) return `${parts[0]}-${parts[1]}`;
    return s;
  }

  // No separator: common "BTCUSD"
  // Best-effort: if ends with USD and length > 3, split.
  if (s.endsWith("USD") && s.length > 3) {
    const base = s.slice(0, -3);
    const quote = "USD";
    return `${base}-${quote}`;
  }

  return s;
}

function buildRowsFromBalancesItems(items, allowedVenuesSet) {
  const out = [];
  const heldSymbols = new Set();

  const arr = Array.isArray(items) ? items : [];
  for (const b of arr) {
    if (!b || typeof b !== "object") continue;

    const v = normalizeVenue(b.venue);
    if (allowedVenuesSet && allowedVenuesSet.size > 0) {
      if (!v || !allowedVenuesSet.has(v)) continue;
    }

    const asset = String(b.asset || "").trim().toUpperCase();
    if (!asset || asset === "USD") continue;

    const qty = toNum(b.total) ?? 0;
    if (!qty || Math.abs(qty) <= 0) continue;

    const sym = symbolFromAsset(asset);
    if (!sym) continue;

    heldSymbols.add(sym);

    out.push({
      // canonical display fields
      venue: v,
      asset,
      symbol: sym,

      // numeric fields (match NUM_FIELD_KEYS as much as possible)
      total: qty,
      px_usd: toNum(b.px_usd),
      total_usd: toNum(b.total_usd),

      // market enrichment placeholders
      change_1h: null,
      change_1d: null,
      change_1w: null,
    });
  }

  return { rows: out, heldSymbols: Array.from(heldSymbols).sort() };
}

export default function LosersWindow({
  apiBase,
  enabledVenues = [],
  hideTableData = false,

  // optional controlled filter
  venueFilter: venueFilterProp,
  onVenueFilterChange,

  // optional seed for local filter
  initialVenueFilter = "",

  // keep signature parity (optional)
  onTopGainer,

  onClose,
  height = 560,

  // lets parent make this window draggable by using the header as a handle
  onDragHandleMouseDown,
}) {
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshSeconds, setRefreshSeconds] = useState(300);
  const [lastUpdated, setLastUpdated] = useState(null);

  const enabledVenuesNorm = useMemo(
    () => (enabledVenues || []).map((v) => normalizeVenue(v)).filter(Boolean),
    [enabledVenues]
  );

  // local filter (used only when venueFilterProp is undefined)
  const [venueFilterLocal, setVenueFilterLocal] = useState(() =>
    normalizeVenueFilterValue(initialVenueFilter)
  );

  const venueFilterRaw = venueFilterProp !== undefined ? venueFilterProp : venueFilterLocal;
  const vf = useMemo(() => normalizeVenueFilterValue(venueFilterRaw), [venueFilterRaw]);

  const setVenueFilter = (v) => {
    const next = normalizeVenueFilterValue(v);
    if (typeof onVenueFilterChange === "function") onVenueFilterChange(next);
    else setVenueFilterLocal(next);
  };

  const [heldSymbols, setHeldSymbols] = useState([]);
  const [heldErr, setHeldErr] = useState(null);

  const [rows, setRows] = useState([]);
  const [scanErr, setScanErr] = useState(null);
  const [loading, setLoading] = useState(false);

  // DEFAULT SORT FOR LOSERS: 1d ASC
  const [sort, setSort] = useState({ key: "change_1d", dir: "asc" });

  // Prevent “flip-flop” due to overlapping refreshes completing out-of-order
  const refreshSeqRef = useRef(0);

  const ui = useMemo(
    () => ({
      wrap: {
        height,
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
        userSelect: "none",
      },
      title: { fontSize: 14, fontWeight: 900, margin: 0 },
      sub: { fontSize: 12, opacity: 0.75 },
      right: { display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" },
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
      warn: {
        border: "1px solid #4a1f1f",
        background: "#160b0b",
        color: "#ffd2d2",
        borderRadius: 12,
        padding: 10,
      },
      chips: { display: "flex", flexWrap: "wrap", gap: 6, marginTop: 8 },
      chip: {
        border: "1px solid #2a2a2a",
        background: "#0f0f0f",
        borderRadius: 999,
        padding: "4px 8px",
        fontSize: 11,
        opacity: 0.9,
      },
      tableWrap: {
        marginTop: 10,
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 12,
        overflow: "hidden",
      },
      thBtn: {
        appearance: "none",
        border: "none",
        background: "transparent",
        color: "inherit",
        padding: 0,
        margin: 0,
        cursor: "pointer",
        fontWeight: 900,
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
      },
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
      },
      td: {
        fontSize: 12,
        padding: "8px 10px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        whiteSpace: "nowrap",
      },
      tdR: {
        fontSize: 12,
        padding: "8px 10px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        whiteSpace: "nowrap",
        textAlign: "right",
        fontVariantNumeric: "tabular-nums",
      },
      sortArrow: { fontSize: 11, opacity: 0.9 },
    }),
    [height, onDragHandleMouseDown]
  );

  const mask = (s) => (hideTableData ? "••••" : String(s ?? "—"));
  const maskNum = (s) => (hideTableData ? "••••" : s);

  const venuesLabel = vf ? vf : "all enabled venues";

  function toggleSort(nextKey) {
    setSort((s) => {
      const key = String(nextKey || "");
      if (!key) return s;
      if (s.key === key) return { key: s.key, dir: s.dir === "asc" ? "desc" : "asc" };
      return { key, dir: "desc" };
    });
  }

  function getSortArrow(key) {
    if (sort.key !== key) return "";
    return sort.dir === "asc" ? "▲" : "▼";
  }

  // Core: build the table rows from balances (multi-venue truth), then enrich with market changes.
  async function refreshFromBalancesThenEnrich(seq) {
    const base = trimApiBase(apiBase);
    if (!base) {
      if (seq === refreshSeqRef.current) {
        setRows([]);
        setHeldSymbols([]);
        setHeldErr("No API base provided.");
        setScanErr(null);
      }
      return;
    }

    const vList = vf ? [vf] : enabledVenuesNorm.slice();
    const allowedSet = new Set(vList.map(normalizeVenue).filter(Boolean));

    const fetchBalancesForVenue = async (v) => {
      const url = `${base}/api/balances/latest?venue=${encodeURIComponent(v)}&with_prices=true`;
      const res = await fetch(url, { method: "GET" });
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`balances/latest(${v}) failed (${res.status}): ${txt || res.statusText}`);
      }
      return res.json();
    };

    // Market enrichment via scanner (single call with correct venue scoping).
    const fetchScannerAggregate = async () => {
      const p = new URLSearchParams();
      p.set("limit", "250");

      // If a venue filter is selected, include it explicitly (and also append venues= for compat).
      // Otherwise, pass all enabled venues as repeated venues= params (matches AppHeader logic).
      if (vf) {
        p.set("venue", vf);
        p.append("venues", vf);
      } else {
        for (const v of enabledVenuesNorm) p.append("venues", v);
      }

      // Intentionally unchanged: same enrichment source as TopGainersWindow.
      const url = `${base}/api/scanners/top_gainers?${p.toString()}`;
      const res = await fetch(url, { method: "GET" });
      if (!res.ok) return null;
      return res.json().catch(() => null);
    };

    try {
      if (seq === refreshSeqRef.current) {
        setHeldErr(null);
        setScanErr(null);
      }

      // 1) Pull balances across venues (fan-out).
      const settled = await Promise.allSettled(vList.map((v) => fetchBalancesForVenue(v)));

      const mergedBalanceItems = [];
      for (const r of settled) {
        if (r.status !== "fulfilled") continue;
        const items = Array.isArray(r.value?.items) ? r.value.items : [];
        mergedBalanceItems.push(...items);
      }

      const { rows: baseRows, heldSymbols: held } = buildRowsFromBalancesItems(mergedBalanceItems, allowedSet);

      if (seq === refreshSeqRef.current) {
        setRows(baseRows);
        setHeldSymbols(held);
        setHeldErr(null);
      }

      // If nothing to show, stop here.
      if (!baseRows.length) return;

      // 2) Enrich with scanner changes (best-effort, does not drive rows).
      const scanJson = await fetchScannerAggregate();

      // Build two maps:
      // - exactMap: venue:symbol -> changes (preferred when scanner provides venue per item)
      // - symMap: symbol -> changes (fallback if scanner items lack venue)
      const exactMap = new Map();
      const symMap = new Map();

      if (!scanJson || typeof scanJson !== "object") {
        if (seq === refreshSeqRef.current) {
          setScanErr("Scanner did not return JSON. (Non-fatal; balances table remains correct.)");
        }
        return;
      }

      const items = Array.isArray(scanJson.items) ? scanJson.items : [];

      for (const it of items) {
        if (!it || typeof it !== "object") continue;

        const venueGuess =
          normalizeVenue(it.venue) ||
          normalizeVenue(it.exchange) ||
          normalizeVenue(it.venue_id) ||
          normalizeVenue(it.source) ||
          "";

        const symRaw =
          pickStr(it, ["symbol", "pair", "market"], "") ||
          symbolFromAsset(pickStr(it, ["asset", "base", "ticker"], ""));

        const sym = canonicalizeSymbol(symRaw);
        if (!sym) continue;

        const payload = {
          change_1h: pickNum(it, NUM_FIELD_KEYS.change_1h),
          change_1d: pickNum(it, NUM_FIELD_KEYS.change_1d),
          change_1w: pickNum(it, NUM_FIELD_KEYS.change_1w),
        };

        // If all fields are null, skip (avoid overwriting useful data with nulls)
        if (payload.change_1h === null && payload.change_1d === null && payload.change_1w === null) continue;

        if (venueGuess) exactMap.set(`${venueGuess}:${sym}`, payload);

        // Symbol-level fallback: prefer the highest 1d if multiple entries exist
        const prev = symMap.get(sym);
        if (!prev) {
          symMap.set(sym, payload);
        } else {
          const prev1d = toNum(prev.change_1d);
          const next1d = toNum(payload.change_1d);
          if (prev1d === null && next1d !== null) symMap.set(sym, payload);
          else if (prev1d !== null && next1d !== null && next1d > prev1d) symMap.set(sym, payload);
        }
      }

      if (seq === refreshSeqRef.current) {
        setRows((prev) => {
          const arr = Array.isArray(prev) ? prev : [];
          return arr.map((r0) => {
            const v0 = normalizeVenue(r0.venue);
            const s0 = canonicalizeSymbol(r0.symbol);
            const exact = exactMap.get(`${v0}:${s0}`);
            const fallback = symMap.get(s0);

            const ch = exact || fallback;
            if (!ch) return r0;

            return {
              ...r0,
              change_1h: ch.change_1h ?? r0.change_1h,
              change_1d: ch.change_1d ?? r0.change_1d,
              change_1w: ch.change_1w ?? r0.change_1w,
            };
          });
        });

        // Non-fatal note only if scanner returns nothing useful for our held set
        const anyEnriched = baseRows.some((r0) => {
          const v0 = normalizeVenue(r0.venue);
          const s0 = canonicalizeSymbol(r0.symbol);
          return exactMap.has(`${v0}:${s0}`) || symMap.has(s0);
        });

        if (!anyEnriched) {
          setScanErr(
            "Scanner returned no matching change fields for held symbols. (Non-fatal; check /api/scanners/top_gainers payload and symbol formats.)"
          );
        } else {
          setScanErr(null);
        }
      }
    } catch (e) {
      if (seq === refreshSeqRef.current) {
        // If balances fail, show as primary error since balances drive the table.
        setRows([]);
        setHeldSymbols([]);
        setHeldErr(e?.message || "Failed to load balances for scanner window.");
      }
    }
  }

  async function doRefresh() {
    const seq = ++refreshSeqRef.current;
    setLoading(true);
    try {
      await refreshFromBalancesThenEnrich(seq);
      if (seq === refreshSeqRef.current) setLastUpdated(new Date().toISOString());
    } finally {
      if (seq === refreshSeqRef.current) setLoading(false);
    }
  }

  useEffect(() => {
    doRefresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiBase, enabledVenuesNorm.join("|"), vf]);

  useEffect(() => {
    if (!autoRefresh) return;
    const ms = clampSeconds(refreshSeconds, 300) * 1000;
    const t = setInterval(() => doRefresh(), ms);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, refreshSeconds, apiBase, enabledVenuesNorm.join("|"), vf]);

  // Sorting over the displayed rows (now based on balances rows).
  const sortedRows = useMemo(() => {
    const sortKey = sort.key;
    const sortDir = sort.dir;
    const dirMul = sortDir === "asc" ? 1 : -1;

    const isNumKey = new Set(["total", "px_usd", "total_usd", "change_1h", "change_1d", "change_1w"]).has(sortKey);

    const getVal = (r) => {
      switch (sortKey) {
        case "asset":
          return pickStr(r, ["asset", "base", "ticker"], "");
        case "symbol":
          return pickStr(r, ["symbol", "pair", "market"], "");
        case "venues":
          return venuesTextForRow(r);
        case "total":
          return pickNum(r, NUM_FIELD_KEYS.total);
        case "px_usd":
          return pickNum(r, NUM_FIELD_KEYS.px_usd);
        case "total_usd":
          return pickNum(r, NUM_FIELD_KEYS.total_usd);
        case "change_1h":
          return pickNum(r, NUM_FIELD_KEYS.change_1h);
        case "change_1d":
          return pickNum(r, NUM_FIELD_KEYS.change_1d);
        case "change_1w":
          return pickNum(r, NUM_FIELD_KEYS.change_1w);
        default:
          return pickNum(r, NUM_FIELD_KEYS.change_1d);
      }
    };

    const tieKey = (r) => {
      const a = safeKey(pickStr(r, ["asset", "base", "ticker"], ""));
      const s = safeKey(pickStr(r, ["symbol", "pair", "market"], ""));
      const v = safeKey(venuesTextForRow(r));
      return `${a}|${s}|${v}`;
    };

    const arr = Array.isArray(rows) ? [...rows] : [];
    arr.sort((a, b) => {
      const va = getVal(a);
      const vb = getVal(b);

      const aNull = va === null || va === undefined || va === "";
      const bNull = vb === null || vb === undefined || vb === "";

      if (aNull && bNull) return tieKey(a).localeCompare(tieKey(b));
      if (aNull) return 1;
      if (bNull) return -1;

      if (isNumKey) {
        const na = Number(va);
        const nb = Number(vb);

        const aBad = !Number.isFinite(na);
        const bBad = !Number.isFinite(nb);
        if (aBad && bBad) return tieKey(a).localeCompare(tieKey(b));
        if (aBad) return 1;
        if (bBad) return -1;

        if (na === nb) return tieKey(a).localeCompare(tieKey(b));
        return (na < nb ? -1 : 1) * dirMul;
      }

      const sa = String(va);
      const sb = String(vb);
      const c = sa.localeCompare(sb);
      if (c !== 0) return c * dirMul;

      return tieKey(a).localeCompare(tieKey(b));
    });

    return arr;
  }, [rows, sort]);

  useEffect(() => {
    if (typeof onTopGainer !== "function") return;

    const best = (sortedRows || []).find((r) => pickNum(r, NUM_FIELD_KEYS.change_1d) !== null);
    if (!best) {
      onTopGainer(null);
      return;
    }
    onTopGainer({
      asset: pickStr(best, ["asset", "base", "ticker"], null) || null,
      symbol: pickStr(best, ["symbol", "pair", "market"], null) || null,
      change_1d: pickNum(best, NUM_FIELD_KEYS.change_1d),
      venue_filter: vf || "",
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortedRows, vf]);

  const maskRow = (s) => (hideTableData ? "••••" : s);

  return (
    <div style={ui.wrap}>
      <div
        style={ui.header}
        onMouseDown={onDragHandleMouseDown}
        title={onDragHandleMouseDown ? "Drag the header to move this window" : undefined}
      >
        <div>
          <div style={ui.title}>Losers</div>
          <div style={ui.sub}>
            Shows held assets only. Venue: {mask(venuesLabel)} {loading ? " (Loading…)" : ""}
          </div>
        </div>

        <div style={ui.right}>
          <select
            style={{ ...ui.ctl, minWidth: 150 }}
            value={vf}
            onChange={(e) => setVenueFilter(e.target.value)}
            title="Filter by venue (defaults to all enabled venues)"
          >
            <option value="">All enabled venues</option>
            {enabledVenuesNorm.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
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
            title="Refresh seconds"
          />

          <button style={ui.btn} onClick={doRefresh}>
            Refresh
          </button>

          <button style={ui.btn} onClick={() => onClose?.()} title="Close">
            Close
          </button>
        </div>
      </div>

      <div style={ui.body}>
        {heldErr ? (
          <div style={ui.warn}>
            <div style={{ fontSize: 12, fontWeight: 900, marginBottom: 6 }}>Scanner error</div>
            <div style={{ fontSize: 12, opacity: 0.9 }}>{heldErr}</div>
            <div style={{ marginTop: 10, fontSize: 12, opacity: 0.85, ...ui.mono }}>
              - API base: {String(apiBase || "")}
              <br />- Balances driver: /api/balances/latest?with_prices=true (fanout)
            </div>
          </div>
        ) : (
          <div style={ui.card}>
            <div style={{ fontSize: 12, opacity: 0.85, ...ui.mono }}>
              - Enabled venues:{" "}
              {hideTableData ? "••••" : enabledVenuesNorm.length ? enabledVenuesNorm.join(", ") : "—"}
              <br />- Venue filter: {mask(venuesLabel)}
              <br />- Held symbols (from balances): {maskNum(String(heldSymbols.length))}
              <br />- Rows (from balances): {maskNum(String(rows.length))}
              <br />- Sort: {hideTableData ? "••••" : `${sort.key} ${sort.dir}`} (click header again to toggle asc/desc)
              {scanErr ? (
                <>
                  <br />- Scanner note: {scanErr}
                </>
              ) : null}
            </div>

            {!hideTableData && heldSymbols.length > 0 && (
              <div style={ui.chips}>
                {heldSymbols.slice(0, 24).map((s) => (
                  <div key={s} style={{ ...ui.chip, ...ui.mono }}>
                    {s}
                  </div>
                ))}
                {heldSymbols.length > 24 && (
                  <div style={{ ...ui.chip, opacity: 0.7 }}>+{heldSymbols.length - 24} more…</div>
                )}
              </div>
            )}

            <div style={{ marginTop: 10, fontSize: 12, opacity: 0.8 }}>
              Last updated: <span style={ui.mono}>{hideTableData ? "••••" : lastUpdated || "—"}</span>
            </div>
          </div>
        )}

        <div style={ui.tableWrap}>
          <div style={{ maxHeight: 420, overflow: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={ui.th}>
                    <button style={ui.thBtn} onClick={() => toggleSort("asset")} title="Sort by Asset">
                      Asset <span style={ui.sortArrow}>{getSortArrow("asset")}</span>
                    </button>
                  </th>
                  <th style={ui.th}>
                    <button style={ui.thBtn} onClick={() => toggleSort("symbol")} title="Sort by Symbol">
                      Symbol <span style={ui.sortArrow}>{getSortArrow("symbol")}</span>
                    </button>
                  </th>
                  <th style={ui.th}>
                    <button style={ui.thBtn} onClick={() => toggleSort("venues")} title="Sort by Venues">
                      Venues <span style={ui.sortArrow}>{getSortArrow("venues")}</span>
                    </button>
                  </th>
                  <th style={{ ...ui.th, textAlign: "right" }}>
                    <button style={ui.thBtn} onClick={() => toggleSort("total")} title="Sort by Qty">
                      Qty <span style={ui.sortArrow}>{getSortArrow("total")}</span>
                    </button>
                  </th>
                  <th style={{ ...ui.th, textAlign: "right" }}>
                    <button style={ui.thBtn} onClick={() => toggleSort("px_usd")} title="Sort by Px USD">
                      Px USD <span style={ui.sortArrow}>{getSortArrow("px_usd")}</span>
                    </button>
                  </th>
                  <th style={{ ...ui.th, textAlign: "right" }}>
                    <button style={ui.thBtn} onClick={() => toggleSort("total_usd")} title="Sort by Total USD">
                      Total USD <span style={ui.sortArrow}>{getSortArrow("total_usd")}</span>
                    </button>
                  </th>
                  <th style={{ ...ui.th, textAlign: "right" }}>
                    <button style={ui.thBtn} onClick={() => toggleSort("change_1h")} title="Sort by 1h change">
                      1h <span style={ui.sortArrow}>{getSortArrow("change_1h")}</span>
                    </button>
                  </th>
                  <th style={{ ...ui.th, textAlign: "right" }}>
                    <button style={ui.thBtn} onClick={() => toggleSort("change_1d")} title="Sort by 1d change">
                      1d <span style={ui.sortArrow}>{getSortArrow("change_1d")}</span>
                    </button>
                  </th>
                  <th style={{ ...ui.th, textAlign: "right" }}>
                    <button style={ui.thBtn} onClick={() => toggleSort("change_1w")} title="Sort by 1w change">
                      1w <span style={ui.sortArrow}>{getSortArrow("change_1w")}</span>
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedRows.length === 0 ? (
                  <tr>
                    <td colSpan={9} style={{ ...ui.td, opacity: 0.75 }}>
                      No rows yet. Confirm /api/balances/latest?with_prices=true is returning items for your enabled venues.
                    </td>
                  </tr>
                ) : (
                  sortedRows.map((r, idx) => {
                    const vText = venuesTextForRow(r);

                    const assetText = pickStr(r, ["asset", "base", "ticker"], "—");
                    const symbolText = pickStr(r, ["symbol", "pair", "market"], "—");

                    const qty = pickNum(r, NUM_FIELD_KEYS.total);
                    const pxUsd = pickNum(r, NUM_FIELD_KEYS.px_usd);
                    const totUsd = pickNum(r, NUM_FIELD_KEYS.total_usd);
                    const c1h = pickNum(r, NUM_FIELD_KEYS.change_1h);
                    const c1d = pickNum(r, NUM_FIELD_KEYS.change_1d);
                    const c1w = pickNum(r, NUM_FIELD_KEYS.change_1w);

                    const kVenue = safeKey(vText);
                    return (
                      <tr key={`${symbolText}:${kVenue}:${idx}`}>
                        <td style={{ ...ui.td, fontWeight: 900 }}>{hideTableData ? "••••" : assetText}</td>
                        <td style={{ ...ui.td, ...ui.mono }}>{hideTableData ? "••••" : symbolText}</td>
                        <td style={ui.td}>{hideTableData ? "••••" : vText}</td>
                        <td style={ui.tdR}>{hideTableData ? "••••" : fmtQty(qty)}</td>
                        <td style={ui.tdR}>{hideTableData ? "••••" : fmtUsd(pxUsd)}</td>
                        <td style={ui.tdR}>{hideTableData ? "••••" : fmtUsd(totUsd)}</td>
                        <td style={ui.tdR}>{hideTableData ? "••••" : c1h == null ? "—" : `${fmt2(c1h)}%`}</td>
                        <td style={ui.tdR}>{hideTableData ? "••••" : c1d == null ? "—" : `${fmt2(c1d)}%`}</td>
                        <td style={ui.tdR}>{hideTableData ? "••••" : c1w == null ? "—" : `${fmt2(c1w)}%`}</td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{ marginTop: 10, fontSize: 11, opacity: 0.7 }}>
          Note: 1h/1d/1w fields are enriched from /api/scanners/top_gainers (scoped by venue filter or all enabled venues).
          Rows remain balances-driven so holdings display is correct even if scanner coverage is incomplete.
        </div>
      </div>
    </div>
  );
}
