// frontend/src/ArbChip.jsx
import { useEffect, useMemo, useRef, useState } from "react";

/**
 * Same-tab snapshot coalescing:
 * - If multiple ArbChip instances (or rapid refreshes) request the same {apiBase,symbol,venues} concurrently,
 *   we only execute one fetch and share the promise result.
 * - Short TTL avoids burst fetches when multiple widgets mount together.
 */
const ARB_INFLIGHT = new Map(); // key -> Promise
const ARB_CACHE = new Map(); // key -> { ts, data }

function nowMs() {
  return Date.now();
}

async function sharedArbCall(key, fn, { ttlMs = 1200 } = {}) {
  const k = String(key || "");
  if (!k) return await fn();

  const now = nowMs();
  const cached = ARB_CACHE.get(k);
  if (cached && now - cached.ts <= ttlMs) return cached.data;

  const existing = ARB_INFLIGHT.get(k);
  if (existing) return existing;

  const p = (async () => {
    const data = await fn();
    ARB_CACHE.set(k, { ts: nowMs(), data });
    return data;
  })();

  ARB_INFLIGHT.set(k, p);

  try {
    return await p;
  } finally {
    ARB_INFLIGHT.delete(k);
  }
}

export default function ArbChip({
  apiBase,
  symbol,
  venues,
  refreshMs = 8000,
  fmtPrice,
  hideTableData = false,
  hideVenueNames = false,
  styles,
  thresholdPct = 0.1,
  fetchArbSnapshot,
  fetchArbSnapshotMode = "object",
  popoverAlign = "right",

  chipVariant = "pill",
  chipTitle = "Arb",
}) {
  const [open, setOpen] = useState(false);
  const [snap, setSnap] = useState(null);
  const [err, setErr] = useState(null);
  const [loading, setLoading] = useState(false);

  // ─────────────────────────────────────────────────────────────
  // Persisted UI settings
  // ─────────────────────────────────────────────────────────────
  const LS_AUTO = "utt_arbchip_auto_refresh";
  const LS_REFRESH_MS = "utt_arbchip_refresh_ms";
  const LS_BG_MODE = "utt_arbchip_bg_mode"; // "open_only" | "slow" | "normal"
  const LS_BG_SLOW_MS = "utt_arbchip_bg_slow_ms";

  const readLSBool = (k, dflt) => {
    try {
      const v = localStorage.getItem(k);
      if (v === null || v === undefined || v === "") return dflt;
      if (v === "1" || v === "true" || v === "yes" || v === "on") return true;
      if (v === "0" || v === "false" || v === "no" || v === "off") return false;
      return dflt;
    } catch {
      return dflt;
    }
  };

  const readLSNum = (k, dflt) => {
    try {
      const v = localStorage.getItem(k);
      const n = Number(v);
      return Number.isFinite(n) ? n : dflt;
    } catch {
      return dflt;
    }
  };

  const readLSStr = (k, dflt) => {
    try {
      const v = localStorage.getItem(k);
      return (v ?? "").trim() || dflt;
    } catch {
      return dflt;
    }
  };

  const [autoRefresh, setAutoRefresh] = useState(() => readLSBool(LS_AUTO, true));

  const [uiRefreshMs, setUiRefreshMs] = useState(() => {
    const initial = Math.max(1500, Number(refreshMs) || 8000);
    return Math.max(1500, readLSNum(LS_REFRESH_MS, initial));
  });

  const [bgMode, setBgMode] = useState(() => readLSStr(LS_BG_MODE, "slow"));

  const [bgSlowMs, setBgSlowMs] = useState(() => {
    const n = readLSNum(LS_BG_SLOW_MS, 30000);
    return Math.max(1500, n);
  });

  useEffect(() => {
    try {
      localStorage.setItem(LS_AUTO, autoRefresh ? "1" : "0");
      localStorage.setItem(LS_REFRESH_MS, String(uiRefreshMs));
      localStorage.setItem(LS_BG_MODE, String(bgMode));
      localStorage.setItem(LS_BG_SLOW_MS, String(bgSlowMs));
    } catch {
      // ignore
    }
  }, [autoRefresh, uiRefreshMs, bgMode, bgSlowMs]);

  // ─────────────────────────────────────────────────────────────
  // Style fallbacks
  // ─────────────────────────────────────────────────────────────
  const safeStyles = styles || {};
  const pill =
    safeStyles.pill || {
      border: "1px solid #2a2a2a",
      background: "#101010",
      borderRadius: 999,
      padding: "6px 10px",
    };
  const button =
    safeStyles.button || {
      border: "1px solid #2a2a2a",
      background: "#151515",
      borderRadius: 10,
      padding: "6px 10px",
      color: "#eaeaea",
    };
  const muted = safeStyles.muted || { color: "#a9a9a9" };

  // ─────────────────────────────────────────────────────────────
  // Stable inputs
  // ─────────────────────────────────────────────────────────────
  const sym = useMemo(() => String(symbol || "").trim(), [symbol]);
  const vList = useMemo(() => {
    return Array.isArray(venues) ? venues.map((v) => String(v || "").trim()).filter(Boolean) : [];
  }, [venues]);
  const vKey = useMemo(() => vList.join("|"), [vList]);

  const maskVenue = (v) => (hideVenueNames ? "••••" : String(v || "—"));
  const mask = (s) => (hideTableData ? "••••" : s);

  const toNum = (x) => {
    if (x === null || x === undefined) return null;
    const n = Number(x);
    return Number.isFinite(n) ? n : null;
  };

  const priceFmtBase = (x) => {
    if (!fmtPrice) return String(x ?? "—");
    return fmtPrice(x);
  };

  const priceFmtArb = (x) => {
    const n = toNum(x);
    if (n === null) return String(x ?? "—");

    const s = priceFmtBase(x);
    if (typeof s !== "string") return String(s ?? "—");

    if (Math.abs(n) >= 1) {
      if (!s.includes(".")) return `${s}.00`;
      const [intPart, fracPartRaw] = s.split(".");
      const fracPart = fracPartRaw ?? "";
      if (fracPart.length === 0) return `${intPart}.00`;
      if (fracPart.length === 1) return `${intPart}.${fracPart}0`;
      return s;
    }
    return s;
  };

  // ─────────────────────────────────────────────────────────────
  // Snapshot normalization
  // ─────────────────────────────────────────────────────────────
  const normalizeSnapshot = (raw) => {
    if (!raw || typeof raw !== "object") return null;

    if (typeof raw.detail === "string" && !raw.bestAsk && !raw.best_ask) {
      return { _error: raw.detail };
    }

    const bestAskRaw = raw.bestAsk ?? raw.best_ask ?? null;
    const bestBidRaw = raw.bestBid ?? raw.best_bid ?? null;

    const bestAskVenue = raw.bestAskVenue ?? raw.best_ask_venue ?? bestAskRaw?.venue ?? bestAskRaw?.exchange ?? null;
    const bestBidVenue = raw.bestBidVenue ?? raw.best_bid_venue ?? bestBidRaw?.venue ?? bestBidRaw?.exchange ?? null;

    const bestAskPrice =
      raw.bestAskPrice ??
      raw.best_ask_price ??
      bestAskRaw?.price ??
      bestAskRaw?.ask ??
      (typeof bestAskRaw === "number" ? bestAskRaw : null);

    const bestBidPrice =
      raw.bestBidPrice ??
      raw.best_bid_price ??
      bestBidRaw?.price ??
      bestBidRaw?.bid ??
      (typeof bestBidRaw === "number" ? bestBidRaw : null);

    const perVenueRaw = raw.perVenue ?? raw.per_venue ?? raw.venues ?? raw.items ?? null;
    const perVenue = Array.isArray(perVenueRaw)
      ? perVenueRaw.map((r) => {
          const venue = r?.venue ?? r?.exchange ?? r?.name ?? r?.key ?? "—";
          const ask =
            r?.ask ??
            r?.bestAsk ??
            r?.best_ask ??
            r?.bestAskPrice ??
            r?.best_ask_price ??
            r?.topAsk ??
            r?.top_ask ??
            null;
          const bid =
            r?.bid ??
            r?.bestBid ??
            r?.best_bid ??
            r?.bestBidPrice ??
            r?.best_bid_price ??
            r?.topBid ??
            r?.top_bid ??
            null;
          return { venue, ask, bid };
        })
      : [];

    return {
      bestAsk: { venue: bestAskVenue, price: bestAskPrice },
      bestBid: { venue: bestBidVenue, price: bestBidPrice },
      perVenue,
    };
  };

  // ─────────────────────────────────────────────────────────────
  // Cross-tab leader lock + shared cache
  // ─────────────────────────────────────────────────────────────
  const tabIdRef = useRef(
    (() => {
      try {
        if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
      } catch {
        // ignore
      }
      return `tab_${Math.random().toString(16).slice(2)}_${Date.now()}`;
    })()
  );

  const lockKey = useMemo(() => `utt:lock:arb:${sym}:${vKey}`, [sym, vKey]);
  const cacheKey = useMemo(() => `utt:cache:arb:${sym}:${vKey}`, [sym, vKey]);

  const readJsonLS = (k) => {
    try {
      const s = localStorage.getItem(k);
      if (!s) return null;
      return JSON.parse(s);
    } catch {
      return null;
    }
  };

  const writeJsonLS = (k, obj) => {
    try {
      localStorage.setItem(k, JSON.stringify(obj));
      return true;
    } catch {
      return false;
    }
  };

  const effectiveIntervalMs = useMemo(() => {
    const fg = Math.max(1500, Number(uiRefreshMs) || 8000);
    const bg = Math.max(1500, Number(bgSlowMs) || 30000);

    if (!autoRefresh) return null;

    if (bgMode === "open_only") {
      return open ? fg : null;
    }
    if (bgMode === "slow") {
      return open ? fg : bg;
    }
    return fg;
  }, [autoRefresh, uiRefreshMs, bgSlowMs, bgMode, open]);

  const leaseTtlMs = useMemo(() => {
    const base = Math.max(15000, Number(effectiveIntervalMs || 0) * 3 || 15000);
    return Math.min(120000, base);
  }, [effectiveIntervalMs]);

  const isLeaderForThisKey = () => {
    if (!sym || !vKey) return false;

    const now = Date.now();
    const mine = tabIdRef.current;

    const cur = readJsonLS(lockKey);
    const curId = cur?.id;
    const curExp = Number(cur?.expires || 0);

    if (!curId || curExp <= now || curId === mine) {
      writeJsonLS(lockKey, { id: mine, expires: now + leaseTtlMs });
    }

    const check = readJsonLS(lockKey);
    return check?.id === mine && Number(check?.expires || 0) > now;
  };

  const setFromCache = () => {
    const c = readJsonLS(cacheKey);
    const data = c?.data;
    if (!data) return false;
    const norm = normalizeSnapshot(data);
    if (norm?._error) return false;
    if (!norm) return false;
    setSnap(norm);
    return true;
  };

  useEffect(() => {
    if (!cacheKey) return;

    setFromCache();

    const onStorage = (e) => {
      if (!e) return;
      if (e.key !== cacheKey) return;
      setFromCache();
    };

    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cacheKey]);

  // ─────────────────────────────────────────────────────────────
  // Polling controls + backoff
  // ─────────────────────────────────────────────────────────────
  const timerRef = useRef(null);
  const inFlightRef = useRef(false);
  const lastFetchAtRef = useRef(0);
  const cooldownUntilRef = useRef(0);

  const shouldPollNow = () => {
    try {
      if (typeof document !== "undefined" && document.visibilityState && document.visibilityState !== "visible") {
        return false;
      }
    } catch {
      // ignore
    }
    return true;
  };

  const looksRateLimited = (msg) => {
    const s = String(msg || "").toLowerCase();
    return s.includes("429") || s.includes("rate limit") || s.includes("rate-limited") || s.includes("too many requests");
  };

  const callFetchArbSnapshotOnce = async () => {
    if (typeof fetchArbSnapshot !== "function") {
      throw new Error("fetchArbSnapshot is not a function");
    }

    const mode = String(fetchArbSnapshotMode || "object").toLowerCase();

    if (mode === "args") {
      return await fetchArbSnapshot(sym, vList, apiBase);
    }

    return await fetchArbSnapshot({ apiBase, symbol: sym, venues: vList });
  };

  async function loadOnce({ reason } = {}) {
    if (!sym || vList.length === 0 || typeof fetchArbSnapshot !== "function") {
      setSnap(null);
      setErr(null);
      return;
    }

    if (!shouldPollNow()) return;

    const leader = isLeaderForThisKey();
    if (!leader && reason !== "manual") {
      setFromCache();
      return;
    }

    const now = Date.now();
    if (now < cooldownUntilRef.current && reason !== "manual") {
      return;
    }

    if (inFlightRef.current) return;

    const minGap = Math.max(250, Number(effectiveIntervalMs || 0) || 0);
    if (minGap > 0 && now - lastFetchAtRef.current < minGap * 0.5 && reason !== "manual") {
      return;
    }

    inFlightRef.current = true;
    lastFetchAtRef.current = now;

    setLoading(true);
    setErr(null);

    try {
      const callKey = `arb:${String(apiBase || "")}|${sym}|${vKey}`;
      const raw = await sharedArbCall(
        callKey,
        async () => await callFetchArbSnapshotOnce(),
        { ttlMs: 1200 }
      );

      const norm = normalizeSnapshot(raw);

      if (norm?._error) {
        setSnap(null);
        setErr(norm._error);
        if (looksRateLimited(norm._error)) cooldownUntilRef.current = Date.now() + 30_000;
      } else if (!norm) {
        setSnap(null);
        setErr("No arbitrage snapshot returned.");
      } else {
        setSnap(norm);
        writeJsonLS(cacheKey, { ts: Date.now(), data: norm });
      }
    } catch (e) {
      const msg = e?.message || "Arb fetch failed";
      setSnap(null);
      setErr(msg);

      if (looksRateLimited(msg)) cooldownUntilRef.current = Date.now() + 30_000;
    } finally {
      inFlightRef.current = false;
      setLoading(false);
    }
  }

  useEffect(() => {
    loadOnce({ reason: "mount" });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiBase, sym, vKey]);

  // Jittered setTimeout loop (instead of setInterval) to avoid synchronization bursts
  useEffect(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }

    if (!effectiveIntervalMs) return;

    let canceled = false;

    const loop = async () => {
      if (canceled) return;
      await loadOnce({ reason: "interval" });
      if (canceled) return;
      timerRef.current = setTimeout(loop, Math.max(1500, Number(effectiveIntervalMs) || 8000));
    };

    const jitterMs = Math.floor(Math.random() * 800); // 0–800ms
    timerRef.current = setTimeout(loop, Math.max(0, jitterMs));

    return () => {
      canceled = true;
      if (timerRef.current) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [effectiveIntervalMs, apiBase, sym, vKey]);

  const spreadPct = useMemo(() => {
    const ask = toNum(snap?.bestAsk?.price);
    const bid = toNum(snap?.bestBid?.price);
    if (ask === null || bid === null || ask <= 0) return null;
    return ((bid - ask) / ask) * 100;
  }, [snap]);

  const spreadAbs = useMemo(() => {
    const ask = toNum(snap?.bestAsk?.price);
    const bid = toNum(snap?.bestBid?.price);
    if (ask === null || bid === null) return null;
    return bid - ask;
  }, [snap]);

  const pctLabel = useMemo(() => {
    if (spreadPct === null || spreadPct === undefined) return "—";
    const v = spreadPct;
    const sign = v > 0 ? "+" : "";
    return `${sign}${v.toFixed(2)}%`;
  }, [spreadPct]);

  const tone = useMemo(() => {
    if (err) return "warn";
    if (!snap || spreadPct === null) return "neutral";
    if (spreadPct >= thresholdPct) return "good";
    if (spreadPct > 0) return "soft";
    return "neutral";
  }, [snap, spreadPct, thresholdPct, err]);

  const chipStyle = useMemo(() => {
    const base = {
      ...pill,
      display: "inline-flex",
      alignItems: "center",
      gap: 8,
      cursor: "pointer",
      userSelect: "none",
    };
    if (tone === "good") return { ...base, border: "1px solid #203a20", background: "#0f1a0f", color: "#cdeccd" };
    if (tone === "soft") return { ...base, border: "1px solid #3b3413", background: "#151208", color: "#f2e6b7" };
    if (tone === "warn") return { ...base, border: "1px solid #4a1f1f", background: "#160b0b", color: "#ffd2d2" };
    return { ...base, border: "1px solid #2a2a2a", background: "#101010", color: "#cfcfcf" };
  }, [pill, tone]);

  const intervalLabel = useMemo(() => {
    if (!effectiveIntervalMs) return "off";
    const ms = Number(effectiveIntervalMs) || 0;
    if (ms >= 1000) return `${Math.round(ms / 100) / 10}s`;
    return `${ms}ms`;
  }, [effectiveIntervalMs]);

  const selectStyle = {
    ...button,
    padding: "6px 10px",
    height: 32,
  };

  const popoverPos = useMemo(() => {
    const a = String(popoverAlign || "right").toLowerCase();
    return a === "left" ? { left: 0 } : { right: 0 };
  }, [popoverAlign]);

  const chipAsToolTab = String(chipVariant || "pill").toLowerCase() === "tooltab";

  const toolTabBaseStyle = useMemo(
    () => ({
      display: "inline-flex",
      flexDirection: "column",
      alignItems: "flex-start",
      justifyContent: "center",
      gap: 2,
      padding: "8px 12px",
      borderRadius: 999,
      border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
      background: "rgba(255,255,255,0.04)",
      color: "inherit",
      cursor: "pointer",
      userSelect: "none",
      minWidth: 140,
    }),
    []
  );

  const toolTabOpenStyle = useMemo(
    () => ({
      ...toolTabBaseStyle,
      border: "1px solid rgba(140, 190, 255, 0.55)",
      background: "rgba(140, 190, 255, 0.10)",
      boxShadow: "0 0 0 1px rgba(140, 190, 255, 0.18) inset",
    }),
    [toolTabBaseStyle]
  );

  const toolTabSymLabel = useMemo(() => {
    if (hideTableData) return "••••";
    return sym || "—";
  }, [hideTableData, sym]);

  const toolTabSubLabel = useMemo(() => {
    if (hideTableData) return "••••";
    if (!toolTabSymLabel || toolTabSymLabel === "—") return "—";
    if (loading) return `${toolTabSymLabel} …`;
    if (err) return `${toolTabSymLabel} Error`;
    if (!snap || pctLabel === "—") return `${toolTabSymLabel} —`;
    return `${toolTabSymLabel} ${pctLabel}`;
  }, [hideTableData, toolTabSymLabel, loading, err, snap, pctLabel]);

  return (
    <div style={{ position: "relative", display: "inline-block" }}>
      {chipAsToolTab ? (
        <button
          type="button"
          onClick={() => setOpen((p) => !p)}
          style={open ? toolTabOpenStyle : toolTabBaseStyle}
          title={err ? `Arb error: ${err}` : `Best bid/ask across venues for ${sym} (auto: ${intervalLabel})`}
        >
          <div style={{ display: "flex", alignItems: "baseline", gap: 8, lineHeight: 1.1 }}>
            <span style={{ fontWeight: 800, fontSize: 13 }}>{chipTitle}</span>
            <span style={{ fontSize: 11, opacity: 0.75 }}>{open ? "Open" : "Closed"}</span>
          </div>
          <div style={{ fontSize: 11, opacity: 0.75, fontVariantNumeric: "tabular-nums" }}>{toolTabSubLabel}</div>
        </button>
      ) : (
        <div
          style={chipStyle}
          title={err ? `Arb error: ${err}` : `Best bid/ask across venues for ${sym} (auto: ${intervalLabel})`}
          onClick={() => setOpen((p) => !p)}
        >
          <b style={{ letterSpacing: 0.2 }}>{chipTitle}</b>
          <span style={{ fontVariantNumeric: "tabular-nums" }}>{loading ? "…" : pctLabel}</span>
        </div>
      )}

      {open && (
        <div
          style={{
            position: "absolute",
            marginTop: 8,
            width: "min(560px, 92vw)",
            borderRadius: 14,
            border: "1px solid #2a2a2a",
            background: "#0f0f0f",
            boxShadow: "0 12px 40px rgba(0,0,0,0.55)",
            padding: 12,
            zIndex: 9999,
            ...popoverPos,
          }}
        >
          <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 12 }}>
            <div style={{ fontSize: 12, fontWeight: 900 }}>
              Arbitrage snapshot: <span style={muted}>{hideTableData ? "••••" : sym}</span>
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button style={{ ...button, padding: "6px 10px" }} onClick={() => loadOnce({ reason: "manual" })} type="button">
                Refresh
              </button>
              <button style={{ ...button, padding: "6px 10px", opacity: 0.9 }} onClick={() => setOpen(false)} type="button">
                Close
              </button>
            </div>
          </div>

          <div
            style={{
              marginTop: 10,
              borderRadius: 12,
              border: "1px solid #222",
              background: "#0b0b0b",
              padding: 10,
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: 10,
              fontSize: 12,
            }}
          >
            <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(Boolean(e.target.checked))} />
              <span style={{ fontWeight: 800 }}>Auto-refresh</span>
              <span style={muted}>(currently: {intervalLabel})</span>
            </label>

            <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 8 }}>
              <span style={muted}>Interval</span>
              <select
                style={selectStyle}
                value={String(uiRefreshMs)}
                onChange={(e) => {
                  const n = Number(e.target.value);
                  if (Number.isFinite(n)) setUiRefreshMs(Math.max(1500, n));
                }}
                disabled={!autoRefresh}
              >
                <option value="30000">30s</option>
                <option value="60000">60s</option>
                <option value="120000">120s</option>
                <option value="300000">300s</option>
                <option value="600000">600s</option>
              </select>
            </div>

            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ fontWeight: 800 }}>When collapsed</span>
              <select
                style={selectStyle}
                value={bgMode}
                onChange={(e) => setBgMode(String(e.target.value))}
                disabled={!autoRefresh}
              >
                <option value="open_only">Do not refresh</option>
                <option value="slow">Refresh slowly</option>
                <option value="normal">Refresh normally</option>
              </select>
            </div>

            <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 8 }}>
              <span style={muted}>Collapsed interval</span>
              <select
                style={selectStyle}
                value={String(bgSlowMs)}
                onChange={(e) => {
                  const n = Number(e.target.value);
                  if (Number.isFinite(n)) setBgSlowMs(Math.max(1500, n));
                }}
                disabled={!autoRefresh || bgMode !== "slow"}
              >
                <option value="30000">30s</option>
                <option value="60000">60s</option>
                <option value="120000">120s</option>
                <option value="300000">300s</option>
                <option value="600000">600s</option>
              </select>
            </div>

            <div style={{ gridColumn: "1 / -1", fontSize: 10, ...muted }}>
              Notes: polling pauses when the browser tab is not visible; in-flight requests are deduped; cross-tab leader lock prevents duplicate polling.
            </div>
          </div>

          {err && (
            <div
              style={{
                marginTop: 10,
                borderRadius: 10,
                padding: "8px 10px",
                border: "1px solid #4a1f1f",
                background: "#160b0b",
                color: "#ffd2d2",
                fontSize: 11,
              }}
            >
              {hideTableData ? "Arb error (hidden)." : `Error: ${err}`}
            </div>
          )}

          {!err && (
            <div
              style={{
                marginTop: 10,
                display: "grid",
                gridTemplateColumns: "140px 1fr",
                rowGap: 6,
                columnGap: 10,
                fontSize: 12,
              }}
            >
              <div style={muted}>Best Ask</div>
              <div style={{ fontWeight: 800 }}>
                {maskVenue(snap?.bestAsk?.venue)} @ {mask(priceFmtArb(snap?.bestAsk?.price))}
              </div>

              <div style={muted}>Best Bid</div>
              <div style={{ fontWeight: 800 }}>
                {maskVenue(snap?.bestBid?.venue)} @ {mask(priceFmtArb(snap?.bestBid?.price))}
              </div>

              <div style={muted}>Spread</div>
              <div style={{ fontWeight: 800 }}>
                {mask(spreadAbs === null ? "—" : priceFmtArb(spreadAbs))} <span style={muted}>({pctLabel})</span>
              </div>

              <div style={muted}>Venues</div>
              <div style={muted}>{hideVenueNames ? "••••" : vList.join(", ")}</div>
            </div>
          )}

          {!err && Array.isArray(snap?.perVenue) && snap.perVenue.length > 0 && (
            <div style={{ marginTop: 12, borderTop: "1px solid #222", paddingTop: 10 }}>
              <div style={{ fontSize: 11, fontWeight: 900, marginBottom: 6 }}>Per-venue top-of-book</div>

              <div style={{ maxHeight: 220, overflow: "auto", fontSize: 11 }}>
                {snap.perVenue.map((r) => (
                  <div
                    key={r.venue}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "120px 1fr 1fr",
                      gap: 10,
                      padding: "6px 0",
                      borderBottom: "1px solid #1a1a1a",
                    }}
                  >
                    <div style={{ fontWeight: 800 }}>{maskVenue(r.venue)}</div>
                    <div style={muted}>Ask: {mask(r.ask == null ? "—" : priceFmtArb(r.ask))}</div>
                    <div style={muted}>Bid: {mask(r.bid == null ? "—" : priceFmtArb(r.bid))}</div>
                  </div>
                ))}
              </div>

              <div style={{ marginTop: 8, fontSize: 10, ...muted }}>
                Notes: This is raw top-of-book; it does not account for fees, slippage, transfer latency, or inventory constraints.
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
