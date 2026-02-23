import { useEffect, useMemo, useRef, useState } from "react";

const LS_OB_BOX = "utt_ob_box_v2";
const LS_OB_LOCK = "utt_ob_lock_v2";

// persist auto refresh + interval
const LS_OB_AUTO = "utt_ob_auto_v1";
const LS_OB_SEC = "utt_ob_sec_v1";

function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
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
      return `Unknown token symbol ${sym} on ${venue} — use mint:<ADDRESS> in the pair (e.g. mint:<UTTT_MINT>-SOL) or set UTT_SOLANA_MINTS_JSON override`;
    }
    if (err === "symbol_ambiguous") {
      const sym = String(detailObj.symbol || "").trim() || "(unknown)";
      return `Ambiguous token symbol ${sym} on ${venue} — use mint:<ADDRESS> in the pair or set UTT_SOLANA_MINTS_JSON override`;
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
  obSymbol,
  setObSymbol,
  obDepth,
  setObDepth,
  appContainerRef,
  hideVenueNames = false,
  onPickPrice,
  onPickQty,
}) {
  const [obBids, setObBids] = useState([]);
  const [obAsks, setObAsks] = useState([]);
  const [obLoading, setObLoading] = useState(false);
  const [obError, setObError] = useState(null);

  // NEW: local draft so typing doesn't spam the backend
  const [symbolDraft, setSymbolDraft] = useState(String(obSymbol || ""));

  // Order rules (price display + click-to-ticket normalization) for ANY venue
  const [priceDecimals, setPriceDecimals] = useState(null);
  const [sizeDecimals, setSizeDecimals] = useState(null);
  const [priceIncrement, setPriceIncrement] = useState(null);

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

  const inFlightRef = useRef(false);
  const abortRef = useRef(null);

  // NEW: error gating to stop hammering known-bad pairs
  const pairNotFoundRef = useRef(false);

  // NEW: 429 backoff/cooldown
  const cooldownUntilRef = useRef(0);
  const cooldownPowRef = useRef(0);

  const [inlineMode, setInlineMode] = useState(false);

  const DEFAULT_W = 460;
  const DEFAULT_H = 520;

  const MIN_W = 320;
  const MIN_H = 260;
  const MAX_W = 900;
  const MAX_H = Math.max(260, Math.floor(window?.innerHeight ? window.innerHeight * 0.9 : 800));

  const [locked, setLocked] = useState(() => {
    const v = localStorage.getItem(LS_OB_LOCK);
    return v === "1";
  });

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
    localStorage.setItem(LS_OB_LOCK, locked ? "1" : "0");
  }, [locked]);

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

  // Keep draft in sync when parent sets obSymbol (e.g. from clicks elsewhere)
  useEffect(() => {
    setSymbolDraft(String(obSymbol || ""));
    // Reset gating when symbol changes externally
    pairNotFoundRef.current = false;
    cooldownUntilRef.current = 0;
    cooldownPowRef.current = 0;
  }, [obSymbol]);

  // Reset gating when venue changes
  useEffect(() => {
    pairNotFoundRef.current = false;
    cooldownUntilRef.current = 0;
    cooldownPowRef.current = 0;
  }, [effectiveVenue]);


  const lockedRef = useRef(locked);
  const boxRef = useRef(box);
  useEffect(() => { lockedRef.current = locked; }, [locked]);
  useEffect(() => { boxRef.current = box; }, [box]);

  const dragStateRef = useRef(null);
  const resizeStateRef = useRef(null);

  const asksWrapRef = useRef(null);
  const bidsWrapRef = useRef(null);

  const venueLabel = hideVenueNames ? "••••" : String(effectiveVenue || "");

  function getGutterBounds() {
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    const el = appContainerRef?.current;
    const rect = el?.getBoundingClientRect?.();

    const margin = 0;

    if (!rect) {
      return {
        minX: margin,
        maxX: vw - margin,
        minY: margin,
        maxY: vh - margin,
        gutterLeft: margin,
        gutterWidth: vw - margin * 2,
      };
    }

    const containerRight = rect.right;
    const gutterLeft = Math.ceil(containerRight + margin);
    const gutterWidth = vw - gutterLeft - margin;

    return {
      minX: gutterLeft,
      maxX: vw - margin,
      minY: margin,
      maxY: vh - margin,
      gutterLeft,
      gutterWidth,
      containerRight,
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
    const recompute = () => {
      const b = getGutterBounds();
      const canGutter = Number.isFinite(b.gutterWidth) ? b.gutterWidth >= MIN_W + 4 : false;
      setInlineMode(!canGutter);

      if (canGutter) {
        setBox((prev) => {
          // When locked, keep the widget visually anchored to the viewport edges
          // (DevTools open/close changes window.innerWidth/Height, which would otherwise shove it).
          if (lockedRef.current) {
            const vw = window.innerWidth;
            const vh = window.innerHeight;
            const w = clamp(prev.w || DEFAULT_W, MIN_W, MAX_W);
            const h = clamp(prev.h || DEFAULT_H, MIN_H, MAX_H);

            const prevX = prev.x ?? b.minX;
            const prevY = prev.y ?? 0;

            const right =
              Number.isFinite(prev.right) ? prev.right : vw - (prevX + w);
            const bottom =
              Number.isFinite(prev.bottom) ? prev.bottom : vh - (prevY + h);

            const x = clamp(vw - w - right, b.minX, b.maxX - w);
            const y = clamp(vh - h - bottom, b.minY, b.maxY - h);

            // Preserve w/h; only update x/y and anchors.
            return { ...prev, x, y, w, h, right, bottom };
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
  }, []);

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
        if (Number.isFinite(px) && Number.isFinite(sz)) out.push({ price: px, size: sz });
      }
    }
    return out;
  }

  const asksSorted = useMemo(() => [...(obAsks || [])].sort((a, b) => b.price - a.price), [obAsks]);
  const bidsSorted = useMemo(() => [...(obBids || [])].sort((a, b) => Number(b.price) - Number(a.price)), [obBids]);

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

  function fmtPriceCell(p) {
    // If we have rules-derived decimals (common for BTC-quoted pairs), use them.
    const np = normPriceByRules(p);
    if (np === null) return "—";

    if (Number.isFinite(Number(priceDecimals))) {
      const d = clamp(Number(priceDecimals), 0, 18);
      return Number(np).toFixed(d);
    }

    // Fall back to app-wide formatter (may be 8 decimals).
    return fmtNum(np);
  }


  function fmtSizeCell(sz) {
    const v = Number(sz);
    if (!Number.isFinite(v)) return "—";
    if (Number.isFinite(Number(sizeDecimals))) {
      const d = clamp(Number(sizeDecimals), 0, 18);
      return v.toFixed(d);
    }
    return fmtNum(v);
  }

  async function fetchOrderBook(opts = {}) {
    const v = String(opts.venueOverride ?? effectiveVenue ?? "").toLowerCase().trim();
    const sym = String(opts.symbolOverride ?? obSymbol ?? "").trim();
    const depth = Math.max(1, Math.min(200, Number(opts.depthOverride ?? obDepth) || 25));

    if (!v || !sym) return;

    // gating: known-bad pair
    if (!opts.force && pairNotFoundRef.current) return;

    // gating: cooldown (e.g., 429)
    const now = Date.now();
    if (!opts.force && now < (cooldownUntilRef.current || 0)) return;

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

      // IMPORTANT:
      // - _ts busts browser/proxy caches
      // - force=true (when opts.force) requests a live fetch server-side
            const forceQ = opts.force ? "&force=true" : "";

      const isSolJup = v === "solana_jupiter";
      const url = isSolJup
        ? `${apiBase}/api/solana_dex/jupiter/orderbook?symbol=${encodeURIComponent(sym)}&depth=${encodeURIComponent(
            String(depth)
          )}${forceQ}&_ts=${Date.now()}`
        : `${apiBase}/api/market/orderbook?venue=${encodeURIComponent(v)}&symbol=${encodeURIComponent(
            sym
          )}&depth=${encodeURIComponent(String(depth))}${forceQ}&_ts=${Date.now()}`;

      const r = await fetch(url, { signal: ac.signal });

      // handle 429 explicitly (cooldown)
      if (r.status === 429) {
        cooldownPowRef.current = clamp((cooldownPowRef.current || 0) + 1, 0, 6);
        const backoffMs = Math.min(300000, 15000 * Math.pow(2, cooldownPowRef.current)); // 15s, 30s, 60s... up to 5m
        cooldownUntilRef.current = Date.now() + backoffMs;

        const txt = await r.text().catch(() => "");
        throw new Error(txt || `HTTP 429 Too Many Requests`);
      }

      if (!r.ok) {
        const txt = await r.text().catch(() => "");
        throw new Error(txt || `HTTP ${r.status}`);
      }

      // success: clear cooldown
      cooldownPowRef.current = 0;
      cooldownUntilRef.current = 0;

      const data = await r.json();

      // DEX-only formatting hints (opt-in by venue)
      if (isSolJup) {
        const pd = Number(data?.priceDecimals);
        if (Number.isFinite(pd)) setPriceDecimals(pd);
        const sd = Number(data?.sizeDecimals);
        if (Number.isFinite(sd)) setSizeDecimals(sd);
      }

      setObAsks(normalizeSide(data?.asks || []));
      setObBids(normalizeSide(data?.bids || []));

      snapToCenterAnchors();
    } catch (e) {
      // ignore abort errors
      const msg = String(e?.message || "");
      if (msg.toLowerCase().includes("aborted")) {
        return;
      }

      setObAsks([]);
      setObBids([]);

      const raw = e?.message || "Failed to load order book";
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
  }, [obAutoRefresh, obAutoSeconds, effectiveVenue, obSymbol, obDepth, apiBase]);

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

  function handlePickPrice(px) {
    // Normalize clicked price when rules are known; otherwise pass through.
    const outPx = normPriceByRules(px);
    if (typeof onPickPrice === "function" && outPx !== null && Number.isFinite(Number(outPx))) {
      onPickPrice(Number(outPx));
    }
  }

  function handlePickQty(q) {
    if (typeof onPickQty === "function" && Number.isFinite(Number(q))) onPickQty(Number(q));
  }

  const depthN = Math.max(1, Math.min(200, Number(obDepth) || 25));
  const asksView = asksSorted.slice(0, depthN);
  const bidsView = bidsSorted.slice(0, depthN);

  const BOTTOM_SPACER = 12;
  const SHELL_PAD = 8;
  const SHELL_PAD_BOTTOM = 16;

  const obShellStyle = inlineMode
    ? {
        ...styles.orderBookDock,
        width: "100%",
        maxWidth: "100%",
        resize: "vertical",
        overflow: "hidden",
        marginTop: 0,
        padding: SHELL_PAD,
        paddingBottom: SHELL_PAD_BOTTOM,
      }
    : {
        ...styles.orderBookDock,
        width: box.w,
        height: box.h,
        resize: "none",
        overflow: "hidden",
        padding: SHELL_PAD,
        paddingBottom: SHELL_PAD_BOTTOM,
      };

  const fixedWrapperStyle = inlineMode
    ? { marginTop: 0 }
    : { position: "fixed", left: box.x, top: box.y, zIndex: 60, userSelect: "none" };

  const approxChrome = 145;
  const remaining = Math.max(200, (inlineMode ? DEFAULT_H : box.h) - approxChrome);
  const half = Math.max(110, Math.floor(remaining / 2));

  const ASK = { border: "rgba(53, 208, 127, 0.55)", bg: "rgba(53, 208, 127, 0.06)" };
  const BID = { border: "rgba(224, 79, 79, 0.55)", bg: "rgba(224, 79, 79, 0.06)" };
  const OB_TEXT = "#ffffff";

  const BIDS_INNER_PAD_BOTTOM = 10;

  const asksWrapStyle = {
    ...styles.obTableWrap,
    maxHeight: half,
    marginTop: 3,
    border: `1px solid ${ASK.border}`,
    background: ASK.bg,
    borderRadius: 10,
    boxSizing: "border-box",
  };

  const bidsWrapStyle = {
    ...styles.obTableWrap,
    maxHeight: half,
    marginTop: 3,
    border: `1px solid ${BID.border}`,
    background: BID.bg,
    borderRadius: 10,
    paddingBottom: BIDS_INNER_PAD_BOTTOM,
    boxSizing: "border-box",
  };

  const GAP = 6;
  const rowStyle = { display: "flex", gap: GAP, flexWrap: "wrap", alignItems: "center" };

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

  const btnCompact = (extra = {}) => ({
    ...styles.button,
    padding: "6px 8px",
    borderRadius: 10,
    ...extra,
  });

  const thCompact = { ...styles.obTh, padding: "6px 8px", fontSize: 12, color: OB_TEXT };
  const tdCompact = { ...styles.obTd, padding: "6px 8px", fontSize: 12, color: OB_TEXT };

  const asksTh = { ...thCompact };
  const bidsTh = { ...thCompact };

  const asksTd = (extra = {}) => ({ ...tdCompact, ...extra });
  const bidsTd = (extra = {}) => ({ ...tdCompact, ...extra });

  const asksTitleStyle = { ...styles.obSectionTitle, marginTop: 8, fontSize: 11, color: OB_TEXT };
  const bidsTitleStyle = { ...styles.obSectionTitle, marginTop: 8, fontSize: 11, color: OB_TEXT };

  function commitSymbolAndRefresh(force = false) {
    const next = String(symbolDraft || "").trim();
    if (!next) return;

    // reset gating; user is explicitly trying
    pairNotFoundRef.current = false;
    cooldownUntilRef.current = 0;
    cooldownPowRef.current = 0;

    if (next !== String(obSymbol || "")) {
      setObSymbol(next);
      // fetch immediately using override (so we don't wait for parent state to propagate)
      void fetchOrderBook({ symbolOverride: next, force: true });
      void fetchOrderRules(next);
    } else {
      void fetchOrderBook({ force: !!force });
      void fetchOrderRules(next);
    }
  }

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
          <span style={{ ...styles.widgetSub, fontSize: 11 }}>
            Venue used: <b>{venueLabel || "—"}</b>
          </span>
        </div>

        <div style={rowStyle}>
          <div style={pillCompact()}>
            <span>Symbol</span>
            <input
              style={inputCompact({ width: 140 })}
              value={symbolDraft}
              placeholder="e.g. BTC-USD"
              onChange={(e) => setSymbolDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") commitSymbolAndRefresh(true);
              }}
            />
          </div>

          <div style={pillCompact()}>
            <span>Depth</span>
            <input
              style={inputCompact({ width: 70 })}
              type="number"
              min="1"
              max="200"
              value={obDepth}
              onChange={(e) => setObDepth(e.target.value)}
            />
          </div>

          <button
            style={{ ...btnCompact(), ...(obLoading ? styles.buttonDisabled : {}) }}
            disabled={obLoading}
            onClick={() => commitSymbolAndRefresh(true)}
          >
            {obLoading ? "Loading…" : "Refresh"}
          </button>
        </div>

        <div style={{ ...rowStyle, marginTop: 6 }}>
          <label style={pillCompact()} title="Lock position + size">
            <input type="checkbox" checked={locked} onChange={(e) => {
              const next = !!e.target.checked;
              setLocked(next);
              if (next) {
                // Capture anchor offsets so viewport resize (DevTools) doesn't shove the widget.
                setBox((prev) => {
                  const vw = window.innerWidth;
                  const vh = window.innerHeight;
                  const w = prev.w || DEFAULT_W;
                  const h = prev.h || DEFAULT_H;
                  const x = prev.x ?? 0;
                  const y = prev.y ?? 0;
                  const right = vw - (x + w);
                  const bottom = vh - (y + h);
                  return { ...prev, right, bottom };
                });
              }
            }} />
            <span>Lock</span>
          </label>

          <label style={pillCompact()} title="Auto refresh order book on a timer (pauses when tab is hidden).">
            <input type="checkbox" checked={obAutoRefresh} onChange={(e) => setObAutoRefresh(e.target.checked)} />
            <span>Auto refresh</span>
          </label>

          <div style={pillCompact()} title="Refresh interval (seconds)">
            <span>Every</span>
            <input
              style={inputCompact({ width: 70 })}
              type="number"
              min="30"
              max="300"
              value={obAutoSeconds}
              disabled={!obAutoRefresh}
              onChange={(e) => setObAutoSeconds(e.target.value)}
            />
            <span style={styles.muted}>sec</span>
          </div>
        </div>

        {obError && <div style={{ ...styles.codeError, marginTop: 6, padding: 8 }}>{obError}</div>}

        <div style={asksTitleStyle}>Asks</div>
        <div ref={asksWrapRef} style={asksWrapStyle}>
          <table style={styles.obInnerTable}>
            <thead>
              <tr>
                <th style={asksTh}>Price</th>
                <th style={asksTh}>Size</th>
              </tr>
            </thead>
            <tbody>
              {asksView.map((x, idx) => (
                <tr key={`a-${idx}`}>
                  <td
                    style={{ ...asksTd(), cursor: "pointer", userSelect: "none" }}
                    title="Click to set ticket Limit price"
                    onClick={() => handlePickPrice(x.price)}
                  >
                    {fmtPriceCell(x.price)}
                  </td>
                  <td
                    style={{ ...asksTd(), cursor: "pointer", userSelect: "none" }}
                    title="Click to set ticket Qty"
                    onClick={() => handlePickQty(x.size)}
                  >
                    {fmtSizeCell(x.size)}
                  </td>
                </tr>
              ))}
              {asksView.length === 0 && (
                <tr>
                  <td style={tdCompact} colSpan={2}>
                    <span style={styles.muted}>No asks loaded.</span>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={bidsTitleStyle}>Bids</div>
        <div ref={bidsWrapRef} style={bidsWrapStyle}>
          <table style={styles.obInnerTable}>
            <thead>
              <tr>
                <th style={bidsTh}>Price</th>
                <th style={bidsTh}>Size</th>
              </tr>
            </thead>
            <tbody>
              {bidsView.map((x, idx) => (
                <tr key={`b-${idx}`}>
                  <td
                    style={{ ...bidsTd(), cursor: "pointer", userSelect: "none" }}
                    title="Click to set ticket Limit price"
                    onClick={() => handlePickPrice(x.price)}
                  >
                    {fmtPriceCell(x.price)}
                  </td>
                  <td
                    style={{ ...bidsTd(), cursor: "pointer", userSelect: "none" }}
                    title="Click to set ticket Qty"
                    onClick={() => handlePickQty(x.size)}
                  >
                    {fmtSizeCell(x.size)}
                  </td>
                </tr>
              ))}
              {bidsView.length === 0 && (
                <tr>
                  <td style={tdCompact} colSpan={2}>
                    <span style={styles.muted}>No bids loaded.</span>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={{ height: 12 }} />

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
