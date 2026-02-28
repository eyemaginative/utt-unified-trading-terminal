// frontend/src/OrderTicketWidget.jsx

import { useEffect, useMemo, useRef, useState } from "react";
import { getOrderRules } from "./lib/api";
import { expandExponential } from "./lib/format";

// Auth (local token) — used to gate funds actions.
const UTT_AUTH_TOKEN_KEY = 'utt_auth_token_v1';
function getAuthToken() {
  try { return localStorage.getItem(UTT_AUTH_TOKEN_KEY) || ''; } catch { return ''; }
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

  const [side, setSide] = useState("buy");

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

  const [inlineMode, setInlineMode] = useState(false);

  const DEFAULT_W = 420;
  const DEFAULT_H = 330;

  const MIN_W = 320;
  const MIN_H = 250;
  const MAX_W = 900;

  const MAX_H = useMemo(() => {
    const vh = HAS_WINDOW && Number.isFinite(window.innerHeight) ? window.innerHeight : 700;
    return Math.max(250, Math.floor(vh * 0.85));
  }, []);

  const [locked, setLocked] = useState(() => lsGet(LS_OT_LOCK, "0") === "1");

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

  useEffect(() => lsSet(LS_OT_LOCK, locked ? "1" : "0"), [locked]);
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
  }, []);

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
        setRules(data || null);
      } catch (e) {
        if (cancelled || rulesReqIdRef.current !== reqId) return;
        setRules(null);
        setRulesErr(extractRulesError(e));
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
  }, [rulesLoading, rulesErr, rules, hideTableData, uiMinQty]);

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
  // Uses injected wallet (Solflare) public key + backend /api/solana_dex endpoints.
  // ─────────────────────────────────────────────────────────────
  const solanaResolveCacheRef = useRef({}); // assetKey -> { mint, decimals }

  function getInjectedSolanaPubkeyBase58() {
    try {
      const w = typeof window !== "undefined" ? window : null;
      if (!w) return null;

      // Prefer Solflare if present
      const sf = w.solflare;
      const pk1 = sf?.publicKey;
      if (pk1) {
        if (typeof pk1?.toBase58 === "function") return pk1.toBase58();
        if (typeof pk1?.toString === "function") return pk1.toString();
        if (typeof pk1 === "string") return pk1;
      }

      // Fallback: generic solana provider
      const sol = w.solana;
      const pk2 = sol?.publicKey;
      if (pk2) {
        if (typeof pk2?.toBase58 === "function") return pk2.toBase58();
        if (typeof pk2?.toString === "function") return pk2.toString();
        if (typeof pk2 === "string") return pk2;
      }

      return null;
    } catch {
      return null;
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
    const { silent = false, venueOverride = null, focusAssets = null } = opts;

    const v = String(venueOverride || effectiveVenue || "").toLowerCase().trim();
    if (!v) return { avail: balAvail, hash: computeBalHash(balAvail), focusHash: "" };

    if (!silent) {
      setBalLoading(true);
      setBalErr(null);
    }

    try {
      if (!apiBase) throw new Error("apiBase not set");
      // DEX-only: Solana venues do not have adapter-backed /api/balances/latest.
      if (isSolanaDexVenue) {
        const address = getInjectedSolanaPubkeyBase58();
        if (!address) throw new Error("Connect a Solana wallet (Solflare) to load balances.");

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

        const sol = Number(j?.sol);
        if (Number.isFinite(sol)) {
          nextAvail["SOL"] = { available: sol, total: sol, hold: null };
          nextAvail["WSOL"] = { available: sol, total: sol, hold: null };
        }

        const toks = Array.isArray(j?.tokens) ? j.tokens : [];
        const mintToUi = {};
        for (const t of toks) {
          const mint = String(t?.mint || "").trim();
          const uiAmt = Number(t?.uiAmount);
          if (!mint) continue;
          mintToUi[mint] = Number.isFinite(uiAmt) ? uiAmt : null;
        }

        // Resolve only what we need for this ticket (base/quote + common aliases).
        const want = new Set(
          [baseAsset, quoteAsset, "USD", "USDC", "USDT", "PYUSD"]
            .map((x) => String(x || "").trim().toUpperCase())
            .filter(Boolean)
        );

        for (const a of want) {
          if (a === "SOL" || a === "WSOL") continue;
          try {
            const res = await solanaResolveAsset(a);
            const mint = res?.mint;
            if (!mint) continue;
            const ui = mintToUi[mint];
            if (ui === null || ui === undefined) continue;
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

    // DEX-only: Solana venues don't have a refresh adapter path; just re-load wallet balances.
    if (isSolanaDexVenue) {
      const beforeFullHash = computeBalHash(balAvail);
      const beforeFocusHash = focusAssets ? computeFocusHash(balAvail, focusAssets) : "";

      setBalLoading(true);
      setBalErr(null);
      try {
        const { hash: afterFullHash, focusHash: afterFocusHash } = await loadAvailBalances({
          silent: true,
          venueOverride: v,
          focusAssets,
        });

        if (focusAssets) return !!afterFocusHash && afterFocusHash !== beforeFocusHash;
        return !!afterFullHash && afterFullHash !== beforeFullHash;
      } catch (e) {
        setBalErr(e?.message || "Failed loading Solana balances");
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
    loadAvailBalances();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [effectiveVenue, apiBase, baseAsset, quoteAsset]);

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

  // ─────────────────────────────────────────────────────────────
  // Pre-trade checks
  // ─────────────────────────────────────────────────────────────
  const preTrade = useMemo(() => {
    const lines = [];
    const fails = [];

    if (rulesLoading) return { status: "neutral", title: "Pre-trade checks: loading…", lines: [], block: false };

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
  }, [rulesLoading, rulesErr, rules, uiMinQty, qty, limitPrice, qtyNum, pxNum, notional, hideTableData]);

  const preTradeStyle = useMemo(() => {
    if (!preTrade) return null;
    if (preTrade.status === "ok") return { border: "1px solid #203a20", background: "#0f1a0f", color: "#cdeccd" };
    if (preTrade.status === "fail") return { border: "1px solid #4a1f1f", background: "#160b0b", color: "#ffd2d2" };
    return { border: "1px solid #2a2a2a", background: "#101010", color: "#cfcfcf" };
  }, [preTrade]);

  const canSubmitBase = useMemo(() => {
    const v = String(effectiveVenue || "").trim();
    const s = String(otSymbol || "").trim();
    return !!v && !!s && qtyNum !== null && pxNum !== null && (side === "buy" || side === "sell");
  }, [effectiveVenue, otSymbol, qtyNum, pxNum, side]);

  const canSubmit = useMemo(() => {
    if (!canSubmitBase) return false;
    if (preTrade?.block) return false;
    return true;
  }, [canSubmitBase, preTrade]);

  const buySpendQuote = useMemo(() => {
    if (side !== "buy") return null;
    if (qtyNum === null || pxNum === null) return null;
    const spend = qtyNum * pxNum;
    return Number.isFinite(spend) ? spend : null;
  }, [side, qtyNum, pxNum]);

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
    openSubmitResultModal("info", "Submitting…", "Submitting");
    void submitLimitOrder().catch((e) => {
      const msg = e?.message || String(e);
      openSubmitResultModal("error", msg, "Order Submit Failed");
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
  const safeMuted = safeStyles.muted || {};
  const safeWidgetTitleRow = safeStyles.widgetTitleRow || {};
  const safeWidgetSub = safeStyles.widgetSub || {};
  const safeCodeError = safeStyles.codeError || {};

  const shellStyleBase = inlineMode
    ? { ...safeDock, width: "100%", maxWidth: "100%", resize: "vertical", overflow: "hidden", marginTop: 0 }
    : { ...safeDock, width: box.w, height: box.h, resize: "none", overflow: "hidden" };

  const sideAccent = side === "buy" ? "#1f6f3a" : "#7a2b2b";
  const sideBg = side === "buy" ? "rgba(31, 111, 58, 0.07)" : "rgba(122, 43, 43, 0.07)";

  const shellStyle = {
    ...shellStyleBase,
    boxShadow: `0 0 0 1px ${sideAccent} inset`,
    background: shellStyleBase?.background ? shellStyleBase.background : undefined,
    backgroundImage: `linear-gradient(${sideBg}, ${sideBg})`,
  };

  const fixedWrapperStyle = inlineMode
    ? {}
    : { position: "fixed", left: box.x, top: box.y, zIndex: 61, userSelect: "none" };

  const rowStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" };
  const rowTightStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginTop: 6 };
  const sectionGap = 6;

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

  // NEW: allow re-opening the last submit result without re-submitting
  const hasLastSubmitResult = useMemo(
    () => submitResultPayload !== null && submitResultKind !== null,
    [submitResultPayload, submitResultKind]
  );

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
      { k: "Type", v: "LIMIT" },
      { k: "Qty", v: hideTableData ? "••••" : qStr },
      { k: "Limit", v: hideTableData ? "••••" : pxStr },
      { k: `Total (${totalLabel})`, v: hideTableData ? "••••" : totStr },
      ...(autoCalc ? [{ k: `Requested Total (${totalLabel})`, v: hideTableData ? "••••" : reqTotStr }] : []),
      { k: "TIF", v: String(tif || "gtc").toUpperCase() },
      { k: "Post-only", v: postOnly ? "YES" : "NO" },
      ...(clientOid ? [{ k: "Client OID", v: hideTableData ? "••••" : String(clientOid) }] : []),
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
  ]);

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

          <label style={safePill} title="Lock position + size">
            <input type="checkbox" checked={locked} onChange={(e) => {
              const next = !!e.target.checked;
              setLocked(next);
              if (next) {
                // Capture anchor offsets so viewport resize (DevTools) doesn't shove the widget.
                setBox((prev) => {
                  const vw = window.innerWidth;
                  const vh = window.innerHeight;
                  const b = getGutterBounds();
                  const w = prev.w || DEFAULT_W;
                  const h = prev.h || DEFAULT_H;
                  const x = Number.isFinite(prev.x) ? prev.x : b.minX;
                  const y = Number.isFinite(prev.y) ? prev.y : b.minY;
                  const left = x - b.minX;
                  const top = y - b.minY;
                  const right = vw - (x + w);
                  const bottom = vh - (y + h);
                  const anchorX = left <= right ? "left" : "right";
                  const anchorY = top <= bottom ? "top" : "bottom";
                  return { ...prev, left, top, right, bottom, anchorX, anchorY };
                });
              }
            }} />
            <span>Lock</span>
          </label>
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
                if (isSolanaDexVenue && autoCalc) {
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
              onClick={() => refreshAvailBalances({ maxPolls: 2, pollBackoffMs: [800, 1200] })}
              disabled={balLoading}
              title="Refresh balances from venue"
            >
              {balLoading ? "…" : "Refresh"}
            </button>
          </div>

          {balErr && (
            <div style={{ ...safeMuted, fontSize: 11, color: "#ff6b6b", lineHeight: 1.1 }}>
              Bal: {hideTableData ? "Hidden" : balErr}
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
          <div style={safePill}>
            <span>TIF</span>
            <select style={safeSelect} value={tif} onChange={(e) => setTif(e.target.value)}>
              <option value="gtc">GTC</option>
              <option value="ioc">IOC</option>
              <option value="fok">FOK</option>
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
        </div>

        <div style={{ marginTop: 6, ...safeMuted, fontSize: 12, lineHeight: 1.15 }}>
          Type: <b>Limit</b> • Est. Total ({totalLabel}):{" "}
          <b>{notional === null ? "—" : fmtNum ? fmtNum(notional) : String(notional)}</b>
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
                ? "Fill symbol, qty, and limit price"
                : preTrade?.block
                  ? "Blocked by pre-trade checks"
                  : "Review and confirm order"
            }
          >
            {submitting ? "Submitting…" : side === "buy" ? "Place Buy Limit" : "Place Sell Limit"}
          </button>

          <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
            Endpoint: <code>/api/trade/order</code>
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

        {!inlineMode && (
          <div
            onMouseDown={onResizeMouseDown}
            title={locked ? "Locked" : "Resize from top-left"}
            style={{
              position: "absolute",
              left: 6,
              top: 6,
              width: 18,
              height: 18,
              borderRadius: 6,
              border: "1px solid #2a2a2a",
              background: "#151515",
              cursor: locked ? "default" : "nwse-resize",
              zIndex: 5,
              opacity: locked ? 0.4 : 1,
            }}
          />
        )}

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
                <div style={{ fontSize: 14, fontWeight: 900 }}>Confirm {side === "buy" ? "BUY" : "SELL"} Limit Order</div>
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
                  {submitting ? "Submitting…" : "Confirm & Submit"}
                </button>
              </div>

              <div style={{ marginTop: 10, fontSize: 11, color: "#a9a9a9", lineHeight: 1.25 }}>
                Confirm submits immediately via <code>/api/trade/order</code>. Cancel returns you to the form without submitting.
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
