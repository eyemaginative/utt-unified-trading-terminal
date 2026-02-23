// TradingViewChartWidget.jsx
import { useEffect, useMemo, useRef, useState } from "react";

/**
 * TradingViewChartWidget
 * - Draggable + resizable (now includes explicit vertical resize handles)
 * - Bounds selectable:
 *    - viewport (default): can move anywhere on screen (clamped to visible viewport)
 *    - container: constrained to appContainerRef inner bounds and below headerRef (legacy)
 * - Persists geometry + lock state to localStorage
 *
 * NEW (sync with AppHeader Widgets->Chart):
 * - No internal minimize state.
 * - If parent provides {visible, setVisible}, the widget Hide button toggles visible.chart=false
 * - When hidden, widget returns null (fully unmounted; no leftover bar)
 *
 * THEME SYNC:
 * - Reads TerminalTablesWidget theme from localStorage:
 *     utt_tables_theme_v1 (JSON string)
 *     utt_tables_theme_custom_v1 (JSON object)
 * - Uses it to:
 *     (a) pick TradingView widget theme (dark/light)
 *     (b) ensure our few hardcoded colors use CSS vars that AppHeader sets from the same palette
 */

let TV_LOADER_PROMISE = null;
const LS_GEOM_KEY = "utt_tv_chart_geom_v1";

// TerminalTablesWidget theme persistence keys (exact)
const LS_TABLES_THEME_KEY = "utt_tables_theme_v1";
const LS_TABLES_THEME_CUSTOM_KEY = "utt_tables_theme_custom_v1";

function loadTvJs() {
  if (typeof window === "undefined") return Promise.resolve(false);
  if (window.TradingView && typeof window.TradingView.widget === "function") return Promise.resolve(true);

  if (!TV_LOADER_PROMISE) {
    TV_LOADER_PROMISE = new Promise((resolve) => {
      const existing = document.querySelector('script[data-tvjs="true"]');
      if (existing) {
        existing.addEventListener("load", () => resolve(true));
        existing.addEventListener("error", () => resolve(false));
        return;
      }

      const s = document.createElement("script");
      s.src = "https://s3.tradingview.com/tv.js";
      s.async = true;
      s.defer = true;
      s.dataset.tvjs = "true";
      s.onload = () => resolve(true);
      s.onerror = () => resolve(false);
      document.head.appendChild(s);
    });
  }
  return TV_LOADER_PROMISE;
}

function safeParseJson(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function readTablesThemeKeyFromStorage() {
  try {
    const raw = localStorage.getItem(LS_TABLES_THEME_KEY);
    if (!raw) return "geminiDark";
    const v = JSON.parse(raw);
    return typeof v === "string" && v ? v : "geminiDark";
  } catch {
    return "geminiDark";
  }
}

function readTablesCustomThemeFromStorage() {
  try {
    const raw = localStorage.getItem(LS_TABLES_THEME_CUSTOM_KEY);
    if (!raw) return {};
    const v = JSON.parse(raw);
    return v && typeof v === "object" ? v : {};
  } catch {
    return {};
  }
}

function toTvTheme(themeKey) {
  const k = String(themeKey || "").toLowerCase().trim();
  // All current UTT palettes are dark; if you add a light theme later, this will auto-switch.
  if (k.includes("light")) return "light";
  return "dark";
}

function toTvExchange(venue) {
  const v = String(venue || "").toLowerCase().trim();
  if (v === "gemini") return "GEMINI";
  if (v === "coinbase") return "COINBASE";
  if (v === "kraken") return "KRAKEN";
  return "COINBASE";
}

function parseCanonSymbol(symbolCanon) {
  const s = String(symbolCanon || "").trim().toUpperCase();
  if (!s) return { base: "BTC", quote: "USD" };

  if (s.includes("-")) {
    const [base, quote] = s.split("-");
    return { base: (base || "BTC").trim(), quote: (quote || "USD").trim() };
  }
  if (s.includes("/")) {
    const [base, quote] = s.split("/");
    return { base: (base || "BTC").trim(), quote: (quote || "USD").trim() };
  }

  if (s.length >= 6) {
    const base = s.slice(0, s.length - 3);
    const quote = s.slice(-3);
    return { base: base || "BTC", quote: quote || "USD" };
  }

  return { base: "BTC", quote: "USD" };
}

function buildTvSymbol({ venue, symbolCanon }) {
  const ex = toTvExchange(venue);
  const { base, quote } = parseCanonSymbol(symbolCanon);

  const baseAdj = ex === "KRAKEN" && base === "BTC" ? "XBT" : base;
  return `${ex}:${baseAdj}${quote}`;
}

function clamp(n, lo, hi) {
  if (!Number.isFinite(n)) return lo;
  return Math.max(lo, Math.min(hi, n));
}

function numPx(v) {
  const n = parseFloat(String(v || "0").replace("px", ""));
  return Number.isFinite(n) ? n : 0;
}

/**
 * Bounds are based on boundsMode:
 * - "viewport": clamp to visible viewport edges
 * - "container": clamp to container inner content and below header (legacy)
 */
function computeBounds(appContainerRef, headerRef, padding = 10, topGap = 10, boundsMode = "viewport") {
  const mode = String(boundsMode || "viewport").toLowerCase().trim();

  const winW = typeof window !== "undefined" ? window.innerWidth : 1280;
  const winH = typeof window !== "undefined" ? window.innerHeight : 720;

  if (mode !== "container") {
    return {
      left: padding,
      right: winW - padding,
      top: padding,
      bottom: winH - padding,
    };
  }

  const containerEl = appContainerRef?.current;
  const headerEl = headerRef?.current;

  const cRect = containerEl?.getBoundingClientRect?.() || { left: 0, top: 0, right: winW, width: winW };
  const hRect = headerEl?.getBoundingClientRect?.() || { bottom: cRect.top };

  let innerLeft = cRect.left;
  let innerRight = cRect.right;
  if (containerEl && typeof window !== "undefined") {
    const cs = window.getComputedStyle(containerEl);
    const padL = numPx(cs.paddingLeft);
    const padR = numPx(cs.paddingRight);
    innerLeft = cRect.left + padL;
    innerRight = cRect.right - padR;
  }

  const left = innerLeft + padding;
  const right = innerRight - padding;

  const top = Math.max(hRect.bottom + topGap, cRect.top + padding);
  const bottom = winH - padding;

  return { left, right, top, bottom };
}

function defaultGeom() {
  return { x: 0, y: 0, w: 820, h: 430, locked: false };
}

export default function TradingViewChartWidget({
  styles,
  appContainerRef,
  headerRef,
  venue = "coinbase",
  symbolCanon = "BTC-USD",
  interval = "15",
  hideVenueNames = false,
  boundsMode = "viewport",

  // NEW: sync with AppHeader's "Widgets -> Chart"
  visible,
  setVisible,
}) {
  const isVisible = typeof visible?.chart === "boolean" ? !!visible.chart : true;

  // If parent visibility hides chart, fully unmount (no leftover bar).
  if (!isVisible) return null;

  // Theme sync with TerminalTablesWidget (exact keys).
  const [tablesThemeKey, setTablesThemeKey] = useState(() => {
    if (typeof window === "undefined") return "geminiDark";
    return readTablesThemeKeyFromStorage();
  });
  const [tablesCustomTheme, setTablesCustomTheme] = useState(() => {
    if (typeof window === "undefined") return {};
    return readTablesCustomThemeFromStorage();
  });

  useEffect(() => {
    if (typeof window === "undefined") return;

    let lastTheme = String(tablesThemeKey || "");
    let lastCustom = JSON.stringify(tablesCustomTheme || {});

    const sync = () => {
      try {
        const tk = readTablesThemeKeyFromStorage();
        const ct = readTablesCustomThemeFromStorage();
        const ctStr = JSON.stringify(ct || {});
        if (tk !== lastTheme) {
          lastTheme = tk;
          setTablesThemeKey(tk);
        }
        if (ctStr !== lastCustom) {
          lastCustom = ctStr;
          setTablesCustomTheme(ct);
        }
      } catch {
        // ignore
      }
    };

    // storage only fires cross-tab; we also poll so same-tab updates are picked up.
    const onStorage = (e) => {
      if (!e) return;
      if (e.key === LS_TABLES_THEME_KEY || e.key === LS_TABLES_THEME_CUSTOM_KEY) sync();
    };

    const t = setInterval(sync, 700);
    window.addEventListener("storage", onStorage);

    return () => {
      clearInterval(t);
      window.removeEventListener("storage", onStorage);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const tvTheme = useMemo(() => toTvTheme(tablesThemeKey), [tablesThemeKey]);

  const tvHostRef = useRef(null);
  const widgetIdRef = useRef(`tv_${Math.random().toString(16).slice(2)}`);

  const saved = useMemo(() => safeParseJson(localStorage.getItem(LS_GEOM_KEY) || ""), []);
  const initial = useMemo(() => ({ ...defaultGeom(), ...(saved && typeof saved === "object" ? saved : {}) }), [saved]);

  const [locked, setLocked] = useState(!!initial.locked);
  const [pos, setPos] = useState({ x: Number(initial.x) || 0, y: Number(initial.y) || 0 });
  const [size, setSize] = useState({ w: Number(initial.w) || 820, h: Number(initial.h) || 430 });

  const posRef = useRef(pos);
  const sizeRef = useRef(size);
  const lockedRef = useRef(locked);

  useEffect(() => {
    posRef.current = pos;
  }, [pos]);
  useEffect(() => {
    sizeRef.current = size;
  }, [size]);
  useEffect(() => {
    lockedRef.current = locked;
  }, [locked]);

  useEffect(() => {
    localStorage.setItem(
      LS_GEOM_KEY,
      JSON.stringify({
        x: pos.x,
        y: pos.y,
        w: size.w,
        h: size.h,
        locked: locked,
      })
    );
  }, [pos.x, pos.y, size.w, size.h, locked]);

  const dragRef = useRef({ active: false, sx: 0, sy: 0, ox: 0, oy: 0 });
  const resizeRef = useRef({
    active: false,
    mode: null, // "corner" | "bottom" | "top"
    sx: 0,
    sy: 0,
    ox: 0,
    oy: 0,
    ow: 0,
    oh: 0,
  });

  const tvSymbol = useMemo(() => buildTvSymbol({ venue, symbolCanon }), [venue, symbolCanon]);

  // Establish initial position.
  useEffect(() => {
    const b = computeBounds(appContainerRef, headerRef, 10, 10, boundsMode);

    const minW = 360;
    const minH = 240;

    const defaultX = b.left + 8;
    const defaultY = b.top + 8;

    setPos((p) => {
      if (Number.isFinite(p?.x) && Number.isFinite(p?.y) && (p.x !== 0 || p.y !== 0)) return p;

      const maxX = b.right - (sizeRef.current?.w ?? 820);
      const maxY = b.bottom - (sizeRef.current?.h ?? 430);

      return {
        x: clamp(defaultX, b.left, maxX),
        y: clamp(defaultY, b.top, maxY),
      };
    });

    setSize((s) => {
      const maxW = Math.max(minW, b.right - b.left);
      const maxH = Math.max(minH, b.bottom - b.top);
      return {
        w: clamp(s?.w ?? 820, minW, maxW),
        h: clamp(s?.h ?? 430, minH, maxH),
      };
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [boundsMode]);

  // Re-clamp on layout changes (resize/scroll)
  useEffect(() => {
    const reClamp = () => {
      const b = computeBounds(appContainerRef, headerRef, 10, 10, boundsMode);

      const minW = 360;
      const minH = 240;

      const currSize = sizeRef.current;
      const maxW = Math.max(minW, b.right - b.left);
      const maxH = Math.max(minH, b.bottom - b.top);

      const w = clamp(currSize.w, minW, maxW);
      const h = clamp(currSize.h, minH, maxH);

      if (w !== currSize.w || h !== currSize.h) {
        setSize({ w, h });
      }

      const currPos = posRef.current;
      const maxX = b.right - w;
      const maxY = b.bottom - h;

      const x = clamp(currPos.x, b.left, maxX);
      const y = clamp(currPos.y, b.top, maxY);

      if (x !== currPos.x || y !== currPos.y) {
        setPos({ x, y });
      }
    };

    reClamp();
    window.addEventListener("resize", reClamp);
    window.addEventListener("scroll", reClamp, true);
    return () => {
      window.removeEventListener("resize", reClamp);
      window.removeEventListener("scroll", reClamp, true);
    };
  }, [appContainerRef, headerRef, boundsMode]);

  // Pointer listeners for drag/resize
  useEffect(() => {
    const onMove = (e) => {
      if (lockedRef.current) return;

      if (dragRef.current.active) {
        const dx = e.clientX - dragRef.current.sx;
        const dy = e.clientY - dragRef.current.sy;

        const b = computeBounds(appContainerRef, headerRef, 10, 10, boundsMode);
        const nextX = dragRef.current.ox + dx;
        const nextY = dragRef.current.oy + dy;

        const maxX = b.right - sizeRef.current.w;
        const maxY = b.bottom - sizeRef.current.h;

        setPos({
          x: clamp(nextX, b.left, maxX),
          y: clamp(nextY, b.top, maxY),
        });
        return;
      }

      if (resizeRef.current.active) {
        const dx = e.clientX - resizeRef.current.sx;
        const dy = e.clientY - resizeRef.current.sy;

        const b = computeBounds(appContainerRef, headerRef, 10, 10, boundsMode);
        const minW = 360;
        const minH = 240;

        if (resizeRef.current.mode === "corner") {
          const maxW = Math.max(minW, b.right - posRef.current.x);
          const maxH = Math.max(minH, b.bottom - posRef.current.y);

          const nextW = resizeRef.current.ow + dx;
          const nextH = resizeRef.current.oh + dy;

          setSize({
            w: clamp(nextW, minW, maxW),
            h: clamp(nextH, minH, maxH),
          });
          return;
        }

        if (resizeRef.current.mode === "bottom") {
          const maxH = Math.max(minH, b.bottom - posRef.current.y);
          const nextH = resizeRef.current.oh + dy;
          setSize((s) => ({ ...s, h: clamp(nextH, minH, maxH) }));
          return;
        }

        if (resizeRef.current.mode === "top") {
          const startY = resizeRef.current.oy;
          const startH = resizeRef.current.oh;

          let nextY = startY + dy;
          nextY = clamp(nextY, b.top, b.bottom - minH);

          const maxH = Math.max(minH, b.bottom - nextY);
          const nextH = clamp(startH - (nextY - startY), minH, maxH);

          setPos((p) => ({ ...p, y: nextY }));
          setSize((s) => ({ ...s, h: nextH }));
          return;
        }
      }
    };

    const onUp = () => {
      dragRef.current.active = false;
      resizeRef.current.active = false;
      resizeRef.current.mode = null;
    };

    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    return () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
  }, [appContainerRef, headerRef, boundsMode]);

  // Build / rebuild TradingView widget
  useEffect(() => {
    let cancelled = false;

    const mount = async () => {
      const ok = await loadTvJs();
      if (cancelled) return;

      const host = tvHostRef.current;
      if (!host) return;

      host.innerHTML = "";

      if (!ok || !window.TradingView || typeof window.TradingView.widget !== "function") {
        host.innerHTML = `<div style="color:#ff6b6b;font-family:system-ui;padding:10px;">TradingView failed to load.</div>`;
        return;
      }

      const inner = document.createElement("div");
      inner.id = widgetIdRef.current;
      inner.style.width = "100%";
      inner.style.height = "100%";
      host.appendChild(inner);

      // eslint-disable-next-line no-new
      new window.TradingView.widget({
        container_id: widgetIdRef.current,
        symbol: tvSymbol,
        interval: String(interval || "15"),
        timezone: "Etc/UTC",
        theme: tvTheme,
        style: "1",
        locale: "en",
        enable_publishing: false,
        hide_side_toolbar: false,
        allow_symbol_change: true,
        save_image: false,
        withdateranges: true,
        hide_top_toolbar: false,
        details: true,
        hotlist: true,
        calendar: false,
        studies: [],
        width: "100%",
        height: "100%",
      });
    };

    mount();

    return () => {
      cancelled = true;
      if (tvHostRef.current) tvHostRef.current.innerHTML = "";
    };
  }, [tvSymbol, interval, tvTheme, tablesThemeKey, tablesCustomTheme]);

  const frameStyle = {
    ...styles.orderBookDock,
    position: "fixed",
    left: pos.x,
    top: pos.y,
    width: size.w,
    height: size.h,
    padding: 10,
    zIndex: 50,
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
  };

  const titleBarStyle = {
    ...styles.widgetTitleRow,
    cursor: locked ? "default" : "grab",
    userSelect: "none",
    marginBottom: 8,
  };

  const controlsRowStyle = {
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    flexWrap: "wrap",
    justifyContent: "flex-end",
  };

  const smallBtn = (disabled) => ({
    ...styles.button,
    padding: "6px 8px",
    fontSize: 12,
    ...(disabled ? styles.buttonDisabled : {}),
  });

  const labelVenue = hideVenueNames ? "••••" : String(venue || "").toUpperCase();
  const openUrl = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(tvSymbol)}`;

  // Use CSS vars set by AppHeader (derived from the same tables palette).
  const handleCommon = {
    position: "absolute",
    borderRadius: 10,
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    background: "var(--utt-surface-2, #151515)",
    zIndex: 52,
    opacity: locked ? 0.35 : 1,
  };

  const bottomHandle = {
    ...handleCommon,
    left: "50%",
    transform: "translateX(-50%)",
    bottom: 6,
    width: 140,
    height: 10,
    cursor: locked ? "default" : "ns-resize",
  };

  const topHandle = {
    ...handleCommon,
    left: "50%",
    transform: "translateX(-50%)",
    top: 46,
    width: 140,
    height: 10,
    cursor: locked ? "default" : "ns-resize",
  };

  const cornerHandle = {
    position: "absolute",
    right: 6,
    bottom: 6,
    width: 18,
    height: 18,
    borderRadius: 6,
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    background: "var(--utt-surface-2, #151515)",
    cursor: locked ? "default" : "nwse-resize",
    zIndex: 52,
    opacity: locked ? 0.35 : 1,
  };

  const requestHide = () => {
    if (typeof setVisible === "function") {
      setVisible((v) => ({ ...(v || {}), chart: false }));
    }
  };

  return (
    <div style={frameStyle}>
      <div
        style={titleBarStyle}
        onPointerDown={(e) => {
          if (locked) return;
          const target = e.target;
          const isButton = target?.tagName === "BUTTON" || target?.closest?.("button");
          if (isButton) return;

          dragRef.current.active = true;
          dragRef.current.sx = e.clientX;
          dragRef.current.sy = e.clientY;
          dragRef.current.ox = posRef.current.x;
          dragRef.current.oy = posRef.current.y;

          try {
            e.currentTarget.setPointerCapture(e.pointerId);
          } catch {
            // ignore
          }
        }}
      >
        <div>
          <div style={styles.widgetTitle}>Chart</div>
          <div style={styles.widgetSub}>
            {labelVenue} • {symbolCanon} • {tvSymbol}
          </div>
        </div>

        <div style={controlsRowStyle}>
          <button
            style={smallBtn(false)}
            onClick={() => window.open(openUrl, "_blank", "noopener,noreferrer")}
            title="Open this symbol on TradingView"
          >
            Open
          </button>

          <button
            style={smallBtn(false)}
            onClick={requestHide}
            title="Hide this widget (syncs with AppHeader Widgets → Chart)"
          >
            Hide
          </button>

          <button
            style={smallBtn(false)}
            onClick={() => setLocked((v) => !v)}
            title={locked ? "Unlock movement/resize" : "Lock movement/resize"}
          >
            {locked ? "Unlock" : "Lock"}
          </button>

          <button
            style={smallBtn(false)}
            onClick={() => {
              const b = computeBounds(appContainerRef, headerRef, 10, 10, boundsMode);
              const minW = 360;
              const minH = 240;

              const w = clamp(820, minW, Math.max(minW, b.right - b.left));
              const h = clamp(430, minH, Math.max(minH, b.bottom - b.top));

              const x = clamp(b.left + 8, b.left, b.right - w);
              const y = clamp(b.top + 8, b.top, b.bottom - h);

              setSize({ w, h });
              setPos({ x, y });
            }}
            title="Reset chart position/size"
          >
            Reset
          </button>
        </div>
      </div>

      {/* TOP RESIZE HANDLE (resize up: y + h) */}
      <div
        style={topHandle}
        onPointerDown={(e) => {
          if (locked) return;
          e.stopPropagation();

          resizeRef.current.active = true;
          resizeRef.current.mode = "top";
          resizeRef.current.sx = e.clientX;
          resizeRef.current.sy = e.clientY;
          resizeRef.current.oy = posRef.current.y;
          resizeRef.current.oh = sizeRef.current.h;

          try {
            e.currentTarget.setPointerCapture(e.pointerId);
          } catch {
            // ignore
          }
        }}
        title={locked ? "Locked" : "Resize up (adjusts top + height)"}
      />

      {/* Widget body */}
      <div
        ref={tvHostRef}
        style={{
          flex: 1,
          border: "1px solid var(--utt-border-1, #2a2a2a)",
          borderRadius: 10,
          overflow: "hidden",
          background: "var(--utt-surface-0, #0f0f0f)",
          minHeight: 200,
        }}
      />

      {/* BOTTOM RESIZE HANDLE (height only) */}
      <div
        style={bottomHandle}
        onPointerDown={(e) => {
          if (locked) return;
          e.stopPropagation();

          resizeRef.current.active = true;
          resizeRef.current.mode = "bottom";
          resizeRef.current.sx = e.clientX;
          resizeRef.current.sy = e.clientY;
          resizeRef.current.oh = sizeRef.current.h;

          try {
            e.currentTarget.setPointerCapture(e.pointerId);
          } catch {
            // ignore
          }
        }}
        title={locked ? "Locked" : "Resize down (height)"}
      />

      {/* CORNER RESIZE HANDLE (width + height) */}
      <div
        style={cornerHandle}
        onPointerDown={(e) => {
          if (locked) return;
          e.stopPropagation();

          resizeRef.current.active = true;
          resizeRef.current.mode = "corner";
          resizeRef.current.sx = e.clientX;
          resizeRef.current.sy = e.clientY;
          resizeRef.current.ow = sizeRef.current.w;
          resizeRef.current.oh = sizeRef.current.h;

          try {
            e.currentTarget.setPointerCapture(e.pointerId);
          } catch {
            // ignore
          }
        }}
        title={locked ? "Locked" : "Resize"}
      />
    </div>
  );
}
