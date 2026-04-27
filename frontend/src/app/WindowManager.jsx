// frontend/src/WindowManager.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";

const GRID_SIZE = 12;
const VIEWPORT_MARGIN = 16;
const HEADER_HEIGHT = 40;
const MIN_WIDTH = 420;
const MIN_HEIGHT = 320;
const MAX_WIDTH = 2200;
const MAX_HEIGHT = 2000;

/**
 * WindowManager (INLINE)
 *
 * Renders “tool windows” inside the main SPA as floating, draggable panes.
 * This avoids browser popup blockers and keeps tools in the same React tree.
 *
 * Props:
 * - windows: [{ id, title, open, width, height, left, top, payload }]
 * - renderWindow: (win) => ReactElement
 * - onClosed: (id) => void
 */
export default function WindowManager({ windows = [], renderWindow, onClosed }) {
  const onClosedRef = useRef(onClosed);
  const renderWindowRef = useRef(renderWindow);
  const zTopRef = useRef(1000);
  const zByIdRef = useRef({}); // id -> z
  const shellRefs = useRef({}); // id -> element

  // Persisted geometry per-tool
  const [geomById, setGeomById] = useState(() => ({})); // id -> { left, top, width, height }
  const dragRef = useRef(null); // { id, startX, startY, startLeft, startTop }

  useEffect(() => {
    onClosedRef.current = onClosed;
  }, [onClosed]);

  useEffect(() => {
    renderWindowRef.current = renderWindow;
  }, [renderWindow]);

  const openWindows = useMemo(() => {
    const arr = Array.isArray(windows) ? windows : [];
    const dedup = new Map();
    for (const w of arr) {
      if (!w || !w.open) continue;
      const id = String(w?.id || "").trim();
      if (!id) continue;
      dedup.set(id, w);
    }
    return Array.from(dedup.values());
  }, [windows]);

  // Ensure any newly opened windows get an initial geometry and z-index.
  useEffect(() => {
    if (typeof window === "undefined") return;

    setGeomById((prev) => {
      let next = prev;
      for (const w of openWindows) {
        const id = String(w?.id || "").trim();
        if (!id) continue;

        // z-index
        if (!zByIdRef.current[id]) {
          zTopRef.current += 1;
          zByIdRef.current[id] = zTopRef.current;
        }

        if (prev[id]) continue;

        const stored = readGeomLS(id);
        const width = clampNum(stored?.width ?? w?.width ?? 980, MIN_WIDTH, MAX_WIDTH);
        const height = clampNum(stored?.height ?? w?.height ?? 640, MIN_HEIGHT, MAX_HEIGHT);

        const defLeft = Number.isFinite(Number(w?.left)) ? Number(w.left) : 60;
        const defTop = Number.isFinite(Number(w?.top)) ? Number(w.top) : 110;

        const snapped = normalizeRect({
          left: stored?.left ?? defLeft,
          top: stored?.top ?? defTop,
          width,
          height,
        });

        next = next === prev ? { ...prev } : next;
        next[id] = snapped;
      }
      return next;
    });
  }, [openWindows]);

  // Keep windows clamped to viewport after browser resizes.
  useEffect(() => {
    if (typeof window === "undefined") return undefined;

    function onViewportResize() {
      setGeomById((prev) => {
        let changed = false;
        const next = { ...prev };
        for (const [id, g] of Object.entries(prev)) {
          const normalized = normalizeRect(g);
          if (
            normalized.left !== g.left ||
            normalized.top !== g.top ||
            normalized.width !== g.width ||
            normalized.height !== g.height
          ) {
            next[id] = normalized;
            writeGeomLS(id, normalized);
            changed = true;
          }
        }
        return changed ? next : prev;
      });
    }

    window.addEventListener("resize", onViewportResize);
    return () => window.removeEventListener("resize", onViewportResize);
  }, []);

  // Observe actual DOM size so browser CSS-resize persists reliably.
  useEffect(() => {
    if (typeof window === "undefined" || typeof ResizeObserver === "undefined") return undefined;

    const observers = [];
    for (const w of openWindows) {
      const id = String(w?.id || "").trim();
      const node = shellRefs.current[id];
      if (!id || !node) continue;

      const ro = new ResizeObserver((entries) => {
        const entry = entries?.[0];
        if (!entry) return;
        const rect = entry.target.getBoundingClientRect();
        setGeomById((prev) => {
          const g = prev[id];
          if (!g) return prev;
          const normalized = normalizeRect({
            left: g.left,
            top: g.top,
            width: rect.width,
            height: rect.height,
          });
          if (
            normalized.width === g.width &&
            normalized.height === g.height &&
            normalized.left === g.left &&
            normalized.top === g.top
          ) {
            return prev;
          }
          writeGeomLS(id, normalized);
          return { ...prev, [id]: normalized };
        });
      });

      ro.observe(node);
      observers.push(ro);
    }

    return () => {
      for (const ro of observers) ro.disconnect();
    };
  }, [openWindows]);

  // Global drag listeners
  useEffect(() => {
    if (typeof window === "undefined") return;

    function onMove(e) {
      const d = dragRef.current;
      if (!d) return;

      const x = "touches" in e && e.touches?.[0] ? e.touches[0].clientX : e.clientX;
      const y = "touches" in e && e.touches?.[0] ? e.touches[0].clientY : e.clientY;
      if (!Number.isFinite(Number(x)) || !Number.isFinite(Number(y))) return;

      if ("touches" in e) e.preventDefault?.();

      const dx = Number(x) - d.startX;
      const dy = Number(y) - d.startY;

      setGeomById((prev) => {
        const g = prev[d.id];
        if (!g) return prev;
        const nextRect = normalizeRect({
          left: d.startLeft + dx,
          top: d.startTop + dy,
          width: g.width,
          height: g.height,
        });
        return { ...prev, [d.id]: nextRect };
      });
    }

    function onUp() {
      const d = dragRef.current;
      if (!d) return;
      dragRef.current = null;

      // Persist after drag end / pick up any DOM-resized size.
      persistSizeFromDom(d.id);
    }

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    window.addEventListener("touchmove", onMove, { passive: false });
    window.addEventListener("touchend", onUp);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      window.removeEventListener("touchmove", onMove);
      window.removeEventListener("touchend", onUp);
    };
  }, []);

  // Close windows that are no longer open in parent state
  useEffect(() => {
    const openIds = new Set(openWindows.map((w) => String(w?.id || "").trim()).filter(Boolean));
    setGeomById((prev) => {
      let next = prev;
      for (const id of Object.keys(prev)) {
        if (openIds.has(id)) continue;
        next = next === prev ? { ...prev } : next;
        delete next[id];
        delete shellRefs.current[id];
      }
      return next;
    });
  }, [openWindows]);

  if (!openWindows.length) return null;

  return (
    <>
      {openWindows.map((w) => {
        const id = String(w?.id || "").trim();
        if (!id) return null;
        const g = geomById[id];
        if (!g) return null;

        const z = zByIdRef.current[id] || 1000;
        const title = String(w?.title || id);

        const el = renderWindowRef.current ? renderWindowRef.current(w) : null;

        return (
          <div
            key={id}
            ref={(node) => {
              if (node) shellRefs.current[id] = node;
              else delete shellRefs.current[id];
            }}
            data-utt-tool-shell="true"
            style={{
              position: "fixed",
              left: g.left,
              top: g.top,
              width: g.width,
              height: g.height,
              minWidth: MIN_WIDTH,
              minHeight: MIN_HEIGHT,
              maxWidth: `calc(100vw - ${VIEWPORT_MARGIN * 2}px)`,
              maxHeight: `calc(100vh - ${VIEWPORT_MARGIN * 2}px)`,
              zIndex: z,
              display: "flex",
              flexDirection: "column",
              boxSizing: "border-box",
              borderRadius: 14,
              overflow: "hidden",
              resize: "both",
              background:
                "linear-gradient(180deg, rgba(13,18,26,0.985) 0%, rgba(12,16,23,0.975) 100%)",
              border: "1px solid var(--utt-border-1, rgba(110,255,214,0.18))",
              boxShadow:
                "0 18px 42px rgba(0,0,0,0.52), 0 0 0 1px rgba(130,255,225,0.04) inset, 0 0 24px rgba(54,208,170,0.10)",
              backdropFilter: "blur(10px) saturate(1.08)",
              color: "var(--utt-text, #e9eef7)",
            }}
            onMouseDown={() => bringToFront(id)}
            onTouchStart={() => bringToFront(id)}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 12,
                minHeight: HEADER_HEIGHT,
                padding: "8px 12px",
                background:
                  "linear-gradient(180deg, rgba(22,29,39,0.98) 0%, rgba(17,22,31,0.98) 100%)",
                borderBottom: "1px solid rgba(120,255,223,0.12)",
                cursor: "move",
                userSelect: "none",
                boxSizing: "border-box",
              }}
              onMouseDown={(e) => startDrag(e, id)}
              onTouchStart={(e) => startDrag(e, id)}
              title="Drag to move"
            >
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 10,
                  minWidth: 0,
                  flex: "1 1 auto",
                }}
              >
                <div
                  aria-hidden="true"
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: 999,
                    background: "linear-gradient(180deg, rgba(107,255,221,0.95), rgba(48,180,146,0.95))",
                    boxShadow: "0 0 10px rgba(76,224,186,0.55)",
                    flex: "0 0 auto",
                  }}
                />
                <div
                  style={{
                    fontWeight: 700,
                    fontSize: 13,
                    letterSpacing: 0.45,
                    textTransform: "uppercase",
                    opacity: 0.96,
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                  }}
                >
                  {title}
                </div>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8, flex: "0 0 auto" }}>
                <button
                  type="button"
                  onClick={() => onClosedRef.current?.(id)}
                  style={miniBtnStyle}
                  title="Close"
                >
                  Close
                </button>
              </div>
            </div>

            <div
              style={{
                flex: "1 1 auto",
                minHeight: 0,
                minWidth: 0,
                overflow: "auto",
                padding: 12,
                boxSizing: "border-box",
                background:
                  "linear-gradient(180deg, rgba(13,17,24,0.74) 0%, rgba(10,13,20,0.82) 100%)",
              }}
              onMouseUp={() => persistSizeFromDom(id)}
              onTouchEnd={() => persistSizeFromDom(id)}
            >
              {el}
            </div>

            <div
              style={{
                position: "absolute",
                right: 2,
                bottom: 2,
                width: 18,
                height: 18,
                pointerEvents: "none",
                opacity: 0.72,
                background:
                  "linear-gradient(135deg, transparent 0 46%, rgba(120,255,223,0.48) 46% 54%, transparent 54% 100%)",
              }}
            />
          </div>
        );
      })}
    </>
  );

  function bringToFront(id) {
    try {
      zTopRef.current += 1;
      zByIdRef.current[id] = zTopRef.current;
      // force a re-render by touching state (no-op update)
      setGeomById((prev) => ({ ...prev }));
    } catch {
      // ignore
    }
  }

  function startDrag(e, id) {
    try {
      e.preventDefault?.();
      e.stopPropagation?.();

      const g = geomById[id];
      if (!g) return;

      const x = "touches" in e && e.touches?.[0] ? e.touches[0].clientX : e.clientX;
      const y = "touches" in e && e.touches?.[0] ? e.touches[0].clientY : e.clientY;
      if (!Number.isFinite(Number(x)) || !Number.isFinite(Number(y))) return;

      bringToFront(id);
      dragRef.current = {
        id,
        startX: Number(x),
        startY: Number(y),
        startLeft: Number(g.left) || 0,
        startTop: Number(g.top) || 0,
      };
    } catch {
      // ignore
    }
  }

  function persistSizeFromDom(id) {
    try {
      const node = shellRefs.current[id];
      const g = geomById[id];
      if (!g) return;
      if (!node) {
        writeGeomLS(id, g);
        return;
      }
      const rect = node.getBoundingClientRect();
      const normalized = normalizeRect({
        left: g.left,
        top: g.top,
        width: rect.width,
        height: rect.height,
      });
      setGeomById((prev) => ({ ...prev, [id]: normalized }));
      writeGeomLS(id, normalized);
    } catch {
      // ignore
    }
  }
}

const miniBtnStyle = {
  fontSize: 12,
  lineHeight: 1.1,
  padding: "5px 10px",
  borderRadius: 10,
  border: "1px solid rgba(112,255,220,0.16)",
  background: "linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.03))",
  color: "var(--utt-text, #e9eef7)",
  cursor: "pointer",
  boxShadow: "0 0 0 1px rgba(255,255,255,0.02) inset",
};

function clampNum(v, lo, hi) {
  const x = Number(v);
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}

function snapNum(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return 0;
  return Math.round(x / GRID_SIZE) * GRID_SIZE;
}

function getViewportBounds() {
  if (typeof window === "undefined") {
    return {
      maxLeft: 0,
      maxTop: 0,
      maxWidth: MAX_WIDTH,
      maxHeight: MAX_HEIGHT,
    };
  }
  return {
    maxLeft: Math.max(VIEWPORT_MARGIN, window.innerWidth - VIEWPORT_MARGIN - MIN_WIDTH),
    maxTop: Math.max(VIEWPORT_MARGIN, window.innerHeight - VIEWPORT_MARGIN - HEADER_HEIGHT),
    maxWidth: Math.max(MIN_WIDTH, window.innerWidth - VIEWPORT_MARGIN * 2),
    maxHeight: Math.max(MIN_HEIGHT, window.innerHeight - VIEWPORT_MARGIN * 2),
  };
}

function normalizeRect(rect) {
  const bounds = getViewportBounds();
  const width = clampNum(snapNum(rect?.width), MIN_WIDTH, Math.min(MAX_WIDTH, bounds.maxWidth));
  const height = clampNum(snapNum(rect?.height), MIN_HEIGHT, Math.min(MAX_HEIGHT, bounds.maxHeight));
  const maxLeft = Math.max(VIEWPORT_MARGIN, bounds.maxWidth + VIEWPORT_MARGIN - width);
  const maxTop = Math.max(VIEWPORT_MARGIN, bounds.maxHeight + VIEWPORT_MARGIN - HEADER_HEIGHT);
  const left = clampNum(snapNum(rect?.left), VIEWPORT_MARGIN, maxLeft);
  const top = clampNum(snapNum(rect?.top), VIEWPORT_MARGIN, maxTop);
  return { left, top, width, height };
}

function lsKey(id) {
  return `utt_tool_geom_v2_${String(id || "").trim()}`;
}

function readGeomLS(id) {
  try {
    const raw = localStorage.getItem(lsKey(id)) || localStorage.getItem(`utt_tool_geom_v1_${String(id || "").trim()}`);
    if (!raw) return null;
    const j = JSON.parse(raw);
    if (!j || typeof j !== "object") return null;
    return {
      left: Number.isFinite(Number(j.left)) ? Number(j.left) : undefined,
      top: Number.isFinite(Number(j.top)) ? Number(j.top) : undefined,
      width: Number.isFinite(Number(j.width)) ? Number(j.width) : undefined,
      height: Number.isFinite(Number(j.height)) ? Number(j.height) : undefined,
    };
  } catch {
    return null;
  }
}

function writeGeomLS(id, geom) {
  try {
    const normalized = normalizeRect(geom);
    localStorage.setItem(lsKey(id), JSON.stringify(normalized));
  } catch {
    // ignore
  }
}
