// frontend/src/WindowManager.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";

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
        const width = clampNum(stored?.width ?? w?.width ?? 980, 380, 2200);
        const height = clampNum(stored?.height ?? w?.height ?? 640, 260, 2000);

        const defLeft = Number.isFinite(Number(w?.left)) ? Number(w.left) : 60;
        const defTop = Number.isFinite(Number(w?.top)) ? Number(w.top) : 110;

        const left = clampNum(stored?.left ?? defLeft, 0, Math.max(0, window.innerWidth - 60));
        const top = clampNum(stored?.top ?? defTop, 0, Math.max(0, window.innerHeight - 60));

        next = next === prev ? { ...prev } : next;
        next[id] = { left, top, width, height };
      }
      return next;
    });
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

      const dx = Number(x) - d.startX;
      const dy = Number(y) - d.startY;

      setGeomById((prev) => {
        const g = prev[d.id];
        if (!g) return prev;
        const left = clampNum(d.startLeft + dx, 0, Math.max(0, window.innerWidth - 40));
        const top = clampNum(d.startTop + dy, 0, Math.max(0, window.innerHeight - 40));
        const next = { ...prev, [d.id]: { ...g, left, top } };
        return next;
      });
    }

    function onUp() {
      const d = dragRef.current;
      if (!d) return;
      dragRef.current = null;

      // Persist after drag end
      try {
        const g = geomById[d.id];
        if (g) writeGeomLS(d.id, g);
      } catch {
        // ignore
      }
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [geomById]);

  // Close windows that are no longer open in parent state
  useEffect(() => {
    const openIds = new Set(openWindows.map((w) => String(w?.id || "").trim()).filter(Boolean));
    setGeomById((prev) => {
      let next = prev;
      for (const id of Object.keys(prev)) {
        if (openIds.has(id)) continue;
        next = next === prev ? { ...prev } : next;
        delete next[id];
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
            style={{
              position: "fixed",
              left: g.left,
              top: g.top,
              width: g.width,
              height: g.height,
              zIndex: z,
              display: "flex",
              flexDirection: "column",
              borderRadius: 12,
              overflow: "hidden",
              background: "var(--utt-surface-1, rgba(20,22,28,0.98))",
              border: "1px solid var(--utt-border-1, rgba(255,255,255,0.10))",
              boxShadow: "0 10px 30px rgba(0,0,0,0.55)",
              backdropFilter: "blur(6px)",
            }}
            onMouseDown={() => bringToFront(id)}
            onTouchStart={() => bringToFront(id)}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 10,
                padding: "8px 10px",
                background: "var(--utt-surface-2, rgba(28,30,38,0.98))",
                borderBottom: "1px solid var(--utt-border-1, rgba(255,255,255,0.10))",
                cursor: "move",
                userSelect: "none",
              }}
              onMouseDown={(e) => startDrag(e, id)}
              onTouchStart={(e) => startDrag(e, id)}
              title="Drag to move"
            >
              <div style={{ fontWeight: 700, fontSize: 13, letterSpacing: 0.2, opacity: 0.95 }}>{title}</div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
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
                overflow: "auto",
                padding: 10,
                boxSizing: "border-box",
              }}
              // Persist size on pointer-up after user resizes via CSS resize handle.
              onMouseUp={() => persistSizeFromDom(id)}
              onTouchEnd={() => persistSizeFromDom(id)}
            >
              {el}
            </div>

            {/* Resize handle: use CSS resize, but keep it subtle */}
            <div
              style={{
                position: "absolute",
                right: 0,
                bottom: 0,
                width: 18,
                height: 18,
                cursor: "nwse-resize",
                opacity: 0.55,
              }}
              onMouseDown={(e) => {
                // Let browser resize take over; just bring to front.
                e.stopPropagation();
                bringToFront(id);
              }}
            />
            <style>{`/* allow resizing only for our window shell */
              [data-utt-tool-shell] { resize: both; }
            `}</style>
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
      // We don't have a ref per window, so just persist current geom.
      // Most users will resize via browser's resize behavior; we snapshot current geom.
      const g = geomById[id];
      if (g) writeGeomLS(id, g);
    } catch {
      // ignore
    }
  }
}

const miniBtnStyle = {
  fontSize: 12,
  padding: "4px 10px",
  borderRadius: 10,
  border: "1px solid var(--utt-border-1, rgba(255,255,255,0.16))",
  background: "rgba(255,255,255,0.06)",
  color: "var(--utt-text, #e9eef7)",
  cursor: "pointer",
};

function clampNum(v, lo, hi) {
  const x = Number(v);
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}

function lsKey(id) {
  return `utt_tool_geom_v1_${String(id || "").trim()}`;
}

function readGeomLS(id) {
  try {
    const raw = localStorage.getItem(lsKey(id));
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
    const g = {
      left: Number(geom?.left) || 0,
      top: Number(geom?.top) || 0,
      width: Number(geom?.width) || 980,
      height: Number(geom?.height) || 640,
    };
    localStorage.setItem(lsKey(id), JSON.stringify(g));
  } catch {
    // ignore
  }
}
