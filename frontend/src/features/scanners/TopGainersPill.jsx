// frontend/src/features/scanners/TopGainersPill.jsx
import { useEffect, useMemo, useState } from "react";
import TopGainersService from "./topGainersService";

function fmt2(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

export default function TopGainersPill({
  apiBase,
  enabledVenues = [],
  // window wiring
  isOpen = false,
  onOpen,
  onClose,

  // optional: keep pill scoped to a venue
  venueFilter = "",

  // optional: hide sensitive data
  hideTableData = false,
}) {
  const enabledVenuesNorm = useMemo(
    () => (enabledVenues || []).map((v) => String(v || "").trim().toLowerCase()).filter(Boolean).sort(),
    [enabledVenues]
  );

  const [snap, setSnap] = useState(() => TopGainersService.readCache() || null);

  useEffect(() => {
    TopGainersService.start({
      apiBase,
      enabledVenues: enabledVenuesNorm,
      venueFilter,
      // autoRefresh/refreshSeconds are persisted in LS and read by service
    });

    const unsub = TopGainersService.subscribe((s) => setSnap(s));
    return () => unsub();
  }, [apiBase, enabledVenuesNorm.join("|"), venueFilter]);

  const topText = useMemo(() => {
    const top = snap?.top;
    if (!top || !top.symbol) return "—";
    const sym = hideTableData ? "••••" : top.symbol;
    const pct = hideTableData ? "••••" : `${fmt2(top.change_1d)}%`;
    return `${sym} ${pct}`;
  }, [snap, hideTableData]);

  const label = "Top Gainers";
  const statusText = isOpen ? "Open" : "Closed";

  return (
    <button
      type="button"
      onClick={() => {
        if (isOpen) onClose?.();
        else onOpen?.();
      }}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 10,
        borderRadius: 999,
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-surface-2, #151515)",
        color: "var(--utt-page-fg, #eee)",
        padding: "8px 12px",
        cursor: "pointer",
        userSelect: "none",
      }}
      title="Top Gainers (polls even when window is closed)"
    >
      <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", lineHeight: 1.1 }}>
        <div style={{ fontSize: 13, fontWeight: 900 }}>
          {label} <span style={{ fontSize: 11, opacity: 0.7, fontWeight: 800 }}>{statusText}</span>
        </div>
        <div style={{ fontSize: 12, opacity: 0.85, fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" }}>
          {topText}
        </div>
      </div>
    </button>
  );
}
