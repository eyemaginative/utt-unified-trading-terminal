// frontend/src/features/scanners/VolumeWindow.jsx
import { useEffect, useMemo, useState } from "react";

function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

export default function VolumeWindow({ apiBase }) {
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshSeconds, setRefreshSeconds] = useState(300);
  const [lastUpdated, setLastUpdated] = useState(null);

  const ui = useMemo(
    () => ({
      wrap: {
        height: "100%",
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
    }),
    []
  );

  async function doRefresh() {
    setLastUpdated(new Date().toISOString());
  }

  useEffect(() => {
    if (!autoRefresh) return;
    const ms = clampSeconds(refreshSeconds, 300) * 1000;
    const t = setInterval(() => doRefresh(), ms);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, refreshSeconds]);

  return (
    <div style={ui.wrap}>
      <div style={ui.header}>
        <div>
          <div style={ui.title}>Volume</div>
          <div style={ui.sub}>Top 250 symbols by 24h volume (scaffold).</div>
        </div>

        <div style={ui.right}>
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

          <button style={ui.btn} onClick={doRefresh}>
            Refresh
          </button>
        </div>
      </div>

      <div style={ui.body}>
        <div style={ui.card}>
          <div style={{ fontSize: 12, opacity: 0.8, marginBottom: 6 }}>Hook up endpoint (next step):</div>
          <div style={{ fontSize: 12, opacity: 0.85, ...ui.mono }}>
            - API base: {String(apiBase || "")}
            <br />- Endpoint: TBD (e.g. /api/scanners/volume?limit=250)
            <br />- Output: symbol, venue, 24h volume, price, 24h change, etc.
          </div>

          <div style={{ marginTop: 10, fontSize: 12, opacity: 0.8 }}>
            Last updated: <span style={ui.mono}>{lastUpdated || "—"}</span>
          </div>
        </div>

        <div style={{ marginTop: 10, fontSize: 12, opacity: 0.75 }}>
          Placeholder table area (scroll once rows exist).
        </div>
      </div>
    </div>
  );
}
