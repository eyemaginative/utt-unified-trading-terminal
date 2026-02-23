// frontend/src/features/arb/ArbWindow.jsx
import { useEffect, useMemo, useState } from "react";

function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

export default function ArbWindow({
  styles,
  apiBase,
  // optional payload hooks (wire later)
  symbolCanon,
  venues,
  fmtPrice,
  fetchArbSnapshot,
}) {
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshSeconds, setRefreshSeconds] = useState(300);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const headerStyles = useMemo(
    () => ({
      wrap: {
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 10,
        padding: "10px 10px",
        borderBottom: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-surface-2, #151515)",
      },
      left: { display: "flex", flexDirection: "column", gap: 2 },
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
      small: { fontSize: 12, opacity: 0.8 },
      mono: { fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" },
      error: {
        marginTop: 8,
        color: "#ff6b6b",
        whiteSpace: "pre-wrap",
        border: "1px solid rgba(255,107,107,0.25)",
        background: "rgba(40,10,10,0.55)",
        padding: 10,
        borderRadius: 10,
      },
    }),
    []
  );

  async function doRefresh() {
    setLoading(true);
    setErr(null);
    try {
      // Placeholder for wiring later:
      // if (typeof fetchArbSnapshot === "function") await fetchArbSnapshot({ apiBase, symbol: symbolCanon, venues });
      setLastUpdated(new Date().toISOString());
    } catch (e) {
      const msg = e?.message || "Arb refresh failed";
      setErr(String(msg));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!autoRefresh) return;

    const ms = clampSeconds(refreshSeconds, 300) * 1000;
    const t = setInterval(() => {
      doRefresh();
    }, ms);

    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, refreshSeconds]);

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-surface-1, #121212)",
        borderRadius: 14,
        overflow: "hidden",
      }}
    >
      <div style={headerStyles.wrap}>
        <div style={headerStyles.left}>
          <div style={headerStyles.title}>Arb</div>
          <div style={headerStyles.sub}>
            Window scaffold (next step: migrate ArbChip UI/data into this tool window)
          </div>
        </div>

        <div style={headerStyles.right}>
          <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, opacity: 0.9 }}>
            <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(!!e.target.checked)} />
            Auto
          </label>

          <input
            style={{ ...headerStyles.ctl, width: 92 }}
            value={String(refreshSeconds)}
            onChange={(e) => setRefreshSeconds(e.target.value)}
            onBlur={() => setRefreshSeconds((v) => clampSeconds(v, 300))}
            inputMode="numeric"
            placeholder="seconds"
            title="Refresh seconds"
          />

          <button style={headerStyles.btn} onClick={doRefresh} disabled={loading}>
            {loading ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </div>

      <div style={headerStyles.body}>
        <div style={headerStyles.card}>
          <div style={{ ...headerStyles.small, marginBottom: 8 }}>
            Planned wiring:
            <div style={{ marginTop: 6, ...headerStyles.mono }}>
              - data: getArbSnapshot / arb scan service
              <br />- params: symbol={String(symbolCanon || "BTC-USD")} venues={(venues || []).join(", ") || "(default list)"}
              <br />- format: fmtPrice (if provided)
              <br />- base: apiBase={String(apiBase || "")}
            </div>
          </div>

          <div style={{ ...headerStyles.small }}>
            Last updated:{" "}
            <span style={headerStyles.mono}>{lastUpdated ? lastUpdated : "—"}</span>
          </div>

          {err ? <div style={headerStyles.error}>{err}</div> : null}
        </div>

        <div style={{ marginTop: 10, ...styles?.table ? {} : {} }}>
          <div style={{ fontSize: 12, opacity: 0.75 }}>
            Placeholder table (hook up Arb rows next).
          </div>
        </div>
      </div>
    </div>
  );
}
