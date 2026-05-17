// frontend/src/features/scanners/MarketCapWindow.jsx
import { useEffect, useMemo, useRef, useState } from "react";


function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

function trimApiBase(base) {
  return String(base || "").replace(/\/+$/, "");
}


function assetFromSymbol(symbolLike) {
  const raw = String(symbolLike || "").trim().toUpperCase();
  if (!raw) return "";
  const clean = raw.replace(/\s+/g, "").replace(/[\/_]/g, "-");
  if (clean.includes("-")) return clean.split("-").filter(Boolean)[0] || "";
  for (const suffix of ["USDT", "USDC", "USD", "HDX", "SOL", "DOT", "BTC", "ETH", "DOGE"]) {
    if (clean.endsWith(suffix) && clean.length > suffix.length) return clean.slice(0, -suffix.length);
  }
  return clean;
}

function isAbortLikeError(e) {
  const name = String(e?.name || "").toLowerCase();
  const msg = String(e?.message || e || "").toLowerCase();
  return name === "aborterror" || msg.includes("aborted") || msg.includes("abort");
}

async function fetchJson(url, timeoutMs = 60000) {
  let timer = null;
  try {
    const timeoutPromise = new Promise((_, reject) => {
      timer = window.setTimeout(() => reject(new Error("Market metrics refresh timed out")), timeoutMs);
    });
    const res = await Promise.race([
      fetch(url, { cache: "no-store" }),
      timeoutPromise,
    ]);
    if (!res.ok) {
      let msg = `HTTP ${res.status}`;
      try {
        const body = await res.json();
        msg = String(body?.detail || body?.message || msg);
      } catch {
        // keep HTTP status message
      }
      throw new Error(msg);
    }
    return res.json();
  } finally {
    if (timer) window.clearTimeout(timer);
  }
}

function toNum(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function fmtMoney(v, hide = false) {
  if (hide) return "••••";
  const n = toNum(v);
  if (n === null) return "—";
  if (Math.abs(n) >= 1_000_000_000_000) return `$${(n / 1_000_000_000_000).toFixed(2)}T`;
  if (Math.abs(n) >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(2)}B`;
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000) return `$${(n / 1_000).toFixed(2)}K`;
  if (Math.abs(n) >= 1) return `$${n.toFixed(2)}`;
  if (Math.abs(n) >= 0.000001) return `$${n.toFixed(8)}`;
  return `$${n.toPrecision(4)}`;
}

function fmtCompact(v, hide = false) {
  if (hide) return "••••";
  const n = toNum(v);
  if (n === null) return "—";
  if (Math.abs(n) >= 1_000_000_000_000) return `${(n / 1_000_000_000_000).toFixed(2)}T`;
  if (Math.abs(n) >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B`;
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(2)}K`;
  return n.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

function fmtPct(v, hide = false) {
  if (hide) return "••••";
  const n = toNum(v);
  if (n === null) return "—";
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
}

function fmtTime(v, hide = false) {
  if (hide) return "••••";
  if (!v) return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString();
}

function cellTitle(v) {
  if (v === null || v === undefined || v === "") return "";
  return String(v);
}

export default function MarketCapWindow({
  apiBase,
  selectedSymbol = "",
  hideTableData = false,
  onClose,
  height,
  onDragHandleMouseDown,
}) {
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshSeconds, setRefreshSeconds] = useState(300);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [rows, setRows] = useState([]);
  const [errors, setErrors] = useState([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const mountedRef = useRef(false);
  const requestSeqRef = useRef(0);

  const selectedAsset = useMemo(() => assetFromSymbol(selectedSymbol), [selectedSymbol]);
  const includeAssetsKey = selectedAsset || "";

  const ui = useMemo(
    () => ({
      wrap: {
        height: height || "100%",
        minHeight: 320,
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
        userSelect: onDragHandleMouseDown ? "none" : "auto",
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
      table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
      th: {
        textAlign: "left",
        position: "sticky",
        top: 0,
        zIndex: 1,
        background: "var(--utt-surface-2, #151515)",
        borderBottom: "1px solid var(--utt-border-1, #2a2a2a)",
        padding: "8px 7px",
        whiteSpace: "nowrap",
      },
      td: {
        borderBottom: "1px solid color-mix(in srgb, var(--utt-border-1, #2a2a2a) 70%, transparent)",
        padding: "8px 7px",
        verticalAlign: "top",
      },
      pill: {
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        padding: "2px 7px",
        borderRadius: 999,
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        background: "var(--utt-control-bg, #0f0f0f)",
        maxWidth: 180,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      },
    }),
    [height, onDragHandleMouseDown]
  );

  async function doRefresh() {
    const base = trimApiBase(apiBase);
    if (!base || busy) return;

    const seq = requestSeqRef.current + 1;
    requestSeqRef.current = seq;

    const p = new URLSearchParams();
    p.set("assets", "owned");
    if (includeAssetsKey) p.set("include_assets", includeAssetsKey);
    p.set("ttl_s", String(clampSeconds(refreshSeconds, 300)));
    p.set("limit", "40");

    setBusy(true);
    setErr("");

    try {
      const json = await fetchJson(`${base}/api/market_metrics/summary?${p.toString()}`);
      if (!mountedRef.current || requestSeqRef.current !== seq) return;
      const nextRows = Array.isArray(json?.items) ? json.items : [];
      setRows(nextRows);
      setErrors(Array.isArray(json?.errors) ? json.errors : []);
      setLastUpdated(json?.updated_at || new Date().toISOString());
    } catch (e) {
      if (!mountedRef.current || requestSeqRef.current !== seq || isAbortLikeError(e)) return;
      setErr(String(e?.message || e || "Market cap refresh failed"));
    } finally {
      if (mountedRef.current && requestSeqRef.current === seq) setBusy(false);
    }
  }

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      requestSeqRef.current += 1;
    };
  }, []);

  useEffect(() => {
    doRefresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiBase, includeAssetsKey]);

  useEffect(() => {
    if (!autoRefresh) return;
    const ms = clampSeconds(refreshSeconds, 300) * 1000;
    const t = setInterval(() => doRefresh(), ms);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, refreshSeconds, apiBase, includeAssetsKey]);

  const sortedRows = useMemo(() => {
    const arr = Array.isArray(rows) ? rows.slice() : [];
    return arr.sort((a, b) => {
      const an = toNum(a?.market_cap_usd);
      const bn = toNum(b?.market_cap_usd);
      if (an === null && bn === null) return String(a?.asset || "").localeCompare(String(b?.asset || ""));
      if (an === null) return 1;
      if (bn === null) return -1;
      return bn - an;
    });
  }, [rows]);

  const selectedRow = useMemo(() => {
    const a = String(selectedAsset || "").trim().toUpperCase();
    if (!a) return null;
    return sortedRows.find((r) => String(r?.asset || "").trim().toUpperCase() === a) || null;
  }, [sortedRows, selectedAsset]);

  const topLine = useMemo(() => {
    const preferred = selectedRow || sortedRows.find((r) => toNum(r?.market_cap_usd) !== null);
    if (!preferred) return selectedAsset ? `${selectedAsset} cap —` : "Global cap data";
    const asset = String(preferred.asset || selectedAsset || "—").toUpperCase();
    return `${asset} cap ${fmtMoney(preferred.market_cap_usd ?? preferred.fdv_usd, hideTableData)}`;
  }, [selectedRow, selectedAsset, sortedRows, hideTableData]);

  return (
    <div style={ui.wrap}>
      <div style={ui.header} onMouseDown={onDragHandleMouseDown} title={onDragHandleMouseDown ? "Drag to move" : undefined}>
        <div>
          <div style={ui.title}>Market Cap</div>
          <div style={ui.sub}>{hideTableData ? "••••" : topLine}</div>
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

          <button style={ui.btn} onClick={doRefresh} disabled={busy}>
            {busy ? "Refreshing…" : "Refresh"}
          </button>

          {onClose ? (
            <button style={ui.btn} onClick={onClose}>
              Close
            </button>
          ) : null}
        </div>
      </div>

      <div style={ui.body}>
        {err ? (
          <div style={{ ...ui.card, marginBottom: 10, borderColor: "rgba(255,100,100,0.45)", color: "var(--utt-danger, #ff7b7b)" }}>
            {err}
          </div>
        ) : null}

        {errors.length ? (
          <div style={{ ...ui.card, marginBottom: 10, fontSize: 12, opacity: 0.85 }}>
            {errors.slice(0, 3).map((e, idx) => (
              <div key={idx}>{String(e?.message || e?.error || e)}</div>
            ))}
          </div>
        ) : null}

        <div style={{ overflow: "auto", border: "1px solid var(--utt-border-1, #2a2a2a)", borderRadius: 12 }}>
          <table style={ui.table}>
            <thead>
              <tr>
                <th style={ui.th}>Rank</th>
                <th style={ui.th}>Asset</th>
                <th style={ui.th}>Price</th>
                <th style={ui.th}>24h</th>
                <th style={ui.th}>Market Cap</th>
                <th style={ui.th}>FDV / Supply Cap</th>
                <th style={ui.th}>Supply Source</th>
                <th style={ui.th}>Price Source</th>
                <th style={ui.th}>Updated</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.length ? (
                sortedRows.map((r, idx) => {
                  const asset = String(r?.asset || "—").toUpperCase();
                  const warnings = Array.isArray(r?.warnings) ? r.warnings.filter(Boolean) : [];
                  return (
                    <tr key={`${asset}:${r?.source || ""}:${idx}`}>
                      <td style={ui.td}>{hideTableData ? "••••" : r?.rank || "—"}</td>
                      <td style={{ ...ui.td, fontWeight: 900 }}>
                        {hideTableData ? "••••" : asset}
                        {warnings.length ? (
                          <div style={{ fontSize: 10, opacity: 0.62, marginTop: 2 }} title={warnings.join("; ")}>
                            {warnings.length} warning{warnings.length === 1 ? "" : "s"}
                          </div>
                        ) : null}
                      </td>
                      <td style={ui.td}>{fmtMoney(r?.price_usd, hideTableData)}</td>
                      <td style={ui.td}>{fmtPct(r?.change_24h_pct, hideTableData)}</td>
                      <td style={{ ...ui.td, fontWeight: 800 }}>{fmtMoney(r?.market_cap_usd, hideTableData)}</td>
                      <td style={ui.td}>
                        <div>{fmtMoney(r?.fdv_usd, hideTableData)}</div>
                        <div style={{ fontSize: 10, opacity: 0.68 }}>{fmtCompact(r?.circulating_supply, hideTableData)} circ</div>
                      </td>
                      <td style={ui.td} title={cellTitle(r?.supply_source)}>
                        <span style={ui.pill}>{hideTableData ? "••••" : r?.supply_source || "—"}</span>
                      </td>
                      <td style={ui.td} title={cellTitle(r?.price_source || r?.source)}>
                        <span style={ui.pill}>{hideTableData ? "••••" : r?.price_source || r?.source || "—"}</span>
                      </td>
                      <td style={{ ...ui.td, ...ui.mono, fontSize: 11 }}>{fmtTime(r?.updated_at || lastUpdated, hideTableData)}</td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td style={ui.td} colSpan={9}>
                    {busy ? "Loading market cap metrics…" : "No market cap rows returned yet."}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={{ marginTop: 10, fontSize: 12, opacity: 0.78 }}>
          Last updated: <span style={ui.mono}>{hideTableData ? "••••" : lastUpdated || "—"}</span>
        </div>
      </div>
    </div>
  );
}
