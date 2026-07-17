import React, { useEffect, useMemo, useState } from "react";

const HISTORY_CSS = `
.utt-rh-history { display: grid; gap: 10px; }
.utt-rh-history-header {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  padding: 10px;
  border: 1px solid rgba(66,232,255,0.26);
  border-radius: 10px;
  background: linear-gradient(135deg, rgba(66,232,255,0.08), rgba(0,0,0,0.2) 60%, rgba(93,255,154,0.05));
}
.utt-rh-history-title {
  color: var(--wallet-cyan, #42e8ff);
  font-family: "Roboto Mono", "Cascadia Code", ui-monospace, monospace;
  font-size: 13px;
  font-weight: 900;
  letter-spacing: 0.065em;
  text-transform: uppercase;
}
.utt-rh-history-meta { color: var(--utt-hdr-muted, rgba(233,250,255,0.68)); font-family: ui-monospace, monospace; font-size: 11px; }
.utt-rh-history-chip {
  display: inline-flex;
  align-items: center;
  min-height: 22px;
  padding: 1px 7px;
  border: 1px solid rgba(66,232,255,0.3);
  border-radius: 999px;
  color: var(--wallet-cyan, #42e8ff);
  background: rgba(66,232,255,0.06);
  font-family: ui-monospace, monospace;
  font-size: 10px;
  font-weight: 900;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
.utt-rh-history-chip--good { color: var(--wallet-green, #5dff9a); border-color: rgba(93,255,154,0.4); background: rgba(93,255,154,0.07); }
.utt-rh-history-chip--warn { color: var(--wallet-amber, #ffc857); border-color: rgba(255,200,87,0.42); background: rgba(255,200,87,0.07); }
.utt-rh-history-chip--bad { color: var(--wallet-red, #ff5f7a); border-color: rgba(255,95,122,0.42); background: rgba(255,95,122,0.08); }
.utt-rh-history-controls { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.utt-rh-history-summary { color: var(--utt-hdr-muted, rgba(233,250,255,0.72)); font-family: ui-monospace, monospace; font-size: 11px; }
.utt-rh-history-empty {
  padding: 18px;
  border: 1px dashed rgba(66,232,255,0.24);
  border-radius: 9px;
  color: var(--utt-hdr-muted, rgba(233,250,255,0.68));
  background: rgba(0,0,0,0.18);
  text-align: center;
}
.utt-rh-history-table { min-width: 1500px; }
.utt-rh-history-table td { vertical-align: top; }
.utt-rh-history-mono { font-family: "Cascadia Code", ui-monospace, monospace; }
.utt-rh-history-link { color: var(--wallet-cyan, #42e8ff); text-decoration: none; }
.utt-rh-history-link:hover { text-decoration: underline; }
.utt-rh-history-detail td { padding: 0 !important; }
.utt-rh-history-detail pre {
  margin: 0;
  padding: 10px;
  border-left: 3px solid rgba(66,232,255,0.5);
  color: rgba(233,250,255,0.86);
  background: rgba(1,8,12,0.9);
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  font-family: "Cascadia Code", ui-monospace, monospace;
  font-size: 11px;
}
`;

function compact(value, left = 8, right = 6) {
  const text = String(value || "").trim();
  if (!text) return "—";
  if (text.length <= left + right + 3) return text;
  return `${text.slice(0, left)}…${text.slice(-right)}`;
}

function classificationTone(value) {
  const text = String(value || "unknown").toLowerCase();
  if (["failed", "reverted"].includes(text)) return "utt-rh-history-chip--bad";
  if (["approval", "bridge_candidate", "swap_candidate", "unknown"].includes(text)) return "utt-rh-history-chip--warn";
  return "utt-rh-history-chip--good";
}

function statusTone(value) {
  const text = String(value || "unknown").toLowerCase();
  if (["error", "failed", "reverted"].includes(text)) return "utt-rh-history-chip--bad";
  if (["ok", "success", "successful"].includes(text)) return "utt-rh-history-chip--good";
  return "utt-rh-history-chip--warn";
}

function directionLabel(value) {
  const text = String(value || "other").toLowerCase();
  if (text === "in") return "IN";
  if (text === "out") return "OUT";
  if (text === "self") return "SELF";
  return "OTHER";
}

function formatTimestamp(value) {
  const text = String(value || "").trim();
  if (!text) return "—";
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? text : date.toLocaleString();
}

export default function RobinhoodChainHistoryPanel({
  api,
  wallet,
  hideTableData = false,
  loadRequestId = 0,
}) {
  const [items, setItems] = useState([]);
  const [nextCursor, setNextCursor] = useState("");
  const [historyMeta, setHistoryMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState({});

  const address = String(wallet?.address || "").trim();
  const isRobinhoodChain = (
    String(wallet?.wallet_id || "").trim().toLowerCase() === "robinhood_chain" ||
    String(wallet?.network || "").trim().toLowerCase() === "robinhood_chain"
  );
  const visibleAddress = hideTableData ? "••••••••" : compact(address, 12, 10);

  const counts = useMemo(() => {
    const out = {};
    for (const item of items) {
      const key = String(item?.classification || "unknown");
      out[key] = (out[key] || 0) + 1;
    }
    return out;
  }, [items]);

  async function loadHistory({ cursor = "", append = false, forceRefresh = false } = {}) {
    if (!address || !isRobinhoodChain || typeof api !== "function") return;
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      if (cursor) params.set("cursor", cursor);
      if (forceRefresh) params.set("force_refresh", "true");
      const suffix = params.toString() ? `?${params.toString()}` : "";
      const result = await api(
        `/api/robinhood_chain/address/${encodeURIComponent(address)}/history${suffix}`,
      );
      const rows = Array.isArray(result?.items) ? result.items : [];
      setItems((previous) => {
        if (!append) return rows;
        const merged = [...previous, ...rows];
        const seen = new Set();
        return merged.filter((row, index) => {
          const key = String(row?.id || row?.transaction_hash || index);
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
      });
      setNextCursor(String(result?.next_cursor || ""));
      setHistoryMeta(result || null);
      if (!append) setExpanded({});
    } catch (loadError) {
      setError(loadError?.message || String(loadError));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!loadRequestId || !address || !isRobinhoodChain) return;
    setItems([]);
    setNextCursor("");
    setHistoryMeta(null);
    setExpanded({});
    loadHistory({ forceRefresh: false });
    // loadRequestId is intentionally the explicit user-action trigger.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadRequestId, address, isRobinhoodChain]);

  if (!wallet) {
    return (
      <div className="utt-wallet-panel">
        <style>{HISTORY_CSS}</style>
        <div className="utt-wallet-panel-title">Robinhood Chain Transactions</div>
        <div className="utt-rh-history-empty">
          Select <b>Txs</b> on a saved Robinhood Chain wallet address. Opening this tab alone does not request history.
        </div>
      </div>
    );
  }

  if (!isRobinhoodChain) {
    return (
      <div className="utt-wallet-panel">
        <style>{HISTORY_CSS}</style>
        <div className="utt-wallet-panel-title">Robinhood Chain Transactions</div>
        <div className="utt-rh-history-empty">The selected wallet is not a Robinhood Chain address.</div>
      </div>
    );
  }

  return (
    <div className="utt-wallet-panel utt-rh-history">
      <style>{HISTORY_CSS}</style>

      <div className="utt-rh-history-header">
        <div>
          <div className="utt-rh-history-title">RH-EVM // Read-only transaction history</div>
          <div className="utt-rh-history-meta" title={hideTableData ? "Address hidden" : address}>
            {wallet?.label || "Robinhood Chain wallet"} • {visibleAddress}
          </div>
        </div>
        <div style={{ flex: 1 }} />
        <span className="utt-rh-history-chip utt-rh-history-chip--good">Chain 4663</span>
        <span className="utt-rh-history-chip">Display only</span>
        {historyMeta?.cached ? <span className="utt-rh-history-chip">Cached</span> : null}
        {historyMeta?.stale ? <span className="utt-rh-history-chip utt-rh-history-chip--warn">Stale</span> : null}
        {historyMeta?.partial ? <span className="utt-rh-history-chip utt-rh-history-chip--warn">Partial</span> : null}
      </div>

      <div className="utt-rh-history-controls">
        <button type="button" onClick={() => loadHistory({ forceRefresh: true })} disabled={loading || !address}>
          Force refresh
        </button>
        <button
          type="button"
          onClick={() => loadHistory({ cursor: nextCursor, append: true })}
          disabled={loading || !nextCursor}
        >
          Load more
        </button>
        <button
          type="button"
          onClick={() => {
            setItems([]);
            setNextCursor("");
            setHistoryMeta(null);
            setExpanded({});
            setError("");
          }}
          disabled={loading || (!items.length && !historyMeta)}
        >
          Clear view
        </button>
        <span className="utt-rh-history-summary">
          {loading ? "Reading bounded Blockscout history…" : `${items.length} displayed`}
          {historyMeta?.fetched_at ? ` • ${formatTimestamp(historyMeta.fetched_at)}` : ""}
          {Object.keys(counts).length ? ` • ${Object.entries(counts).map(([key, value]) => `${key}:${value}`).join(" ")}` : ""}
        </span>
      </div>

      {error ? <div className="utt-wallet-error"><b>History error:</b> {error}</div> : null}

      {!loading && !items.length ? (
        <div className="utt-rh-history-empty">
          No history rows are loaded. Use <b>Txs</b> on the address row or <b>Force refresh</b> here.
          No transaction is persisted or sent to the ledger.
        </div>
      ) : null}

      {items.length ? (
        <div className="utt-wallet-table-wrap">
          <table className="utt-wallet-table utt-rh-history-table">
            <thead>
              <tr>
                {[
                  "details",
                  "timestamp",
                  "transaction",
                  "status",
                  "classification",
                  "direction",
                  "asset",
                  "amount",
                  "from",
                  "to",
                  "method",
                  "fee ETH",
                  "block",
                  "confirmations",
                  "contract",
                  "explorer",
                ].map((heading) => <th key={heading}>{heading}</th>)}
              </tr>
            </thead>
            <tbody>
              {items.map((item, rowIndex) => {
                const rowId = String(item?.id || item?.transaction_hash || rowIndex);
                const isExpanded = !!expanded[rowId];
                return (
                  <React.Fragment key={rowId}>
                    <tr>
                      <td>
                        <button
                          type="button"
                          onClick={() => setExpanded((previous) => ({ ...previous, [rowId]: !previous[rowId] }))}
                          aria-expanded={isExpanded}
                        >
                          {isExpanded ? "Hide" : "Inspect"}
                        </button>
                      </td>
                      <td>{formatTimestamp(item?.timestamp)}</td>
                      <td className="utt-rh-history-mono" title={hideTableData ? "Transaction hidden" : item?.transaction_hash || ""}>
                        {hideTableData ? "••••••••" : compact(item?.transaction_hash, 10, 8)}
                      </td>
                      <td><span className={`utt-rh-history-chip ${statusTone(item?.status)}`}>{item?.status || "unknown"}</span></td>
                      <td><span className={`utt-rh-history-chip ${classificationTone(item?.classification)}`}>{item?.classification || "unknown"}</span></td>
                      <td><span className="utt-rh-history-chip">{directionLabel(item?.direction)}</span></td>
                      <td>{item?.asset || "—"}{item?.registered ? " · REG" : ""}</td>
                      <td className="utt-rh-history-mono">{String(item?.amount ?? "—")}</td>
                      <td className="utt-rh-history-mono" title={hideTableData ? "Address hidden" : item?.from_address || ""}>{hideTableData ? "••••••••" : compact(item?.from_address)}</td>
                      <td className="utt-rh-history-mono" title={hideTableData ? "Address hidden" : item?.to_address || ""}>{hideTableData ? "••••••••" : compact(item?.to_address)}</td>
                      <td>{item?.method || "—"}</td>
                      <td className="utt-rh-history-mono">{String(item?.fee_eth ?? "0")}</td>
                      <td>{item?.block_number ?? "—"}</td>
                      <td>{item?.confirmations ?? "—"}</td>
                      <td className="utt-rh-history-mono" title={hideTableData ? "Contract hidden" : item?.contract_address || ""}>{hideTableData && item?.contract_address ? "••••••••" : compact(item?.contract_address)}</td>
                      <td>
                        {item?.explorer_url ? (
                          <a className="utt-rh-history-link" href={item.explorer_url} target="_blank" rel="noreferrer">Open</a>
                        ) : "—"}
                      </td>
                    </tr>
                    {isExpanded ? (
                      <tr className="utt-rh-history-detail">
                        <td colSpan={16}>
                          <pre>{JSON.stringify({
                            ...item,
                            from_address: hideTableData ? "••••••••" : item?.from_address,
                            to_address: hideTableData ? "••••••••" : item?.to_address,
                            contract_address: hideTableData && item?.contract_address ? "••••••••" : item?.contract_address,
                          }, null, 2)}</pre>
                        </td>
                      </tr>
                    ) : null}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : null}

      <div className="utt-rh-history-meta">
        Fixed sources: Blockscout address transactions and ERC-20 transfers. No wallet-address transaction cache, deposits,
        withdrawals, ledger entries, FIFO lots, or basis records are created by this panel.
      </div>
    </div>
  );
}
