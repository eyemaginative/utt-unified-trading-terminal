import React, { useMemo } from "react";

const PREVIEW_CSS = `
.utt-rh-accounting {
  display: grid;
  gap: 10px;
  margin-top: 10px;
  padding: 12px;
  border: 1px solid rgba(153,101,255,0.38);
  border-radius: 12px;
  background:
    linear-gradient(135deg, rgba(153,101,255,0.11), rgba(0,0,0,0.25) 48%, rgba(66,232,255,0.07)),
    rgba(3,9,16,0.94);
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.04), 0 0 24px rgba(153,101,255,0.08);
}
.utt-rh-accounting-head { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.utt-rh-accounting-title {
  color: #c7a7ff;
  font-family: "Roboto Mono", "Cascadia Code", ui-monospace, monospace;
  font-size: 13px;
  font-weight: 900;
  letter-spacing: 0.065em;
  text-transform: uppercase;
}
.utt-rh-accounting-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
.utt-rh-accounting-card {
  min-width: 0;
  padding: 9px;
  border: 1px solid rgba(66,232,255,0.18);
  border-radius: 9px;
  background: rgba(0,0,0,0.22);
}
.utt-rh-accounting-label { color: rgba(233,250,255,0.56); font-family: ui-monospace, monospace; font-size: 10px; letter-spacing: .065em; text-transform: uppercase; }
.utt-rh-accounting-value { margin-top: 4px; overflow-wrap: anywhere; color: var(--utt-page-fg, #e9faff); font-family: ui-monospace, monospace; font-weight: 800; }
.utt-rh-accounting-section { padding: 9px; border: 1px solid rgba(66,232,255,0.16); border-radius: 9px; background: rgba(0,0,0,0.18); }
.utt-rh-accounting-section h4 { margin: 0 0 7px; color: var(--wallet-cyan, #42e8ff); font-family: ui-monospace, monospace; letter-spacing: .055em; text-transform: uppercase; }
.utt-rh-accounting-warning { padding: 7px 9px; border-left: 3px solid var(--wallet-amber, #ffc857); color: #ffe7ac; background: rgba(255,200,87,0.07); }
.utt-rh-accounting-safe { padding: 7px 9px; border-left: 3px solid var(--wallet-green, #5dff9a); color: #caffdd; background: rgba(93,255,154,0.06); }
.utt-rh-accounting-table { width: 100%; border-collapse: collapse; }
.utt-rh-accounting-table th, .utt-rh-accounting-table td { padding: 6px; border-bottom: 1px solid rgba(66,232,255,0.1); text-align: left; white-space: nowrap; }
.utt-rh-accounting-table th { color: var(--wallet-cyan, #42e8ff); font-family: ui-monospace, monospace; font-size: 10px; text-transform: uppercase; }
.utt-rh-accounting-mono { font-family: "Cascadia Code", ui-monospace, monospace; }
.utt-rh-accounting-json { max-height: 320px; margin: 0; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 11px; }
@media (max-width: 980px) { .utt-rh-accounting-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 580px) { .utt-rh-accounting-grid { grid-template-columns: 1fr; } }
`;

function compact(value, left = 10, right = 8) {
  const text = String(value || "").trim();
  if (!text) return "—";
  if (text.length <= left + right + 3) return text;
  return `${text.slice(0, left)}…${text.slice(-right)}`;
}

function fmt(value) {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return number.toLocaleString(undefined, { maximumFractionDigits: 12 });
}

function redactSensitive(value, key = "") {
  if (Array.isArray(value)) return value.map((item) => redactSensitive(item, key));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([childKey, childValue]) => [
      childKey,
      redactSensitive(childValue, childKey),
    ]));
  }
  const normalizedKey = String(key || "").toLowerCase();
  const text = String(value ?? "");
  const isSensitiveKey = (
    normalizedKey.includes("address") ||
    normalizedKey.includes("transaction_hash") ||
    normalizedKey.endsWith("txid") ||
    normalizedKey.endsWith("proof_url") ||
    normalizedKey === "explorer_url"
  );
  const isAddressCounterparty = normalizedKey === "counterparty" && /^0x[0-9a-f]{40}$/i.test(text);
  return isSensitiveKey || isAddressCounterparty ? "••••••••" : value;
}

export default function RobinhoodChainAccountingPreview({
  preview,
  loading = false,
  error = "",
  hideTableData = false,
  onRefresh,
  onClose,
}) {
  const basisRows = useMemo(() => {
    const groups = Array.isArray(preview?.basis_preview?.outgoing_assets)
      ? preview.basis_preview.outgoing_assets
      : [];
    return groups.flatMap((group) => (
      Array.isArray(group?.slices)
        ? group.slices.map((slice) => ({ ...slice, asset: group?.scope?.asset, scope: group?.scope }))
        : []
    ));
  }, [preview]);

  if (!preview && !loading && !error) return null;
  const safety = preview?.safety || {};
  const existingCounts = preview?.existing_state?.counts || {};
  const candidateRecords = preview?.candidate_records || {};
  const activityLegs = Array.isArray(preview?.activity_legs) ? preview.activity_legs : [];

  return (
    <div className="utt-rh-accounting" aria-live="polite">
      <style>{PREVIEW_CSS}</style>
      <div className="utt-rh-accounting-head">
        <div>
          <div className="utt-rh-accounting-title">RH-EVM // Accounting & transfer preview</div>
          <div className="utt-rh-history-meta">
            Explicit read-only analysis. No record can be applied from this panel.
          </div>
        </div>
        <div style={{ flex: 1 }} />
        <span className="utt-rh-history-chip">Read only</span>
        <span className="utt-rh-history-chip utt-rh-history-chip--good">Will mutate: no</span>
        <button type="button" onClick={onRefresh} disabled={loading || !preview?.transaction_hash}>Refresh preview</button>
        <button type="button" onClick={onClose} disabled={loading}>Close preview</button>
      </div>

      {loading ? <div className="utt-rh-accounting-safe">Reading exact transaction detail and current local accounting state…</div> : null}
      {error ? <div className="utt-wallet-error"><b>Accounting preview error:</b> {error}</div> : null}

      {preview ? (
        <>
          <div className="utt-rh-accounting-grid">
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Classification</div><div className="utt-rh-accounting-value">{preview.classification || "unknown"}</div></div>
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Confidence</div><div className="utt-rh-accounting-value">{preview.confidence || "—"}</div></div>
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Transaction</div><div className="utt-rh-accounting-value" title={hideTableData ? "Transaction hidden" : preview.transaction_hash}>{hideTableData ? "••••••••" : compact(preview.transaction_hash)}</div></div>
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Source scope</div><div className="utt-rh-accounting-value">{preview.venue || "—"} / {preview.wallet_id || "default"}</div></div>
          </div>

          {(preview.warnings || []).map((warning, index) => <div className="utt-rh-accounting-warning" key={`${index}:${warning}`}>{warning}</div>)}

          <div className="utt-rh-accounting-section">
            <h4>Activity legs</h4>
            {activityLegs.length ? (
              <div className="utt-wallet-table-wrap">
                <table className="utt-rh-accounting-table">
                  <thead><tr><th>Direction</th><th>Asset</th><th>Quantity</th><th>Counterparty</th><th>Owned</th><th>Contract</th><th>Registered</th></tr></thead>
                  <tbody>
                    {activityLegs.map((leg, index) => (
                      <tr key={leg.id || index}>
                        <td>{String(leg.direction || "—").toUpperCase()}</td>
                        <td>{leg.asset || "—"}</td>
                        <td className="utt-rh-accounting-mono">{fmt(leg.quantity)}</td>
                        <td className="utt-rh-accounting-mono" title={hideTableData ? "Address hidden" : leg.counterparty || ""}>{hideTableData && leg.counterparty ? "••••••••" : compact(leg.counterparty)}</td>
                        <td>{leg.counterparty_owned ? "YES" : "NO"}</td>
                        <td className="utt-rh-accounting-mono" title={hideTableData ? "Contract hidden" : leg.contract_address || ""}>{hideTableData && leg.contract_address ? "••••••••" : compact(leg.contract_address)}</td>
                        <td>{leg.registered ? "YES" : "NO"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : <div className="utt-rh-history-meta">No positive asset movement was normalized for this wallet.</div>}
          </div>

          <div className="utt-rh-accounting-grid">
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Deposit candidates</div><div className="utt-rh-accounting-value">{candidateRecords?.deposits?.length || 0}</div></div>
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Withdrawal candidates</div><div className="utt-rh-accounting-value">{candidateRecords?.withdrawals?.length || 0}</div></div>
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Transfer candidates</div><div className="utt-rh-accounting-value">{candidateRecords?.transfer_links?.length || 0}</div></div>
            <div className="utt-rh-accounting-card"><div className="utt-rh-accounting-label">Existing local references</div><div className="utt-rh-accounting-value">{Object.values(existingCounts).reduce((sum, value) => sum + (Number(value) || 0), 0)}</div></div>
          </div>

          <div className="utt-rh-accounting-section">
            <h4>Read-only FIFO preview</h4>
            {basisRows.length ? (
              <div className="utt-wallet-table-wrap">
                <table className="utt-rh-accounting-table">
                  <thead><tr><th>Asset</th><th>Lot</th><th>Acquired</th><th>Available</th><th>Preview qty</th><th>Basis moved USD</th><th>Missing basis</th></tr></thead>
                  <tbody>
                    {basisRows.map((row, index) => (
                      <tr key={`${row.lot_id}:${index}`}>
                        <td>{row.asset || "—"}</td>
                        <td className="utt-rh-accounting-mono">{compact(row.lot_id, 8, 6)}</td>
                        <td>{row.acquired_at || "—"}</td>
                        <td className="utt-rh-accounting-mono">{fmt(row.qty_available_before)}</td>
                        <td className="utt-rh-accounting-mono">{fmt(row.qty_previewed)}</td>
                        <td className="utt-rh-accounting-mono">{fmt(row.basis_moved_usd)}</td>
                        <td>{row.basis_is_missing ? "YES" : "NO"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : <div className="utt-rh-history-meta">No outgoing asset leg requires a FIFO preview for this classification.</div>}
          </div>

          <div className="utt-rh-accounting-safe">
            Verified safety flags: read_only={String(!!safety.read_only)} · can_apply={String(!!safety.can_apply)} ·
            ledger_mutation={String(!!safety.ledger_mutation)} · fifo_mutation={String(!!safety.fifo_mutation)} ·
            signing={String(!!safety.signing)} · broadcast={String(!!safety.broadcast)}
          </div>

          <details className="utt-rh-accounting-section">
            <summary>Inspect complete preview payload</summary>
            <pre className="utt-rh-accounting-json">{JSON.stringify(
              hideTableData ? redactSensitive(preview) : preview,
              null,
              2,
            )}</pre>
          </details>
        </>
      ) : null}
    </div>
  );
}
