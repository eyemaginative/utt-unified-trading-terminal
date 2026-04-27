// frontend/src/features/registry/TokenRegistryWindow.jsx
import React, { useCallback, useEffect, useMemo, useState } from "react";

const LS_SOLANA_DETECTED_TOKENS_KEY = "utt_solana_detected_tokens_v1";

export default function TokenRegistryWindow({ apiBase = "", onClose }) {
  const API_BASE = String(apiBase || "").trim() || "";

  const [chain, setChain] = useState("solana");
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState(null);
  const [suggestions, setSuggestions] = useState([]);
  const [dismissed, setDismissed] = useState(() => new Set());

  // Add form
  const [symbol, setSymbol] = useState("");
  const [address, setAddress] = useState("");
  const [decimals, setDecimals] = useState("");
  const [venue, setVenue] = useState(""); // optional override scope (blank = global)

  // Inline edit
  const [editId, setEditId] = useState(null);
  const [editRow, setEditRow] = useState({ symbol: "", address: "", decimals: "", venue: "" });

  const loadSuggestions = useCallback(() => {
    try {
      const raw = localStorage.getItem(LS_SOLANA_DETECTED_TOKENS_KEY) || "[]";
      const arr = JSON.parse(raw);
      setSuggestions(Array.isArray(arr) ? arr : []);
    } catch {
      setSuggestions([]);
    }
  }, []);

  const existingAddressSet = useMemo(() => {
    const s = new Set();
    for (const row of items || []) {
      const a = String(row?.address || "").trim();
      if (!a) continue;
      s.add(a);
      s.add(a.toLowerCase());
    }
    return s;
  }, [items]);

  const visibleSuggestions = useMemo(() => {
    return (suggestions || []).filter((it) => {
      const a = String(it?.address || "").trim();
      if (!a) return false;
      if (dismissed.has(a)) return false;
      if (existingAddressSet.has(a) || existingAddressSet.has(a.toLowerCase())) return false;
      return true;
    });
  }, [suggestions, dismissed, existingAddressSet]);

  const canAdd = useMemo(() => {
    const s = String(symbol || "").trim();
    const a = String(address || "").trim();
    const d = String(decimals || "").trim();
    if (!s || !a || !d) return false;
    const di = Number(d);
    if (!Number.isFinite(di) || di < 0 || di > 18) return false;
    return true;
  }, [symbol, address, decimals]);

  const load = useCallback(async () => {
    setLoading(true);
    setErr(null);
    try {
      const url = `${API_BASE}/api/token_registry?chain=${encodeURIComponent(chain)}`;
      const r = await fetch(url, { method: "GET", headers: { accept: "application/json" } });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      const arr = Array.isArray(j?.items) ? j.items : [];
      setItems(arr);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [API_BASE, chain]);

  useEffect(() => {
    load();
    loadSuggestions();
  }, [load, loadSuggestions]);

  useEffect(() => {
    const onFocus = () => loadSuggestions();
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, [loadSuggestions]);

  const useSuggestion = useCallback((sug) => {
    setSymbol(String(sug?.symbol || "").trim());
    setAddress(String(sug?.address || "").trim());
    setDecimals(sug?.decimals == null ? "" : String(sug.decimals));
    setVenue(String(sug?.venue || "").trim());
  }, []);

  const addSuggestion = useCallback(async (sug) => {
    const payload = {
      chain,
      symbol: String(sug?.symbol || "").trim(),
      address: String(sug?.address || "").trim(),
      decimals: Number(sug?.decimals),
    };
    const v = String(sug?.venue || "").trim();
    if (v) payload.venue = v;
    if (!payload.symbol || !payload.address || !Number.isFinite(payload.decimals)) {
      useSuggestion(sug);
      setErr("Suggestion is incomplete. Review and save manually.");
      return;
    }
    setSaving(true);
    setErr(null);
    try {
      const r = await fetch(`${API_BASE}/api/token_registry`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
      setDismissed((prev) => { const n = new Set(prev); n.add(payload.address); return n; });
      await load();
      loadSuggestions();
    } catch (e) {
      setErr(String(e?.message || e));
      useSuggestion(sug);
    } finally {
      setSaving(false);
    }
  }, [API_BASE, chain, load, loadSuggestions, useSuggestion]);

  const onAdd = useCallback(async () => {
    if (!canAdd) return;
    setSaving(true);
    setErr(null);
    try {
      const payload = {
        chain,
        symbol: String(symbol || "").trim(),
        address: String(address || "").trim(),
        decimals: Number(String(decimals || "").trim()),
      };
      const v = String(venue || "").trim();
      if (v) payload.venue = v;

      const r = await fetch(`${API_BASE}/api/token_registry`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);

      setSymbol("");
      setAddress("");
      setDecimals("");
      setVenue("");
      await load();
      loadSuggestions();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }, [API_BASE, canAdd, chain, symbol, address, decimals, venue, load]);

  const startEdit = useCallback((row) => {
    setEditId(row?.id || null);
    setEditRow({
      symbol: String(row?.symbol || ""),
      address: String(row?.address || ""),
      decimals: String(row?.decimals ?? ""),
      venue: String(row?.venue || ""),
    });
  }, []);

  const cancelEdit = useCallback(() => {
    setEditId(null);
    setEditRow({ symbol: "", address: "", decimals: "", venue: "" });
  }, []);

  const saveEdit = useCallback(async () => {
    const id = editId;
    if (!id) return;

    const s = String(editRow.symbol || "").trim();
    const a = String(editRow.address || "").trim();
    const d = Number(String(editRow.decimals || "").trim());
    if (!s || !a || !Number.isFinite(d) || d < 0 || d > 18) {
      setErr("Edit: symbol/address/decimals invalid.");
      return;
    }

    setSaving(true);
    setErr(null);
    try {
      const payload = {
        chain,
        symbol: s,
        address: a,
        decimals: d,
      };
      const v = String(editRow.venue || "").trim();
      if (v) payload.venue = v;

      const r = await fetch(`${API_BASE}/api/token_registry/${encodeURIComponent(String(id))}`, {
        method: "PUT",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => null);
      if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);

      cancelEdit();
      await load();
      loadSuggestions();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }, [API_BASE, chain, editId, editRow, load, cancelEdit]);

  const delRow = useCallback(
    async (row) => {
      const id = row?.id;
      if (!id) return;
      const ok = confirm(`Delete token mapping?\n\n${row?.symbol} (${row?.chain})\n${row?.address}`);
      if (!ok) return;

      setSaving(true);
      setErr(null);
      try {
        const r = await fetch(`${API_BASE}/api/token_registry/${encodeURIComponent(String(id))}`, {
          method: "DELETE",
          headers: { accept: "application/json" },
        });
        const j = await r.json().catch(() => null);
        if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
        await load();
        loadSuggestions();
      } catch (e) {
        setErr(String(e?.message || e));
      } finally {
        setSaving(false);
      }
    },
    [API_BASE, load]
  );

  const testResolve = useCallback(
    async (sym) => {
      const a = String(sym || "").trim();
      if (!a) return;
      setErr(null);
      try {
        const r = await fetch(`${API_BASE}/api/solana_dex/resolve?asset=${encodeURIComponent(a)}`, {
          method: "GET",
          headers: { accept: "application/json" },
        });
        const j = await r.json().catch(() => null);
        if (!r.ok) throw new Error(j?.detail ? JSON.stringify(j.detail) : `HTTP ${r.status}`);
        alert(`Resolved:\n\nasset=${j?.asset}\nmint=${j?.mint}\ndecimals=${j?.decimals}`);
      } catch (e) {
        setErr(String(e?.message || e));
      }
    },
    [API_BASE]
  );

  return (
    <div style={{ color: "var(--utt-text, #e9eef7)" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center", marginBottom: 10 }}>
        <div style={{ fontWeight: 800, fontSize: 14 }}>Token / Symbol Registry</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <select value={chain} onChange={(e) => setChain(e.target.value)} style={selectStyle}>
            <option value="solana">solana</option>
          </select>
          <button type="button" onClick={load} style={btnStyle} disabled={loading}>
            {loading ? "Loading…" : "Refresh"}
          </button>
          {onClose && (
            <button type="button" onClick={onClose} style={btnStyle}>
              Close
            </button>
          )}
        </div>
      </div>

      {visibleSuggestions.length ? (
        <div style={panelStyle}>
          <div style={{ fontWeight: 700, marginBottom: 8 }}>Detected suggestions</div>
          <div style={{ fontSize: 12, opacity: 0.8, marginBottom: 8 }}>Detected from recent Solana order rows. One-click add when symbol + decimals are available, or prefill the form to review.</div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0 }}>
              <thead>
                <tr>
                  <th style={thStyle}>Symbol</th>
                  <th style={thStyle}>Address/Mint</th>
                  <th style={thStyle}>Decimals</th>
                  <th style={thStyle}>Source</th>
                  <th style={{ ...thStyle, width: 240 }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {visibleSuggestions.slice(0, 50).map((sug) => {
                  const ready = !!String(sug?.symbol || "").trim() && Number.isFinite(Number(sug?.decimals));
                  return (
                    <tr key={String(sug?.address || Math.random())}>
                      <td style={tdStyle}><span style={{ fontWeight: 700, opacity: sug?.symbol ? 1 : 0.55 }}>{sug?.symbol || "(symbol unknown)"}</span></td>
                      <td style={tdStyle}><code style={codeStyle} title={String(sug?.address || "")}>{String(sug?.address || "")}</code></td>
                      <td style={tdStyle}><span style={{ opacity: Number.isFinite(Number(sug?.decimals)) ? 1 : 0.55 }}>{Number.isFinite(Number(sug?.decimals)) ? Number(sug.decimals) : "—"}</span></td>
                      <td style={tdStyle}><span style={{ opacity: 0.8 }}>{String(sug?.sourceSymbol || "") || "—"}</span></td>
                      <td style={tdStyle}>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <button type="button" style={btnStyle} onClick={() => useSuggestion(sug)}>Use</button>
                          <button type="button" style={btnStyle} onClick={() => addSuggestion(sug)} disabled={!ready || saving}>{saving ? "Saving…" : "Add"}</button>
                          <button type="button" style={btnStyle} onClick={() => setDismissed((prev) => { const n = new Set(prev); n.add(String(sug?.address || "")); return n; })}>Dismiss</button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}

      <div style={panelStyle}>
        <div style={{ fontWeight: 700, marginBottom: 8 }}>Add token</div>
        <div style={{ display: "grid", gridTemplateColumns: "160px 1fr 110px 160px 110px", gap: 8, alignItems: "center" }}>
          <input value={symbol} onChange={(e) => setSymbol(e.target.value)} placeholder="SYMBOL (e.g. UTTT)" style={inputStyle} />
          <input value={address} onChange={(e) => setAddress(e.target.value)} placeholder="Mint / contract address" style={inputStyle} />
          <input value={decimals} onChange={(e) => setDecimals(e.target.value)} placeholder="decimals" style={inputStyle} />
          <input value={venue} onChange={(e) => setVenue(e.target.value)} placeholder="venue override (optional)" style={inputStyle} />
          <button type="button" onClick={onAdd} style={btnStyle} disabled={!canAdd || saving}>
            {saving ? "Saving…" : "Add"}
          </button>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, opacity: 0.75 }}>
          Tip: leave “venue override” blank for global entries. Use it later only if a specific venue needs a decimals/mint override.
        </div>
      </div>

      {err && <div style={{ ...panelStyle, borderColor: "rgba(255,120,120,0.35)", background: "rgba(40,10,10,0.45)" }}>{err}</div>}

      <div style={{ marginTop: 10 }}>
        <div style={{ fontWeight: 700, marginBottom: 8 }}>Mappings</div>

        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0 }}>
            <thead>
              <tr>
                <th style={thStyle}>Symbol</th>
                <th style={thStyle}>Address/Mint</th>
                <th style={thStyle}>Decimals</th>
                <th style={thStyle}>Venue</th>
                <th style={{ ...thStyle, width: 240 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {(items || []).map((row) => {
                const isEdit = String(editId || "") === String(row?.id || "");
                return (
                  <tr key={row.id} style={{ borderTop: "1px solid rgba(255,255,255,0.08)" }}>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.symbol} onChange={(e) => setEditRow((p) => ({ ...p, symbol: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span style={{ fontWeight: 700 }}>{row.symbol}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.address} onChange={(e) => setEditRow((p) => ({ ...p, address: e.target.value }))} style={inputStyle} />
                      ) : (
                        <code style={codeStyle}>{row.address}</code>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.decimals} onChange={(e) => setEditRow((p) => ({ ...p, decimals: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span>{row.decimals}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {isEdit ? (
                        <input value={editRow.venue} onChange={(e) => setEditRow((p) => ({ ...p, venue: e.target.value }))} style={inputStyle} />
                      ) : (
                        <span style={{ opacity: row.venue ? 1 : 0.55 }}>{row.venue || "—"}</span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {!isEdit ? (
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <button type="button" style={btnStyle} onClick={() => testResolve(row.symbol)}>
                            Test resolve
                          </button>
                          <button type="button" style={btnStyle} onClick={() => startEdit(row)}>
                            Edit
                          </button>
                          <button type="button" style={dangerBtnStyle} onClick={() => delRow(row)}>
                            Delete
                          </button>
                        </div>
                      ) : (
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <button type="button" style={btnStyle} onClick={saveEdit} disabled={saving}>
                            {saving ? "Saving…" : "Save"}
                          </button>
                          <button type="button" style={btnStyle} onClick={cancelEdit} disabled={saving}>
                            Cancel
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
              {!items?.length && (
                <tr>
                  <td colSpan={5} style={{ ...tdStyle, opacity: 0.7 }}>
                    No mappings yet. Add a symbol + mint + decimals above.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={{ marginTop: 8, fontSize: 12, opacity: 0.7 }}>
          Backend: <code style={codeStyle}>/api/token_registry</code>
        </div>
      </div>
    </div>
  );
}

const panelStyle = {
  marginTop: 10,
  padding: 10,
  borderRadius: 12,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "rgba(255,255,255,0.04)",
};

const inputStyle = {
  width: "100%",
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,0.12)",
  background: "rgba(0,0,0,0.25)",
  color: "var(--utt-text, #e9eef7)",
  outline: "none",
  boxSizing: "border-box",
};

const selectStyle = {
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,0.12)",
  background: "rgba(0,0,0,0.25)",
  color: "var(--utt-text, #e9eef7)",
  outline: "none",
};

const btnStyle = {
  fontSize: 12,
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,0.16)",
  background: "rgba(255,255,255,0.06)",
  color: "var(--utt-text, #e9eef7)",
  cursor: "pointer",
};

const dangerBtnStyle = {
  ...btnStyle,
  border: "1px solid rgba(255,120,120,0.35)",
  background: "rgba(120,30,30,0.25)",
};

const thStyle = {
  textAlign: "left",
  fontSize: 12,
  padding: "8px 10px",
  borderBottom: "1px solid rgba(255,255,255,0.10)",
  opacity: 0.85,
  whiteSpace: "nowrap",
};

const tdStyle = {
  fontSize: 12,
  padding: "10px",
  borderBottom: "1px solid rgba(255,255,255,0.06)",
  verticalAlign: "top",
};

const codeStyle = {
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
  fontSize: 11,
  opacity: 0.9,
};
