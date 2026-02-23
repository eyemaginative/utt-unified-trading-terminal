// frontend/src/features/wallets/WalletAddressesWindow.jsx
import React, { useEffect, useMemo, useState } from "react";

/**
 * WalletAddressesWindow (MVP)
 *
 * Backend endpoints (current):
 *  - GET    /api/wallet_addresses?asset=&network=&limit=
 *  - POST   /api/wallet_addresses
 *  - DELETE /api/wallet_addresses/{address_id}
 *  - GET    /api/wallet_addresses/balances/latest?with_prices=1&limit=
 *  - POST   /api/wallet_addresses/balances/refresh
 *  - POST   /api/wallet_addresses/tx/ingest
 *
 * Notes:
 *  - This is intentionally balances/snapshots-focused (Blockchair polling),
 *    because tx-ingestion / onchain networks is the next backend step.
 *
 * Props (optional):
 *  - apiBase: string (e.g. "http://localhost:8000") or "" for same-origin
 *  - hideTableData: boolean (redacts addresses)
 *  - onClose: () => void
 */
export default function WalletAddressesWindow({ apiBase = "", hideTableData = false, onClose }) {
  const [tab, setTab] = useState("addresses"); // addresses | balances
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const [addresses, setAddresses] = useState([]);
  const [balances, setBalances] = useState([]);

  const [flt, setFlt] = useState({
    asset: "",
    wallet_id: "",
    network: "",
    limit: 500,
  });

  const [form, setForm] = useState({
    asset: "BTC",
    wallet_id: "",
    network: "mainnet",
    address: "",
    label: "",
    owner_scope: "user",
  });

  const [editingId, setEditingId] = useState(null);

  const [withPrices, setWithPrices] = useState(true);
  const [balLimit, setBalLimit] = useState(2000);

  // Balances refresh debug/status
  const [balLastRefresh, setBalLastRefresh] = useState(null);

  // Tx ingest → ledger controls
  const [txWriteLedger, setTxWriteLedger] = useState(true);
  const [txLimitPerAddress, setTxLimitPerAddress] = useState(200);
  const [txLastResult, setTxLastResult] = useState(null);

  const txStats = useMemo(() => deriveTxStats(txLastResult), [txLastResult]);

  const redacted = (v) => (hideTableData ? "••••••••" : v);


  function _asInt(v) {
    const n = Number(v);
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }

  function _asCount(v, fallback = null) {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }

  function deriveTxStats(res) {
    if (!res) return null;

    // Accept multiple backend shapes; prefer explicit fields when present.
    const linkedDeposits =
      res.linked_deposits ??
      res.deposits_linked ??
      res.depositsLinked ??
      res?.linked?.deposits ??
      res?.linked?.deposit ??
      res?.ledger?.linked_deposits ??
      res?.ledger?.deposits;

    const linkedWithdrawals =
      res.linked_withdrawals ??
      res.withdrawals_linked ??
      res.withdrawalsLinked ??
      res?.linked?.withdrawals ??
      res?.linked?.withdrawal ??
      res?.ledger?.linked_withdrawals ??
      res?.ledger?.withdrawals;

    const pending =
      res.pending ??
      res.pending_rows ??
      res.pendingCount ??
      res?.backlog?.pending ??
      res?.stats?.pending;

    const rawByReason =
      res.skipped_by_reason ??
      res.skips_by_reason ??
      res.skip_reasons ??
      res?.skip?.by_reason ??
      res?.skips?.by_reason ??
      res?.stats?.skipped_by_reason;

    let skippedByReason = null;
    if (rawByReason && typeof rawByReason === "object") {
      if (Array.isArray(rawByReason)) {
        // [{reason, n}] or [{k,v}]
        const m = {};
        for (const r of rawByReason) {
          const reason = r?.reason ?? r?.k ?? r?.key ?? r?.name;
          const n = r?.n ?? r?.count ?? r?.v ?? r?.value;
          if (reason != null) m[String(reason)] = _asCount(n, 0);
        }
        skippedByReason = m;
      } else {
        // {reason: n}
        skippedByReason = {};
        for (const [k, v] of Object.entries(rawByReason)) skippedByReason[String(k)] = _asCount(v, 0);
      }
    }

    let skippedTotal =
      res.skipped_total ??
      res.skipped ??
      res.skips_total ??
      res?.skip?.total ??
      res?.stats?.skipped_total;

    if (skippedTotal == null && skippedByReason) {
      skippedTotal = Object.values(skippedByReason).reduce((a, b) => a + (Number.isFinite(Number(b)) ? Number(b) : 0), 0);
    }

    const out = {
      linkedDeposits: _asInt(linkedDeposits),
      linkedWithdrawals: _asInt(linkedWithdrawals),
      skippedTotal: _asInt(skippedTotal),
      skippedByReason,
      pending: _asInt(pending),
    };

    // If everything is missing, return null so UI can hide summary.
    const any =
      out.linkedDeposits != null ||
      out.linkedWithdrawals != null ||
      out.skippedTotal != null ||
      out.pending != null ||
      (out.skippedByReason && Object.keys(out.skippedByReason).length > 0);

    return any ? out : null;
  }


  function normBase(b) {
    if (!b) return "";
    return b.endsWith("/") ? b.slice(0, -1) : b;
  }

  function _protocolHint(url) {
    try {
      const pageProto = window?.location?.protocol;
      const u = new URL(url, window.location.href);
      if (pageProto === "https:" && u.protocol === "http:") {
        return " (Mixed content: https page -> http API blocked by browser. Use https API or serve frontend over http.)";
      }
      return "";
    } catch {
      return "";
    }
  }

  async function api(path, opts = {}) {
    const base = normBase(apiBase);
    const url = `${base}${path}`;
    const method = opts.method || "GET";

    const headers = {
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    };

    const init = { method, headers };
    if (opts.body !== undefined) init.body = JSON.stringify(opts.body);

    let res;
    let text = "";
    try {
      res = await fetch(url, init);
      text = await res.text();
    } catch (e) {
      // This is where "Failed to fetch" comes from (CORS, mixed content, network down, etc.)
      const hint = _protocolHint(url);
      console.error("[WalletAddressesWindow.api] fetch threw:", { url, method, init, error: e });
      const msg = (e && e.message) ? e.message : String(e);
      throw new Error(`Fetch failed: ${method} ${url}${hint} :: ${msg}`);
    }

    let data;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }

    if (!res.ok) {
      const msg =
        (data && (data.detail || data.error || data.message)) ||
        `HTTP ${res.status} ${res.statusText}`;
      throw new Error(`${msg} (${method} ${url})`);
    }
    return data;
  }

  const listQuery = useMemo(() => {
    const p = new URLSearchParams();
    if (flt.asset?.trim()) p.set("asset", flt.asset.trim());
    if (flt.wallet_id?.trim()) p.set("wallet_id", flt.wallet_id.trim());
    if (flt.network?.trim()) p.set("network", flt.network.trim());
    p.set("limit", String(flt.limit || 500));
    const qs = p.toString();
    return qs ? `?${qs}` : "";
  }, [flt]);

  async function loadAddresses() {
    setBusy(true);
    setErr("");
    try {
      const rows = await api(`/api/wallet_addresses${listQuery}`);
      setAddresses(Array.isArray(rows) ? rows : rows?.items || []);
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  async function loadBalances() {
    setBusy(true);
    setErr("");
    try {
      const qs = new URLSearchParams();
      qs.set("with_prices", withPrices ? "1" : "0");
      qs.set("limit", String(balLimit || 2000));
      const res = await api(`/api/wallet_addresses/balances/latest?${qs.toString()}`);
      const items = Array.isArray(res) ? res : res?.items || [];
      setBalances(items);
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  async function refreshBalances(ids = null) {
    setBusy(true);
    setErr("");
    try {
      const body = ids ? { ids } : {};
      const res = await api(`/api/wallet_addresses/balances/refresh`, { method: "POST", body });

      // surface refresh result in the UI
      setBalLastRefresh(res || null);

      const refreshed = Number(res?.refreshed || 0);
      const errors = Array.isArray(res?.errors) ? res.errors : [];
      if (errors.length) {
        console.error("[wallet_balances_refresh] errors:", errors);
        setErr(`Balances refresh completed: refreshed ${refreshed}, errors ${errors.length} (see console).`);
      }

      // auto reload latest balances
      await loadBalances();
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  async function ingestTx(ids = null) {
    setBusy(true);
    setErr("");
    setTxLastResult(null);
    try {
      const body = {
        limit_per_address: Number(txLimitPerAddress || 200),
        write_ledger: txWriteLedger ? 1 : 0,
      };
      if (Array.isArray(ids) && ids.length) body.ids = ids;

      const res = await api(`/api/wallet_addresses/tx/ingest`, { method: "POST", body });
      setTxLastResult(res || { ok: true });

      if (tab === "balances") {
        await loadBalances();
      } else {
        await loadAddresses();
      }
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  async function createAddress() {
    setBusy(true);
    setErr("");
    try {
      const payload = {
        asset: String(form.asset || "").trim().toUpperCase(),
        wallet_id: String(form.wallet_id || "").trim() || null,
        network: String(form.network || "").trim(),
        address: String(form.address || "").trim(),
        label: String(form.label || "").trim() || null,
        owner_scope: String(form.owner_scope || "user").trim().toLowerCase(),
      };

      if (!payload.address) throw new Error("Address is required.");
      if (!payload.asset) throw new Error("Asset is required.");
      if (!payload.network) throw new Error("Network is required.");

      if (editingId) {
        await api(`/api/wallet_addresses/${editingId}`, { method: "PATCH", body: payload });
      } else {
        await api(`/api/wallet_addresses`, { method: "POST", body: payload });
      }
      await loadAddresses();
      setForm((p) => ({ ...p, address: "", label: "" }));
      setEditingId(null);
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  function beginEdit(row) {
    setEditingId(row.id);
    setForm({
      asset: row.asset || "",
      wallet_id: row.wallet_id || "",
      network: row.network || "",
      address: row.address || "",
      label: row.label || "",
      owner_scope: row.owner_scope || "user",
    });
  }

  async function deleteAddress(id) {
    if (!window.confirm("Delete this wallet address record?")) return;
    setBusy(true);
    setErr("");
    try {
      await api(`/api/wallet_addresses/${id}`, { method: "DELETE" });
      await loadAddresses();
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    loadAddresses();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div style={{ padding: 12, fontFamily: "system-ui, sans-serif", fontSize: 13 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <div style={{ fontWeight: 700, fontSize: 16 }}>Wallet Addresses</div>
        <div style={{ opacity: 0.7 }}>({tab})</div>
        <div style={{ flex: 1 }} />
        <button onClick={() => (tab === "balances" ? loadBalances() : loadAddresses())} disabled={busy}>
          Refresh
        </button>
        {typeof onClose === "function" && <button onClick={onClose}>Close</button>}
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 10 }}>
        <button onClick={() => setTab("addresses")} disabled={tab === "addresses"}>
          Addresses
        </button>
        <button
          onClick={() => {
            setTab("balances");
            loadBalances();
          }}
          disabled={tab === "balances"}
        >
          Balances
        </button>
        <div style={{ flex: 1 }} />
        {busy && <div style={{ opacity: 0.8 }}>Working…</div>}
      </div>

      {err && (
        <div
          style={{
            marginBottom: 10,
            padding: 8,
            background: "#ffecec",
            border: "1px solid #ffb3b3",
            borderRadius: 8,
            color: "#111",
            fontWeight: 600,
          }}
        >
          <b>Error:</b> {err}
        </div>
      )}

      {tab === "addresses" && (
        <>
          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Add Wallet Address (MVP)</div>

            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>Asset</label>
              <input value={form.asset} onChange={(e) => setForm((p) => ({ ...p, asset: e.target.value.toUpperCase() }))} />

              <label>Venue</label>
              <input
                placeholder="e.g. robinhood, dex-trade (blank = self-custody)"
                value={form.wallet_id}
                onChange={(e) => setForm((p) => ({ ...p, wallet_id: e.target.value }))}
              />

              <label>Network</label>
              <input value={form.network} onChange={(e) => setForm((p) => ({ ...p, network: e.target.value }))} />

              <label>Address</label>
              <input value={form.address} onChange={(e) => setForm((p) => ({ ...p, address: e.target.value }))} />

              <label>Label</label>
              <input value={form.label} onChange={(e) => setForm((p) => ({ ...p, label: e.target.value }))} />

              <label>Owner scope</label>
              <input value={form.owner_scope} onChange={(e) => setForm((p) => ({ ...p, owner_scope: e.target.value }))} />
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center" }}>
              <button onClick={createAddress} disabled={busy || !String(form.address || "").trim()}>
                {editingId ? "Save" : "Create"}
              </button>
              {editingId ? (
                <button
                  onClick={() => {
                    setEditingId(null);
                    setForm({ asset: "BTC", wallet_id: "", network: "mainnet", address: "", label: "", owner_scope: "user" });
                    setErr("");
                  }}
                  disabled={busy}
                >
                  Cancel
                </button>
              ) : null}
              <div style={{ opacity: 0.75 }}>
                Next step: extend to <b>Venue</b> + <b>Purpose</b> + <b>Chain/Network registry</b> + <b>tx ingest</b> once backend is updated.
              </div>
            </div>
          </div>

          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Tx Ingest → Deposits/Withdrawals</div>

            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>Write to ledger</label>
              <input type="checkbox" checked={txWriteLedger} onChange={(e) => setTxWriteLedger(e.target.checked)} />

              <label>Limit / address</label>
              <input
                type="number"
                value={txLimitPerAddress}
                onChange={(e) => setTxLimitPerAddress(Number(e.target.value || 200))}
                min={1}
                max={5000}
              />
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center" }}>
              <button
                onClick={() => {
                  if (!window.confirm("Run on-chain tx ingest for ALL wallet addresses?")) return;
                  ingestTx(null);
                }}
                disabled={busy}
              >
                Ingest txs (all)
              </button>
              <div style={{ opacity: 0.75 }}>
                Uses <code>/api/wallet_addresses/tx/ingest</code>. Enforces policy (skip coinbase; deposits-only robinhood/dex-trade; self-custody both).
              </div>
            </div>

            {txLastResult ? (
              <div style={{ marginTop: 10, padding: 8, background: "#111", border: "1px solid #333", borderRadius: 8, color: "#eee" }}>
                <div style={{ fontWeight: 700, marginBottom: 4 }}>Last ingest result</div>
                {txStats ? (
                  <div style={{ marginBottom: 8, padding: 8, background: "#171717", border: "1px solid #2a2a2a", borderRadius: 8 }}>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }}>
                      <div>
                        Linked deposits (selected addresses): <b>{txStats.linkedDeposits != null ? txStats.linkedDeposits : "—"}</b>
                      </div>
                      <div>
                        Linked withdrawals (selected addresses): <b>{txStats.linkedWithdrawals != null ? txStats.linkedWithdrawals : "—"}</b>
                      </div>
                      <div>
                        Skipped (selected addresses): <b>{txStats.skippedTotal != null ? txStats.skippedTotal : "—"}</b>
                      </div>
                      <div>
                        Pending (selected addresses): <b>{txStats.pending != null ? txStats.pending : "—"}</b>
                      </div>
                    </div>

                    {txStats.skippedByReason && Object.keys(txStats.skippedByReason).length > 0 ? (
                      <div style={{ marginTop: 8, opacity: 0.95 }}>
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>Skipped by reason</div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                          {Object.entries(txStats.skippedByReason)
                            .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
                            .map(([reason, n]) => (
                              <div
                                key={reason}
                                style={{
                                  padding: "2px 8px",
                                  borderRadius: 999,
                                  border: "1px solid #333",
                                  background: "#0f0f0f",
                                  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
                                  fontSize: 12,
                                }}
                              >
                                {reason}: <b>{String(n)}</b>
                              </div>
                            ))}
                        </div>
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <div style={{ marginBottom: 8, opacity: 0.85 }}>
                    Counters: <span style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" }}>n/a</span>{" "}
                    (selected addresses; backend response did not include linked/skipped/pending fields)
                  </div>
                )}
                <pre style={{ margin: 0, whiteSpace: "pre-wrap", color: "#eee" }}>{JSON.stringify(txLastResult, null, 2)}</pre>
              </div>
            ) : null}
          </div>

          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Filters</div>
            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>Asset</label>
              <input value={flt.asset} onChange={(e) => setFlt((p) => ({ ...p, asset: e.target.value.toUpperCase() }))} />

              <label>Venue</label>
              <input
                placeholder="e.g. robinhood, dex-trade"
                value={flt.wallet_id}
                onChange={(e) => setFlt((p) => ({ ...p, wallet_id: e.target.value }))}
              />

              <label>Network</label>
              <input value={flt.network} onChange={(e) => setFlt((p) => ({ ...p, network: e.target.value }))} />

              <label>Limit</label>
              <input
                type="number"
                value={flt.limit}
                onChange={(e) => setFlt((p) => ({ ...p, limit: Number(e.target.value || 500) }))}
                min={1}
                max={2000}
              />
            </div>

            <div style={{ marginTop: 10 }}>
              <button onClick={loadAddresses} disabled={busy}>
                Apply
              </button>
            </div>
          </div>

          <div style={{ padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Wallet Addresses</div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ borderCollapse: "collapse", width: "100%" }}>
                <thead>
                  <tr>
                    {["id", "asset", "venue", "network", "address", "label", "owner_scope", "created_at", "actions"].map((h) => (
                      <th key={h} style={{ textAlign: "left", borderBottom: "1px solid #ccc", padding: 6 }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(addresses || []).map((a) => (
                    <tr key={a.id}>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.id}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.asset || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.wallet_id || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.network || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee", fontFamily: "monospace" }}>{redacted(a.address || "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.label || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.owner_scope || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.created_at || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>
                        <button
                          onClick={() => {
                            if (!window.confirm("Refresh balance snapshot for this address?")) return;
                            refreshBalances([a.id]);
                          }}
                          disabled={busy}
                          style={{ marginRight: 6 }}
                        >
                          Balance
                        </button>
                        <button
                          onClick={() => {
                            if (!window.confirm("Ingest on-chain tx history for this address?")) return;
                            ingestTx([a.id]);
                          }}
                          disabled={busy}
                          style={{ marginRight: 6 }}
                        >
                          Txs
                        </button>
                        <button onClick={() => beginEdit(a)} disabled={busy} style={{ marginRight: 6 }}>
                          Edit
                        </button>
                        <button onClick={() => deleteAddress(a.id)} disabled={busy}>
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                  {(addresses || []).length === 0 && (
                    <tr>
                      <td colSpan={9} style={{ padding: 8, opacity: 0.7 }}>
                        No addresses yet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {tab === "balances" && (
        <>
          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Balances Controls</div>
            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>With prices</label>
              <input type="checkbox" checked={withPrices} onChange={(e) => setWithPrices(e.target.checked)} />

              <label>Limit</label>
              <input
                type="number"
                value={balLimit}
                onChange={(e) => setBalLimit(Number(e.target.value || 2000))}
                min={1}
                max={5000}
              />
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <button onClick={loadBalances} disabled={busy}>
                Load latest
              </button>
              <button onClick={() => refreshBalances(null)} disabled={busy}>
                Refresh all (explorer)
              </button>

              {balLastRefresh ? (
                <div style={{ opacity: 0.85 }}>
                  Last refresh: <b>{Number(balLastRefresh.refreshed || 0)}</b> refreshed •{" "}
                  <b>{Array.isArray(balLastRefresh.errors) ? balLastRefresh.errors.length : 0}</b> errors
                </div>
              ) : (
                <div style={{ opacity: 0.75 }}>Explorer refresh may take time per address.</div>
              )}
            </div>

            {balLastRefresh && Array.isArray(balLastRefresh.errors) && balLastRefresh.errors.length > 0 ? (
              <div style={{ marginTop: 10, paddingTop: 10, borderTop: "1px solid #eee" }}>
                <div style={{ fontWeight: 600, marginBottom: 6, opacity: 0.9 }}>Refresh errors</div>
                <div style={{ maxHeight: 140, overflow: "auto", fontSize: 12, opacity: 0.95 }}>
                  {balLastRefresh.errors.map((er, idx) => {
                    const asset = er?.asset || "";
                    const network = er?.network || "";
                    const addr = er?.address || "";
                    const msg = er?.error || er?.message || er?.detail || JSON.stringify(er);
                    return (
                      <div key={(er?.id || "") + idx} style={{ marginBottom: 8 }}>
                        <div>
                          <b>{asset}</b> {network ? `(${network})` : ""} {addr ? `• ${redacted(addr)}` : ""}
                        </div>
                        <div style={{ opacity: 0.85, marginTop: 2, whiteSpace: "pre-wrap" }}>{String(msg)}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : null}
          </div>

          <div style={{ padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Latest Balances</div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ borderCollapse: "collapse", width: "100%" }}>
                <thead>
                  <tr>
                    {["id", "asset", "network", "address", "label", "balance", "usd_price", "usd_value", "fetched_at"].map((h) => (
                      <th key={h} style={{ textAlign: "left", borderBottom: "1px solid #ccc", padding: 6 }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(balances || []).map((b) => (
                    <tr key={`${b.id}:${b.fetched_at || ""}`}>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.id}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.asset || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.network || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee", fontFamily: "monospace" }}>{redacted(b.address || "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.label || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.balance ?? "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.usd_price ?? "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.usd_value ?? "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.fetched_at || ""}</td>
                    </tr>
                  ))}
                  {(balances || []).length === 0 && (
                    <tr>
                      <td colSpan={9} style={{ padding: 8, opacity: 0.7 }}>
                        No balance snapshots yet (try “Refresh all” first).
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
