// frontend/src/pages/BalancesOnlyPage.jsx
import { useEffect, useMemo, useRef, useState } from "react";
import { API_BASE, refreshBalances, getLatestBalances } from "../lib/api";

function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

function toNum(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function fmt2(n) {
  const x = toNum(n);
  if (x === null) return "";
  return x.toFixed(2);
}

function fmt8(n) {
  const x = toNum(n);
  if (x === null) return "";
  return x.toFixed(8);
}

export default function BalancesOnlyPage() {
  const [tab, setTab] = useState("venues"); // venues | wallets

  // Venues balances viewer
  const [items, setItems] = useState([]);
  const [withPrices, setWithPrices] = useState(true);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [intervalSec, setIntervalSec] = useState(300);
  const [busy, setBusy] = useState(false);
  const timerRef = useRef(null);

  // Custom wallets manager
  const [waOwnerScope, setWaOwnerScope] = useState("default");
  const [waList, setWaList] = useState([]);
  const [waBalances, setWaBalances] = useState([]);
  const [waBusy, setWaBusy] = useState(false);
  const [waMsg, setWaMsg] = useState("");

  const [newAsset, setNewAsset] = useState("BTC");
  const [newNetwork, setNewNetwork] = useState("BTC");
  const [newAddress, setNewAddress] = useState("");
  const [newLabel, setNewLabel] = useState("");

  const venueTotals = useMemo(() => {
    let totalUsd = 0;
    for (const it of items) {
      if (it && typeof it.total_usd === "number") totalUsd += it.total_usd;
    }
    return totalUsd;
  }, [items]);

  async function loadVenuesLatest() {
    const res = await getLatestBalances(API_BASE, "all", { with_prices: withPrices ? "1" : "0" });
    setItems(res?.items || []);
  }

  async function doRefreshVenues() {
    setBusy(true);
    try {
      await refreshBalances(API_BASE, "all");
      await loadVenuesLatest();
    } finally {
      setBusy(false);
    }
  }

  async function loadWalletAddresses() {
    const url = `${API_BASE}/api/wallet_addresses?owner_scope=${encodeURIComponent(waOwnerScope)}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`wallet_addresses list failed: ${r.status}`);
    const j = await r.json();
    setWaList(Array.isArray(j) ? j : []);
  }

  async function loadWalletBalancesLatest() {
    const url = `${API_BASE}/api/wallet_addresses/balances/latest?owner_scope=${encodeURIComponent(
      waOwnerScope
    )}&with_prices=1`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`wallet balances latest failed: ${r.status}`);
    const j = await r.json();
    setWaBalances(Array.isArray(j?.items) ? j.items : []);
  }

  async function refreshWalletBalances(ids = null) {
    setWaBusy(true);
    setWaMsg("");
    try {
      const r = await fetch(`${API_BASE}/api/wallet_addresses/balances/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          owner_scope: waOwnerScope,
          ids: Array.isArray(ids) && ids.length ? ids : null,
        }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.detail || `refresh failed (${r.status})`);
      await loadWalletBalancesLatest();
      setWaMsg(`Refreshed ${j?.refreshed ?? 0} address(es).`);
    } catch (e) {
      setWaMsg(String(e?.message || e));
    } finally {
      setWaBusy(false);
    }
  }

  async function addWalletAddress(e) {
    e.preventDefault();
    setWaBusy(true);
    setWaMsg("");
    try {
      const asset = String(newAsset || "").trim().toUpperCase();
      const network = String(newNetwork || "").trim().toUpperCase();
      const address = String(newAddress || "").trim();
      const label = String(newLabel || "").trim();

      const r = await fetch(`${API_BASE}/api/wallet_addresses`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          asset,
          network,
          address,
          label: label || null,
          owner_scope: waOwnerScope,
        }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.detail || `create failed (${r.status})`);
      setNewAddress("");
      setNewLabel("");
      await loadWalletAddresses();
      await refreshWalletBalances([j.id]);
      setWaMsg("Address added.");
    } catch (e2) {
      setWaMsg(String(e2?.message || e2));
    } finally {
      setWaBusy(false);
    }
  }

  async function deleteWalletAddress(id) {
    if (!id) return;
    setWaBusy(true);
    setWaMsg("");
    try {
      const r = await fetch(`${API_BASE}/api/wallet_addresses/${encodeURIComponent(id)}`, {
        method: "DELETE",
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.detail || `delete failed (${r.status})`);
      await loadWalletAddresses();
      await loadWalletBalancesLatest();
      setWaMsg("Address deleted.");
    } catch (e) {
      setWaMsg(String(e?.message || e));
    } finally {
      setWaBusy(false);
    }
  }

  // Initial load
  useEffect(() => {
    loadVenuesLatest().catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Auto refresh venues
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = null;
    if (!autoRefresh) return;
    const sec = clampSeconds(intervalSec, 300);
    timerRef.current = setInterval(() => {
      doRefreshVenues().catch(() => {});
    }, sec * 1000);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
      timerRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, intervalSec, withPrices]);

  // Load wallets data when switching to wallets tab or owner scope changes
  useEffect(() => {
    if (tab !== "wallets") return;
    setWaMsg("");
    loadWalletAddresses().catch(() => {});
    loadWalletBalancesLatest().catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, waOwnerScope]);

  return (
    <div style={{ padding: 16, fontFamily: "sans-serif" }}>
      <h2 style={{ margin: 0 }}>Balances</h2>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 8, marginTop: 10, marginBottom: 14 }}>
        <button
          onClick={() => setTab("venues")}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "1px solid var(--utt-border, #444)",
            background: tab === "venues" ? "var(--utt-accent, #2c72ff)" : "transparent",
            color: tab === "venues" ? "#fff" : "inherit",
            cursor: "pointer",
          }}
        >
          Venues
        </button>
        <button
          onClick={() => setTab("wallets")}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "1px solid var(--utt-border, #444)",
            background: tab === "wallets" ? "var(--utt-accent, #2c72ff)" : "transparent",
            color: tab === "wallets" ? "#fff" : "inherit",
            cursor: "pointer",
          }}
        >
          Deposits / Withdrawals Evidence (Custom Wallets)
        </button>
      </div>

      {tab === "venues" && (
        <>
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
            <button onClick={doRefreshVenues} disabled={busy} style={{ padding: "6px 10px" }}>
              {busy ? "Refreshing..." : "Refresh Now"}
            </button>

            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input type="checkbox" checked={withPrices} onChange={(e) => setWithPrices(e.target.checked)} />
              with prices (USD)
            </label>

            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
              auto refresh
            </label>

            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              interval (sec)
              <input
                type="number"
                value={intervalSec}
                onChange={(e) => setIntervalSec(Number(e.target.value))}
                style={{ width: 90 }}
                min={10}
              />
            </label>

            <div style={{ marginLeft: "auto", opacity: 0.85 }}>
              Total USD: <b>${fmt2(venueTotals)}</b>
            </div>
          </div>

          <div style={{ marginTop: 12, overflow: "auto", border: "1px solid var(--utt-border, #333)", borderRadius: 10 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 800 }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "left", padding: 8 }}>Venue</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Asset</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Total</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Available</th>
                  {withPrices && <th style={{ textAlign: "right", padding: 8 }}>Px USD</th>}
                  {withPrices && <th style={{ textAlign: "right", padding: 8 }}>Total USD</th>}
                </tr>
              </thead>
              <tbody>
                {items.map((it, idx) => (
                  <tr key={idx} style={{ borderTop: "1px solid var(--utt-border, #333)" }}>
                    <td style={{ padding: 8 }}>{it.venue}</td>
                    <td style={{ padding: 8 }}>{it.asset}</td>
                    <td style={{ padding: 8, textAlign: "right" }}>{fmt8(it.total)}</td>
                    <td style={{ padding: 8, textAlign: "right" }}>{fmt8(it.available)}</td>
                    {withPrices && <td style={{ padding: 8, textAlign: "right" }}>{fmt2(it.px_usd)}</td>}
                    {withPrices && <td style={{ padding: 8, textAlign: "right" }}>{fmt2(it.total_usd)}</td>}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {tab === "wallets" && (
        <>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              owner scope
              <input
                value={waOwnerScope}
                onChange={(e) => setWaOwnerScope(e.target.value)}
                style={{ width: 140 }}
                placeholder="default"
              />
            </label>

            <button onClick={() => refreshWalletBalances(null)} disabled={waBusy} style={{ padding: "6px 10px" }}>
              {waBusy ? "Working..." : "Refresh On-chain Balances"}
            </button>

            <button
              onClick={() => {
                loadWalletAddresses().catch(() => {});
                loadWalletBalancesLatest().catch(() => {});
              }}
              disabled={waBusy}
              style={{ padding: "6px 10px" }}
            >
              Reload
            </button>

            {waMsg && <div style={{ marginLeft: "auto", opacity: 0.9 }}>{waMsg}</div>}
          </div>

          <div style={{ marginTop: 14, display: "grid", gridTemplateColumns: "1fr", gap: 12 }}>
            {/* Add form */}
            <form
              onSubmit={addWalletAddress}
              style={{
                border: "1px solid var(--utt-border, #333)",
                borderRadius: 10,
                padding: 12,
                display: "grid",
                gridTemplateColumns: "120px 120px 1fr 180px 120px",
                gap: 10,
                alignItems: "center",
                minWidth: 900,
                overflow: "auto",
              }}
            >
              <label style={{ display: "grid", gap: 4 }}>
                Asset
                <select value={newAsset} onChange={(e) => { setNewAsset(e.target.value); setNewNetwork(e.target.value); }}>
                  <option>BTC</option>
                  <option>DOGE</option>
                  <option>DOT</option>
                </select>
              </label>
              <label style={{ display: "grid", gap: 4 }}>
                Network
                <select value={newNetwork} onChange={(e) => setNewNetwork(e.target.value)}>
                  <option>BTC</option>
                  <option>DOGE</option>
                  <option>DOT</option>
                </select>
              </label>
              <label style={{ display: "grid", gap: 4 }}>
                Address
                <input value={newAddress} onChange={(e) => setNewAddress(e.target.value)} placeholder="paste address" />
              </label>
              <label style={{ display: "grid", gap: 4 }}>
                Label (optional)
                <input value={newLabel} onChange={(e) => setNewLabel(e.target.value)} placeholder="e.g., cold-1" />
              </label>
              <button type="submit" disabled={waBusy || !newAddress.trim()} style={{ padding: "6px 10px" }}>
                Add
              </button>
            </form>

            {/* Address list */}
            <div style={{ border: "1px solid var(--utt-border, #333)", borderRadius: 10, padding: 12, overflow: "auto" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                <b>Registered addresses</b>
                <span style={{ opacity: 0.8 }}>{waList.length} total</span>
              </div>

              <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 900 }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: 8 }}>Asset</th>
                    <th style={{ textAlign: "left", padding: 8 }}>Network</th>
                    <th style={{ textAlign: "left", padding: 8 }}>Label</th>
                    <th style={{ textAlign: "left", padding: 8 }}>Address</th>
                    <th style={{ textAlign: "right", padding: 8 }}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {waList.map((a) => (
                    <tr key={a.id} style={{ borderTop: "1px solid var(--utt-border, #333)" }}>
                      <td style={{ padding: 8 }}>{a.asset}</td>
                      <td style={{ padding: 8 }}>{a.network}</td>
                      <td style={{ padding: 8 }}>{a.label || ""}</td>
                      <td style={{ padding: 8, fontFamily: "monospace" }}>{a.address}</td>
                      <td style={{ padding: 8, textAlign: "right", whiteSpace: "nowrap" }}>
                        <button
                          onClick={() => refreshWalletBalances([a.id])}
                          disabled={waBusy}
                          style={{ padding: "4px 8px", marginRight: 8 }}
                        >
                          Refresh
                        </button>
                        <button onClick={() => deleteWalletAddress(a.id)} disabled={waBusy} style={{ padding: "4px 8px" }}>
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                  {!waList.length && (
                    <tr>
                      <td colSpan={5} style={{ padding: 10, opacity: 0.8 }}>
                        No addresses registered yet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Latest balances */}
            <div style={{ border: "1px solid var(--utt-border, #333)", borderRadius: 10, padding: 12, overflow: "auto" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                <b>Latest on-chain balances</b>
                <span style={{ opacity: 0.8 }}>{waBalances.length} rows</span>
              </div>

              <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 900 }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: 8 }}>Asset</th>
                    <th style={{ textAlign: "left", padding: 8 }}>Network</th>
                    <th style={{ textAlign: "left", padding: 8 }}>Wallet</th>
                    <th style={{ textAlign: "right", padding: 8 }}>Total</th>
                    <th style={{ textAlign: "right", padding: 8 }}>Px USD</th>
                    <th style={{ textAlign: "right", padding: 8 }}>Total USD</th>
                    <th style={{ textAlign: "left", padding: 8 }}>Captured</th>
                  </tr>
                </thead>
                <tbody>
                  {waBalances.map((b, idx) => (
                    <tr key={idx} style={{ borderTop: "1px solid var(--utt-border, #333)" }}>
                      <td style={{ padding: 8 }}>{b.asset}</td>
                      <td style={{ padding: 8 }}>{b.network}</td>
                      <td style={{ padding: 8 }}>{b.wallet_id || b.label || b.address}</td>
                      <td style={{ padding: 8, textAlign: "right" }}>{fmt8(b.total)}</td>
                      <td style={{ padding: 8, textAlign: "right" }}>{fmt2(b.px_usd)}</td>
                      <td style={{ padding: 8, textAlign: "right" }}>{fmt2(b.total_usd)}</td>
                      <td style={{ padding: 8 }}>{b.captured_at ? String(b.captured_at) : ""}</td>
                    </tr>
                  ))}
                  {!waBalances.length && (
                    <tr>
                      <td colSpan={7} style={{ padding: 10, opacity: 0.8 }}>
                        No snapshots yet. Click “Refresh On-chain Balances”.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>

              <div style={{ marginTop: 10, opacity: 0.85 }}>
                Notes: BTC uses an Esplora endpoint (mempool.space). DOGE uses BlockCypher. DOT uses Subscan and requires
                SUBSCAN_API_KEY in your backend environment.
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
