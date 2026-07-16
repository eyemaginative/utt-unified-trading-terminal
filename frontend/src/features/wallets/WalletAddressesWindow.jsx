// frontend/src/features/wallets/WalletAddressesWindow.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";

/**
 * WalletAddressesWindow (MVP)
 *
 * Backend endpoints (current):
 *  - GET    /api/wallet_addresses?asset=&network=&limit=
 *  - POST   /api/wallet_addresses
 *  - PATCH  /api/wallet_addresses/{address_id}
 *  - DELETE /api/wallet_addresses/{address_id}
 *  - GET    /api/wallet_addresses/balances/latest?limit=
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
const ROBINHOOD_CHAIN_NETWORK = Object.freeze({
  chainIdHex: "0x1237",
  chainIdDecimal: 4663,
  chainName: "Robinhood Chain",
  nativeCurrency: Object.freeze({ name: "Ether", symbol: "ETH", decimals: 18 }),
  rpcUrls: Object.freeze(["https://rpc.mainnet.chain.robinhood.com/"]),
  blockExplorerUrls: Object.freeze(["https://robinhoodchain.blockscout.com"]),
});

const WALLET_CYBER_CSS = `
.utt-wallet-addresses-cyber {
  --wallet-cyan: var(--utt-cyber-cyan, #42e8ff);
  --wallet-cyan-soft: rgba(66, 232, 255, 0.14);
  --wallet-green: var(--utt-cyber-green, #5dff9a);
  --wallet-amber: var(--utt-cyber-amber, #ffc857);
  --wallet-red: var(--utt-cyber-red, #ff5f7a);
  --wallet-panel: var(--utt-surface-1, #08131c);
  --wallet-panel-strong: var(--utt-surface-2, #0b1d29);
  --wallet-border: var(--utt-border-1, rgba(66,232,255,0.28));
  position: relative;
  min-height: 100%;
  padding: 12px;
  overflow: auto;
  color: var(--utt-page-fg, #e9faff);
  background:
    linear-gradient(rgba(66,232,255,0.025) 1px, transparent 1px),
    linear-gradient(90deg, rgba(66,232,255,0.025) 1px, transparent 1px),
    radial-gradient(900px 420px at 12% -10%, rgba(0, 204, 255, 0.15), transparent 60%),
    var(--utt-page-bg, #050a0f);
  background-size: 28px 28px, 28px 28px, auto, auto;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  font-size: 13px;
}
.utt-wallet-addresses-cyber::before {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  background: repeating-linear-gradient(180deg, transparent 0, transparent 3px, rgba(255,255,255,0.012) 4px);
  mix-blend-mode: screen;
}
.utt-wallet-addresses-cyber > * { position: relative; z-index: 1; }
.utt-wallet-titlebar {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
  padding: 10px 12px;
  border: 1px solid var(--wallet-border);
  border-radius: 12px;
  background: linear-gradient(135deg, rgba(66,232,255,0.11), rgba(0,0,0,0.18) 55%, rgba(93,255,154,0.06));
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.05), 0 12px 28px rgba(0,0,0,0.28);
}
.utt-wallet-title {
  color: var(--wallet-cyan);
  font-family: "Roboto Mono", "Cascadia Code", ui-monospace, monospace;
  font-size: 16px;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  text-shadow: 0 0 14px rgba(66,232,255,0.35);
}
.utt-wallet-subtitle { color: var(--utt-hdr-muted, rgba(233,250,255,0.68)); font-family: ui-monospace, monospace; }
.utt-wallet-tabs {
  display: flex;
  gap: 6px;
  margin-bottom: 10px;
  padding: 5px;
  border: 1px solid var(--wallet-border);
  border-radius: 10px;
  background: rgba(0,0,0,0.22);
}
.utt-wallet-addresses-cyber button {
  min-height: 30px;
  border: 1px solid var(--wallet-border);
  border-radius: 8px;
  padding: 5px 10px;
  color: var(--utt-page-fg, #e9faff);
  background: linear-gradient(180deg, rgba(66,232,255,0.09), rgba(0,0,0,0.24));
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
  cursor: pointer;
  font-weight: 700;
  letter-spacing: 0.015em;
  transition: border-color 120ms ease, background 120ms ease, transform 120ms ease, box-shadow 120ms ease;
}
.utt-wallet-addresses-cyber button:hover:not(:disabled) {
  border-color: var(--wallet-cyan);
  background: linear-gradient(180deg, rgba(66,232,255,0.18), rgba(0,0,0,0.24));
  box-shadow: 0 0 0 1px rgba(66,232,255,0.14), 0 0 16px rgba(66,232,255,0.12);
  transform: translateY(-1px);
}
.utt-wallet-addresses-cyber button:focus-visible,
.utt-wallet-addresses-cyber input:focus-visible {
  outline: 2px solid var(--wallet-cyan);
  outline-offset: 2px;
}
.utt-wallet-addresses-cyber button:disabled { opacity: 0.48; cursor: not-allowed; transform: none; }
.utt-wallet-tabs button[disabled] {
  opacity: 1;
  color: #041014;
  border-color: var(--wallet-cyan);
  background: var(--wallet-cyan);
  box-shadow: 0 0 18px rgba(66,232,255,0.22);
}
.utt-wallet-addresses-cyber input:not([type="checkbox"]) {
  min-width: 0;
  min-height: 30px;
  box-sizing: border-box;
  border: 1px solid var(--wallet-border);
  border-radius: 7px;
  padding: 5px 8px;
  color: var(--utt-page-fg, #e9faff);
  background: var(--utt-control-bg, #061018);
  font-family: inherit;
}
.utt-wallet-addresses-cyber input::placeholder { color: rgba(210,238,246,0.44); }
.utt-wallet-addresses-cyber input[type="checkbox"] { accent-color: var(--wallet-cyan); }
.utt-wallet-addresses-cyber label {
  color: var(--utt-hdr-muted, rgba(233,250,255,0.72));
  font-family: ui-monospace, monospace;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.03em;
  text-transform: uppercase;
}
.utt-wallet-addresses-cyber code {
  color: var(--wallet-cyan);
  background: rgba(66,232,255,0.08);
  border: 1px solid rgba(66,232,255,0.16);
  border-radius: 4px;
  padding: 1px 4px;
}
.utt-wallet-panel {
  margin-bottom: 12px;
  padding: 11px;
  border: 1px solid var(--wallet-border) !important;
  border-radius: 12px;
  background: linear-gradient(145deg, var(--wallet-panel), rgba(0,0,0,0.18));
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.035), 0 10px 24px rgba(0,0,0,0.2);
}
.utt-wallet-panel-title {
  margin-bottom: 9px;
  color: var(--wallet-cyan);
  font-family: "Roboto Mono", "Cascadia Code", ui-monospace, monospace;
  font-weight: 800;
  letter-spacing: 0.055em;
  text-transform: uppercase;
}
.utt-wallet-form-grid {
  display: grid !important;
  grid-template-columns: minmax(120px, 140px) minmax(180px, 1fr) minmax(120px, 140px) minmax(180px, 1fr) !important;
  gap: 8px 10px !important;
  align-items: center;
}
.utt-wallet-action-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.utt-wallet-error {
  margin-bottom: 10px;
  padding: 9px 10px;
  border: 1px solid rgba(255,95,122,0.58);
  border-radius: 9px;
  color: #ffd9e0;
  background: rgba(90,10,28,0.48);
  box-shadow: 0 0 18px rgba(255,95,122,0.08);
  font-weight: 700;
}
.utt-wallet-terminal-output {
  margin-top: 10px;
  padding: 9px;
  border: 1px solid var(--wallet-border) !important;
  border-radius: 9px;
  color: var(--utt-page-fg, #e9faff) !important;
  background: rgba(1,8,12,0.86) !important;
  font-family: "Cascadia Code", ui-monospace, monospace;
}
.utt-wallet-table-wrap {
  overflow: auto;
  max-width: 100%;
  border: 1px solid rgba(66,232,255,0.13);
  border-radius: 9px;
  background: rgba(0,0,0,0.18);
}
.utt-wallet-table { width: 100%; border-collapse: separate !important; border-spacing: 0; }
.utt-wallet-table thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  padding: 7px !important;
  border-bottom: 1px solid var(--wallet-border) !important;
  color: var(--wallet-cyan);
  background: var(--wallet-panel-strong) !important;
  font-family: ui-monospace, monospace;
  font-size: 11px;
  letter-spacing: 0.045em;
  text-transform: uppercase;
  white-space: nowrap;
}
.utt-wallet-table tbody td {
  padding: 7px !important;
  border-bottom: 1px solid rgba(66,232,255,0.08) !important;
  background: transparent;
  white-space: nowrap;
}
.utt-wallet-table tbody tr:hover td { background: rgba(66,232,255,0.045); }
.utt-wallet-metamask-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  margin: 8px 0 10px;
}
.utt-wallet-metamask-cell {
  min-width: 0;
  padding: 8px;
  border: 1px solid rgba(66,232,255,0.16);
  border-radius: 8px;
  background: rgba(0,0,0,0.24);
}
.utt-wallet-metamask-label {
  margin-bottom: 4px;
  color: rgba(224,246,252,0.58);
  font-family: ui-monospace, monospace;
  font-size: 10px;
  letter-spacing: 0.07em;
  text-transform: uppercase;
}
.utt-wallet-metamask-value { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: ui-monospace, monospace; }
.utt-wallet-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 24px;
  padding: 2px 8px;
  border: 1px solid var(--wallet-border);
  border-radius: 999px;
  font-family: ui-monospace, monospace;
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.045em;
  text-transform: uppercase;
}
.utt-wallet-chip::before { content: ""; width: 7px; height: 7px; border-radius: 50%; background: currentColor; box-shadow: 0 0 10px currentColor; }
.utt-wallet-chip--good { color: var(--wallet-green); border-color: rgba(93,255,154,0.38); background: rgba(93,255,154,0.08); }
.utt-wallet-chip--warn { color: var(--wallet-amber); border-color: rgba(255,200,87,0.38); background: rgba(255,200,87,0.08); }
.utt-wallet-chip--bad { color: var(--wallet-red); border-color: rgba(255,95,122,0.38); background: rgba(255,95,122,0.08); }
.utt-wallet-chip--neutral { color: var(--wallet-cyan); background: rgba(66,232,255,0.07); }
.utt-wallet-metamask-note { color: var(--utt-hdr-muted, rgba(233,250,255,0.7)); line-height: 1.45; }
.utt-wallet-metamask-message { margin-top: 8px; padding: 7px 8px; border-left: 3px solid var(--wallet-cyan); background: rgba(66,232,255,0.06); }
.utt-wallet-metamask-message--error { border-left-color: var(--wallet-red); color: #ffd9e0; background: rgba(255,95,122,0.07); }
.utt-wallet-danger-button { border-color: rgba(255,95,122,0.42) !important; color: #ffd0d9 !important; }
.utt-wallet-primary-button { border-color: rgba(66,232,255,0.64) !important; color: var(--wallet-cyan) !important; }
.utt-wallet-good-button { border-color: rgba(93,255,154,0.52) !important; color: var(--wallet-green) !important; }
@media (max-width: 900px) {
  .utt-wallet-metamask-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .utt-wallet-form-grid { grid-template-columns: minmax(110px, 135px) minmax(0, 1fr) !important; }
}
@media (max-width: 580px) {
  .utt-wallet-addresses-cyber { padding: 8px; }
  .utt-wallet-titlebar { align-items: flex-start; flex-wrap: wrap; }
  .utt-wallet-metamask-grid { grid-template-columns: 1fr; }
  .utt-wallet-form-grid { grid-template-columns: 1fr !important; }
  .utt-wallet-form-grid label { margin-top: 4px; }
}
`;

function getMetaMaskProvider() {
  if (typeof window === "undefined") return null;
  const injected = window.ethereum;
  if (!injected) return null;
  if (Array.isArray(injected.providers)) {
    const exact = injected.providers.find((provider) => provider?.isMetaMask);
    if (exact) return exact;
  }
  return injected?.isMetaMask ? injected : null;
}

function hasInjectedEvmProvider() {
  return typeof window !== "undefined" && !!window.ethereum;
}

function normalizeEvmChainId(value) {
  try {
    const raw = String(value ?? "").trim();
    if (!raw) return "";
    return `0x${BigInt(raw).toString(16)}`;
  } catch {
    return "";
  }
}

function chainIdDecimalLabel(value) {
  try {
    const raw = String(value ?? "").trim();
    if (!raw) return "Unknown";
    return String(BigInt(raw));
  } catch {
    return "Unknown";
  }
}

function compactEvmAddress(address) {
  const raw = String(address || "").trim();
  if (raw.length <= 18) return raw || "—";
  return `${raw.slice(0, 10)}…${raw.slice(-8)}`;
}

export default function WalletAddressesWindow({ apiBase = "", hideTableData = false, onClose }) {
  const [tab, setTab] = useState("addresses"); // addresses | balances
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const metamaskProviderRef = useRef(null);
  const [metamaskState, setMetamaskState] = useState({
    checked: false,
    available: false,
    injectedAvailable: false,
    address: "",
    chainId: "",
    lastEvent: "",
  });
  const [metamaskBusy, setMetamaskBusy] = useState(false);
  const [metamaskError, setMetamaskError] = useState("");
  const [metamaskNotice, setMetamaskNotice] = useState("");

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

  const metamaskChainIdHex = normalizeEvmChainId(metamaskState.chainId);
  const metamaskConnected = !!String(metamaskState.address || "").trim();
  const metamaskOnRobinhoodChain = metamaskChainIdHex === ROBINHOOD_CHAIN_NETWORK.chainIdHex;
  const metamaskReady = metamaskState.available && metamaskConnected && metamaskOnRobinhoodChain;

  function metamaskErrorMessage(error, fallback) {
    const code = Number(error?.code);
    if (code === 4001) return "MetaMask request was declined.";
    if (code === 4902) return "Robinhood Chain is not installed in MetaMask. Use Add Robinhood Chain.";
    return error?.message || String(error || fallback || "MetaMask request failed.");
  }

  async function readMetaMaskState({ requestAccounts = false, reason = "refresh" } = {}) {
    const provider = metamaskProviderRef.current || getMetaMaskProvider();
    metamaskProviderRef.current = provider;

    if (!provider) {
      setMetamaskState((prev) => ({
        ...prev,
        checked: true,
        available: false,
        injectedAvailable: hasInjectedEvmProvider(),
        address: "",
        chainId: "",
        lastEvent: reason,
      }));
      return { provider: null, accounts: [], chainId: "" };
    }

    const accounts = requestAccounts
      ? await provider.request({ method: "eth_requestAccounts" })
      : await provider.request({ method: "eth_accounts" });
    const chainId = await provider.request({ method: "eth_chainId" });
    const normalizedAccounts = Array.isArray(accounts) ? accounts : [];
    const address = String(normalizedAccounts[0] || "").trim();
    const normalizedChainId = normalizeEvmChainId(chainId);

    setMetamaskState({
      checked: true,
      available: true,
      injectedAvailable: true,
      address,
      chainId: normalizedChainId,
      lastEvent: reason,
    });

    return { provider, accounts: normalizedAccounts, chainId: normalizedChainId };
  }

  async function connectMetaMask() {
    setMetamaskBusy(true);
    setMetamaskError("");
    setMetamaskNotice("");
    try {
      const result = await readMetaMaskState({ requestAccounts: true, reason: "connect" });
      const address = String(result?.accounts?.[0] || "").trim();
      if (!address) throw new Error("MetaMask returned no account.");
      setMetamaskNotice("MetaMask account connected to UTT. No signature or transaction was requested.");
    } catch (error) {
      setMetamaskError(metamaskErrorMessage(error, "MetaMask connection failed."));
    } finally {
      setMetamaskBusy(false);
    }
  }

  async function switchMetaMaskToRobinhoodChain() {
    const provider = metamaskProviderRef.current || getMetaMaskProvider();
    if (!provider) {
      setMetamaskError("MetaMask was not detected.");
      return;
    }
    setMetamaskBusy(true);
    setMetamaskError("");
    setMetamaskNotice("");
    try {
      await provider.request({
        method: "wallet_switchEthereumChain",
        params: [{ chainId: ROBINHOOD_CHAIN_NETWORK.chainIdHex }],
      });
      await readMetaMaskState({ reason: "network_switch" });
      setMetamaskNotice("MetaMask is now set to Robinhood Chain.");
    } catch (error) {
      setMetamaskError(metamaskErrorMessage(error, "Unable to switch MetaMask network."));
    } finally {
      setMetamaskBusy(false);
    }
  }

  async function addRobinhoodChainToMetaMask() {
    const provider = metamaskProviderRef.current || getMetaMaskProvider();
    if (!provider) {
      setMetamaskError("MetaMask was not detected.");
      return;
    }
    setMetamaskBusy(true);
    setMetamaskError("");
    setMetamaskNotice("");
    try {
      await provider.request({
        method: "wallet_addEthereumChain",
        params: [{
          chainId: ROBINHOOD_CHAIN_NETWORK.chainIdHex,
          chainName: ROBINHOOD_CHAIN_NETWORK.chainName,
          nativeCurrency: ROBINHOOD_CHAIN_NETWORK.nativeCurrency,
          rpcUrls: [...ROBINHOOD_CHAIN_NETWORK.rpcUrls],
          blockExplorerUrls: [...ROBINHOOD_CHAIN_NETWORK.blockExplorerUrls],
        }],
      });
      await readMetaMaskState({ reason: "network_add" });
      setMetamaskNotice("Robinhood Chain network details were submitted to MetaMask for approval.");
    } catch (error) {
      setMetamaskError(metamaskErrorMessage(error, "Unable to add Robinhood Chain."));
    } finally {
      setMetamaskBusy(false);
    }
  }

  function useConnectedMetaMaskAddress() {
    const address = String(metamaskState.address || "").trim();
    if (!address) {
      setMetamaskError("Connect a MetaMask account first.");
      return;
    }
    setEditingId(null);
    setForm({
      asset: "ALL",
      wallet_id: "robinhood_chain",
      network: "robinhood_chain",
      address,
      label: "Robinhood Chain MetaMask",
      owner_scope: "user",
    });
    setMetamaskError("");
    setMetamaskNotice("Connected address copied into the wallet form. Review it, then use Create to save it.");
  }

  async function saveConnectedMetaMaskWallet() {
    const address = String(metamaskState.address || "").trim();
    if (!address) {
      setMetamaskError("Connect a MetaMask account first.");
      return;
    }
    if (!metamaskOnRobinhoodChain) {
      setMetamaskError("Switch MetaMask to Robinhood Chain before saving this wallet record.");
      return;
    }

    const payload = {
      asset: "ALL",
      wallet_id: "robinhood_chain",
      network: "robinhood_chain",
      address,
      label: "Robinhood Chain MetaMask",
      owner_scope: "user",
    };

    setBusy(true);
    setMetamaskBusy(true);
    setErr("");
    setMetamaskError("");
    setMetamaskNotice("");
    try {
      await api(`/api/wallet_addresses`, { method: "POST", body: payload });
      await loadAddresses();
      setForm(payload);
      setEditingId(null);
      setMetamaskNotice("Robinhood Chain wallet saved. The ALL row is metadata-only until EVM balance reads are enabled.");
    } catch (error) {
      setMetamaskError(error?.message || String(error));
    } finally {
      setMetamaskBusy(false);
      setBusy(false);
    }
  }

  function clearLocalMetaMaskState() {
    setMetamaskState((prev) => ({ ...prev, address: "", lastEvent: "local_clear" }));
    setMetamaskError("");
    setMetamaskNotice("UTT local connection state was cleared. MetaMask itself was not disconnected or locked.");
  }

  function applyHydrationWalletMode() {
    setForm((p) => ({
      ...p,
      asset: "ALL",
      wallet_id: "polkadot_hydration",
      network: "hydration",
      label: p.label || "Hydration SubWallet",
    }));
  }


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
      return items;
    } catch (e) {
      setErr(e?.message || String(e));
      return [];
    } finally {
      setBusy(false);
    }
  }

  function isHydrationAddressRow(row) {
    const walletId = String(row?.wallet_id ?? row?.walletId ?? row?.venue ?? row?.venue_override ?? "").trim().toLowerCase();
    const network = String(row?.network ?? row?.chain ?? row?.network_name ?? "").trim().toLowerCase();
    return (
      walletId === "polkadot_hydration" ||
      walletId === "hydration" ||
      walletId.includes("hydration") ||
      network === "hydration" ||
      network === "polkadot_hydration" ||
      network.includes("hydration")
    );
  }


  function firstHydrationOrderbookPrice(levels) {
    const arr = Array.isArray(levels) ? levels : [];
    for (const lvl of arr) {
      const px = Array.isArray(lvl)
        ? Number(lvl?.[0] ?? lvl?.price)
        : Number(lvl?.price ?? lvl?.px ?? lvl?.rate ?? lvl?.limit ?? lvl?.p);
      if (Number.isFinite(px) && px > 0) return px;
    }
    return null;
  }

  function hydrationOrderbookMid(data) {
    const direct = Number(
      data?.mid ??
      data?.midPrice ??
      data?.mid_price ??
      data?.price ??
      data?.markPrice ??
      data?.mark_price ??
      data?.spotPrice ??
      data?.spot_price ??
      data?.pool?.spotPrice ??
      data?.pool?.spot_price
    );
    if (Number.isFinite(direct) && direct > 0) return direct;

    const rawBid = Number(data?.bestBid ?? data?.best_bid ?? data?.bid ?? data?.bids?.[0]?.price);
    const rawAsk = Number(data?.bestAsk ?? data?.best_ask ?? data?.ask ?? data?.asks?.[0]?.price);
    const bid = Number.isFinite(rawBid) && rawBid > 0 ? rawBid : firstHydrationOrderbookPrice(data?.bids);
    const ask = Number.isFinite(rawAsk) && rawAsk > 0 ? rawAsk : firstHydrationOrderbookPrice(data?.asks);
    if (bid !== null && ask !== null && bid > 0 && ask > 0) return (bid + ask) / 2;
    if (bid !== null && bid > 0) return bid;
    if (ask !== null && ask > 0) return ask;
    return null;
  }

  async function fetchHydrationOrderbookMid(symbol) {
    const sym = String(symbol || "").trim().toUpperCase();
    if (sym !== "UTTT-HDX") return null;
    try {
      const data = await api(`/api/polkadot_dex/hydration/orderbook?symbol=${encodeURIComponent(sym)}&depth=5&route_mode=manual_xyk`);
      if (data?.ok === false) return null;
      const mid = hydrationOrderbookMid(data);
      return mid !== null && mid > 0 ? mid : null;
    } catch {
      return null;
    }
  }

  async function fetchHydrationDerivedUsdPrices() {
    const out = {
      prices: { USDT: 1, USDC: 1, HOLLAR: 1 },
      sources: { USDT: "stable", USDC: "stable", HOLLAR: "stable" },
    };

    const asHydrationPriceNum = (v) => {
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    try {
      const data = await api("/api/polkadot_dex/hydration/prices?assets=HDX,DOT,USDT,UTTT,HOLLAR&refresh=true");
      const maps = [
        data?.prices_usd,
        data?.usd_prices,
        data?.pricesUsd,
        data?.usdPrices,
        data?.prices,
        data?.price_map,
        data?.priceMap,
        data?.usd,
      ];
      const sourceMaps = [
        data?.priceSources,
        data?.price_sources,
        data?.usd_sources,
        data?.usdSources,
        data?.sources,
      ];
      const symbols = ["HDX", "DOT", "UTTT", "USDT", "USDC", "HOLLAR"];

      for (const sym of symbols) {
        for (const m of maps) {
          if (!m || typeof m !== "object") continue;
          const entry = m?.[sym] ?? m?.[sym.toLowerCase()];
          const val = asHydrationPriceNum(
            entry && typeof entry === "object"
              ? entry?.px_usd ?? entry?.price_usd ?? entry?.priceUsd ?? entry?.usd_price ?? entry?.usdPrice ?? entry?.price ?? entry?.usd
              : entry
          );
          if (val !== null) {
            out.prices[sym] = val;
            break;
          }
        }

        for (const sm of sourceMaps) {
          if (!sm || typeof sm !== "object") continue;
          const src = sm?.[sym] ?? sm?.[sym.toLowerCase()];
          if (src !== undefined && src !== null && String(src).trim()) {
            out.sources[sym] = String(src);
            break;
          }
        }
      }
    } catch {
      // Keep stablecoin defaults and avoid falling back to generic Hydration orderbook pairs.
    }

    return out;
  }

  function applyHydrationDerivedUsdPricesToSnapshotRows(items, derived) {
    const prices = derived?.prices || {};
    const sources = derived?.sources || {};
    const asNum = (v) => {
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    return (items || []).map((it) => {
      const sym = String(it?.asset || it?.symbol || "").trim().toUpperCase();
      const balance = asNum(it?.balance ?? it?.total ?? it?.available) ?? 0;
      const existingPx = asNum(it?.usd_price ?? it?.px_usd ?? it?.price_usd);
      const existingUsd = asNum(it?.usd_value ?? it?.total_usd ?? it?.value_usd);
      const derivedPx = asNum(prices?.[sym]);
      const px = existingPx !== null ? existingPx : derivedPx;
      const usdValue = existingUsd !== null ? existingUsd : (px !== null ? balance * px : null);

      return {
        ...it,
        usd_price: px !== null ? px : it?.usd_price ?? "",
        usd_value: usdValue !== null ? usdValue : it?.usd_value ?? "",
        usd_source_symbol: it?.usd_source_symbol && it.usd_source_symbol !== "—"
          ? it.usd_source_symbol
          : (px !== null ? (sources?.[sym] || "derived") : it?.usd_source_symbol || ""),
      };
    });
  }

  function normalizeHydrationBalanceRows(data, sourceRow) {
    const address = String(sourceRow?.address || data?.address || "").trim();
    const label = sourceRow?.label || "Hydration SubWallet";
    const fetchedAt = data?.fetched_at || data?.fetchedAt || new Date().toISOString();

    const asNum = (v) => {
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    const canonicalHydrationSymbol = (symbol, assetId) => {
      const s = String(symbol || "").trim();
      const id = String(assetId || "").trim();
      const key = (s || id).toLowerCase();
      const byId = {
        "0": "HDX",
        native: "HDX",
        hdx: "HDX",
        "5": "DOT",
        dot: "DOT",
        "10": "USDT",
        usdt: "USDT",
        "222": "HOLLAR",
        hollar: "HOLLAR",
        "1001331": "UTTT",
        uttt: "UTTT",
      };
      return byId[key] || s || id;
    };

    const getHydrationUsdPrice = (symbol, assetId, item = null) => {
      const direct = asNum(
        item?.px_usd ??
        item?.price_usd ??
        item?.priceUsd ??
        item?.usd_price ??
        item?.usdPrice ??
        item?.priceUSD ??
        item?.usd
      );
      if (direct !== null) return direct;

      const sym = String(symbol || "").trim().toUpperCase();
      const id = String(assetId || "").trim();
      if (sym === "USDT" || sym === "USDC" || sym === "HOLLAR") return 1;

      const maps = [
        data?.prices_usd,
        data?.pricesUsd,
        data?.usd_prices,
        data?.usdPrices,
        data?.prices,
        data?.price_map,
        data?.priceMap,
        data?.usd,
      ];
      const keys = [sym, sym.toLowerCase(), id, id.toLowerCase()].filter(Boolean);
      for (const m of maps) {
        if (!m || typeof m !== "object") continue;
        for (const k of keys) {
          const entry = m?.[k];
          const val = asNum(
            typeof entry === "object"
              ? entry?.px_usd ?? entry?.price_usd ?? entry?.priceUsd ?? entry?.usd_price ?? entry?.usdPrice ?? entry?.price ?? entry?.usd
              : entry
          );
          if (val !== null) return val;
        }
      }

      return asNum(
        sym === "HDX" ? (data?.hdx_usd ?? data?.hdxUsd ?? data?.hdx_price_usd ?? data?.hdxPriceUsd) :
        sym === "DOT" ? (data?.dot_usd ?? data?.dotUsd ?? data?.dot_price_usd ?? data?.dotPriceUsd) :
        sym === "UTTT" ? (data?.uttt_usd ?? data?.utttUsd ?? data?.uttt_price_usd ?? data?.utttPriceUsd) :
        null
      );
    };

    let rawItems = Array.isArray(data?.items)
      ? data.items
      : Array.isArray(data?.balances)
        ? data.balances
        : Array.isArray(data?.tokens)
          ? data.tokens
          : Array.isArray(data?.assets)
            ? data.assets
            : data?.balances && typeof data.balances === "object"
              ? Object.entries(data.balances).map(([asset, value]) => ({ asset, ...(value && typeof value === "object" ? value : { total: value }) }))
              : data?.items && typeof data.items === "object"
                ? Object.entries(data.items).map(([asset, value]) => ({ asset, ...(value && typeof value === "object" ? value : { total: value }) }))
                : [];

    const hasNativeHdx = (rawItems || []).some((it) => {
      const sym = canonicalHydrationSymbol(
        it?.symbol ?? it?.asset ?? it?.ticker ?? it?.currency ?? it?.assetSymbol ?? it?.asset_symbol,
        it?.asset_id ?? it?.assetId ?? it?.id ?? it?.token_id ?? it?.tokenId
      );
      return String(sym || "").toUpperCase() === "HDX";
    });

    if (!hasNativeHdx) {
      const nativeObj =
        (data?.native && typeof data.native === "object" ? data.native : null) ||
        (data?.native_balance && typeof data.native_balance === "object" ? data.native_balance : null) ||
        (data?.nativeBalance && typeof data.nativeBalance === "object" ? data.nativeBalance : null) ||
        (data?.hdx && typeof data.hdx === "object" ? data.hdx : null) ||
        (data?.hdx_balance && typeof data.hdx_balance === "object" ? data.hdx_balance : null) ||
        (data?.hdxBalance && typeof data.hdxBalance === "object" ? data.hdxBalance : null) ||
        null;
      const nativeNumber = asNum(
        data?.hdx_ui ??
        data?.hdxUi ??
        data?.hdx_balance_ui ??
        data?.hdxBalanceUi ??
        data?.native_ui ??
        data?.nativeUi ??
        data?.native_balance_ui ??
        data?.nativeBalanceUi ??
        data?.hdx_balance ??
        data?.hdxBalance ??
        data?.native_balance ??
        data?.nativeBalance
      );
      const candidate = nativeObj
        ? { asset: "HDX", symbol: "HDX", asset_id: "native", ...nativeObj }
        : nativeNumber !== null
          ? { asset: "HDX", symbol: "HDX", asset_id: "native", total: nativeNumber, available: nativeNumber }
          : null;
      if (candidate) rawItems = [candidate, ...(rawItems || [])];
    }

    const out = [];
    for (const it of rawItems || []) {
      const rawAsset = String(
        it?.symbol ??
        it?.asset ??
        it?.ticker ??
        it?.currency ??
        it?.assetSymbol ??
        it?.asset_symbol ??
        ""
      ).trim();
      const assetId = String(
        it?.asset_id ??
        it?.assetId ??
        it?.id ??
        it?.token_id ??
        it?.tokenId ??
        ""
      ).trim();
      const asset = canonicalHydrationSymbol(rawAsset, assetId);

      const free = asNum(
        it?.free_ui ??
        it?.freeUi ??
        it?.available_ui ??
        it?.availableUi ??
        it?.available ??
        it?.free ??
        it?.spendable ??
        it?.amount_ui ??
        it?.amountUi ??
        it?.uiAmount ??
        it?.ui_amount
      );

      const reserved = asNum(
        it?.reserved_ui ??
        it?.reservedUi ??
        it?.reserved ??
        it?.hold_ui ??
        it?.holdUi ??
        it?.hold ??
        it?.locked ??
        0
      );

      const totalExplicit = asNum(
        it?.total_ui ??
        it?.totalUi ??
        it?.balance_ui ??
        it?.balanceUi ??
        it?.total ??
        it?.balance ??
        it?.amount
      );

      const balance = totalExplicit ?? ((free ?? 0) + (reserved ?? 0));
      const usdPrice = getHydrationUsdPrice(asset, assetId, it);
      const usdValue = asNum(it?.total_usd ?? it?.usd_value ?? it?.usdValue ?? it?.value_usd ?? it?.valueUsd) ??
        (usdPrice != null && balance != null ? balance * usdPrice : null);

      if (!asset && balance == null) continue;

      out.push({
        id: `hydration:${sourceRow?.id || address}:${asset || out.length}`,
        asset: asset || "Hydration",
        network: "hydration",
        address,
        label,
        balance: balance ?? "",
        usd_price: usdPrice ?? "",
        usd_value: usdValue ?? "",
        fetched_at: fetchedAt,
        source: data?.source || data?.venue || "polkadot_dex/balances",
      });
    }
    return out;
  }

  async function refreshHydrationBalanceRow(row) {
    const address = String(row?.address || "").trim();
    if (!address) throw new Error("Hydration wallet row is missing an address.");
    const res = await api(`/api/polkadot_dex/balances?address=${encodeURIComponent(address)}`);
    if (res?.ok === false) throw new Error(res?.detail || res?.error || "Hydration balances failed.");

    const normalizedRows = normalizeHydrationBalanceRows(res, row);
    try {
      const derived = await fetchHydrationDerivedUsdPrices();
      return applyHydrationDerivedUsdPricesToSnapshotRows(normalizedRows, derived);
    } catch {
      return normalizedRows;
    }
  }

  async function refreshBalances(ids = null) {
    setBusy(true);
    setErr("");
    try {
      const selectedRows = Array.isArray(ids) && ids.length
        ? (addresses || []).filter((a) => ids.includes(a.id))
        : (addresses || []);

      const hydrationRows = selectedRows.filter((a) => isHydrationAddressRow(a));
      const legacyRows = selectedRows.filter((a) => !isHydrationAddressRow(a));

      const hydrationLiveRows = [];
      const hydrationErrors = [];
      for (const row of hydrationRows) {
        try {
          const rows = await refreshHydrationBalanceRow(row);
          hydrationLiveRows.push(...rows);
        } catch (e) {
          hydrationErrors.push({
            id: row?.id,
            asset: row?.asset,
            network: row?.network,
            address: row?.address,
            error: e?.message || String(e),
          });
        }
      }

      const shouldRunLegacyRefresh = Array.isArray(ids) && ids.length
        ? legacyRows.length > 0
        : hydrationRows.length === 0 || legacyRows.length > 0;

      let res = null;
      let latest = [];
      if (shouldRunLegacyRefresh) {
        const body = Array.isArray(ids) && ids.length
          ? { ids: legacyRows.map((a) => a.id).filter(Boolean) }
          : hydrationRows.length && legacyRows.length
            ? { ids: legacyRows.map((a) => a.id).filter(Boolean) }
            : {};

        if (!Array.isArray(body.ids) || body.ids.length) {
          res = await api(`/api/wallet_addresses/balances/refresh`, { method: "POST", body });
          latest = await loadBalances();
        }
      }

      const mergedErrors = [
        ...(Array.isArray(res?.errors) ? res.errors : []),
        ...hydrationErrors,
      ];

      const refreshed = Number(res?.refreshed || 0) + hydrationRows.length - hydrationErrors.length;
      const finalResult = {
        ...(res || {}),
        refreshed,
        errors: mergedErrors,
        hydration_live: {
          attempted: hydrationRows.length,
          refreshed: hydrationRows.length - hydrationErrors.length,
          rows: hydrationLiveRows.length,
          endpoint: "/api/polkadot_dex/balances",
        },
      };

      // surface refresh result in the UI
      setBalLastRefresh(finalResult);

      if (hydrationLiveRows.length) {
        setBalances([...(hydrationLiveRows || []), ...(latest || [])]);
        setTab("balances");
      }

      if (mergedErrors.length) {
        console.error("[wallet_balances_refresh] errors:", mergedErrors);
        setErr(`Balances refresh completed: refreshed ${refreshed}, errors ${mergedErrors.length} (see console).`);
      }
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
      const walletId = String(form.wallet_id || "").trim();
      const network = String(form.network || "").trim();
      const isHydrationWallet =
        walletId.toLowerCase() === "polkadot_hydration" ||
        network.toLowerCase() === "hydration";
      const asset = String(form.asset || "").trim().toUpperCase() || (isHydrationWallet ? "ALL" : "");

      const payload = {
        asset,
        wallet_id: walletId || null,
        network,
        address: String(form.address || "").trim(),
        label: String(form.label || "").trim() || null,
        owner_scope: String(form.owner_scope || "user").trim().toLowerCase(),
      };

      if (!payload.address) throw new Error("Address is required.");
      if (!payload.asset) throw new Error("Asset is required. Use ALL for Hydration/SubWallet all-asset detection.");
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
    let active = true;
    const provider = getMetaMaskProvider();
    metamaskProviderRef.current = provider;

    if (!provider) {
      setMetamaskState({
        checked: true,
        available: false,
        injectedAvailable: hasInjectedEvmProvider(),
        address: "",
        chainId: "",
        lastEvent: "detect",
      });
      return undefined;
    }

    const onAccountsChanged = (accounts) => {
      if (!active) return;
      const next = Array.isArray(accounts) ? String(accounts[0] || "").trim() : "";
      setMetamaskState((prev) => ({
        ...prev,
        checked: true,
        available: true,
        injectedAvailable: true,
        address: next,
        lastEvent: "accountsChanged",
      }));
      setMetamaskNotice(next ? "MetaMask account changed." : "MetaMask reported no connected account.");
      setMetamaskError("");
    };

    const onChainChanged = (chainId) => {
      if (!active) return;
      setMetamaskState((prev) => ({
        ...prev,
        checked: true,
        available: true,
        injectedAvailable: true,
        chainId: normalizeEvmChainId(chainId),
        lastEvent: "chainChanged",
      }));
      setMetamaskNotice("MetaMask network changed.");
      setMetamaskError("");
    };

    try {
      provider.on?.("accountsChanged", onAccountsChanged);
      provider.on?.("chainChanged", onChainChanged);
    } catch {
      // Event support is optional; explicit refresh/connect controls remain available.
    }

    Promise.allSettled([
      provider.request({ method: "eth_accounts" }),
      provider.request({ method: "eth_chainId" }),
    ]).then(([accountsResult, chainResult]) => {
      if (!active) return;
      const accounts = accountsResult.status === "fulfilled" && Array.isArray(accountsResult.value)
        ? accountsResult.value
        : [];
      const chainId = chainResult.status === "fulfilled" ? normalizeEvmChainId(chainResult.value) : "";
      setMetamaskState({
        checked: true,
        available: true,
        injectedAvailable: true,
        address: String(accounts[0] || "").trim(),
        chainId,
        lastEvent: "initial_silent_read",
      });
    }).catch(() => {
      if (!active) return;
      setMetamaskState((prev) => ({ ...prev, checked: true, available: true, injectedAvailable: true }));
    });

    return () => {
      active = false;
      try {
        provider.removeListener?.("accountsChanged", onAccountsChanged);
        provider.removeListener?.("chainChanged", onChainChanged);
      } catch {
        // ignore provider cleanup failures
      }
    };
  }, []);

  useEffect(() => {
    loadAddresses();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="utt-wallet-addresses-cyber">
      <style>{WALLET_CYBER_CSS}</style>
      <div className="utt-wallet-titlebar">
        <div className="utt-wallet-title">Wallet Addresses // Chain Registry</div>
        <div className="utt-wallet-subtitle">[{tab}]</div>
        <div style={{ flex: 1 }} />
        <button onClick={() => (tab === "balances" ? loadBalances() : loadAddresses())} disabled={busy}>
          Refresh
        </button>
        {typeof onClose === "function" && <button onClick={onClose}>Close</button>}
      </div>

      <div className="utt-wallet-tabs">
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
        <div className="utt-wallet-error">
          <b>Error:</b> {err}
        </div>
      )}

      {tab === "addresses" && (
        <>
          <div className="utt-wallet-panel utt-wallet-metamask-panel">
            <div className="utt-wallet-panel-title">Robinhood Chain // MetaMask Link</div>

            <div className="utt-wallet-action-row" style={{ marginBottom: 8 }}>
              <span className={`utt-wallet-chip ${metamaskState.available ? "utt-wallet-chip--good" : metamaskState.injectedAvailable ? "utt-wallet-chip--warn" : "utt-wallet-chip--bad"}`}>
                {!metamaskState.checked
                  ? "Checking provider"
                  : metamaskState.available
                    ? "MetaMask detected"
                    : metamaskState.injectedAvailable
                      ? "Other EVM provider"
                      : "MetaMask unavailable"}
              </span>
              <span className={`utt-wallet-chip ${metamaskConnected ? "utt-wallet-chip--good" : "utt-wallet-chip--neutral"}`}>
                {metamaskConnected ? "Account connected" : "UTT disconnected"}
              </span>
              <span className={`utt-wallet-chip ${metamaskOnRobinhoodChain ? "utt-wallet-chip--good" : metamaskState.chainId ? "utt-wallet-chip--warn" : "utt-wallet-chip--neutral"}`}>
                {metamaskOnRobinhoodChain ? "Robinhood Chain" : metamaskState.chainId ? "Wrong network" : "Chain unknown"}
              </span>
            </div>

            <div className="utt-wallet-metamask-grid">
              <div className="utt-wallet-metamask-cell">
                <div className="utt-wallet-metamask-label">Provider</div>
                <div className="utt-wallet-metamask-value">{metamaskState.available ? "MetaMask / EIP-1193" : metamaskState.injectedAvailable ? "Non-MetaMask EVM provider" : "Not detected"}</div>
              </div>
              <div className="utt-wallet-metamask-cell" title={hideTableData ? "Address hidden" : metamaskState.address || "No connected account"}>
                <div className="utt-wallet-metamask-label">Selected account</div>
                <div className="utt-wallet-metamask-value">{metamaskConnected ? redacted(compactEvmAddress(metamaskState.address)) : "—"}</div>
              </div>
              <div className="utt-wallet-metamask-cell">
                <div className="utt-wallet-metamask-label">Current chain</div>
                <div className="utt-wallet-metamask-value">{metamaskState.chainId ? `${chainIdDecimalLabel(metamaskState.chainId)} / ${metamaskChainIdHex}` : "Unknown"}</div>
              </div>
              <div className="utt-wallet-metamask-cell">
                <div className="utt-wallet-metamask-label">Expected chain</div>
                <div className="utt-wallet-metamask-value">{ROBINHOOD_CHAIN_NETWORK.chainIdDecimal} / {ROBINHOOD_CHAIN_NETWORK.chainIdHex}</div>
              </div>
            </div>

            <div className="utt-wallet-action-row">
              <button type="button" className="utt-wallet-primary-button" onClick={connectMetaMask} disabled={busy || metamaskBusy || !metamaskState.available}>
                {metamaskConnected ? "Refresh MetaMask account" : "Connect MetaMask"}
              </button>
              <button type="button" onClick={switchMetaMaskToRobinhoodChain} disabled={busy || metamaskBusy || !metamaskState.available || metamaskOnRobinhoodChain}>
                Switch to Robinhood Chain
              </button>
              <button type="button" onClick={addRobinhoodChainToMetaMask} disabled={busy || metamaskBusy || !metamaskState.available}>
                Add Robinhood Chain
              </button>
              <button type="button" onClick={useConnectedMetaMaskAddress} disabled={busy || metamaskBusy || !metamaskConnected}>
                Use connected address
              </button>
              <button type="button" className="utt-wallet-good-button" onClick={saveConnectedMetaMaskWallet} disabled={busy || metamaskBusy || !metamaskReady}>
                Save Robinhood Chain wallet
              </button>
              <button type="button" onClick={clearLocalMetaMaskState} disabled={busy || metamaskBusy || !metamaskConnected}>
                Clear UTT state
              </button>
              {metamaskBusy ? <span className="utt-wallet-subtitle">MetaMask request pending…</span> : null}
            </div>

            <div className="utt-wallet-metamask-note" style={{ marginTop: 9 }}>
              Connection is read-only. UTT requests only account access and network information. Saving creates an <code>ALL</code> metadata row for <code>robinhood_chain</code>; it does not read balances, sign messages, approve tokens, or send transactions.
            </div>
            {metamaskNotice ? <div className="utt-wallet-metamask-message">{metamaskNotice}</div> : null}
            {metamaskError ? <div className="utt-wallet-metamask-message utt-wallet-metamask-message--error">{metamaskError}</div> : null}
          </div>

          <div className="utt-wallet-panel">
            <div className="utt-wallet-panel-title">Add Wallet Address</div>

            <div style={{ marginBottom: 10, display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
              <div style={{ fontWeight: 600, opacity: 0.9 }}>Hydration/SubWallet:</div>
              <button type="button" onClick={applyHydrationWalletMode} disabled={busy}>
                Use Hydration wallet
              </button>
              <div style={{ flexBasis: "100%", opacity: 0.75 }}>
                Use one account-level Hydration row: <b>Asset</b> <code>ALL</code>, <b>Venue</b> <code>polkadot_hydration</code>, and <b>Network</b> <code>hydration</code>. The same SubWallet/Substrate address can be scanned for all supported Hydration assets; no per-asset address rows are needed.
                For Solana bridge reserve / treasury rows, use <b>Asset</b> <code>UTTT</code> once the Token Registry has the Solana UTTT mint/decimals row. Use <code>ALL</code> only as metadata-only fallback.
              </div>
            </div>

            <div className="utt-wallet-form-grid">
              <label>Asset / scope</label>
              <input
                placeholder="e.g. BTC, SOL, UTTT, or ALL for metadata-only rows"
                value={form.asset}
                onChange={(e) => setForm((p) => ({ ...p, asset: e.target.value.toUpperCase() }))}
              />

              <label>Venue</label>
              <input
                placeholder="e.g. robinhood_chain, polkadot_hydration, robinhood (blank = self-custody)"
                value={form.wallet_id}
                onChange={(e) => setForm((p) => ({ ...p, wallet_id: e.target.value }))}
              />

              <label>Network</label>
              <input placeholder="e.g. hydration, solana, mainnet" value={form.network} onChange={(e) => setForm((p) => ({ ...p, network: e.target.value }))} />

              <label>Address</label>
              <input placeholder="SubWallet/Substrate or chain address" value={form.address} onChange={(e) => setForm((p) => ({ ...p, address: e.target.value }))} />

              <label>Label</label>
              <input value={form.label} onChange={(e) => setForm((p) => ({ ...p, label: e.target.value }))} />

              <label>Owner scope</label>
              <input value={form.owner_scope} onChange={(e) => setForm((p) => ({ ...p, owner_scope: e.target.value }))} />
            </div>

            <div className="utt-wallet-action-row" style={{ marginTop: 10 }}>
              <button className="utt-wallet-primary-button" onClick={createAddress} disabled={busy || !String(form.address || "").trim()}>
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
                Hydration/SubWallet rows should normally use <b>Asset</b> <code>ALL</code>, <b>Venue</b> <code>polkadot_hydration</code>, and <b>Network</b> <code>hydration</code>. Solana SPL treasury rows such as UTTT should use the token symbol when the mint is registered. Blank venue remains self-custody.
              </div>
            </div>
          </div>

          <div className="utt-wallet-panel">
            <div className="utt-wallet-panel-title">Tx Ingest → Deposits/Withdrawals</div>

            <div className="utt-wallet-form-grid">
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

            <div className="utt-wallet-action-row" style={{ marginTop: 10 }}>
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
                Uses <code>/api/wallet_addresses/tx/ingest</code>. Enforces policy (skip coinbase; deposits-only robinhood/dex-trade; self-custody both). Hydration wallet rows use <code>ALL</code> + <code>polkadot_hydration</code> + <code>hydration</code>; backend asset scanning/tx ingest support remains endpoint-dependent.
              </div>
            </div>

            {txLastResult ? (
              <div className="utt-wallet-terminal-output">
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

          <div className="utt-wallet-panel">
            <div className="utt-wallet-panel-title">Filters</div>
            <div className="utt-wallet-form-grid">
              <label>Asset</label>
              <input value={flt.asset} onChange={(e) => setFlt((p) => ({ ...p, asset: e.target.value.toUpperCase() }))} />

              <label>Venue</label>
              <input
                placeholder="e.g. robinhood_chain, polkadot_hydration, robinhood"
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

            <div className="utt-wallet-action-row" style={{ marginTop: 10 }}>
              <button onClick={loadAddresses} disabled={busy}>
                Apply
              </button>
              <button
                type="button"
                onClick={() => setFlt((p) => ({ ...p, wallet_id: "polkadot_hydration", network: "hydration" }))}
                disabled={busy}
              >
                Filter Hydration
              </button>
              <button type="button" onClick={() => setFlt((p) => ({ ...p, asset: "", wallet_id: "", network: "" }))} disabled={busy}>
                Clear filters
              </button>
            </div>
          </div>

          <div className="utt-wallet-panel">
            <div className="utt-wallet-panel-title">Wallet Addresses</div>
            <div className="utt-wallet-table-wrap">
              <table className="utt-wallet-table">
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
                        <button className="utt-wallet-danger-button" onClick={() => deleteAddress(a.id)} disabled={busy}>
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
          <div className="utt-wallet-panel">
            <div className="utt-wallet-panel-title">Balances Controls</div>
            <div className="utt-wallet-form-grid">
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

            <div className="utt-wallet-action-row" style={{ marginTop: 10 }}>
              <button onClick={loadBalances} disabled={busy}>
                Load latest
              </button>
              <button onClick={() => refreshBalances(null)} disabled={busy}>
                Refresh all balances
              </button>

              {balLastRefresh ? (
                <div style={{ opacity: 0.85 }}>
                  Last refresh: <b>{Number(balLastRefresh.refreshed || 0)}</b> refreshed •{" "}
                  <b>{Array.isArray(balLastRefresh.errors) ? balLastRefresh.errors.length : 0}</b> errors
                  {balLastRefresh?.hydration_live?.attempted ? (
                    <span>
                      {" "}• Hydration live: <b>{Number(balLastRefresh.hydration_live.refreshed || 0)}</b> wallets / <b>{Number(balLastRefresh.hydration_live.rows || 0)}</b> assets
                    </span>
                  ) : null}
                </div>
              ) : (
                <div style={{ opacity: 0.75 }}>
                  Explorer refresh may take time per address. Hydration/SubWallet <code>ALL</code> rows use the live Polkadot-Hydration balance endpoint instead of the legacy explorer snapshot path.
                </div>
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

          <div className="utt-wallet-panel">
            <div className="utt-wallet-panel-title">Latest Balances</div>
            <div className="utt-wallet-table-wrap">
              <table className="utt-wallet-table">
                <thead>
                  <tr>
                    {["id", "asset", "network", "address", "label", "balance", "usd_price", "usd_value", "usd_source_symbol", "fetched_at"].map((h) => (
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
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.usd_source_symbol ?? b.usd_source ?? b.price_source ?? "")}</td>
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
