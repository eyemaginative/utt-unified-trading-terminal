// frontend/src/OrderTicketWidget.jsx

import { useEffect, useMemo, useRef, useState } from "react";
import { Connection, clusterApiUrl } from "@solana/web3.js";
import { useWallet } from "@solana/wallet-adapter-react";
import { UnifiedWalletButton } from "@jup-ag/wallet-adapter";
import { getOrderRules } from "./lib/api";
import { expandExponential } from "./lib/format";

// Auth (local token) — used to gate funds actions.
const UTT_AUTH_TOKEN_KEY = 'utt_auth_token_v1';
function getAuthToken() {
  try { return localStorage.getItem(UTT_AUTH_TOKEN_KEY) || ''; } catch { return ''; }
}

const LS_OT_SOL_WALLET = "utt_ot_sol_wallet_v1";
const LS_OT_SOL_ROUTER = "utt_ot_sol_router_v1";

function getPreferredSolanaWalletKey() {
  try { return localStorage.getItem(LS_OT_SOL_WALLET) || "solflare"; } catch { return "solflare"; }
}
function setPreferredSolanaWalletKey(v) {
  try { localStorage.setItem(LS_OT_SOL_WALLET, String(v || "solflare")); } catch {}
}
function getPreferredSolanaRouterMode() {
  try {
    const raw = String(localStorage.getItem(LS_OT_SOL_ROUTER) || "auto").toLowerCase().trim();
    const v = raw === "jupiter" ? "metis" : raw; // back-compat
    return v === "ultra" || v === "metis" || v === "raydium" ? v : "auto";
  } catch {
    return "auto";
  }
}
function setPreferredSolanaRouterMode(v) {
  try {
    const next = String(v || "auto").toLowerCase().trim();
    localStorage.setItem(LS_OT_SOL_ROUTER, next === "ultra" || next === "metis" || next === "raydium" ? next : "auto");
  } catch {}
}

function solanaProviderPubkeyBase58(provider) {
  try {
    const pk = provider?.publicKey;
    if (!pk) return null;
    if (typeof pk?.toBase58 === "function") return pk.toBase58();
    if (typeof pk?.toString === "function") return pk.toString();
    if (typeof pk === "string") return pk;
    return null;
  } catch {
    return null;
  }
}

function isSolanaProviderLike(provider) {
  try {
    return !!provider && (
      typeof provider?.connect === "function" ||
      typeof provider?.signTransaction === "function" ||
      typeof provider?.signAndSendTransaction === "function" ||
      !!provider?.publicKey
    );
  } catch {
    return false;
  }
}

function unwrapSolanaProvider(candidate) {
  try {
    if (!candidate) return null;
    if (isSolanaProviderLike(candidate)) return candidate;
    if (isSolanaProviderLike(candidate?.solana)) return candidate.solana;
    if (isSolanaProviderLike(candidate?.provider)) return candidate.provider;
    return null;
  } catch {
    return null;
  }
}

function isJupiterLikeProvider(provider) {
  try {
    if (!provider) return false;
    return !!(
      provider?.isJupiter ||
      provider?.isJupiterWallet ||
      provider?.isJup ||
      provider?.isJupWallet ||
      String(provider?.name || provider?.walletName || provider?.providerName || "").toLowerCase().includes("jupiter")
    );
  } catch {
    return false;
  }
}

const WALLET_STANDARD_REGISTER_EVENT = "register-wallet";
const WALLET_STANDARD_APP_READY_EVENT = "app-ready";

function getWalletStandardState() {
  try {
    const w = typeof window !== "undefined" ? window : null;
    if (!w) return null;
    if (!w.__uttWalletStandardState) {
      w.__uttWalletStandardState = {
        initialized: false,
        primed: false,
        wallets: [],
        seen: new Set(),
        listeners: new Set(),
        onRegister: null,
      };
    }
    return w.__uttWalletStandardState;
  } catch {
    return null;
  }
}

function walletStandardWalletId(wallet) {
  try {
    const name = String(wallet?.name || "").trim().toLowerCase();
    const version = String(wallet?.version || "").trim().toLowerCase();
    const chains = Array.isArray(wallet?.chains) ? wallet.chains.map((x) => String(x || "").trim().toLowerCase()).sort().join(",") : "";
    const features = wallet?.features && typeof wallet.features === "object"
      ? Object.keys(wallet.features).map((x) => String(x || "").trim().toLowerCase()).sort().join(",")
      : "";
    const icon = String(wallet?.icon || "").trim().toLowerCase();
    return [name, version, chains, features, icon].join("|");
  } catch {
    return "";
  }
}

function isWalletStandardSolanaWallet(wallet) {
  try {
    if (!wallet || typeof wallet !== "object") return false;
    const name = String(wallet?.name || "").trim();
    if (!name) return false;

    const chains = Array.isArray(wallet?.chains) ? wallet.chains.map((x) => String(x || "").toLowerCase()) : [];
    const featureKeys = wallet?.features && typeof wallet.features === "object"
      ? Object.keys(wallet.features).map((x) => String(x || "").toLowerCase())
      : [];

    return (
      chains.some((c) => c.includes("solana")) ||
      featureKeys.some((k) => k.includes("solana:")) ||
      isJupiterLikeProvider(wallet)
    );
  } catch {
    return false;
  }
}

function isWalletStandardJupiterWallet(wallet) {
  try {
    return isJupiterLikeProvider(wallet) || String(wallet?.name || "").toLowerCase().includes("jupiter");
  } catch {
    return false;
  }
}

function notifyWalletStandardListeners() {
  try {
    const st = getWalletStandardState();
    if (!st) return;
    const snapshot = Array.isArray(st.wallets) ? st.wallets.slice() : [];
    for (const cb of st.listeners || []) {
      try { cb(snapshot); } catch {}
    }
  } catch {
    // ignore
  }
}

function addWalletStandardWalletCandidate(candidate) {
  try {
    const wallet =
      candidate?.wallet ||
      candidate?.adapter ||
      candidate ||
      null;

    if (!isWalletStandardSolanaWallet(wallet)) return;

    const st = getWalletStandardState();
    if (!st) return;

    const id = walletStandardWalletId(wallet);
    if (!id) return;
    if (st.seen.has(id)) return;

    st.seen.add(id);
    st.wallets = [...(Array.isArray(st.wallets) ? st.wallets : []), wallet];
    notifyWalletStandardListeners();
  } catch {
    // ignore
  }
}

function handleWalletStandardRegisterEvent(event) {
  try {
    const detail = event?.detail;

    if (typeof detail?.register === "function") {
      detail.register((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    if (typeof detail === "function") {
      detail((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    if (Array.isArray(detail?.wallets)) {
      detail.wallets.forEach((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    if (Array.isArray(detail)) {
      detail.forEach((wallet) => addWalletStandardWalletCandidate(wallet));
      return;
    }

    addWalletStandardWalletCandidate(detail?.wallet || detail);
  } catch {
    // ignore
  }
}

function ensureWalletStandardBridge() {
  try {
    const st = getWalletStandardState();
    const w = typeof window !== "undefined" ? window : null;
    if (!st || !w || st.initialized) return;

    st.initialized = true;
    st.onRegister = (event) => handleWalletStandardRegisterEvent(event);

    w.addEventListener(WALLET_STANDARD_REGISTER_EVENT, st.onRegister);

    try {
      w.dispatchEvent(new Event(WALLET_STANDARD_APP_READY_EVENT));
    } catch {
      // ignore
    }
  } catch {
    // ignore
  }
}

async function primeWalletStandardWallets() {
  try {
    const st = getWalletStandardState();
    if (!st || st.primed) return;
    st.primed = true;
    ensureWalletStandardBridge();

    try {
      // Keep this package-free so Vite does not hard-fail when @wallet-standard/app
      // is not installed in the frontend. Wallet-standard extensions can still
      // register through the browser event bridge above.
      const navWallets = typeof navigator !== "undefined" ? navigator?.wallets : null;
      const wallets =
        Array.isArray(navWallets) ? navWallets :
        Array.isArray(navWallets?.wallets) ? navWallets.wallets :
        typeof navWallets?.get === "function" ? navWallets.get() :
        [];
      if (Array.isArray(wallets)) {
        wallets.forEach((wallet) => addWalletStandardWalletCandidate(wallet));
      }
    } catch {
      // Event bridge still works without any helper package.
    }
  } catch {
    // ignore
  }
}

function getWalletStandardWallets() {
  try {
    ensureWalletStandardBridge();
    const st = getWalletStandardState();
    return Array.isArray(st?.wallets) ? st.wallets.slice() : [];
  } catch {
    return [];
  }
}

function subscribeWalletStandardWallets(callback) {
  try {
    ensureWalletStandardBridge();
    const st = getWalletStandardState();
    if (!st || typeof callback !== "function") return () => {};

    st.listeners.add(callback);
    callback(getWalletStandardWallets());
    return () => {
      try { st.listeners.delete(callback); } catch {}
    };
  } catch {
    return () => {};
  }
}

const B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

function base58EncodeBytes(bytesLike) {
  try {
    const bytes = bytesLike instanceof Uint8Array ? bytesLike : Uint8Array.from(bytesLike || []);
    if (!bytes.length) return "";
    let zeros = 0;
    while (zeros < bytes.length && bytes[zeros] === 0) zeros += 1;

    let digits = [0];
    for (let i = zeros; i < bytes.length; i += 1) {
      let carry = bytes[i];
      for (let j = 0; j < digits.length; j += 1) {
        const x = digits[j] * 256 + carry;
        digits[j] = x % 58;
        carry = Math.floor(x / 58);
      }
      while (carry > 0) {
        digits.push(carry % 58);
        carry = Math.floor(carry / 58);
      }
    }

    let out = "1".repeat(zeros);
    for (let i = digits.length - 1; i >= 0; i -= 1) out += B58_ALPHABET[digits[i]];
    return out;
  } catch {
    return "";
  }
}

function coerceWalletStandardSignature(value) {
  try {
    if (!value) return null;
    if (typeof value === "string") return value;
    if (value instanceof Uint8Array) return base58EncodeBytes(value);
    if (ArrayBuffer.isView(value)) return base58EncodeBytes(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    if (value instanceof ArrayBuffer) return base58EncodeBytes(new Uint8Array(value));
    if (Array.isArray(value)) return base58EncodeBytes(Uint8Array.from(value));
    return null;
  } catch {
    return null;
  }
}

function walletStandardAccountAddress(account) {
  try {
    return String(account?.address || account?.publicKey || "").trim() || null;
  } catch {
    return null;
  }
}

function walletStandardPublicKeyShim(address) {
  if (!address) return null;
  return {
    toBase58: () => String(address),
    toString: () => String(address),
  };
}

function walletStandardSolanaChain(wallet) {
  try {
    const chains = Array.isArray(wallet?.chains) ? wallet.chains.map((x) => String(x || "")) : [];
    return chains.find((c) => c.toLowerCase().includes("solana")) || "solana:mainnet";
  } catch {
    return "solana:mainnet";
  }
}

async function callWalletStandardFeatureMethod(featureObj, methodName, input) {
  if (!featureObj || typeof featureObj?.[methodName] !== "function") {
    throw new Error(`Wallet missing ${methodName}`);
  }
  try {
    return await featureObj[methodName](input);
  } catch (e1) {
    try {
      return await featureObj[methodName]([input]);
    } catch {
      throw e1;
    }
  }
}

function createWalletStandardSolanaProvider(wallet) {
  const provider = {
    __walletStandard: true,
    __walletStandardWallet: wallet,
    __walletStandardAccount: (Array.isArray(wallet?.accounts) ? wallet.accounts[0] : null) || null,
    get publicKey() {
      const account = this.__walletStandardAccount || (Array.isArray(this.__walletStandardWallet?.accounts) ? this.__walletStandardWallet.accounts[0] : null);
      return walletStandardPublicKeyShim(walletStandardAccountAddress(account));
    },
    async connect() {
      const feature = this.__walletStandardWallet?.features?.["standard:connect"];
      if (feature && typeof feature.connect === "function") {
        const out = await feature.connect();
        const accounts =
          Array.isArray(out?.accounts) ? out.accounts :
          Array.isArray(this.__walletStandardWallet?.accounts) ? this.__walletStandardWallet.accounts :
          [];
        this.__walletStandardAccount = accounts[0] || this.__walletStandardAccount || null;
      } else if (!this.__walletStandardAccount && Array.isArray(this.__walletStandardWallet?.accounts)) {
        this.__walletStandardAccount = this.__walletStandardWallet.accounts[0] || null;
      }

      return this.publicKey ? { publicKey: this.publicKey } : null;
    },
    async signTransaction(transaction) {
      const wallet = this.__walletStandardWallet;
      const account = this.__walletStandardAccount || (await this.connect(), this.__walletStandardAccount);
      if (!account) throw new Error("Wallet account unavailable.");

      const feature = wallet?.features?.["solana:signTransaction"];
      const out = await callWalletStandardFeatureMethod(feature, "signTransaction", {
        account,
        chain: walletStandardSolanaChain(wallet),
        transaction,
      });

      const first = Array.isArray(out) ? out[0] : out;
      return first?.signedTransaction || first?.transaction || first || null;
    },
    async signAndSendTransaction(transaction) {
      const wallet = this.__walletStandardWallet;
      const account = this.__walletStandardAccount || (await this.connect(), this.__walletStandardAccount);
      if (!account) throw new Error("Wallet account unavailable.");

      const feature = wallet?.features?.["solana:signAndSendTransaction"];
      const out = await callWalletStandardFeatureMethod(feature, "signAndSendTransaction", {
        account,
        chain: walletStandardSolanaChain(wallet),
        transaction,
      });

      const first = Array.isArray(out) ? out[0] : out;
      const signature =
        coerceWalletStandardSignature(first?.signature) ||
        coerceWalletStandardSignature(Array.isArray(first?.signatures) ? first.signatures[0] : null);

      if (!signature) throw new Error("Wallet did not return a signature.");
      return { signature };
    },
  };

  return provider;
}

function collectSolanaProviderCandidates(root) {
  const out = [];
  const push = (candidate) => {
    try {
      if (!candidate) return;
      out.push(candidate);
      if (Array.isArray(candidate?.providers)) {
        for (const p of candidate.providers) out.push(p);
      }
      if (Array.isArray(candidate?.wallets)) {
        for (const p of candidate.wallets) out.push(p);
      }
    } catch {
      // ignore
    }
  };

  try {
    push(root?.solana);
    push(root?.phantom);
    push(root?.phantom?.solana);
    push(root?.solflare);
    push(root?.solflare?.solana);
    push(root?.backpack);
    push(root?.backpack?.solana);
    push(root?.jupiterWallet);
    push(root?.jupiterWallet?.solana);
    push(root?.jupiter);
    push(root?.jupiter?.solana);
    push(root?.jup);
    push(root?.jup?.solana);
    push(root?.xnft?.solana);
  } catch {
    // ignore
  }

  return out;
}

function classifyInjectedSolanaProvider(candidate) {
  try {
    const provider = unwrapSolanaProvider(candidate) || candidate || null;
    if (!provider) return { key: null, provider: null };

    if (isJupiterLikeProvider(candidate) || isJupiterLikeProvider(provider)) {
      return { key: "jupiter", provider };
    }
    if (provider?.isPhantom || candidate?.isPhantom) {
      return { key: "phantom", provider };
    }
    if (provider?.isBackpack || candidate?.isBackpack) {
      return { key: "backpack", provider };
    }
    if (provider?.isSolflare || candidate?.isSolflare) {
      return { key: "solflare", provider };
    }

    const nameBlob = [
      candidate?.name,
      candidate?.walletName,
      candidate?.providerName,
      provider?.name,
      provider?.walletName,
      provider?.providerName,
    ].map((x) => String(x || "").toLowerCase()).join(" ");

    if (nameBlob.includes("jupiter")) return { key: "jupiter", provider };
    if (nameBlob.includes("phantom")) return { key: "phantom", provider };
    if (nameBlob.includes("backpack")) return { key: "backpack", provider };
    if (nameBlob.includes("solflare")) return { key: "solflare", provider };

    return { key: null, provider: null };
  } catch {
    return { key: null, provider: null };
  }
}

function getInjectedSolanaProviders(walletStandardWallets = null) {
  try {
    ensureWalletStandardBridge();
    const w = typeof window !== "undefined" ? window : null;
    if (!w) return {};

    const providers = {};
    const candidates = collectSolanaProviderCandidates(w);
    for (const candidate of candidates) {
      const { key, provider } = classifyInjectedSolanaProvider(candidate);
      if (!key || !provider || providers[key]) continue;
      providers[key] = provider;
    }

    const walletStandardList = Array.isArray(walletStandardWallets) ? walletStandardWallets : getWalletStandardWallets();
    const wsJupiter = walletStandardList.find((wallet) => isWalletStandardJupiterWallet(wallet));
    if (!providers.jupiter && wsJupiter) {
      providers.jupiter = createWalletStandardSolanaProvider(wsJupiter);
    }

    return providers;
  } catch {
    return {};
  }
}

function getInjectedSolanaProvider(preferred = "solflare", walletStandardWallets = null) {
  const providers = getInjectedSolanaProviders(walletStandardWallets);
  const order = ["jupiter", "solflare", "phantom", "backpack"];

  const pref = String(preferred || "solflare").toLowerCase().trim();
  if (providers[pref]) return { key: pref, provider: providers[pref] };

  for (const key of order) {
    const p = providers[key];
    if (p && solanaProviderPubkeyBase58(p)) return { key, provider: p };
  }
  for (const key of order) {
    const p = providers[key];
    if (p) return { key, provider: p };
  }
  return { key: null, provider: null };
}

function getInstalledSolanaWalletOptions(walletStandardWallets = null) {
  const providers = getInjectedSolanaProviders(walletStandardWallets);
  const labels = { jupiter: "Jupiter", solflare: "Solflare", phantom: "Phantom", backpack: "Backpack" };
  const order = ["jupiter", "solflare", "phantom", "backpack"];
  return order.filter((k) => !!providers[k]).map((k) => ({ key: k, label: labels[k] || k }));
}


const LS_OT_BOX = "utt_ot_box_v2";
const LS_OT_LOCK = "utt_ot_lock_v2";

// Back-compat storage keys (Total was originally "USD sizing")
const LS_OT_TOTAL_USD = "utt_ot_total_usd_v1";
const LS_OT_AUTOQTY = "utt_ot_autoqty_v1";

// ─────────────────────────────────────────────────────────────
// Safe environment helpers (prevents “blank UI” from storage/window issues)
// ─────────────────────────────────────────────────────────────
const HAS_WINDOW = typeof window !== "undefined";
function lsGet(key, fallback = null) {
  try {
    if (typeof localStorage === "undefined") return fallback;
    const v = localStorage.getItem(key);
    return v === null || v === undefined ? fallback : v;
  } catch {
    return fallback;
  }
}
function lsSet(key, value) {
  try {
    if (typeof localStorage === "undefined") return;
    localStorage.setItem(key, value);
  } catch {
    // ignore
  }
}
function safeJsonParse(s, fallback = null) {
  try {
    return JSON.parse(s);
  } catch {
    return fallback;
  }
}

function extractRulesError(e) {
  // Axios shape
  const status = e?.response?.status;
  const data = e?.response?.data;

  const detail =
    typeof data === "string"
      ? data
      : data?.detail
        ? typeof data.detail === "string"
          ? data.detail
          : JSON.stringify(data.detail)
        : null;

  if (status && detail) return `HTTP ${status}: ${detail}`;
  if (status) return `HTTP ${status}`;
  if (e?.message) return e.message;

  try {
    return JSON.stringify(e);
  } catch {
    return "Failed loading rules";
  }
}


function classifyWalletAdapterNameToKey(nameLike) {
  try {
    const s = String(nameLike || "").toLowerCase().trim();
    if (!s) return null;
    if (s.includes("jupiter") || s.includes("jup.ag") || s === "jup" || s.includes(" jup ")) return "jupiter";
    if (s.includes("solflare")) return "solflare";
    if (s.includes("phantom")) return "phantom";
    if (s.includes("backpack")) return "backpack";
    return null;
  } catch {
    return null;
  }
}

function createWalletAdapterBridgeProvider(walletApi, connection) {
  if (!walletApi) return null;

  return {
    __walletAdapterBridge: true,
    get publicKey() {
      return walletApi?.publicKey || null;
    },
    async connect() {
      if (walletApi?.connected && walletApi?.publicKey) {
        return { publicKey: walletApi.publicKey };
      }
      if (typeof walletApi?.connect === "function") {
        await walletApi.connect();
      }
      return walletApi?.publicKey ? { publicKey: walletApi.publicKey } : null;
    },
    async signTransaction(transaction) {
      if (typeof walletApi?.signTransaction !== "function") {
        throw new Error("Selected wallet does not support signTransaction.");
      }
      return await walletApi.signTransaction(transaction);
    },
    async signAndSendTransaction(transaction) {
      if (typeof walletApi?.sendTransaction === "function") {
        const signature = await walletApi.sendTransaction(transaction, connection);
        if (!signature) throw new Error("Wallet did not return a signature.");
        return { signature: String(signature) };
      }
      if (typeof walletApi?.signTransaction === "function") {
        const signed = await walletApi.signTransaction(transaction);
        const raw = typeof signed?.serialize === "function" ? signed.serialize() : null;
        if (!raw) throw new Error("Wallet did not return a serializable signed transaction.");
        const signature = await connection.sendRawTransaction(raw);
        if (!signature) throw new Error("RPC did not return a signature.");
        return { signature: String(signature) };
      }
      throw new Error("Selected wallet does not support sendTransaction or signTransaction.");
    },
  };
}

function shortenWalletAddress(addr, left = 6, right = 4) {
  try {
    const s = String(addr || "").trim();
    if (!s) return "";
    if (s.length <= left + right + 1) return s;
    return `${s.slice(0, left)}…${s.slice(-right)}`;
  } catch {
    return "";
  }
}

function getSolanaWalletVisualMeta(key, nameLike, iconLike) {
  try {
    const k = String(key || "").toLowerCase().trim();
    const name = String(nameLike || "").trim();
    const icon = String(iconLike || "").trim();

    const presets = {
      jupiter: {
        label: "Jupiter",
        color: "#43d3c5",
        border: "rgba(67, 211, 197, 0.35)",
        glow: "rgba(67, 211, 197, 0.18)",
        fallbackBg: "linear-gradient(135deg, #36cfc9, #2f7cf6)",
        fallbackFg: "#071014",
        fallbackText: "J",
      },
      solflare: {
        label: "Solflare",
        color: "#f7d34a",
        border: "rgba(247, 211, 74, 0.35)",
        glow: "rgba(247, 211, 74, 0.18)",
        fallbackBg: "#f7d34a",
        fallbackFg: "#101010",
        fallbackText: "S",
      },
      phantom: {
        label: "Phantom",
        color: "#a78bfa",
        border: "rgba(167, 139, 250, 0.35)",
        glow: "rgba(167, 139, 250, 0.18)",
        fallbackBg: "#8b5cf6",
        fallbackFg: "#ffffff",
        fallbackText: "P",
      },
      backpack: {
        label: "Backpack",
        color: "#ef4444",
        border: "rgba(239, 68, 68, 0.35)",
        glow: "rgba(239, 68, 68, 0.18)",
        fallbackBg: "#ef4444",
        fallbackFg: "#ffffff",
        fallbackText: "B",
      },
    };

    const byKey = presets[k];
    if (byKey) {
      return {
        ...byKey,
        icon,
        label: byKey.label || name || "Wallet",
      };
    }

    return {
      label: name || "Wallet",
      color: "#7dd3fc",
      border: "rgba(125, 211, 252, 0.30)",
      glow: "rgba(125, 211, 252, 0.16)",
      fallbackBg: "#0f172a",
      fallbackFg: "#e5f3ff",
      fallbackText: String((name || "W").slice(0, 1) || "W").toUpperCase(),
      icon,
    };
  } catch {
    return {
      label: "Wallet",
      color: "#7dd3fc",
      border: "rgba(125, 211, 252, 0.30)",
      glow: "rgba(125, 211, 252, 0.16)",
      fallbackBg: "#0f172a",
      fallbackFg: "#e5f3ff",
      fallbackText: "W",
      icon: "",
    };
  }
}

export default function OrderTicketWidget({
  apiBase,
  effectiveVenue,
  fmtNum,
  styles,
  otSymbol,
  setOtSymbol,
  appContainerRef,
  hideVenueNames = false,

  // driven by App.jsx "Hide table data" checkbox
  hideTableData = false,

  qty: qtyProp,
  setQty: setQtyProp,
  limitPrice: limitPriceProp,
  setLimitPrice: setLimitPriceProp,
}) {
  // Optional toast emitter (some app shells inject this; keep safe/no-op if absent)
  const onToast = (typeof window !== "undefined" && (window.__uttOnToast || window.uttOnToast))
    ? (window.__uttOnToast || window.uttOnToast)
    : undefined;

  const walletKit = useWallet();
  const walletKitButtonHostRef = useRef(null);
  const solanaRpcConnection = useMemo(() => new Connection(clusterApiUrl("mainnet-beta"), "confirmed"), []);

  const [side, setSide] = useState("buy");
  const [solanaOrderMode, setSolanaOrderMode] = useState("swap"); // solana_jupiter only: "swap" | "limit"
  const JUPITER_LIMIT_MIN_USD = 10.10;

  const [qtyLocal, setQtyLocal] = useState("");
  const [limitPriceLocal, setLimitPriceLocal] = useState("");

  const qty = qtyProp !== undefined ? qtyProp : qtyLocal;
  const setQty = typeof setQtyProp === "function" ? setQtyProp : setQtyLocal;

  const limitPrice = limitPriceProp !== undefined ? limitPriceProp : limitPriceLocal;
  const setLimitPrice = typeof setLimitPriceProp === "function" ? setLimitPriceProp : setLimitPriceLocal;

  // NEW: prevents auto-normalization from fighting the user's typing
  const limitEditingRef = useRef(false);
  const limitSourceRef = useRef("init"); // "user" | "blur" | "auto" | "sci" | "init"

  const [postOnly, setPostOnly] = useState(false);
  const [tif, setTif] = useState("gtc");
  const [solanaExpiryPreset, setSolanaExpiryPreset] = useState("never"); // solana_jupiter limit only
  const [solanaExpiryCustom, setSolanaExpiryCustom] = useState("");
  const [clientOid, setClientOid] = useState("");

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState(null);
  const [submitOk, setSubmitOk] = useState(null);

  const [showConfirm, setShowConfirm] = useState(false);

  // NEW: submission result modal (instead of inline JSON block)
  const [showSubmitResult, setShowSubmitResult] = useState(false);
  const [submitResultKind, setSubmitResultKind] = useState(null); // "ok" | "error"
  const [submitResultPayload, setSubmitResultPayload] = useState(null); // object|string
  const [submitResultText, setSubmitResultText] = useState(""); // preformatted string for display/copy
  const [submitResultTitle, setSubmitResultTitle] = useState(""); // heading for modal

  const venueLabel = hideVenueNames ? "••••" : String(effectiveVenue || "");

  const isSolanaDexVenue = useMemo(() => {
    const v = String(effectiveVenue || "").toLowerCase().trim();
    return v === "solana_jupiter" || v === "solana_dex" || v.startsWith("solana_");
  }, [effectiveVenue]);
  const isSolanaJupiterVenue = useMemo(() => {
    const v = String(effectiveVenue || "").toLowerCase().trim();
    return v === "solana_jupiter";
  }, [effectiveVenue]);
  const isSolanaLimitMode = isSolanaJupiterVenue && solanaOrderMode === "limit";
  const [preferredSolanaWallet, setPreferredSolanaWallet] = useState(() => getPreferredSolanaWalletKey());
  const [preferredSolanaRouterMode, setPreferredSolanaRouterModeState] = useState(() => getPreferredSolanaRouterMode());
  const [walletStandardWallets, setWalletStandardWallets] = useState(() => getWalletStandardWallets());
  const [walletKitPendingConnectName, setWalletKitPendingConnectName] = useState("");
  useEffect(() => { setPreferredSolanaWalletKey(preferredSolanaWallet); }, [preferredSolanaWallet]);
  useEffect(() => { setPreferredSolanaRouterMode(preferredSolanaRouterMode); }, [preferredSolanaRouterMode]);
  useEffect(() => {
    if (!isSolanaDexVenue) return;
    const unsub = subscribeWalletStandardWallets((wallets) => setWalletStandardWallets(Array.isArray(wallets) ? wallets : []));
    void primeWalletStandardWallets().then(() => {
      try { setWalletStandardWallets(getWalletStandardWallets()); } catch {}
    });
    return () => {
      try { unsub?.(); } catch {}
    };
  }, [isSolanaDexVenue]);

  const walletKitRawAdapterName = useMemo(() => {
    return String(
      walletKit?.wallet?.adapter?.name ||
      walletKit?.wallet?.name ||
      walletKit?.wallet?.adapter?.url ||
      ""
    ).trim();
  }, [walletKit?.wallet]);

  const walletKitSelectedKey = useMemo(() => {
    const selectedName =
      walletKit?.wallet?.adapter?.name ||
      walletKit?.wallet?.adapter?.url ||
      walletKit?.wallet?.adapter?.icon ||
      walletKit?.wallet?.adapter?.publicKey ||
      walletKit?.wallet?.adapter?.toString?.() ||
      walletKit?.wallet?.name ||
      "";
    return classifyWalletAdapterNameToKey(selectedName);
  }, [walletKit?.wallet]);

  const walletKitConnected = useMemo(() => {
    return !!walletKit?.connected && !!walletKit?.publicKey;
  }, [walletKit?.connected, walletKit?.publicKey]);

  const walletKitBridgeProvider = useMemo(() => {
    if (!isSolanaDexVenue) return null;
    if (!walletKitSelectedKey) return null;
    return createWalletAdapterBridgeProvider(walletKit, solanaRpcConnection);
  }, [isSolanaDexVenue, walletKitSelectedKey, walletKit, solanaRpcConnection]);

  useEffect(() => {
    if (!isSolanaDexVenue) return;
    if (!walletKitConnected) return;
    if (!walletKitSelectedKey) return;
    if (preferredSolanaWallet === walletKitSelectedKey) return;
    setPreferredSolanaWallet(walletKitSelectedKey);
  }, [isSolanaDexVenue, walletKitConnected, walletKitSelectedKey, preferredSolanaWallet]);

  const resolveInjectedSolanaProvider = useMemo(() => {
    return (preferred) => {
      const baseProviders = getInjectedSolanaProviders(walletStandardWallets);
      const merged = { ...baseProviders };
      if (walletKitSelectedKey && walletKitBridgeProvider) {
        merged[walletKitSelectedKey] = walletKitBridgeProvider;
      }

      const order = ["jupiter", "solflare", "phantom", "backpack"];

      if (walletKitConnected && walletKitSelectedKey && merged[walletKitSelectedKey]) {
        return { key: walletKitSelectedKey, provider: merged[walletKitSelectedKey] };
      }

      const pref = String(preferred || "solflare").toLowerCase().trim();
      if (merged[pref]) return { key: pref, provider: merged[pref] };

      for (const key of order) {
        const p = merged[key];
        if (p && solanaProviderPubkeyBase58(p)) return { key, provider: p };
      }
      for (const key of order) {
        const p = merged[key];
        if (p) return { key, provider: p };
      }
      return { key: null, provider: null };
    };
  }, [walletStandardWallets, walletKitSelectedKey, walletKitBridgeProvider, walletKitConnected]);

  const installedSolanaWallets = useMemo(() => {
    if (!isSolanaDexVenue) return [];
    const base = getInstalledSolanaWalletOptions(walletStandardWallets);
    const labels = { jupiter: "Jupiter", solflare: "Solflare", phantom: "Phantom", backpack: "Backpack" };
    if (walletKitSelectedKey && !base.some((x) => x?.key === walletKitSelectedKey)) {
      return [{ key: walletKitSelectedKey, label: labels[walletKitSelectedKey] || walletKitSelectedKey }, ...base];
    }
    return base;
  }, [isSolanaDexVenue, walletStandardWallets, walletKitSelectedKey]);
  const solanaWalletState = useMemo(() => {
    if (!isSolanaDexVenue) return { key: null, label: null, connected: false, address: null };
    const { key, provider } = resolveInjectedSolanaProvider(preferredSolanaWallet);
    const labels = { jupiter: "Jupiter", solflare: "Solflare", phantom: "Phantom", backpack: "Backpack" };
    const address = solanaProviderPubkeyBase58(provider);
    return {
      key,
      label: labels[key] || "Wallet",
      connected: !!address,
      address: address || null,
    };
  }, [isSolanaDexVenue, preferredSolanaWallet, resolveInjectedSolanaProvider]);
  const solanaWalletLabel = solanaWalletState.label;
  const solanaWalletConnected = solanaWalletState.connected;


  const [inlineMode, setInlineMode] = useState(true);

  // Right-rail tile containment mode: keep this widget fully contained inside the App rail tile.
  const forceTileMode = true;

  const DEFAULT_W = 420;
  const DEFAULT_H = 330;

  const MIN_W = 320;
  const MIN_H = 250;
  const MAX_W = 900;

  const MAX_H = useMemo(() => {
    const vh = HAS_WINDOW && Number.isFinite(window.innerHeight) ? window.innerHeight : 700;
    return Math.max(250, Math.floor(vh * 0.85));
  }, []);

  const [locked, setLocked] = useState(() => lsGet(LS_OT_LOCK, "0") === "1");

  const [box, setBox] = useState(() => {
    const saved = safeJsonParse(lsGet(LS_OT_BOX, "null"), null);
    return saved && typeof saved === "object"
      ? { x: saved.x ?? 0, y: saved.y ?? 0, w: saved.w ?? DEFAULT_W, h: saved.h ?? DEFAULT_H }
      : { x: 0, y: 0, w: DEFAULT_W, h: DEFAULT_H };
  });

  // ─────────────────────────────────────────────────────────────
  // Total (Quote) ↔ Qty (Bidirectional auto-calc)
  // ─────────────────────────────────────────────────────────────
  const [totalQuote, setTotalQuote] = useState(() => lsGet(LS_OT_TOTAL_USD, "") || "");
  const [autoCalc, setAutoCalc] = useState(() => lsGet(LS_OT_AUTOQTY, "1") !== "0");

  const lastEditedRef = useRef("total"); // "total" | "qty"
  const autoCalcWriteGuardRef = useRef({ qty: null, total: null });

  useEffect(() => lsSet(LS_OT_TOTAL_USD, String(totalQuote ?? "")), [totalQuote]);
  useEffect(() => lsSet(LS_OT_AUTOQTY, autoCalc ? "1" : "0"), [autoCalc]);

  useEffect(() => lsSet(LS_OT_LOCK, locked ? "1" : "0"), [locked]);
  useEffect(() => lsSet(LS_OT_BOX, JSON.stringify(box)), [box]);


  const lockedRef = useRef(locked);
  const boxRef = useRef(box);
  useEffect(() => { lockedRef.current = locked; }, [locked]);
  useEffect(() => { boxRef.current = box; }, [box]);

  const dragStateRef = useRef(null);
  const resizeStateRef = useRef(null);

  // NOTE: Use the same coordinate-space model as OrderBookWidget.
  // We store and clamp x/y in *page* coords (visualViewport offsets applied), because
  // Brave vertical tabs / docked DevTools shift visualViewport and can otherwise cause
  // x to be permanently clamped to a boundary (making horizontal drag feel "stuck").
  function getViewport() {
    const vv = typeof window !== "undefined" ? window.visualViewport : null;
    const vw = Math.round(vv?.width ?? window.innerWidth);
    const vh = Math.round(vv?.height ?? window.innerHeight);
    const ox = Math.round(vv?.offsetLeft ?? 0);
    const oy = Math.round(vv?.offsetTop ?? 0);
    return { vw, vh, ox, oy };
  }

  function getGutterBounds() {
    const { vw, vh, ox, oy } = getViewport();

    // If we have an app container, treat its right edge as the gutter split.
    const el = appContainerRef?.current;
    const rect = el?.getBoundingClientRect?.();

    const margin = 0;

    if (!rect) {
      return {
        minX: ox + margin,
        maxX: ox + vw - margin,
        minY: oy + margin,
        maxY: oy + vh - margin,
        gutterLeft: ox + margin,
        gutterWidth: Math.max(0, vw - margin * 2),
        vw,
        vh,
        ox,
        oy,
      };
    }

    // rect.* are relative to the current visual viewport; convert to absolute page coords.
    const containerRight = ox + rect.right;
    const gutterLeft = Math.ceil(containerRight + margin);
    const gutterWidth = Math.max(0, Math.floor((ox + vw) - gutterLeft - margin));

    return {
      minX: gutterLeft,
      maxX: ox + vw - margin,
      minY: oy + margin,
      maxY: oy + vh - margin,
      gutterLeft,
      gutterWidth,
      vw,
      vh,
      ox,
      oy,
    };
  }


  function clamp(n, lo, hi) {
    if (!Number.isFinite(n)) return lo;
    return Math.max(lo, Math.min(hi, n));
  }

  function clampBox(next) {
    const b = getGutterBounds();
    const w = clamp(next.w, MIN_W, Math.min(MAX_W, b.maxX - b.minX));
    const h = clamp(next.h, MIN_H, Math.min(MAX_H, b.maxY - b.minY));
    const x = clamp(next.x, b.minX, b.maxX - w);
    const y = clamp(next.y, b.minY, b.maxY - h);
    return { x, y, w, h };
  }

  useEffect(() => {
    if (forceTileMode) {
      setInlineMode(true);
      return;
    }

    const recompute = () => {
      const b = getGutterBounds();
      const canGutter = Number.isFinite(b.gutterWidth) ? b.gutterWidth >= MIN_W + 4 : false;
      setInlineMode(!canGutter);

      if (canGutter) {
        setBox((prev) => {
          // b.vw/vh are visualViewport dimensions; b.ox/oy are offsets (page coords).
          const vwAbs = (Number.isFinite(b.ox) ? b.ox : 0) + (Number.isFinite(b.vw) ? b.vw : (HAS_WINDOW ? window.innerWidth : 1200));
          const vhAbs = (Number.isFinite(b.oy) ? b.oy : 0) + (Number.isFinite(b.vh) ? b.vh : (HAS_WINDOW ? window.innerHeight : 800));
          const w = clamp(prev.w || DEFAULT_W, MIN_W, Math.min(MAX_W, b.gutterWidth));
          const h = clamp(prev.h || DEFAULT_H, MIN_H, Math.min(MAX_H, b.maxY - b.minY));

          if (lockedRef.current) {
            const curX = Number.isFinite(prev.x) ? prev.x : b.minX;
            const curY = Number.isFinite(prev.y) ? prev.y : b.minY;

            const left = Number.isFinite(prev.left) ? prev.left : (curX - b.minX);
            const top = Number.isFinite(prev.top) ? prev.top : (curY - b.minY);
            const right = Number.isFinite(prev.right) ? prev.right : (vwAbs - (curX + w));
            const bottom = Number.isFinite(prev.bottom) ? prev.bottom : (vhAbs - (curY + h));

            const anchorX = prev.anchorX === "right" || prev.anchorX === "left"
              ? prev.anchorX
              : (left <= right ? "left" : "right");
            const anchorY = prev.anchorY === "bottom" || prev.anchorY === "top"
              ? prev.anchorY
              : (top <= bottom ? "top" : "bottom");

            const rawX = anchorX === "right" ? (vwAbs - w - right) : (b.minX + left);
            const rawY = anchorY === "bottom" ? (vhAbs - h - bottom) : (b.minY + top);

            const x = clamp(rawX, b.minX, b.maxX - w);
            const y = clamp(rawY, b.minY, b.maxY - h);
            const clamped = clampBox({ x, y, w, h });
            // Preserve lock metadata so we don't "re-decide" anchors on overlay/resize.
            return { ...prev, ...clamped, left, top, right, bottom, anchorX, anchorY };
          }

          const x = clamp(prev.x ?? b.minX, b.minX, b.maxX - w);
          const y = clamp(prev.y ?? b.maxY - h, b.minY, b.maxY - h);
          return clampBox({ x, y, w, h });
        });
      }
    };

    recompute();
    if (HAS_WINDOW) window.addEventListener("resize", recompute);
    return () => {
      if (HAS_WINDOW) window.removeEventListener("resize", recompute);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [forceTileMode]);

  useEffect(() => {
    if (inlineMode) return;
    setBox((prev) => clampBox(prev));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [inlineMode, effectiveVenue]);

  function onDragMouseDown(e) {
    if (inlineMode || locked) return;
    e.preventDefault();

    dragStateRef.current = { startX: e.clientX, startY: e.clientY, startBox: { ...box } };

    const onMove = (ev) => {
      const st = dragStateRef.current;
      if (!st) return;
      const dx = ev.clientX - st.startX;
      const dy = ev.clientY - st.startY;
      setBox(clampBox({ ...st.startBox, x: st.startBox.x + dx, y: st.startBox.y + dy }));
    };

    const onUp = () => {
      dragStateRef.current = null;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }

  function onResizeMouseDown(e) {
    if (inlineMode || locked) return;
    e.preventDefault();
    e.stopPropagation();

    const start = { ...box };
    // Resize from bottom-right corner (keep x/y fixed; change w/h).
    resizeStateRef.current = { startX: e.clientX, startY: e.clientY, startBox: start };

    const onMove = (ev) => {
      const st = resizeStateRef.current;
      if (!st) return;

      const dx = ev.clientX - st.startX;
      const dy = ev.clientY - st.startY;

      const rawW = st.startBox.w + dx;
      const rawH = st.startBox.h + dy;

      const b = getGutterBounds();
      const w = clamp(rawW, MIN_W, Math.min(MAX_W, b.maxX - b.minX));
      const h = clamp(rawH, MIN_H, Math.min(MAX_H, b.maxY - b.minY));

      const x = st.startBox.x;
      const y = st.startBox.y;

      setBox(clampBox({ x, y, w, h }));
    };

    const onUp = () => {
      resizeStateRef.current = null;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }

  // ─────────────────────────────────────────────────────────────
  // Symbol parsing (base/quote) + balances available
  // ─────────────────────────────────────────────────────────────
  function parseBaseQuote(sym) {
    const s = String(sym || "").trim().toUpperCase();
    if (!s) return { base: null, quote: null };

    let base = null;
    let quote = null;

    if (s.includes("-")) {
      const [a, b] = s.split("-");
      base = (a || "").trim() || null;
      quote = (b || "").trim() || null;
    } else if (s.includes("/")) {
      const [a, b] = s.split("/");
      base = (a || "").trim() || null;
      quote = (b || "").trim() || null;
    } else {
      base = s || null;
      quote = null;
    }

    return { base, quote };
  }

  const { base: baseAsset, quote: quoteAsset } = useMemo(() => parseBaseQuote(otSymbol), [otSymbol]);

  const quoteIsUsdLike = useMemo(() => {
    const q = String(quoteAsset || "").toUpperCase().trim();
    return q === "USD" || q === "USDT" || q === "USDC";
  }, [quoteAsset]);

  const totalQuoteDecimals = useMemo(() => (quoteIsUsdLike ? 2 : 8), [quoteIsUsdLike]);

  // Null-safe number parsing (prevents null → 0 bugs via Number(null))
  const toFiniteOrNull = (x) => {
    if (x === null || x === undefined) return null;
    const n = Number(x);
    return Number.isFinite(n) ? n : null;
  };

  // ─────────────────────────────────────────────────────────────
  // Order Rules
  // ─────────────────────────────────────────────────────────────
  const [rulesLoading, setRulesLoading] = useState(false);
  const [rulesErr, setRulesErr] = useState(null);
  const [rules, setRules] = useState(null);
  const rulesReqIdRef = useRef(0);

  useEffect(() => {
    const v = String(effectiveVenue || "").trim().toLowerCase();
    const s = String(otSymbol || "").trim();

    if (!v || !s) {
      setRules(null);
      setRulesErr(null);
      setRulesLoading(false);
      return;
    }

    const reqId = ++rulesReqIdRef.current;
    let cancelled = false;

    const t = setTimeout(async () => {
      try {
        setRulesLoading(true);
        setRulesErr(null);

        const data = await getOrderRules(
          { venue: v, symbol: s, side, type: "limit", tif, post_only: postOnly },
          { apiBase }
        );

        if (cancelled || rulesReqIdRef.current !== reqId) return;

        // Solana-Jupiter is swap-style; if backend returns "unknown constraints" (often with 0 decimals),
        // override with sane defaults so ticket math + validation behaves like CEX precision-wise.
        const vLower = String(v || "").trim().toLowerCase();
        const isSol = vLower === "solana_jupiter" || vLower === "solana-dex" || vLower.startsWith("solana_");
        const warns = Array.isArray(data?.warnings) ? data.warnings.map((x) => String(x || "")) : [];
        const warnText = warns.join(" ").toLowerCase();

        if (
          isSol &&
          (
            data == null ||
            Number(data?.price_decimals ?? 0) <= 0 ||
            Number(data?.qty_decimals ?? 0) <= 0 ||
            warnText.includes("does not implement get_order_rules") ||
            warnText.includes("constraints unknown")
          )
        ) {
          setRules({
            ...(data || {}),
            venue: vLower,
            symbol: s,
            type: "swap",
            price_decimals: 9,
            qty_decimals: 6,
            price_increment: 0.000000001,
            qty_increment: 0.000001,
            min_qty: Number(data?.min_qty ?? 0) || 0,
            min_notional: Number(data?.min_notional ?? 0) || 0,
            errors: [],
            warnings: [],
          });
          setRulesErr(null);
        } else {
          // Solana pairs quoted in SOL often require 9dp pricing; ensure we never clamp below that.
          if (isSol && data && typeof data === "object") {
            const symU = String(s || "").toUpperCase();
            const isSolQuoted = symU.endsWith("-SOL") || symU.startsWith("SOL-");
            if (isSolQuoted) {
              const pd = Math.max(Number(data?.price_decimals ?? 0) || 0, 9);
              const pi = 1 / Math.pow(10, pd);
              setRules({
                ...data,
                price_decimals: pd,
                price_increment: Number.isFinite(pi) ? pi : data?.price_increment,
              });
              return;
            }
          }
          setRules(data || null);
        }
      } catch (e) {
        if (cancelled || rulesReqIdRef.current !== reqId) return;

        const vLower = String(v || "").trim().toLowerCase();
        const errMsg = extractRulesError(e);

        // Solana-Jupiter is swap-style; if backend doesn't implement get_order_rules yet,
        // fall back to sane decimals so UI doesn't clamp to 0 and block.
        if (
          (vLower === "solana_jupiter" || vLower === "solana-dex" || vLower.startsWith("solana_")) &&
          typeof errMsg === "string" &&
          (
            errMsg.toLowerCase().includes("does not implement get_order_rules") ||
            errMsg.toLowerCase().includes("constraints unknown")
          )
        ) {
          setRules({
            venue: vLower,
            symbol: s,
            type: "swap",
            // Conservative defaults for USDC/SOL style quoting
            price_decimals: 9,
            qty_decimals: 6,
            price_increment: 0.000000001,
            qty_increment: 0.000001,
            min_qty: 0,
            min_notional: 0,
            errors: [],
          });
          setRulesErr(null);
        } else {
          setRules(null);
          setRulesErr(errMsg);
        }
      } finally {
        if (cancelled || rulesReqIdRef.current !== reqId) return;
        setRulesLoading(false);
      }
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [effectiveVenue, otSymbol, side, tif, postOnly, apiBase]);

  // ─────────────────────────────────────────────────────────────
  // Helpers: formatting + increment math
  // ─────────────────────────────────────────────────────────────
  function trimFixedStr(s) {
    const x = String(s ?? "");
    if (!x) return "";
    if (!x.includes(".")) return x;
    return x.replace(/0+$/g, "").replace(/\.$/g, "");
  }

  function fmtStepValue(v, decimalsHint) {
    if (v === null || v === undefined) return null;

    const raw = String(v).trim();
    if (!raw) return null;

    const n = Number(raw);
    if (!Number.isFinite(n)) return raw;

    const dh = Number(decimalsHint);
    if (Number.isFinite(dh) && dh >= 0) {
      const cap = Math.min(Math.max(Math.trunc(dh), 0), 18);
      return trimFixedStr(n.toFixed(cap));
    }

    return n.toLocaleString(undefined, { useGrouping: false, maximumFractionDigits: 18 });
  }

  function countDecimalsFromString(x) {
    const s = String(x ?? "").trim();
    if (!s) return 0;
    if (s.includes("e") || s.includes("E")) return null;
    const i = s.indexOf(".");
    if (i < 0) return 0;
    return Math.max(0, s.length - i - 1);
  }

  function decimalsFromIncrement(x) {
    const s = String(x ?? "").trim();
    if (!s) return null;

    if (s.includes("e") || s.includes("E")) {
      const n = Number(s);
      if (!Number.isFinite(n) || n <= 0) return null;
      const p = Math.round(-Math.log10(n));
      return Number.isFinite(p) && p >= 0 && p <= 18 ? p : null;
    }

    const i = s.indexOf(".");
    if (i < 0) return 0;
    return Math.max(0, s.length - i - 1);
  }

  function parseDecimalToScaledInt(str, scaleDec) {
    const s0 = String(str ?? "").trim();
    if (!s0) return null;
    if (s0.includes("e") || s0.includes("E")) return null;

    const neg = s0.startsWith("-");
    const s = neg ? s0.slice(1) : s0;

    const parts = s.split(".");
    const whole = parts[0] || "0";
    const frac = parts[1] || "";

    if (!/^\d+$/.test(whole) || (frac && !/^\d+$/.test(frac))) return null;

    const fracPadded = (frac + "0".repeat(scaleDec)).slice(0, scaleDec);
    const combined = (whole.replace(/^0+(?=\d)/, "") || "0") + fracPadded;

    const combinedNorm = combined.replace(/^0+(?=\d)/, "") || "0";
    try {
      const bi = BigInt(combinedNorm);
      return neg ? -bi : bi;
    } catch {
      return null;
    }
  }

  function isMultipleOfStep(valueStr, stepStr, decimalsHint) {
    const dec = Number.isFinite(Number(decimalsHint))
      ? Math.min(Math.max(Math.trunc(Number(decimalsHint)), 0), 18)
      : decimalsFromIncrement(stepStr);
    if (dec === null || dec === undefined) return null;

    const vInt = parseDecimalToScaledInt(valueStr, dec);
    const sInt = parseDecimalToScaledInt(stepStr, dec);
    if (vInt === null || sInt === null) return null;
    if (sInt === 0n) return null;

    return vInt % sInt === 0n;
  }

  function floorToStepNumber(rawNum, stepStr, decimalsHint) {
    const stepNum = Number(stepStr);
    if (!Number.isFinite(rawNum) || rawNum <= 0) return null;
    if (!Number.isFinite(stepNum) || stepNum <= 0) return rawNum;

    const dec = Number.isFinite(Number(decimalsHint))
      ? Math.min(Math.max(Math.trunc(Number(decimalsHint)), 0), 18)
      : decimalsFromIncrement(stepStr);

    if (!Number.isFinite(dec) || dec === null || dec === undefined) {
      const k = Math.floor(rawNum / stepNum);
      const q = k * stepNum;
      return Number.isFinite(q) && q > 0 ? q : null;
    }

    const scale = 10 ** dec;
    if (!Number.isFinite(scale) || scale <= 0) return null;

    const rawScaled = Math.floor(rawNum * scale + 1e-9);
    const stepScaled = Math.round(stepNum * scale);
    if (!Number.isFinite(rawScaled) || !Number.isFinite(stepScaled) || stepScaled <= 0) return null;

    const flooredScaled = Math.floor(rawScaled / stepScaled) * stepScaled;
    const q = flooredScaled / scale;
    return Number.isFinite(q) && q > 0 ? q : null;
  }

  // NEW: ceil rounding to step (used for SELL limit safety)
  function ceilToStepNumber(rawNum, stepStr, decimalsHint) {
    const stepNum = Number(stepStr);
    if (!Number.isFinite(rawNum) || rawNum <= 0) return null;
    if (!Number.isFinite(stepNum) || stepNum <= 0) return rawNum;

    const dec = Number.isFinite(Number(decimalsHint))
      ? Math.min(Math.max(Math.trunc(Number(decimalsHint)), 0), 18)
      : decimalsFromIncrement(stepStr);

    if (!Number.isFinite(dec) || dec === null || dec === undefined) {
      const k = Math.ceil(rawNum / stepNum);
      const q = k * stepNum;
      return Number.isFinite(q) && q > 0 ? q : null;
    }

    const scale = 10 ** dec;
    if (!Number.isFinite(scale) || scale <= 0) return null;

    // Ceil in scaled space; a tiny epsilon avoids accidental bump from float noise
    const rawScaled = Math.ceil(rawNum * scale - 1e-9);
    const stepScaled = Math.round(stepNum * scale);
    if (!Number.isFinite(rawScaled) || !Number.isFinite(stepScaled) || stepScaled <= 0) return null;

    const ceiledScaled = Math.ceil(rawScaled / stepScaled) * stepScaled;
    const q = ceiledScaled / scale;
    return Number.isFinite(q) && q > 0 ? q : null;
  }

  // ─────────────────────────────────────────────────────────────
  // NEW: sanitize numeric input as a string (prevents “e-” from persisting)
  // ─────────────────────────────────────────────────────────────
  function sanitizeDecimalInput(raw, { allowLeadingDot = true } = {}) {
    let s = String(raw ?? "");
    if (!s) return "";

    // Expand scientific notation if user pasted/auto-set it.
    s = expandExponential(s);

    // Normalize separators/spaces
    s = s.replace(/,/g, "").trim();

    // Keep digits + dot only
    s = s.replace(/[^\d.]/g, "");

    // Only one dot
    const firstDot = s.indexOf(".");
    if (firstDot >= 0) {
      const left = s.slice(0, firstDot + 1);
      const right = s.slice(firstDot + 1).replace(/\./g, "");
      s = left + right;
    }

    if (allowLeadingDot && s.startsWith(".")) s = `0${s}`;

    return s;
  }

  // ─────────────────────────────────────────────────────────────
  // NEW: normalize limit price string to venue tick/decimals (UI-side)
  //      Directional rounding:
  //        - BUY: floor (do not exceed user's max)
  //        - SELL: ceil  (do not go below user's min)
  // ─────────────────────────────────────────────────────────────
  function normalizeLimitPriceStr(rawStr, rulesObj, sideForRounding) {
    const cleaned = sanitizeDecimalInput(expandExponential(rawStr));
    if (!cleaned) return "";

    // If rules are unavailable or contain errors, do not mutate user entry.
    if (!rulesObj) return cleaned;
    const errs = Array.isArray(rulesObj?.errors) ? rulesObj.errors : [];
    if (errs.length > 0) return cleaned;

    const pi = rulesObj?.price_increment;
    const pxDec = rulesObj?.price_decimals;

    const n = Number(cleaned);
    if (!Number.isFinite(n) || n <= 0) return cleaned;

    const roundingSide = String(sideForRounding || "").toLowerCase().trim();
    const wantCeil = roundingSide === "sell";

    // Prefer tick quantization when available.
    if (pi !== null && pi !== undefined && String(pi).trim() && Number(pi) > 0) {
      const piStr = String(pi).trim();
      const qNum = wantCeil ? ceilToStepNumber(n, piStr, pxDec) : floorToStepNumber(n, piStr, pxDec);
      if (qNum === null) return cleaned;

      const dec =
        Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0
          ? Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18)
          : decimalsFromIncrement(piStr);

      if (dec === null || dec === undefined || !Number.isFinite(dec)) {
        return String(qNum);
      }

      // For prices, keep fixed decimals (cents) instead of trimming.
      return Number(qNum).toFixed(dec);
    }

    // Fallback: clamp by decimals if we have them.
    if (Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0) {
      const dec = Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18);
      return Number(n).toFixed(dec);
    }

    return cleaned;
  }

  // If upstream provides sci notation (number or "5.6e-8" string), normalize it once.
  // Do not fight the user while they are typing.
  useEffect(() => {
    if (limitEditingRef.current) return;

    const s = String(limitPrice ?? "").trim();
    if (!s) return;
    if (/[eE]/.test(s)) {
      const expanded = expandExponential(s);
      const cleaned = sanitizeDecimalInput(expanded);
      if (cleaned && cleaned !== s) {
        limitSourceRef.current = "sci";
        setLimitPrice(cleaned);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [limitPrice]);

  // NEW: auto-normalize limit price when rules/side/limitPrice change,
  // but only when the user is not actively editing the Limit field.
  useEffect(() => {
    if (limitEditingRef.current) return;

    // DEX (Solana) venues: limit price is informational for swap-style flows.
    // Do NOT clamp/round it using CEX-style venue rules (which may be unknown and default to 0 decimals).
    if (isSolanaDexVenue) return;

    const lp = String(limitPrice ?? "");
    if (!lp) return;

    const normalized = normalizeLimitPriceStr(lp, rules, side);
    if (!normalized) return;

    // Avoid loops and avoid pointless writes.
    if (String(normalized) !== String(lp)) {
      limitSourceRef.current = "auto";
      setLimitPrice(normalized);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rules, side, limitPrice]);

  // ─────────────────────────────────────────────────────────────
  // Effective min qty (gated to Crypto.com only)
  // ─────────────────────────────────────────────────────────────
  const isCryptoCom = useMemo(
    () => String(effectiveVenue || "").toLowerCase().trim() === "cryptocom",
    [effectiveVenue]
  );

  const limitPxNumForMin = useMemo(() => {
    const s = String(limitPrice ?? "").trim();
    if (!s) return null;
    const n = Number(expandExponential(s));
    if (!Number.isFinite(n) || n <= 0) return null;
    return n;
  }, [limitPrice]);

  const uiMinQty = useMemo(() => {
    if (!rules) return null;
    if (!isCryptoCom) return null; // critical: do not affect other venues

    const minNotional = toFiniteOrNull(rules?.min_notional);
    const qtyStep = toFiniteOrNull(rules?.base_increment);

    // Default display when we cannot compute from entered price
    const fallback = toFiniteOrNull(rules?.min_qty) ?? qtyStep;

    const px = limitPxNumForMin;

    // If we have min_notional and a user-entered price, compute required qty from that price.
    if (minNotional !== null && minNotional > 0 && px !== null && px > 0) {
      const rawReq = minNotional / px;

      if (qtyStep !== null && qtyStep > 0) {
        const rounded = ceilToStepNumber(rawReq, String(qtyStep), rules?.qty_decimals);
        if (rounded !== null && rounded > 0) return Math.max(qtyStep, rounded);
        return Math.max(qtyStep, rawReq);
      }

      return rawReq;
    }

    return fallback;
  }, [rules, limitPxNumForMin, isCryptoCom]);

  const rulesBanner = useMemo(() => {
    if (rulesLoading) return { kind: "info", lines: ["Rules: loading…"] };

    if (rulesErr) {
      if (hideTableData) return { kind: "warn", lines: ["Rules: unavailable."] };
      return { kind: "warn", lines: [`Rules: ${rulesErr}`] };
    }

    if (!rules) return null;

    const errs = Array.isArray(rules?.errors) ? rules.errors : [];
    const warns = Array.isArray(rules?.warnings) ? rules.warnings : [];
    const suggested = rules?.suggested_symbol ? String(rules.suggested_symbol) : null;

    const lines = [];
    for (const e of errs) lines.push(hideTableData ? "Order rule error." : String(e));
    for (const w of warns) lines.push(hideTableData ? "Order rule warning." : String(w));

    if (lines.length === 0 && !hideTableData) {
      const bi = rules?.base_increment ?? null;
      const pi = rules?.price_increment ?? null;
      const mq = uiMinQty ?? (rules?.min_qty ?? null);
      const mn = rules?.min_notional ?? null;

      const biStr = fmtStepValue(bi, rules?.qty_decimals);
      const piStr = fmtStepValue(pi, rules?.price_decimals);

      const parts = [];
      if (biStr) parts.push(`qty step: ${biStr}`);
      if (piStr) parts.push(`price step: ${piStr}`);
      if (mq !== null && mq !== undefined) parts.push(`min qty: ${mq}`);
      if (mn !== null && mn !== undefined) parts.push(`min notional: ${mn}`);

      lines.push(parts.length ? `Rules: ok (${parts.join(" • ")})` : "Rules: ok");
    }

    if (suggested && !hideTableData) lines.push(`Suggested: ${suggested}`);

    if (lines.length === 0) return null;
    return { kind: errs.length > 0 ? "error" : "warn", lines };
  }, [rulesLoading, rulesErr, rules, hideTableData, uiMinQty]);

  const rulesBannerStyle = useMemo(() => {
    if (!rulesBanner) return null;
    if (rulesBanner.kind === "error") return { border: "1px solid #4a1f1f", background: "#160b0b", color: "#ffd2d2" };
    if (rulesBanner.kind === "info") return { border: "1px solid #2a2a2a", background: "#101010", color: "#cfcfcf" };
    return { border: "1px solid #3b3413", background: "#151208", color: "#f2e6b7" };
  }, [rulesBanner]);

  // ─────────────────────────────────────────────────────────────
  // Balances: available
  // ─────────────────────────────────────────────────────────────
  const [balAvail, setBalAvail] = useState({});
  const [balLoading, setBalLoading] = useState(false);
  const [balErr, setBalErr] = useState(null);

  useEffect(() => {
    if (!isSolanaDexVenue) setBalErr(null);
  }, [isSolanaDexVenue]);

  useEffect(() => {
    if (!isSolanaJupiterVenue) setSolanaOrderMode("swap");
  }, [isSolanaJupiterVenue]);

  function normalizeBalItems(items, venueKey) {
    const out = {};
    for (const b of items || []) {
      const a = String(b?.asset || "").toUpperCase().trim();
      if (!a) continue;

      if (venueKey) {
        const vv = String(b?.venue || "").toLowerCase().trim();
        if (vv && vv !== venueKey) continue;
      }

      const available = Number(b?.available);
      const total = Number(b?.total);
      const hold = Number(b?.hold);

      out[a] = {
        available: Number.isFinite(available) ? available : null,
        total: Number.isFinite(total) ? total : null,
        hold: Number.isFinite(hold) ? hold : null,
      };
    }
    return out;
  }

  function computeBalHash(obj) {
    try {
      const entries = Object.entries(obj || {}).sort(([a], [b]) => String(a).localeCompare(String(b)));
      return JSON.stringify(entries);
    } catch {
      return "";
    }
  }

  // NEW: focus hashing for base/quote only (so we can wait for the pair we traded)
  function computeFocusHash(availObj, focusAssets) {
    try {
      const fa = Array.isArray(focusAssets) ? focusAssets.filter(Boolean) : [];
      const norm = fa.map((x) => String(x).toUpperCase().trim()).filter(Boolean);
      const parts = [];
      for (const a of norm) {
        const v = availObj?.[a] ?? null;
        parts.push([a, v?.available ?? null, v?.total ?? null, v?.hold ?? null]);
      }
      return JSON.stringify(parts);
    } catch {
      return "";
    }
  }


  // ─────────────────────────────────────────────────────────────
  // Solana DEX balances support (DEX-only, opt-in by venue)
  // Uses injected Solana wallet public key (Jupiter / Solflare / Phantom / Backpack) + backend /api/solana_dex endpoints.
  // ─────────────────────────────────────────────────────────────
  const solanaResolveCacheRef = useRef({}); // assetKey -> { mint, decimals }

  function getInjectedSolanaPubkeyBase58() {
    try {
      const { provider } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      return solanaProviderPubkeyBase58(provider);
    } catch {
      return null;
    }
  }

  function getInjectedSolanaWalletLabel() {
    try {
      const { key } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (key === "jupiter") return "Jupiter";
      if (key === "solflare") return "Solflare";
      if (key === "phantom") return "Phantom";
      if (key === "backpack") return "Backpack";
      return "Wallet";
    } catch {
      return "Wallet";
    }
  }

  function getInjectedSolanaWalletConnected() {
    try {
      return !!getInjectedSolanaPubkeyBase58();
    } catch {
      return false;
    }
  }

  function isBlockedJupiterTokenError(msg) {
    const s = String(msg || "").toLowerCase();
    if (!s) return false;
    return (
      s.includes("not tradable on jupiter") ||
      s.includes("not supported on jupiter") ||
      s.includes("token not tradable") ||
      s.includes("token_not_tradable") ||
      s.includes("not supported") ||
      s.includes("could not find any route") ||
      s.includes("no route") ||
      s.includes("route not found") ||
      s.includes("jupiter_swap_failed")
    );
  }

  async function fetchSolanaSwapTx({ provider, symbol, side, amount, address, slippageBps, tok }) {
    const base = String(apiBase || "").replace(/\/+$/, "");
    const route =
      provider === "raydium"
        ? `${base}/api/solana_dex/raydium/swap_tx`
        : provider === "jupiter_ultra"
          ? `${base}/api/solana_dex/jupiter/ultra_order`
          : `${base}/api/solana_dex/jupiter/swap_tx`;

    const headers = { "Content-Type": "application/json" };
    if (tok) headers.Authorization = `Bearer ${tok}`;

    const payload = {
      symbol,
      side,
      amount,
      slippage_bps: slippageBps,
      user_pubkey: address,
    };

    const r = await fetch(route, { method: "POST", headers, body: JSON.stringify(payload) });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `HTTP ${r.status}`);
    }

    const j = await r.json();
    const txB64 =
      j?.swapTransaction ||
      j?.transaction ||
      (Array.isArray(j?.transactions) && j.transactions.length ? j.transactions[0] : null);

    if (!txB64) {
      throw new Error(`Missing swap transaction in ${provider} response`);
    }

    return { provider: provider || "jupiter_metis", data: j, txB64: String(txB64) };
  }

  async function executeSolanaUltraSwap({ signedTransaction, requestId, tok }) {
    const base = String(apiBase || "").replace(/\/+$/, "");
    const route = `${base}/api/solana_dex/jupiter/ultra_execute`;
    const headers = { "Content-Type": "application/json" };
    if (tok) headers.Authorization = `Bearer ${tok}`;

    const r = await fetch(route, {
      method: "POST",
      headers,
      body: JSON.stringify({ signedTransaction, requestId }),
    });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `HTTP ${r.status}`);
    }
    return await r.json();
  }


  async function ensureSolanaWalletConnected() {
    try {
      const { provider } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (!provider) return null;

      const existing = solanaProviderPubkeyBase58(provider);
      if (existing) return existing;

      if (typeof provider.connect === "function") {
        await provider.connect();
      }

      return solanaProviderPubkeyBase58(provider);
    } catch {
      return null;
    }
  }

  async function solanaResolveAsset(asset) {
    const a = String(asset || "").trim();
    if (!a) return null;

    const key = a.toUpperCase();
    const cached = solanaResolveCacheRef.current?.[key];
    if (cached?.mint && cached?.decimals !== null && cached?.decimals !== undefined) return cached;

    if (!apiBase) return null;

    const url = new URL(`${apiBase}/api/solana_dex/resolve`);
    url.searchParams.set("asset", a);
    url.searchParams.set("_ts", String(Date.now()));

    const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(txt || `solana resolve HTTP ${r.status}`);
    }
    const j = await r.json();
    const mint = j?.mint ? String(j.mint) : null;
    const decimals = Number.isFinite(Number(j?.decimals)) ? Math.trunc(Number(j.decimals)) : null;

    const out = mint ? { mint, decimals } : null;
    if (out) {
      solanaResolveCacheRef.current = { ...(solanaResolveCacheRef.current || {}), [key]: out };
    }
    return out;
  }

  async function loadAvailBalances(opts = {}) {
    const { silent = false, venueOverride = null, focusAssets = null } = opts;

    const v = String(venueOverride || effectiveVenue || "").toLowerCase().trim();
    if (!v) return { avail: balAvail, hash: computeBalHash(balAvail), focusHash: "" };

    if (!silent) {
      setBalLoading(true);
      setBalErr(null);
    }

    try {
      if (!apiBase) throw new Error("apiBase not set");
      // DEX-only: Solana venues do not have adapter-backed /api/balances/latest.
      if (isSolanaDexVenue) {
        let address = getInjectedSolanaPubkeyBase58();
        if (!address) {
          const addr2 = await ensureSolanaWalletConnected();
          if (!addr2) throw new Error("Connect a supported Solana wallet (Jupiter / Solflare / Phantom / Backpack) to load balances.");
          address = addr2;
        }

        const url = new URL(`${apiBase}/api/solana_dex/balances`);
        url.searchParams.set("address", address);
        url.searchParams.set("_ts", String(Date.now()));

        const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
        if (!r.ok) {
          const txt = await r.text();
          throw new Error(txt || `HTTP ${r.status}`);
        }

        const j = await r.json();
        const nextAvail = {};

        const sol = Number(j?.sol);
        if (Number.isFinite(sol)) {
          nextAvail["SOL"] = { available: sol, total: sol, hold: null };
          nextAvail["WSOL"] = { available: sol, total: sol, hold: null };
        }

        const toks = Array.isArray(j?.tokens) ? j.tokens : [];
        const mintToUi = {};
        const symbolToUi = {};
        const uiAmtFromToken = (t) => {
          const uiRaw = t?.uiAmount ?? t?.ui_amount ?? t?.uiAmountString ?? t?.ui_amount_string ?? t?.uiAmountStr;
          let ui = typeof uiRaw === "number" ? uiRaw : parseFloat(String(uiRaw ?? ""));
          if (Number.isFinite(ui)) return ui;

          const amtRaw = t?.amount ?? t?.rawAmount ?? t?.raw_amount ?? t?.tokenAmount ?? t?.token_amount;
          const decRaw = t?.decimals ?? t?.decimal ?? t?.dec ?? t?.precision;
          const amt = typeof amtRaw === "number" ? amtRaw : parseFloat(String(amtRaw ?? ""));
          const dec = typeof decRaw === "number" ? decRaw : parseInt(String(decRaw ?? ""), 10);
          if (Number.isFinite(amt) && Number.isFinite(dec) && dec >= 0) return amt / Math.pow(10, dec);

          return null;
        };

        for (const t of toks) {
          const mint = String(t?.mint || t?.address || t?.tokenMint || t?.token_mint || "").trim();
          if (!mint) continue;

          const uiAmt = uiAmtFromToken(t);
          mintToUi[mint] = uiAmt;

          const sym = String(t?.symbol || t?.asset || "").trim();
          if (sym) symbolToUi[sym.toUpperCase()] = uiAmt;
        }

        // Resolve only what we need for this ticket (base/quote + common aliases).
        const want = new Set(
          [baseAsset, quoteAsset, "USD", "USDC", "USDT", "PYUSD"]
            .map((x) => String(x || "").trim().toUpperCase())
            .filter(Boolean)
        );

        for (const a of want) {
          if (a === "SOL" || a === "WSOL") continue;
          const symUi = symbolToUi[String(a).toUpperCase()];
          if (Number.isFinite(symUi)) {
            nextAvail[a] = { available: symUi, total: symUi, hold: null };
            continue;
          }
          try {
            const res = await solanaResolveAsset(a);
            const mint = res?.mint;
            if (!mint) continue;
            const ui = mintToUi[mint];
            if (!Number.isFinite(ui)) continue;
            nextAvail[a] = { available: ui, total: ui, hold: null };

            // Convenience: treat USD as USDC for Solana venues (keep both keys filled if present).
            if (a === "USDC" && !nextAvail["USD"]) nextAvail["USD"] = { available: ui, total: ui, hold: null };
            if (a === "USD" && !nextAvail["USDC"]) nextAvail["USDC"] = { available: ui, total: ui, hold: null };
          } catch {
            // ignore resolve failures; balances will simply be missing for that asset
          }
        }

        const nextHash = computeBalHash(nextAvail);
        const nextFocusHash = focusAssets ? computeFocusHash(nextAvail, focusAssets) : "";

        setBalAvail(nextAvail);
        return { avail: nextAvail, hash: nextHash, focusHash: nextFocusHash };
      }

      const url = new URL(`${apiBase}/api/balances/latest`);
      url.searchParams.set("venue", v);
      url.searchParams.set("sort", "asset:asc");
      url.searchParams.set("_ts", String(Date.now()));

      const r = await fetch(url.toString(), { method: "GET", cache: "no-store" });
      if (!r.ok) {
        const txt = await r.text();
        throw new Error(txt || `HTTP ${r.status}`);
      }

      const j = await r.json();
      const items = Array.isArray(j?.items) ? j.items : [];
      const nextAvail = normalizeBalItems(items, v);

      const nextHash = computeBalHash(nextAvail);
      const nextFocusHash = focusAssets ? computeFocusHash(nextAvail, focusAssets) : "";

      setBalAvail(nextAvail);
      return { avail: nextAvail, hash: nextHash, focusHash: nextFocusHash };
    } catch (e) {
      setBalAvail({});
      setBalErr(e?.message || "Failed loading balances");
      return { avail: {}, hash: "", focusHash: "" };
    } finally {
      if (!silent) setBalLoading(false);
    }
  }

  // UPDATED: refresh can be tuned (force, polling window, focusAssets)
  async function refreshAvailBalances(opts = {}) {
    const {
      venueOverride = null,
      force = false,
      focusAssets = null,

      // new defaults: "a few tries is enough"
      maxPolls = 5, // hard cap on GETs after refresh
      initialDelayMs = 900, // let venue settle before first GET
      pollBackoffMs = [600, 900, 1300, 1800, 2200], // per-attempt delays
    } = opts;

    const v = String(venueOverride || effectiveVenue || "").toLowerCase().trim();
    if (!v) return false;

    // DEX-only: Solana venues don't have a refresh adapter path; just re-load wallet balances.
    if (isSolanaDexVenue) {
      const beforeFullHash = computeBalHash(balAvail);
      const beforeFocusHash = focusAssets ? computeFocusHash(balAvail, focusAssets) : "";

      setBalLoading(true);
      setBalErr(null);
      try {
        const { hash: afterFullHash, focusHash: afterFocusHash } = await loadAvailBalances({
          silent: true,
          venueOverride: v,
          focusAssets,
        });

        if (focusAssets) return !!afterFocusHash && afterFocusHash !== beforeFocusHash;
        return !!afterFullHash && afterFullHash !== beforeFullHash;
      } catch (e) {
        setBalErr(e?.message || "Failed loading Solana balances");
        return false;
      } finally {
        setBalLoading(false);
      }
    }

    setBalLoading(true);
    setBalErr(null);

    // compute BEFORE snapshots from current state once
    const beforeFullHash = computeBalHash(balAvail);
    const beforeFocusHash = focusAssets ? computeFocusHash(balAvail, focusAssets) : "";

    let changed = false;

    try {
      if (!apiBase) throw new Error("apiBase not set");

      const postUrl = `${apiBase}/api/balances/refresh`;

      let rr = await fetch(postUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ venue: v, force: !!force }),
      });

      // compatibility fallback for older schema
      if (rr.status === 422) {
        rr = await fetch(postUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ input: { venue: v, force: !!force } }),
        });
      }

      // fallback if refresh endpoint was GET-only in older versions
      if (rr.status === 405 || rr.status === 404) {
        const getUrl = new URL(`${apiBase}/api/balances/refresh`);
        getUrl.searchParams.set("venue", v);
        getUrl.searchParams.set("force", force ? "true" : "false");
        getUrl.searchParams.set("_ts", String(Date.now()));
        rr = await fetch(getUrl.toString(), { method: "GET", cache: "no-store" });
      }

      if (!rr.ok) {
        const txt = await rr.text();
        throw new Error(txt || `balances refresh HTTP ${rr.status}`);
      }

      // settle delay
      await new Promise((r) => setTimeout(r, Math.max(0, Number(initialDelayMs) || 0)));

      // poll a few times at most
      const polls = Math.max(1, Math.min(10, Math.floor(Number(maxPolls) || 5)));
      for (let i = 0; i < polls; i++) {
        const { hash: afterFullHash, focusHash: afterFocusHash } = await loadAvailBalances({
          silent: true,
          venueOverride: v,
          focusAssets,
        });

        if (focusAssets) {
          if (afterFocusHash && afterFocusHash !== beforeFocusHash) {
            changed = true;
            break;
          }
        } else {
          if (afterFullHash && afterFullHash !== beforeFullHash) {
            changed = true;
            break;
          }
        }

        const delay =
          Array.isArray(pollBackoffMs) && pollBackoffMs[i] != null
            ? Math.max(150, Math.floor(Number(pollBackoffMs[i]) || 0))
            : 900;

        await new Promise((r) => setTimeout(r, delay));
      }
    } catch (e) {
      setBalErr(e?.message || "Failed refreshing balances");
    } finally {
      setBalLoading(false);
    }

    return changed;
  }

  useEffect(() => {
    if (isSolanaDexVenue) {
      setBalAvail({});
      setBalErr(null);
    }
    loadAvailBalances();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [effectiveVenue, apiBase, baseAsset, quoteAsset, isSolanaDexVenue, walletKitConnected, walletKitSelectedKey, solanaWalletState?.address]);

  const baseBal = useMemo(() => (baseAsset ? (balAvail?.[baseAsset] ?? null) : null), [balAvail, baseAsset]);
  const quoteBal = useMemo(() => (quoteAsset ? (balAvail?.[quoteAsset] ?? null) : null), [balAvail, quoteAsset]);

  const baseAvail = useMemo(() => toFiniteOrNull(baseBal?.available), [baseBal]);
  const quoteAvail = useMemo(() => toFiniteOrNull(quoteBal?.available), [quoteBal]);

  const relevantAvailLabel = useMemo(() => {
    if (side === "sell") return baseAsset ? `${baseAsset} available` : "Base available";
    return quoteAsset ? `${quoteAsset} available` : "Quote available";
  }, [side, baseAsset, quoteAsset]);

  const relevantAvailValue = useMemo(() => (side === "sell" ? baseAvail : quoteAvail), [side, baseAvail, quoteAvail]);

  // ─────────────────────────────────────────────────────────────
  // Numbers + derived calcs
  // ─────────────────────────────────────────────────────────────
  const qtyNum = useMemo(() => {
    const x = Number(qty);
    return Number.isFinite(x) && x > 0 ? x : null;
  }, [qty]);

  const pxNum = useMemo(() => {
    // IMPORTANT: limitPrice is treated as a string in UI, but we convert here for math.
    const x = Number(expandExponential(limitPrice));
    return Number.isFinite(x) && x > 0 ? x : null;
  }, [limitPrice]);

  const totalQuoteNum = useMemo(() => {
    const x = Number(totalQuote);
    return Number.isFinite(x) && x > 0 ? x : null;
  }, [totalQuote]);

  function fmtPlain(n, { maxFrac }) {
    if (n === null || n === undefined) return "";
    const x = Number(n);
    if (!Number.isFinite(x)) return "";
    return x.toLocaleString(undefined, { useGrouping: false, maximumFractionDigits: maxFrac });
  }

  const notional = useMemo(() => (qtyNum === null || pxNum === null ? null : qtyNum * pxNum), [qtyNum, pxNum]);

  const qtyFromTotal = useMemo(() => {
    if (pxNum === null || totalQuoteNum === null) return null;

    const raw = totalQuoteNum / pxNum;
    if (!Number.isFinite(raw) || raw <= 0) return null;

    const bi = rules?.base_increment ?? null;
    const biStr = bi === null || bi === undefined ? null : String(bi).trim();
    const decHint = rules?.qty_decimals;

    if (biStr && Number(biStr) > 0) return floorToStepNumber(raw, biStr, decHint);

    const factor = 10 ** 8;
    const floored = Math.floor(raw * factor) / factor;
    return Number.isFinite(floored) && floored > 0 ? floored : null;
  }, [pxNum, totalQuoteNum, rules]);

  const totalFromQty = useMemo(() => {
    if (qtyNum === null || pxNum === null) return null;
    const raw = qtyNum * pxNum;
    return Number.isFinite(raw) && raw > 0 ? raw : null;
  }, [qtyNum, pxNum]);

  const jupiterFrontendInputUsdValue = useMemo(() => {
    if (!isSolanaLimitMode) return null;
    const q = String(quoteAsset || "").toUpperCase().trim();
    const stableQuote = q === "USD" || q === "USDC" || q === "USDT" || q === "PYUSD";
    if (!stableQuote) return null;

    if (side === "buy") {
      return totalQuoteNum !== null && totalQuoteNum > 0 ? totalQuoteNum : null;
    }
    return notional !== null && notional > 0 ? notional : null;
  }, [isSolanaLimitMode, quoteAsset, side, totalQuoteNum, notional]);

  const jupiterMinFrontendEnforceable = useMemo(() => {
    return isSolanaLimitMode && jupiterFrontendInputUsdValue !== null;
  }, [isSolanaLimitMode, jupiterFrontendInputUsdValue]);
  const solanaExpiredAt = useMemo(() => {
    if (!isSolanaLimitMode) return undefined;

    const nowSec = Math.floor(Date.now() / 1000);
    const preset = String(solanaExpiryPreset || "never").toLowerCase().trim();

    if (preset === "never") return undefined;
    if (preset === "10m") return nowSec + 10 * 60;
    if (preset === "1h") return nowSec + 60 * 60;
    if (preset === "1d") return nowSec + 24 * 60 * 60;
    if (preset === "7d") return nowSec + 7 * 24 * 60 * 60;
    if (preset === "custom") {
      const raw = String(solanaExpiryCustom || "").trim();
      if (!raw) return null;
      const ms = Date.parse(raw);
      if (!Number.isFinite(ms)) return null;
      const sec = Math.floor(ms / 1000);
      if (sec <= nowSec) return null;
      return sec;
    }
    return undefined;
  }, [isSolanaLimitMode, solanaExpiryPreset, solanaExpiryCustom]);

  const solanaExpiryLabel = useMemo(() => {
    const preset = String(solanaExpiryPreset || "never").toLowerCase().trim();
    if (!isSolanaLimitMode) return "—";
    if (preset === "never") return "Never";
    if (preset === "10m") return "10m";
    if (preset === "1h") return "1h";
    if (preset === "1d") return "1d";
    if (preset === "7d") return "7d";
    if (preset === "custom") return solanaExpiryCustom ? String(solanaExpiryCustom) : "Custom";
    return "Never";
  }, [isSolanaLimitMode, solanaExpiryPreset, solanaExpiryCustom]);

  useEffect(() => {
    if (!autoCalc) return;

    if (lastEditedRef.current !== "qty" && lastEditedRef.current !== "total") lastEditedRef.current = "total";
    const mode = lastEditedRef.current;

    if (mode === "total") {
      if (qtyFromTotal === null) return;
      const maxFrac = Number.isFinite(Number(rules?.qty_decimals))
        ? Math.min(Math.max(Math.trunc(Number(rules.qty_decimals)), 0), 18)
        : 18;
      const nextQty = fmtPlain(qtyFromTotal, { maxFrac });
      if (!nextQty) return;

      if (String(nextQty) !== String(qty ?? "")) {
        if (autoCalcWriteGuardRef.current.qty !== nextQty) {
          autoCalcWriteGuardRef.current.qty = nextQty;
          setQty(nextQty);
        }
      }
      return;
    }

    if (mode === "qty") {
      if (totalFromQty === null) return;

      const nextTotal = fmtPlain(totalFromQty, { maxFrac: totalQuoteDecimals });
      if (!nextTotal) return;

      if (String(nextTotal) !== String(totalQuote ?? "")) {
        if (autoCalcWriteGuardRef.current.total !== nextTotal) {
          autoCalcWriteGuardRef.current.total = nextTotal;
          setTotalQuote(nextTotal);
        }
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoCalc, qtyFromTotal, totalFromQty, totalQuoteDecimals, pxNum, rules]);

  useEffect(() => {
    autoCalcWriteGuardRef.current.qty = null;
  }, [qty]);

  useEffect(() => {
    autoCalcWriteGuardRef.current.total = null;
  }, [totalQuote]);

  // ─────────────────────────────────────────────────────────────
  // Pre-trade checks
  // ─────────────────────────────────────────────────────────────
  const preTrade = useMemo(() => {
    const lines = [];
    const fails = [];

    if (rulesLoading) return { status: "neutral", title: "Pre-trade checks: loading…", lines: [], block: false };

    if (isSolanaLimitMode) {
      if (qtyNum === null) {
        lines.push("Qty missing/invalid.");
        fails.push("qty_missing");
      }
      if (pxNum === null) {
        lines.push("Limit price missing/invalid.");
        fails.push("px_missing");
      }

      if (jupiterMinFrontendEnforceable) {
        if (jupiterFrontendInputUsdValue + 1e-12 < JUPITER_LIMIT_MIN_USD) {
          lines.push(
            hideTableData
              ? "Jupiter limit minimum not met."
              : `Jupiter limit minimum: need current input-token value ≥ $${JUPITER_LIMIT_MIN_USD.toFixed(2)}.`
          );
          fails.push("jupiter_min_usd");
        }
      } else {
        lines.push(
          hideTableData
            ? "Jupiter limit minimum will be checked on submit."
            : `Jupiter limit minimum (${JUPITER_LIMIT_MIN_USD.toFixed(2)} USD current input value) will be enforced by backend on submit.`
        );
      }

      if (String(solanaExpiryPreset || "never").toLowerCase().trim() === "custom") {
        if (solanaExpiredAt === null) {
          lines.push(
            hideTableData
              ? "Custom expiry invalid."
              : "Custom expiry must be a valid future date/time."
          );
          fails.push("solana_expiry_invalid");
        }
      }

      if (fails.length === 0) return { status: "ok", title: "Pre-trade checks: OK", lines, block: false };
      return { status: "fail", title: "Pre-trade checks: FAIL (blocked)", lines, block: true };
    }

    if (rulesErr || !rules) {
      return {
        status: "neutral",
        title: hideTableData ? "Pre-trade checks: unavailable." : "Pre-trade checks: unavailable.",
        lines: hideTableData ? [] : rulesErr ? [String(rulesErr)] : [],
        block: false,
      };
    }

    const errs = Array.isArray(rules?.errors) ? rules.errors : [];
    if (errs.length > 0) {
      return {
        status: "neutral",
        title: "Pre-trade checks: unavailable.",
        lines: hideTableData ? [] : errs.map((x) => String(x)),
        block: false,
      };
    }

    const minQty = uiMinQty ?? rules?.min_qty;
    const minNotional = rules?.min_notional;
    const baseInc = rules?.base_increment;
    const priceInc = rules?.price_increment;

    const qtyDec = rules?.qty_decimals;
    const pxDec = rules?.price_decimals;

    const limitStrExpanded = expandExponential(limitPrice);

    if (qtyNum === null) {
      lines.push("Qty missing/invalid.");
      fails.push("qty_missing");
    }
    if (pxNum === null) {
      lines.push("Limit price missing/invalid.");
      fails.push("px_missing");
    }

    if (qtyNum !== null && minQty !== null && minQty !== undefined && Number.isFinite(Number(minQty))) {
      const mq = Number(minQty);
      if (qtyNum < mq) {
        lines.push(hideTableData ? "Qty below min." : `Qty min: need ≥ ${mq}.`);
        fails.push("qty_min");
      }
    }

    if (notional !== null && minNotional !== null && minNotional !== undefined && Number.isFinite(Number(minNotional))) {
      const mn = Number(minNotional);
      if (notional < mn) {
        lines.push(hideTableData ? "Notional below min." : `Notional min: need ≥ ${mn}.`);
        fails.push("notional_min");
      }
    }

    if (qtyNum !== null) {
      const dCount = countDecimalsFromString(qty);
      if (dCount !== null && Number.isFinite(Number(qtyDec)) && Number(qtyDec) >= 0) {
        const allowed = Math.min(Math.max(Math.trunc(Number(qtyDec)), 0), 18);
        if (dCount > allowed) {
          lines.push(hideTableData ? "Qty precision too high." : `Qty precision: ${dCount} decimals → allowed ${allowed}.`);
          fails.push("qty_precision");
        }
      }
    }

    if (pxNum !== null) {
      const dCount = countDecimalsFromString(limitStrExpanded);
      if (dCount !== null && Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0) {
        const allowed = Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18);
        if (dCount > allowed) {
          lines.push(hideTableData ? "Price precision too high." : `Price precision: ${dCount} decimals → allowed ${allowed}.`);
          fails.push("px_precision");
        }
      }
    }

    if (qtyNum !== null && baseInc !== null && baseInc !== undefined && String(baseInc).trim()) {
      const biStr = String(baseInc).trim();
      const ok = isMultipleOfStep(String(qty), biStr, qtyDec);
      if (ok === false) {
        lines.push(
          hideTableData
            ? "Qty step invalid."
            : `Qty step: qty must be a multiple of ${fmtStepValue(biStr, qtyDec) ?? biStr}.`
        );
        fails.push("qty_step");
      }
    }

    if (pxNum !== null && priceInc !== null && priceInc !== undefined && String(priceInc).trim()) {
      const piStr = String(priceInc).trim();
      const ok = isMultipleOfStep(String(limitStrExpanded), piStr, pxDec);
      if (ok === false) {
        lines.push(
          hideTableData
            ? "Price tick invalid."
            : `Price tick: price must be a multiple of ${fmtStepValue(piStr, pxDec) ?? piStr}.`
        );
        fails.push("px_tick");
      }
    }

    if (fails.length === 0) return { status: "ok", title: "Pre-trade checks: OK", lines: [], block: false };
    return { status: "fail", title: "Pre-trade checks: FAIL (blocked)", lines, block: true };
  }, [
    rulesLoading,
    rulesErr,
    rules,
    uiMinQty,
    qty,
    limitPrice,
    qtyNum,
    pxNum,
    notional,
    hideTableData,
    isSolanaLimitMode,
    jupiterFrontendInputUsdValue,
    jupiterMinFrontendEnforceable,
    side,
    solanaExpiryPreset,
    solanaExpiredAt,
  ]);

  const preTradeStyle = useMemo(() => {
    if (!preTrade) return null;
    if (preTrade.status === "ok") return { border: "1px solid #203a20", background: "#0f1a0f", color: "#cdeccd" };
    if (preTrade.status === "fail") return { border: "1px solid #4a1f1f", background: "#160b0b", color: "#ffd2d2" };
    return { border: "1px solid #2a2a2a", background: "#101010", color: "#cfcfcf" };
  }, [preTrade]);

  const canSubmitBase = useMemo(() => {
    const v = String(effectiveVenue || "").trim();
    const s = String(otSymbol || "").trim();
    if (!v || !s) return false;
    if (!(side === "buy" || side === "sell")) return false;

    // Solana DEX venues are swap-style:
    // - BUY uses Total (quote spend)
    // - SELL uses Qty (base spend)
    // Limit price is not required.
    if (isSolanaDexVenue) {
      if (isSolanaLimitMode) return qtyNum !== null && pxNum !== null;
      if (side === "buy") return totalQuoteNum !== null;
      return qtyNum !== null;
    }

    // CEX-style limit order
    return qtyNum !== null && pxNum !== null;
  }, [effectiveVenue, otSymbol, side, isSolanaDexVenue, qtyNum, pxNum, totalQuoteNum]);

  const canSubmit = useMemo(() => {
    if (!canSubmitBase) return false;
    if (preTrade?.block) return false;
    return true;
  }, [canSubmitBase, preTrade]);

  const buySpendQuote = useMemo(() => {
    if (side !== "buy") return null;

    // Solana DEX BUY spends quote = Total field
    if (isSolanaDexVenue) {
      return totalQuoteNum === null ? null : totalQuoteNum;
    }

    // CEX BUY spends quote = qty * limit
    if (qtyNum === null || pxNum === null) return null;
    const spend = qtyNum * pxNum;
    return Number.isFinite(spend) ? spend : null;
  }, [side, isSolanaDexVenue, qtyNum, pxNum, totalQuoteNum]);

  const buySpendCapacityQuote = useMemo(() => {
    if (side !== "buy") return null;
    const qAvail = toFiniteOrNull(quoteAvail);
    if (qAvail === null || qAvail < 0) return null;
    return qAvail;
  }, [side, quoteAvail]);

  const sellCapacity = useMemo(() => {
    if (side !== "sell") return null;
    const bAvail = toFiniteOrNull(baseAvail);
    return bAvail === null ? null : bAvail;
  }, [side, baseAvail]);

  const balanceWarning = useMemo(() => {
    if (side === "buy") {
      if (!quoteAsset) return null;
      if (buySpendQuote === null || buySpendCapacityQuote === null) return null;

      if (buySpendQuote > buySpendCapacityQuote + 1e-12) {
        return hideTableData
          ? "Insufficient available balance for this buy."
          : `Insufficient ${quoteAsset} available: need ${buySpendQuote.toLocaleString(undefined, {
              maximumFractionDigits: 12,
            })}, have ${buySpendCapacityQuote.toLocaleString(undefined, { maximumFractionDigits: 12 })}.`;
      }
      return null;
    }

    if (side === "sell") {
      if (!baseAsset) return null;
      if (qtyNum === null || sellCapacity === null) return null;

      if (qtyNum > sellCapacity + 1e-12) {
        return hideTableData
          ? "Insufficient available balance for this sell."
          : `Insufficient ${baseAsset} available: need ${qtyNum.toLocaleString(undefined, {
              maximumFractionDigits: 12,
            })}, have ${sellCapacity.toLocaleString(undefined, { maximumFractionDigits: 12 })}.`;
      }
      return null;
    }
    return null;
  }, [side, hideTableData, quoteAsset, baseAsset, buySpendQuote, buySpendCapacityQuote, qtyNum, sellCapacity]);

  // NEW: helper to open the submission result modal deterministically
  function openSubmitResultModal(kind, payload, title) {
    const t = String(title || (kind === "error" ? "Order Submit Failed" : "Order Submit Result"));
    setSubmitResultKind(kind);
    setSubmitResultPayload(payload);
    setSubmitResultTitle(t);

    if (hideTableData) {
      setSubmitResultText(
        kind === "error"
          ? "Result hidden (Hide table data is enabled). Disable Hide table data to view error details."
          : "Result hidden (Hide table data is enabled). Disable Hide table data to view order details."
      );
    } else {
      try {
        if (typeof payload === "string") setSubmitResultText(payload);
        else setSubmitResultText(JSON.stringify(payload, null, 2));
      } catch {
        setSubmitResultText(String(payload ?? ""));
      }
    }

    setShowSubmitResult(true);
  }

  // UPDATED: refresh balances AFTER submit returns OK (longer window + focus base/quote)
  async function refreshBalancesAfterSubmit({ venueKey, focusBase, focusQuote } = {}) {
    try {
      const v = String(venueKey || "").toLowerCase().trim();
      if (!v) return;

      const focus = [focusBase, focusQuote].filter(Boolean);

      // Let backend/venue settle, especially for holds/reserved amounts.
      await new Promise((r) => setTimeout(r, 900));

      const changed = await refreshAvailBalances({
        venueOverride: v,
        force: true, // post-submit should be strict
        focusAssets: focus.length ? focus : null,
        maxPolls: 5,
        initialDelayMs: 0, // already waited above
        pollBackoffMs: [600, 900, 1300, 1800, 2200],
      });

      // If nothing changed yet, schedule one follow-up pass (non-spammy).
      if (!changed) {
        setTimeout(() => {
          refreshAvailBalances({
            venueOverride: v,
            force: false,
            focusAssets: focus.length ? focus : null,
            maxPolls: 3,
            initialDelayMs: 0,
            pollBackoffMs: [900, 1400, 2000],
          });
        }, 4000);
      }
    } catch {
      // Any errors are surfaced by refreshAvailBalances via balErr.
    }
  }

  
  async function submitSolanaSwapOrder() {
    const tok = getAuthToken();

    if (!tok) {
      const msg = "Login required to place orders.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    // Never silently no-op.
    if (!canSubmit) {
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Swap is not currently submittable — check Qty/Total and venue rules.";
      onToast?.({ kind: "warn", msg: reason });
      openSubmitResultModal("error", reason, "Swap Not Submitted");
      return;
    }

    if (!apiBase) {
      const msg = "apiBase not set";
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    const v = String(effectiveVenue || "").toLowerCase().trim();
    const sym = String(otSymbol || "").trim();
    let address = getInjectedSolanaPubkeyBase58();

    if (!address) {
      address = await ensureSolanaWalletConnected();
    }
    if (!address) {
      const msg = "Connect a supported Solana wallet (Jupiter / Solflare / Phantom / Backpack) to submit swaps.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    // Amount in HUMAN units of the INPUT token:
    // - BUY  => QUOTE spend ("Total")
    // - SELL => BASE qty ("Qty")
    const amount = side === "buy" ? Number(totalQuoteNum) : Number(qtyNum);
    if (!Number.isFinite(amount) || amount <= 0) {
      const msg = side === "buy" ? "Enter a valid Total amount." : "Enter a valid Qty amount.";
      openSubmitResultModal("error", msg, "Swap Submit Failed");
      return;
    }

    setSubmitting(true);
    setSubmitError(null);
    setSubmitOk(null);

    try {
      const slippageBps = 100;
      let swapResp = null;

      const routerMode = String(preferredSolanaRouterMode || "auto").toLowerCase().trim();
      const preferRaydium = v === "solana_raydium" || routerMode === "raydium";
      const ultraOnly = routerMode === "ultra";
      const metisOnly = routerMode === "metis";

      if (preferRaydium) {
        swapResp = await fetchSolanaSwapTx({
          provider: "raydium",
          symbol: sym,
          side,
          amount,
          address,
          slippageBps,
          tok,
        });
      } else if (ultraOnly) {
        swapResp = await fetchSolanaSwapTx({
          provider: "jupiter_ultra",
          symbol: sym,
          side,
          amount,
          address,
          slippageBps,
          tok,
        });
      } else if (metisOnly) {
        swapResp = await fetchSolanaSwapTx({
          provider: "jupiter_metis",
          symbol: sym,
          side,
          amount,
          address,
          slippageBps,
          tok,
        });
      } else {
        try {
          swapResp = await fetchSolanaSwapTx({
            provider: "jupiter_ultra",
            symbol: sym,
            side,
            amount,
            address,
            slippageBps,
            tok,
          });
        } catch (eUltra) {
          const msgUltra = eUltra?.message || "Failed to build Jupiter Ultra swap";
          if (!isBlockedJupiterTokenError(msgUltra)) throw eUltra;

          try {
            swapResp = await fetchSolanaSwapTx({
              provider: "jupiter_metis",
              symbol: sym,
              side,
              amount,
              address,
              slippageBps,
              tok,
            });
          } catch (eMetis) {
            const msgMetis = eMetis?.message || "Failed to build Jupiter Metis swap";
            if (!isBlockedJupiterTokenError(msgMetis)) throw eMetis;

            onToast?.({
              kind: "warn",
              msg: "Jupiter Ultra/Metis blocked or could not route this token — retrying through Raydium.",
            });

            swapResp = await fetchSolanaSwapTx({
              provider: "raydium",
              symbol: sym,
              side,
              amount,
              address,
              slippageBps,
              tok,
            });
          }
        }
      }

      const { provider, data: j, txB64: b64 } = swapResp || {};
      if (!b64) throw new Error("Missing swap transaction in response");

      // Deserialize VersionedTransaction
      const { VersionedTransaction } = await import("@solana/web3.js");
      const bytes = Uint8Array.from(atob(String(b64)), (c) => c.charCodeAt(0));
      const tx = VersionedTransaction.deserialize(bytes);

      const { provider: providerWallet } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (!providerWallet) throw new Error("No supported Solana wallet provider found (Jupiter / Solflare / Phantom / Backpack).");

      let signature = null;

      if (provider === "jupiter_ultra") {
        if (typeof providerWallet.signTransaction !== "function") {
          throw new Error("Wallet provider missing signTransaction (required for Jupiter Ultra).");
        }
        const signedTx = await providerWallet.signTransaction(tx);
        if (!signedTx || typeof signedTx.serialize !== "function") {
          throw new Error("Wallet did not return a signed transaction for Jupiter Ultra.");
        }
        const signedBytes = signedTx.serialize();
        const signedB64 = btoa(String.fromCharCode(...Array.from(signedBytes)));
        const requestId = j?.requestId || j?.order?.requestId;
        if (!requestId) throw new Error("Missing requestId from Jupiter Ultra order response.");
        const execResp = await executeSolanaUltraSwap({ signedTransaction: signedB64, requestId, tok });
        signature = execResp?.signature || "";
      } else {
        if (typeof providerWallet.signAndSendTransaction === "function") {
          const res = await providerWallet.signAndSendTransaction(tx);
          signature = res?.signature || res?.sig || res;
        } else if (typeof providerWallet.signTransaction === "function") {
          throw new Error("Wallet does not support signAndSendTransaction (required).");
        } else {
          throw new Error("Wallet provider missing signAndSendTransaction.");
        }
      }

      signature = signature ? String(signature) : "";
      if (!signature) throw new Error("Missing signature from wallet response");

      if (provider === "jupiter" || provider === "jupiter_metis") {
        const base = String(apiBase || "").replace(/\/+$/, "");
        const recUrl = `${base}/api/solana_dex/jupiter/record_submit`;

        try {
          const headers = { "Content-Type": "application/json" };
          if (tok) headers.Authorization = `Bearer ${tok}`;

          const recPayload = {
            signature,
            chain: "solana",
            venue: v || "solana_jupiter",
            ts: Math.floor(Date.now() / 1000),
            wallet_address: address,
            raw_symbol: sym,
            resolved_symbol: null,
            side,
            base_qty: side === "sell" ? Number(qtyNum) : null,
            quote_qty: side === "buy" ? Number(totalQuoteNum) : null,
            price: null,
            fee_quote: null,
            status: "submitted",
            raw: { quote: j?.quote ?? null, last_valid_block_height: j?.last_valid_block_height ?? null },
          };
          await fetch(recUrl, { method: "POST", headers, body: JSON.stringify(recPayload) });
        } catch {
          // ignore
        }
      }

      const okPayload = { ok: true, provider: provider || "jupiter_metis", signature };
      setSubmitOk(okPayload);
      openSubmitResultModal("ok", okPayload, `${String(provider || "jupiter_metis").replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase())} Swap Submitted`);

      refreshBalancesAfterSubmit({ venueKey: provider === "raydium" ? "solana_raydium" : v, focusBase: baseAsset, focusQuote: quoteAsset });
    } catch (e) {
      const msg = e?.message || "Failed to submit swap";
      setSubmitError(msg);
      openSubmitResultModal("error", msg, "Swap Submit Failed");
    } finally {
      setSubmitting(false);
    }
  }


async function submitSolanaTriggerLimitOrder() {
    const tok = getAuthToken();

    if (!tok) {
      const msg = "Login required to place orders.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
      return;
    }

    if (!canSubmit) {
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Jupiter limit order is not currently submittable — check Qty/Price and minimum rules.";
      onToast?.({ kind: "warn", msg: reason });
      openSubmitResultModal("error", reason, "Jupiter Limit Not Submitted");
      return;
    }

    if (!apiBase) {
      const msg = "apiBase not set";
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
      return;
    }

    let address = getInjectedSolanaPubkeyBase58();
    if (!address) address = await ensureSolanaWalletConnected();
    if (!address) {
      const msg = "Connect a supported Solana wallet (Jupiter / Solflare / Phantom / Backpack) to submit Jupiter limit orders.";
      onToast?.({ kind: "warn", msg });
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
      return;
    }

    const quantity = Number(qtyNum);
    const limit_price = Number(pxNum);
    if (!Number.isFinite(quantity) || quantity <= 0) {
      openSubmitResultModal("error", "Enter a valid Qty amount.", "Jupiter Limit Submit Failed");
      return;
    }
    if (!Number.isFinite(limit_price) || limit_price <= 0) {
      openSubmitResultModal("error", "Enter a valid Limit price.", "Jupiter Limit Submit Failed");
      return;
    }

    setSubmitting(true);
    setSubmitError(null);
    setSubmitOk(null);

    try {
      const base = String(apiBase || "").replace(/\/+$/, "");
      const url = `${base}/api/solana_dex/jupiter/trigger/create_order`;
      const headers = { "Content-Type": "application/json" };
      if (tok) headers.Authorization = `Bearer ${tok}`;

      const expired_at = solanaExpiredAt === undefined ? undefined : String(solanaExpiredAt);

      const payload = {
        symbol: String(otSymbol || "").trim(),
        side,
        quantity,
        limit_price,
        user_pubkey: address,
        payer: address,
        expired_at,
        slippage_bps: 0,
        wrap_and_unwrap_sol: true,
      };

      const r = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });

      if (!r.ok) {
        const txt = await r.text();
        throw new Error(txt || `HTTP ${r.status}`);
      }

      const j = await r.json();
      const txB64 = j?.transaction;
      if (!txB64) throw new Error("Missing transaction in Jupiter Trigger response");

      const { VersionedTransaction } = await import("@solana/web3.js");
      const bytes = Uint8Array.from(atob(String(txB64)), (c) => c.charCodeAt(0));
      const tx = VersionedTransaction.deserialize(bytes);

      const w = typeof window !== "undefined" ? window : null;
      const { provider, key: providerKey } = resolveInjectedSolanaProvider(preferredSolanaWallet);
      if (!provider) throw new Error("No supported Solana wallet provider found (Jupiter / Solflare / Phantom / Backpack).");

      let signature = null;
      if (typeof provider.signAndSendTransaction === "function") {
        const res = await provider.signAndSendTransaction(tx);
        signature = res?.signature || res?.sig || res;
      } else {
        throw new Error("Wallet provider missing signAndSendTransaction.");
      }

      signature = signature ? String(signature) : "";

      try {
        await fetch(`${base}/api/solana_dex/jupiter/trigger/register_open_order`, {
          method: "POST",
          headers,
          body: JSON.stringify({
            symbol: String(otSymbol || "").trim(),
            side,
            quantity,
            limit_price,
            user_pubkey: address,
            signature: signature || "",
            request_id: j?.requestId ?? null,
            order: j?.order ?? "",
            expired_at,
          }),
        });
      } catch {}

      const okPayload = {
        ok: true,
        mode: "limit",
        signature: signature || null,
        requestId: j?.requestId ?? null,
        order: j?.order ?? null,
      };

      setSubmitOk(okPayload);
      openSubmitResultModal("ok", okPayload, "Jupiter Limit Submitted");
      refreshBalancesAfterSubmit({ venueKey: "solana_jupiter", focusBase: baseAsset, focusQuote: quoteAsset });
    } catch (e) {
      const msg = e?.message || "Failed to submit Jupiter limit order";
      setSubmitError(msg);
      openSubmitResultModal("error", msg, "Jupiter Limit Submit Failed");
    } finally {
      setSubmitting(false);
    }
  }

async function submitLimitOrder() {
  const tok = getAuthToken();

  // Never silently no-op.
  // If something changed after the confirm modal opened, surface why.
  if (!canSubmit) {
    const reason =
      preTrade?.message ||
      (preTrade?.status ? String(preTrade.status) : "") ||
      "Order is not currently submittable — check Qty/Price and venue rules.";
    onToast?.({ kind: "warn", msg: reason });
    openSubmitResultModal("error", reason, "Order Not Submitted");
    return;
  }

  // Do not silently no-op when logged out.
  // Attempt the request without Authorization so we always get a network response (401/403).
  if (!tok) {
    onToast?.({ kind: "warn", msg: "Login required to place orders." });
  }

  setSubmitting(true);
    setSubmitError(null);
    setSubmitOk(null);

    try {
      const v = String(effectiveVenue || "").toLowerCase().trim();
      const sym = String(otSymbol || "").trim();

      const payload = {
        venue: v,
        symbol: sym,
        side,
        type: "limit",
        qty: Number(qtyNum),
        limit_price: Number(pxNum),
        tif,
        post_only: !!postOnly,
        client_order_id: clientOid ? String(clientOid).trim() : undefined,
      };

      const headers = { "Content-Type": "application/json" };
      if (tok) headers.Authorization = `Bearer ${tok}`;

      const base = String(apiBase || "").replace(/\/+$/, "");
      const url = `${base}/api/trade/order`;

      const r = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });

      if (!r.ok) {
        const txt = await r.text();
        const errMsg = txt || `HTTP ${r.status}`;
        throw new Error(errMsg);
      }

      const j = await r.json();
      setSubmitOk(j);

      // Show modal instead of inline printing below the widget
      openSubmitResultModal("ok", j, "Order Submitted");

      // UPDATED: capture venue + base/quote at submit time and refresh deterministically.
      refreshBalancesAfterSubmit({ venueKey: v, focusBase: baseAsset, focusQuote: quoteAsset });
    } catch (e) {
      const msg = e?.message || "Failed to submit order";
      setSubmitError(msg);

      // Show modal for error too (same UX pattern)
      openSubmitResultModal("error", msg, "Order Submit Failed");
    } finally {
      setSubmitting(false);
    }
  }

  function openConfirm() {
    if (submitting) return;
    if (!canSubmit) {
      // Restore prior UX expectation: give an explicit reason instead of "nothing happens".
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Order is not currently submittable — check Qty/Price and venue rules.";
      onToast?.({ kind: "warn", msg: reason });
      return;
    }
    setShowConfirm(true);
  }

  function confirmAndSubmit() {
    if (submitting) return;
    if (!canSubmit) {
      const reason =
        preTrade?.message ||
        (preTrade?.status ? String(preTrade.status) : "") ||
        "Order is not currently submittable — check Qty/Price and venue rules.";
      onToast?.({ kind: "warn", msg: reason });
      setShowConfirm(false);
      return;
    }
    setShowConfirm(false);
    // Surface immediate feedback and never allow a silent no-op.
    openSubmitResultModal("info", "Submitting…", "Submitting");
    void (
      isSolanaLimitMode
        ? submitSolanaTriggerLimitOrder()
        : isSolanaDexVenue
          ? submitSolanaSwapOrder()
          : submitLimitOrder()
    ).catch((e) => {
      const msg = e?.message || String(e);
      openSubmitResultModal(
        "error",
        msg,
        isSolanaLimitMode ? "Jupiter Limit Submit Failed" : isSolanaDexVenue ? "Swap Submit Failed" : "Order Submit Failed"
      );
    });
  }

  // Defensive: if styles is missing, do not crash the entire UI.
  const safeStyles = styles || {};
  const safeDock = safeStyles.orderBookDock || {};
  const safeButton = safeStyles.button || {};
  const safeButtonDisabled = safeStyles.buttonDisabled || {};
  const safeInput = safeStyles.input || {};
  const safeSelect = safeStyles.select || {};
  const safePill = safeStyles.pill || {};
  const darkSelectStyle = {
    ...safeSelect,
    minWidth: 110,
    padding: "4px 6px",
    background: "#101010",
    backgroundColor: "#101010",
    color: "#eaeaea",
    border: "1px solid rgba(255,255,255,0.14)",
  };
  const darkOptionStyle = { backgroundColor: "#101010", color: "#eaeaea" };
  const safeMuted = safeStyles.muted || {};
  const safeWidgetTitleRow = safeStyles.widgetTitleRow || {};
  const safeWidgetSub = safeStyles.widgetSub || {};
  const safeCodeError = safeStyles.codeError || {};

  const shellStyleBase = inlineMode
    ? {
        ...safeDock,
        width: "100%",
        maxWidth: "100%",
        height: "100%",
        maxHeight: "100%",
        resize: "none",
        overflow: "hidden",
        marginTop: 0,
        display: "flex",
        flexDirection: "column",
        flex: "1 1 auto",
        minHeight: 0,
        minWidth: 0,
        boxSizing: "border-box",
      }
    : {
        ...safeDock,
        width: box.w,
        height: box.h,
        resize: "none",
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        boxSizing: "border-box",
      };

  const sideAccent = side === "buy" ? "#1f6f3a" : "#7a2b2b";
  const sideBg = side === "buy" ? "rgba(31, 111, 58, 0.07)" : "rgba(122, 43, 43, 0.07)";

  const shellStyle = {
    ...shellStyleBase,
    boxShadow: `0 0 0 1px ${sideAccent} inset`,
    background: shellStyleBase?.background ? shellStyleBase.background : undefined,
    backgroundImage: `linear-gradient(${sideBg}, ${sideBg})`,
  };

  const fixedWrapperStyle = inlineMode
    ? {
        width: "100%",
        height: "100%",
        minHeight: 0,
        minWidth: 0,
        display: "flex",
        flexDirection: "column",
      }
    : { position: "fixed", left: box.x, top: box.y, zIndex: 61, userSelect: "none" };

  const rowStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" };
  const rowTightStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginTop: 6 };
  const sectionGap = 6;


  const ticketScrollBodyStyle = {
    display: "flex",
    flexDirection: "column",
    flex: "1 1 auto",
    minHeight: 0,
    minWidth: 0,
    overflowY: "auto",
    overflowX: "hidden",
    paddingRight: 6,
    scrollbarWidth: "thin",
    scrollbarColor: "rgba(255,255,255,0.22) transparent",
  };

  const sideBtnBase = { ...safeButton, padding: "6px 10px", borderRadius: 10, fontWeight: 800, lineHeight: 1.1 };
  const sideBtnActive = { background: "#151515", border: "1px solid #3a3a3a" };

  const fmtAvail = (n) => {
    if (n === null || n === undefined) return "—";
    const x = Number(n);
    if (!Number.isFinite(x)) return "—";
    return x.toLocaleString(undefined, { maximumFractionDigits: 12 });
  };

  const maskIfHidden = (s) => (hideTableData ? "••••" : s);

  const totalLabel = useMemo(() => {
    const q = String(quoteAsset || "").trim();
    return q ? q : "Quote";
  }, [quoteAsset]);

  const walletKitButtonLabel = useMemo(() => {
    if (!walletKitConnected) return "Connect Wallet";

    const address =
      solanaWalletState?.address ||
      solanaProviderPubkeyBase58(walletKitBridgeProvider) ||
      (typeof walletKit?.publicKey?.toBase58 === "function"
        ? walletKit.publicKey.toBase58()
        : typeof walletKit?.publicKey?.toString === "function"
          ? walletKit.publicKey.toString()
          : "");

    if (!address) return "Wallet Connected";
    return hideTableData ? "••••" : shortenWalletAddress(address);
  }, [walletKitConnected, solanaWalletState?.address, walletKitBridgeProvider, walletKit?.publicKey, hideTableData]);

  const walletKitButtonTitle = useMemo(() => {
    if (!walletKitConnected) return "Open the Jupiter Wallet Kit connect dialog";
    const address = solanaWalletState?.address || "";
    const label = solanaWalletLabel || walletKitRawAdapterName || "Wallet";
    if (address && !hideTableData) return `${label}: ${address}`;
    return "Open the Jupiter Wallet Kit wallet manager";
  }, [walletKitConnected, solanaWalletState?.address, solanaWalletLabel, walletKitRawAdapterName, hideTableData]);

  const walletButtonVisualKey = useMemo(() => {
    return solanaWalletState?.key || walletKitSelectedKey || classifyWalletAdapterNameToKey(walletKitRawAdapterName) || null;
  }, [solanaWalletState?.key, walletKitSelectedKey, walletKitRawAdapterName]);

  const walletButtonVisualAddress = useMemo(() => {
    return (
      solanaWalletState?.address ||
      solanaProviderPubkeyBase58(walletKitBridgeProvider) ||
      (typeof walletKit?.publicKey?.toBase58 === "function"
        ? walletKit.publicKey.toBase58()
        : typeof walletKit?.publicKey?.toString === "function"
          ? walletKit.publicKey.toString()
          : "")
    );
  }, [solanaWalletState?.address, walletKitBridgeProvider, walletKit?.publicKey]);

  const walletButtonVisualMeta = useMemo(() => {
    return getSolanaWalletVisualMeta(
      walletButtonVisualKey,
      solanaWalletLabel || walletKitRawAdapterName || "Wallet",
      walletKit?.wallet?.adapter?.icon || walletKit?.wallet?.icon || ""
    );
  }, [walletButtonVisualKey, solanaWalletLabel, walletKitRawAdapterName, walletKit?.wallet]);

  const walletKitSelectableWallets = useMemo(() => {
    const raw = Array.isArray(walletKit?.wallets) ? walletKit.wallets : [];
    const seen = new Set();
    const out = [];

    for (const entry of raw) {
      const adapterName = String(entry?.adapter?.name || entry?.name || "").trim();
      const key = classifyWalletAdapterNameToKey(adapterName);
      if (!key || !adapterName) continue;
      const uniq = `${key}::${adapterName}`;
      if (seen.has(uniq)) continue;
      seen.add(uniq);
      out.push({
        key,
        name: adapterName,
        label: getSolanaWalletVisualMeta(key, adapterName, entry?.adapter?.icon || entry?.icon || "").label || adapterName,
      });
    }

    const order = { jupiter: 0, solflare: 1, phantom: 2, backpack: 3 };
    out.sort((a, b) => {
      const oa = Number.isFinite(order[a.key]) ? order[a.key] : 99;
      const ob = Number.isFinite(order[b.key]) ? order[b.key] : 99;
      if (oa !== ob) return oa - ob;
      return String(a.label || a.name || "").localeCompare(String(b.label || b.name || ""));
    });
    return out;
  }, [walletKit?.wallets]);

  const walletKitSelectedAdapterName = useMemo(() => {
    return String(walletKit?.wallet?.adapter?.name || walletKit?.wallet?.name || "").trim();
  }, [walletKit?.wallet]);

  async function handleWalletKitSelectChange(nextName) {
    const targetName = String(nextName || "").trim();
    if (!targetName) return;

    try {
      const currentName = String(walletKit?.wallet?.adapter?.name || walletKit?.wallet?.name || "").trim();
      const sameWallet = currentName && currentName === targetName;

      setBalAvail({});
      setBalErr(null);
      setWalletKitPendingConnectName(targetName);

      const nextKey = classifyWalletAdapterNameToKey(targetName);
      if (nextKey) setPreferredSolanaWallet(nextKey);

      if (sameWallet) {
        if (!walletKit?.connected && typeof walletKit?.connect === "function") {
          try { await walletKit.connect(); } finally { setWalletKitPendingConnectName(""); }
        } else {
          setWalletKitPendingConnectName("");
        }
        return;
      }

      // Let Wallet Kit switch adapters first; keep the manager button usable in case
      // the target wallet requires an explicit modal step/approval.
      if (typeof walletKit?.select === "function") {
        walletKit.select(targetName);
      }

      if (typeof window !== "undefined") {
        window.setTimeout(() => {
          try { openWalletKitManager(); } catch {}
        }, 40);
      }
    } catch (e) {
      const msg = e?.message || `Failed to switch wallet to ${targetName}.`;
      setWalletKitPendingConnectName("");
      setSubmitError(msg);
      openSubmitResultModal("error", msg, "Wallet Switch Failed");
    }
  }

  useEffect(() => {
    if (!isSolanaDexVenue) return;
    const targetName = String(walletKitPendingConnectName || "").trim();
    if (!targetName) return;

    const selectedName = String(walletKit?.wallet?.adapter?.name || walletKit?.wallet?.name || "").trim();
    if (!selectedName || selectedName !== targetName) return;

    if (walletKit?.connected && walletKit?.publicKey) {
      setWalletKitPendingConnectName("");
      return;
    }

    if (typeof walletKit?.connect !== "function") {
      setWalletKitPendingConnectName("");
      return;
    }

    let cancelled = false;
    const t = setTimeout(() => {
      void (async () => {
        try {
          await walletKit.connect();
        } catch (e) {
          if (cancelled) return;
          const msg = e?.message || `Failed to connect ${targetName}.`;
          setSubmitError(msg);
          openSubmitResultModal("error", msg, "Wallet Connect Failed");
        } finally {
          if (!cancelled) setWalletKitPendingConnectName("");
        }
      })();
    }, 160);

    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [isSolanaDexVenue, walletKitPendingConnectName, walletKit?.wallet, walletKit?.connected, walletKit?.publicKey]);

  useEffect(() => {
    const targetName = String(walletKitPendingConnectName || "").trim();
    if (!targetName) return;

    const t = setTimeout(() => {
      setWalletKitPendingConnectName((cur) => (String(cur || "").trim() === targetName ? "" : cur));
    }, 2500);

    return () => clearTimeout(t);
  }, [walletKitPendingConnectName]);



  // NEW: allow re-opening the last submit result without re-submitting
  const hasLastSubmitResult = useMemo(
    () => submitResultPayload !== null && submitResultKind !== null,
    [submitResultPayload, submitResultKind]
  );

  const solanaSubmitEndpointLabel = useMemo(() => {
    if (!isSolanaDexVenue) return "/api/trade/order";
    if (isSolanaLimitMode) return "/api/solana_dex/jupiter/trigger/create_order";
    const v = String(effectiveVenue || "").toLowerCase().trim();
    const routerMode = String(preferredSolanaRouterMode || "auto").toLowerCase().trim();
    if (v === "solana_raydium" || routerMode === "raydium") return "/api/solana_dex/raydium/swap_tx";
    if (routerMode === "ultra") return "/api/solana_dex/jupiter/ultra_order → /api/solana_dex/jupiter/ultra_execute";
    if (routerMode === "metis") return "/api/solana_dex/jupiter/swap_tx";
    return "/api/solana_dex/jupiter/ultra_order → /api/solana_dex/jupiter/ultra_execute → fallback /api/solana_dex/jupiter/swap_tx → fallback /api/solana_dex/raydium/swap_tx";
  }, [isSolanaDexVenue, isSolanaLimitMode, effectiveVenue, preferredSolanaRouterMode]);

  async function copySubmitResultToClipboard() {
    try {
      if (!HAS_WINDOW || !navigator?.clipboard?.writeText) return;
      await navigator.clipboard.writeText(String(submitResultText || ""));
    } catch {
      // ignore
    }
  }

  // Input display value: expand exponent without mutating what user is typing.
  const limitDisplayValue = useMemo(() => {
    const s = String(limitPrice ?? "");
    if (!s) return "";
    return expandExponential(s);
  }, [limitPrice]);

  const confirmLines = useMemo(() => {
    const v = venueLabel || "—";
    const sym = String(otSymbol || "").trim() || "—";
    const qStr = qtyNum === null ? "—" : qtyNum.toLocaleString(undefined, { maximumFractionDigits: 18 });

    // IMPORTANT: never show sci in confirmation UI
    const pxStr = !limitPrice || pxNum === null ? "—" : expandExponential(limitPrice).toString();

    const totStr =
      notional === null ? "—" : notional.toLocaleString(undefined, { maximumFractionDigits: totalQuoteDecimals });
    const reqTotStr =
      totalQuoteNum === null ? "—" : totalQuoteNum.toLocaleString(undefined, { maximumFractionDigits: totalQuoteDecimals });

    return [
      { k: "Venue", v: hideVenueNames ? "••••" : v },
      { k: "Symbol", v: hideTableData ? "••••" : sym },
      { k: "Side", v: side.toUpperCase() },
      { k: "Type", v: isSolanaJupiterVenue ? (solanaOrderMode === "limit" ? "LIMIT" : "SWAP") : "LIMIT" },
      { k: "Qty", v: hideTableData ? "••••" : qStr },
      { k: "Limit", v: hideTableData ? "••••" : pxStr },
      { k: `Total (${totalLabel})`, v: hideTableData ? "••••" : totStr },
      ...(autoCalc ? [{ k: `Requested Total (${totalLabel})`, v: hideTableData ? "••••" : reqTotStr }] : []),
      ...(isSolanaLimitMode
        ? [{ k: "Expiry", v: hideTableData ? "••••" : solanaExpiryLabel }]
        : [{ k: "TIF", v: String(tif || "gtc").toUpperCase() }]),
      ...(!isSolanaLimitMode ? [{ k: "Post-only", v: postOnly ? "YES" : "NO" }] : []),
      ...(!isSolanaLimitMode && clientOid ? [{ k: "Client OID", v: hideTableData ? "••••" : String(clientOid) }] : []),
    ];
  }, [
    venueLabel,
    otSymbol,
    side,
    qtyNum,
    pxNum,
    limitPrice,
    notional,
    totalQuoteDecimals,
    totalQuoteNum,
    totalLabel,
    tif,
    postOnly,
    clientOid,
    hideTableData,
    hideVenueNames,
    autoCalc,
    isSolanaJupiterVenue,
    isSolanaLimitMode,
    solanaOrderMode,
    solanaExpiryLabel,
  ]);

  function openWalletKitManager() {
    try {
      const host = walletKitButtonHostRef.current;
      if (!host) return;
      const btn = host.querySelector('button, [role="button"], a');
      if (btn && typeof btn.click === "function") {
        btn.click();
      }
    } catch {
      // ignore
    }
  }


  return (
    <div style={fixedWrapperStyle}>
      <div style={shellStyle}>
        <div
          style={{
            ...safeWidgetTitleRow,
            cursor: inlineMode || locked ? "default" : "move",
            paddingBottom: 4,
            borderBottom: "1px solid #2a2a2a",
            marginBottom: 8,
          }}
          onMouseDown={onDragMouseDown}
          title={inlineMode ? "" : locked ? "Locked" : "Drag to move (snug gutter, no margins)"}
        >
          <h3 style={{ ...styles.widgetTitle, fontSize: 16, lineHeight: "18px" }}>Order Ticket</h3>
          <span style={safeWidgetSub}>
            Venue used: <b>{venueLabel || "—"}</b>
          </span>
        </div>


        {(forceTileMode || !inlineMode) && (
          <style>{`
            .utt-order-ticket-scroll::-webkit-scrollbar { width: 10px; }
            .utt-order-ticket-scroll::-webkit-scrollbar-track { background: transparent; }
            .utt-order-ticket-scroll::-webkit-scrollbar-thumb {
              background: rgba(255,255,255,0.18);
              border-radius: 999px;
              border: 2px solid transparent;
              background-clip: padding-box;
            }
            .utt-order-ticket-scroll::-webkit-scrollbar-thumb:hover {
              background: rgba(255,255,255,0.28);
              border: 2px solid transparent;
              background-clip: padding-box;
            }
          `}</style>
        )}

        <div
          style={ticketScrollBodyStyle}
          className="utt-order-ticket-scroll"
        >
          <div style={rowStyle}>
          <div style={safePill}>
            <span>Symbol</span>
            <input
              style={{ ...safeInput, width: 150 }}
              value={otSymbol}
              placeholder="e.g. BTC-USD"
              onChange={(e) => setOtSymbol(e.target.value)}
            />
          </div>

          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <button
              style={{
                ...sideBtnBase,
                ...(side === "buy" ? sideBtnActive : null),
                boxShadow: side === "buy" ? `0 0 0 1px ${sideAccent} inset` : undefined,
              }}
              onClick={() => setSide("buy")}
              type="button"
            >
              Buy
            </button>
            <button
              style={{
                ...sideBtnBase,
                ...(side === "sell" ? sideBtnActive : null),
                boxShadow: side === "sell" ? `0 0 0 1px ${sideAccent} inset` : undefined,
              }}
              onClick={() => setSide("sell")}
              type="button"
            >
              Sell
            </button>
          </div>

          {isSolanaJupiterVenue && (
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <button
                style={{
                  ...sideBtnBase,
                  ...(solanaOrderMode === "swap" ? sideBtnActive : null),
                  boxShadow: solanaOrderMode === "swap" ? "0 0 0 1px #2f4f8f inset" : undefined,
                }}
                onClick={() => setSolanaOrderMode("swap")}
                type="button"
                title="Use the existing Jupiter swap flow"
              >
                Swap
              </button>
              <button
                style={{
                  ...sideBtnBase,
                  ...(solanaOrderMode === "limit" ? sideBtnActive : null),
                  boxShadow: solanaOrderMode === "limit" ? "0 0 0 1px #8f6a2f inset" : undefined,
                }}
                onClick={() => setSolanaOrderMode("limit")}
                type="button"
                title="Use Jupiter Trigger limit orders"
              >
                Limit
              </button>
            </div>
          )}

          <label style={safePill} title="Lock position + size">
            <input type="checkbox" checked={locked} onChange={(e) => {
              const next = !!e.target.checked;
              setLocked(next);
              if (next) {
                // Capture anchor offsets so viewport resize (DevTools) doesn't shove the widget.
                setBox((prev) => {
                  const vw = window.innerWidth;
                  const vh = window.innerHeight;
                  const b = getGutterBounds();
                  const w = prev.w || DEFAULT_W;
                  const h = prev.h || DEFAULT_H;
                  const x = Number.isFinite(prev.x) ? prev.x : b.minX;
                  const y = Number.isFinite(prev.y) ? prev.y : b.minY;
                  const left = x - b.minX;
                  const top = y - b.minY;
                  const right = vw - (x + w);
                  const bottom = vh - (y + h);
                  const anchorX = left <= right ? "left" : "right";
                  const anchorY = top <= bottom ? "top" : "bottom";
                  return { ...prev, left, top, right, bottom, anchorX, anchorY };
                });
              }
            }} />
            <span>Lock</span>
          </label>
        </div>

        {rulesBanner && (
          <div
            style={{
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 10,
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
              ...rulesBannerStyle,
            }}
            title="Policy/rules checks are advisory; backend/venue may still accept/reject."
          >
            {rulesBanner.lines.map((ln, i) => (
              <div key={i}>{ln}</div>
            ))}
          </div>
        )}

        {preTrade && (
          <div
            style={{
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 10,
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
              ...preTradeStyle,
            }}
            title="Pre-trade checks use venue constraints (min + increments). When checks fail and rules are known, submit is blocked."
          >
            <div style={{ fontWeight: 900, marginBottom: preTrade.lines?.length ? 4 : 0 }}>{preTrade.title}</div>
            {Array.isArray(preTrade.lines) &&
              preTrade.lines.map((ln, i) => (
                <div key={i}>• {ln}</div>
              ))}
          </div>
        )}

        {isSolanaLimitMode && (
          <div
            style={{
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 10,
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
              border: "1px solid #3b3413",
              background: "#151208",
              color: "#f2e6b7",
            }}
            title="Jupiter requires a minimum current input-token value for Trigger limit orders."
          >
            Jupiter limit minimum: <b>${JUPITER_LIMIT_MIN_USD.toFixed(2)}</b>
            {jupiterMinFrontendEnforceable && jupiterFrontendInputUsdValue !== null ? (
              <> • Current frontend-estimated input value: <b>${jupiterFrontendInputUsdValue.toFixed(4)}</b></>
            ) : (
              <> • Backend will enforce current USD input-value minimum on submit.</>
            )}
          </div>
        )}

        <div style={{ ...rowTightStyle, marginTop: sectionGap }}>
          <div style={safePill}>
            <span>Qty</span>
            <input
              style={{ ...safeInput, width: 125 }}
              value={qty}
              placeholder="Amount"
              onChange={(e) => {
                lastEditedRef.current = "qty";
                setQty(e.target.value);
              }}
              inputMode="decimal"
            />
          </div>

          <div style={safePill}>
            <span>Limit</span>
            <input
              style={{ ...safeInput, width: 140 }}
              type="text"
              inputMode="decimal"
              pattern="^[0-9]*[.]?[0-9]*$"
              value={limitDisplayValue}
              placeholder="Limit price"
              onFocus={() => {
                limitEditingRef.current = true;
                limitSourceRef.current = "user";
              }}
              onChange={(e) => {
                limitEditingRef.current = true;
                limitSourceRef.current = "user";

                const cleaned = sanitizeDecimalInput(e.target.value);

                // Solana DEX venues: keep the user-picked decimal price as-is; do not CEX-normalize.
                if (isSolanaDexVenue) {
                  setLimitPrice(cleaned);
                  return;
                }

                // If user pasted/entered too many decimals, normalize immediately (prevents “stuck disabled button”).
                const d = countDecimalsFromString(expandExponential(cleaned));
                const pxDec = rules?.price_decimals;
                const allowed =
                  Number.isFinite(Number(pxDec)) && Number(pxDec) >= 0
                    ? Math.min(Math.max(Math.trunc(Number(pxDec)), 0), 18)
                    : null;

                if (d !== null && allowed !== null && d > allowed) {
                  const normalized = normalizeLimitPriceStr(cleaned, rules, side);
                  setLimitPrice(normalized);
                  return;
                }

                setLimitPrice(cleaned);
              }}
              onBlur={() => {
                limitEditingRef.current = false;
                limitSourceRef.current = "blur";

                if (!limitPrice) return;

                // Solana DEX venues: do not clamp/round limit price on blur.
                if (isSolanaDexVenue) return;

                const normalized = normalizeLimitPriceStr(limitPrice, rules, side);
                if (normalized && normalized !== String(limitPrice)) setLimitPrice(normalized);
              }}
            />
          </div>

          <div style={safePill} title={`Total (${totalLabel}) to spend/receive.`}>
            <span>Total</span>
            <input
              style={{ ...safeInput, width: 120 }}
              value={totalQuote}
              placeholder={totalLabel}
              onChange={(e) => {
                lastEditedRef.current = "total";
                const cleaned = sanitizeDecimalInput(e.target.value);
                setTotalQuote(cleaned);

                // DEX-only: ensure Total→Qty auto-calc works even when venue rules are unavailable.
                if (isSolanaDexVenue && autoCalc) {
                  const t = Number(cleaned);
                  const p = Number(expandExponential(limitPrice));
                  if (Number.isFinite(t) && t > 0 && Number.isFinite(p) && p > 0) {
                    const raw = t / p;
                    if (Number.isFinite(raw) && raw > 0) {
                      const nextQty = fmtPlain(raw, { maxFrac: 18 });
                      if (nextQty) setQty(nextQty);
                    }
                  }
                }
              }}
              inputMode="decimal"
            />
          </div>

          <label style={safePill} title="When enabled, Qty and Total stay in sync.">
            <input type="checkbox" checked={autoCalc} onChange={(e) => setAutoCalc(e.target.checked)} />
            <span>Auto-calc</span>
          </label>
        </div>

        <div style={{ ...rowTightStyle, marginTop: sectionGap }}>
          <div style={{ ...safePill, gap: 8 }}>
            <span style={{ opacity: 0.85 }}>Avail</span>

            <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
              {baseAsset ? (
                <>
                  <b>{baseAsset}</b>: {maskIfHidden(fmtAvail(baseAvail))}
                </>
              ) : (
                <>Base: —</>
              )}
            </span>

            <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
              {quoteAsset ? (
                <>
                  <b>{quoteAsset}</b>: {maskIfHidden(fmtAvail(quoteAvail))}
                </>
              ) : (
                <>Quote: —</>
              )}
            </span>

            <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
              Focus({side}):{" "}
              <b>
                {relevantAvailLabel}: {maskIfHidden(fmtAvail(relevantAvailValue))}
              </b>
            </span>

            <button
              style={{ ...safeButton, padding: "5px 8px", lineHeight: 1.05 }}
              onClick={() => refreshAvailBalances({ maxPolls: 2, pollBackoffMs: [800, 1200] })}
              disabled={balLoading}
              title="Refresh balances from venue"
            >
              {balLoading ? "…" : "Refresh"}
            </button>
          </div>

          {isSolanaDexVenue && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                flexWrap: "wrap",
                marginTop: 6,
                padding: "6px 10px",
                borderRadius: 10,
                border: "1px solid rgba(255,255,255,0.08)",
                background: "rgba(255,255,255,0.03)",
                fontSize: 12,
                opacity: 0.95,
              }}
            >
              <span style={{ display: "inline-flex", alignItems: "center", gap: 8, minWidth: 0 }}>
                <span
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: 999,
                    background: solanaWalletConnected ? "rgba(46, 204, 113, 0.95)" : "rgba(231, 76, 60, 0.95)",
                    boxShadow: "0 0 0 2px rgba(0,0,0,0.35)",
                  }}
                />
                {solanaWalletConnected ? (
                  <span>
                    Connected w/<b style={{ marginLeft: 4 }}>{solanaWalletLabel || "Wallet"}</b>
                  </span>
                ) : (
                  <span>
                    Disconnected <span style={{ opacity: 0.75 }}>({solanaWalletLabel || "Wallet"})</span>
                  </span>
                )}
              </span>

              <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1, opacity: 0.9 }}>
                {walletKitRawAdapterName
                  ? `Managed by Wallet Kit: ${hideTableData ? "••••" : walletKitRawAdapterName}`
                  : solanaWalletLabel
                    ? `Resolved wallet: ${hideTableData ? "••••" : solanaWalletLabel}`
                    : "Use Wallet Kit to connect a supported Solana wallet."}
              </span>

              {isSolanaJupiterVenue ? (
                <label style={{ display: "inline-flex", alignItems: "center", gap: 6, marginLeft: "auto", opacity: 0.92, flexWrap: "nowrap" }}>
                  <span>Router</span>
                  <select
                    style={{ ...darkSelectStyle, minWidth: 104 }}
                    value={preferredSolanaRouterMode}
                    onChange={(e) => setPreferredSolanaRouterModeState(e.target.value)}
                    title="Swap routing source"
                  >
                    <option value="auto" style={darkOptionStyle}>Auto</option>
                    <option value="ultra" style={darkOptionStyle}>Jupiter Ultra</option>
                    <option value="metis" style={darkOptionStyle}>Jupiter Metis</option>
                    <option value="raydium" style={darkOptionStyle}>Raydium</option>
                  </select>
                </label>
              ) : null}
            </div>
          )}

          {isSolanaDexVenue && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                flexWrap: "wrap",
                marginTop: 6,
                padding: "6px 10px",
                borderRadius: 10,
                border: "1px solid rgba(255,255,255,0.08)",
                background: "rgba(255,255,255,0.02)",
                fontSize: 12,
                opacity: 0.95,
                position: "relative",
              }}
            >
              <span style={{ opacity: 0.9, fontWeight: 700 }}>Wallet Kit</span>
              <label style={{ display: "inline-flex", alignItems: "center", gap: 6, flexWrap: "nowrap" }}>
                <span style={{ opacity: 0.82 }}>Wallet</span>
                <select
                  style={{ ...darkSelectStyle, minWidth: 112 }}
                  value={walletKitSelectedAdapterName || ""}
                  onChange={(e) => { void handleWalletKitSelectChange(e.target.value); }}
                  title="Switch Wallet Kit wallet"
                  
                >
                  {!walletKitSelectedAdapterName && <option value="" style={darkOptionStyle}>Select wallet</option>}
                  {walletKitSelectableWallets.map((opt) => (
                    <option key={opt.name} value={opt.name} style={darkOptionStyle}>
                      {opt.label}
                    </option>
                  ))}
                </select>
              </label>
              <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
                {walletKitPendingConnectName
                  ? `Switching to: ${hideTableData ? "••••" : walletKitPendingConnectName}`
                  : walletKitRawAdapterName
                    ? `${walletKitConnected ? "Selected" : "Last selected"}: ${hideTableData ? "••••" : walletKitRawAdapterName}`
                    : "Open wallet manager"}
              </span>
              <button
                type="button"
                style={{
                  ...safeButton,
                  padding: walletKitConnected ? "6px 8px" : "7px 10px",
                  marginLeft: "auto",
                  minWidth: walletKitConnected ? 132 : 120,
                  maxWidth: walletKitConnected ? 176 : 144,
                  flex: "0 1 auto",
                  fontWeight: 800,
                  borderColor: walletKitConnected ? walletButtonVisualMeta?.border : safeButton?.borderColor,
                  boxShadow: walletKitConnected ? `0 0 0 1px ${walletButtonVisualMeta?.border || "rgba(255,255,255,0.10)"} inset, 0 0 14px ${walletButtonVisualMeta?.glow || "transparent"}` : undefined,
                }}
                onClick={openWalletKitManager}
                title={walletKitButtonTitle}
                
              >
                {walletKitConnected ? (
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 5, minWidth: 0, maxWidth: "100%" }}>
                    {walletButtonVisualMeta?.icon ? (
                      <img
                        src={walletButtonVisualMeta.icon}
                        alt={walletButtonVisualMeta.label || "Wallet"}
                        style={{
                          width: 18,
                          height: 18,
                          borderRadius: 6,
                          objectFit: "cover",
                          boxShadow: "0 0 0 1px rgba(255,255,255,0.12) inset",
                          flex: "0 0 auto",
                        }}
                      />
                    ) : (
                      <span
                        aria-hidden="true"
                        style={{
                          width: 18,
                          height: 18,
                          borderRadius: 6,
                          display: "inline-flex",
                          alignItems: "center",
                          justifyContent: "center",
                          background: walletButtonVisualMeta?.fallbackBg || "#0f172a",
                          color: walletButtonVisualMeta?.fallbackFg || "#e5f3ff",
                          fontSize: 11,
                          fontWeight: 900,
                          lineHeight: 1,
                          flex: "0 0 auto",
                        }}
                      >
                        {walletButtonVisualMeta?.fallbackText || "W"}
                      </span>
                    )}

                    <span style={{ display: "inline-flex", alignItems: "baseline", gap: 4, minWidth: 0, maxWidth: "100%", whiteSpace: "nowrap", overflow: "hidden" }}>
                      <span style={{ color: walletButtonVisualMeta?.color || "#eaeaea", fontWeight: 900, fontSize: 11, flex: "0 0 auto" }}>
                        {hideTableData ? "••••" : (walletButtonVisualMeta?.label || "Wallet")}
                      </span>
                      <span
                        style={{
                          color: walletButtonVisualMeta?.color || "#eaeaea",
                          opacity: 0.98,
                          fontWeight: 800,
                          fontSize: 11,
                          minWidth: 0,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                        }}
                      >
                        {hideTableData ? "••••" : shortenWalletAddress(walletButtonVisualAddress)}
                      </span>
                    </span>
                  </span>
                ) : (
                  walletKitButtonLabel
                )}
              </button>
              <div
                ref={walletKitButtonHostRef}
                aria-hidden="true"
                style={{
                  position: "absolute",
                  right: 0,
                  bottom: 0,
                  width: 1,
                  height: 1,
                  overflow: "hidden",
                  opacity: 0,
                  pointerEvents: "none",
                }}
              >
                <UnifiedWalletButton />
              </div>
            </div>
          )}

          {balErr && (isSolanaDexVenue || !String(balErr).includes("429")) && (
            <div style={{ ...safeMuted, fontSize: 11, color: "#ff6b6b", lineHeight: 1.1 }}>
              Bal: {hideTableData ? "Hidden" : balErr}
            </div>
          )}
        </div>

        {balanceWarning && (
          <div
            style={{
              marginTop: 6,
              border: "1px solid #3b3413",
              background: "#151208",
              padding: "6px 8px",
              borderRadius: 10,
              color: "#f2e6b7",
              fontSize: 11,
              lineHeight: 1.15,
              whiteSpace: "pre-wrap",
            }}
          >
            {balanceWarning}
          </div>
        )}

        <div style={{ ...rowTightStyle, marginTop: sectionGap }}>
          {isSolanaLimitMode ? (
            <>
              <div style={safePill}>
                <span>Expiry</span>
                <select style={darkSelectStyle} value={solanaExpiryPreset} onChange={(e) => setSolanaExpiryPreset(e.target.value)}>
                  <option value="never" style={darkOptionStyle}>Never</option>
                  <option value="10m" style={darkOptionStyle}>10m</option>
                  <option value="1h" style={darkOptionStyle}>1h</option>
                  <option value="1d" style={darkOptionStyle}>1d</option>
                  <option value="7d" style={darkOptionStyle}>7d</option>
                  <option value="custom" style={darkOptionStyle}>Custom</option>
                </select>
              </div>

              {String(solanaExpiryPreset || "never").toLowerCase().trim() === "custom" && (
                <div style={safePill}>
                  <span>Custom</span>
                  <input
                    style={{ ...safeInput, width: 190 }}
                    type="datetime-local"
                    value={solanaExpiryCustom}
                    onChange={(e) => setSolanaExpiryCustom(e.target.value)}
                  />
                </div>
              )}
            </>
          ) : (
            <>
              <div style={safePill}>
                <span>TIF</span>
                <select style={darkSelectStyle} value={tif} onChange={(e) => setTif(e.target.value)}>
                  <option value="gtc" style={darkOptionStyle}>GTC</option>
                  <option value="ioc" style={darkOptionStyle}>IOC</option>
                  <option value="fok" style={darkOptionStyle}>FOK</option>
                </select>
              </div>

              <label style={safePill}>
                <input type="checkbox" checked={postOnly} onChange={(e) => setPostOnly(e.target.checked)} />
                <span>Post-only</span>
              </label>

              <div style={safePill}>
                <span>Client OID</span>
                <input
                  style={{ ...safeInput, width: 140 }}
                  value={clientOid}
                  placeholder="optional"
                  onChange={(e) => setClientOid(e.target.value)}
                />
              </div>
            </>
          )}
        </div>

        <div style={{ marginTop: 6, ...safeMuted, fontSize: 12, lineHeight: 1.15 }}>
          Type: <b>{isSolanaJupiterVenue ? (solanaOrderMode === "limit" ? "Limit" : "Swap") : "Limit"}</b>
          {isSolanaLimitMode ? <> • Expiry: <b>{hideTableData ? "••••" : solanaExpiryLabel}</b></> : null}
          {" "}• Est. Total ({totalLabel}): <b>{notional === null ? "—" : fmtNum ? fmtNum(notional) : String(notional)}</b>
        </div>

        <div style={{ marginTop: 8, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <button
            style={{
              ...safeButton,
              ...(submitting || !canSubmit ? safeButtonDisabled : {}),
              padding: "9px 12px",
              fontWeight: 900,
            }}
            disabled={submitting || !canSubmit}
            onClick={openConfirm}
            title={
              !canSubmitBase
                ? (isSolanaLimitMode ? "Fill symbol, qty, and limit price" : isSolanaDexVenue ? "Fill symbol and order amount" : "Fill symbol, qty, and limit price")
                : preTrade?.block
                  ? "Blocked by pre-trade checks"
                  : "Review and confirm order"
            }
          >
            {submitting
              ? "Submitting…"
              : isSolanaLimitMode
                ? side === "buy" ? "Place Buy Limit" : "Place Sell Limit"
                : isSolanaDexVenue
                  ? side === "buy" ? "Swap Buy" : "Swap Sell"
                  : side === "buy" ? "Place Buy Limit" : "Place Sell Limit"}
          </button>

          <span style={{ ...safeMuted, fontSize: 11, lineHeight: 1.1 }}>
            Endpoint: <code>{solanaSubmitEndpointLabel}</code>
          </span>

          {hasLastSubmitResult && (
            <button
              type="button"
              style={{ ...safeButton, padding: "7px 10px", opacity: 0.95 }}
              onClick={() =>
                openSubmitResultModal(
                  submitResultKind,
                  submitResultPayload,
                  submitResultKind === "error" ? "Order Submit Failed" : "Order Submitted"
                )
              }
              title="View the last submit result"
            >
              View last result
            </button>
          )}
        </div>

        </div>

        {(forceTileMode || !inlineMode) && (
          <div
            onMouseDown={onResizeMouseDown}
            title={locked ? "Locked" : "Resize from top-left"}
            style={{
              position: "absolute",
              left: 6,
              top: 6,
              width: 18,
              height: 18,
              borderRadius: 6,
              border: "1px solid #2a2a2a",
              background: "#151515",
              cursor: locked ? "default" : "nwse-resize",
              zIndex: 5,
              opacity: locked ? 0.4 : 1,
            }}
          />
        )}

        {/* Confirm submit modal (existing) */}
        {showConfirm && (
          <div
            style={{
              position: "fixed",
              inset: 0,
              background: "rgba(0,0,0,0.55)",
              zIndex: 9999,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 16,
            }}
            onMouseDown={() => setShowConfirm(false)}
            role="dialog"
            aria-modal="true"
          >
            <div
              style={{
                width: "min(560px, 94vw)",
                borderRadius: 14,
                border: `1px solid ${sideAccent}`,
                background: "#101010",
                boxShadow: "0 12px 40px rgba(0,0,0,0.5)",
                padding: 14,
              }}
              onMouseDown={(e) => e.stopPropagation()}
            >
              <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 10 }}>
                <div style={{ fontSize: 14, fontWeight: 900 }}>
                  Confirm {side === "buy" ? "BUY" : "SELL"} {isSolanaLimitMode ? "Jupiter Limit Order" : isSolanaDexVenue ? "Swap" : "Limit Order"}
                </div>
                <button
                  type="button"
                  onClick={() => setShowConfirm(false)}
                  style={{ ...safeButton, padding: "6px 10px", opacity: 0.9 }}
                >
                  Close
                </button>
              </div>

              {preTrade?.status === "fail" && (
                <div
                  style={{
                    marginTop: 10,
                    borderRadius: 10,
                    padding: "8px 10px",
                    border: "1px solid #4a1f1f",
                    background: "#160b0b",
                    color: "#ffd2d2",
                    fontSize: 11,
                    lineHeight: 1.2,
                  }}
                >
                  This order is blocked by pre-trade checks. Fix the Qty/Limit to match venue increments/minimums.
                </div>
              )}

              <div style={{ marginTop: 10, borderTop: "1px solid #2a2a2a", paddingTop: 10 }}>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "160px 1fr",
                    rowGap: 6,
                    columnGap: 10,
                    fontSize: 12,
                  }}
                >
                  {confirmLines.map((x) => (
                    <div key={x.k} style={{ display: "contents" }}>
                      <div style={{ color: "#a9a9a9" }}>{x.k}</div>
                      <div style={{ color: "#eaeaea", fontWeight: 700 }}>{x.v}</div>
                    </div>
                  ))}
                </div>
              </div>

              <div style={{ marginTop: 12, display: "flex", gap: 10, justifyContent: "flex-end", flexWrap: "wrap" }}>
                <button
                  type="button"
                  onClick={() => setShowConfirm(false)}
                  style={{ ...safeButton, padding: "8px 12px", opacity: 0.95 }}
                >
                  Cancel
                </button>

                <button
                  type="button"
                  onClick={confirmAndSubmit}
                  disabled={submitting || !canSubmit}
                  style={{
                    ...safeButton,
                    ...(submitting || !canSubmit ? safeButtonDisabled : {}),
                    padding: "8px 12px",
                    fontWeight: 900,
                    boxShadow: `0 0 0 1px ${sideAccent} inset`,
                  }}
                >
                  {submitting ? "Submitting…" : "Confirm & Submit"}
                </button>
              </div>

              <div style={{ marginTop: 10, fontSize: 11, color: "#a9a9a9", lineHeight: 1.25 }}>
                Confirm submits immediately via{" "}
                <code>{solanaSubmitEndpointLabel}</code>.
                {" "}Cancel returns you to the form without submitting.
              </div>
            </div>
          </div>
        )}

        {/* NEW: submission result modal */}
        {showSubmitResult && (
          <div
            style={{
              position: "fixed",
              inset: 0,
              background: "rgba(0,0,0,0.55)",
              zIndex: 10000,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 16,
            }}
            onMouseDown={() => setShowSubmitResult(false)}
            role="dialog"
            aria-modal="true"
          >
            <div
              style={{
                width: "min(720px, 96vw)",
                maxHeight: "min(78vh, 720px)",
                overflow: "hidden",
                borderRadius: 14,
                border: `1px solid ${submitResultKind === "error" ? "#7a2b2b" : "#1f6f3a"}`,
                background: "#101010",
                boxShadow: "0 12px 40px rgba(0,0,0,0.5)",
                padding: 14,
                display: "flex",
                flexDirection: "column",
                gap: 10,
              }}
              onMouseDown={(e) => e.stopPropagation()}
            >
              <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 10 }}>
                <div style={{ fontSize: 14, fontWeight: 900 }}>{submitResultTitle || "Order Submit Result"}</div>
                <button
                  type="button"
                  onClick={() => setShowSubmitResult(false)}
                  style={{ ...safeButton, padding: "6px 10px", opacity: 0.9 }}
                >
                  Close
                </button>
              </div>

              <div
                style={{
                  display: "flex",
                  gap: 10,
                  justifyContent: "space-between",
                  alignItems: "center",
                  flexWrap: "wrap",
                }}
              >
                <div style={{ fontSize: 12, color: submitResultKind === "error" ? "#ffd2d2" : "#cdeccd" }}>
                  {submitResultKind === "error" ? "Status: ERROR" : "Status: OK"}
                </div>

                <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                  <button
                    type="button"
                    onClick={() => {
                      setSubmitResultKind(submitResultKind || null);
                      setSubmitResultPayload(submitResultPayload);
                      copySubmitResultToClipboard();
                    }}
                    style={{ ...safeButton, padding: "7px 10px", opacity: 0.95 }}
                    title="Copy the result text to clipboard"
                    disabled={!HAS_WINDOW || !navigator?.clipboard?.writeText}
                  >
                    Copy
                  </button>

                  <button
                    type="button"
                    onClick={() => setShowSubmitResult(false)}
                    style={{ ...safeButton, padding: "7px 10px", opacity: 0.95 }}
                  >
                    OK
                  </button>
                </div>
              </div>

              <div
                style={{
                  borderTop: "1px solid #2a2a2a",
                  paddingTop: 10,
                  overflow: "auto",
                  flex: 1,
                }}
              >
                <pre
                  style={{
                    margin: 0,
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    fontSize: 11,
                    lineHeight: 1.2,
                    color: submitResultKind === "error" ? "#ffd2d2" : "#cdeccd",
                    background: submitResultKind === "error" ? "#160b0b" : "#0f1a0f",
                    border: submitResultKind === "error" ? "1px solid #4a1f1f" : "1px solid #203a20",
                    borderRadius: 12,
                    padding: 10,
                  }}
                >
                  {submitResultText || (hideTableData ? "Result hidden." : "—")}
                </pre>

                {!hideTableData && submitOk && submitError && (
                  <div style={{ marginTop: 8, ...safeMuted, fontSize: 11 }}>
                    Note: both submitOk and submitError are set. This should not happen; if it does, it indicates a UI state race.
                  </div>
                )}
              </div>

              <div style={{ fontSize: 11, color: "#a9a9a9", lineHeight: 1.25 }}>
                This modal replaces the inline JSON printout previously shown below the Order Ticket.
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}