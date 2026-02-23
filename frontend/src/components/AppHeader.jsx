// frontend/src/components/AppHeader.jsx
import { useEffect, useMemo, useRef, useState } from "react";
import ArbChip from "../ArbChip";
import TopGainersWindow from "../features/scanners/TopGainersWindow";
import uttBanner from "../assets/utt-banner.jpg";
import { sharedFetchJSON } from "../lib/sharedFetch";

/**
 * THEME SYNC (exact):
 * TerminalTablesWidget persists:
 *   - utt_tables_theme_v1 (JSON string themeKey)
 *   - utt_tables_theme_custom_v1 (JSON object customTheme)
 *
 * AppHeader reads those keys directly and applies:
 *   - global shell vars: --utt-surface-*, --utt-border-*, --utt-text, etc.
 *   - header vars:       --utt-hdr-*
 *
 * This makes AppHeader + TradingViewChartWidget match the Tables theme precisely.
 */

const LS_THEME_KEY = "utt_tables_theme_v1";
const LS_THEME_CUSTOM_KEY = "utt_tables_theme_custom_v1";

// Header banner (user-uploaded)
const LS_BANNER_KEY = "utt_header_banner_v1";
const BANNER_MAX_BYTES = 2 * 1024 * 1024; // 2MB
const BANNER_RECOMMENDED_W = 1920;
const BANNER_RECOMMENDED_H = 200;

// Donate (read-only config; only "hide addresses" is user-local)
const LS_DONATE_HIDE_ADDRS_KEY = "utt_donate_hide_addrs_v1";

// IMPORTANT: This is the immutable donation config for your official build.
// Users cannot edit this in-app (no localStorage for addresses).
// Forks can change it at build-time (unavoidable for open-source).
const DONATE_CONFIG = Object.freeze({
  title: "Support UTT",
  note: "If you find Unified Trading Terminal useful, donations help keep development moving.",
  // TODO: Replace with your PayPal URL (e.g., https://paypal.me/yourname or a hosted donate link)
  paypalUrl: "",
  coins: [
    // TODO: Replace addresses with your real addresses
    { key: "btc", label: "Bitcoin (BTC)", address: "" },
    { key: "eth", label: "Ethereum (ETH)", address: "" },
    { key: "doge", label: "Dogecoin (DOGE)", address: "" },
    { key: "sol", label: "Solana (SOL)", address: "" },
    { key: "ltc", label: "Litecoin (LTC)", address: "" },
    { key: "dot", label: "Polkadot (DOT)", address: "" },
    { key: "dash", label: "Dash (DASH)", address: "" },
  ],
});

// Copied from TerminalTablesWidget.jsx (palette table + helpers) to guarantee exact match.
const PALETTES = {
  geminiDark: {
    label: "Graphite",
    widgetBg: "#0f1114",
    widgetBg2: "#141922",
    panelBg: "#0d1016",
    border: "rgba(255,255,255,0.12)",
    border2: "rgba(255,255,255,0.08)",
    text: "#e8eef8",
    muted: "rgba(232,238,248,0.62)",
    link: "#9ad",
    warn: "#f7b955",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.40,
  },
  oled: {
    label: "OLED",
    widgetBg: "#000000",
    widgetBg2: "#0a0a0a",
    panelBg: "#000000",
    border: "rgba(255,255,255,0.10)",
    border2: "rgba(255,255,255,0.06)",
    text: "#e9eef7",
    muted: "rgba(233,238,247,0.58)",
    link: "#9ad",
    warn: "#f7b955",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.55,
  },
  midnight: {
    label: "Midnight",
    widgetBg: "#070a12",
    widgetBg2: "#0b1020",
    panelBg: "#050812",
    border: "rgba(170,200,255,0.16)",
    border2: "rgba(170,200,255,0.10)",
    text: "#e7efff",
    muted: "rgba(231,239,255,0.60)",
    link: "#7fb2ff",
    warn: "#f7b955",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.45,
  },
  dusk: {
    label: "Dusk",
    widgetBg: "#120a12",
    widgetBg2: "#1a1022",
    panelBg: "#0d0811",
    border: "rgba(255,180,230,0.16)",
    border2: "rgba(255,180,230,0.10)",
    text: "#ffe9f7",
    muted: "rgba(255,233,247,0.62)",
    link: "#ffb4e6",
    warn: "#f7b955",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.45,
  },
  slate: {
    label: "Slate",
    widgetBg: "#0d1117",
    widgetBg2: "#121a24",
    panelBg: "#0b0f14",
    border: "rgba(255,255,255,0.14)",
    border2: "rgba(255,255,255,0.08)",
    text: "#e6edf3",
    muted: "rgba(230,237,243,0.62)",
    link: "#86b6ff",
    warn: "#f7b955",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.40,
  },
  steel: {
    label: "Steel",
    widgetBg: "#0c1016",
    widgetBg2: "#121a24",
    panelBg: "#0a0d12",
    border: "rgba(220,235,255,0.16)",
    border2: "rgba(220,235,255,0.10)",
    text: "#eaf2ff",
    muted: "rgba(234,242,255,0.60)",
    link: "#a7c7ff",
    warn: "#f7b955",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.42,
  },
  highContrast: {
    label: "High Contrast",
    widgetBg: "#050505",
    widgetBg2: "#0b0b0b",
    panelBg: "#000000",
    border: "rgba(255,255,255,0.20)",
    border2: "rgba(255,255,255,0.12)",
    text: "#ffffff",
    muted: "rgba(255,255,255,0.70)",
    link: "#9ad",
    warn: "#ffd15a",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.60,
  },
  custom: {
    label: "Custom",
    widgetBg: "#0f1114",
    widgetBg2: "#141922",
    panelBg: "#0d1016",
    border: "rgba(255,255,255,0.12)",
    border2: "rgba(255,255,255,0.08)",
    text: "#e8eef8",
    muted: "rgba(232,238,248,0.62)",
    link: "#9ad",
    warn: "#f7b955",
    danger: "#ff6b6b",
    good: "#55e38c",
    shadowColor: "#000000",
    shadowAlpha: 0.35,
  },
};

function isHexColor(s) {
  return /^#[0-9a-fA-F]{6}$/.test(String(s || "").trim());
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function hexToRgb(hex) {
  const h = String(hex || "").trim().replace("#", "");
  if (h.length !== 6) return { r: 0, g: 0, b: 0 };
  const r = parseInt(h.slice(0, 2), 16);
  const g = parseInt(h.slice(2, 4), 16);
  const b = parseInt(h.slice(4, 6), 16);
  return {
    r: Number.isFinite(r) ? r : 0,
    g: Number.isFinite(g) ? g : 0,
    b: Number.isFinite(b) ? b : 0,
  };
}

function buildShadowFrom(pal, customTheme) {
  const base = pal || PALETTES.geminiDark;
  const colorHex = isHexColor(customTheme?.shadowColor) ? customTheme.shadowColor : base.shadowColor || "#000000";
  const aRaw = Number(customTheme?.shadowAlpha);
  const a = Number.isFinite(aRaw) ? clamp(aRaw, 0, 1) : Number(base.shadowAlpha) || 0.35;
  const { r, g, b } = hexToRgb(colorHex);
  return `0 10px 24px rgba(${r},${g},${b},${a})`;
}

function readThemeFromStorage() {
  try {
    const raw = localStorage.getItem(LS_THEME_KEY);
    if (!raw) return "geminiDark";
    const v = JSON.parse(raw);
    return typeof v === "string" && v ? v : "geminiDark";
  } catch {
    return "geminiDark";
  }
}

function readCustomThemeFromStorage() {
  try {
    const raw = localStorage.getItem(LS_THEME_CUSTOM_KEY);
    if (!raw) return {};
    const v = JSON.parse(raw);
    return v && typeof v === "object" ? v : {};
  } catch {
    return {};
  }
}

function resolvePalette(themeKey, customTheme) {
  const key = String(themeKey || "").trim();
  const base = PALETTES[key] || PALETTES.geminiDark;

  if (key !== "custom") {
    return {
      ...base,
      shadow: buildShadowFrom(base, {}),
    };
  }

  const merged = { ...base };
  const keys = ["widgetBg", "widgetBg2", "panelBg", "border", "border2", "text", "muted", "link", "warn", "danger", "good"];
  for (const k of keys) {
    const v = customTheme?.[k];
    if (typeof v === "string" && v.trim()) merged[k] = v.trim();
  }

  return {
    ...merged,
    shadowColor: isHexColor(customTheme?.shadowColor) ? customTheme.shadowColor : merged.shadowColor,
    shadowAlpha: Number.isFinite(Number(customTheme?.shadowAlpha)) ? clamp(Number(customTheme.shadowAlpha), 0, 1) : merged.shadowAlpha,
    shadow: buildShadowFrom(merged, customTheme),
  };
}

function normalizeVenue(v) {
  return String(v || "").trim().toLowerCase();
}

function normalizeVenueFilterValue(v) {
  const s = normalizeVenue(v);
  if (!s) return "";
  if (s === "all" || s === "all venues" || s === "all enabled venues") return "";
  return s;
}

function readBannerFromStorage() {
  try {
    const raw = localStorage.getItem(LS_BANNER_KEY);
    if (!raw) return null;
    const v = JSON.parse(raw);
    if (!v || typeof v !== "object") return null;
    const dataUrl = String(v.dataUrl || "").trim();
    if (!dataUrl.startsWith("data:image/")) return null;
    return {
      dataUrl,
      name: typeof v.name === "string" ? v.name : "",
      type: typeof v.type === "string" ? v.type : "",
      sizeBytes: Number.isFinite(Number(v.sizeBytes)) ? Number(v.sizeBytes) : null,
      width: Number.isFinite(Number(v.width)) ? Number(v.width) : null,
      height: Number.isFinite(Number(v.height)) ? Number(v.height) : null,
      at: typeof v.at === "string" ? v.at : "",
    };
  } catch {
    return null;
  }
}

function readDonateHideAddrsFromStorage() {
  try {
    const raw = localStorage.getItem(LS_DONATE_HIDE_ADDRS_KEY);
    if (!raw) return false;
    return String(raw) === "1";
  } catch {
    return false;
  }
}

function writeDonateHideAddrsToStorage(v) {
  try {
    localStorage.setItem(LS_DONATE_HIDE_ADDRS_KEY, v ? "1" : "0");
  } catch {
    // ignore
  }
}

function CameraIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" style={{ display: "block" }}>
      <path
        d="M9 4.5h6l1.2 2H20a2.5 2.5 0 0 1 2.5 2.5v9A2.5 2.5 0 0 1 20 20.5H4A2.5 2.5 0 0 1 1.5 18V9A2.5 2.5 0 0 1 4 6.5h3.8L9 4.5Z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinejoin="round"
      />
      <path d="M12 17a4 4 0 1 0 0-8 4 4 0 0 0 0 8Z" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
    </svg>
  );
}

function UploadIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" style={{ display: "block" }}>
      <path d="M12 16V6" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M8.5 9.5 12 6l3.5 3.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M4.5 16.5v2A2 2 0 0 0 6.5 20.5h11A2 2 0 0 0 19.5 18.5v-2" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function DonateIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" style={{ display: "block" }}>
      <path
        d="M12 21s-7-4.6-9.5-8.7C.9 9.6 2.1 6.9 4.6 6c2-.7 3.9.1 5 1.7 1.1-1.6 3-2.4 5-1.7 2.5.9 3.7 3.6 2.1 6.3C19 16.4 12 21 12 21Z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function ToolChip({
  title,
  subLabel,
  isOpen,
  onClick,
  showStatus = true,
  showSubLabel = true,
}) {
  const base = {
    display: "inline-flex",
    flexDirection: "column",
    alignItems: "flex-start",
    justifyContent: "center",
    gap: 2,
    padding: "8px 12px",
    borderRadius: 999,
    border: "1px solid var(--utt-hdr-pill-border, rgba(255,255,255,0.12))",
    background: "var(--utt-hdr-pill-bg, rgba(255,255,255,0.04))",
    color: "inherit",
    cursor: "pointer",
    userSelect: "none",
    minWidth: 140,
  };

  const open = {
    ...base,
    border: "1px solid color-mix(in srgb, var(--utt-hdr-link, #9ad) 55%, transparent)",
    background: "color-mix(in srgb, var(--utt-hdr-link, #9ad) 12%, var(--utt-hdr-pill-bg, rgba(255,255,255,0.04)))",
    boxShadow: "0 0 0 1px color-mix(in srgb, var(--utt-hdr-link, #9ad) 22%, transparent) inset",
  };

	  return (
	    <button type="button" onClick={onClick} style={isOpen ? open : base} title={`${title} window`}>
	      <div style={{ display: "flex", alignItems: "baseline", gap: 8, lineHeight: 1.1 }}>
	        <span style={{ fontWeight: 800, fontSize: 13 }}>{title}</span>
	        {showStatus ? (
	          <span style={{ fontSize: 11, opacity: 0.75 }}>{isOpen ? "Open" : "Closed"}</span>
	        ) : null}
	      </div>
	      {showSubLabel ? <div style={{ fontSize: 11, opacity: 0.75 }}>{subLabel || "—"}</div> : null}
	    </button>
	  );
}

// ---------------------------
// Top Gainers background poller (always mounted in header)
// ---------------------------
const TG_LS_PREFIX = "utt:scanner:top_gainers";
const tgLsKey = (suffix) => `${TG_LS_PREFIX}:${suffix}`;
const TG_CACHE_KEY = tgLsKey("chip_cache_v1");

function tgReadBool(key, fallback) {
  try {
    const v = localStorage.getItem(key);
    if (v === null || v === undefined) return fallback;
    const s = String(v).trim().toLowerCase();
    if (s === "1" || s === "true" || s === "yes" || s === "on") return true;
    if (s === "0" || s === "false" || s === "no" || s === "off") return false;
    return fallback;
  } catch {
    return fallback;
  }
}

function tgReadInt(key, fallback) {
  try {
    const v = localStorage.getItem(key);
    const n = Number(v);
    if (!Number.isFinite(n)) return fallback;
    return Math.floor(n);
  } catch {
    return fallback;
  }
}

function tgClampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

function tgTrimApiBase(apiBase) {
  const s = String(apiBase || "").trim();
  return s.replace(/\/+$/, "");
}

function tgToNum(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function tgNormalizeVenue(v) {
  return String(v || "").trim().toLowerCase();
}

function tgNormalizeVenueFilterValue(v) {
  const s = tgNormalizeVenue(v);
  if (!s) return "";
  if (s === "all" || s === "all venues" || s === "all enabled venues") return "";
  return s;
}

function tgPickNum(r, keys) {
  if (!r || typeof r !== "object") return null;
  for (const k of keys) {
    if (r[k] !== undefined && r[k] !== null) {
      const n = tgToNum(r[k]);
      if (n !== null) return n;
    }
  }
  return null;
}

function tgPickStr(r, keys, fallback = "") {
  if (!r || typeof r !== "object") return fallback;
  for (const k of keys) {
    const v = r[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") return String(v);
  }
  return fallback;
}

function tgSymbolFromAsset(asset) {
  const a = String(asset || "").trim().toUpperCase();
  if (!a) return "";
  return `${a}-USD`;
}

function tgCanonicalizeSymbol(symRaw) {
  const s0 = String(symRaw || "").trim();
  if (!s0) return "";
  const up = s0.toUpperCase();

  let s = up.replace(/\s+/g, "").replace(/[\/_]/g, "-");

  if (s.includes("-")) {
    const parts = s.split("-").filter(Boolean);
    if (parts.length >= 2) return `${parts[0]}-${parts[1]}`;
    return s;
  }

  if (s.endsWith("USD") && s.length > 3) {
    const base = s.slice(0, -3);
    return `${base}-USD`;
  }

  return s;
}

const TG_NUM_KEYS = {
  change_1d: [
    "change_1d",
    "change1d",
    "1d_change",
    "1dChange",
    "pct_1d",
    "pct1d",
    "pct_change_1d",
    "pctChange1d",
    "percent_change_1d",
    "percentChange1d",
    "change_24h",
    "change24h",
    "pct_change_24h",
    "pctChange24h",
    "percent_change_24h",
    "percentChange24h",
    "price_change_24h",
    "priceChange24h",
  ],
};

function tgReadChipCache() {
  try {
    const raw = localStorage.getItem(TG_CACHE_KEY);
    if (!raw) return null;
    const v = JSON.parse(raw);
    if (!v || typeof v !== "object") return null;
    return {
      top: v.top && typeof v.top === "object" ? v.top : null,
      at: typeof v.at === "string" ? v.at : null,
    };
  } catch {
    return null;
  }
}

function tgWriteChipCache(payload) {
  try {
    localStorage.setItem(TG_CACHE_KEY, JSON.stringify(payload));
  } catch {
    // ignore
  }
}

async function tgFetchBalancesLatestOne(base, venueOpt, signal) {
  const params = new URLSearchParams();
  params.set("with_prices", "true");
  if (venueOpt) params.set("venue", venueOpt);
  const url = `${base}/api/balances/latest?${params.toString()}`;
  const json = await sharedFetchJSON(url, { signal, ttlMs: 1200 });
  return Array.isArray(json?.items) ? json.items : [];
}

async function tgFetchScannerTopGainers(base, venuesArr, signal) {
  const p = new URLSearchParams();
  p.set("limit", "250");
  const vv = (Array.isArray(venuesArr) ? venuesArr : []).map(tgNormalizeVenue).filter(Boolean);
  if (vv.length) for (const v of vv) p.append("venues", v);
  const url = `${base}/api/scanners/top_gainers?${p.toString()}`;
  const json = await sharedFetchJSON(url, { signal, ttlMs: 1200 });
  return Array.isArray(json?.items) ? json.items : [];
}

function buildHeldSymbolsFromBalances(items, allowedVenuesSet) {
  const held = new Set();
  const arr = Array.isArray(items) ? items : [];
  for (const b of arr) {
    if (!b || typeof b !== "object") continue;
    const v = tgNormalizeVenue(b.venue);
    if (allowedVenuesSet && allowedVenuesSet.size > 0) {
      if (!v || !allowedVenuesSet.has(v)) continue;
    }
    const asset = String(b.asset || "").trim().toUpperCase();
    if (!asset || asset === "USD") continue;
    const qty = tgToNum(b.total) ?? 0;
    if (!qty || Math.abs(qty) <= 0) continue;
    const sym = tgSymbolFromAsset(asset);
    if (!sym) continue;
    held.add(sym);
  }
  return Array.from(held).sort();
}

export default function AppHeader({
  headerRef,
  headerStyles,
  styles,

  API_BASE,
  loadingSupportedVenues,
  venuesLoaded,

  // venue picker
  venue,
  setVenue,
  supportedVenues,
  ALL_VENUES_VALUE,
  labelVenueOption,

// DEX account context (only used when dexMode=true)
dexMode,
dexVenue,
dexAccounts,
dexAccount,
setDexAccount,
addDexAccount,

  // safety
  dryRunKnown,
  isDryRun,
  armedKnown,
  isArmed,
  loadingArm,
  armDisabled,
  disarmDisabled,
  doSetArmed,
  loadArmStatus,
  btnHeader,

  // background refresh
  pollEnabled,
  setPollEnabled,
  pollSeconds,
  setPollSeconds,

  // market picker
  marketInput,
  setMarketInput,
  applyMarketSymbol,
  applyMarketToTab,
  setApplyMarketToTab,

  // global masking
  hideTableDataGlobal,
  setHideTableDataGlobal,

  // widget visibility
  visible,
  setVisible,
  onResetWidgets,

  // arb chip
  obSymbol,
  arbVenues,
  fmtPrice,
  hideVenueNames,
  fetchArbSnapshot,

  // tool windows (Arb + scanners)
  toolWindows,
  toggleToolWindow,

  // screenshot capture
  shotBusy,
  captureFullUiScreenshot,

  // totals + error
  headerAllVenuesTotalText,
  error,
}) {
  const [themeKey, setThemeKey] = useState(() => {
    if (typeof window === "undefined") return "geminiDark";
    return readThemeFromStorage();
  });
  const [customTheme, setCustomTheme] = useState(() => {
    if (typeof window === "undefined") return {};
    return readCustomThemeFromStorage();
  });

  const [banner, setBanner] = useState(() => {
    if (typeof window === "undefined") return null;
    return readBannerFromStorage();
  });
  const [bannerMsg, setBannerMsg] = useState("");
  const bannerInputRef = useRef(null);

  // FIX: banner height expands to contain overlay stack (upload row + optional msg + donate row)
  const bannerDisplayHeight = bannerMsg ? 128 : 104;

  // Donate state (config is read-only; only hide-toggle is persisted)
  const donateCfg = DONATE_CONFIG;
  const [donateOpen, setDonateOpen] = useState(false);
  const [donateHideAddrs, setDonateHideAddrs] = useState(() => {
    if (typeof window === "undefined") return false;
    return readDonateHideAddrsFromStorage();
  });
  const [donateMsg, setDonateMsg] = useState("");

  const donateBtnRef = useRef(null);
  const donatePopRef = useRef(null);

  useEffect(() => {
    if (typeof window === "undefined") return;

    let lastKey = String(themeKey || "");
    let lastCustom = JSON.stringify(customTheme || {});

    const sync = () => {
      try {
        const k = readThemeFromStorage();
        const c = readCustomThemeFromStorage();
        const cStr = JSON.stringify(c || {});
        if (k !== lastKey) {
          lastKey = k;
          setThemeKey(k);
        }
        if (cStr !== lastCustom) {
          lastCustom = cStr;
          setCustomTheme(c);
        }
      } catch {
        // ignore
      }
    };

    const onStorage = (e) => {
      if (!e) return;
      if (e.key === LS_THEME_KEY || e.key === LS_THEME_CUSTOM_KEY) sync();
    };

    const t = setInterval(sync, 700);
    window.addEventListener("storage", onStorage);
    return () => {
      clearInterval(t);
      window.removeEventListener("storage", onStorage);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const onStorage = (e) => {
      if (!e) return;
      if (e.key !== LS_BANNER_KEY) return;
      setBanner(readBannerFromStorage());
    };

    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  // Keep donate "hide addresses" in sync across tabs/windows
  useEffect(() => {
    if (typeof window === "undefined") return;

    const onStorage = (e) => {
      if (!e) return;
      if (e.key === LS_DONATE_HIDE_ADDRS_KEY) {
        setDonateHideAddrs(readDonateHideAddrsFromStorage());
      }
    };

    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const pal = useMemo(() => resolvePalette(themeKey, customTheme), [themeKey, customTheme]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const root = document.documentElement;

    root.style.setProperty("--utt-page-bg", pal.widgetBg);
    root.style.setProperty("--utt-page-fg", pal.text);
    root.style.setProperty("--utt-surface-0", pal.widgetBg);
    root.style.setProperty("--utt-surface-1", pal.widgetBg2);
    root.style.setProperty("--utt-surface-2", pal.panelBg);
    root.style.setProperty("--utt-border-1", pal.border);
    root.style.setProperty("--utt-border-2", pal.border2);
    root.style.setProperty("--utt-row-border", pal.border2);
    root.style.setProperty("--utt-control-bg", pal.panelBg);
    root.style.setProperty("--utt-button-bg", pal.widgetBg2);
    root.style.setProperty("--utt-text", pal.text);
    root.style.setProperty("--utt-muted", pal.muted);
    root.style.setProperty("--utt-link", pal.link);
    root.style.setProperty("--utt-warn", pal.warn);
    root.style.setProperty("--utt-danger", pal.danger);
    root.style.setProperty("--utt-good", pal.good);
    root.style.setProperty("--utt-shadow", pal.shadow);

    root.style.setProperty("--utt-hdr-bg", pal.widgetBg);
    root.style.setProperty("--utt-hdr-fg", pal.text);
    root.style.setProperty("--utt-hdr-muted", pal.muted);
    root.style.setProperty("--utt-hdr-border", pal.border);
    root.style.setProperty("--utt-hdr-pill-bg", pal.widgetBg2);
    root.style.setProperty("--utt-hdr-pill-border", pal.border);
    root.style.setProperty("--utt-hdr-ctl-bg", pal.panelBg);
    root.style.setProperty("--utt-hdr-ctl-border", pal.border2);
    root.style.setProperty("--utt-hdr-btn-bg", pal.widgetBg2);
    root.style.setProperty("--utt-hdr-btn-border", pal.border);
    root.style.setProperty("--utt-hdr-link", pal.link);
    root.style.setProperty("--utt-hdr-error", pal.danger);
    root.style.setProperty("--utt-hdr-shadow", pal.shadow);
  }, [pal]);

  const screenshotLinkStyle = {
    background: "transparent",
    border: "none",
    color: "var(--utt-hdr-link, #9ad)",
    padding: 0,
    cursor: shotBusy ? "not-allowed" : "pointer",
    textDecoration: "underline",
    fontSize: 12,
    opacity: shotBusy ? 0.7 : 1,
    whiteSpace: "nowrap",
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
  };

  const toolTabsRowStyle = {
    marginTop: 10,
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
    gap: 10,
    flexWrap: "wrap",
  };

  // UPDATED: allow a second row (Donate) under the banner upload controls.
  const bannerCtlWrapStyle = {
    position: "absolute",
    right: 10,
    top: 10,
    display: "flex",
    flexDirection: "column",
    alignItems: "flex-end",
    gap: 8,
    padding: "6px 10px",
    borderRadius: 10,
    background: "color-mix(in srgb, var(--utt-hdr-bg, #0f1114) 72%, transparent)",
    border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
    backdropFilter: "blur(6px)",
  };

  const bannerCtlRowStyle = {
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
    gap: 10,
    flexWrap: "wrap",
  };

  const bannerBtnStyle = {
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    padding: "6px 10px",
    borderRadius: 10,
    background: "var(--utt-hdr-btn-bg, rgba(255,255,255,0.04))",
    border: "1px solid var(--utt-hdr-btn-border, rgba(255,255,255,0.12))",
    color: "var(--utt-hdr-fg, #e8eef8)",
    cursor: "pointer",
    fontSize: 12,
    fontWeight: 700,
    userSelect: "none",
    whiteSpace: "nowrap",
  };

  const bannerBtnSecondaryStyle = {
    ...bannerBtnStyle,
    fontWeight: 700,
    opacity: banner ? 1 : 0.55,
    cursor: banner ? "pointer" : "not-allowed",
  };

  const bannerReqStyle = {
    fontSize: 11,
    opacity: 0.75,
    whiteSpace: "nowrap",
  };

  const bannerWarnStyle = {
    fontSize: 11,
    opacity: 0.85,
    color: "var(--utt-hdr-warn, var(--utt-warn, #f7b955))",
    whiteSpace: "nowrap",
    maxWidth: 520,
    overflow: "hidden",
    textOverflow: "ellipsis",
  };

  const donateBtnStyle = {
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    padding: "6px 10px",
    borderRadius: 10,
    background: "color-mix(in srgb, var(--utt-hdr-link, #9ad) 10%, var(--utt-hdr-btn-bg, rgba(255,255,255,0.04)))",
    border: "1px solid color-mix(in srgb, var(--utt-hdr-link, #9ad) 35%, var(--utt-hdr-btn-border, rgba(255,255,255,0.12)))",
    color: "var(--utt-hdr-fg, #e8eef8)",
    cursor: "pointer",
    fontSize: 12,
    fontWeight: 900,
    userSelect: "none",
    whiteSpace: "nowrap",
    letterSpacing: 0.3,
  };

  // ---------------------------
  // Donate popover: FIXED positioning + viewport clamp (prevents header clipping)
  // ---------------------------
  const DONATE_MARGIN = 10;
  const DONATE_GAP = 10;
  const DONATE_MAX_W = 560;

  const [donatePos, setDonatePos] = useState(null); // { x, y, w }

  const computeDonateWidth = () => {
    const vw = Math.max(320, window.innerWidth || 0);
    const w = Math.min(DONATE_MAX_W, Math.floor(vw * 0.92));
    return Math.max(320, w);
  };

  const clampDonatePos = (x, y, w) => {
    const vw = Math.max(320, window.innerWidth || 0);
    const vh = Math.max(320, window.innerHeight || 0);

    const ww = Math.min(Math.max(320, w || computeDonateWidth()), vw - DONATE_MARGIN * 2);
    const maxX = Math.max(DONATE_MARGIN, vw - ww - DONATE_MARGIN);

    // We can't know panel height precisely; we clamp Y to keep header visible.
    const maxY = Math.max(DONATE_MARGIN, vh - 80);
    const cx = clamp(x, DONATE_MARGIN, maxX);
    const cy = clamp(y, DONATE_MARGIN, maxY);

    return { x: cx, y: cy, w: ww };
  };

  const placeDonateNearButton = () => {
    const btn = donateBtnRef.current;
    if (!btn) return;

    const rect = btn.getBoundingClientRect();
    const w = computeDonateWidth();

    const desiredX = rect.right - w;
    const desiredY = rect.bottom + DONATE_GAP;

    setDonatePos(clampDonatePos(desiredX, desiredY, w));
  };

  useEffect(() => {
    if (!donateOpen) return;

    // On open: place it (fixed) under the button
    placeDonateNearButton();

    // Clamp on resize
    const onResize = () => {
      setDonatePos((p) => {
        const w = computeDonateWidth();
        if (!p) return clampDonatePos(DONATE_MARGIN, 120, w);
        return clampDonatePos(p.x, p.y, Math.min(p.w || w, w));
      });
    };

    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [donateOpen]);

  // FIX: donate popover should be fully opaque + scrollable body
  // Guard against accidental transparent theme values:
  // If a user’s Custom theme sets panel/widget backgrounds to transparent/low-alpha,
  // force the Donate popover to render with a solid, readable surface.
  const pickOpaqueColor = (c) => {
    const raw = String(c || "").trim();
    if (!raw) return null;
    const s = raw.toLowerCase();

    if (s === "transparent") return null;

    // Detect very low-alpha rgba()/hsla() and treat as transparent.
    // This is intentionally conservative; if parsing fails, we keep the value.
    try {
      if (s.startsWith("rgba(") && s.endsWith(")")) {
        const parts = s.slice(5, -1).split(",").map((p) => p.trim());
        const a = Number(parts[3]);
        if (Number.isFinite(a) && a < 0.98) return null;
      }
      if (s.startsWith("hsla(") && s.endsWith(")")) {
        const parts = s.slice(5, -1).split(",").map((p) => p.trim());
        const a = Number(String(parts[3] || "").replace("%", ""));
        if (Number.isFinite(a) && a < 0.98) return null;
      }
    } catch {
      // ignore
    }

    return raw;
  };

  const donateSolidBg = useMemo(() => pickOpaqueColor(pal?.panelBg) || pickOpaqueColor(pal?.widgetBg2) || pickOpaqueColor(pal?.widgetBg) || "#0f1114", [pal]);
  const donateSolidHeaderBg = useMemo(() => pickOpaqueColor(pal?.widgetBg) || pickOpaqueColor(pal?.panelBg) || donateSolidBg, [pal, donateSolidBg]);
  const donateSolidCtlBg = useMemo(() => pickOpaqueColor(pal?.panelBg) || pickOpaqueColor(pal?.widgetBg2) || donateSolidBg, [pal, donateSolidBg]);

  const donatePanelStyle = {
    position: "fixed",
    left: donatePos?.x ?? DONATE_MARGIN,
    top: donatePos?.y ?? 120,
    zIndex: 20000,
    width: donatePos?.w ?? 520,
    maxWidth: "92vw",

    color: "var(--utt-hdr-fg, #e8eef8)",

    // fully opaque
    backgroundColor: donateSolidBg,
    border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
    borderRadius: 14,
    boxShadow: "var(--utt-hdr-shadow, 0 10px 24px rgba(0,0,0,0.35))",
    overflow: "hidden",
    opacity: 1,
  };

  const donatePanelHeaderStyle = {
    padding: "10px 12px",
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 10,
    borderBottom: "1px solid var(--utt-hdr-ctl-border, rgba(255,255,255,0.08))",
    backgroundColor: donateSolidHeaderBg,
    backgroundImage: "linear-gradient(rgba(255,255,255,0.05), rgba(255,255,255,0.02))",
  };

  const donatePanelBodyStyle = {
    padding: 12,
    display: "flex",
    flexDirection: "column",
    gap: 10,

    // scrollability
    maxHeight: "min(62vh, 520px)",
    overflowY: "auto",
    overscrollBehavior: "contain",
  };

  const donateRowStyle = {
    display: "grid",
    gridTemplateColumns: "170px 1fr auto",
    gap: 10,
    alignItems: "center",
  };

  const donateInputStyle = {
    width: "100%",
    background: donateSolidCtlBg,
    border: "1px solid var(--utt-hdr-ctl-border, rgba(255,255,255,0.10))",
    color: "var(--utt-hdr-fg, #e8eef8)",
    borderRadius: 10,
    padding: "8px 10px",
    fontSize: 12,
    outline: "none",
  };

  const donateSmallBtnStyle = {
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    padding: "7px 10px",
    borderRadius: 10,
    background: "var(--utt-hdr-btn-bg, rgba(255,255,255,0.04))",
    border: "1px solid var(--utt-hdr-btn-border, rgba(255,255,255,0.12))",
    color: "var(--utt-hdr-fg, #e8eef8)",
    cursor: "pointer",
    fontSize: 12,
    fontWeight: 800,
    userSelect: "none",
    whiteSpace: "nowrap",
  };

  const donatePrimaryBtnStyle = {
    ...donateSmallBtnStyle,
    border: "1px solid color-mix(in srgb, var(--utt-hdr-link, #9ad) 45%, var(--utt-hdr-btn-border, rgba(255,255,255,0.12)))",
    background: "color-mix(in srgb, var(--utt-hdr-link, #9ad) 12%, var(--utt-hdr-btn-bg, rgba(255,255,255,0.04)))",
  };

  const openBannerPicker = () => {
    setBannerMsg("");
    const el = bannerInputRef.current;
    if (el) el.click();
  };

  const clearBanner = () => {
    try {
      localStorage.removeItem(LS_BANNER_KEY);
    } catch {
      // ignore
    }
    setBanner(null);
    setBannerMsg("");
  };

  const handleBannerPicked = async (file) => {
    if (!file) return;

    const type = String(file.type || "").toLowerCase();
    if (!type.startsWith("image/")) {
      setBannerMsg("Banner upload rejected: file must be an image (JPG/PNG/WebP).");
      return;
    }
    if (Number(file.size || 0) > BANNER_MAX_BYTES) {
      setBannerMsg(`Banner upload rejected: file exceeds ${(BANNER_MAX_BYTES / (1024 * 1024)).toFixed(0)}MB.`);
      return;
    }

    const dataUrl = await new Promise((resolve, reject) => {
      try {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result || ""));
        reader.onerror = () => reject(new Error("File read failed"));
        reader.readAsDataURL(file);
      } catch (e) {
        reject(e);
      }
    }).catch(() => "");

    if (!dataUrl || !String(dataUrl).startsWith("data:image/")) {
      setBannerMsg("Banner upload failed: could not decode the selected image.");
      return;
    }

    const dim = await new Promise((resolve) => {
      try {
        const img = new Image();
        img.onload = () => resolve({ width: img.naturalWidth || null, height: img.naturalHeight || null });
        img.onerror = () => resolve({ width: null, height: null });
        img.src = dataUrl;
      } catch {
        resolve({ width: null, height: null });
      }
    });

    const payload = {
      dataUrl,
      name: String(file.name || ""),
      type: String(file.type || ""),
      sizeBytes: Number(file.size || 0),
      width: dim?.width ?? null,
      height: dim?.height ?? null,
      at: new Date().toISOString(),
    };

    try {
      localStorage.setItem(LS_BANNER_KEY, JSON.stringify(payload));
    } catch {
      // ignore
    }

    setBanner(payload);

    const w = Number(dim?.width);
    const h = Number(dim?.height);
    if (Number.isFinite(w) && Number.isFinite(h) && w > 0 && h > 0) {
      const aspect = w / h;
      if (aspect < 4.5) {
        setBannerMsg(`Banner note: your image is ${w}×${h}. For best results, use a wide banner (recommended ~${BANNER_RECOMMENDED_W}×${BANNER_RECOMMENDED_H}).`);
      } else {
        setBannerMsg("");
      }
    } else {
      setBannerMsg("");
    }
  };

  const isArbTool = (w) => {
    const id = String(w?.id ?? "").trim().toLowerCase();
    const title = String(w?.title ?? "").trim().toLowerCase();
    return id === "arb" || title === "arb";
  };

  const isTopGainersTool = (w) => {
    const id = String(w?.id ?? "").trim().toLowerCase();
    const title = String(w?.title ?? "").trim().toLowerCase();
    return id === "top_gainers" || title === "top gainers" || title === "topgainers";
  };

  // FIX: stabilize enabled venues (sorting prevents dependency churn)
  const enabledVenuesForScanners = useMemo(() => (supportedVenues || []).map((v) => normalizeVenue(v)).filter(Boolean).sort(), [supportedVenues]);

  const tg = useMemo(() => {
    const found = (toolWindows || []).find((w) => isTopGainersTool(w));
    return found || { id: "top_gainers", title: "Top Gainers" };
  }, [toolWindows]);

  const [tgPopoverOpen, setTgPopoverOpen] = useState(false);

  const LS_TG_VENUE = "utt_top_gainers_venue_filter";
  const [tgVenueFilter, setTgVenueFilter] = useState(() => {
    try {
      const raw = (localStorage.getItem(LS_TG_VENUE) || "").trim();
      return normalizeVenueFilterValue(raw);
    } catch {
      return "";
    }
  });

  const setTgVenueFilterSafe = (v) => setTgVenueFilter(normalizeVenueFilterValue(v));

  useEffect(() => {
    try {
      localStorage.setItem(LS_TG_VENUE, String(tgVenueFilter || ""));
    } catch {
      // ignore
    }
  }, [tgVenueFilter]);

  useEffect(() => {
    const vf = normalizeVenueFilterValue(tgVenueFilter);
    if (!vf) return;
    if (!enabledVenuesForScanners.includes(vf)) {
      setTgVenueFilter("");
    }
  }, [enabledVenuesForScanners, tgVenueFilter]);

  // Background-driven chip summary (stays fresh even when window is closed)
  const cachedChip = useMemo(() => (typeof window === "undefined" ? null : tgReadChipCache()), []);
  const [tgTop, setTgTop] = useState(() => cachedChip?.top || null);
  const [tgTopAt, setTgTopAt] = useState(() => cachedChip?.at || null);

  // Keep chip cache in sync across tabs
  useEffect(() => {
    if (typeof window === "undefined") return;

    const onStorage = (e) => {
      if (!e) return;
      if (e.key !== TG_CACHE_KEY) return;
      const v = tgReadChipCache();
      if (v?.top) setTgTop(v.top);
      if (v?.at) setTgTopAt(v.at);
    };

    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const tgPollTimerRef = useRef(null);
  const tgAbortRef = useRef(null);
  const tgInFlightRef = useRef(false);

  const doTopGainersChipRefresh = async ({ reason } = {}) => {
    const base = tgTrimApiBase(API_BASE);
    if (!base) return;

    // Auto enabled is controlled by the window setting (shared)
    const auto = tgReadBool(tgLsKey("autoRefresh"), true);
    if (!auto && reason !== "manual") return;

    if (tgInFlightRef.current) return;
    tgInFlightRef.current = true;

    try {
      tgAbortRef.current?.abort?.();
    } catch {
      // ignore
    }
    const controller = new AbortController();
    tgAbortRef.current = controller;

    try {
      const vf = tgNormalizeVenueFilterValue(tgVenueFilter);
      const venues = vf ? [vf] : enabledVenuesForScanners.slice();
      const allowedSet = new Set(venues.map(tgNormalizeVenue).filter(Boolean));

      // Balances -> held symbols
      let merged = [];
      if (vf) {
        merged = await tgFetchBalancesLatestOne(base, vf, controller.signal);
      } else {
        const vlist = enabledVenuesForScanners.length ? enabledVenuesForScanners.slice() : [];
        if (!vlist.length) {
          merged = await tgFetchBalancesLatestOne(base, "", controller.signal);
        } else {
          const results = await Promise.allSettled(vlist.map((v) => tgFetchBalancesLatestOne(base, v, controller.signal)));
          const ok = [];
          for (const r of results) {
            if (r.status === "fulfilled") ok.push(...(Array.isArray(r.value) ? r.value : []));
          }
          merged = ok;
        }
      }

      const heldSymbols = buildHeldSymbolsFromBalances(merged, allowedSet);
      if (!heldSymbols.length) return;

      // Scanner -> best match among held symbols
      const items = await tgFetchScannerTopGainers(base, venues, controller.signal);

      const bestBySymbol = new Map(); // sym -> { asset, symbol, change_1d }
      for (const it of items) {
        if (!it || typeof it !== "object") continue;

        const symRaw =
          tgPickStr(it, ["symbol", "pair", "market"], "") || tgSymbolFromAsset(tgPickStr(it, ["asset", "base", "ticker"], ""));
        const sym = tgCanonicalizeSymbol(symRaw);
        if (!sym) continue;

        const c1d = tgPickNum(it, TG_NUM_KEYS.change_1d);
        if (c1d === null) continue;

        const prev = bestBySymbol.get(sym);
        if (!prev || (Number.isFinite(Number(c1d)) && Number(c1d) > Number(prev.change_1d))) {
          const assetGuess = tgPickStr(it, ["asset", "base", "ticker"], "") || sym.split("-")[0] || "";
          bestBySymbol.set(sym, { asset: assetGuess.toUpperCase(), symbol: sym, change_1d: c1d, venue_filter: vf || "" });
        }
      }

      let best = null;
      for (const sym of heldSymbols) {
        const hit = bestBySymbol.get(sym);
        if (!hit) continue;
        if (!best) best = hit;
        else if (Number(hit.change_1d) > Number(best.change_1d)) best = hit;
      }
      if (!best) return;

      const at = new Date().toISOString();
      setTgTop(best);
      setTgTopAt(at);
      tgWriteChipCache({ top: best, at });
    } catch {
      // ignore (chip should be best-effort / non-fatal)
    } finally {
      tgInFlightRef.current = false;
    }
  };

  // Schedule background polling:
  // - first tick after 0–800ms jitter
  // - then every refreshSeconds
  useEffect(() => {
    if (typeof window === "undefined") return;

    if (tgPollTimerRef.current) {
      clearTimeout(tgPollTimerRef.current);
      tgPollTimerRef.current = null;
    }

    let canceled = false;

    const loop = async () => {
      if (canceled) return;
      const sec = tgClampSeconds(tgReadInt(tgLsKey("refreshSeconds"), 300), 300);
      await doTopGainersChipRefresh({ reason: "interval" });
      if (canceled) return;
      tgPollTimerRef.current = setTimeout(loop, sec * 1000);
    };

    const jitterMs = Math.floor(Math.random() * 800);
    tgPollTimerRef.current = setTimeout(loop, jitterMs);

    return () => {
      canceled = true;
      if (tgPollTimerRef.current) {
        clearTimeout(tgPollTimerRef.current);
        tgPollTimerRef.current = null;
      }
      try {
        tgAbortRef.current?.abort?.();
      } catch {
        // ignore
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [API_BASE, enabledVenuesForScanners.join("|"), tgVenueFilter]);

  const mask = (s) => (hideTableDataGlobal ? "••••" : String(s ?? "—"));
  const fmtPct = (n) => {
    if (hideTableDataGlobal) return "••••";
    const x = Number(n);
    if (!Number.isFinite(x)) return "—";
    const sign = x >= 0 ? "+" : "";
    return `${sign}${x.toFixed(2)}%`;
  };

  const tgSubLabel = useMemo(() => {
    if (!tgTop || !tgTop.asset) return hideTableDataGlobal ? "••••" : "—";
    const a = mask(tgTop.asset);
    const pct = fmtPct(tgTop.change_1d);
    return `${a} ${pct} (1d)`;
  }, [tgTop, hideTableDataGlobal]);

  // Popover behavior (close on outside click / ESC)
  const tgBtnRef = useRef(null);
  const tgPopRef = useRef(null);

  useEffect(() => {
    if (!tgPopoverOpen) return;

    const onDown = (e) => {
      const btn = tgBtnRef.current;
      const pop = tgPopRef.current;
      const t = e.target;
      if (btn && btn.contains(t)) return;
      if (pop && pop.contains(t)) return;
      setTgPopoverOpen(false);
    };

    const onKey = (e) => {
      if (e.key === "Escape") {
        setTgPopoverOpen(false);
      }
    };

    document.addEventListener("mousedown", onDown, true);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousedown", onDown, true);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [tgPopoverOpen]);

  // Donate popover behavior (close on outside click / ESC)
  useEffect(() => {
    if (!donateOpen) return;

    const onDown = (e) => {
      const btn = donateBtnRef.current;
      const pop = donatePopRef.current;
      const t = e.target;
      if (btn && btn.contains(t)) return;
      if (pop && pop.contains(t)) return;
      setDonateOpen(false);
      setDonateMsg("");
    };

    const onKey = (e) => {
      if (e.key === "Escape") {
        setDonateOpen(false);
        setDonateMsg("");
      }
    };

    document.addEventListener("mousedown", onDown, true);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousedown", onDown, true);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [donateOpen]);

  const copyText = async (txt) => {
    const s = String(txt || "").trim();
    if (!s) return false;

    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(s);
        return true;
      }
    } catch {
      // ignore
    }

    try {
      const ta = document.createElement("textarea");
      ta.value = s;
      ta.style.position = "fixed";
      ta.style.left = "-9999px";
      ta.style.top = "-9999px";
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      return !!ok;
    } catch {
      return false;
    }
  };

  const toggleHideAddrs = () => {
    const next = !donateHideAddrs;
    setDonateHideAddrs(next);
    writeDonateHideAddrsToStorage(next);
  };

  // ---------------------------
  // Draggable / movable TG panel
  // ---------------------------
  const POP_MARGIN = 8;
  const POP_GAP = 10;
  const POP_MAX_W = 860;

  const [tgPos, setTgPos] = useState(null); // { x, y, w }
  const draggingRef = useRef(false);
  const dragStartRef = useRef({ mx: 0, my: 0, x: 0, y: 0, w: POP_MAX_W });

  const computeTgWidth = () => {
    const vw = Math.max(320, window.innerWidth || 0);
    const w = Math.min(POP_MAX_W, Math.floor(vw * 0.92));
    return Math.max(320, w);
  };

  const clampTgPos = (x, y, w) => {
    const vw = Math.max(320, window.innerWidth || 0);
    const vh = Math.max(320, window.innerHeight || 0);
    const ww = Math.min(Math.max(320, w || computeTgWidth()), vw - POP_MARGIN * 2);

    const maxX = Math.max(POP_MARGIN, vw - ww - POP_MARGIN);
    const maxY = Math.max(POP_MARGIN, vh - 60);
    const cx = clamp(x, POP_MARGIN, maxX);
    const cy = clamp(y, POP_MARGIN, maxY);
    return { x: cx, y: cy, w: ww };
  };

  const placeTgNearButton = () => {
    const btn = tgBtnRef.current;
    if (!btn) return;

    const rect = btn.getBoundingClientRect();
    const w = computeTgWidth();

    const desiredX = rect.right - w;
    const desiredY = rect.bottom + POP_GAP;

    setTgPos(clampTgPos(desiredX, desiredY, w));
  };

  useEffect(() => {
    if (!tgPopoverOpen) return;
    if (!tgPos) {
      placeTgNearButton();
      return;
    }
    setTgPos((p) => (p ? clampTgPos(p.x, p.y, p.w) : p));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tgPopoverOpen]);

  useEffect(() => {
    if (!tgPopoverOpen) return;

    const onResize = () => {
      setTgPos((p) => {
        if (!p) return p;
        const w = computeTgWidth();
        return clampTgPos(p.x, p.y, Math.min(p.w || w, w));
      });
    };

    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tgPopoverOpen]);

  const startDrag = (e) => {
    if (e.button !== 0) return;

    const t = e.target;
    const interactive = t?.closest?.("button, a, input, select, textarea, label");
    if (interactive) return;

    if (!tgPos) return;

    draggingRef.current = true;
    dragStartRef.current = { mx: e.clientX, my: e.clientY, x: tgPos.x, y: tgPos.y, w: tgPos.w };

    const prevUserSelect = document.body.style.userSelect;
    const prevCursor = document.body.style.cursor;
    document.body.style.userSelect = "none";
    document.body.style.cursor = "grabbing";

    const onMove = (ev) => {
      if (!draggingRef.current) return;
      const dx = ev.clientX - dragStartRef.current.mx;
      const dy = ev.clientY - dragStartRef.current.my;
      const nx = dragStartRef.current.x + dx;
      const ny = dragStartRef.current.y + dy;
      setTgPos(clampTgPos(nx, ny, dragStartRef.current.w));
    };

    const onUp = () => {
      draggingRef.current = false;
      document.body.style.userSelect = prevUserSelect;
      document.body.style.cursor = prevCursor;
      window.removeEventListener("mousemove", onMove, true);
      window.removeEventListener("mouseup", onUp, true);
      window.removeEventListener("mousemove", onMove, true);
      window.removeEventListener("mouseup", onUp, true);
    };

    window.addEventListener("mousemove", onMove, true);
    window.addEventListener("mouseup", onUp, true);
  };

  const floatingPanelStyle = useMemo(() => {
    if (!tgPos) {
      return {
        position: "fixed",
        left: POP_MARGIN,
        top: 120,
        zIndex: 9999,
        width: computeTgWidth(),
        maxWidth: "92vw",
        filter: "drop-shadow(0 18px 40px rgba(0,0,0,0.55))",
      };
    }

    return {
      position: "fixed",
      left: tgPos.x,
      top: tgPos.y,
      zIndex: 9999,
      width: tgPos.w,
      maxWidth: "92vw",
      filter: "drop-shadow(0 18px 40px rgba(0,0,0,0.55))",
    };
  }, [tgPos]);

  return (
    <div ref={headerRef} style={headerStyles.headerWrap}>
      <div style={{ margin: "4px 0 8px 0", position: "relative" }}>
        <h1 style={{ position: "absolute", left: -9999, width: 1, height: 1, overflow: "hidden" }}>Unified Trading Terminal</h1>

        <img
          src={banner?.dataUrl || uttBanner}
          alt="Unified Trading Terminal"
          style={{
            display: "block",
            width: "100%",
            height: bannerDisplayHeight,
            objectFit: "cover",
            borderRadius: 12,
            border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.10))",
          }}
          draggable={false}
        />

        <div style={bannerCtlWrapStyle}>
          <input
            ref={bannerInputRef}
            type="file"
            accept="image/png,image/jpeg,image/webp,image/*"
            style={{ display: "none" }}
            onChange={(e) => {
              const f = e.target?.files?.[0] || null;
              e.target.value = "";
              if (f) handleBannerPicked(f);
            }}
          />

          <div style={bannerCtlRowStyle}>
            <button type="button" style={bannerBtnStyle} onClick={openBannerPicker} title="Upload or change the header banner">
              <UploadIcon size={14} />
              <span>{banner ? "Change Banner" : "Upload Banner"}</span>
            </button>

            <button
              type="button"
              style={bannerBtnSecondaryStyle}
              onClick={() => (banner ? clearBanner() : null)}
              disabled={!banner}
              title={banner ? "Revert to the default UTT banner" : "No custom banner set"}
            >
              Reset
            </button>

            <div style={bannerReqStyle} title="Recommended banner size and limits">
              Recommended: {BANNER_RECOMMENDED_W}×{BANNER_RECOMMENDED_H}+ • Max {(BANNER_MAX_BYTES / (1024 * 1024)).toFixed(0)}MB • JPG/PNG/WebP
            </div>
          </div>

          {!!bannerMsg && <div style={bannerWarnStyle}>{bannerMsg}</div>}

          {/* Donate button lives under the banner upload controls */}
          <div style={{ position: "relative", alignSelf: "flex-end" }}>
            <button
              ref={donateBtnRef}
              type="button"
              style={donateBtnStyle}
              onClick={() => {
                const next = !donateOpen;
                setDonateOpen(next);
                setDonateMsg("");
                if (!donateOpen && next) {
                  // place under button after render
                  setTimeout(() => placeDonateNearButton(), 0);
                }
              }}
              title="Donate (crypto / PayPal)"
              aria-label="Donate"
            >
              <DonateIcon size={14} />
              <span>DONATE</span>
            </button>
          </div>
        </div>
      </div>

      {/* FIXED popover (outside of banner stack) so it cannot be clipped */}
      {donateOpen && (
        <div ref={donatePopRef} style={donatePanelStyle}>
          <div style={donatePanelHeaderStyle}>
            <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 0 }}>
              <div style={{ fontWeight: 900, fontSize: 13, lineHeight: 1.1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {donateCfg.title || "Support UTT"}
              </div>
              <div style={{ fontSize: 11, opacity: 0.75, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {donateCfg.note || "Donations help keep development moving."}
              </div>
            </div>

            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, opacity: 0.9, cursor: "pointer", userSelect: "none" }}>
                <input type="checkbox" checked={donateHideAddrs} onChange={toggleHideAddrs} />
                <span>Hide addresses</span>
              </label>

              <button
                type="button"
                style={donateSmallBtnStyle}
                onClick={() => {
                  setDonateOpen(false);
                  setDonateMsg("");
                }}
                title="Close"
              >
                Close
              </button>
            </div>
          </div>

          <div style={donatePanelBodyStyle}>
            {!!donateMsg && <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-link, #9ad)" }}>{donateMsg}</div>}

            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10 }}>
              <div style={{ fontWeight: 800, fontSize: 12, opacity: 0.9 }}>PayPal</div>
              <button
                type="button"
                style={donatePrimaryBtnStyle}
                disabled={!String(donateCfg.paypalUrl || "").trim()}
                onClick={() => {
                  const u = String(donateCfg.paypalUrl || "").trim();
                  if (!u) return;
                  try {
                    window.open(u, "_blank", "noopener,noreferrer");
                  } catch {
                    // ignore
                  }
                }}
                title={String(donateCfg.paypalUrl || "").trim() ? "Open PayPal link" : "PayPal not configured in this build"}
              >
                Open PayPal
              </button>
            </div>

            <div style={{ fontWeight: 800, fontSize: 12, opacity: 0.9, marginTop: 2 }}>Crypto</div>

            {(donateCfg.coins || []).map((c) => {
              const addr = String(c?.address || "").trim();
              const shown = donateHideAddrs ? (addr ? "••••••••••••••••" : "") : addr;

              return (
                <div key={c.key} style={donateRowStyle}>
                  <div style={{ fontSize: 12, opacity: 0.9, fontWeight: 800 }}>{c.label}</div>
                  <input
                    style={donateInputStyle}
                    value={shown}
                    readOnly
                    placeholder="(not set)"
                    onFocus={(e) => e.target.select()}
                    title={addr ? addr : "Not set in this build"}
                  />
                  <button
                    type="button"
                    style={donateSmallBtnStyle}
                    disabled={!addr || donateHideAddrs}
                    onClick={async () => {
                      if (!addr) return;
                      const ok = await copyText(addr);
                      setDonateMsg(ok ? `${c.label}: copied.` : `${c.label}: copy failed.`);
                      setTimeout(() => setDonateMsg(""), 1400);
                    }}
                    title={donateHideAddrs ? "Disable Hide addresses to copy" : addr ? "Copy address" : "Not set in this build"}
                  >
                    Copy
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div style={headerStyles.toolbar}>
        <div style={headerStyles.pill}>
          <span>Venue</span>
          <select style={headerStyles.select} value={venue} onChange={(e) => setVenue(e.target.value)}>
            <option value={ALL_VENUES_VALUE}>{labelVenueOption(ALL_VENUES_VALUE)}</option>
            {(supportedVenues || []).map((v) => (
              <option key={v} value={v}>
                {labelVenueOption(v)}
              </option>
            ))}
          </select>
        </div>

        <div style={headerStyles.pill} title="Trading safety: DRY_RUN is process-level; ARMED is runtime toggle.">
          <span>Safety</span>
          <span style={{ ...headerStyles.mutedSmall, fontSize: 12 }}>
            DRY_RUN: <b>{dryRunKnown ? (isDryRun ? "ON" : "OFF") : "…"}</b>
          </span>
          <span style={{ ...headerStyles.mutedSmall, fontSize: 12 }}>
            ARMED: <b>{armedKnown ? (isArmed ? "YES" : "NO") : "…"}</b>
          </span>

          <button
            style={btnHeader(isArmed ? disarmDisabled : armDisabled)}
            disabled={isArmed ? disarmDisabled : armDisabled}
            onClick={() => (isArmed ? doSetArmed(false) : doSetArmed(true))}
            title={
              isArmed
                ? "Disarm live trading (forces dry-run routing)."
                : dryRunKnown && isDryRun
                ? "Cannot ARM while DRY_RUN=true. Set DRY_RUN=false and restart backend."
                : "Arm live trading (only effective if DRY_RUN=false and LIVE_VENUES allows the venue)."
            }
          >
            {loadingArm ? "Working…" : isArmed ? "Disarm" : "Arm"}
          </button>

          <button style={btnHeader(loadingArm)} disabled={loadingArm} onClick={() => loadArmStatus()} title="Refresh safety status">
            Refresh
          </button>
        </div>

        <label style={headerStyles.pill}>
          <input type="checkbox" checked={pollEnabled} onChange={(e) => setPollEnabled(e.target.checked)} />
          <span>Background refresh</span>
        </label>

        <div style={headerStyles.pill}>
          <span>Every</span>
          <input
            style={{ ...headerStyles.input, width: 90 }}
            type="number"
            min="3"
            max="300"
            value={pollSeconds}
            onChange={(e) => setPollSeconds(e.target.value)}
            disabled={!pollEnabled}
          />
          <span className="muted">sec</span>
        </div>

        <div style={headerStyles.pill}>
          <span>Market</span>
          <input
            style={{ ...headerStyles.input, width: 200 }}
            value={marketInput}
            placeholder="e.g. BTC-USD"
            onChange={(e) => setMarketInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") applyMarketSymbol();
            }}
          />
          <button style={btnHeader(!marketInput.trim())} disabled={!marketInput.trim()} onClick={applyMarketSymbol}>
            Apply
          </button>
        </div>

        <label style={headerStyles.pill} title="When checked, Apply will also set the current tab’s symbol filter (Orders tabs).">
          <input type="checkbox" checked={applyMarketToTab} onChange={(e) => setApplyMarketToTab(e.target.checked)} />
          <span>Apply to tab</span>
        </label>

        <label style={headerStyles.pill} title="Masks table values and also hides venue names across the UI/widgets.">
          <input type="checkbox" checked={hideTableDataGlobal} onChange={(e) => setHideTableDataGlobal(e.target.checked)} />
          <span>Hide table data</span>
        </label>

        <div style={headerStyles.pill} title="Show/hide widgets (persisted).">
          <span>Widgets</span>

          <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={!!visible.chart} onChange={(e) => setVisible((v) => ({ ...v, chart: e.target.checked }))} />
            <span>Chart</span>
          </label>

          <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={!!visible.tables} onChange={(e) => setVisible((v) => ({ ...v, tables: e.target.checked }))} />
            <span>Tables</span>
          </label>

          <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={!!visible.orderBook} onChange={(e) => setVisible((v) => ({ ...v, orderBook: e.target.checked }))} />
            <span>Order Book</span>
          </label>

          <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={!!visible.orderTicket} onChange={(e) => setVisible((v) => ({ ...v, orderTicket: e.target.checked }))} />
            <span>Order Ticket</span>
          </label>
        </div>

        <button style={btnHeader(false)} onClick={onResetWidgets} title="Reset widget visibility">
          Reset Widgets
        </button>

        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 12 }}>
          <button
            type="button"
            style={screenshotLinkStyle}
            disabled={shotBusy}
            onClick={captureFullUiScreenshot}
            title="Capture a screenshot of the rendered UI (select your current browser tab when prompted)."
            aria-label={shotBusy ? "Capturing screenshot" : "Capture screenshot"}
          >
            {shotBusy ? (
              <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                <CameraIcon size={14} />
                <span>Capturing…</span>
              </span>
            ) : (
              <CameraIcon size={14} />
            )}
          </button>

          <div style={{ ...headerStyles.mutedSmall, fontSize: 12, whiteSpace: "nowrap" }}>
            Total Portfolio (All Venues): <b>${headerAllVenuesTotalText}</b>
          </div>

          <div style={{ ...headerStyles.mutedSmall, fontSize: 12 }}>
            API: {API_BASE}
            {loadingSupportedVenues ? " (venues…)" : venuesLoaded ? "" : " (venues…)"}{" "}
          </div>
        </div>
      </div>

      <div style={toolTabsRowStyle}>
        <ArbChip
          apiBase={API_BASE}
          symbol={obSymbol}
          venues={arbVenues}
          refreshMs={8000}
          fmtPrice={fmtPrice}
          hideTableData={hideTableDataGlobal}
          hideVenueNames={hideVenueNames}
          styles={styles}
          thresholdPct={0.1}
          fetchArbSnapshot={fetchArbSnapshot}
          popoverAlign="left"
          chipVariant="tooltab"
          chipTitle="Arbitrage"
        />

        <div style={{ position: "relative" }}>
          <span ref={tgBtnRef} style={{ display: "inline-block" }}>
            <ToolChip
              title={tg.title || "Top Gainers"}
              subLabel={hideTableDataGlobal ? "••••" : tgSubLabel}
              isOpen={!!tgPopoverOpen}
              onClick={() => {
                const wasOpen = tgPopoverOpen;
                const next = !wasOpen;
                setTgPopoverOpen(next);
                if (!wasOpen && next) {
                  setTimeout(() => placeTgNearButton(), 0);
                }
              }}
            />
          </span>

          {tgPopoverOpen && (
            <div ref={tgPopRef} style={floatingPanelStyle}>
              <TopGainersWindow
                apiBase={API_BASE}
                enabledVenues={enabledVenuesForScanners}
                hideTableData={hideTableDataGlobal}
                venueFilter={tgVenueFilter}
                onVenueFilterChange={setTgVenueFilterSafe}
                onClose={() => setTgPopoverOpen(false)}
                height={560}
                onDragHandleMouseDown={startDrag}
              />

              {!hideTableDataGlobal && tgTopAt && <div style={{ marginTop: 6, fontSize: 11, opacity: 0.65, textAlign: "right" }}>chip summary updated: {tgTopAt}</div>}
            </div>
          )}
        </div>

        {(toolWindows || [])
          .filter((w) => !isArbTool(w) && !isTopGainersTool(w))
	      .map((w) => {
	        const idLower = String(w?.id || "").toLowerCase();
	        const titleLower = String(w?.title || "").toLowerCase();
	        const isLedger = idLower === "ledger" || titleLower === "ledger";
	        const isWalletAddresses = idLower === "wallet_addresses" || titleLower === "wallet addresses";
	        return (
	          <ToolChip
	            key={w.id}
	            title={w.title}
	            subLabel={isLedger ? null : isWalletAddresses ? (hideTableDataGlobal ? "••••" : "On-chain") : "—"}
	            showStatus={!isLedger}
	            showSubLabel={!isLedger}
	            isOpen={!!w.isOpen || !!w.open}
	            onClick={() => toggleToolWindow?.(w.id)}
	          />
	        );
	      })}
      </div>

      {error && <div style={headerStyles.error}>{error}</div>}
    </div>
  );
}