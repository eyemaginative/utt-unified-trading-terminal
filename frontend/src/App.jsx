// frontend/src/App.jsx
// App.jsx
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  API_BASE,
  refreshBalances,
  getLatestBalances,
  getPricesUSD,
  getOrders,
  cancelOrder,
  getAllOrders,
  refreshVenueOrders,

  // safety + order views
  getSafetyStatus,
  setArmed as apiSetArmed,
  confirmOrderView,

  // discovery endpoints
  refreshSymbols,
  getNewSymbols,
  getUnheldNewSymbols,
  listSymbolViews,
  confirmSymbolView,

  // unified cancel-by-ref
  cancelOrderByRef,

  // arb snapshot
  getArbSnapshot,

  // venues registry (single source of truth)
  getVenuesRawSafe,
} from "./lib/api";
import { fmtNum, fmtTime, isCancelableStatus } from "./lib/format";
import TradingViewChartWidget from "./TradingViewChartWidget";
import OrderBookWidget from "./OrderBookWidget";
import OrderTicketWidget from "./OrderTicketWidget";
import TerminalTablesWidget from "./TerminalTablesWidget";
import useAppState from "./app/useAppState";
import WindowManager from "./app/WindowManager";
import AppHeader from "./components/AppHeader";
// ErrorBoundary: prevents "gray screen" by surfacing runtime errors in UI.
class AppErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, info: null };
  }
  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }
  componentDidCatch(error, info) {
    this.setState({ info });
    // Also log to console for devtools.
    // eslint-disable-next-line no-console
    console.error("AppErrorBoundary caught error:", error, info);
  }
  render() {
    if (this.state.hasError) {
      const msg = this.state.error ? String(this.state.error?.message || this.state.error) : "Unknown error";
      return (
        <div style={{ padding: 16, fontFamily: "ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto" }}>
          <div style={{ fontSize: 18, fontWeight: 700, marginBottom: 8 }}>UTT UI crashed</div>
          <div style={{ opacity: 0.85, marginBottom: 12 }}>Open DevTools Console for full stack trace.</div>
          <pre style={{ whiteSpace: "pre-wrap", background: "rgba(255,255,255,0.06)", padding: 12, borderRadius: 8 }}>{msg}</pre>
        </div>
      );
    }
    return this.props.children;
  }
}


// --- URL routing (no react-router-dom) ---
// Routes:
//   /market/:venue/:symbol   -> e.g. /market/coinbase/ADA-USD
//   /market/:venue           -> e.g. /market/coinbase (symbol omitted)
const MARKET_ROUTE_RE = /^\/market\/([^/]+)(?:\/([^/]+))?\/?$/i;

function parseMarketRoute(pathname) {
  const m = MARKET_ROUTE_RE.exec(String(pathname || ""));
  if (!m) return null;
  const venue = decodeURIComponent(m[1] || "").trim();
  const symbol = decodeURIComponent(m[2] || "").trim();
  return { venue, symbol };
}

function buildMarketRoute(venue, symbol) {
  const v = String(venue || "").trim();
  const s = String(symbol || "").trim();
  if (!v) return "/";
  if (!s) return `/market/${encodeURIComponent(v)}`;
  return `/market/${encodeURIComponent(v)}/${encodeURIComponent(s)}`;
}

// Tool windows
import ArbWindow from "./features/arb/ArbWindow";
import TopGainersWindow from "./features/scanners/TopGainersWindow";
import MarketCapWindow from "./features/scanners/MarketCapWindow";
import VolumeWindow from "./features/scanners/VolumeWindow";

import LedgerWindow from "./features/basis/LedgerWindow";
import WalletAddressesWindow from "./features/wallets/WalletAddressesWindow";
// Fallback base styling (global). Header gets its own overrides driven by the Tables theme bus.
// NOTE: Most surfaces now reference CSS vars so App shell can follow Tables theme (without fully re-theming every widget yet).
const fallbackStyles = {
  page: {
    minHeight: "100vh",
    background: "var(--utt-page-bg, #111)",
    color: "var(--utt-page-fg, #eee)",
    fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif",
  },
  container: {
    maxWidth: 1280,
    margin: "0 auto",
    padding: 10,
  },
  appRow: { display: "block" },
  headerWrap: { position: "relative" },
  title: {
    margin: "4px 0 8px 0",
    fontSize: 30,
    fontWeight: 850,
    letterSpacing: 0.2,
    lineHeight: 1.05,
  },
  toolbar: {
    display: "flex",
    gap: 10,
    rowGap: 8,
    alignItems: "center",
    flexWrap: "wrap",
    color: "var(--utt-page-fg, #ddd)",
  },
  pill: {
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    padding: "6px 8px",
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    borderRadius: 10,
    background: "var(--utt-surface-2, #151515)",
  },
  select: {
    background: "var(--utt-control-bg, #0f0f0f)",
    color: "var(--utt-page-fg, #eee)",
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    borderRadius: 8,
    padding: "5px 8px",
  },
  input: {
    background: "var(--utt-control-bg, #0f0f0f)",
    color: "var(--utt-page-fg, #eee)",
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    borderRadius: 8,
    padding: "5px 8px",
  },
  button: {
    background: "var(--utt-button-bg, #1b1b1b)",
    color: "var(--utt-page-fg, #eee)",
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    borderRadius: 10,
    padding: "6px 9px",
    cursor: "pointer",
    whiteSpace: "nowrap",
  },
  buttonDisabled: { opacity: 0.55, cursor: "not-allowed" },
  error: {
    marginTop: 12,
    color: "#ff6b6b",
    whiteSpace: "pre-wrap",
    border: "1px solid rgba(255,107,107,0.25)",
    background: "rgba(40,10,10,0.55)",
    padding: 10,
    borderRadius: 10,
  },
  sectionRow: { display: "flex", alignItems: "center", gap: 10, marginTop: 18, flexWrap: "wrap" },
  h2: { margin: 0, fontSize: 22 },

  table: {
    width: "100%",
    borderCollapse: "collapse",
    marginTop: 10,
    background: "var(--utt-surface-1, #121212)",
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    borderRadius: 12,
    overflow: "hidden",
  },
  th: {
    textAlign: "left",
    fontWeight: 700,
    fontSize: 13,
    padding: "10px 10px",
    borderBottom: "1px solid var(--utt-border-1, #2a2a2a)",
    background: "var(--utt-surface-2, #151515)",
    userSelect: "none",
    whiteSpace: "nowrap",
  },
  td: {
    fontSize: 13,
    padding: "9px 10px",
    borderBottom: "1px solid var(--utt-row-border, #1f1f1f)",
    whiteSpace: "nowrap",
  },
  linkyHeader: { cursor: "pointer" },
  muted: { opacity: 0.7 },

  orderBookDock: {
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    background: "var(--utt-surface-1, #121212)",
    borderRadius: 12,
    padding: 12,
    boxShadow: "0 10px 24px rgba(0,0,0,0.35)",
  },
  obResizeHandle: {
    position: "absolute",
    left: 6,
    bottom: 6,
    width: 18,
    height: 18,
    borderRadius: 6,
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    background: "var(--utt-surface-2, #151515)",
    cursor: "nesw-resize",
    zIndex: 5,
  },
  widgetTitleRow: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 10,
    flexWrap: "wrap",
    marginBottom: 8,
  },
  widgetTitle: { margin: 0, fontSize: 20, fontWeight: 800 },
  widgetSub: { fontSize: 12, opacity: 0.75 },
  codeError: {
    marginTop: 8,
    color: "#ff6b6b",
    whiteSpace: "pre-wrap",
    border: "1px solid rgba(255,107,107,0.25)",
    background: "rgba(40,10,10,0.55)",
    padding: 10,
    borderRadius: 10,
  },
  obSectionTitle: { opacity: 0.8, marginTop: 10, fontSize: 12 },
  obTableWrap: {
    marginTop: 6,
    border: "1px solid var(--utt-border-1, #2a2a2a)",
    borderRadius: 12,
    overflow: "auto",
    maxHeight: 200,
    background: "var(--utt-surface-0, #0f0f0f)",
  },
  obInnerTable: { width: "100%", borderCollapse: "collapse" },
  obTh: {
    textAlign: "left",
    fontWeight: 700,
    fontSize: 13,
    padding: "10px 10px",
    borderBottom: "1px solid var(--utt-border-1, #2a2a2a)",
    background: "var(--utt-surface-2, #151515)",
    userSelect: "none",
    whiteSpace: "nowrap",
    position: "sticky",
    top: 0,
    zIndex: 2,
  },
  obTd: {
    fontSize: 13,
    padding: "9px 10px",
    borderBottom: "1px solid var(--utt-row-border, #1f1f1f)",
    whiteSpace: "nowrap",
  },
};

const ALL_VENUES_VALUE = "ALL";

// NOTE: This list is used for *core trading/balances polling* fallback.
// Do not restrict it to “discovery-capable” venues.
const DEFAULT_SUPPORTED_VENUES = ["gemini", "coinbase", "kraken", "robinhood", "dex_trade"];

// Arb venues (preferred cross-venue scan list)
const ARB_VENUES = ["coinbase", "kraken", "gemini", "robinhood", "dex_trade"];

const LS_VISIBLE_WIDGETS = "utt_visible_widgets_v1";
const DEFAULT_VISIBLE = {
  chart: true,
  tables: true,
  orderBook: true,
  orderTicket: true,
};

// persist background refresh
const LS_POLL_ENABLED = "utt_poll_enabled_v1";
const LS_POLL_SECONDS = "utt_poll_seconds_v1";


// All Orders: Ledger sync settings (shared with TerminalTablesWidget)
const LS_AO_LEDGER_SYNC_ON_SYNCLOAD_KEY = "utt_ao_ledger_sync_on_syncload_v1";
const LS_AO_LEDGER_SYNC_WALLET_ID_KEY = "utt_ao_ledger_sync_wallet_id_v1";
const LS_AO_LEDGER_SYNC_MODE_KEY = "utt_ao_ledger_sync_mode_v1";
const LS_AO_LEDGER_SYNC_LIMIT_KEY = "utt_ao_ledger_sync_limit_v1";
const LS_AO_LEDGER_SYNC_DRY_RUN_KEY = "utt_ao_ledger_sync_dry_run_v1";

// persist default hide-cancelled toggles
const LS_HIDE_CANCELLED_LOCAL = "utt_hide_cancelled_local_v1";
// NOTE: bumped key to v2 to avoid inheriting the old default that hid canceled orders by default.
const LS_HIDE_CANCELLED_UNIFIED = "utt_hide_cancelled_unified_v2";

// discovery prefs
const LS_DISCOVER_VENUE = "utt_discover_venue_v1";
const LS_DISCOVER_EPS = "utt_discover_eps_v1";

// Discovery viewed state (local cache). Server persistence is attempted if endpoints exist.
const LS_DISCOVERY_VIEWED_MAP = "utt_discovery_viewed_symbols_v1";

// Venue markets cache (balances hover -> market list)
const VENUE_MARKETS_CACHE_TTL_MS = 60_000;

// ─────────────────────────────────────────────────────────────
// Theme bridge (Header + App shell)
// App header AND App shell follow TerminalTablesWidget palette selection via window.__uttThemeBus.
// This does not force a full re-theme of external widgets yet, but fixes “App still shows fallback”
// by making fallbackStyles resolve via CSS vars.
// ─────────────────────────────────────────────────────────────
const UTT_THEME_CANDIDATES = [
  "utt_theme",
  "utt_theme_v1",
  "utt_theme_name",
  "utt_theme_name_v1",
  "utt_terminal_theme",
  "utt_terminal_theme_v1",
  "utt_tables_theme",
  "utt_tables_theme_v1",
  "utt_tables_palette",
  "utt_tables_palette_v1",
];

// Header presets (stable internal names)
const HEADER_PRESETS = {
  Dark: {
    hdrBg: "linear-gradient(180deg, #141414 0%, #101010 100%)",
    hdrFg: "#eaeaea",
    hdrMuted: "rgba(234,234,234,0.75)",
    hdrBorder: "rgba(255,255,255,0.10)",
    pillBg: "rgba(0,0,0,0.28)",
    pillBorder: "rgba(255,255,255,0.12)",
    ctlBg: "rgba(0,0,0,0.35)",
    ctlBorder: "rgba(255,255,255,0.12)",
    btnBg: "rgba(255,255,255,0.06)",
    btnBorder: "rgba(255,255,255,0.12)",
    link: "#9ad",
  },
  "Midnight Blue": {
    hdrBg:
      "radial-gradient(900px 420px at 18% 0%, rgba(11,42,74,0.95) 0%, rgba(7,26,43,0.95) 45%, rgba(5,11,18,0.95) 100%)",
    hdrFg: "#eaf2ff",
    hdrMuted: "rgba(234,242,255,0.78)",
    hdrBorder: "rgba(234,242,255,0.16)",
    pillBg: "rgba(7,26,43,0.40)",
    pillBorder: "rgba(234,242,255,0.16)",
    ctlBg: "rgba(5,11,18,0.50)",
    ctlBorder: "rgba(234,242,255,0.18)",
    btnBg: "rgba(234,242,255,0.08)",
    btnBorder: "rgba(234,242,255,0.18)",
    link: "#9ad",
  },
  Graphite: {
    hdrBg: "linear-gradient(180deg, #1a1d22 0%, #15181c 100%)",
    hdrFg: "#eef2f7",
    hdrMuted: "rgba(238,242,247,0.78)",
    hdrBorder: "rgba(238,242,247,0.14)",
    pillBg: "rgba(0,0,0,0.22)",
    pillBorder: "rgba(238,242,247,0.14)",
    ctlBg: "rgba(0,0,0,0.28)",
    ctlBorder: "rgba(238,242,247,0.16)",
    btnBg: "rgba(238,242,247,0.08)",
    btnBorder: "rgba(238,242,247,0.16)",
    link: "#9ad",
  },
  OLED: {
    hdrBg: "linear-gradient(180deg, #000 0%, #070707 100%)",
    hdrFg: "#f2f2f2",
    hdrMuted: "rgba(242,242,242,0.78)",
    hdrBorder: "rgba(242,242,242,0.14)",
    pillBg: "rgba(255,255,255,0.06)",
    pillBorder: "rgba(255,255,255,0.12)",
    ctlBg: "rgba(255,255,255,0.06)",
    ctlBorder: "rgba(255,255,255,0.12)",
    btnBg: "rgba(255,255,255,0.08)",
    btnBorder: "rgba(255,255,255,0.14)",
    link: "#9ad",
  },
};

// App shell presets (CSS vars consumed by fallbackStyles)
const SHELL_PRESETS = {
  Dark: {
    pageBg: "#111",
    pageFg: "#eee",
    surface0: "#0f0f0f",
    surface1: "#121212",
    surface2: "#151515",
    border1: "#2a2a2a",
    rowBorder: "#1f1f1f",
    controlBg: "#0f0f0f",
    buttonBg: "#1b1b1b",
  },
  "Midnight Blue": {
    pageBg: "#06101a",
    pageFg: "#eaf2ff",
    surface0: "#040b12",
    surface1: "#071422",
    surface2: "#0a1a2b",
    border1: "rgba(234,242,255,0.18)",
    rowBorder: "rgba(234,242,255,0.10)",
    controlBg: "rgba(5,11,18,0.55)",
    buttonBg: "rgba(234,242,255,0.08)",
  },
  Graphite: {
    pageBg: "#121418",
    pageFg: "#eef2f7",
    surface0: "#0c0e11",
    surface1: "#15181c",
    surface2: "#1a1d22",
    border1: "rgba(238,242,247,0.16)",
    rowBorder: "rgba(238,242,247,0.10)",
    controlBg: "rgba(0,0,0,0.28)",
    buttonBg: "rgba(238,242,247,0.08)",
  },
  OLED: {
    pageBg: "#000",
    pageFg: "#f2f2f2",
    surface0: "#000",
    surface1: "#050505",
    surface2: "#0a0a0a",
    border1: "rgba(255,255,255,0.14)",
    rowBorder: "rgba(255,255,255,0.10)",
    controlBg: "rgba(255,255,255,0.06)",
    buttonBg: "rgba(255,255,255,0.08)",
  },
};

function canonThemeStr(s) {
  return String(s || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "");
}

// Maps the Tables palette names (free-form) to our stable header preset keys above.
function mapThemeName(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";

  const lc = s.toLowerCase();
  const cc = canonThemeStr(s);

  // Exact/near-exact names commonly used in Tables palettes.
  const exact = {
    "utt dark (default)": "Dark",
    "utt dark": "Dark",
    dark: "Dark",
    graphite: "Graphite",
    "midnight blue": "Midnight Blue",
    "utt midnight blue": "Midnight Blue",
    oled: "OLED",
    "true black": "OLED",
    "pitch black": "OLED",
  };
  if (exact[lc]) return exact[lc];

  // Canonical matches (handles "midnight_blue", "midnight-blue", "MidnightBlue", etc.)
  const canonExact = {
    uttdarkdefault: "Dark",
    uttdark: "Dark",
    dark: "Dark",
    graphite: "Graphite",
    uttgraphite: "Graphite",
    midnightblue: "Midnight Blue",
    uttmidnightblue: "Midnight Blue",
    oled: "OLED",
    trueblack: "OLED",
    pitchblack: "OLED",
  };
  if (canonExact[cc]) return canonExact[cc];

  // Heuristic mapping (future-proofing for palette renames)
  if (cc.includes("midnight") && cc.includes("blue")) return "Midnight Blue";
  if (cc.includes("graphite")) return "Graphite";
  if (cc.includes("oled") || cc.includes("trueblack") || cc.includes("pitchblack")) return "OLED";
  if (cc.includes("dark")) return "Dark";

  return "";
}

// Public normalizer used by runtime updates; defaults to Dark for safety.
function normalizeThemeName(raw) {
  return mapThemeName(raw) || "Dark";
}

function tryParseThemeValue(raw) {
  if (raw === null || raw === undefined) return "";
  try {
    const j = JSON.parse(raw);
    if (typeof j === "string") return j;
    if (j && typeof j === "object") {
      return (
        j.tablesTheme ||
        j.tables_theme ||
        j.tablesPalette ||
        j.tables_palette ||
        j.palette ||
        j.preset ||
        j.name ||
        j.theme ||
        j.value ||
        ""
      );
    }
    return "";
  } catch {
    return String(raw);
  }
}

function detectThemeNameFromLocalStorage() {
  try {
    // 1) Try known keys (ONLY if the key exists and has a value)
    for (const k of UTT_THEME_CANDIDATES) {
      const v = localStorage.getItem(k);
      if (v === null || v === undefined) continue;

      const parsed = tryParseThemeValue(v);
      const mapped = mapThemeName(parsed);
      if (mapped) return mapped;
    }

    // 2) Heuristic scan (value-based)
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (!key) continue;
      const raw = localStorage.getItem(key);
      if (raw === null || raw === undefined) continue;

      const val = canonThemeStr(raw);
      if (val.includes("midnight") && val.includes("blue")) return "Midnight Blue";
      if (val.includes("graphite")) return "Graphite";
      if (val.includes("oled") || val.includes("trueblack") || val.includes("pitchblack")) return "OLED";
      if (val === "dark" || val.includes("dark")) return "Dark";
    }
  } catch {
    // ignore
  }
  return "Dark";
}

function applyHeaderThemeVars(headerEl, presetKey) {
  const key = normalizeThemeName(presetKey);
  const preset = HEADER_PRESETS[key] || HEADER_PRESETS.Dark;

  try {
    if (!headerEl) return;

    headerEl.style.setProperty("--utt-hdr-bg", preset.hdrBg);
    headerEl.style.setProperty("--utt-hdr-fg", preset.hdrFg);
    headerEl.style.setProperty("--utt-hdr-muted", preset.hdrMuted);
    headerEl.style.setProperty("--utt-hdr-border", preset.hdrBorder);

    headerEl.style.setProperty("--utt-hdr-pill-bg", preset.pillBg);
    headerEl.style.setProperty("--utt-hdr-pill-border", preset.pillBorder);

    headerEl.style.setProperty("--utt-hdr-ctl-bg", preset.ctlBg);
    headerEl.style.setProperty("--utt-hdr-ctl-border", preset.ctlBorder);

    headerEl.style.setProperty("--utt-hdr-btn-bg", preset.btnBg);
    headerEl.style.setProperty("--utt-hdr-btn-border", preset.btnBorder);

    headerEl.style.setProperty("--utt-hdr-link", preset.link);
  } catch {
    // ignore
  }
}

// Apply “App shell” vars onto the root App element so fallbackStyles resolve using the active Tables theme.
function applyShellThemeVars(appEl, presetKey) {
  const key = normalizeThemeName(presetKey);
  const preset = SHELL_PRESETS[key] || SHELL_PRESETS.Dark;

  try {
    if (!appEl) return;

    appEl.style.setProperty("--utt-page-bg", preset.pageBg);
    appEl.style.setProperty("--utt-page-fg", preset.pageFg);

    appEl.style.setProperty("--utt-surface-0", preset.surface0);
    appEl.style.setProperty("--utt-surface-1", preset.surface1);
    appEl.style.setProperty("--utt-surface-2", preset.surface2);

    appEl.style.setProperty("--utt-border-1", preset.border1);
    appEl.style.setProperty("--utt-row-border", preset.rowBorder);

    appEl.style.setProperty("--utt-control-bg", preset.controlBg);
    appEl.style.setProperty("--utt-button-bg", preset.buttonBg);
  } catch {
    // ignore
  }
}

function safeParseJson(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function SortHeader({ label, active, dir }) {
  const arrow = active ? (dir === "asc" ? "▲" : "▼") : "";
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
      {label} <span style={{ opacity: 0.8 }}>{arrow}</span>
    </span>
  );
}

function isTerminalBucket(bucket) {
  return String(bucket || "").toLowerCase() === "terminal";
}

function isTerminalStatus(status) {
  const s = String(status || "").toLowerCase();
  return s === "filled" || s === "canceled" || s === "cancelled" || s === "rejected" || s === "done" || s === "closed";
}

function isCanceledStatus(status) {
  const s = String(status || "").toLowerCase();
  return s === "canceled" || s === "cancelled";
}

// unified hide predicate (for Local + Unified tables)
function isHiddenByHideCancelled(status) {
  const s = String(status || "").toLowerCase();
  return s === "canceled" || s === "cancelled";
}

function fmtEco(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 8 });
}

function calcGrossTotal(o) {
  const fq = Number(o?.filled_qty);
  const ap = Number(o?.avg_fill_price);
  if (Number.isFinite(fq) && Number.isFinite(ap) && fq > 0 && ap > 0) return fq * ap;

  const q = Number(o?.qty);
  const lp = Number(o?.limit_price);
  if (Number.isFinite(q) && Number.isFinite(lp) && q > 0 && lp > 0) return q * lp;

  return null;
}

function calcFee(o) {
  const fee = Number(o?.fee);
  return Number.isFinite(fee) ? fee : null;
}

function calcNetTotal(o) {
  const taf = Number(o?.total_after_fee);
  if (Number.isFinite(taf)) return taf;

  const gross = calcGrossTotal(o);
  const fee = calcFee(o);
  if (Number.isFinite(gross) && Number.isFinite(fee)) return gross - fee;

  return null;
}

function fmtUsd(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function fmtPxUsd(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 8 });
}

// Price formatter for ArbChip (kept local so App.jsx compiles even if lib/format doesn't export fmtPrice)
function fmtPrice(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 10 });
}

function normalizePricesUsdResponse(res) {
  if (!res) return {};

  if (typeof res === "object" && !Array.isArray(res)) {
    const maybeMap = { ...res };

    if (maybeMap.prices && typeof maybeMap.prices === "object") return maybeMap.prices;

    if (maybeMap.items && Array.isArray(maybeMap.items)) {
      const out = {};
      for (const it of maybeMap.items) {
        const a = String(it?.asset || it?.symbol || "").toUpperCase().trim();
        const px = Number(it?.px_usd ?? it?.price_usd ?? it?.price ?? it?.px);
        if (a && Number.isFinite(px)) out[a] = px;
      }
      return out;
    }

    const out = {};
    for (const [k, v] of Object.entries(maybeMap)) {
      const a = String(k || "").toUpperCase().trim();
      const px = Number(v);
      if (a && Number.isFinite(px)) out[a] = px;
    }
    return out;
  }

  return {};
}

// All Orders scope model (Design A)
function normalizeScope(v) {
  const s = String(v || "").trim().toUpperCase();
  if (s === "LOCAL") return "LOCAL";
  if (s === "VENUES") return "VENUES";
  return ""; // ALL
}

function readBoolLS(key, defaultVal) {
  const raw = localStorage.getItem(key);
  if (raw === null || raw === undefined) return defaultVal;
  const parsed = safeParseJson(raw);
  if (parsed === null) {
    const s = String(raw).toLowerCase().trim();
    if (s === "true") return true;
    if (s === "false") return false;
    return defaultVal;
  }
  return !!parsed;
}

function readNumLS(key, defaultVal) {
  const raw = localStorage.getItem(key);
  if (raw === null || raw === undefined) return defaultVal;
  const n = Number(raw);
  return Number.isFinite(n) ? n : defaultVal;
}

function normalizeVenueList(input) {
  let arr = [];
  if (Array.isArray(input)) {
    arr = input;
  } else if (input && typeof input === "object") {
    if (Array.isArray(input.venues)) arr = input.venues;
    else if (Array.isArray(input.items)) arr = input.items;
    else if (Array.isArray(input.supported_venues)) arr = input.supported_venues;
  }

  const out = [];
  const seen = new Set();

  const pick = (x) => {
    if (typeof x === "string") return x;
    if (!x || typeof x !== "object") return "";
    return x.venue ?? x.id ?? x.name ?? x.key ?? x.code ?? x.slug ?? x.value ?? "";
  };

  for (const x of arr) {
    let v = String(pick(x) ?? "").trim().toLowerCase();
    if (!v) continue;
    if (v === "[object object]") continue;
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

function normalizeSymbolCanon(s) {
  const sym = String(s || "").trim().toUpperCase();
  return sym;
}

// Local cache key: `${venue}|${symbolCanon}`
function discoveryKey(venue, symbolCanon) {
  const v = String(venue || "").trim().toLowerCase();
  const s = String(symbolCanon || "").trim().toUpperCase();
  if (!v || !s) return "";
  return `${v}|${s}`;
}

// Server view_key: `{venue}:{symbol_canon}`
function discoveryViewKey(venue, symbolCanon) {
  const v = String(venue || "").trim().toLowerCase();
  const s = String(symbolCanon || "").trim().toUpperCase();
  if (!v || !s) return "";
  return `${v}:${s}`;
}

function normalizeVenue(v) {
  return String(v || "").trim().toLowerCase();
}

// Converts many possible backend response shapes into: [{ venue, symbolCanon }]
function normalizeVenueMarketsResponse(v, asset, res) {
  const venue = normalizeVenue(v);
  const a = String(asset || "").trim().toUpperCase();

  const rawItems = (() => {
    if (!res) return [];
    if (Array.isArray(res)) return res;
    if (Array.isArray(res?.items)) return res.items;
    if (Array.isArray(res?.markets)) return res.markets;
    if (Array.isArray(res?.symbols)) return res.symbols;
    if (Array.isArray(res?.pairs)) return res.pairs;
    if (Array.isArray(res?.data)) return res.data;
    return [];
  })();

  const out = [];
  for (const it of rawItems) {
    if (typeof it === "string") {
      const s = String(it || "").trim();
      if (!s) continue;
      out.push({ venue, symbolCanon: normalizeSymbolCanon(s) });
      continue;
    }

    if (it && typeof it === "object") {
      const sym =
        it.symbolCanon ??
        it.symbol_canon ??
        it.symbol ??
        it.market ??
        it.product_id ??
        it.product ??
        it.pair ??
        it.id ??
        "";

      const vv = normalizeVenue(it.venue ?? it.venueName ?? venue);

      const symCanon = normalizeSymbolCanon(sym);
      if (!symCanon) continue;

      out.push({ venue: vv || venue, symbolCanon: symCanon });
      continue;
    }
  }

  if (out.length === 0 && !a) return [];
  return out;
}

function extractThemeNameFromBusPayload(payload) {
  if (!payload) return "";
  if (typeof payload === "string") return payload;

  if (payload && typeof payload === "object") {
    return (
      payload.tablesTheme ||
      payload.tables_theme ||
      payload.tablesPalette ||
      payload.tables_palette ||
      payload.palette ||
      payload.paletteName ||
      payload.preset ||
      payload.presetName ||
      payload.theme ||
      payload.name ||
      payload.value ||
      ""
    );
  }
  return "";
}

// Utility: best-effort titlecase fallback for unknown venues
function prettyVenueName(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  if (s.includes("_")) {
    return s
      .split("_")
      .map((x) => (x ? x[0].toUpperCase() + x.slice(1) : x))
      .join("-");
  }
  return s[0].toUpperCase() + s.slice(1);
}

export default function App() {
  const styles = useMemo(() => fallbackStyles, []);

  const _appState = useAppState();
  void _appState;

  const appContainerRef = useRef(null);
  const headerRef = useRef(null);

  const [themeKey, setThemeKey] = useState(() => detectThemeNameFromLocalStorage());

  useEffect(() => {
    applyHeaderThemeVars(headerRef.current, themeKey);
    applyShellThemeVars(appContainerRef.current, themeKey);
  }, [themeKey]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    let cancelled = false;
    let unsubscribe = null;
    let tries = 0;

    const onTheme = (payload) => {
      const rawName = extractThemeNameFromBusPayload(payload);
      const mapped = normalizeThemeName(rawName);
      if (!mapped) return;
      setThemeKey((prev) => (prev === mapped ? prev : mapped));
    };

    const attach = () => {
      if (cancelled) return;

      const bus = window.__uttThemeBus;
      if (!bus) {
        tries += 1;
        if (tries <= 40) setTimeout(attach, 100);
        return;
      }

      try {
        const initial =
          (typeof bus.getTablesTheme === "function" ? bus.getTablesTheme() : null) ||
          (typeof bus.getTablesPalette === "function" ? bus.getTablesPalette() : null) ||
          bus.tablesTheme ||
          bus.tables_theme ||
          bus.tablesPalette ||
          bus.tables_palette ||
          bus.theme ||
          bus.currentTheme ||
          null;

        const rawInitial = extractThemeNameFromBusPayload(initial);
        if (rawInitial) onTheme(rawInitial);
      } catch {
        // ignore
      }

      try {
        if (typeof bus.subscribeTablesTheme === "function") {
          unsubscribe = bus.subscribeTablesTheme(onTheme);
        } else if (typeof bus.subscribe === "function") {
          unsubscribe = bus.subscribe(onTheme);
        } else if (typeof bus.on === "function") {
          bus.on("tablesTheme", onTheme);
          unsubscribe = () => {
            try {
              bus.off?.("tablesTheme", onTheme);
            } catch {
              // ignore
            }
          };
        } else if (typeof bus.addListener === "function") {
          bus.addListener(onTheme);
          unsubscribe = () => {
            try {
              bus.removeListener?.(onTheme);
            } catch {
              // ignore
            }
          };
        } else if (typeof bus.setTablesTheme === "function") {
          const orig = bus.setTablesTheme;
          bus.setTablesTheme = (...args) => {
            try {
              onTheme(args?.[0]);
            } catch {
              // ignore
            }
            return orig.apply(bus, args);
          };
          unsubscribe = () => {
            try {
              bus.setTablesTheme = orig;
            } catch {
              // ignore
            }
          };
        }
      } catch {
        // ignore
      }
    };

    attach();

    return () => {
      cancelled = true;
      try {
        if (typeof unsubscribe === "function") unsubscribe();
      } catch {
        // ignore
      }
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const ls = window.localStorage;
    if (!ls) return;

    if (window.__uttThemeLsPatchInstalled) return;
    window.__uttThemeLsPatchInstalled = true;

    const origSetItem = ls.setItem.bind(ls);
    const origRemoveItem = ls.removeItem.bind(ls);

    const keyLooksThemeRelated = (k) => {
      const kk = String(k || "");
      if (!kk) return false;
      if (UTT_THEME_CANDIDATES.includes(kk)) return true;
      const lc = kk.toLowerCase();
      return lc.startsWith("utt_") && lc.includes("theme");
    };

    const valueLooksThemeRelated = (v) => {
      const s = canonThemeStr(v);
      return (
              (s.includes("midnight") && s.includes("blue")) ||
        s.includes("graphite") ||
        s.includes("oled") ||
        s.includes("trueblack") ||
        s.includes("pitchblack") ||
        s.includes("dark")
      );
    };

    const updateFromValueOrRescan = (maybeValue) => {
      const mappedFromValue = mapThemeName(tryParseThemeValue(maybeValue));
      const mapped = mappedFromValue || detectThemeNameFromLocalStorage();
      setThemeKey((prev) => (prev === mapped ? prev : mapped));
    };

    ls.setItem = (key, value) => {
      origSetItem(key, value);
      if (keyLooksThemeRelated(key) || valueLooksThemeRelated(value)) {
        updateFromValueOrRescan(value);
      }
    };

    ls.removeItem = (key) => {
      origRemoveItem(key);
      if (keyLooksThemeRelated(key)) {
        updateFromValueOrRescan(null);
      }
    };

    return () => {
      // Intentionally do not uninstall
    };
  }, []);

  useEffect(() => {
    const onStorage = (e) => {
      if (!e) return;
      if (!e.key) return;
      if (!UTT_THEME_CANDIDATES.includes(e.key)) return;
      const mapped = normalizeThemeName(tryParseThemeValue(e.newValue));
      if (mapped) setThemeKey((prev) => (prev === mapped ? prev : mapped));
    };

    try {
      window.addEventListener("storage", onStorage);
      return () => window.removeEventListener("storage", onStorage);
    } catch {
      return undefined;
    }
  }, []);

  const headerStyles = useMemo(() => {
    const merge = (a, b) => ({ ...(a || {}), ...(b || {}) });

    return {
      headerWrap: merge(styles.headerWrap, {
        background: "var(--utt-hdr-bg, transparent)",
        color: "var(--utt-hdr-fg, inherit)",
        border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.10))",
        borderRadius: 14,
        padding: 10,
        boxShadow: "0 10px 24px rgba(0,0,0,0.30)",
      }),
      title: merge(styles.title, { color: "var(--utt-hdr-fg, inherit)" }),
      toolbar: merge(styles.toolbar, { color: "var(--utt-hdr-fg, inherit)" }),
      pill: merge(styles.pill, {
        background: "var(--utt-hdr-pill-bg, rgba(0,0,0,0.22))",
        border: "1px solid var(--utt-hdr-pill-border, rgba(255,255,255,0.12))",
      }),
      select: merge(styles.select, {
        background: "var(--utt-hdr-ctl-bg, rgba(0,0,0,0.28))",
        border: "1px solid var(--utt-hdr-ctl-border, rgba(255,255,255,0.12))",
      }),
      input: merge(styles.input, {
        background: "var(--utt-hdr-ctl-bg, rgba(0,0,0,0.28))",
        border: "1px solid var(--utt-hdr-ctl-border, rgba(255,255,255,0.12))",
      }),
      button: merge(styles.button, {
        background: "var(--utt-hdr-btn-bg, rgba(255,255,255,0.06))",
        border: "1px solid var(--utt-hdr-btn-border, rgba(255,255,255,0.12))",
      }),
      mutedSmall: { opacity: 1, color: "var(--utt-hdr-muted, rgba(255,255,255,0.75))" },
      error: merge(styles.error, {
        border: "1px solid rgba(255,120,120,0.35)",
        background: "rgba(40,10,10,0.55)",
      }),
      link: { color: "var(--utt-hdr-link, #9ad)" },
    };
  }, [styles]);

  const [venue, setVenue] = useState("gemini");
  const [tab, setTab] = useState("balances");
  const [error, setError] = useState(null);

  // ─────────────────────────────────────────────────────────────
  // Tool windows (WindowManager) state
  // ─────────────────────────────────────────────────────────────
  const TOOL_IDS = useMemo(
    () => ({
      arb: "arb",
      topGainers: "top_gainers",
      marketCap: "market_cap",
      volume: "volume",
      walletAddresses: "wallet_addresses",
      deposits: "deposits",
    }),
    []
  );

  // NOTE: payload.focusSeq is used to force a state change when a tool is already open,
  // so WindowManager’s effect runs and can call win.focus() (idempotent “open/focus”).
  const [toolWindows, setToolWindows] = useState(() => [
    {
      id: TOOL_IDS.arb,
      title: "Arb",
      open: false,
      width: 920,
      height: 560,
      payload: { pollEnabled: true, pollSeconds: 300, focusSeq: 0 },
    },
    {
      id: TOOL_IDS.topGainers,
      title: "Top Gainers",
      open: false,
      width: 980,
      height: 620,
      payload: { pollEnabled: true, pollSeconds: 300, focusSeq: 0 },
    },
    {
      id: TOOL_IDS.marketCap,
      title: "Market Cap",
      open: false,
      width: 980,
      height: 620,
      payload: { pollEnabled: true, pollSeconds: 300, focusSeq: 0 },
    },
    {
      id: TOOL_IDS.volume,
      title: "Volume",
      open: false,
      width: 980,
      height: 620,
      payload: { pollEnabled: true, pollSeconds: 300, focusSeq: 0 },
    },
    {
      id: TOOL_IDS.walletAddresses,
      title: "Wallet Addresses",
      open: false,
      x: 20,
      y: 76,
      width: 980,
      height: 620,
      z: 12,
      payload: { focusSeq: 0 },
    },
    {
      id: TOOL_IDS.deposits,
      title: "Ledger",
      open: false,
      width: 980,
      height: 620,
      payload: { focusSeq: 0 },
    },
  ]);

  // IMPORTANT FIX: “tool tab click” is idempotent (open/focus only; never creates duplicates)
  // - If closed: open it
  // - If already open: keep open and bump focusSeq so WindowManager re-renders and focuses
  const toggleToolWindow = useCallback((id) => {
    const key = String(id || "").trim();
    if (!key) return;

    setToolWindows((prev) =>
      (prev || []).map((w) => {
        if (w.id !== key) return w;

        const payload = w?.payload && typeof w.payload === "object" ? w.payload : {};
        const focusSeq = Number(payload.focusSeq) || 0;

        return {
          ...w,
          open: true,
          payload: { ...payload, focusSeq: focusSeq + 1 },
        };
      })
    );
  }, []);

  const closeToolWindow = useCallback((id) => {
    const key = String(id || "").trim();
    if (!key) return;
    setToolWindows((prev) => (prev || []).map((w) => (w.id === key ? { ...w, open: false } : w)));
  }, []);

  const isToolOpen = useCallback(
    (id) => {
      const key = String(id || "").trim();
      return (toolWindows || []).some((w) => w.id === key && !!w.open);
    },
    [toolWindows]
  );
  void isToolOpen;

  // ─────────────────────────────────────────────────────────────
  // Venues registry (single source of truth)
  // ─────────────────────────────────────────────────────────────
  const [venuesRaw, setVenuesRaw] = useState([]);
  const [venuesLoaded, setVenuesLoaded] = useState(false);

  // UI-local venue enable/disable overrides (persisted in localStorage).
  // These override the backend registry's `enabled` flag for the frontend only.
  const LS_VENUE_OVERRIDES_KEY = "utt_venue_overrides_v1";
  const [venueOverrides, setVenueOverrides] = useState(() => {
    try {
      const raw = localStorage.getItem(LS_VENUE_OVERRIDES_KEY);
      if (!raw) return {};
      const obj = JSON.parse(raw);
      return obj && typeof obj === "object" ? obj : {};
    } catch {
      return {};
    }
  });

  const setVenueOverride = useCallback((venueId, enabled) => {
    const k = String(venueId || "").trim().toLowerCase();
    if (!k) return;
    setVenueOverrides((prev) => {
      const next = { ...(prev && typeof prev === "object" ? prev : {}) };
      next[k] = !!enabled;
      try {
        localStorage.setItem(LS_VENUE_OVERRIDES_KEY, JSON.stringify(next));
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  const [supportedVenues, setSupportedVenues] = useState(() => [...DEFAULT_SUPPORTED_VENUES]);
  const [loadingSupportedVenues, setLoadingSupportedVenues] = useState(false);

  const venuesEnabled = useMemo(() => {
    const raw = Array.isArray(venuesRaw) ? venuesRaw : [];

    const getId = (row) =>
      String(row?.venue ?? row?.id ?? row?.slug ?? row?.key ?? row?.code ?? row?.name ?? "")
        .trim()
        .toLowerCase();

    const isEnabled = (row) => {
      const id = getId(row);
      if (id && Object.prototype.hasOwnProperty.call(venueOverrides || {}, id)) return !!venueOverrides[id];
      return row?.enabled !== false;
    };

    return raw.filter((v) => isEnabled(v));
  }, [venuesRaw, venueOverrides]);

  // Venue selector should only show *currently enabled* venues (after UI-local overrides),
  // while the Manage popup can still list all venues (enabled + disabled).
  const supportedVenuesForSelector = useMemo(() => {
    return normalizeVenueList(venuesEnabled);
  }, [venuesEnabled]);

  const tradingVenues = useMemo(() => venuesEnabled.filter((v) => !!v?.supports?.trading), [venuesEnabled]);
  const orderbookVenues = useMemo(() => venuesEnabled.filter((v) => !!v?.supports?.orderbook), [venuesEnabled]);
  const balancesVenues = useMemo(() => venuesEnabled.filter((v) => !!v?.supports?.balances), [venuesEnabled]);

  const tradingVenuesList = useMemo(() => normalizeVenueList(tradingVenues), [tradingVenues]);
  const orderbookVenuesList = useMemo(() => normalizeVenueList(orderbookVenues), [orderbookVenues]);
  const balancesVenuesList = useMemo(() => normalizeVenueList(balancesVenues), [balancesVenues]);

  useEffect(() => {
    let alive = true;

    (async () => {
      try {
        setLoadingSupportedVenues(true);

        const res = await getVenuesRawSafe({ include_disabled: true });
        if (!alive) return;

        const raw =
          Array.isArray(res)
            ? res
            : Array.isArray(res?.venues)
            ? res.venues
            : Array.isArray(res?.items)
            ? res.items
            : Array.isArray(res?.supported_venues)
            ? res.supported_venues
            : [];

        setVenuesRaw(raw);
        setVenuesLoaded(true);

        const getId = (row) =>


          String(row?.venue ?? row?.id ?? row?.slug ?? row?.key ?? row?.code ?? row?.name ?? "")


            .trim()


            .toLowerCase();


        const isEnabled = (row) => {


          const id = getId(row);


          if (id && Object.prototype.hasOwnProperty.call(venueOverrides || {}, id)) return !!venueOverrides[id];


          return row?.enabled !== false;


        };


        const enabled = raw.filter((v) => isEnabled(v));
        const enabledIds = normalizeVenueList(enabled);

        const merged = normalizeVenueList([...(DEFAULT_SUPPORTED_VENUES || []), ...(ARB_VENUES || []), ...(enabledIds || [])]);

        if (merged.length > 0) setSupportedVenues(merged);
      } catch {
        setVenuesLoaded(true);
      } finally {
        if (alive) setLoadingSupportedVenues(false);
      }
    })();

    return () => {
      alive = false;
    };

  // When UI venue overrides change, recompute supportedVenues without refetching the registry.
  useEffect(() => {
    if (!venuesLoaded) return;

    const raw = Array.isArray(venuesRaw) ? venuesRaw : [];
    const getId = (row) =>
      String(row?.venue ?? row?.id ?? row?.slug ?? row?.key ?? row?.code ?? row?.name ?? "")
        .trim()
        .toLowerCase();
    const isEnabled = (row) => {
      const id = getId(row);
      if (id && Object.prototype.hasOwnProperty.call(venueOverrides || {}, id)) return !!venueOverrides[id];
      return row?.enabled !== false;
    };

    const enabled = raw.filter((v) => isEnabled(v));
    const enabledIds = normalizeVenueList(enabled);
    const merged = normalizeVenueList([...(DEFAULT_SUPPORTED_VENUES || []), ...(ARB_VENUES || []), ...(enabledIds || [])]);
    if (merged.length > 0) setSupportedVenues(merged);
  }, [venueOverrides, venuesRaw, venuesLoaded]);

  }, []);

  function getVenueLabelFromRegistry(value) {
    const v = String(value || "").trim().toLowerCase();
    if (!v) return "";

    const hit = (venuesRaw || []).find((x) => {
      const id = String(x?.venue ?? x?.id ?? x?.slug ?? x?.key ?? x?.code ?? x?.name ?? "").trim().toLowerCase();
      return id && id === v;
    });

    const label =
      hit?.display_name ??
      hit?.displayName ??
      hit?.label ??
      hit?.title ??
      hit?.name ??
      hit?.venue ??
      hit?.id ??
      "";

    const s = String(label || "").trim();
    return s || prettyVenueName(v);
  }

  const venuesById = useMemo(() => {
    const out = {};
    for (const row of Array.isArray(venuesRaw) ? venuesRaw : []) {
      const id = String(row?.venue ?? row?.id ?? row?.slug ?? row?.key ?? row?.code ?? row?.name ?? "")
        .trim()
        .toLowerCase();
      if (!id) continue;
      out[id] = row;
    }
    return out;
  }, [venuesRaw]);

  const venueSupports = useCallback(
    (venueId, cap) => {
      const v = String(venueId || "").trim().toLowerCase();
      const c = String(cap || "").trim().toLowerCase();
      if (!v || !c) return true;
      if (v === ALL_VENUES_VALUE.toLowerCase()) return true;

      const hasRegistry = venuesLoaded && Object.keys(venuesById || {}).length > 0;
      if (!hasRegistry) return true;

      const row = venuesById?.[v];
      if (!row) return true;

      if (row?.enabled === false) return false;

      // Solana DEX venues are swap-based, but we still surface them as trade/orderbook-capable
      // so the widgets mount (routing inside widgets is already Solana-gated).
      if (v.startsWith("solana_") && (c === "orderbook" || c === "trading")) return true;

      const sup = row?.supports ?? row?.capabilities ?? {};
      if (sup && typeof sup === "object" && Object.prototype.hasOwnProperty.call(sup, c)) return !!sup[c];

      const legacyKey = `supports_${c}`;
      if (Object.prototype.hasOwnProperty.call(row, legacyKey)) return !!row?.[legacyKey];

      return true;
    },
    [venuesById, venuesLoaded]
  );

  useEffect(() => {
    if (venue === ALL_VENUES_VALUE) return;

    const v = String(venue || "").trim().toLowerCase();
    if (!v) return;

    // While we're syncing from the URL, do not force venue fallback.
    if (routeSyncRef.current) return;

    // If the URL encodes a venue that isn't loaded yet, allow the route-sync logic to reconcile.
    try {
      const parsed = parseMarketRoute(window.location.pathname);
      const urlVenue = parsed ? normalizeVenue(parsed.venue) : "";
      if (urlVenue && urlVenue === v) return;
    } catch {
      // ignore
    }

    if (supportedVenues.includes(v)) return;
    if (supportedVenues.length > 0) setVenue(supportedVenues[0]);
  }, [supportedVenues, venue, venuesLoaded]);

  const venueMarketsCacheRef = useRef({});

  const [pollEnabled, setPollEnabled] = useState(() => readBoolLS(LS_POLL_ENABLED, true));
  const [pollSeconds, setPollSeconds] = useState(() => readNumLS(LS_POLL_SECONDS, 300));

  useEffect(() => {
    localStorage.setItem(LS_POLL_ENABLED, JSON.stringify(!!pollEnabled));
  }, [pollEnabled]);

  useEffect(() => {
    const n = Math.max(3, Number(pollSeconds) || 300);
    localStorage.setItem(LS_POLL_SECONDS, String(n));
  }, [pollSeconds]);

  const [hideTableDataGlobal, setHideTableDataGlobal] = useState(false);

  const [marketInput, setMarketInput] = useState("");
  const [applyMarketToTab, setApplyMarketToTab] = useState(true);

  // Prevent feedback loops when we are *reading* the URL and setting state.
  const routeSyncRef = useRef(false);

  // Route->state sync helpers (avoid stale closures in popstate handlers).
  const supportedVenuesRef = useRef(supportedVenues);
  const venuesLoadedRef = useRef(venuesLoaded);
  const venueRef = useRef(venue);
  const marketInputRef = useRef(marketInput);
  const setActiveMarketRef = useRef(null);

  useEffect(() => { supportedVenuesRef.current = supportedVenues; }, [supportedVenues]);
  useEffect(() => { venuesLoadedRef.current = venuesLoaded; }, [venuesLoaded]);
  useEffect(() => { venueRef.current = venue; }, [venue]);
  useEffect(() => { marketInputRef.current = marketInput; }, [marketInput]);

  function setUrlForMarket(nextVenue, nextSymbol, { replace = false } = {}) {
    const v = normalizeVenue(nextVenue);
    const s = normalizeSymbolCanon(nextSymbol);

    const basePath = buildMarketRoute(v, s);
    const nextUrl = `${basePath}${window.location.search || ""}${window.location.hash || ""}`;
    const curUrl = `${window.location.pathname}${window.location.search || ""}${window.location.hash || ""}`;

    if (curUrl === nextUrl) return;

    const fn = replace ? window.history.replaceState : window.history.pushState;
    fn.call(window.history, null, "", nextUrl);
  }

  // Wrapper so changing the Venue dropdown also updates the URL (using current Market, if present)
  function setVenueFromHeader(nextVenue) {
    setVenue(nextVenue);

    // If we are currently syncing *from* the URL, do not write back into it.
    if (routeSyncRef.current) return;

    const sym = String(marketInput || "").trim().toUpperCase();
    // Keep URL in sync even if sym is empty (then route becomes /market/:venue)
    setUrlForMarket(nextVenue, sym, { replace: true });
  }

  // Picks a safe venue string for URL routes.
  // - Never writes ALL/unknown venues into the URL (because /market/:venue is validated against supportedVenues)
  // - Prefers an explicitly provided venue if valid; otherwise falls back to current venue; otherwise first supported venue.
  function pickUrlVenue(preferredVenue) {
    const cand = normalizeVenue(preferredVenue);
    if (cand && Array.isArray(supportedVenues) && supportedVenues.includes(cand)) return cand;

    const cur = normalizeVenue(venue);
    const allNorm = normalizeVenue(ALL_VENUES_VALUE);
    if (cur && cur !== allNorm && Array.isArray(supportedVenues) && supportedVenues.includes(cur)) return cur;

    return Array.isArray(supportedVenues) && supportedVenues[0] ? supportedVenues[0] : "gemini";
  }

  useEffect(() => {
    // Keep App state in sync with the browser URL (including back/forward).
    // This must tolerate venues loading after initial render and avoid stale closures.

    const applyFromLocation = (pathname) => {
      const parsed = parseMarketRoute(pathname);
      if (!parsed) return;

      const nextVenue = normalizeVenue(parsed.venue);
      const nextSymbol = normalizeSymbolCanon(parsed.symbol);

      const sup = Array.isArray(supportedVenuesRef.current) ? supportedVenuesRef.current : [];
      const loaded = !!venuesLoadedRef.current;

      // While venues are loading, we allow the URL venue even if we can't validate it yet.
      // After venues load, only accept venues that exist in supportedVenues.
      const venueOk = !!nextVenue && (!loaded || sup.length === 0 || sup.includes(nextVenue));

      routeSyncRef.current = true;
      try {
        if (nextVenue && venueOk) setVenue(nextVenue);
        else if (nextVenue && !loaded) setVenue(nextVenue);

        if (nextSymbol) setMarketInput(nextSymbol);

        // Apply market immediately when both pieces exist and venue is usable.
        if (nextSymbol) {
          const vToUse = venueOk
            ? nextVenue
            : (normalizeVenue(venueRef.current) || (sup[0] || "gemini"));

          // Use ref to avoid stale setActiveMarket closures.
          const fn = setActiveMarketRef.current;
          if (typeof fn === "function") {
            fn(nextSymbol, vToUse, { applyToTab: true, setVenueIfValid: false });
          }
        }
      } finally {
        // Release on next tick to avoid immediate write-back while React is settling state.
        setTimeout(() => {
          routeSyncRef.current = false;
        }, 0);
      }
    };

    // Initial load
    applyFromLocation(window.location.pathname);

    // Back/forward navigation
    const onPop = () => applyFromLocation(window.location.pathname);
    window.addEventListener("popstate", onPop);

    return () => window.removeEventListener("popstate", onPop);
  }, []);

  // If venues finish loading after initial mount, re-apply the current URL once
  // so the header fields always reflect the browser location.
  useEffect(() => {
    if (!venuesLoaded) return;
    if (routeSyncRef.current) return;
    try {
      // Re-run in a guarded sync block to avoid URL write-back.
      routeSyncRef.current = true;
      const parsed = parseMarketRoute(window.location.pathname);
      if (!parsed) return;
      const nextVenue = normalizeVenue(parsed.venue);
      const nextSymbol = normalizeSymbolCanon(parsed.symbol);
      if (nextVenue && supportedVenues.includes(nextVenue)) setVenue(nextVenue);
      if (nextSymbol) setMarketInput(nextSymbol);
    } finally {
      setTimeout(() => {
        routeSyncRef.current = false;
      }, 0);
    }
  }, [venuesLoaded, supportedVenues]);


  const balancesReqIdRef = useRef(0);
  const pollInFlightRef = useRef(false);
  const portfolioPollInFlightRef = useRef(false);
  const balancesRefreshInFlightRef = useRef(false);

  // FIX: prevent sync overlap + per-venue backoff state (coinbase “too many errors”)
  const syncInFlightRef = useRef(false);
  const venueBackoffRef = useRef({}); // { [venue]: { until: number, reason: string } }
  const ledgerSyncInFlightRef = useRef(false);

  const [visible, setVisible] = useState(() => {
    const saved = safeParseJson(localStorage.getItem(LS_VISIBLE_WIDGETS) || "");
    if (saved && typeof saved === "object") return { ...DEFAULT_VISIBLE, ...saved };
    return { ...DEFAULT_VISIBLE };
  });

  useEffect(() => {
    localStorage.setItem(LS_VISIBLE_WIDGETS, JSON.stringify(visible));
  }, [visible]);

  const [armStatus, setArmStatus] = useState({ dry_run: null, armed: null });
  const [loadingArm, setLoadingArm] = useState(false);

  function mask(value) {
    if (!hideTableDataGlobal) return value;
    return "••••";
  }

  function maskMaybe(value) {
    if (!hideTableDataGlobal) return value;
    return "••••";
  }

  const hideVenueNames = hideTableDataGlobal;

  function labelVenueOption(value) {
    if (hideVenueNames) return "••••";
    if (value === ALL_VENUES_VALUE) return "All Venues";

    const regName = getVenueLabelFromRegistry(value);
    if (regName) return regName;

    if (value === "gemini") return "Gemini";
    if (value === "coinbase") return "Coinbase";
    if (value === "kraken") return "Kraken";
    if (value === "robinhood") return "Robinhood";
    if (value === "dex_trade") return "Dex-Trade";
    return String(value || "");
  }

  async function loadArmStatus() {
    try {
      const j = await getSafetyStatus();
      setArmStatus({
        dry_run: !!j?.dry_run,
        armed: !!j?.armed,
      });
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error loading /api/arm";
      setError(String(msg));
      setArmStatus({ dry_run: null, armed: null });
    }
  }

  async function doSetArmed(nextArmed) {
    try {
      setLoadingArm(true);
      setError(null);

      const j = await apiSetArmed(!!nextArmed);
      setArmStatus({
        dry_run: !!j?.dry_run,
        armed: !!j?.armed,
      });
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error setting armed state";
      setError(String(msg));
      try {
        await loadArmStatus();
      } catch {
        // ignore
      }
    } finally {
      setLoadingArm(false);
    }
  }

  useEffect(() => {
    loadArmStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ─────────────────────────────────────────────────────────────
  // Full-UI Screenshot Capture (tab/screen capture)
  // ─────────────────────────────────────────────────────────────
  const [shotBusy, setShotBusy] = useState(false);

  async function captureFullUiScreenshot() {
    if (shotBusy) return;

    if (!navigator?.mediaDevices?.getDisplayMedia) {
      setError("Screenshot capture is not supported in this browser (getDisplayMedia unavailable).");
      return;
    }

    setShotBusy(true);

    let stream = null;
    try {
      stream = await navigator.mediaDevices.getDisplayMedia({
        video: {
          displaySurface: "browser",
          preferCurrentTab: true,
          selfBrowserSurface: "include",
        },
        audio: false,
      });

      const track = stream.getVideoTracks()?.[0];
      if (!track) throw new Error("No video track returned from getDisplayMedia().");

      let bitmap = null;
      if (typeof window !== "undefined" && "ImageCapture" in window) {
        try {
          // eslint-disable-next-line no-undef
          const ic = new ImageCapture(track);
          bitmap = await ic.grabFrame();
        } catch {
          bitmap = null;
        }
      }

      const video = document.createElement("video");
      video.srcObject = stream;
      video.muted = true;
      video.playsInline = true;

      await new Promise((resolve) => {
        const done = () => resolve();
        video.onloadedmetadata = done;
        setTimeout(done, 150);
      });

      try {
        // eslint-disable-next-line no-await-in-loop
        await video.play();
      } catch {
        // ignore
      }

      const w = Number(bitmap?.width ?? video.videoWidth ?? 0);
      const h = Number(bitmap?.height ?? video.videoHeight ?? 0);

      if (!w || !h) throw new Error("Could not determine capture dimensions (videoWidth/videoHeight are 0).");

      const canvas = document.createElement("canvas");
      canvas.width = w;
      canvas.height = h;

      const ctx = canvas.getContext("2d");
      if (!ctx) throw new Error("Could not create 2D canvas context.");

      if (bitmap) ctx.drawImage(bitmap, 0, 0);
      else ctx.drawImage(video, 0, 0, w, h);

      const blob = await new Promise((resolve) => canvas.toBlob(resolve, "image/png"));
      if (!blob) throw new Error("Failed to encode PNG blob.");

      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      const ts = new Date().toISOString().replace(/[:.]/g, "-");
      a.href = url;
      a.download = `utt-screenshot-${ts}.png`;
      document.body.appendChild(a);
      a.click();
      a.remove();

      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } catch (e) {
      const msg = e?.message || "Unknown screenshot error";
      if (!String(msg).toLowerCase().includes("cancel")) {
        setError(`Screenshot failed: ${msg}`);
      }
    } finally {
      try {
        if (stream) {
          for (const t of stream.getTracks()) t.stop();
        }
      } catch {
        // ignore
      }
      setShotBusy(false);
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Venue markets (Balances hover -> market list)
  // ─────────────────────────────────────────────────────────────
  async function getVenueMarkets({ venue: venueIn, asset } = {}) {
    const vv = normalizeVenue(venueIn);
    const a = String(asset || "").trim().toUpperCase();
    if (!vv || !a) return [];

    const cacheKey = `${vv}|${a}`;
    const now = Date.now();
    const cached = venueMarketsCacheRef.current?.[cacheKey];
    if (cached && Number.isFinite(cached.ts) && now - cached.ts < VENUE_MARKETS_CACHE_TTL_MS && Array.isArray(cached.items)) {
      return cached.items;
    }

    try {
      const qs = new URLSearchParams({ venue: vv, asset: a }).toString();
      const url = `${API_BASE}/api/market/venue_markets?${qs}`;

      const r = await fetch(url, { method: "GET", headers: { Accept: "application/json" }, cache: "no-store" });

      if (!r.ok) {
        venueMarketsCacheRef.current = { ...(venueMarketsCacheRef.current || {}), [cacheKey]: { ts: now, items: [] } };
        return [];
      }

      const j = await r.json();
      const items = normalizeVenueMarketsResponse(vv, a, j);

      venueMarketsCacheRef.current = { ...(venueMarketsCacheRef.current || {}), [cacheKey]: { ts: now, items } };
      return items;
    } catch {
      venueMarketsCacheRef.current = { ...(venueMarketsCacheRef.current || {}), [cacheKey]: { ts: now, items: [] } };
      return [];
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Balances
  // ─────────────────────────────────────────────────────────────
  const [balances, setBalances] = useState([]);
  const [portfolioTotalUsd, setPortfolioTotalUsd] = useState(null);
  const [portfolioTotalUsdAllVenues, setPortfolioTotalUsdAllVenues] = useState(null);
  const [portfolioAllVenuesUpdatedAt, setPortfolioAllVenuesUpdatedAt] = useState(null);
  const [loadingBalances, setLoadingBalances] = useState(false);

  const [balSortKey, setBalSortKey] = useState("total_usd");
  const [balSortDir, setBalSortDir] = useState("desc");

  const [hideZeroBalances, setHideZeroBalances] = useState(false);
  const [hideBalancesView, setHideBalancesView] = useState(false);

  useEffect(() => {
    if (tab !== "balances") return;
    if (balSortKey !== "total_usd" || balSortDir !== "desc") {
      setBalSortKey("total_usd");
      setBalSortDir("desc");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  const balancesSorted = useMemo(() => {
    const dir = balSortDir === "asc" ? 1 : -1;
    const key = balSortKey;

    const EPS = 1e-12;
    const filtered = hideZeroBalances ? (balances || []).filter((b) => Math.abs(Number(b?.total ?? 0)) > EPS) : [...(balances || [])];

    const copy = [...filtered];

    const missingSentinel = dir === 1 ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;

    copy.sort((a, b) => {
      if (key === "asset") {
        return String(a.asset || "").localeCompare(String(b.asset || "")) * dir;
      }

      let av = Number(a?.[key]);
      let bv = Number(b?.[key]);

      if (!Number.isFinite(av)) av = missingSentinel;
      if (!Number.isFinite(bv)) bv = missingSentinel;

      if (av === bv) return String(a.asset || "").localeCompare(String(b.asset || ""));
      return (av - bv) * dir;
    });

    return copy;
  }, [balances, balSortKey, balSortDir, hideZeroBalances]);

  function toggleBalanceSort(key) {
    if (balSortKey === key) {
      setBalSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setBalSortKey(key);
      setBalSortDir(key === "asset" ? "asc" : "desc");
    }
  }

  async function doRefreshBalances({ forceAllVenuesTotal = true } = {}) {
    if (balancesRefreshInFlightRef.current) return;
    balancesRefreshInFlightRef.current = true;

    const reqId = ++balancesReqIdRef.current;

    const isAllVenuesView = venue === ALL_VENUES_VALUE;

    if (!isAllVenuesView) {
      const vNorm = normalizeVenue(venue);
      if (vNorm && !venueSupports(vNorm, "balances")) {
        setError(`Balances are not supported for venue "${prettyVenueName(vNorm)}". Select a balances-capable venue or use ALL.`);
        balancesRefreshInFlightRef.current = false;
        return;
      }
    }

    // Some venue-like sources are valid for markets/activity but are not balances-refreshable on the backend.
    // If they slip into the ALL candidate list, /api/balances/refresh will 422 and poison the fan-out.
    const POISON_BALANCES_REFRESH_VENUES = new Set(["solana_dex", "solana_jupiter"]);
    const filterBalancesRefreshVenues = (arr) =>
      (arr || []).filter((v) => {
        const vNorm = normalizeVenue(v);
        if (!vNorm) return false;
        if (vNorm === ALL_VENUES_VALUE) return false;
        if (POISON_BALANCES_REFRESH_VENUES.has(vNorm)) return false;
        return venueSupports(vNorm, "balances");
      });

    const balancesVenueCandidates = filterBalancesRefreshVenues(
      balancesVenuesList.length > 0 ? balancesVenuesList : supportedVenues
    );
    const venuesToUse = isAllVenuesView ? balancesVenueCandidates : [venue];

    try {
      setLoadingBalances(true);
      setError(null);

      if (isAllVenuesView) {
        for (const v of venuesToUse) {
          await refreshBalances(v);
        }
      } else {
        await refreshBalances(venue);
      }

      let baseItems = [];

      if (isAllVenuesView) {
        const perVenue = await Promise.all(
          venuesToUse.map(async (v) => {
            const res = await getLatestBalances({ venue: v, sort: "asset:asc" });
            const items = (res.items || []).map((b) => {
              const asset = String(b.asset || "").toUpperCase().trim();
              const total = Number(b.total ?? 0) || 0;
              const available = Number(b.available ?? 0) || 0;
              const hold = Number(b.hold ?? 0) || 0;

              return {
                ...b,
                venue: v,
                asset,
                total,
                available,
                hold,
                px_usd: b.px_usd ?? null,
                total_usd: b.total_usd ?? null,
                available_usd: b.available_usd ?? null,
                hold_usd: b.hold_usd ?? null,
                usd_source_symbol: b.usd_source_symbol ?? null,
              };
            });
            return items;
          })
        );

        baseItems = perVenue.flat();
      } else {
        const res = await getLatestBalances({ venue, sort: "asset:asc" });

        baseItems = (res.items || []).map((b) => {
          const asset = String(b.asset || "").toUpperCase().trim();
          const total = Number(b.total ?? 0) || 0;
          const available = Number(b.available ?? 0) || 0;
          const hold = Number(b.hold ?? 0) || 0;

          return {
            ...b,
            venue,
            asset,
            total,
            available,
            hold,
            px_usd: b.px_usd ?? null,
            total_usd: b.total_usd ?? null,
            available_usd: b.available_usd ?? null,
            hold_usd: b.hold_usd ?? null,
            usd_source_symbol: b.usd_source_symbol ?? null,
          };
        });
      }

      if (balancesReqIdRef.current !== reqId) return;

      setBalances(baseItems);

      if (baseItems.length === 0) {
        setPortfolioTotalUsd(0);
        if (isAllVenuesView) setPortfolioTotalUsdAllVenues(0);
        return;
      }

      const assetsByVenue = {};
      for (const b of baseItems) {
        const v = String(b.venue || "").toLowerCase().trim() || "";
        const a = String(b.asset || "").toUpperCase().trim();
        if (!v || !a) continue;
        if (!assetsByVenue[v]) assetsByVenue[v] = new Set();
        assetsByVenue[v].add(a);
      }

      const pxMapByVenue = {};

      for (const v of Object.keys(assetsByVenue)) {
        const assets = Array.from(assetsByVenue[v] || []);
        if (assets.length === 0) {
          pxMapByVenue[v] = {};
          continue;
        }

        try {
          const pxRes = await getPricesUSD({ venue: v, assets });
          if (balancesReqIdRef.current !== reqId) return;
          pxMapByVenue[v] = normalizePricesUsdResponse(pxRes);
        } catch {
          pxMapByVenue[v] = {};
        }
      }

      let unifiedPortCurrentView = 0;

      const merged = (baseItems || []).map((b) => {
        const v = String(b.venue || "").toLowerCase().trim();
        const pxMap = pxMapByVenue[v] || {};
        const asset = String(b.asset || "").toUpperCase().trim();

        let px = null;
        if (asset === "USD") px = 1.0;
        else if (asset === "USDT") px = 1.0;
        else {
          const val = pxMap?.[asset];
          px = Number.isFinite(Number(val)) ? Number(val) : null;
        }

        const usdSrc = asset === "USD" ? "USD" : asset === "USDT" ? "USDT≈USD" : px !== null ? `${asset}-USD` : null;

        const totalUsd = px !== null ? Number(b.total) * Number(px) : null;
        const availUsd = px !== null ? Number(b.available) * Number(px) : null;
        const holdUsd = px !== null ? Number(b.hold) * Number(px) : null;

        if (totalUsd !== null && Number.isFinite(totalUsd)) unifiedPortCurrentView += totalUsd;

        return { ...b, px_usd: px, total_usd: totalUsd, available_usd: availUsd, hold_usd: holdUsd, usd_source_symbol: usdSrc };
      });

      if (balancesReqIdRef.current !== reqId) return;

      setBalances(merged);
      setPortfolioTotalUsd(Number.isFinite(unifiedPortCurrentView) ? unifiedPortCurrentView : null);

      if (isAllVenuesView) {
        setPortfolioTotalUsdAllVenues(Number.isFinite(unifiedPortCurrentView) ? unifiedPortCurrentView : null);
      } else if (forceAllVenuesTotal) {
      const allVenueItemsNested = await Promise.all(
        balancesVenueCandidates.map(async (v) => {
          try {
            const res = await getLatestBalances({
              venue: v,
              sort: "asset:asc",
              with_prices: true,
            });

            return (res.items || []).map((b) => ({
              venue: v,
              asset: String(b.asset || "").toUpperCase().trim(),
              total: Number(b.total ?? 0) || 0,
              total_usd: Number(b.total_usd),
              price_usd: Number(b.price_usd),
            }));
          } catch {
            return [];
          }
        })
      );

      const allVenueItems = allVenueItemsNested.flat();

      let allTotal = 0;

      for (const it of allVenueItems) {
        const tu = Number(it?.total_usd);
        if (Number.isFinite(tu)) {
          allTotal += tu;
          continue;
        }

        const total = Number(it?.total);
        const px = Number(it?.price_usd);
        if (Number.isFinite(total) && Number.isFinite(px)) {
          allTotal += total * px;
        }
      }

      const finalTotal = Number.isFinite(allTotal) ? allTotal : null;
      setPortfolioTotalUsdAllVenues(finalTotal);
      setPortfolioAllVenuesUpdatedAt(Date.now());
      }
    } catch (e) {
      if (balancesReqIdRef.current !== reqId) throw e;
      const msg = e?.response?.data?.detail || e?.message || "Unknown error refreshing balances";
      setError(`Balances refresh failed: ${msg}`);
      throw e;
    } finally {
      if (balancesReqIdRef.current === reqId) setLoadingBalances(false);
      balancesRefreshInFlightRef.current = false;
    }
  }

  async function doRefreshBalancesSafe(opts) {
    try {
      await doRefreshBalances(opts);
    } catch {
      // swallow
    }
  }

  // Recompute ONLY the header “All Venues” total without depending on current tab/venue.
  async function refreshAllVenuesTotalOnlySafe() {
    try {
      const balancesVenueCandidates =
        balancesVenuesList && balancesVenuesList.length > 0
          ? balancesVenuesList
          : supportedVenues;

      // Pull latest balances WITH PRICES per venue (matches backend logs)
      const perVenue = await Promise.all(
        balancesVenueCandidates.map(async (v) => {
          try {
            const res = await getLatestBalances({
              venue: v,
              sort: "asset:asc",
              with_prices: true,
            });
            return res.items || [];
          } catch {
            return [];
          }
        })
      );

      const items = perVenue.flat();

      let totalUsd = 0;

      for (const b of items) {
        // Prefer backend-computed total_usd if present
        const tu = Number(b?.total_usd);
        if (Number.isFinite(tu)) {
          totalUsd += tu;
          continue;
        }

        // Fallback: total * price_usd
        const total = Number(b?.total);
        const px = Number(b?.price_usd);
        if (Number.isFinite(total) && Number.isFinite(px)) {
          totalUsd += total * px;
        }
      }

      const finalTotal = Number.isFinite(totalUsd) ? totalUsd : null;

      // Update timestamp even if dollar value doesn't change
      setPortfolioTotalUsdAllVenues(finalTotal);
      setPortfolioAllVenuesUpdatedAt(Date.now());
    } catch {
      // best-effort: do not surface background errors
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Local Orders
  // ─────────────────────────────────────────────────────────────
  const [orders, setOrders] = useState([]);
  const [loadingOrders, setLoadingOrders] = useState(false);
  const [statusFilter, setStatusFilter] = useState("");
  const [localSymbolFilter, setLocalSymbolFilter] = useState("");

  const [hideCancelledLocal, setHideCancelledLocal] = useState(() => readBoolLS(LS_HIDE_CANCELLED_LOCAL, true));
  useEffect(() => {
    localStorage.setItem(LS_HIDE_CANCELLED_LOCAL, JSON.stringify(!!hideCancelledLocal));
  }, [hideCancelledLocal]);

  const localOrdersVisible = useMemo(() => {
    const sym = String(localSymbolFilter || "").trim().toUpperCase();
    let out = [...(orders || [])];

    if (sym) out = out.filter((o) => String(o.symbol_canon || o.symbol || "").toUpperCase().includes(sym));

    const statusIsHidden = isHiddenByHideCancelled(statusFilter);
    if (hideCancelledLocal && !statusIsHidden) {
      out = out.filter((o) => !isHiddenByHideCancelled(o.status));
    }

    return out;
  }, [orders, localSymbolFilter, hideCancelledLocal, statusFilter]);

  async function doLoadOrders() {
    try {
      if (venue === ALL_VENUES_VALUE) {
        setOrders([]);
        setError("Local Orders requires a single venue. Please select a venue (not ALL).");
        return;
      }

      const vNorm = normalizeVenue(venue);
      if (vNorm && !venueSupports(vNorm, "trading")) {
        setOrders([]);
        setError(`Local Orders are not supported for venue "${prettyVenueName(vNorm)}". Select a trading-capable venue.`);
        return;
      }

      setLoadingOrders(true);
      setError(null);

      const res = await getOrders({
        venue,
        sort: "created_at:desc",
        status: statusFilter || undefined,
        page: 1,
        page_size: 200,
      });
      setOrders(res.items || []);
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error loading orders";
      setError(String(msg));
    } finally {
      setLoadingOrders(false);
    }
  }

  async function doCancelOrder(orderId) {
    try {
      setError(null);
      await cancelOrder(orderId);
      await doLoadOrders();
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error canceling order";
      setError(String(msg));
    }
  }

  async function doCancelUnifiedOrder(payload) {
    const cancel_ref = String(payload?.cancel_ref || payload?.cancelRef || payload?.row?.cancel_ref || payload?.row?.cancelRef || "").trim();

    if (!cancel_ref) {
      setError("Cannot cancel: missing cancel_ref on this unified order row.");
      return;
    }

    try {
      setError(null);
      await cancelOrderByRef(cancel_ref);
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error canceling unified order";
      setError(String(msg));
    }
  }

  // ─────────────────────────────────────────────────────────────
  // All Orders (Unified)
  // ─────────────────────────────────────────────────────────────
  const [allOrders, setAllOrders] = useState([]);
  const [allTotal, setAllTotal] = useState(0);
  const [loadingAll, setLoadingAll] = useState(false);

  const [aoScope, setAoScope] = useState("");
  const [aoVenue, setAoVenue] = useState("");
  const [aoStatusBucket, setAoStatusBucket] = useState("");
  const [aoSymbol, setAoSymbol] = useState("");
  const [aoSortField, setAoSortField] = useState("closed_at");
  const [aoSortDir, setAoSortDir] = useState("desc");
  const [aoPage, setAoPage] = useState(1);
  const [aoPageSize, setAoPageSize] = useState(50);

  const [hideCancelledUnified, setHideCancelledUnified] = useState(() => readBoolLS(LS_HIDE_CANCELLED_UNIFIED, false));
  useEffect(() => {
    localStorage.setItem(LS_HIDE_CANCELLED_UNIFIED, JSON.stringify(!!hideCancelledUnified));
  }, [hideCancelledUnified]);

  const aoSort = `${aoSortField}:${aoSortDir}`;

  function toggleAllSort(field) {
    if (aoSortField === field) setAoSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else {
      setAoSortField(field);
      setAoSortDir("desc");
    }
    setAoPage(1);
  }

  const allOrdersVisible = useMemo(() => {
    let out = [...(allOrders || [])];
    if (hideCancelledUnified) out = out.filter((o) => !isHiddenByHideCancelled(o.status));
    return out;
  }, [allOrders, hideCancelledUnified]);

  function parseMaybeTime(v) {
    if (v === null || v === undefined) return null;
    if (v instanceof Date) {
      const t = v.getTime();
      return Number.isFinite(t) ? t : null;
    }
    const s = String(v || "").trim();
    if (!s) return null;
    const t = Date.parse(s);
    return Number.isFinite(t) ? t : null;
  }

  function sortValueForField(o, field) {
    const f = String(field || "").trim();
    const raw = o?.[f];

    if (f.endsWith("_at") || f === "time" || f === "timestamp") {
      const t = parseMaybeTime(raw);
      return t;
    }

    const n = Number(raw);
    if (Number.isFinite(n)) return n;

    const s = raw === null || raw === undefined ? "" : String(raw);
    return s;
  }

  function compareBySort(a, b, field, dir) {
    const dAsc = String(dir || "desc").toLowerCase() === "asc";
    const d = dAsc ? 1 : -1;

    const av = sortValueForField(a, field);
    const bv = sortValueForField(b, field);

    const aMissing = av === null || av === undefined || av === "";
    const bMissing = bv === null || bv === undefined || bv === "";

    if (aMissing && bMissing) {
      const at = parseMaybeTime(a?.created_at) ?? 0;
      const bt = parseMaybeTime(b?.created_at) ?? 0;
      if (at !== bt) return (at - bt) * -1;
      const ak = String(a?.id ?? a?.order_id ?? a?.view_key ?? "");
      const bk = String(b?.id ?? b?.order_id ?? b?.view_key ?? "");
      return ak.localeCompare(bk);
    }
    if (aMissing && !bMissing) return 1;
    if (!aMissing && bMissing) return -1;

    if (typeof av === "number" && typeof bv === "number") {
      if (av === bv) return 0;
      return (av - bv) * d;
    }

    const as = String(av);
    const bs = String(bv);
    const c = as.localeCompare(bs);
    return c * d;
  }

  async function fetchAllOrdersOneBucket({ scopeNorm, venueVal, symbolVal, bucket, sort, page, pageSize }) {
    const params = { sort, page, page_size: pageSize };
    if (scopeNorm) params.scope = scopeNorm;
    if (venueVal) params.venue = venueVal;
    if (symbolVal) params.symbol = symbolVal;
    if (bucket === "open" || bucket === "terminal") params.status_bucket = bucket;
    return await getAllOrders(params);
  }

  async function doLoadAllOrders() {
    try {
      setLoadingAll(true);
      setError(null);

      const scopeNorm = normalizeScope(aoScope);

      const statusBucketNorm = (() => {
        const s = String(aoStatusBucket || "").trim().toLowerCase();
        if (s === "open" || s === "terminal") return s;
        return "";
      })();

      const venueVal = String(aoVenue || "").trim();
      const symbolVal = String(aoSymbol || "").trim();

      if (!statusBucketNorm) {
        const requested = Math.max(1, Number(aoPage) || 1);
        const pageSize = Math.max(1, Number(aoPageSize) || 50);
        const need = requested * pageSize;

        const MAX_FETCH = 5000;
        const fetchSize = Math.min(MAX_FETCH, need);

        const [openRes, termRes] = await Promise.all([
          fetchAllOrdersOneBucket({ scopeNorm, venueVal: venueVal || "", symbolVal: symbolVal || "", bucket: "open", sort: aoSort, page: 1, pageSize: fetchSize }),
          fetchAllOrdersOneBucket({
            scopeNorm,
            venueVal: venueVal || "",
            symbolVal: symbolVal || "",
            bucket: "terminal",
            sort: aoSort,
            page: 1,
            pageSize: fetchSize,
          }),
        ]);

        const openItems = Array.isArray(openRes?.items) ? openRes.items : [];
        const termItems = Array.isArray(termRes?.items) ? termRes.items : [];

        const merged = [...openItems, ...termItems];

        merged.sort((a, b) => {
          const c = compareBySort(a, b, aoSortField, aoSortDir);
          if (c !== 0) return c;

          const at = parseMaybeTime(a?.created_at) ?? 0;
          const bt = parseMaybeTime(b?.created_at) ?? 0;
          if (at !== bt) return (at - bt) * -1;

          const ak = String(a?.id ?? a?.order_id ?? a?.view_key ?? "");
          const bk = String(b?.id ?? b?.order_id ?? b?.view_key ?? "");
          return ak.localeCompare(bk);
        });

        const start = (requested - 1) * pageSize;
        const pageItems = merged.slice(start, start + pageSize);

        const openTotalNum = Number(openRes?.total);
        const termTotalNum = Number(termRes?.total);

        const total =
          (Number.isFinite(openTotalNum) ? openTotalNum : openItems.length) +
          (Number.isFinite(termTotalNum) ? termTotalNum : termItems.length);

        setAllOrders(pageItems);
        setAllTotal(Number.isFinite(total) ? total : pageItems.length);
        return;
      }

      const res = await fetchAllOrdersOneBucket({
        scopeNorm,
        venueVal: venueVal || "",
        symbolVal: symbolVal || "",
        bucket: statusBucketNorm,
        sort: aoSort,
        page: aoPage,
        pageSize: aoPageSize,
      });

      const items = Array.isArray(res?.items) ? res.items : [];
      const totalNum = Number(res?.total);

      setAllOrders(items);
      setAllTotal(Number.isFinite(totalNum) ? totalNum : items.length);
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error loading all_orders";
      setError(String(msg));
    } finally {
      setLoadingAll(false);
    }
  }

  // UPDATED: “attempt all venues even if one fails” + coinbase backoff + no overlap
  async function doSyncAndLoadAllOrders(opts = {}) {
    // avoid overlapping syncs (button spam / poll overlap)
    if (syncInFlightRef.current) return;
    syncInFlightRef.current = true;

    try {
      setLoadingAll(true);
      setError(null);

      const scopeNorm = normalizeScope(aoScope);

      // LOCAL scope does not require venue refresh.
      if (scopeNorm === "LOCAL") {
        await doLoadAllOrders();
        return;
      }

      const force = !!opts?.force;

      // Prefer caller-provided venue; fallback to UI aoVenue only if caller didn't provide one.
      const vRaw = opts?.venue ?? aoVenue ?? "";
      const v = String(vRaw || "").trim().toLowerCase();

      // Treat "", null, "all", and ALL_VENUES_VALUE as "all venues"
      const wantsAllVenues = !v || v === "all" || v === String(ALL_VENUES_VALUE || "").toLowerCase();

      const refreshCandidates = tradingVenuesList.length > 0 ? tradingVenuesList : supportedVenues;

      // If a specific venue was requested and is valid, refresh only that one. Otherwise refresh all candidates.
      const venuesToRefresh = wantsAllVenues ? refreshCandidates : refreshCandidates.includes(v) ? [v] : refreshCandidates;

      const now = Date.now();
      const errors = [];

      const isBackedOff = (vv) => {
        const b = venueBackoffRef.current?.[vv];
        return b && Number.isFinite(b.until) && b.until > now;
      };

      const setBackoff = (vv, ms, reason) => {
        venueBackoffRef.current = {
          ...(venueBackoffRef.current || {}),
          [vv]: { until: Date.now() + ms, reason: String(reason || "") },
        };
      };

      // Call refreshVenueOrders in a signature-tolerant way (boolean vs object vs no-arg force).
      const refreshOne = async (vv) => {
        if (!force) return await refreshVenueOrders(vv);

        // If force=true was requested, try compatible call patterns.
        try {
          return await refreshVenueOrders(vv, true);
        } catch (e1) {
          try {
            return await refreshVenueOrders(vv, { force: true });
          } catch (e2) {
            return await refreshVenueOrders(vv);
          }
        }
      };

      const safeRefreshOne = async (vv) => {
        if (isBackedOff(vv)) return;

        try {
          await refreshOne(vv);
        } catch (e) {
          const msg = e?.response?.data?.detail || e?.message || String(e || "unknown error");
          errors.push({ venue: vv, msg });

          // Recommended: if Coinbase is in “Too many errors” mode, back off for 15m
          if (vv === "coinbase" && String(msg).toLowerCase().includes("too many errors")) {
            setBackoff("coinbase", 15 * 60 * 1000, msg);
          }
        }
      };

      for (const vv of venuesToRefresh) {
        // eslint-disable-next-line no-await-in-loop
        await safeRefreshOne(vv);
      }

      await doLoadAllOrders();

      if (errors.length > 0) {
        const summary = errors.map((x) => `${x.venue}: ${x.msg}`).join("\n");
        setError(`Some venues failed to refresh:\n${summary}`);
      }
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error syncing venue orders";
      setError(String(msg));
      try {
        await doLoadAllOrders();
      } catch {
        // ignore
      }
    } finally {
      setLoadingAll(false);
      syncInFlightRef.current = false;
    }
  }


// Background: when the header “Background refresh” triggers All Orders Sync+Load,
// also run Ledger Sync (so tax/net-after-tax updates without manual clicks).
function clampLedgerLimit(n, fallback = 5000) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.min(5000, Math.max(1, Math.floor(x)));
}

function readStrLS(key, defaultVal) {
  const raw = localStorage.getItem(key);
  if (raw === null || raw === undefined) return defaultVal;
  return String(raw);
}

async function doLedgerSyncFromLocalStorage({ silent = true } = {}) {
  if (typeof window === "undefined") return null;
  if (ledgerSyncInFlightRef.current) return null;

  // Respect the same toggle/settings as TerminalTablesWidget.
  const enabled = readBoolLS(LS_AO_LEDGER_SYNC_ON_SYNCLOAD_KEY, true);
  if (!enabled) return null;

  const walletId = String(readStrLS(LS_AO_LEDGER_SYNC_WALLET_ID_KEY, "default") || "default").trim() || "default";
  const mode = String(readStrLS(LS_AO_LEDGER_SYNC_MODE_KEY, "ALL") || "ALL").trim() || "ALL";
  const limit = clampLedgerLimit(readNumLS(LS_AO_LEDGER_SYNC_LIMIT_KEY, 5000), 5000);
  const dryRun = readBoolLS(LS_AO_LEDGER_SYNC_DRY_RUN_KEY, false);

  const qs = new URLSearchParams({
    wallet_id: walletId,
    mode,
    limit: String(limit),
    dry_run: dryRun ? "true" : "false",
  }).toString();

  ledgerSyncInFlightRef.current = true;
  try {
    const res = await fetch(`/api/ledger/sync?${qs}`, { method: "POST" });

    let data = null;
    const ct = res.headers?.get?.("content-type") || "";
    if (ct.includes("application/json")) data = await res.json();
    else {
      const t = await res.text();
      data = t ? { detail: t } : {};
    }

    if (!res.ok) {
      const msg =
        (data && (data.detail || data.message || data.error)) ||
        `HTTP ${res.status} ${res.statusText}`;
      throw new Error(String(msg));
    }

    // Reload All Orders so computed fields (e.g., realized/tax/net-after-tax) populate.
    try {
      await doLoadAllOrders();
    } catch {
      // ignore
    }

    return data;
  } catch (e) {
    const msg = e?.message || String(e || "Ledger sync failed");
    if (!silent) setError(`Ledger sync failed: ${msg}`);
    else {
      // best-effort: background refresh should not hard-fail the UI
      try { console.warn("[UTT] ledger sync (background) failed:", msg); } catch {}
    }
    return null;
  } finally {
    ledgerSyncInFlightRef.current = false;
  }
}



  const aoPageSizeNum = Math.max(1, Number(aoPageSize) || 50);
  const aoTotalPages = Math.max(1, Math.ceil((Number(allTotal) || 0) / aoPageSizeNum));

  async function setViewedConfirmed(viewKey, confirmed) {
    const key = String(viewKey || "").trim();
    if (!key) return;

    const optimisticViewedAt = confirmed ? new Date().toISOString() : null;

    setAllOrders((prev) =>
      (prev || []).map((x) => (x.view_key !== key ? x : { ...x, viewed_confirmed: !!confirmed, viewed_at: optimisticViewedAt }))
    );

    try {
      setError(null);

      const j = await confirmOrderView({ view_key: key, viewed_confirmed: !!confirmed });
      const serverConfirmed = !!j?.viewed_confirmed;
      const serverViewedAt = j?.viewed_at ?? (serverConfirmed ? optimisticViewedAt : null);

      setAllOrders((prev) =>
        (prev || []).map((x) => (x.view_key !== key ? x : { ...x, viewed_confirmed: serverConfirmed, viewed_at: serverViewedAt }))
      );

      await doLoadAllOrders();
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error saving viewed flag";
      setError(String(msg));
      await doLoadAllOrders();
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Discover (Symbols discovery zone)
  // ─────────────────────────────────────────────────────────────
  const [discVenue, setDiscVenue] = useState(() => {
    const saved = safeParseJson(localStorage.getItem(LS_DISCOVER_VENUE) || "");
    return typeof saved === "string" && saved ? saved : "gemini";
  });

  const [discEps, setDiscEps] = useState(() => {
    const n = readNumLS(LS_DISCOVER_EPS, 1e-8);
    return Number.isFinite(n) && n > 0 ? n : 1e-8;
  });

  const [discNew, setDiscNew] = useState([]);
  const [discUnheld, setDiscUnheld] = useState([]);
  const [discMeta, setDiscMeta] = useState({ new_count: 0, unheld_count: 0, venue: null });
  const [loadingDiscover, setLoadingDiscover] = useState(false);

  const didInitDiscoverRef = useRef(false);

  useEffect(() => {
    localStorage.setItem(LS_DISCOVER_VENUE, JSON.stringify(String(discVenue || "gemini")));
  }, [discVenue]);

  useEffect(() => {
    const n = Number(discEps);
    if (Number.isFinite(n) && n > 0) localStorage.setItem(LS_DISCOVER_EPS, String(n));
  }, [discEps]);

  function effectiveDiscoverVenue() {
    const v = String(discVenue || "").trim().toLowerCase();
    if (v && supportedVenues.includes(v)) return v;

    const cur = venue === ALL_VENUES_VALUE ? supportedVenues[0] || "gemini" : String(venue || "").trim().toLowerCase();
    if (cur && supportedVenues.includes(cur)) return cur;

    return supportedVenues[0] || "gemini";
  }

  async function doLoadDiscover({ refreshFirst = false } = {}) {
    const v = effectiveDiscoverVenue();
    const eps = Number(discEps);
    const epsSafe = Number.isFinite(eps) && eps > 0 ? eps : 1e-8;

    try {
      setLoadingDiscover(true);
      setError(null);

      if (refreshFirst) {
        await refreshSymbols(v);
      }

      const [newRes, unheldRes] = await Promise.all([getNewSymbols({ venue: v }), getUnheldNewSymbols({ venue: v, eps: epsSafe })]);

      const newItems = Array.isArray(newRes?.items)
        ? newRes.items
        : Array.isArray(newRes?.new)
        ? newRes.new
        : Array.isArray(newRes?.symbols)
        ? newRes.symbols
        : Array.isArray(newRes)
        ? newRes
        : [];

      const unheldItems = Array.isArray(unheldRes?.items)
        ? unheldRes.items
        : Array.isArray(unheldRes?.unheld_new)
        ? unheldRes.unheld_new
        : Array.isArray(unheldRes?.symbols)
        ? unheldRes.symbols
        : Array.isArray(unheldRes)
        ? unheldRes
        : [];

      setDiscNew(newItems);
      setDiscUnheld(unheldItems);

      const nc = Number(newRes?.count ?? newItems.length);
      const uc = Number(unheldRes?.count ?? unheldItems.length);

      setDiscMeta({
        venue: v,
        new_count: Number.isFinite(nc) ? nc : newItems.length,
        unheld_count: Number.isFinite(uc) ? uc : unheldItems.length,
        baseline_captured_at: newRes?.baseline_captured_at ?? unheldRes?.baseline_captured_at ?? null,
        latest_captured_at: newRes?.latest_captured_at ?? unheldRes?.latest_captured_at ?? null,
      });
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Unknown error loading discovery lists";
      setError(String(msg));
    } finally {
      setLoadingDiscover(false);
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Discovery "viewed/verified" state (local cache + optional server persistence)
  // ─────────────────────────────────────────────────────────────
  const [discoveryViewedMap, setDiscoveryViewedMap] = useState(() => {
    const saved = safeParseJson(localStorage.getItem(LS_DISCOVERY_VIEWED_MAP) || "");
    if (saved && typeof saved === "object") return saved;
    return {};
  });

  useEffect(() => {
    localStorage.setItem(LS_DISCOVERY_VIEWED_MAP, JSON.stringify(discoveryViewedMap || {}));
  }, [discoveryViewedMap]);

  const symViewsLoadInFlightRef = useRef(false);
  const symViewsLoadedForVenueRef = useRef({});

  function isDiscoverySymbolViewed(v, symbolCanon) {
    const k = discoveryKey(v, symbolCanon);
    if (!k) return false;
    return !!discoveryViewedMap?.[k];
  }

  function setDiscoverySymbolViewedLocal(v, symbolCanon, viewed) {
    const k = discoveryKey(v, symbolCanon);
    if (!k) return;
    setDiscoveryViewedMap((prev) => {
      const next = { ...(prev || {}) };
      if (viewed) next[k] = true;
      else delete next[k];
      return next;
    });
  }

  async function persistDiscoverySymbolView(v, symbolCanon, viewed) {
    const view_key = discoveryViewKey(v, symbolCanon);
    if (!view_key) return;

    try {
      await confirmSymbolView({ view_key, viewed_confirmed: !!viewed });
    } catch (e) {
      const status = Number(e?.response?.status);
      if (status && status !== 404 && status !== 405) {
        // Non-fatal; keep silent
      }
    }
  }

  async function loadDiscoverySymbolViewsFromServer(v) {
    const vv = String(v || "").trim().toLowerCase();
    if (!vv) return;
    if (symViewsLoadInFlightRef.current) return;
    if (symViewsLoadedForVenueRef.current?.[vv]) return;

    symViewsLoadInFlightRef.current = true;

    try {
      const j = await listSymbolViews({ venue: vv });

      const items = Array.isArray(j?.items) ? j.items : Array.isArray(j?.views) ? j.views : Array.isArray(j) ? j : [];

      const nextPairs = [];

      for (const it of items) {
        if (typeof it === "string") {
          const parts = it.split(":");
          if (parts.length >= 2) {
            const venuePart = String(parts[0] || "").trim().toLowerCase();
            const symPart = String(parts.slice(1).join(":") || "").trim().toUpperCase();
            if (venuePart && symPart && venuePart === vv) nextPairs.push([venuePart, symPart, true]);
          }
          continue;
        }

        const key = String(it?.view_key || it?.key || "").trim();
        const confirmed = it?.viewed_confirmed ?? it?.confirmed ?? it?.viewed ?? true;
        if (!key) continue;

        const parts = key.split(":");
        if (parts.length >= 2) {
          const venuePart = String(parts[0] || "").trim().toLowerCase();
          const symPart = String(parts.slice(1).join(":") || "").trim().toUpperCase();
          if (venuePart && symPart && venuePart === vv) nextPairs.push([venuePart, symPart, !!confirmed]);
        }
      }

      if (nextPairs.length > 0) {
        setDiscoveryViewedMap((prev) => {
          const next = { ...(prev || {}) };
          for (const [venuePart, symPart, confirmed] of nextPairs) {
            const k = discoveryKey(venuePart, symPart);
            if (!k) continue;
            if (confirmed) next[k] = true;
            else delete next[k];
          }
          return next;
        });
      }

      symViewsLoadedForVenueRef.current = { ...(symViewsLoadedForVenueRef.current || {}), [vv]: true };
    } catch {
      // ignore
    } finally {
      symViewsLoadInFlightRef.current = false;
    }
  }

  function setDiscoverySymbolViewed(v, symbolCanon, viewed) {
    setDiscoverySymbolViewedLocal(v, symbolCanon, viewed);
    persistDiscoverySymbolView(v, symbolCanon, viewed);
  }

  function toggleDiscoverySymbolViewed(v, symbolCanon) {
    const cur = isDiscoverySymbolViewed(v, symbolCanon);
    setDiscoverySymbolViewed(v, symbolCanon, !cur);
  }

  useEffect(() => {
    if (tab !== "discover") return;

    if (!didInitDiscoverRef.current) {
      didInitDiscoverRef.current = true;
      const cur = venue === ALL_VENUES_VALUE ? supportedVenues[0] || "gemini" : String(venue || "").trim().toLowerCase();
      if (cur && supportedVenues.includes(cur)) setDiscVenue(cur);
      else setDiscVenue(supportedVenues[0] || "gemini");
    }

    const v = effectiveDiscoverVenue();
    doLoadDiscover({ refreshFirst: false }).finally(() => {
      loadDiscoverySymbolViewsFromServer(v);
    });

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  useEffect(() => {
    if (tab !== "discover") return;
    const v = effectiveDiscoverVenue();
    loadDiscoverySymbolViewsFromServer(v);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [discVenue, supportedVenues, tab]);

  // ─────────────────────────────────────────────────────────────
  // Order Book widget state
  // ─────────────────────────────────────────────────────────────
  const [obSymbol, setObSymbol] = useState("BTC-USD");
  const [obDepth, setObDepth] = useState(25);

  const selectedVenueNorm = normalizeVenue(venue);
  const isAllMode = venue === ALL_VENUES_VALUE;

  const canShowOrderBook = isAllMode ? orderbookVenuesList.length > 0 : venueSupports(selectedVenueNorm, "orderbook");
  const canShowOrderTicket = isAllMode ? tradingVenuesList.length > 0 : venueSupports(selectedVenueNorm, "trading");

  const fallbackObVenue = orderbookVenuesList[0] || supportedVenues[0] || "gemini";
  const fallbackTradeVenue = tradingVenuesList[0] || supportedVenues[0] || "gemini";
  const fallbackChartVenue = fallbackObVenue || "coinbase";

  const effectiveObVenue = isAllMode ? fallbackObVenue : selectedVenueNorm;
  const effectiveTradeVenue = isAllMode ? fallbackTradeVenue : selectedVenueNorm;

  const effectiveChartVenue = canShowOrderBook ? effectiveObVenue : fallbackChartVenue;

  const arbVenues = useMemo(() => {
    const sup = Array.isArray(supportedVenues) ? supportedVenues : [];
    const preferred = ARB_VENUES.filter((v) => sup.includes(v));
    return preferred.length > 0 ? preferred : sup;
  }, [supportedVenues]);

  // ─────────────────────────────────────────────────────────────
  // Order Ticket state
  // ─────────────────────────────────────────────────────────────
  const [otSymbol, setOtSymbol] = useState(obSymbol);
  const [otQty, setOtQty] = useState("");
  const [otLimitPrice, setOtLimitPrice] = useState("");

  useEffect(() => {
    setOtSymbol(String(obSymbol || "").trim());
  }, [obSymbol]);

  // ─────────────────────────────────────────────────────────────
  // Market/Venue selection unification
  // ─────────────────────────────────────────────────────────────
  function clearTabMarketFilter(targetTab) {
    const t = String(targetTab || tab || "").trim();
    if (t === "localOrders") {
      setLocalSymbolFilter("");
    } else if (t === "allOrders") {
      setAoSymbol("");
      setAoPage(1);
    }
  }

  function setActiveMarket(symbolCanon, venueCandidate, { applyToTab = true, setInput = true, setVenueIfValid = false } = {}) {
    const sym = normalizeSymbolCanon(symbolCanon);

    if (!sym) {
      if (setInput) setMarketInput("");
      if (applyToTab) clearTabMarketFilter(tab);
      return;
    }

    if (setInput) setMarketInput(sym);
    setObSymbol(sym);

    const v = String(venueCandidate || "").trim().toLowerCase();
    const vOk = v && supportedVenues.includes(v);

    if (setVenueIfValid && vOk) {
      if (String(venue || "").trim().toLowerCase() !== v) setVenue(v);
    }

    if (applyToTab && applyMarketToTab) {
      if (tab === "localOrders") setLocalSymbolFilter(sym);
      else if (tab === "allOrders") {
        setAoSymbol(sym);
        setAoPage(1);
      }
    }
  }

  // Keep a ref to the latest setActiveMarket so URL-driven sync (popstate) always uses current logic.
  setActiveMarketRef.current = setActiveMarket;

  function handlePickMarket(payload) {
    if (!payload || typeof payload !== "object") return;

    const sym = payload.symbolCanon ?? payload.symbol ?? payload.market ?? "";
    const v = payload.venue ?? payload.venueName ?? "";
    const applyToTab = payload.applyToTab ?? true;

    const rawOpts = payload.opts && typeof payload.opts === "object" ? payload.opts : {};

    const hasVenue = String(v || "").trim().length > 0;
    const opts = {
      setInput: true,
      applyToTab,
      ...rawOpts,
      setVenueIfValid: rawOpts?.setVenueIfValid ?? hasVenue,
    };

    setActiveMarket(sym, v, opts);

    // Keep URL in sync when selecting a market via clicks elsewhere in the UI.
    if (!routeSyncRef.current) {
      const symCanon = normalizeSymbolCanon(sym);
      if (symCanon) setUrlForMarket(pickUrlVenue(v), symCanon, { replace: false });
    }
  }

  function handleClearMarketFilter(targetTab) {
    clearTabMarketFilter(targetTab);
  }

  function applyMarketSymbol() {
    const sym = (marketInput || "").trim().toUpperCase();
    if (!sym) return;

    setActiveMarket(sym, venue, { applyToTab: !!applyMarketToTab });

    // Update URL (unless we are currently applying state from the URL itself)
    if (!routeSyncRef.current) {
      setUrlForMarket(venue, sym, { replace: false });
    }
  }

  function applySymbolFromDiscover(sym) {
    if (sym && typeof sym === "object") {
      const s = sym.symbol_canon ?? sym.symbolCanon ?? sym.symbol ?? "";
      const v = sym.venue ?? "";
      if (String(s || "").trim()) {
        setActiveMarket(s, v, { applyToTab: true, setInput: true, setVenueIfValid: !!String(v || "").trim() });

        // Keep URL in sync when selecting a market from Discovery.
        if (!routeSyncRef.current) {
          const symCanon = normalizeSymbolCanon(s);
          if (symCanon) setUrlForMarket(pickUrlVenue(v), symCanon, { replace: false });
        }
      }
      return;
    }

    const s = String(sym || "").trim();
    if (!s) return;
    setActiveMarket(s, null, { applyToTab: true, setInput: true, setVenueIfValid: false });

    // Keep URL in sync when selecting a market from Discovery (venue omitted -> fall back).
    if (!routeSyncRef.current) {
      const symCanon = normalizeSymbolCanon(s);
      if (symCanon) setUrlForMarket(pickUrlVenue(null), symCanon, { replace: false });
    }
  }

  useEffect(() => {
    if (tab === "balances") doRefreshBalancesSafe();
    if (tab === "localOrders") doLoadOrders();
    if (tab === "allOrders") doLoadAllOrders();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [venue]);

  useEffect(() => {
    if (!pollEnabled) return;

    const ms = Math.max(3, Number(pollSeconds) || 10) * 1000;

    const tick = async () => {
      if (pollInFlightRef.current) return;
      pollInFlightRef.current = true;

      try {
        if (tab === "balances") await doRefreshBalancesSafe();
        else if (tab === "localOrders") {
          if (venue !== ALL_VENUES_VALUE) await doLoadOrders();
        } else if (tab === "allOrders") {
          const alreadySyncing = !!syncInFlightRef.current;
          await doSyncAndLoadAllOrders();
          // Only run ledger sync when we actually performed a sync (avoid racing an in-flight manual click/poll).
          if (!alreadySyncing) {
            await doLedgerSyncFromLocalStorage({ silent: true });
          }
        }
      } finally {
        pollInFlightRef.current = false;
      }
    };

    const t = setInterval(() => tick(), ms);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pollEnabled, pollSeconds, tab, venue, statusFilter, aoScope, aoVenue, aoStatusBucket, aoSymbol, aoSort, aoPage, aoPageSize]);

  // Background refresher: keep the header “Total Portfolio (All Venues)” updating
  // even when the user is not sitting on the Balances tab.
  useEffect(() => {
    if (!pollEnabled) return;

    // Align with pollSeconds, but never faster than 30s for safety.
    const ms = Math.max(30, Number(pollSeconds) || 300) * 1000;

    const tick = async () => {
      if (portfolioPollInFlightRef.current) return;
      portfolioPollInFlightRef.current = true;

      try {
        // If Balances tab is active, the normal balances poll covers totals.
        if (tab === "balances") return;

        // Keep background polling aligned with the manual ALL-venues refresh filtering.
        // These venue-like sources are not balances-refreshable on the backend.
        const POISON_BALANCES_REFRESH_VENUES = new Set(["solana_dex", "solana_jupiter"]);
        const balancesVenueCandidates = (balancesVenuesList.length > 0 ? balancesVenuesList : supportedVenues)
          .map((v) => normalizeVenue(v))
          .filter((v) => v && v !== ALL_VENUES_VALUE)
          .filter((v) => !POISON_BALANCES_REFRESH_VENUES.has(v))
          .filter((v) => venueSupports(v, "balances"));

        // Best-effort refresh snapshots for all balances-capable venues.
        for (const v of balancesVenueCandidates) {
          try {
            // eslint-disable-next-line no-await-in-loop
            await refreshBalances(v);
          } catch {
            // ignore per-venue
          }
        }

        await refreshAllVenuesTotalOnlySafe();
      } finally {
        portfolioPollInFlightRef.current = false;
      }
    };

    tick();
    const t = setInterval(tick, ms);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pollEnabled, pollSeconds, tab, supportedVenues, balancesVenuesList]);

  const btn = (disabled) => ({ ...styles.button, ...(disabled ? styles.buttonDisabled : {}) });

  const btnHeader = (disabled) => ({
    ...headerStyles.button,
    ...(disabled ? styles.buttonDisabled : {}),
  });

  const balancesColSpan = venue === ALL_VENUES_VALUE ? 8 : 7;

  const statusIsCanceledLocal = isHiddenByHideCancelled(statusFilter);

  const dryRunKnown = armStatus.dry_run !== null;
  const armedKnown = armStatus.armed !== null;
  const isDryRun = !!armStatus.dry_run;
  const isArmed = !!armStatus.armed;

  const armDisabled = loadingArm || (dryRunKnown && isDryRun) || !dryRunKnown;
  const disarmDisabled = loadingArm || !armedKnown;

  const headerAllVenuesTotalText = hideTableDataGlobal
    ? "••••"
    : portfolioTotalUsdAllVenues === null
      ? "—"
      : fmtUsd(portfolioTotalUsdAllVenues);

  const headerAllVenuesUpdText = hideTableDataGlobal
    ? ""
    : portfolioAllVenuesUpdatedAt
      ? ` (upd ${new Date(portfolioAllVenuesUpdatedAt).toLocaleTimeString()})`
      : "";

  const headerAllVenuesTotalTextWithUpd = `${headerAllVenuesTotalText}${headerAllVenuesUpdText}`;

  const widgetGatePanel = (title, detail) => (
    <div style={{ ...styles.container, paddingTop: 8, paddingBottom: 0 }}>
      <div style={styles.orderBookDock}>
        <div style={{ fontSize: 14, fontWeight: 800, marginBottom: 6 }}>{title}</div>
        <div style={{ fontSize: 12, opacity: 0.8 }}>{detail}</div>
      </div>
    </div>
  );

  return (
    <AppErrorBoundary>
      <div ref={appContainerRef} style={styles.page}>
      <div style={styles.container}>
        <div style={styles.appRow}>
          <AppHeader
            headerRef={headerRef}
            headerStyles={headerStyles}
            styles={styles}
            API_BASE={API_BASE}
            loadingSupportedVenues={loadingSupportedVenues}
            venuesLoaded={venuesLoaded}
            venue={venue}
            setVenue={setVenueFromHeader}
            supportedVenues={supportedVenuesForSelector}
            venuesRaw={venuesRaw}
            venueOverrides={venueOverrides}
            setVenueOverride={setVenueOverride}
            ALL_VENUES_VALUE={ALL_VENUES_VALUE}
            labelVenueOption={labelVenueOption}
            dryRunKnown={dryRunKnown}
            isDryRun={isDryRun}
            armedKnown={armedKnown}
            isArmed={isArmed}
            loadingArm={loadingArm}
            armDisabled={armDisabled}
            disarmDisabled={disarmDisabled}
            doSetArmed={doSetArmed}
            loadArmStatus={loadArmStatus}
            btnHeader={btnHeader}
            pollEnabled={pollEnabled}
            setPollEnabled={setPollEnabled}
            pollSeconds={pollSeconds}
            setPollSeconds={setPollSeconds}
            marketInput={marketInput}
            setMarketInput={setMarketInput}
            applyMarketSymbol={applyMarketSymbol}
            applyMarketToTab={applyMarketToTab}
            setApplyMarketToTab={setApplyMarketToTab}
            hideTableDataGlobal={hideTableDataGlobal}
            setHideTableDataGlobal={setHideTableDataGlobal}
            visible={visible}
            setVisible={setVisible}
            onResetWidgets={() => {
              localStorage.removeItem(LS_VISIBLE_WIDGETS);
              setVisible({ ...DEFAULT_VISIBLE });
            }}
            obSymbol={obSymbol}
            arbVenues={arbVenues}
            fmtPrice={fmtPrice}
            hideVenueNames={hideVenueNames}
            fetchArbSnapshot={({ apiBase, symbol, venues }) => getArbSnapshot({ apiBase, symbol, venues })}
            shotBusy={shotBusy}
            captureFullUiScreenshot={captureFullUiScreenshot}
            headerAllVenuesTotalText={headerAllVenuesTotalTextWithUpd}
            toolWindows={toolWindows}
            toggleToolWindow={toggleToolWindow}
            error={error}
          />
        </div>
      </div>

      {visible.chart && (
        <TradingViewChartWidget
          styles={styles}
          appContainerRef={appContainerRef}
          headerRef={headerRef}
          venue={effectiveChartVenue}
          symbolCanon={obSymbol}
          interval="15"
          hideVenueNames={hideVenueNames}
          visible={visible}
          setVisible={setVisible}
        />
      )}

      {visible.tables && (
        <TerminalTablesWidget
          styles={styles}
          appContainerRef={appContainerRef}
          headerRef={headerRef}
          showChart={!!visible.chart}
		  apiBase={API_BASE}
          hideTableDataGlobal={hideTableDataGlobal}
          hideVenueNames={hideVenueNames}
          mask={mask}
          maskMaybe={maskMaybe}
          venue={venue}
          ALL_VENUES_VALUE={ALL_VENUES_VALUE}
          tab={tab}
          setTab={setTab}
          balancesSorted={balancesSorted}
          balancesColSpan={balancesColSpan}
          loadingBalances={loadingBalances}
          doRefreshBalances={doRefreshBalances}
          hideBalancesView={hideBalancesView}
          setHideBalancesView={setHideBalancesView}
          hideZeroBalances={hideZeroBalances}
          setHideZeroBalances={setHideZeroBalances}
          toggleBalanceSort={toggleBalanceSort}
          balSortKey={balSortKey}
          balSortDir={balSortDir}
          portfolioTotalUsd={portfolioTotalUsd}
          fmtUsd={fmtUsd}
          fmtPxUsd={fmtPxUsd}
          orders={localOrdersVisible}
          loadingOrders={loadingOrders}
          doLoadOrders={doLoadOrders}
          doCancelOrder={doCancelOrder}
          doCancelUnifiedOrder={doCancelUnifiedOrder}
          statusFilter={statusFilter}
          setStatusFilter={setStatusFilter}
          localSymbolFilter={localSymbolFilter}
          setLocalSymbolFilter={setLocalSymbolFilter}
          hideCancelledLocal={hideCancelledLocal}
          setHideCancelledLocal={setHideCancelledLocal}
          statusIsCanceledLocal={statusIsCanceledLocal}
          isCancelableStatus={isCancelableStatus}
          allOrders={allOrdersVisible}
          allTotal={allTotal}
          loadingAll={loadingAll}
          doLoadAllOrders={doLoadAllOrders}
          doSyncAndLoadAllOrders={doSyncAndLoadAllOrders}
          aoSource={aoScope}
          setAoSource={setAoScope}
          aoVenue={aoVenue}
          setAoVenue={setAoVenue}
          aoStatusBucket={aoStatusBucket}
          setAoStatusBucket={setAoStatusBucket}
          aoSymbol={aoSymbol}
          setAoSymbol={setAoSymbol}
          aoSortField={aoSortField}
          aoSortDir={aoSortDir}
          aoSort={aoSort}
          toggleAllSort={toggleAllSort}
          aoPage={aoPage}
          setAoPage={setAoPage}
          aoPageSize={aoPageSize}
          setAoPageSize={setAoPageSize}
          aoTotalPages={aoTotalPages}
          hideCancelledUnified={hideCancelledUnified}
          setHideCancelledUnified={setHideCancelledUnified}
          setViewedConfirmed={setViewedConfirmed}
          fmtTime={fmtTime}
          fmtEco={fmtEco}
          calcGrossTotal={calcGrossTotal}
          calcFee={calcFee}
          calcNetTotal={calcNetTotal}
          isTerminalStatus={isTerminalStatus}
          isTerminalBucket={isTerminalBucket}
          SortHeader={SortHeader}
          btn={btn}
          isCanceledStatus={isCanceledStatus}
          discVenue={discVenue}
          setDiscVenue={setDiscVenue}
          discEps={discEps}
          setDiscEps={setDiscEps}
          discNew={discNew}
          discUnheld={discUnheld}
          discMeta={discMeta}
          loadingDiscover={loadingDiscover}
          doLoadDiscover={doLoadDiscover}
          applySymbolFromDiscover={applySymbolFromDiscover}
          supportedVenues={supportedVenues}
          isDiscoverySymbolViewed={isDiscoverySymbolViewed}
          setDiscoverySymbolViewed={setDiscoverySymbolViewed}
          toggleDiscoverySymbolViewed={toggleDiscoverySymbolViewed}
          onPickMarket={handlePickMarket}
          onClearMarketFilter={handleClearMarketFilter}
          getVenueMarkets={getVenueMarkets}
          tradingVenues={tradingVenuesList}
          orderbookVenues={orderbookVenuesList}
          balancesVenues={balancesVenuesList}
        />
      )}

      {visible.orderBook &&
        (canShowOrderBook ? (
          <OrderBookWidget
            apiBase={API_BASE}
            effectiveVenue={effectiveObVenue}
            fmtNum={fmtNum}
            styles={styles}
            obSymbol={obSymbol}
            setObSymbol={setObSymbol}
            obDepth={obDepth}
            setObDepth={setObDepth}
            appContainerRef={appContainerRef}
            hideVenueNames={hideVenueNames}
            onPickPrice={(px) => setOtLimitPrice(String(px))}
            onPickQty={(q) => setOtQty(String(q))}
            venues={orderbookVenuesList.length > 0 ? orderbookVenuesList : supportedVenues}
          />
        ) : (
          widgetGatePanel(
            "Order Book unavailable",
            isAllMode
              ? "No enabled venues currently advertise orderbook support in the registry."
              : `This venue does not support orderbook. Select a venue that supports orderbook, or switch to ALL.`
          )
        ))}

      {visible.orderTicket &&
        (canShowOrderTicket ? (
          <OrderTicketWidget
            apiBase={API_BASE}
            effectiveVenue={effectiveTradeVenue}
            fmtNum={fmtNum}
            styles={styles}
            otSymbol={otSymbol}
            setOtSymbol={setOtSymbol}
            appContainerRef={appContainerRef}
            hideVenueNames={hideVenueNames}
            hideTableData={hideTableDataGlobal}
            qty={otQty}
            setQty={setOtQty}
            limitPrice={otLimitPrice}
            setLimitPrice={setOtLimitPrice}
            venues={tradingVenuesList.length > 0 ? tradingVenuesList : supportedVenues}
          />
        ) : (
          widgetGatePanel(
            "Order Ticket unavailable",
            isAllMode
              ? "No enabled venues currently advertise trading support in the registry."
              : `This venue does not support trading. Select a venue that supports trading, or switch to ALL.`
          )
        ))}

      {/* Tool windows */}
      <WindowManager
        windows={(toolWindows || []).filter((w) => !!w)}
        onClosed={(wOrId) => {
          const id = typeof wOrId === "string" ? wOrId : wOrId?.id;
          if (id) closeToolWindow(id);
        }}
        renderWindow={(w) => {
          if (!w || !w.id) return null;

          const common = { apiBase: API_BASE };

          if (w.id === TOOL_IDS.arb) {
            return (
              <ArbWindow
                {...common}
                styles={styles}
                symbolCanon={obSymbol}
                venues={arbVenues}
                fmtPrice={fmtPrice}
                fetchArbSnapshot={({ apiBase, symbol, venues }) => getArbSnapshot({ apiBase, symbol, venues })}
              />
            );
          }

          if (w.id === TOOL_IDS.topGainers)
            return (
              <TopGainersWindow
                {...common}
                enabledVenues={supportedVenues}
                hideTableData={hideTableDataGlobal}
                initialVenueFilter={venue === ALL_VENUES_VALUE ? "" : venue}
              />
            );

          if (w.id === TOOL_IDS.marketCap)
            return (
              <MarketCapWindow
                {...common}
                enabledVenues={supportedVenues}
                hideTableData={hideTableDataGlobal}
                initialVenueFilter={venue === ALL_VENUES_VALUE ? "" : venue}
              />
            );

          if (w.id === TOOL_IDS.volume)
            return (
              <VolumeWindow
                {...common}
                enabledVenues={supportedVenues}
                hideTableData={hideTableDataGlobal}
                initialVenueFilter={venue === ALL_VENUES_VALUE ? "" : venue}
              />
            );



          if (w.id === TOOL_IDS.walletAddresses)
            return (
              <WalletAddressesWindow
                {...common}
                apiBase={API_BASE}
                hideTableData={hideTableDataGlobal}
                onClose={() => toggleToolWindow(TOOL_IDS.walletAddresses)}
              />
            );



          if (w.id === TOOL_IDS.deposits)
            return (
              <LedgerWindow
                {...common}
                styles={styles}
                hideTableDataGlobal={hideTableDataGlobal}
                hideVenueNames={hideVenueNames}
                onClose={() => closeToolWindow(TOOL_IDS.deposits)}
              />
            );

          return null;
        }}
      />
    </div>
    </AppErrorBoundary>
  );
}
