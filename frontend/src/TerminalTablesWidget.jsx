// frontend/src/TerminalTablesWidget.jsx
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { fmtQty, fmtPrice, fmtMoney, fmtFee, fmtBal, fmtNum } from "./lib/format";

// Auth (local token) — used to gate funds actions.
const UTT_AUTH_TOKEN_KEY = 'utt_auth_token_v1';
function getAuthToken() {
  try { return localStorage.getItem(UTT_AUTH_TOKEN_KEY) || ''; } catch { return ''; }
}


const LS_KEY = "utt_all_orders_columns_v1";
// One-time migration flag: ensure new All Orders columns are injected into existing custom layouts.
const LS_AO_COLS_MIG_V2 = "utt_all_orders_columns_mig_v2_tax_netaftertax_v1";

// Store ONLY y/h for tables; x/w are derived from app container each render (or chart when docked).
const LS_GEOM_KEY = "utt_tables_widget_geom_v2_yh";

// NEW: optional width override (allows horizontal collapse/resize)
const LS_GEOM_XW_KEY = "utt_tables_widget_geom_v3_xw";

const LS_LOCK_KEY = "utt_tables_widget_lock_v1";

// Docking (tables below chart)
const LS_DOCK_KEY = "utt_tables_widget_dock_v1";
const LS_DOCK_OFFSET_KEY = "utt_tables_widget_dock_offset_v1";
const LS_DOCK_OFFSET_X_KEY = "utt_tables_widget_dock_offset_x_v1";

// Definitive TradingViewChartWidget localStorage key for {x,y,w,h,locked}
const CHART_GEOM_KEY = "utt_tv_chart_geom_v1";

// NEW: Column manager collapse state
const LS_COLMGR_OPEN_KEY = "utt_all_orders_colmgr_open_v1";

// NEW: Discovery filter (last X days)
const LS_DISC_DAYS_KEY = "utt_discovery_days_v1";

// NEW: Unified All Orders "Status" filter
const LS_AO_STATUS_FILTER_KEY = "utt_all_orders_status_filter_v1";

const LS_AO_FILL_SOUND_ENABLED_KEY = "utt_ao_fill_sound_enabled_v1";
const LS_AO_FILL_TOAST_ENABLED_KEY = "utt_ao_fill_toast_enabled_v1";
const LS_AO_FILL_SOUND_TYPE_KEY = "utt_ao_fill_sound_type_v1";
const LS_AO_FILL_SOUND_VOL_KEY = "utt_ao_fill_sound_vol_v1";


// NEW: All Orders tax withholding (Mode A: UI-only, localStorage)
const LS_AO_TAX_WITHHOLD_ENABLED_KEY = "utt_ao_tax_withhold_enabled_v1";
const LS_AO_TAX_FED_PCT_KEY = "utt_ao_tax_fed_pct_v1";
const LS_AO_TAX_STATE_PCT_KEY = "utt_ao_tax_state_pct_v1";
const LS_AO_TAX_ASSUME_NET_WHEN_UNKNOWN_KEY = "utt_ao_tax_assume_net_when_unknown_v1";
const LS_AO_TAX_WIN_X_KEY = "utt_ao_tax_win_x_v1";
const LS_AO_TAX_WIN_Y_KEY = "utt_ao_tax_win_y_v1";

// All Orders: Ledger lot-journal sync (manual / post-Sync+Load)
const LS_AO_LEDGER_SYNC_ON_SYNCLOAD_KEY = "utt_ao_ledger_sync_on_syncload_v1";
const LS_AO_LEDGER_SYNC_WALLET_ID_KEY = "utt_ao_ledger_sync_wallet_id_v1";
const LS_AO_LEDGER_SYNC_MODE_KEY = "utt_ao_ledger_sync_mode_v1";
const LS_AO_LEDGER_SYNC_LIMIT_KEY = "utt_ao_ledger_sync_limit_v1";
const LS_AO_LEDGER_SYNC_DRY_RUN_KEY = "utt_ao_ledger_sync_dry_run_v1";
// NEW: Theme / palette
const LS_THEME_KEY = "utt_tables_theme_v1";

// NEW: Custom theme palette payload
const LS_THEME_CUSTOM_KEY = "utt_tables_theme_custom_v1";

// NEW: Solana detected token suggestions (for Token Registry auto-fill)
const LS_SOLANA_DETECTED_TOKENS_KEY = "utt_solana_detected_tokens_v1";

// Discovery-enabled venues (UI allow-list)
// We only show venues that are BOTH supported by the app AND in this allow-list.
const DEFAULT_DISCOVERY_ALLOW_VENUES = ["gemini", "coinbase", "kraken", "robinhood", "dex_trade", "cryptocom"];
const COLS = {
  created: "created",
  closed: "closed",

  // Action column (row-level cancel)
  actions: "actions",

  viewed: "viewed",
  symbol: "symbol",
  side: "side",
  qty: "qty",
  gross: "gross",
  net: "net",
  tax: "tax",
  netAfterTax: "net_after_tax",
  fee: "fee",
  limit: "limit",
  status: "status",
  type: "type",
  source: "source",
  venue: "venue",
  bucket: "bucket",
};

// Requested placement: after Created/Closed
const PREFERRED_ORDER_V1 = [
  COLS.created,
  COLS.closed,
  COLS.actions,
  COLS.viewed,
  COLS.symbol,
  COLS.side,
  COLS.qty,
  COLS.gross,
  COLS.net,
  COLS.fee,
  COLS.limit,
  COLS.status,
  COLS.type,
  COLS.source,
  COLS.venue,
  COLS.bucket,
];

const PREFERRED_ORDER = [
  COLS.created,
  COLS.closed,
  COLS.actions,
  COLS.viewed,
  COLS.symbol,
  COLS.side,
  COLS.qty,
  COLS.gross,
  COLS.net,
  COLS.tax,
  COLS.netAfterTax,
  COLS.fee,
  COLS.limit,
  COLS.status,
  COLS.type,
  COLS.source,
  COLS.venue,
  COLS.bucket,
];

const LEGACY_ORDER_V1 = [
  COLS.created,
  COLS.actions,
  COLS.source,
  COLS.venue,
  COLS.symbol,
  COLS.side,
  COLS.type,
  COLS.qty,
  COLS.limit,
  COLS.status,
  COLS.bucket,
  COLS.closed,
  COLS.fee,
  COLS.gross,
  COLS.net,
  COLS.viewed,
];

const LEGACY_ORDER = [
  COLS.created,
  COLS.actions,
  COLS.source,
  COLS.venue,
  COLS.symbol,
  COLS.side,
  COLS.type,
  COLS.qty,
  COLS.limit,
  COLS.status,
  COLS.bucket,
  COLS.closed,
  COLS.fee,
  COLS.gross,
  COLS.net,
  COLS.tax,
  COLS.netAfterTax,
  COLS.viewed,
];

function safeParseJson(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function readBoolLS(key, fallback = false) {
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

function writeBoolLS(key, val) {
  try {
    localStorage.setItem(key, val ? "1" : "0");
  } catch {
    // ignore storage errors (private mode, full, etc.)
  }
}

function readStrLS(key, fallback = "") {
  try {
    const v = localStorage.getItem(key);
    if (v === null || v === undefined) return fallback;
    const s = String(v);
    return s.length ? s : fallback;
  } catch {
    return fallback;
  }
}

function writeStrLS(key, val) {
  try {
    localStorage.setItem(key, String(val ?? ""));
  } catch {
    // ignore
  }
}

function readNumLS(key, fallback = 0) {
  try {
    const v = localStorage.getItem(key);
    if (v === null || v === undefined) return fallback;
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  } catch {
    return fallback;
  }
}

function writeNumLS(key, val) {
  try {
    const n = Number(val);
    if (!Number.isFinite(n)) return;
    localStorage.setItem(key, String(n));
  } catch {
    // ignore
  }
}


function copyTextSafe(text) {
  const s = String(text || "").trim();
  if (!s) return;
  try { if (navigator?.clipboard?.writeText) { navigator.clipboard.writeText(s).catch(() => {}); return; } } catch {}
  try {
    const ta = document.createElement("textarea");
    ta.value = s; ta.setAttribute("readonly", "readonly");
    ta.style.position = "fixed"; ta.style.left = "-9999px";
    document.body.appendChild(ta); ta.select(); document.execCommand("copy"); document.body.removeChild(ta);
  } catch {}
}

function readJsonLS(key, fallback) {
  try { const raw = localStorage.getItem(key); const parsed = safeParseJson(raw || ""); return parsed && typeof parsed === "object" ? parsed : fallback; } catch { return fallback; }
}

function writeJsonLS(key, val) {
  try { localStorage.setItem(key, JSON.stringify(val)); } catch {}
}


function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function defaultYH() {
  // Back-compat: older localStorage may only contain {y,h}.
  // We persist x as well so the widget can float freely when undocked.
  return { x: 24, y: 160, h: 560 };
}

// NEW: width override state
function defaultXW() {
  return { w: null };
}

function numPx(v) {
  const n = parseFloat(String(v || "0").replace("px", ""));
  return Number.isFinite(n) ? n : 0;
}

function readChartGeomFromStorage() {
  const raw = localStorage.getItem(CHART_GEOM_KEY);
  const parsed = safeParseJson(raw || "");
  if (parsed && typeof parsed === "object") {
    const x = Number(parsed.x);
    const y = Number(parsed.y);
    const w = Number(parsed.w);
    const h = Number(parsed.h);

    const okYH = Number.isFinite(y) && Number.isFinite(h) && h > 0;
    const okXW = Number.isFinite(x) && Number.isFinite(w) && w > 0;

    if (okYH || okXW) return { x, y, w, h };
  }
  return null;
}

function pickSymbolCanon(row) {
  if (!row) return "";
  return row.symbol_canon || row.symbolCanon || row.symbol || row.canon || row.canonical || row.symbol_venue || "";
}

function pickVenueSymbol(row) {
  if (!row) return "";
  return row.symbol_venue || row.venue_symbol || row.symbolVenue || row.symbol || "";
}

function pickFirstSeen(row) {
  if (!row) return null;
  return row.first_seen_at || row.created_at || row.captured_at || row.detected_at || null;
}

// IMPORTANT: Unified orders can come from multiple sources and may not always use `status`.
// We normalize to a best-effort "display status".
function pickOrderStatus(row) {
  if (!row) return "";
  return (
    row.status ||
    row.order_status ||
    row.orderStatus ||
    row.state ||
    row.status_text ||
    row.statusText ||
    row.exchange_status ||
    row.exchangeStatus ||
    ""
  );
}

function normalizeStatus(s) {
  const t = String(s ?? "").trim();
  return t;
}

function normalizeStatusLower(s) {
  return String(s ?? "").trim().toLowerCase();
}

// Backend accepts open|terminal or omitted. Some older UI state used "all".
// Normalize all/ALL/" " to "" so the request can omit the param.
function normalizeStatusBucket(v) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s || s === "all") return "";
  if (s === "open") return "open";
  if (s === "terminal") return "terminal";
  return "";
}

function arraysEqual(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// REQUIRED FIX (B1): Always ensure Actions exists (back-compat with older saved column sets)
function sanitizeColumns(maybeCols) {
  const allowed = new Set(Object.values(COLS));
  const out = [];
  for (const c of Array.isArray(maybeCols) ? maybeCols : []) {
    const s = String(c || "").trim();
    if (!s || !allowed.has(s)) continue;
    if (!out.includes(s)) out.push(s);
  }

  const cols = out.length ? out : [...PREFERRED_ORDER];

  // Always ensure Actions exists (back-compat with older saved column sets)
  if (!cols.includes(COLS.actions)) {
    const closedIdx = cols.indexOf(COLS.closed);
    const insertAt = closedIdx >= 0 ? closedIdx + 1 : 0;
    cols.splice(insertAt, 0, COLS.actions);
  }

  return cols;
}

// NEW: parse status filter tokens (space/comma separated) → lowercased tokens
function parseStatusFilterTokens(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return [];
  // Split on commas and whitespace. Empty tokens removed.
  return s
    .split(/[\s,]+/g)
    .map((t) => t.trim().toLowerCase())
    .filter(Boolean);
}

/**
 * Balances → market symbol normalization.
 * Keeps this conservative and non-breaking:
 * - BTC/USD, BTC_USD, btcusd → BTC-USD
 * - If already BTC-USD, returns BTC-USD (uppercased)
 */
function normalizeMarketSymbolMaybe(sym) {
  const raw = String(sym ?? "").trim();
  if (!raw) return "";

  // Standard separators
  let s = raw.replaceAll("/", "-").replaceAll("_", "-").trim();
  s = s.toUpperCase();

  // Already canonical-ish
  if (s.includes("-")) return s;

  // Common concatenated forms: BTCUSD, ETHUSDT, DOGEUSDC, etc.
  // Try known quote suffixes (longer first).
  const QUOTES = ["USDT", "USDC", "USD", "EUR", "BTC", "ETH"];
  for (const q of QUOTES) {
    if (s.length > q.length && s.endsWith(q)) {
      const base = s.slice(0, -q.length);
      if (base) return `${base}-${q}`;
    }
  }

  // If no match, return as-is
  return s;
}

function inferBalanceMarketSymbol(b) {
  if (!b) return "";

  // Prefer backend-provided pricing symbol (should be the “right” one for USD valuation)
  const src = normalizeMarketSymbolMaybe(b.usd_source_symbol || "");
  if (src) return src;

  const asset = String(b.asset || "").trim().toUpperCase();
  if (!asset) return "";

  // Avoid silly links for pure USD rows
  if (asset === "USD") return "";

  // Fallback heuristic: ASSET-USD
  return `${asset}-USD`;
}

// Best-effort classification across venues; OR semantics based on substring checks.
function classifyStatusKind(statusLower, bucketMaybeLower) {
  const st = String(statusLower || "").trim().toLowerCase();
  const b = String(bucketMaybeLower || "").trim().toLowerCase();

  // Strong signals first
  if (st.includes("cancel")) return "canceled";
  if (st.includes("reject") || st.includes("fail") || st.includes("expire") || st.includes("error")) return "rejected";

  // Partial before fill
  if (st.includes("partial") || st.includes("partially_filled") || st.includes("partially filled")) return "partial";

  if (st.includes("fill") || st === "done" || st === "filled") return "filled";

  // Ack/pending states
  if (st.includes("ack") || st.includes("accept") || st.includes("accepted")) return "acked";
  if (st.includes("pending") || st.includes("new") || st.includes("queued") || st.includes("await")) return "pending";

  // Open/live/active
  if (st.includes("open") || st.includes("live") || st.includes("active") || st.includes("working")) return "open";

  // Generic "closed" often means terminal but ambiguous; keep it terminal-colored.
  if (st.includes("closed") || st.includes("complete") || st.includes("settled")) return "terminal";

  // If backend bucket says terminal and we can't infer specifics, keep terminal tint.
  if (b === "terminal") return "terminal";

  return st ? "unknown" : b === "terminal" ? "terminal" : "unknown";
}

    // ─────────────────────────────────────────────────────────────
  // Theme / Palette
  // ─────────────────────────────────────────────────────────────
  const PALETTES = {
    geminiDark: {
      // Renamed per request (no “Gemini” label in UI)
      name: "UTT Dark (Default)",
      widgetBg: "#121212",
      widgetBg2: "#151515",
      panelBg: "#101010",
      border: "#2a2a2a",
      border2: "#333333",
      text: "#e8e8e8",
      muted: "#bdbdbd",
      link: "#e1b303",
      warn: "#cb9800",
      danger: "#ff6b6b",
      good: "#59d98e",
      shadow: "rgba(0,0,0,0.35)",
    },
    graphite: {
      name: "Graphite",
      widgetBg: "#0f1115",
      widgetBg2: "#141822",
      panelBg: "#0c0e12",
      border: "#273043",
      border2: "#2f3a52",
      text: "#e6e8ee",
      muted: "#a9b0c0",
      link: "#9ecbff",
      warn: "#f2c14e",
      danger: "#ff6b6b",
      good: "#7ee081",
      shadow: "rgba(0,0,0,0.38)",
    },
    midnight: {
      name: "Midnight Blue",
      widgetBg: "#0b1020",
      widgetBg2: "#0e1630",
      panelBg: "#080c18",
      border: "#1f2a4a",
      border2: "#28355c",
      text: "#e9ecff",
      muted: "#b4bfdc",
      link: "#7aa2ff",
      warn: "#f6c177",
      danger: "#ff6b6b",
      good: "#34d399",
      shadow: "rgba(0,0,0,0.42)",
    },
    highContrast: {
      name: "High Contrast",
      widgetBg: "#0a0a0a",
      widgetBg2: "#111111",
      panelBg: "#000000",
      border: "#4a4a4a",
      border2: "#5a5a5a",
      text: "#ffffff",
      muted: "#d0d0d0",
      link: "#ffd000",
      warn: "#ffd000",
      danger: "#ff5252",
      good: "#00e676",
      shadow: "rgba(0,0,0,0.55)",
    },

    // NEW: Custom palette (values provided by user via editor)
    custom: {
      name: "Custom",
      widgetBg: "#121212",
      widgetBg2: "#151515",
      panelBg: "#101010",
      border: "#2a2a2a",
      border2: "#333333",
      text: "#e8e8e8",
      muted: "#bdbdbd",
      link: "#e1b303",
      warn: "#cb9800",
      danger: "#ff6b6b",
      good: "#59d98e",
      shadow: "rgba(0,0,0,0.35)",
    },
  };

  function readThemeFromStorage() {
    const raw = localStorage.getItem(LS_THEME_KEY);
    const parsed = safeParseJson(raw || "");
    const key = typeof parsed === "string" ? parsed : typeof raw === "string" ? raw.replace(/"/g, "") : "";
    // allow "custom" and any known palette keys; default to geminiDark
    return PALETTES[key] ? key : "geminiDark";
  }

  function isHexColor(s) {
    const v = String(s || "").trim();
    return /^#[0-9A-Fa-f]{6}$/.test(v);
  }

  function hexToRgb(hex) {
    const h = String(hex || "").trim();
    if (!isHexColor(h)) return null;
    const r = parseInt(h.slice(1, 3), 16);
    const g = parseInt(h.slice(3, 5), 16);
    const b = parseInt(h.slice(5, 7), 16);
    if (![r, g, b].every((n) => Number.isFinite(n))) return null;
    return { r, g, b };
  }

  function readCustomThemeFromStorage() {
    const raw = localStorage.getItem(LS_THEME_CUSTOM_KEY);
    const parsed = safeParseJson(raw || "");
    if (!parsed || typeof parsed !== "object") return null;

    const out = {};
    // Only accept known keys (prevents junk)
    const keys = [
      "widgetBg",
      "widgetBg2",
      "panelBg",
      "border",
      "border2",
      "text",
      "muted",
      "link",
      "warn",
      "danger",
      "good",
      "shadowColor",
      "shadowAlpha",
    ];

    for (const k of keys) {
      if (parsed[k] === undefined) continue;
      out[k] = parsed[k];
    }

    return out;
  }

  function buildShadowFrom(colorHex, alpha) {
    const rgb = hexToRgb(colorHex);
    const a = Number(alpha);
    const aOk = Number.isFinite(a) ? clamp(a, 0, 1) : 0.35;

    if (!rgb) return `rgba(0,0,0,${aOk})`;
    return `rgba(${rgb.r},${rgb.g},${rgb.b},${aOk})`;
  }

  function resolvePalette(themeKey, customCfg) {
    const base = PALETTES[themeKey] || PALETTES.geminiDark;

    if (themeKey !== "custom") return base;

    // For custom: merge over a stable base so missing keys never break rendering.
    const fallback = PALETTES.geminiDark;

    const cfg = customCfg && typeof customCfg === "object" ? customCfg : {};
    const shadowColor = isHexColor(cfg.shadowColor) ? cfg.shadowColor : "#000000";
    const shadowAlpha = Number(cfg.shadowAlpha);
    const shadow = buildShadowFrom(shadowColor, Number.isFinite(shadowAlpha) ? shadowAlpha : 0.35);

    const merged = {
      ...fallback,
      ...PALETTES.custom,
      ...cfg,

      // Normalize/validate colors (only accept #RRGGBB)
      widgetBg: isHexColor(cfg.widgetBg) ? cfg.widgetBg : PALETTES.custom.widgetBg || fallback.widgetBg,
      widgetBg2: isHexColor(cfg.widgetBg2) ? cfg.widgetBg2 : PALETTES.custom.widgetBg2 || fallback.widgetBg2,
      panelBg: isHexColor(cfg.panelBg) ? cfg.panelBg : PALETTES.custom.panelBg || fallback.panelBg,
      border: isHexColor(cfg.border) ? cfg.border : PALETTES.custom.border || fallback.border,
      border2: isHexColor(cfg.border2) ? cfg.border2 : PALETTES.custom.border2 || fallback.border2,
      text: isHexColor(cfg.text) ? cfg.text : PALETTES.custom.text || fallback.text,
      muted: isHexColor(cfg.muted) ? cfg.muted : PALETTES.custom.muted || fallback.muted,
      link: isHexColor(cfg.link) ? cfg.link : PALETTES.custom.link || fallback.link,
      warn: isHexColor(cfg.warn) ? cfg.warn : PALETTES.custom.warn || fallback.warn,
      danger: isHexColor(cfg.danger) ? cfg.danger : PALETTES.custom.danger || fallback.danger,
      good: isHexColor(cfg.good) ? cfg.good : PALETTES.custom.good || fallback.good,

      // Always derived from color + alpha controls
      shadow,

      name: "Custom",
    };

    return merged;
  }

  // Build a derived style object from App.jsx `styles` + palette overrides.
  // This keeps layout/spacing intact while allowing color theming here.
  function deriveStyles(styles, pal) {
    const s = styles || {};
    const merge = (base, patch) => ({ ...(base || {}), ...(patch || {}) });

    return {
      ...s,

      muted: merge(s.muted, { color: pal.muted }),

      table: merge(s.table, {
        borderColor: pal.border,
      }),

      th: merge(s.th, {
        borderColor: pal.border,
        color: pal.text,
        background: pal.widgetBg2,
      }),

      td: merge(s.td, {
        borderColor: pal.border,
        color: pal.text,
      }),

      pill: merge(s.pill, {
        borderColor: pal.border,
        background: pal.widgetBg2,
        color: pal.text,
      }),

      input: merge(s.input, {
        borderColor: pal.border,
        background: pal.panelBg,
        color: pal.text,
      }),

      select: merge(s.select, {
        borderColor: pal.border,
        background: pal.panelBg,
        color: pal.text,
      }),

      button: merge(s.button, {
        borderColor: pal.border,
        background: pal.widgetBg2,
        color: pal.text,
      }),

      buttonDisabled: merge(s.buttonDisabled, {
        borderColor: pal.border,
        opacity: 0.55,
      }),

      linkyHeader: merge(s.linkyHeader, {
        color: pal.link,
      }),
    };
  }

  export default function TerminalTablesWidget(props) {
    const {
      styles,
      appContainerRef,
      headerRef,

      // whether chart is currently visible (controls docking behavior)
      showChart,

      hideTableDataGlobal,
      hideVenueNames,
      mask,
      maskMaybe,

      venue,
      ALL_VENUES_VALUE,

      tab,
      setTab,

      // market picker (clickable symbol cells)
      onPickMarket,

      // balances
      balancesSorted,
      balancesColSpan,
      loadingBalances,
      doRefreshBalances,
      hideBalancesView,
      setHideBalancesView,
      hideZeroBalances,
      setHideZeroBalances,
      toggleBalanceSort,
      balSortKey,
      balSortDir,
      portfolioTotalUsd,
      fmtUsd,
      fmtPxUsd,

      // local orders
      orders,
      loadingOrders,
      doLoadOrders,
      doCancelOrder,
      statusFilter,
      setStatusFilter,
      localSymbolFilter,
      setLocalSymbolFilter,
      hideCancelledLocal,
      setHideCancelledLocal,
      statusIsCanceledLocal,
      isCancelableStatus,

      // unified orders
      allOrders,
      allTotal,
      loadingAll,
      doLoadAllOrders,
      doSyncAndLoadAllOrders,
      aoSource,
      setAoSource,
      aoVenue,
      setAoVenue,
      aoStatusBucket,
      setAoStatusBucket,
      aoSymbol,
      setAoSymbol,
      aoSortField,
      aoSortDir,
      aoSort,
      toggleAllSort,
      aoPage,
      setAoPage,
      aoPageSize,
      setAoPageSize,
      aoTotalPages,
      hideCancelledUnified,
      setHideCancelledUnified,
      setViewedConfirmed,

      // NEW: cancel unified orders by cancel_ref (preferred).
      doCancelUnifiedOrder,
      showManualCancelModal,
      setShowManualCancelModal,
      manualCancelVenue,
      setManualCancelVenue,
      manualCancelOrderId,
      setManualCancelOrderId,
      manualCancelBusy,
      doManualJupiterCancel,
      onManualCancelError,

      // Discover
      discVenue,
      setDiscVenue,
      discEps,
      setDiscEps,

      // NEW (optional): Discover filter for "only show newly discovered in last X days"
      discDays,
      setDiscDays,

      discNew,
      discUnheld,
      discMeta,
      loadingDiscover,
      doLoadDiscover,
      applySymbolFromDiscover,
      supportedVenues,

    // Optional: explicit discovery venue allow-list / dropdown list (from App.jsx)
    discoveryVenues: discoveryVenuesProp,
    // Optional: enabled venues list (from App.jsx)
    enabledVenues,

      // Discover "viewed" (LOCAL, persisted in App.jsx LS map)
      isDiscoverySymbolViewed,
      setDiscoverySymbolViewed,
      toggleDiscoverySymbolViewed,

      // helpers
      fmtTime,
      fmtEco,
      calcGrossTotal,
      calcFee,
      calcNetTotal,
      isTerminalStatus,
      isTerminalBucket,
      SortHeader,
      btn,
      isCanceledStatus,

      // NEW (optional for Balances overlib):
      // async ({ venue, asset }) => { markets: string[] } OR string[]
      getVenueMarkets,
    } = props;

    // Theme state
    const [themeKey, setThemeKey] = useState(() => readThemeFromStorage());
    useEffect(() => {
      try {
        localStorage.setItem(LS_THEME_KEY, JSON.stringify(themeKey));
      } catch {
        // ignore
      }
    }, [themeKey]);

    // NEW: custom theme payload
    const [customTheme, setCustomTheme] = useState(() => {
      const saved = readCustomThemeFromStorage();

      // Seed defaults from the custom palette definition (plus shadow controls)
      const base = PALETTES.custom;

      const shadowMatch = String(base.shadow || "").match(/rgba\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*([0-9.]+)\s*\)/i);
      const alphaDefault = shadowMatch ? Number(shadowMatch[1]) : 0.35;

      const seeded = {
        widgetBg: base.widgetBg,
        widgetBg2: base.widgetBg2,
        panelBg: base.panelBg,
        border: base.border,
        border2: base.border2,
        text: base.text,
        muted: base.muted,
        link: base.link,
        warn: base.warn,
        danger: base.danger,
        good: base.good,
        shadowColor: "#000000",
        shadowAlpha: Number.isFinite(alphaDefault) ? alphaDefault : 0.35,
      };

      return saved && typeof saved === "object" ? { ...seeded, ...saved } : seeded;
    });

    useEffect(() => {
      try {
        localStorage.setItem(LS_THEME_CUSTOM_KEY, JSON.stringify(customTheme));
      } catch {
        // ignore
      }
    }, [customTheme]);

    const pal = useMemo(() => resolvePalette(themeKey, customTheme), [themeKey, customTheme]);
    const sx = useMemo(() => deriveStyles(styles, pal), [styles, pal]);

    // Custom theme editor visibility:
    // - Default: shown whenever theme is "custom".
    // - User can hide/collapse it; it will remain hidden until the theme is
    //   re-selected from the dropdown again (i.e., theme transitions into "custom").
    const [customThemeEditorOpen, setCustomThemeEditorOpen] = useState(() => themeKey === "custom");

    // Helper button style that respects derived styles (single source of truth).
    const smallBtn = (disabled) => ({
      ...sx.button,
      padding: "6px 8px",
      fontSize: 12,
      ...(disabled ? sx.buttonDisabled : {}),
    });

    // Balances: local symbol search (UI-only filter).
    // Filters the already-selected venue scope (or "ALL") by asset symbol.
    const [balancesSymbolQuery, setBalancesSymbolQuery] = useState("");


    // ─────────────────────────────────────────────────────────────
    // Solana on-chain balances (live RPC via backend) — opt-in and non-breaking
    // Used to display balances for solana_* venues even when not ingested into Wallet Addresses.
    // ─────────────────────────────────────────────────────────────
    const [solanaOnchain, setSolanaOnchain] = useState(() => ({

      address: "",
      items: [], // [{ asset, amount, mint, symbol }]
      loading: false,
      err: "",
      fetchedAt: null,
    }));

  // Solana Token Registry: mint/address -> symbol (UI-only enrichment for balances)
  const [solanaTokenRegistryMap, setSolanaTokenRegistryMap] = useState(() => ({}));

  const [solanaPrices, setSolanaPrices] = useState({ ok: false, items: {}, ts: 0 });
  // Canonical wrapped SOL mint (for Jupiter pricing lookups)
  const SOL_MINT = "So11111111111111111111111111111111111111112";
  // Solana venues do NOT use the CEX balances refresh pipeline
  const isSolanaVenue = String(venue || "").toLowerCase().startsWith("solana");
  const [showSolanaOnchainDetails, setShowSolanaOnchainDetails] = useState(false);


    function getSolanaUsdPriceForMint(mint) {
      const m = String(mint || "").trim();
      if (!m) return null;
      const p = solanaPrices || {};
      const items = p.items || p.data || p.prices || p.results || {};
      const entry = items?.[m] ?? items?.[String(m).toLowerCase()] ?? null;
      if (entry == null) return null;
      if (typeof entry === "number") return entry;
      const cand =
        entry.price ??
        entry.priceUsd ??
        entry.priceUSD ??
        entry.price_usd ??
        entry.usdPrice ??
        entry.usd ??
        entry.value ??
        entry.v ??
        null;
      if (typeof cand === "number") return cand;
      // tolerate numeric strings (some price APIs return strings)
      if (typeof cand === "string") {
        const f = Number(cand);
        return Number.isFinite(f) ? f : null;
      }
      return null;
    }

    const balancesFiltered = useMemo(() => {
      let rows = balancesSorted || [];
      if (isSolanaVenue) {
        // When the selected venue is Solana, balances come from live on-chain RPC
        // (solanaOnchain.items normalized earlier). Shape it to match the balances table.
        rows = (solanaOnchain?.items || []).map((it) => {
          const mint = String(it?.mint || "").trim();
          const mintShort = mint ? `${mint.slice(0, 6)}…` : "";
          // Prefer explicit symbol or Token Registry (mint->symbol).
          // Avoid falling back to `it.asset` here because for mint-only rows `asset` is already mint-short,
          // which would make the UI show the mint twice.
          const symbol = String(it?.symbol || (mint ? (solanaTokenRegistryMap?.[mint] || solanaTokenRegistryMap?.[String(mint).toLowerCase()]) : "") || "").trim();
          const assetLabel = symbol || mintShort || mint || "";
          const amt = Number(it?.amount ?? 0);
          const px = getSolanaUsdPriceForMint(mint);
          return {
            venue: venue || "solana",
            asset: assetLabel,
            symbol: symbol || null,
            mint: mint || null,
            // Only show mint on a second line when we have a real symbol.
            mint_short: symbol ? (mintShort || null) : null,
            total: amt,
            available: amt,
            hold: 0,
            px_usd: typeof px === "number" ? px : null,
            total_usd: typeof px === "number" ? amt * px : null,
            usd_source_symbol: "USDC",
          };
        });
      }
      const q = String(balancesSymbolQuery || "")
        .trim()
        .toUpperCase();
      if (!q) return rows;

      // Allow short aliases (e.g., "XBT" -> "BTC") by simple contains matching.
      // Asset field is expected to be already canonicalized by the balances service.
      return rows.filter((b) => {
        const a = String(b?.asset || "").trim().toUpperCase();
        return a.includes(q);
      });
    }, [balancesSorted, balancesSymbolQuery, isSolanaVenue, solanaOnchain, venue, solanaPrices, solanaTokenRegistryMap]);

  const solanaPortfolioTotalUsd = useMemo(() => {
    if (!isSolanaVenue) return null;
    const items = solanaOnchain?.items || [];
    let sum = 0;
    for (const t of items) {
      const mint = String(t?.mint || "").trim();
      const amt = Number(t?.amount ?? 0) || 0;
      const p = getSolanaUsdPriceForMint(mint);
      if (typeof p === "number") sum += amt * p;
    }
    return sum;
  }, [isSolanaVenue, solanaOnchain, solanaPrices]);

    // Status visuals derived from palette
    const statusVisualMap = useMemo(() => {
      const bgStrong = pal.panelBg; // subtle “panel” shade (not pure black)
      return {
        open: { bg: "", accent: "transparent" },

        acked: { bg: bgStrong, accent: pal.warn },
        pending: { bg: bgStrong, accent: pal.warn },

        partial: { bg: bgStrong, accent: pal.text },

        filled: { bg: "", accent: "transparent" },
        canceled: { bg: bgStrong, accent: pal.danger },
        rejected: { bg: bgStrong, accent: pal.danger },

        terminal: { bg: "", accent: "transparent" },
        unknown: { bg: bgStrong, accent: "transparent" },
      };
    }, [pal]);

    function statusRowStyle({ statusLower, bucketLower, dim }) {
      const kind = classifyStatusKind(statusLower, bucketLower);
      const vis = statusVisualMap[kind] || statusVisualMap.unknown;

      // Dim controls are applied as opacity so we preserve the hue.
      const opacity = dim ? 0.6 : 1.0;

      return {
        background: vis.bg || "transparent",
        boxShadow: `inset 4px 0 0 ${vis.accent || "transparent"}`,
        opacity,
      };
    }

    const widgetRef = useRef(null);
    const customThemeEditorRef = useRef(null);
    const prevThemeKeyRef = useRef(themeKey);
    const customThemePendingScrollRef = useRef(false);

    // When switching INTO the Custom theme, scroll the editor into view so it
    // doesn't appear to "vanish" behind large tables (Balances/Orders).
    useEffect(() => {
      const prev = prevThemeKeyRef.current;
      prevThemeKeyRef.current = themeKey;

      // Maintain the "open" state:
      // - collapse when leaving "custom"
      // - auto-open when *entering* "custom" (user intent)
      const enteringCustom = themeKey === "custom" && prev !== "custom";

      if (themeKey !== "custom") {
        if (customThemeEditorOpen) setCustomThemeEditorOpen(false);
        customThemePendingScrollRef.current = false;
        return;
      }

      if (enteringCustom) {
        setCustomThemeEditorOpen(true);
        customThemePendingScrollRef.current = true;
        return;
      }

      // If user collapsed it while staying in custom, do not re-open.
      // Also don't attempt to scroll if it's currently hidden.
      if (!customThemeEditorOpen) return;
    }, [themeKey, customThemeEditorOpen]);

    // Perform the deferred scroll once the custom editor has actually rendered.
    useEffect(() => {
      if (themeKey !== "custom") return;
      if (!customThemeEditorOpen) return;
      if (!customThemePendingScrollRef.current) return;

      customThemePendingScrollRef.current = false;

      const el = customThemeEditorRef.current;
      if (!el) return;

      // Next tick so layout has settled.
      setTimeout(() => {
        try {
          el.scrollIntoView({ block: "start", behavior: "smooth" });
        } catch {
          // ignore
        }
      }, 0);
    }, [themeKey, customThemeEditorOpen]);

    // ─────────────────────────────────────────────────────────────
    // Balances refresh hardening:
    // - prevent overlapping refresh calls
    // - show non-destructive banner on errors
    // ─────────────────────────────────────────────────────────────
    const balancesRefreshRef = useRef({ inFlight: false, seq: 0 });
    const solanaRefreshRef = useRef({ inFlight: false, lastAt: 0 });
    const balancesBannerTimerRef = useRef(null);

    const [balancesRefreshingLocal, setBalancesRefreshingLocal] = useState(false);
    const [balancesBanner, setBalancesBanner] = useState(() => ({
      open: false,
      kind: "error", // "error" | "warn" | "info"
      msg: "",
    }));
    function getInjectedSolanaProvider() {
      try {
        return window.solflare || window.solana || null;
      } catch {
        return null;
      }
    }

    function pubkeyToBase58(pk) {
      try {
        if (!pk) return "";
        if (typeof pk === "string") return pk;
        if (typeof pk?.toBase58 === "function") return pk.toBase58();
        if (typeof pk?.toString === "function") return pk.toString();
      } catch {
        // ignore
      }
      return "";
    }

    async function ensureSolanaConnected() {
      const provider = getInjectedSolanaProvider();
      if (!provider) throw new Error("No Solana wallet found (install Solflare).");

      // Some providers expose isConnected; some require connect().
      const already = !!provider.publicKey;
      if (!already) {
        if (typeof provider.connect === "function") {
          await provider.connect();
        }
      }

      const addr = pubkeyToBase58(provider.publicKey);
      if (!addr) throw new Error("Could not read wallet public key.");
      return { provider, address: addr };
    }

    
  
  async function fetchJSONMaybe(url) {
    // Prefer App-provided/global fetchJSON when available.
    if (typeof fetchJSON === "function") return await fetchJSON(url);

    // Fallback: direct fetch with optional auth token (mirrors token-registry behavior)
    const headers = { "Content-Type": "application/json" };
    const tok = (typeof getAuthToken === "function") ? getAuthToken() : null;
    if (tok) headers["Authorization"] = `Bearer ${tok}`;

    const resp = await fetch(url, { headers });
    let js = {};
    try { js = await resp.json(); } catch (e) { js = {}; }

    if (!resp.ok) {
      const msg = (js && (js.detail || js.error)) ? (js.detail || js.error) : `Request failed (${resp.status})`;
      throw new Error(msg);
    }
    return js;
  }

async function loadSolanaTokenRegistryMap() {
    // Best-effort: token registry is local DB-backed, so no paid dependency.
    // Try a couple query shapes; backend can ignore unknown params.
    const urls = [
      "/api/token_registry?network=solana",
      "/api/token_registry?chain=solana",
      "/api/token_registry",
    ];

    let data = null;
    for (const u of urls) {
      try {
        // NOTE: use the existing fetchJSON helper (and fall back to a manual fetch with auth) so token_registry doesn't silently fail.
        if (typeof fetchJSON === "function") {
          data = await fetchJSON(u);
        } else {
          const headers = { "Content-Type": "application/json" };
          const tok = getAuthToken?.();
          if (tok) headers["Authorization"] = `Bearer ${tok}`;
          const resp = await fetch(u, { headers });
          const js = await resp.json().catch(() => ({}));
          if (!resp.ok) throw new Error(js?.detail || js?.error || `token_registry failed (${resp.status})`);
          data = js;
        }
        if (data) break;
      } catch (e) {
        // keep trying
      }
    }

    const items = Array.isArray(data)
      ? data
      : (data && (data.items || data.mappings || data.tokens)) || [];
    const map = {};

    for (const it of items || []) {
      const symRaw = it.symbol || it.asset || it.ticker;
      if (!symRaw) continue;
      const sym = String(symRaw).trim();
      if (!sym) continue;

      const addr =
        it.mint ||
        it.address ||
        it.contract_address ||
        it.contractAddress ||
        it.mint_address ||
        it.mintAddress ||
        it.addr;

      if (!addr) continue;
      const a = String(addr).trim();
      if (!a) continue;

      const v = String(it.venue || it.venue_override || it.venueOverride || "").trim();
      const isGlobal = !v;
      const isSol = v === "solana" || v === "solana_jupiter" || v.startsWith("solana");

      if (isGlobal || isSol) {
        map[a] = sym;
        map[String(a).toLowerCase()] = sym;
      }
    }

    return map;
  }

  // ─────────────────────────────────────────────────────────────
  // Auto-load Solana token registry for All Orders whenever either:
  // - the explicit All Orders venue filter is a Solana venue, OR
  // - the current All Orders dataset contains Solana rows while aoVenue is blank/"all"
  // This fixes mixed-venue views where symbol resolution needs the registry even though
  // the venue filter itself is not set to a Solana-specific value.
  // ─────────────────────────────────────────────────────────────
  useEffect(() => {
  const v = String(aoVenue || "").toLowerCase();
  const venueImpliesSolana = v.startsWith("solana");
  const rowsImpliesSolana =
    tab === "allOrders" &&
    Array.isArray(allOrders) &&
    allOrders.some((r) => String(r?.venue || "").toLowerCase().startsWith("solana"));

  if (!venueImpliesSolana && !rowsImpliesSolana) return;

  const m = solanaTokenRegistryMap;
  const has = m && typeof m === "object" && Object.keys(m).length > 0;
  if (has) return;

  let cancelled = false;
  (async () => {
    try {
      const reg = await loadSolanaTokenRegistryMap();
      if (cancelled) return;
      if (reg && typeof reg === "object") {
        setSolanaTokenRegistryMap(reg);
      }
    } catch (e) {
      // loadSolanaTokenRegistryMap already handles errors/toasts
    }
  })();

  return () => {
    cancelled = true;
  };
}, [aoVenue, tab, allOrders, solanaTokenRegistryMap]);


async function refreshSolanaOnchainBalances() {
      if (!isSolanaVenue) return;

      // Prevent overlap / click-spam bursts (helps avoid public RPC 429 cascades).
      if (solanaRefreshRef?.current?.inFlight) return;
      const now = Date.now();
      const lastAt = Number(solanaRefreshRef?.current?.lastAt || 0);
      if (now - lastAt < 750) return;
      solanaRefreshRef.current.inFlight = true;
      solanaRefreshRef.current.lastAt = now;

      setSolanaOnchain((p) => ({ ...p, loading: true, err: "" }));

      // Enrich mint-only balances with Token Registry symbols (local DB). Refresh this map opportunistically.
      let regMap = solanaTokenRegistryMap;
      try {
        if (!regMap || Object.keys(regMap).length === 0) {
          regMap = await loadSolanaTokenRegistryMap();
          setSolanaTokenRegistryMap(regMap || {});
        }
      } catch (e) {
        // ignore; we can still render mint-short
        regMap = regMap || {};
      }

      try {
        const { address } = await ensureSolanaConnected();
        const headers = { "Content-Type": "application/json" };
        const tok = getAuthToken();
        if (tok) headers["Authorization"] = `Bearer ${tok}`;

        const resp = await fetch(`/api/solana_dex/balances?address=${encodeURIComponent(address)}`, { headers });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || data?.ok === false) {
          throw new Error(data?.detail || data?.error || `balances failed (${resp.status})`);
        }

        // Normalize response into {asset, amount, mint, symbol}
        const items = [];
        const sol = data?.sol ?? data?.sol_ui ?? data?.sol_balance ?? null;
        if (typeof sol === "number") {
          // Give SOL a canonical mint so we can fetch a USD price from Jupiter.
          items.push({ asset: "SOL", amount: sol, mint: SOL_MINT, symbol: "SOL" });
        } else if (typeof data?.sol_lamports === "number") {
          items.push({ asset: "SOL", amount: data.sol_lamports / 1e9, mint: SOL_MINT, symbol: "SOL" });
        }

        const toks = Array.isArray(data?.tokens) ? data.tokens : [];
        for (const t of toks) {
          const mint = String(t?.mint || "").trim();
          const symFromRegistry = regMap[mint] || regMap[String(mint).toLowerCase()] || "";
          const sym = String(t?.symbol || t?.ticker || symFromRegistry || "").trim();
          const asset = sym || mint ? (sym || mint.slice(0, 6) + "…") : "SPL";
          const amt =
            typeof t?.uiAmount === "number"
              ? t.uiAmount
              : typeof t?.ui_amount === "number"
              ? t.ui_amount
              : typeof t?.amount_ui === "number"
              ? t.amount_ui
              : null;
          if (amt == null) continue;
          items.push({ asset, amount: amt, mint, symbol: sym || "" });
        }

        setSolanaOnchain({
          address,
          items,
          loading: false,
          err: "",
          fetchedAt: Date.now(),
        });

        // Solana pricing (Jupiter): fetch USDC-ish prices for displayed mints
        try {
          const mints = items
            .map((t) => String(t?.mint || "").trim())
            .filter(Boolean)
            .slice(0, 100);
          if (mints.length) {
            const pr = await fetchJSONMaybe(`/api/solana_dex/jupiter/prices?ids=${encodeURIComponent(mints.join(","))}`);
            // Accept multiple backend response shapes.
            // Examples we tolerate:
            //  - { ok: true, items: { <mint>: { price: 1.23 } } }
            //  - { data: { <mint>: { price: 1.23 } } }
            //  - { prices: { <mint>: 1.23 } }
            //  - { <mint>: { price: 1.23 } }
            const normalized =
              pr?.items ||
              pr?.data ||
              pr?.prices ||
              pr?.results ||
              (pr && typeof pr === "object" ? pr : {}) ||
              {};
            setSolanaPrices({ ok: pr?.ok !== false, items: normalized || {}, ts: Date.now() });
          } else {
            setSolanaPrices({ ok: true, items: {}, ts: Date.now() });
          }
        } catch (e) {
          setSolanaPrices({ ok: false, items: {}, ts: Date.now(), error: String(e) });
        }
      } catch (e) {
        setSolanaOnchain((p) => ({
          ...p,
          loading: false,
          err: String(e?.message || e || "Failed to load Solana balances"),
        }));
      }
    }

      try { solanaRefreshRef.current.inFlight = false; } catch { /* ignore */ }

    useEffect(() => {
      return () => {
        if (balancesBannerTimerRef.current) {
          clearTimeout(balancesBannerTimerRef.current);
          balancesBannerTimerRef.current = null;
        }
      };
    }, []);

    function showBalancesBanner(kind, msg, ms = 9000) {
      const m = String(msg || "").trim();
      if (!m) return;

      setBalancesBanner({ open: true, kind: kind || "error", msg: m });

      try {
        if (balancesBannerTimerRef.current) clearTimeout(balancesBannerTimerRef.current);
      } catch {
        // ignore
      }

      balancesBannerTimerRef.current = setTimeout(() => {
        setBalancesBanner((p) => ({ ...p, open: false }));
      }, ms);
    }

    async function safeRefreshBalancesFromUI() {
      // For Solana venues, balances refresh must NOT hit /api/balances/refresh (CEX-only).
      // Instead we use the on-chain RPC-backed endpoint via /api/solana_dex/balances.
      if (isSolanaVenue) {
        await refreshSolanaOnchainBalances();
        return;
      }

      if (typeof doRefreshBalances !== "function") return;

      // Hard guard against overlap (independent of loadingBalances prop)
      if (balancesRefreshRef.current.inFlight) return;

      balancesRefreshRef.current.inFlight = true;
      balancesRefreshRef.current.seq += 1;
      const seq = balancesRefreshRef.current.seq;

      setBalancesRefreshingLocal(true);

      try {
        // Always pass the current venue explicitly. App owns ALL-venues fan-out + skip guards.
        // doRefreshBalances may or may not return a promise; normalize.
        await Promise.resolve(doRefreshBalances({ venue }));
        // If App.jsx keeps last-known balances on error, no banner needed here.
      } catch (e) {
        const msg =
          e?.message
            ? String(e.message)
            : typeof e === "string"
              ? e
              : "Failed to refresh balances (kept last known balances).";

        // Only show if we're still the latest request
        if (balancesRefreshRef.current.seq === seq) {
          showBalancesBanner("error", msg);
        }
      } finally {
        // Only clear flags if we're still the latest request
        if (balancesRefreshRef.current.seq === seq) {
          balancesRefreshRef.current.inFlight = false;
          setBalancesRefreshingLocal(false);
        }
      }
    }
  const dragRef = useRef({
    dragging: false,
    dy: 0,
    startClientY: 0,
    startOffset: 0,
    pointerId: null,
  });

  const resizeRef = useRef({
    resizing: false,
    mode: null, // "bottom" | "top" | "right" | "corner"
    startClientY: 0,
    startClientX: 0,
    startY: 0,
    startH: 0,
    startW: 0,
    pointerId: null,
  });

  const lockRafRef = useRef(0);

  const [locked, setLocked] = useState(() => {
    const v = safeParseJson(localStorage.getItem(LS_LOCK_KEY) || "");
    return !!v;
  });

  useEffect(() => {
    localStorage.setItem(LS_LOCK_KEY, JSON.stringify(locked));
  }, [locked]);

  useEffect(() => () => {
    try {
      if (lockRafRef.current) window.cancelAnimationFrame(lockRafRef.current);
    } catch {
      // ignore
    }
  }, []);

  const [dockBelowChart, setDockBelowChart] = useState(() => {
    const v = safeParseJson(localStorage.getItem(LS_DOCK_KEY) || "");
    return v === null ? true : !!v;
  });

  useEffect(() => {
    localStorage.setItem(LS_DOCK_KEY, JSON.stringify(dockBelowChart));
  }, [dockBelowChart]);


  // Docking is only enforced while LOCKED.
  // When unlocked, the widget should move freely (both X and Y), regardless of the Dock toggle.
  const effectiveDockBelowChart = dockBelowChart && locked;

  // Important: when the chart is hidden, the Dock toggle should stay latent.
  // The widget must preserve its current viewport x/y instead of snapping back
  // to the tables-pane left edge just because Dock is enabled.
  const activeDockBelowChart = effectiveDockBelowChart && !!showChart;

  function handleLockedChange(nextLocked) {
    const next = !!nextLocked;

    try {
      if (lockRafRef.current) {
        window.cancelAnimationFrame(lockRafRef.current);
        lockRafRef.current = 0;
      }
    } catch {
      // ignore
    }

    if (!next) {
      setLocked(false);
      return;
    }

    try {
      const r = widgetRef.current?.getBoundingClientRect?.();
      if (r) {
        const snapX = Number.isFinite(r.left) ? Math.round(r.left) : Math.round(Number(geom?.x) || 24);
        const snapY = Number.isFinite(r.top) ? Math.round(r.top) : Math.round(Number(geom?.y) || 160);
        const snapW = Number.isFinite(r.width) ? Math.round(r.width) : Math.round(Number(geom?.w) || 0);
        const snapH = Number.isFinite(r.height) ? Math.round(r.height) : Math.round(Number(geom?.h) || 560);

        setYh((prev) => ({ ...prev, x: snapX, y: snapY, h: snapH }));
        if (Number.isFinite(snapW) && snapW > 0) {
          setXw((prev) => ({ ...(prev || {}), w: snapW }));
        }

        if (dockBelowChart && showChart) {
          const cx = Number(chartGeom?.x);
          const cw = Number(chartGeom?.w);
          const baseLeft = Number.isFinite(cx) && Number.isFinite(cw) && cw > 0
            ? cx
            : containerInnerRaw.left;

          const cy = Number(chartGeom?.y);
          const ch = Number(chartGeom?.h);
          const chartBottom = Number.isFinite(cy) && Number.isFinite(ch) && ch > 0
            ? cy + ch
            : getHeaderSafeY(containerInnerRaw.top);

          setDockOffsetX(Math.round(snapX - baseLeft));
          setDockOffsetY(Math.max(0, Math.round(snapY - chartBottom)));
        }
      }
    } catch {
      // ignore snapshot failures
    }

    // Important: defer the actual lock flip one frame so the snapshotted x/y/w and dock offsets
    // land first. Hiding/showing Tables already proves the remounted locked state is correct;
    // the bad left jump is happening during the live lock transition.
    lockRafRef.current = window.requestAnimationFrame(() => {
      lockRafRef.current = 0;
      setLocked(true);
    });
  }

  const [dockOffsetY, setDockOffsetY] = useState(() => {
    const v = safeParseJson(localStorage.getItem(LS_DOCK_OFFSET_KEY) || "");
    return Number.isFinite(Number(v)) ? Number(v) : 12;
  });

  useEffect(() => {
    localStorage.setItem(LS_DOCK_OFFSET_KEY, JSON.stringify(dockOffsetY));
  }, [dockOffsetY]);

  const [dockOffsetX, setDockOffsetX] = useState(() => {
    const v = safeParseJson(localStorage.getItem(LS_DOCK_OFFSET_X_KEY) || "");
    return Number.isFinite(Number(v)) ? Number(v) : 0;
  });

  useEffect(() => {
    localStorage.setItem(LS_DOCK_OFFSET_X_KEY, JSON.stringify(dockOffsetX));
  }, [dockOffsetX]);

  const [yh, setYh] = useState(() => {
    const saved = safeParseJson(localStorage.getItem(LS_GEOM_KEY) || "");
    return saved && typeof saved === "object" ? { ...defaultYH(), ...saved } : defaultYH();
  });

  useEffect(() => {
    localStorage.setItem(LS_GEOM_KEY, JSON.stringify(yh));
  }, [yh]);

  // NEW: width override state (horizontal collapse/resize)
  const [xw, setXw] = useState(() => {
    const saved = safeParseJson(localStorage.getItem(LS_GEOM_XW_KEY) || "");
    if (saved && typeof saved === "object") {
      const w = saved.w;
      const wNum = w === null || w === undefined ? null : Number(w);
      return { w: Number.isFinite(wNum) && wNum > 0 ? wNum : null };
    }
    return defaultXW();
  });

  useEffect(() => {
    try {
      localStorage.setItem(LS_GEOM_XW_KEY, JSON.stringify({ w: xw?.w ?? null }));
    } catch {
      // ignore
    }
  }, [xw]);

  const [chartGeom, setChartGeom] = useState(() => readChartGeomFromStorage());

  useEffect(() => {
    if (!activeDockBelowChart) return;
    if (!showChart) return;

    let alive = true;
    const tick = () => {
      if (!alive) return;

      const next = readChartGeomFromStorage();
      setChartGeom((prev) => {
        const px = Number(prev?.x);
        const py = Number(prev?.y);
        const pw = Number(prev?.w);
        const ph = Number(prev?.h);

        const nx = Number(next?.x);
        const ny = Number(next?.y);
        const nw = Number(next?.w);
        const nh = Number(next?.h);

        const hasNext = Number.isFinite(ny) || Number.isFinite(nh) || Number.isFinite(nx) || Number.isFinite(nw);
        if (!hasNext) return prev;

        const dx = Number.isFinite(px) && Number.isFinite(nx) ? Math.abs(px - nx) : 999;
        const dy = Number.isFinite(py) && Number.isFinite(ny) ? Math.abs(py - ny) : 999;
        const dw = Number.isFinite(pw) && Number.isFinite(nw) ? Math.abs(pw - nw) : 999;
        const dh = Number.isFinite(ph) && Number.isFinite(nh) ? Math.abs(ph - nh) : 999;

        if (dx > 0.5 || dy > 0.5 || dw > 0.5 || dh > 0.5) return next;
        return prev;
      });

      setTimeout(tick, 200);
    };

    tick();
    return () => {
      alive = false;
     };
  }, [activeDockBelowChart, showChart]);

  function getContainerRectInner() {
    const el = appContainerRef?.current;
    const r = el?.getBoundingClientRect?.();

    if (!r) {
      return {
        left: 16,
        top: 16,
        width: Math.max(760, window.innerWidth - 32),
        padL: 0,
        padR: 0,
      };
    }

    const cs = window.getComputedStyle(el);
    const padL = numPx(cs.paddingLeft);
    const padR = numPx(cs.paddingRight);

    const left = r.left + padL;
    const width = Math.max(320, r.width - padL - padR);

    return { left, top: r.top, width, padL, padR };
  }

  function getHeaderSafeY(containerTop) {
    const hr = headerRef?.current?.getBoundingClientRect?.();
    if (hr && Number.isFinite(hr.bottom)) return hr.bottom + 10;
    return containerTop + 120;
  }

  function getDockedTopY(containerTop) {
    const headerSafe = getHeaderSafeY(containerTop);

    if (!effectiveDockBelowChart) return headerSafe;
    if (!showChart) return headerSafe;

    const y = Number(chartGeom?.y);
    const h = Number(chartGeom?.h);
    if (Number.isFinite(y) && Number.isFinite(h) && h > 0) {
      const bottom = y + h;
      const off = Number(dockOffsetY);
      const gap = Number.isFinite(off) ? off : 12;
      return Math.max(headerSafe, bottom + gap);
    }

    return headerSafe;
  }


  const containerInnerRaw = getContainerRectInner();

  // When the Tables widget is UNLOCKED and not docked-under-chart, we allow it to float anywhere
  // on the viewport (including over the AppHeader). Using viewport bounds prevents the header
  // container from clamping vertical movement.
  const containerInnerViewport = (() => {
    if (typeof window === "undefined") return containerInnerRaw;
    const margin = 8;
    const w = Math.max(320, Math.floor((window.innerWidth || 0) - margin * 2));
    const h = Math.max(320, Math.floor((window.innerHeight || 0) - margin * 2));
    return { left: margin, top: margin, width: w, height: h };
  })();

  const containerInner = !locked && !dockBelowChart ? containerInnerViewport : containerInnerRaw;


  function clampGeomToContainer(x, w, containerLeft, containerWidth) {
    const minW = 320;

    const left = Math.round(containerLeft);
    const rightEdge = Math.round(containerLeft + containerWidth);

    const maxW = Math.max(minW, rightEdge - left);
    const width = clamp(Math.round(w), minW, maxW);

    const maxX = Math.max(left, rightEdge - width);
    const x1 = clamp(Math.round(x), left, maxX);

    return { x: x1, w: width };
  }

  const geom = useMemo(() => {
    const chartX = Number(chartGeom?.x);
    const chartW = Number(chartGeom?.w);

    const wantChartFrame = activeDockBelowChart && Number.isFinite(chartX) && Number.isFinite(chartW) && chartW > 0;

    const baseX = wantChartFrame ? chartX : containerInner.left;
    const baseW = wantChartFrame ? chartW : Math.max(320, containerInner.width);

    // When docked + locked, preserve the live horizontal placement by storing a dock-relative X offset
    // on lock, instead of snapping back to the chart/container left edge.
    const dockX = Number(dockOffsetX);
    const rawX = activeDockBelowChart
      ? baseX + (Number.isFinite(dockX) ? dockX : 0)
      : Number.isFinite(yh?.x)
        ? yh.x
        : baseX;

    // Width override (horizontal resize). Docked mode should not exceed the chart/container width;
    // undocked can grow to the full viewport width.
    const wOverride = Number(xw?.w);
    const hasOverride = Number.isFinite(wOverride) && wOverride > 0;
    const desiredW = hasOverride ? (activeDockBelowChart ? Math.min(baseW, wOverride) : wOverride) : baseW;

    const clampLeft = activeDockBelowChart ? containerInner.left : 8;
    const clampW = activeDockBelowChart ? containerInner.width : Math.max(320, (window.innerWidth || 1200) - 16);

    const { x, w } = clampGeomToContainer(rawX, desiredW, clampLeft, clampW);

    const minH = 340;

    const dockedTop = getDockedTopY(containerInner.top);

    let y0 = Number(yh.y) || 160;
    let h0 = Number(yh.h) || 560;

    const candidateY = activeDockBelowChart ? dockedTop : y0;

    // When free-floating (not docked-under-chart), allow the panel to move anywhere on the viewport
    // including over the AppHeader. When docked, keep it below the header/chart-safe line.
    const minYBase = activeDockBelowChart ? dockedTop : 0;
    const minY = clamp(minYBase, 0, Math.max(0, window.innerHeight - minH - 10));
    const maxY_init = Math.max(minY, window.innerHeight - minH - 10);
    const y1 = clamp(candidateY, minY, maxY_init);

    const maxH1 = Math.max(minH, window.innerHeight - y1 - 10);
    const h1 = clamp(h0, minH, maxH1);

    const maxY2 = Math.max(minY, window.innerHeight - h1 - 10);
    const y2 = clamp(candidateY, minY, maxY2);

    const maxH2 = Math.max(minH, window.innerHeight - y2 - 10);
    const h2 = clamp(h0, minH, maxH2);

    return { x, y: y2, w, h: h2, dockedTop, minH, wantChartFrame };
  }, [
    yh,
    xw?.w,
    containerInner.left,
    containerInner.width,
    containerInner.top,
    headerRef,
    activeDockBelowChart,
    dockOffsetX,
    dockOffsetY,
    chartGeom?.x,
    chartGeom?.y,
    chartGeom?.w,
    chartGeom?.h,
    showChart,
  ]);

  useEffect(() => {
    const onMove = (e) => {
      if (locked) return;

      if (dragRef.current.dragging) {
        if (activeDockBelowChart) {
          const dy = e.clientY - dragRef.current.startClientY;
          const nextOffset = dragRef.current.startOffset + dy;
          setDockOffsetY(clamp(nextOffset, 0, 600));
        } else {
          const nextX = e.clientX - dragRef.current.dx;
          const nextY = e.clientY - dragRef.current.dy;
          setYh((prev) => ({ ...prev, x: nextX, y: nextY }));
        }
        return;
      }

      if (resizeRef.current.resizing) {
        const { mode, startClientY, startClientX, startY, startH, startW } = resizeRef.current;
        const dy = e.clientY - startClientY;
        const dx = e.clientX - startClientX;

        if (mode === "bottom") {
          const nextH = startH + dy;
          setYh((prev) => ({ ...prev, h: nextH }));
        } else if (mode === "top") {
          if (activeDockBelowChart) {
            const nextH = startH - dy;
            setYh((prev) => ({ ...prev, h: nextH }));
          } else {
            const nextY = startY + dy;
            const nextH = startH - dy;
            setYh((prev) => ({ ...prev, y: nextY, h: nextH }));
          }
        } else if (mode === "right") {
          const nextW = startW + dx;
          setXw({ w: nextW });
        } else if (mode === "corner") {
          const nextW = startW + dx;
          const nextH = startH + dy;
          setXw({ w: nextW });
          setYh((prev) => ({ ...prev, h: nextH }));
        }
      }
    };

    const onUp = () => {
      dragRef.current.dragging = false;
      dragRef.current.pointerId = null;
      resizeRef.current.resizing = false;
      resizeRef.current.mode = null;
      resizeRef.current.pointerId = null;
    };

    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    return () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
  }, [locked, activeDockBelowChart]);

  function startDrag(e) {
    if (locked) return;
    if (e.target?.dataset?.noDrag === "1") return;

    const r = widgetRef.current?.getBoundingClientRect();
    if (!r) return;

    dragRef.current.dragging = true;
    dragRef.current.dx = e.clientX - r.left;
    dragRef.current.dy = e.clientY - r.top;
    dragRef.current.startClientX = e.clientX;
    dragRef.current.startClientY = e.clientY;
    dragRef.current.startOffset = Number(dockOffsetY) || 0;
    dragRef.current.pointerId = e.pointerId ?? null;

    try {
      e.currentTarget.setPointerCapture?.(e.pointerId);
    } catch {
      // ignore
    }
  }

  function startResizeBottom(e) {
    if (locked) return;
    e.stopPropagation();

    const r = widgetRef.current?.getBoundingClientRect();
    if (!r) return;

    resizeRef.current.resizing = true;
    resizeRef.current.mode = "bottom";
    resizeRef.current.startClientY = e.clientY;
    resizeRef.current.startClientX = e.clientX;
    resizeRef.current.startY = r.top;
    resizeRef.current.startH = r.height;
    resizeRef.current.startW = r.width;
    resizeRef.current.pointerId = e.pointerId ?? null;

    try {
      e.currentTarget.setPointerCapture?.(e.pointerId);
    } catch {
      // ignore
    }
  }

  function startResizeTop(e) {
    if (locked) return;
    e.stopPropagation();

    const r = widgetRef.current?.getBoundingClientRect();
    if (!r) return;

    resizeRef.current.resizing = true;
    resizeRef.current.mode = "top";
    resizeRef.current.startClientY = e.clientY;
    resizeRef.current.startClientX = e.clientX;
    resizeRef.current.startY = r.top;
    resizeRef.current.startH = r.height;
    resizeRef.current.startW = r.width;
    resizeRef.current.pointerId = e.pointerId ?? null;

    try {
      e.currentTarget.setPointerCapture?.(e.pointerId);
    } catch {
      // ignore
    }
  }

  // NEW: width-only resize from right edge
  function startResizeRight(e) {
    if (locked) return;
    e.stopPropagation();

    const r = widgetRef.current?.getBoundingClientRect();
    if (!r) return;

    resizeRef.current.resizing = true;
    resizeRef.current.mode = "right";
    resizeRef.current.startClientY = e.clientY;
    resizeRef.current.startClientX = e.clientX;
    resizeRef.current.startY = r.top;
    resizeRef.current.startH = r.height;
    resizeRef.current.startW = r.width;
    resizeRef.current.pointerId = e.pointerId ?? null;

    try {
      e.currentTarget.setPointerCapture?.(e.pointerId);
    } catch {
      // ignore
    }
  }

  // NEW: corner resize (width + height)
  function startResizeCorner(e) {
    if (locked) return;
    e.stopPropagation();

    const r = widgetRef.current?.getBoundingClientRect();
    if (!r) return;

    resizeRef.current.resizing = true;
    resizeRef.current.mode = "corner";
    resizeRef.current.startClientY = e.clientY;
    resizeRef.current.startClientX = e.clientX;
    resizeRef.current.startY = r.top;
    resizeRef.current.startH = r.height;
    resizeRef.current.startW = r.width;
    resizeRef.current.pointerId = e.pointerId ?? null;

    try {
      e.currentTarget.setPointerCapture?.(e.pointerId);
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    const onResize = () => setYh((prev) => ({ ...prev }));
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // ─────────────────────────────────────────────────────────────
  // Clickable symbol cells → onPickMarket(...)
  // FIX: Always use the object signature so we cannot swap args accidentally.
  // ─────────────────────────────────────────────────────────────
  function callPickMarket(symbolCanon, venueMaybe, applyToTab = true, opts = {}) {
    const sym = String(symbolCanon || "").trim();
    if (!sym || typeof onPickMarket !== "function") return;

    const ven = String(venueMaybe || "").trim();

    // App.jsx handlePickMarket supports {symbolCanon, venue, applyToTab, opts}
    onPickMarket({
      symbolCanon: sym,
      venue: ven,
      applyToTab: !!applyToTab,
      opts: opts && typeof opts === "object" ? opts : {},
    });
  }

  function renderClickableSymbolCell({
    symbolCanon,
    venueMaybe,
    subLabel,
    subLabelTitle,
    subCopyText,
    onPicked,
    applyToTabOnPick = true,
    pickOpts = {},
    reapplyPickedDelayMs = 0,
  }) {
    const sym = String(symbolCanon || "").trim();
    const ven = String(venueMaybe || "").trim();

    const clickable = !!sym && !hideTableDataGlobal && typeof onPickMarket === "function";
    const text = hideTableDataGlobal ? "••••" : sym || "—";

    return (
      <td
        style={{
          ...sx.td,
          cursor: clickable ? "pointer" : sx.td?.cursor,
          textDecoration: clickable ? "underline" : "none",
          textUnderlineOffset: clickable ? 2 : undefined,
        }}
        title={clickable ? `Pick market: ${sym}${ven ? ` (${ven})` : ""}` : undefined}
        onClick={() => {
          if (!clickable) return;
          callPickMarket(sym, ven, applyToTabOnPick, pickOpts);
          try {
            const runPicked = () => {
              try {
                onPicked?.(sym, ven);
              } catch {
                // ignore
              }
            };
            if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
              window.requestAnimationFrame(() => {
                window.requestAnimationFrame(runPicked);
              });
            } else {
              window.setTimeout(runPicked, 0);
            }

            const delayMs = Number(reapplyPickedDelayMs);
            if (Number.isFinite(delayMs) && delayMs > 0) {
              window.setTimeout(runPicked, delayMs);
            }
          } catch {
            // ignore
          }
        }}
      >
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <span
            data-no-drag="1"
            style={{
              color: clickable ? pal.link : undefined,
              fontWeight: clickable ? 700 : undefined,
            }}
          >
            {text}
          </span>
          {subLabel ? (
            <span
              data-no-drag="1"
              style={{
                ...sx.muted,
                fontSize: 11,
                lineHeight: 1.1,
                userSelect: "text",
                textDecoration: subCopyText && !hideTableDataGlobal ? "underline dotted" : "none",
                textUnderlineOffset: subCopyText && !hideTableDataGlobal ? 2 : undefined,
                cursor: subCopyText && !hideTableDataGlobal ? "copy" : "text",
              }}
              title={hideTableDataGlobal ? undefined : (subLabelTitle || (subCopyText ? `Click to copy: ${subCopyText}` : undefined))}
              onClick={(e) => {
                e.stopPropagation();
                if (hideTableDataGlobal || !subCopyText) return;
                copyTextSafe(subCopyText);
                try {
                  window.prompt("Copy full mint / contract address pair:", String(subCopyText));
                } catch {
                  // ignore
                }
              }}
            >
              {hideTableDataGlobal ? "••••" : subLabel}
            </span>
          ) : null}
        </div>
      </td>
    );
  }

  // ─────────────────────────────────────────────────────────────
  // Balances overlib (hover panel): plumbing only (App.jsx wires getVenueMarkets)
  // ─────────────────────────────────────────────────────────────
  const hoverCloseTimerRef = useRef(null);
  const hoverReqRef = useRef({ key: "", seq: 0 });

  const [balOverlib, setBalOverlib] = useState(() => ({
    open: false,
    key: "", // track current request key to avoid races / allow reuse
    venue: "",
    asset: "",
    anchorX: 0,
    anchorY: 0,
    loading: false,
    error: "",
    markets: [],
  }));

  function clearHoverCloseTimer() {
    if (hoverCloseTimerRef.current) {
      clearTimeout(hoverCloseTimerRef.current);
      hoverCloseTimerRef.current = null;
    }
  }

  function scheduleCloseOverlib(ms = 120) {
    clearHoverCloseTimer();
    hoverCloseTimerRef.current = setTimeout(() => {
      setBalOverlib((prev) => ({ ...prev, open: false, loading: false }));
    }, ms);
  }

  // ✅ FIX: tolerate both string[] and object[] (prevents "[object Object]")
  function normalizeMarketsList(list) {
    const arr = Array.isArray(list) ? list : [];
    const out = [];

    for (const x of arr) {
      let raw = "";

      if (typeof x === "string") {
        raw = x;
      } else if (x && typeof x === "object") {
        raw = x.symbolCanon ?? x.symbol_canon ?? x.symbol ?? x.market ?? x.product_id ?? x.product ?? x.pair ?? x.id ?? "";
      }

      const s = normalizeMarketSymbolMaybe(raw);
      if (!s) continue;
      if (!out.includes(s)) out.push(s);
    }

    return out;
  }

  async function fetchMarketsForBalance({ venueMaybe, assetMaybe }) {
    const ven = String(venueMaybe || "").trim().toLowerCase();
    const asset = String(assetMaybe || "").trim().toUpperCase();
    if (!ven || !asset) return;

    // Keyed request to avoid races
    const key = `${ven}:${asset}`;
    hoverReqRef.current.seq += 1;
    const seq = hoverReqRef.current.seq;
    hoverReqRef.current.key = key;

    setBalOverlib((prev) => ({
      ...prev,
      open: true,
      key,
      venue: ven,
      asset,
      loading: true,
      error: "",
      markets: prev.key === key ? prev.markets : [],
    }));

    // If not yet wired, just show placeholder (no hard failure)
    if (typeof getVenueMarkets !== "function") {
      setBalOverlib((prev) => {
        if (hoverReqRef.current.seq !== seq || hoverReqRef.current.key !== key) return prev;
        return {
          ...prev,
          open: true,
          key,
          venue: ven,
          asset,
          loading: false,
          error: "Market list not wired yet (needs App.jsx + api.js + backend endpoint).",
          markets: [],
        };
      });
      return;
    }

    try {
      const resp = await getVenueMarkets({ venue: ven, asset });
      const markets = Array.isArray(resp) ? resp : Array.isArray(resp?.markets) ? resp.markets : [];
      const normalized = normalizeMarketsList(markets);

      setBalOverlib((prev) => {
        if (hoverReqRef.current.seq !== seq || hoverReqRef.current.key !== key) return prev;
        return {
          ...prev,
          open: true,
          key,
          venue: ven,
          asset,
          loading: false,
          error: "",
          markets: normalized,
        };
      });
    } catch (e) {
      const msg = e?.message ? String(e.message) : "Failed to load markets";
      setBalOverlib((prev) => {
        if (hoverReqRef.current.seq !== seq || hoverReqRef.current.key !== key) return prev;
        return {
          ...prev,
          open: true,
          key,
          venue: ven,
          asset,
          loading: false,
          error: msg,
          markets: [],
        };
      });
    }
  }

  function openOverlibAt({ clientX, clientY, venueMaybe, assetMaybe }) {
    clearHoverCloseTimer();

    // Position inside widget (absolute)
    const wr = widgetRef.current?.getBoundingClientRect?.();
    const x0 = Number(clientX);
    const y0 = Number(clientY);
    const x = wr && Number.isFinite(x0) ? x0 - wr.left : 12;
    const y = wr && Number.isFinite(y0) ? y0 - wr.top : 140;

    setBalOverlib((prev) => ({
      ...prev,
      open: true,
      venue: String(venueMaybe || "").trim().toLowerCase(),
      asset: String(assetMaybe || "").trim().toUpperCase(),
      anchorX: clamp(Math.round(x), 10, Math.max(10, Math.round(geom.w - 260))),
      anchorY: clamp(Math.round(y + 14), 80, Math.max(80, Math.round(geom.h - 220))),
    }));

    // Kick off load (idempotent by key)
    fetchMarketsForBalance({ venueMaybe, assetMaybe });
  }

  function renderBalancesOverlib() {
    if (!balOverlib.open) return null;

    const ven = balOverlib.venue || "";
    const asset = balOverlib.asset || "";

    const panel = {
      position: "fixed",
      left: balOverlib.anchorX,
      top: balOverlib.anchorY,
      width: 320,
      maxWidth: Math.max(260, geom.w - 40),
      border: `1px solid ${pal.border}`,
      background: pal.panelBg,
      borderRadius: 12,
      boxShadow: `0 18px 40px ${pal.shadow}`,
      padding: 10,
      zIndex: 60,
      color: pal.text,
    };

    const header = { display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, marginBottom: 8 };
    const title = { fontWeight: 900, fontSize: 13 };
    const sub = { fontSize: 11, color: pal.muted };

    const chip = (disabled) => ({
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      padding: "6px 8px",
      borderRadius: 10,
      border: `1px solid ${pal.border}`,
      background: pal.widgetBg2,
      fontSize: 12,
      cursor: disabled ? "default" : "pointer",
      opacity: disabled ? 0.55 : 1,
      userSelect: "none",
    });

    return (
      <div data-no-drag="1" style={panel} onMouseEnter={() => clearHoverCloseTimer()} onMouseLeave={() => scheduleCloseOverlib(160)}>
        <div style={header}>
          <div>
            <div style={title}>Markets for {asset || "—"}</div>
            <div style={sub}>
              Venue: <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : ven || "—"}</b>
            </div>
          </div>

          <button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => setBalOverlib((p) => ({ ...p, open: false }))} title="Close">
            Close
          </button>
        </div>

        {balOverlib.loading ? (
          <div style={{ ...sx.muted, fontSize: 12 }}>Loading markets…</div>
        ) : balOverlib.error ? (
          <div style={{ ...sx.muted, fontSize: 12 }}>
            {hideTableDataGlobal ? "••••" : balOverlib.error}
            <div style={{ marginTop: 6, ...sx.muted, fontSize: 11 }}>
              Next: wire <b>getVenueMarkets</b> in App.jsx and implement the backend endpoint.
            </div>
          </div>
        ) : balOverlib.markets.length ? (
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {balOverlib.markets.slice(0, 60).map((m) => {
              const disabled = hideTableDataGlobal || !m || typeof onPickMarket !== "function";
              return (
                <div
                  key={m}
                  data-no-drag="1"
                  style={chip(disabled)}
                  title={disabled ? "Hidden/disabled" : `Open: ${m} (${ven})`}
                  onClick={() => {
                    if (disabled) return;
                    callPickMarket(m, ven);
                    setBalOverlib((p) => ({ ...p, open: false }));
                  }}
                >
                  <span style={{ color: disabled ? pal.muted : pal.link, fontWeight: 800 }}>{hideTableDataGlobal ? "••••" : m}</span>
                </div>
              );
            })}
          </div>
        ) : (
          <div style={{ ...sx.muted, fontSize: 12 }}>No markets returned.</div>
        )}
      </div>
    );
  }

  // NEW (Balances-only): clickable asset cell that routes to inferred market symbol
  function renderBalanceAssetCell(b) {
    const asset = String(b?.asset || "").trim();
    const venRow = String(b?.venue || "").trim();
    const market = normalizeMarketSymbolMaybe(inferBalanceMarketSymbol(b));

    // Only allow clicking if we have a plausible market symbol (contains a dash) and an onPickMarket handler.
    const clickable =
      !hideTableDataGlobal &&
      typeof onPickMarket === "function" &&
      !!market &&
      market.includes("-") &&
      market.length >= 5 &&
      market.length <= 30;

    const mintShort = String(b?.mint_short || "").trim();
    const mintFallback = String(b?.mint || "").trim();
    const mintShort2 = mintShort || (mintFallback ? `${mintFallback.slice(0, 6)}…` : "");
    // Only show the mint line when we have a symbol label (otherwise we'd just duplicate the mint twice).
    const showMint = !hideTableDataGlobal && !!mintShort2 && !!String(b?.symbol || "").trim();
    const display = hideTableDataGlobal ? "••••" : asset || "—";

    // Venue hint: if viewing All venues, use the row venue; otherwise use current venue selection.
    const venueMaybe = venue === ALL_VENUES_VALUE ? venRow || "" : String(venue || "").trim();

    const title = clickable ? `Pick market: ${market}${venueMaybe ? ` (${venueMaybe})` : ""}` : undefined;

    const assetUpper = String(asset || "").trim().toUpperCase();

    return (
      <td
        style={{
          ...sx.td,
          cursor: clickable ? "pointer" : sx.td?.cursor,
          textDecoration: clickable ? "underline" : "none",
          textUnderlineOffset: clickable ? 2 : undefined,
        }}
        title={title}
        onMouseEnter={(e) => {
          if (hideTableDataGlobal) return;
          if (!assetUpper || !venueMaybe) return;
          openOverlibAt({
            clientX: e.clientX,
            clientY: e.clientY,
            venueMaybe,
            assetMaybe: assetUpper,
          });
        }}
        onMouseMove={(e) => {
          // keep it “following” without jitter
          if (!balOverlib.open) return;
          if (balOverlib.asset !== assetUpper) return;
          const wr = widgetRef.current?.getBoundingClientRect?.();
          if (!wr) return;
          const x = clamp(Math.round(e.clientX - wr.left), 10, Math.max(10, Math.round(geom.w - 260)));
          const y = clamp(Math.round(e.clientY - wr.top + 14), 80, Math.max(80, Math.round(geom.h - 220)));
          setBalOverlib((p) => ({ ...p, anchorX: x, anchorY: y }));
        }}
        onMouseLeave={() => scheduleCloseOverlib(140)}
        onClick={() => {
          if (!clickable) return;
          callPickMarket(market, venueMaybe);
        }}
      >
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <span
            data-no-drag="1"
            style={{
              color: clickable ? pal.link : undefined,
              fontWeight: clickable ? 700 : undefined,
            }}
          >
            {display}
          </span>
          {showMint ? <span style={{ fontSize: 11, opacity: 0.7 }}>{mintShort2}</span> : null}
        </div>
      </td>
    );
  }

  // ─────────────────────────────────────────────────────────────
  // Column ordering for All Orders table
  // ─────────────────────────────────────────────────────────────
  const [columns, setColumns] = useState(() => {
    const saved = safeParseJson(localStorage.getItem(LS_KEY) || "");
    return sanitizeColumns(saved);
  });

  const [columnPreset, setColumnPreset] = useState(() => {
    const saved = safeParseJson(localStorage.getItem(LS_KEY) || "");
    const cols = sanitizeColumns(saved);
    if (arraysEqual(cols, LEGACY_ORDER) || arraysEqual(cols, LEGACY_ORDER_V1)) return "legacy";
    if (arraysEqual(cols, PREFERRED_ORDER) || arraysEqual(cols, PREFERRED_ORDER_V1)) return "preferred";
    return "custom";
  });

    // Migration: if a user is on the old preset layouts, upgrade them to include the new Net After Tax column.
  // - Preferred/Legacy presets get auto-upgraded.
  // - Custom layouts are left untouched; the new column can be added via the Column Manager.
  useEffect(() => {
    // Migration A: upgrade old preset layouts (V1) to the current preset definitions.
    if (arraysEqual(columns, PREFERRED_ORDER_V1)) {
      setColumns([...PREFERRED_ORDER]);
      return;
    }
    if (arraysEqual(columns, LEGACY_ORDER_V1)) {
      setColumns([...LEGACY_ORDER]);
      return;
    }

    // Migration B (one-time): if the user is on a custom column layout, inject new columns
    // (Tax + Net After Tax) into their existing order so they become visible immediately.
    // We only do this once per browser profile; afterwards, the Column Manager controls it.
    const alreadyMigrated = localStorage.getItem(LS_AO_COLS_MIG_V2) === "1";
    if (alreadyMigrated) return;

    const hasNet = columns.includes(COLS.net);
    const hasTax = columns.includes(COLS.tax);
    const hasNetAfterTax = columns.includes(COLS.netAfterTax);

    if (!hasTax || !hasNetAfterTax) {
      const next = [...columns];

      // Insert after Net (or append if Net not found).
      const idxNet = next.indexOf(COLS.net);

      // Add Tax first
      if (!hasTax) {
        const insertAt = idxNet >= 0 ? idxNet + 1 : next.length;
        next.splice(insertAt, 0, COLS.tax);
      }

      // Add Net After Tax after Tax (if present) else after Net
      if (!hasNetAfterTax) {
        const idxTax = next.indexOf(COLS.tax);
        const baseIdx = idxTax >= 0 ? idxTax : idxNet;
        const insertAt = baseIdx >= 0 ? baseIdx + 1 : next.length;
        next.splice(insertAt, 0, COLS.netAfterTax);
      }

      setColumns(next);
    }

    localStorage.setItem(LS_AO_COLS_MIG_V2, "1");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

// Phase 2.2 (REQUIRED): Actions column is required and must never disappear.
  useEffect(() => {
    const sanitized = sanitizeColumns(columns);

    // If current state differs, self-heal immediately.
    if (!arraysEqual(columns, sanitized)) {
      setColumns(sanitized);
      return;
    }

    localStorage.setItem(LS_KEY, JSON.stringify(columns));
    if (arraysEqual(columns, LEGACY_ORDER) || arraysEqual(columns, LEGACY_ORDER_V1)) setColumnPreset("legacy");
    else if (arraysEqual(columns, PREFERRED_ORDER) || arraysEqual(columns, PREFERRED_ORDER_V1)) setColumnPreset("preferred");
    else setColumnPreset("custom");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [columns]);

  const [colMgrOpen, setColMgrOpen] = useState(() => {
    const v = safeParseJson(localStorage.getItem(LS_COLMGR_OPEN_KEY) || "");
    return !!v;
  });

  useEffect(() => {
    localStorage.setItem(LS_COLMGR_OPEN_KEY, JSON.stringify(colMgrOpen));
  }, [colMgrOpen]);

  const availableToAdd = useMemo(() => {
    const all = Object.values(COLS);
    const set = new Set(columns);
    return all.filter((c) => !set.has(c));
  }, [columns]);

  function setPreset(p) {
    if (p === "preferred") setColumns([...PREFERRED_ORDER]);
    else if (p === "legacy") setColumns([...LEGACY_ORDER]);
  }

  function resetColumnsToPreferred({ clearStorage = true } = {}) {
    if (clearStorage) {
      try {
        localStorage.removeItem(LS_KEY);
      } catch {
        // ignore
      }
    }
    setColumns([...PREFERRED_ORDER]);
  }

  function moveViewedNearLeft() {
    setColumns((prev) => {
      const copy = [...(prev || [])];
      const i = copy.indexOf(COLS.viewed);
      if (i < 0) return copy;

      // after created, closed, actions
      const target = 3;
      if (i === target) return copy;
      copy.splice(i, 1);
      const t = clamp(target, 0, copy.length);
      copy.splice(t, 0, COLS.viewed);
      return copy;
    });
  }

  function addColumn(col) {
    if (!col) return;
    setColumns((prev) => (prev.includes(col) ? prev : [...prev, col]));
  }

  function removeColumn(col) {
    // hard-block removing Actions; it is a required functional column.
    if (col === COLS.actions) return;
    setColumns((prev) => prev.filter((x) => x !== col));
  }

  function moveColumn(col, dir) {
    setColumns((prev) => {
      const i = prev.indexOf(col);
      if (i < 0) return prev;
      const j = i + dir;
      if (j < 0 || j >= prev.length) return prev;
      const copy = [...prev];
      const tmp = copy[i];
      copy[i] = copy[j];
      copy[j] = tmp;
      return copy;
    });
  }

  function orderBucket(o) {
    const st = normalizeStatus(pickOrderStatus(o));
    return o.status_bucket || (isTerminalStatus?.(st) ? "terminal" : "open");
  }

  // Phase 2.2: palette + existing terminal dimming
  function orderRowStyle(o) {
    const bucket = orderBucket(o);
    const st = normalizeStatus(pickOrderStatus(o));
    const terminal = isTerminalBucket?.(bucket) || isTerminalStatus?.(st);

    const base = statusRowStyle({
      statusLower: normalizeStatusLower(st),
      bucketLower: normalizeStatusLower(bucket),
      dim: !!hideTableDataGlobal,
    });

    // Preserve prior “terminal de-emphasis” intent without overriding the palette
    if (terminal && !hideTableDataGlobal) {
      return { ...base, opacity: 0.85 };
    }
    return base;
  }

  // Local DB orders: palette based on o.status
  function localOrderRowStyle(o) {
    const st = normalizeStatus(o?.status || "");
    const base = statusRowStyle({
      statusLower: normalizeStatusLower(st),
      bucketLower: "",
      dim: !!hideTableDataGlobal,
    });

    // Keep filled/canceled/rejected slightly dimmer than open/acked for readability
    const kind = classifyStatusKind(normalizeStatusLower(st), "");
    if (
      !hideTableDataGlobal &&
      (kind === "filled" || kind === "canceled" || kind === "rejected" || kind === "terminal")
    ) {
      return { ...base, opacity: 0.92 };
    }
    return base;
  }

  // ─────────────────────────────────────────────────────────────
  // High-precision number formatter (10 decimals) for price/qty columns
  // Uses fmtNum if available (supports either signature: (v, opts) or (v, maxDecimals))
  // ─────────────────────────────────────────────────────────────
  function fmtEcoHi(v) {
    if (v === null || v === undefined) return "—";
    // preserve empty-string handling
    const s0 = typeof v === "string" ? v.trim() : v;
    if (s0 === "") return "—";

    try {
      if (typeof fmtNum === "function") {
        // Prefer object signature (most flexible)
        try {
          const out = fmtNum(v, { maxDecimals: 10 });
          if (out !== undefined && out !== null) return out;
        } catch {
          // ignore
        }
        // Fallback: numeric signature
        try {
          const out = fmtNum(v, 10);
          if (out !== undefined && out !== null) return out;
        } catch {
          // ignore
        }
      }
    } catch {
      // ignore
    }

    try {
      return String(v);
    } catch {
      return "—";
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Phase 2.1: Confirm/Cancel modal before cancel action
  // ─────────────────────────────────────────────────────────────
  const [cancelModal, setCancelModal] = useState(() => ({
    open: false,
    kind: "", // "unified" | "local"
    row: null,
    title: "",
  }));

  function closeCancelModal() {
    setCancelModal({ open: false, kind: "", row: null, title: "" });
  }

  async function submitManualCancelOrder() {
    const oid = String(manualCancelOrderId || "").trim();
    if (!oid || manualCancelBusy) return;
    try {
      onManualCancelError?.(null);
      if (manualCancelVenue !== "solana_jupiter") {
        throw new Error(`Manual cancel by order id is not wired for venue "${manualCancelVenue}" yet.`);
      }
      await doManualJupiterCancel?.(oid, { markCanceled: true });
      setManualCancelOrderId?.("");
      setShowManualCancelModal?.(false);
    } catch (e) {
      const msg = e?.response?.data?.detail || e?.message || "Failed to cancel order";
      onManualCancelError?.(String(msg));
    }
  }

  function openCancelModalUnified(o) {
    if (!o) return;
    const sym = pickSymbolCanon(o) || o.symbol || "";
    const ven = String(o.venue || aoVenue || (venue === ALL_VENUES_VALUE ? "" : venue) || "").trim();
    const hdr = `Cancel order${sym ? `: ${sym}` : ""}${ven ? ` (${ven})` : ""}?`;
    setCancelModal({ open: true, kind: "unified", row: o, title: hdr });
  }

  function openCancelModalLocal(o) {
    if (!o) return;
    const sym = pickSymbolCanon(o) || o.symbol || "";
    const ven = String(o.venue || "").trim();
    const hdr = `Cancel local order${sym ? `: ${sym}` : ""}${ven ? ` (${ven})` : ""}?`;
    setCancelModal({ open: true, kind: "local", row: o, title: hdr });
  }

  // ESC closes; ENTER confirms (when modal open)
  useEffect(() => {
    if (!cancelModal.open) return;

    const onKey = (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        closeCancelModal();
      }
      if (e.key === "Enter") {
        // Do not fire when focus is inside an input/textarea/select.
        const tag = String(e.target?.tagName || "").toLowerCase();
        if (tag === "input" || tag === "textarea" || tag === "select") return;
        e.preventDefault();
        try {
          document.getElementById("uttCancelModalConfirmBtn")?.click?.();
        } catch {
          // ignore
        }
      }
    };

    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [cancelModal.open]);

  // ─────────────────────────────────────────────────────────────
  // Cancel helpers (make Local cancel behave exactly like Unified)
  // ─────────────────────────────────────────────────────────────
  function isCancelableStatusAnyCase(st) {
    const s0 = String(st ?? "").trim();
    if (!s0) return false;

    const lo = s0.toLowerCase();
    const up = s0.toUpperCase();

    // If App.jsx provided a canonical checker, use it.
    if (typeof isCancelableStatus === "function") {
      return !!(isCancelableStatus(s0) || isCancelableStatus(lo) || isCancelableStatus(up));
    }

    // Fallback heuristic (covers typical “can cancel” live states)
    // Keep conservative: only allow clearly live/working states.
    if (
      lo === "open" ||
      lo === "acked" ||
      lo === "pending" ||
      lo === "new" ||
      lo === "live" ||
      lo === "active" ||
      lo === "working" ||
      lo.includes("await") ||
      lo.includes("queue") ||
      lo.includes("accept") ||
      lo.includes("partial") ||
      lo.includes("partially")
    ) {
      return true;
    }

    // Explicit terminal-ish states should not be cancelable.
    const kind = classifyStatusKind(lo, "");
    if (kind === "filled" || kind === "canceled" || kind === "rejected" || kind === "terminal") return false;

    return false;
  }

  // Unified cancel handling (row-level)
  function isUnifiedCancelableLegacy(o) {
    if (!o) return false;

    const bucket = orderBucket(o);
    const st = normalizeStatus(pickOrderStatus(o));

    if (isTerminalBucket?.(bucket) || isTerminalStatus?.(st)) return false;

    if (typeof isCanceledStatus === "function" && isCanceledStatus(st)) return false;

    if (typeof isCancelableStatus === "function") {
      const s0 = String(st ?? "").trim();
      if (s0) {
        const stLower = s0.toLowerCase();
        const stUpper = s0.toUpperCase();
        return !!(isCancelableStatus(s0) || isCancelableStatus(stLower) || isCancelableStatus(stUpper));
      }
    }

    return true;
  }

  // prevent double-click cancel storms per-row
  const [cancelingKeys, setCancelingKeys] = useState(() => ({}));

  function rowCancelKey(o) {
    const cancelRef = String(o?.cancel_ref || o?.cancelRef || "").trim();
    if (cancelRef) return `ref:${cancelRef}`;

    const srcRaw = String(o?.source || "").toUpperCase().trim();
    const isLocalish = srcRaw === "LOCAL" || (!srcRaw && o?.id != null);
    if (isLocalish && o?.id != null) return `local:${o.id}`;

    const vid = String(o?.venue_order_id || o?.venueOrderId || o?.order_id || o?.orderId || "").trim();
    if (vid) return `vid:${vid}`;

    return `idx:${String(o?.created_at || "")}:${String(o?.symbol_canon || o?.symbol || "")}`;
  }

  // ─────────────────────────────────────────────────────────────
  // FIX: refresh balances after a successful cancel (releases holds)
  // We do an immediate refresh, plus one delayed refresh to absorb venue eventual-consistency.
  // ─────────────────────────────────────────────────────────────
  const postCancelTimerRef = useRef(null);
  useEffect(() => {
    return () => {
      if (postCancelTimerRef.current) {
        clearTimeout(postCancelTimerRef.current);
        postCancelTimerRef.current = null;
      }
    };
  }, []);

  async function runRefreshBalances(venueMaybe) {
    const ven = String(venueMaybe || "").trim();
    // For Solana venues, skip CEX balances refresh (it 422s) and refresh on-chain balances instead.
    if (String(ven || venue || "").toLowerCase().startsWith("solana")) {
      await refreshSolanaOnchainBalances();
      return;
    }

    if (typeof doRefreshBalances !== "function") return;
    try {
      // If App.jsx supports an optional venue arg, respect it.
      if (doRefreshBalances.length >= 1 && ven) await doRefreshBalances(ven);
      else await doRefreshBalances();
    } catch {
      // ignore (balances refresh is best-effort)
    }
  }

  async function postCancelRefresh(venueMaybe) {
  const tok = getAuthToken();
  if (!tok) {
    toast?.warning?.('Login required to cancel orders.');
    return;
  }
    // Immediate refresh
    await runRefreshBalances(venueMaybe);

    // Delayed refresh (helps for exchanges that release holds a moment after cancel ack)
    try {
      if (postCancelTimerRef.current) clearTimeout(postCancelTimerRef.current);
    } catch {
      // ignore
    }

    postCancelTimerRef.current = setTimeout(() => {
      runRefreshBalances(venueMaybe);
    }, 1200);
  }

  async function cancelUnifiedOrder(o) {
    if (!o) return;

    const k = rowCancelKey(o);
    if (cancelingKeys[k]) return;

    setCancelingKeys((prev) => ({ ...prev, [k]: true }));

    try {
      const cancelRef = String(o.cancel_ref || o.cancelRef || "").trim();
      const ven = String(o?.venue || aoVenue || "").trim();

      // Preferred: unified cancel endpoint using cancel_ref (works for LOCAL and VENUE rows)
      if (cancelRef && typeof doCancelUnifiedOrder === "function") {
        const resp = await doCancelUnifiedOrder({ cancel_ref: cancelRef, row: o });

        if (resp && typeof resp === "object" && resp.ok === false) {
          const msg = resp.error ? String(resp.error) : "Cancel failed";
          try {
            window.alert(msg);
          } catch {
            // ignore
          }
          return;
        }

        // Refresh orders + balances (recompute holds)
        await doSyncAndLoadAllOrders?.();
        await postCancelRefresh(ven);
        return;
      }

      // Fallback: LOCAL cancel by id (treat missing source but present id as local)
      const src = String(o.source || "").toUpperCase().trim();
      const isLocalish = src === "LOCAL" || (!src && o?.id != null);

      if (isLocalish) {
        if (typeof doCancelOrder === "function" && o.id != null) {
          await doCancelOrder(o.id);
          await doSyncAndLoadAllOrders?.();
          await postCancelRefresh(ven);
        }
        return;
      }

      // Fallback: legacy venue cancel handler (if your App.jsx still expects order row)
      if (typeof doCancelUnifiedOrder === "function") {
        const resp = await doCancelUnifiedOrder(o);
        if (resp && typeof resp === "object" && resp.ok === false) {
          const msg = resp.error ? String(resp.error) : "Cancel failed";
          try {
            window.alert(msg);
          } catch {
            // ignore
          }
          return;
        }
        await doSyncAndLoadAllOrders?.();
        await postCancelRefresh(ven);
      }
    } finally {
      setCancelingKeys((prev) => {
        const next = { ...prev };
        delete next[k];
        return next;
      });
    }
  }

  // Local Orders cancel: use same preferred path as Unified (cancel_ref → unified cancel endpoint),
  // else fall back to local id cancel. This fixes cases where Local rows lack usable `id` but do have `cancel_ref`.
  async function cancelLocalOrder(o) {
    if (!o) return;

    const k = rowCancelKey(o);
    if (cancelingKeys[k]) return;

    setCancelingKeys((prev) => ({ ...prev, [k]: true }));

    try {
      const cancelRef = String(o.cancel_ref || o.cancelRef || "").trim();
      const ven = String(o?.venue || "").trim();

      if (cancelRef && typeof doCancelUnifiedOrder === "function") {
        const resp = await doCancelUnifiedOrder({ cancel_ref: cancelRef, row: o });
        if (resp && typeof resp === "object" && resp.ok === false) {
          const msg = resp.error ? String(resp.error) : "Cancel failed";
          try {
            window.alert(msg);
          } catch {
            // ignore
          }
          return;
        }

        // Keep Local + Unified views consistent after a cancel.
        await doLoadOrders?.();
        await doSyncAndLoadAllOrders?.();
        await postCancelRefresh(ven);
        return;
      }

      if (typeof doCancelOrder === "function" && o.id != null) {
        await doCancelOrder(o.id);
        await doLoadOrders?.();
        await doSyncAndLoadAllOrders?.();
        await postCancelRefresh(ven);
        return;
      }

      // If we reach here, we have no valid cancel route.
      try {
        window.alert("Cannot cancel: missing cancel_ref and local id.");
      } catch {
        // ignore
      }
    } finally {
      setCancelingKeys((prev) => {
        const next = { ...prev };
        delete next[k];
        return next;
      });
    }
  }

  async function confirmCancelFromModal() {
    const o = cancelModal.row;
    const kind = cancelModal.kind;

    if (!cancelModal.open || !o) return;

    closeCancelModal();

    if (kind === "unified") {
      await cancelUnifiedOrder(o);
      return;
    }

    if (kind === "local") {
      await cancelLocalOrder(o);
    }
  }

  function pickOrderTaxUsdMaybe(o) {
  if (!o || typeof o !== "object") return null;

  // Tax / withholding fields may evolve as the guardrail work lands.
  // Support a small set of plausible names; return null when absent.
  const candidates = [
    o.tax_usd,
    o.taxUsd,
    o.tax,
    o.tax_withheld_usd,
    o.taxWithheldUsd,
    o.tax_withheld,
    o.taxWithheld,
    o.withholding_tax_usd,
    o.withholdingTaxUsd,
    o.tax_reserve_usd,
    o.taxReserveUsd,
    o.proceeds_tax_usd,
    o.proceedsTaxUsd,
    o.realized_tax_usd,
    o.realizedTaxUsd,
  ];

  for (const v of candidates) {
    if (v === null || v === undefined || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

// Back-compat helper if you still want a numeric tax elsewhere.
function pickOrderTaxUsd(o) {
  const v = pickOrderTaxUsdMaybe(o);
  return v === null ? 0 : v;
}

  
// Gain/PnL helpers (USD). Used for UI-only withholding on realized gains.
function pickOrderGainUsdMaybe(o) {
  if (!o || typeof o !== "object") return null;

  // Prefer an explicit realized gain / pnl field when present.
  const candidates = [
    o.realized_pnl_usd,
    o.realizedPnlUsd,
    o.pnl_usd,
    o.pnlUsd,
    o.gain_usd,
    o.gainUsd,
    o.realized_gain_usd,
    o.realizedGainUsd,
  ];

  for (const v of candidates) {
    if (v === null || v === undefined || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }

  // Derive when possible: proceeds - basis - fee (fee is optional).
  // This increases coverage for rows where backend emits proceeds/basis but not pnl.
  const pickNum = (x) => {
    if (x === null || x === undefined || x === "") return null;
    const n = Number(x);
    return Number.isFinite(n) ? n : null;
  };

  const proceeds = [
    o.realized_proceeds_usd,
    o.realizedProceedsUsd,
    o.proceeds_usd,
    o.proceedsUsd,
  ].map(pickNum).find((n) => n !== null);

  const basis = [
    o.realized_basis_used_usd,
    o.realizedBasisUsedUsd,
    o.basis_used_usd,
    o.basisUsedUsd,
    o.cost_basis_usd,
    o.costBasisUsd,
  ].map(pickNum).find((n) => n !== null);

  if (proceeds === null || basis === null) return null;

  const feeUsd = [
    o.realized_fee_usd,
    o.realizedFeeUsd,
    o.fee_usd,
    o.feeUsd,
  ].map(pickNum).find((n) => n !== null);

  let g = proceeds - basis;
  if (feeUsd !== null) g -= feeUsd;

  // A negative value indicates a realized loss; returning it allows callers
  // to decide whether to clamp to 0 for withholding purposes.
  return Number.isFinite(g) ? g : null;
}

function pickOrderSymbolStr(o) {
  if (!o || typeof o !== "object") return "";
  const candidates = [
    o.symbol,
    o.symbol_canon,
    o.product_id,
    o.productId,
    o.pair,
    o.trading_pair,
    o.tradingPair,
    o.instrument,
    o.market,
  ];
  for (const v of candidates) {
    if (v === null || v === undefined || v === "") continue;
    return String(v);
  }
  return "";
}

function isUsdQuotedSymbol(sym) {
  const s = String(sym || "").trim();
  if (!s) return false;
  // Normalize common delimiters: "DOGE-USD", "DOGE/USD", "DOGE_USD"
  const norm = s.replace(/\s+/g, "").replace(/\//g, "-").replace(/_/g, "-").toUpperCase();
  const parts = norm.split("-").filter(Boolean);
  if (parts.length < 2) return false;
  const quote = parts[parts.length - 1];
  return quote === "USD";
}
function renderAllOrdersHeader(col) {
    const th = { ...sx.th, whiteSpace: "nowrap" };

    if (col === COLS.created) {
      return (
        <th style={{ ...th, ...sx.linkyHeader }} onClick={() => toggleAllSort?.("created_at")} title="Sort by created_at">
          <SortHeader label="Created" active={aoSortField === "created_at"} dir={aoSortDir} />
        </th>
      );
    }

    if (col === COLS.closed) {
      return (
        <th style={{ ...th, ...sx.linkyHeader }} onClick={() => toggleAllSort?.("closed_at")} title="Sort by closed_at">
          <SortHeader label="Closed" active={aoSortField === "closed_at"} dir={aoSortDir} />
        </th>
      );
    }

    if (col === COLS.actions) return <th style={th}>Actions</th>;
    if (col === COLS.viewed) return <th style={th}>Viewed</th>;

    if (col === COLS.symbol) {
      return (
        <th style={{ ...th, ...sx.linkyHeader }} onClick={() => toggleAllSort?.("symbol")} title="Sort by symbol">
          <SortHeader label="Symbol" active={aoSortField === "symbol"} dir={aoSortDir} />
        </th>
      );
    }

    if (col === COLS.side) return <th style={th}>Side</th>;
    if (col === COLS.qty) return <th style={th}>Quantity</th>;
    if (col === COLS.gross) return <th style={th}>Gross</th>;
    if (col === COLS.net) return <th style={th}>Net</th>;
    if (col === COLS.tax) return <th style={th}>Tax</th>;
    if (col === COLS.netAfterTax) return <th style={th}>net-a/tx</th>;
    if (col === COLS.fee) return <th style={th}>Fee</th>;

    if (col === COLS.limit) {
      return (
        <th style={{ ...th, ...sx.linkyHeader }} onClick={() => toggleAllSort?.("limit_price")} title="Sort by limit_price">
          <SortHeader label="Limit" active={aoSortField === "limit_price"} dir={aoSortDir} />
        </th>
      );
    }

    if (col === COLS.status) {
      return (
        <th style={{ ...th, ...sx.linkyHeader }} onClick={() => toggleAllSort?.("status")} title="Sort by status">
          <SortHeader label="Status" active={aoSortField === "status"} dir={aoSortDir} />
        </th>
      );
    }

    if (col === COLS.type) return <th style={th}>Type</th>;
    if (col === COLS.source) return <th style={th}>Source</th>;
    if (col === COLS.venue) return <th style={th}>Venue</th>;
    if (col === COLS.bucket) return <th style={th}>Bucket</th>;

    return <th style={th}>{String(col)}</th>;
  }

  function renderAllOrdersCell(o, col) {
    const td = sx.td;

    const created = o.created_at ? fmtTime?.(o.created_at) : "—";
    const closed = o.closed_at ? fmtTime?.(o.closed_at) : "—";

    const gross = calcGrossTotal?.(o);
    const fee = calcFee?.(o);
    const net = calcNetTotal?.(o);

    
const backendTax = pickOrderTaxUsdMaybe(o);

// Mode A fallback withholding:
// - Only when enabled
// - Only when backend tax is absent
// - Only for FILLED SELL orders
// - Only for USD-quoted pairs (quote == USD)
// - Prefer taxing realized gain (if provided); optionally allow taxing net when gain is unknown.
const symForTax = pickOrderSymbolStr(o);
const isUsdQuote = isUsdQuotedSymbol(symForTax);

const gainUsd = pickOrderGainUsdMaybe(o);

const shouldFallbackTax =
  !!aoTaxWithholdEnabled &&
  backendTax === null &&
  isUsdQuote &&
  isFilledSellOrder(o) &&
  net !== null &&
  net !== undefined;

// Determine the taxable base.
// Prefer realized gain; if unavailable, either skip (default) or fall back to net proceeds (optional).
const taxableBase =
  shouldFallbackTax
    ? (gainUsd !== null
        ? Math.max(0, Number(gainUsd))
        : aoTaxAssumeNetWhenGainUnknown
          ? Math.max(0, Number(net))
          : null)
    : null;

const rate = Number(aoTaxCombinedPct) / 100;
const fallbackTax =
  taxableBase !== null && Number.isFinite(rate) && rate > 0 ? Math.max(0, Number(taxableBase) * rate) : null;

// Use backend tax if present, otherwise fallback tax if applicable, otherwise 0 for net-a/tx math.
const taxUsed = backendTax !== null ? backendTax : fallbackTax !== null ? fallbackTax : 0;

const netAfterTax = net === null || net === undefined ? null : Number(net) - Number(taxUsed);
const bucket = orderBucket(o);
    const st = normalizeStatus(pickOrderStatus(o));

    if (col === COLS.created) return <td style={td}>{maskMaybe?.(created)}</td>;
    if (col === COLS.closed) return <td style={td}>{maskMaybe?.(closed)}</td>;

    if (col === COLS.actions) {
      const terminal = isTerminalBucket?.(bucket) || isTerminalStatus?.(st);

      const src = String(o.source || "").toUpperCase().trim();
      const cancelRef = String(o.cancel_ref || o.cancelRef || "").trim();

      const hasUnifiedHandler = typeof doCancelUnifiedOrder === "function" && !!cancelRef;
      const hasLocalHandler = src === "LOCAL" && typeof doCancelOrder === "function" && o.id != null;

      const canCancelBackend = o.can_cancel === true || o.canCancel === true;

      const canCancelPreferred = !terminal && (hasUnifiedHandler || hasLocalHandler);
      const canCancelLegacy =
        !terminal &&
        typeof doCancelUnifiedOrder === "function" &&
        !hasUnifiedHandler &&
        !hasLocalHandler &&
        isUnifiedCancelableLegacy(o);

      const canCancel = (canCancelBackend && (canCancelPreferred || canCancelLegacy)) || canCancelPreferred || canCancelLegacy;

      const k = rowCancelKey(o);
      const isCanceling = !!cancelingKeys[k];

      const disabled = !!hideTableDataGlobal || !canCancel || isCanceling;

      const title = hideTableDataGlobal
        ? "Hidden"
        : terminal
          ? "Order is terminal"
          : isCanceling
            ? "Canceling…"
            : canCancelPreferred
              ? hasUnifiedHandler
                ? "Cancel (uses cancel_ref)"
                : "Cancel (local order id)"
              : canCancelLegacy
                ? "Cancel (legacy handler: pass row to doCancelUnifiedOrder)"
                : !cancelRef && src !== "LOCAL"
                  ? "Missing cancel_ref for venue order (backend must include cancel_ref)"
                  : "Order is not cancelable";

      return (
        <td style={td}>
          <button
            data-no-drag="1"
            style={btn?.(disabled) ?? smallBtn(disabled)}
            disabled={disabled}
            onClick={() => openCancelModalUnified(o)}
            title={title}
          >
            {isCanceling ? "Canceling…" : "Cancel"}
          </button>
        </td>
      );
    }

    if (col === COLS.viewed) {
      return (
        <td style={td}>
          {o.view_key ? (
            <input
              data-no-drag="1"
              type="checkbox"
              checked={!!o.viewed_confirmed}
              onChange={(e) => setViewedConfirmed?.(o.view_key, e.target.checked)}
              title="Mark as viewed/confirmed"
            />
          ) : (
            <span style={sx.muted}>—</span>
          )}
        </td>
      );
    }

    
      if (col === COLS.symbol) {
        const isSolanaRow =
          typeof o?.venue === "string" &&
          (o.venue === "solana_jupiter" || o.venue === "solana" || o.venue.startsWith("solana"));

        if (!isSolanaRow) {
          // For CEX / non-Solana rows, update App immediately *and* re-assert the All Orders
          // symbol filter locally after the venue-change cycle settles. This keeps Main UI
          // Market/Venue in sync while preventing the table from snapping back to the full set.
          return renderClickableSymbolCell({
            symbolCanon: o.symbolCanon || o.symbol || o.raw_symbol || "—",
            venueMaybe: o.venue,
            subLabel: o.subLabel || null,
            applyToTabOnPick: true,
            pickOpts: {
              suppressAllOrdersReloadOnce: true,
              forceAllOrdersSymbol: true,
            },
            onPicked: (sym, ven) => applyAoSymbolImmediate(sym, ven),
            reapplyPickedDelayMs: 260,
          });
        }

        // Local helpers (do not depend on other table helpers)
        const _shortMint = (s) => {
          const x = (s ?? "").toString().trim();
          if (!x) return "";
          if (x.length <= 10) return x;
          return `${x.slice(0, 4)}…${x.slice(-4)}`;
        };

        const _regGet = (addr) => {
          const key = (addr ?? "").toString().trim();
          if (!key) return null;
          const lower = key.toLowerCase();
          const upper = key.toUpperCase();

          // solanaTokenRegistryMap might be a Map or a plain object
          try {
            if (solanaTokenRegistryMap && typeof solanaTokenRegistryMap.get === "function") {
              return (
                solanaTokenRegistryMap.get(key) ??
                solanaTokenRegistryMap.get(lower) ??
                solanaTokenRegistryMap.get(upper) ??
                null
              );
            }
          } catch (e) {
            // fall through
          }
          if (solanaTokenRegistryMap && typeof solanaTokenRegistryMap === "object") {
            return solanaTokenRegistryMap[key] ?? solanaTokenRegistryMap[lower] ?? solanaTokenRegistryMap[upper] ?? null;
          }
          return null;
        };

        const _regSymbol = (addr) => {
          const e = _regGet(addr);
          if (!e) return null;
          if (typeof e === "string") return e;
          if (typeof e?.symbol === "string" && e.symbol.trim()) return e.symbol.trim();
          return null;
        };

        // Prefer explicit mint fields, then parsed raw payload, then exact raw_symbol parts, then broad extraction.
        let rawObj = null;
        try {
          rawObj = typeof o?.raw === "string" ? JSON.parse(o.raw) : (o?.raw && typeof o.raw === "object" ? o.raw : null);
        } catch {
          rawObj = null;
        }

        const _rawTx = rawObj?.tx || rawObj || null;
        const _rawMeta = _rawTx?.meta || rawObj?.meta || null;
        const _rawDeltas = rawObj?.deltas && typeof rawObj.deltas === "object" ? rawObj.deltas : null;
        const _isMintish = (x) => /^[1-9A-HJ-NP-Za-km-z]{32,120}$/.test(String(x || "").trim());
        const _isKnownMint = (addr) => {
          const s = String(addr || "").trim();
          if (!s) return false;
          if (s === SOL_MINT) return true;
          return !!_regSymbol(s);
        };
        const _mintLabel = (addr) => {
          const s = String(addr || "").trim();
          if (!s) return "";
          if (s === SOL_MINT) return "SOL";
          return _regSymbol(s) || _shortMint(s);
        };
        const _resolveFlatSymbolPart = (part) => {
          const s = String(part || "").trim();
          if (!s) return "";
          if (s === SOL_MINT || s.toUpperCase() === "SOL") return "SOL";
          const reg = _regSymbol(s);
          if (reg) return reg;
          return s;
        };

        let baseMint =
          o?.swap_base_mint ||
          o?.base_mint ||
          o?.input_mint ||
          o?.raw?.inputMint ||
          o?.raw?.input_mint ||
          rawObj?.base_mint ||
          rawObj?.input_mint ||
          _rawTx?.base_mint ||
          _rawTx?.inputMint ||
          null;

        let quoteMint =
          o?.swap_quote_mint ||
          o?.quote_mint ||
          o?.output_mint ||
          o?.raw?.outputMint ||
          o?.raw?.output_mint ||
          rawObj?.quote_mint ||
          rawObj?.output_mint ||
          _rawTx?.quote_mint ||
          _rawTx?.outputMint ||
          null;

        const candidates = [];
        const _push = (x) => {
          const s = (x ?? "").toString().trim();
          if (s && !candidates.includes(s)) candidates.push(s);
        };

        _push(baseMint);
        _push(quoteMint);

        // Deltas written by backend are the most reliable source of actual traded mints.
        if (_rawDeltas) {
          for (const k of Object.keys(_rawDeltas)) {
            if (_isMintish(k)) _push(k);
          }
        }

        // Token balance arrays often contain mint addresses even when top-level fields are missing.
        for (const tb of [
          ...(_rawMeta?.preTokenBalances || []),
          ...(_rawMeta?.postTokenBalances || []),
        ]) {
          const m = tb?.mint;
          if (_isMintish(m)) _push(m);
        }

        const symStr = (o?.raw_symbol || o?.symbol || o?.symbol_venue || "").toString().trim();
        const flatSymbolParts = symStr ? symStr.split("-").map((s) => s.trim()).filter(Boolean) : [];
        const rawMintParts = [];
        if (symStr) {
          // First, prefer explicit dash-separated parts (raw_symbol is often base_mint-quote_mint).
          const parts = symStr.split("-").map((s) => s.trim()).filter(Boolean);
          for (const p of parts) {
            if (_isMintish(p)) {
              rawMintParts.push(p);
              _push(p);
            }
          }
          // Then fall back to broad extraction from long base58-ish substrings.
          const matches = symStr.match(/[1-9A-HJ-NP-Za-km-z]{32,120}/g) || [];
          for (const m of matches) _push(m);
        }

        // Highest-priority fix for flat unresolved symbol strings already stored on the row,
        // e.g. EPjFW...-SOL. Resolve each dash-separated side directly through the registry
        // before falling back to deeper mint extraction.
        if (flatSymbolParts.length === 2) {
          const [leftRaw, rightRaw] = flatSymbolParts;
          const leftResolved = _resolveFlatSymbolPart(leftRaw);
          const rightResolved = _resolveFlatSymbolPart(rightRaw);
          const leftChanged = leftResolved && leftResolved !== leftRaw;
          const rightChanged = rightResolved && rightResolved !== rightRaw;
          const leftIsSol = String(leftResolved || "").toUpperCase() === "SOL";
          const rightIsSol = String(rightResolved || "").toUpperCase() === "SOL";
          if (leftChanged || rightChanged || leftIsSol || rightIsSol) {
            const mainDirect = `${leftResolved || leftRaw}-${rightResolved || rightRaw}`;
            const subDirect = `${_shortMint(leftRaw)} / ${_shortMint(rightRaw)}`;
            return renderClickableSymbolCell({
              symbolCanon: mainDirect,
              venueMaybe: o.venue,
              subLabel: subDirect,
              subLabelTitle: `${leftRaw}${leftRaw && rightRaw ? " / " : ""}${rightRaw}` ,
              subCopyText: `${leftRaw}${leftRaw && rightRaw ? " / " : ""}${rightRaw}` ,
              applyToTabOnPick: false,
              pickOpts: { suppressAllOrdersReloadOnce: true },
              onPicked: (sym, ven) => applyAoSymbolImmediate(sym, ven),
            });
          }
        }

        // Prefer registry-known mints first, but never let an arbitrary unknown outrank a parsed known raw_symbol part.
        const known = [];
        const unknown = [];
        for (const c of candidates) {
          if (_isKnownMint(c) && _mintLabel(c) && _mintLabel(c) !== _shortMint(c)) known.push(c);
          else unknown.push(c);
        }

        // Strong preference: if raw_symbol already encodes a plausible mint pair and at least one side is registry-known,
        // trust that order before looser fallback candidates. This fixes cases like USDC-SOL where the row also carries
        // unrelated top-level mint fields that would otherwise outrank the raw_symbol pair.
        if (rawMintParts.length >= 2) {
          const a = rawMintParts[0];
          const b = rawMintParts[1];
          const aKnown = _isKnownMint(a);
          const bKnown = _isKnownMint(b);
          if ((aKnown && bKnown) || ((a === SOL_MINT || b === SOL_MINT) && (aKnown || bKnown))) {
            baseMint = a;
            quoteMint = b;
          }
        }

        // Secondary preference: when one side is SOL and the other is any registry-known mint, prefer the known-mint/SOL pair.
        if ((!_isKnownMint(baseMint) || !_isKnownMint(quoteMint)) && candidates.includes(SOL_MINT)) {
          const knownNonSol = candidates.filter((c) => c !== SOL_MINT && !!_regSymbol(c));
          if (knownNonSol.length) {
            const rawKnownNonSol = rawMintParts.find((c) => c !== SOL_MINT && !!_regSymbol(c));
            const preferredNonSol = rawKnownNonSol || knownNonSol[0];
            if (rawMintParts.length >= 2 && rawMintParts.includes(SOL_MINT) && rawMintParts.includes(preferredNonSol)) {
              baseMint = rawMintParts[0];
              quoteMint = rawMintParts[1];
            } else {
              baseMint = preferredNonSol;
              quoteMint = SOL_MINT;
            }
          }
        }

        if (!baseMint || !_isMintish(baseMint) || !_mintLabel(baseMint) || baseMint === quoteMint) {
          baseMint = known[0] || candidates[0] || baseMint || null;
        }
        if (!quoteMint || !_isMintish(quoteMint) || !_mintLabel(quoteMint) || quoteMint === baseMint) {
          quoteMint = known.find((x) => x !== baseMint) || candidates.find((x) => x !== baseMint) || quoteMint || null;
        }

        const baseSym = _mintLabel(baseMint) || _shortMint(baseMint);
        const quoteSym = _mintLabel(quoteMint) || _shortMint(quoteMint);
        const main = baseSym && quoteSym ? `${baseSym}-${quoteSym}` : (o?.symbol || o?.raw_symbol || "—");
        const sub =
          baseMint || quoteMint
            ? `${_shortMint(baseMint)}${baseMint && quoteMint ? " / " : ""}${_shortMint(quoteMint)}`
            : null;

        return renderClickableSymbolCell({
          symbolCanon: main,
          venueMaybe: o.venue,
          subLabel: sub,
          subLabelTitle: `${String(baseMint || "").trim()}${baseMint && quoteMint ? " / " : ""}${String(quoteMint || "").trim()}` ,
          subCopyText: `${String(baseMint || "").trim()}${baseMint && quoteMint ? " / " : ""}${String(quoteMint || "").trim()}` ,
          applyToTabOnPick: false,
          pickOpts: { suppressAllOrdersReloadOnce: true },
          onPicked: (sym, ven) => applyAoSymbolImmediate(sym, ven),
        });
      }
if (col === COLS.side) return <td style={td}>{hideTableDataGlobal ? "••••" : o.side || "—"}</td>;

    // UPDATED: use high-precision formatter
    if (col === COLS.qty) return <td style={td}>{mask?.(fmtQty?.(o.qty ?? o.filled_qty))}</td>;
    if (col === COLS.gross) return <td style={td}>{maskMaybe?.(gross === null ? "—" : fmtMoney?.(gross))}</td>;
    if (col === COLS.net)   return <td style={td}>{maskMaybe?.(net === null ? "—" : fmtMoney?.(net))}</td>;
    // Tax (backend if present; else Mode A fallback on USD FILLED SELL when eligible)
    if (col === COLS.tax) {
      const isInventoryError =
        String(o?.realized_status || "").trim().toLowerCase() === "unapplied" &&
        String(o?.realized_error || "").trim().toLowerCase() === "insufficient_inventory";
      const displayTax = backendTax !== null ? backendTax : fallbackTax;
      const displayText =
        displayTax === null
          ? (isInventoryError ? "I/E" : "—")
          : fmtMoney?.(displayTax);
      return (
        <td
          style={td}
          title={isInventoryError && displayTax === null ? "Insufficient inventory" : undefined}
        >
          {maskMaybe?.(displayText)}
        </td>
      );
    }
    if (col === COLS.netAfterTax) return <td style={td}>{maskMaybe?.(netAfterTax === null ? "—" : fmtMoney?.(netAfterTax))}</td>;
    if (col === COLS.fee)   return <td style={td}>{maskMaybe?.(fee === null ? "—" : fmtFee?.(fee))}</td>;

    if (col === COLS.limit) {
      return <td style={td}>{hideTableDataGlobal ? "••••" : o.limit_price != null ? fmtPrice?.(o.limit_price) : "—"}</td>;
    }

    if (col === COLS.status) return <td style={td}>{hideTableDataGlobal ? "••••" : st || "—"}</td>;
    if (col === COLS.type) return <td style={td}>{hideTableDataGlobal ? "••••" : o.type || "—"}</td>;
    if (col === COLS.source) return <td style={td}>{hideTableDataGlobal ? "••••" : o.source || "—"}</td>;
    if (col === COLS.venue) return <td style={td}>{hideTableDataGlobal ? "••••" : o.venue || "—"}</td>;
    if (col === COLS.bucket) return <td style={td}>{hideTableDataGlobal ? "••••" : bucket}</td>;

    return <td style={td}>{hideTableDataGlobal ? "••••" : "—"}</td>;
  }

  // ─────────────────────────────────────────────────────────────
  // All Orders load behavior (deterministic; avoids stale-state flip/flop)
  // ─────────────────────────────────────────────────────────────
  const aoStatusBucketEff = normalizeStatusBucket(aoStatusBucket);

  // Default sort behavior by Bucket (requested):
  // - Bucket=open    => sort by created_at desc
  // - Bucket=ALL/terminal => sort by closed_at desc
  const prevAoBucketRef = useRef(aoStatusBucketEff);
  const pendingAoSortRef = useRef(null);

  function desiredSortForBucket(bucketEff) {
    return bucketEff === "open" ? { field: "created_at", dir: "desc" } : { field: "closed_at", dir: "desc" };
  }

  function requestAoSort(field, dir) {
    if (typeof toggleAllSort !== "function") return;

    const curField = String(aoSortField || "");
    const curDir = String(aoSortDir || "").toLowerCase();

    if (curField === field && curDir === dir) {
      pendingAoSortRef.current = null;
      return;
    }

    pendingAoSortRef.current = { field, dir, tries: 0 };
    toggleAllSort(field);
  }

  function applyDefaultSortForBucket(bucketEff) {
    const { field, dir } = desiredSortForBucket(bucketEff);
    requestAoSort(field, dir);
  }

  // If we asked for a sort, ensure direction matches (at most 2 flips).
  useEffect(() => {
    const p = pendingAoSortRef.current;
    if (!p) return;

    const curField = String(aoSortField || "");
    const curDir = String(aoSortDir || "").toLowerCase();

    if (curField !== p.field) return;

    if (curDir === p.dir) {
      pendingAoSortRef.current = null;
      return;
    }

    if (p.tries >= 2) {
      pendingAoSortRef.current = null;
      return;
    }

    pendingAoSortRef.current = { ...p, tries: p.tries + 1 };
    toggleAllSort?.(p.field);
  }, [aoSortField, aoSortDir, toggleAllSort]);

  // Apply default sort ONLY when Bucket changes while on the All Orders tab.
  useEffect(() => {
    if (tab !== "allOrders") {
      prevAoBucketRef.current = aoStatusBucketEff;
      return;
    }

    const prev = prevAoBucketRef.current;
    if (prev === aoStatusBucketEff) return;

    prevAoBucketRef.current = aoStatusBucketEff;
    applyDefaultSortForBucket(aoStatusBucketEff);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, aoStatusBucketEff]);

  useEffect(() => {
    if (typeof setAoStatusBucket !== "function") return;

    const raw = String(aoStatusBucket ?? "").trim();
    const norm = normalizeStatusBucket(raw);

    if (norm !== raw && norm === "") {
      setAoStatusBucket("");
    }
    if (raw && norm === "" && raw.toLowerCase() !== "all" && raw.toLowerCase() !== "open" && raw.toLowerCase() !== "terminal") {
      setAoStatusBucket("");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [aoStatusBucket]);

  const [aoSymbolDraft, setAoSymbolDraft] = useState(() => aoSymbol || "");
  const [aoLocalSymbolFilter, setAoLocalSymbolFilter] = useState("");
  useEffect(() => {
    if (aoLocalSymbolFilter) return;
    setAoSymbolDraft(aoSymbol || "");
  }, [aoSymbol, aoLocalSymbolFilter]);

  const allOrdersLoadKey = useMemo(() => {
    return JSON.stringify({
      tab,
      aoSource: aoSource || "",
      aoVenue: aoVenue || "",
      aoStatusBucket: aoStatusBucketEff || "", // ALL => ""
      // Critical: when a friendly Solana local symbol filter is active, do NOT let the
      // backend load path keep sending aoSymbol=UTTT-USDC (or similar), because the server
      // stores many Solana rows under raw/mint-style symbols. In that mode, filtering is
      // intentionally client-side over the already loaded dataset.
      aoSymbol: aoLocalSymbolFilter ? "" : (aoSymbol || ""),
      aoSort: aoSort || "",
      aoPage: Number(aoPage) || 1,
      aoPageSize: Number(aoPageSize) || 25,
      hideCancelledUnified: !!hideCancelledUnified,
    });
  }, [tab, aoSource, aoVenue, aoStatusBucketEff, aoSymbol, aoLocalSymbolFilter, aoSort, aoPage, aoPageSize, hideCancelledUnified]);

  const lastAllOrdersLoadKeyRef = useRef("");

  useEffect(() => {
    if (tab !== "allOrders") return;
    if (lastAllOrdersLoadKeyRef.current === allOrdersLoadKey) return;
    lastAllOrdersLoadKeyRef.current = allOrdersLoadKey;

    doLoadAllOrders?.();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allOrdersLoadKey]);

  function shouldUseLocalSolanaSymbolFilter(nextValue, venueMaybe = "") {
    const v = String(nextValue || "").trim();
    if (!v) return false;

    const looksPair = /^[A-Za-z0-9._-]+-[A-Za-z0-9._-]+$/.test(v) && !/[\/] /.test(v);
    if (!looksPair) return false;

    const ven = String(venueMaybe || "").trim().toLowerCase();
    if (ven.startsWith("solana")) return true;

    const aoVen = String(aoVenue || "").trim().toLowerCase();
    if (aoVen.startsWith("solana")) return true;

    // Also honor the MAIN app venue selector. This is the missing case for
    // manual entry: if the user is already in Solana/Jupiter context but types
    // a friendly pair manually, we still need to use the local Solana alias path.
    const mainVen = String(venue || "").trim().toLowerCase();
    if (mainVen.startsWith("solana")) return true;

    const qU = v.toUpperCase();
    const rows = Array.isArray(aoRaw) ? aoRaw : [];
    for (const o of rows) {
      const rowVen = String(o?.venue || "").trim().toLowerCase();
      if (!rowVen.startsWith("solana")) continue;
      const cands = [o?.symbol, o?.raw_symbol, o?.symbol_venue, o?.symbol_canon];
      for (const c of cands) {
        const s = String(c || "").trim().toUpperCase();
        if (s === qU) return true;
      }
    }

    // Heuristic fallback for manual typing in mixed/all-venue views:
    // if the pair looks like a Solana-friendly symbol and one side is a known
    // Solana token-registry symbol (or a canonical SOL/USDC/USDT alias),
    // prefer the local Solana filter path instead of backend CEX-only symbol search.
    const [lhsRaw, rhsRaw] = v.split("-", 2);
    const lhs = String(lhsRaw || "").trim().toUpperCase();
    const rhs = String(rhsRaw || "").trim().toUpperCase();
    const SOL_ALIASES = new Set(["SOL", "WSOL"]);
    const USDQ_ALIASES = new Set(["USDC", "USDT"]);

    const regMap = solanaTokenRegistryMap && typeof solanaTokenRegistryMap === "object" ? solanaTokenRegistryMap : {};
    const regValueSet = new Set();
    try {
      for (const val of Object.values(regMap)) {
        const s = String(val || "").trim().toUpperCase();
        if (s) regValueSet.add(s);
      }
    } catch {
      // ignore
    }

    const leftKnown = regValueSet.has(lhs) || SOL_ALIASES.has(lhs) || USDQ_ALIASES.has(lhs);
    const rightKnown = regValueSet.has(rhs) || SOL_ALIASES.has(rhs) || USDQ_ALIASES.has(rhs);

    if ((SOL_ALIASES.has(rhs) || USDQ_ALIASES.has(rhs)) && leftKnown) return true;
    if ((SOL_ALIASES.has(lhs) || USDQ_ALIASES.has(lhs)) && rightKnown) return true;

    return false;
  }

  function applyAoSymbolImmediate(nextValue, venueMaybe = "") {
    const v = String(nextValue || "").trim();
    setAoSymbolDraft(v);

    const looksFriendlySolanaPair = shouldUseLocalSolanaSymbolFilter(v, venueMaybe);

    const kickReload = () => {
      try {
        if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
          window.requestAnimationFrame(() => {
            window.requestAnimationFrame(() => {
              try { doLoadAllOrders?.(); } catch {}
            });
          });
        } else {
          window.setTimeout(() => {
            try { doLoadAllOrders?.(); } catch {}
          }, 0);
        }
      } catch {
        // ignore
      }
    };

    if (!v) {
      setAoLocalSymbolFilter("");
      setAoSymbol?.("");
      setAoPage?.(1);
      kickReload();
      return;
    }

    if (looksFriendlySolanaPair) {
      setAoLocalSymbolFilter(v);
      // Always clear backend symbol for friendly Solana local-filter mode.
      setAoSymbol?.("");
      setAoPage?.(1);
      kickReload();
      return;
    }

    setAoLocalSymbolFilter(v);
    setAoSymbol?.(v);
    setAoPage?.(1);
    kickReload();
  }

  const _detectSolanaTokenSuggestionsFromRows = useCallback((rows) => {
    const existingMap = solanaTokenRegistryMap && typeof solanaTokenRegistryMap === "object" ? solanaTokenRegistryMap : {};
    const existingRows = readJsonLS(LS_SOLANA_DETECTED_TOKENS_KEY, []);
    const byAddress = new Map();
    for (const it of Array.isArray(existingRows) ? existingRows : []) { const a = String(it?.address || "").trim(); if (a) byAddress.set(a, it); }
    const isMintish = (x) => /^[1-9A-HJ-NP-Za-km-z]{32,120}$/.test(String(x || "").trim());
    const regSym = (addr) => { const s = String(addr || "").trim(); return s ? String(existingMap[s] || existingMap[s.toLowerCase()] || "").trim() : ""; };
    const maybeTokenSymbol = (part) => { const s = String(part || "").trim(); if (!s) return ""; if (s.toUpperCase() === "SOL") return "SOL"; if (isMintish(s)) return regSym(s); if (/^[A-Z0-9._-]{2,20}$/i.test(s)) return s.toUpperCase(); return ""; };
    const parseRaw = (raw) => { if (!raw) return null; if (typeof raw === "string") { try { return JSON.parse(raw); } catch { return null; } } return raw && typeof raw === "object" ? raw : null; };
    const pushSuggestion = (addr, symbol, decimals, row) => {
      const a = String(addr || "").trim();
      if (!isMintish(a) || a === SOL_MINT || regSym(a)) return;
      const d = Number(decimals);
      const prev = byAddress.get(a) || {};
      byAddress.set(a, {
        chain: "solana",
        address: a,
        symbol: String(symbol || prev.symbol || "").trim().toUpperCase(),
        decimals: Number.isFinite(d) ? d : (Number.isFinite(Number(prev.decimals)) ? Number(prev.decimals) : null),
        venue: String(row?.venue || prev.venue || "").trim() || "solana_jupiter",
        sourceSymbol: String(row?.symbol || row?.raw_symbol || row?.symbol_venue || prev.sourceSymbol || "").trim(),
        firstSeenAt: prev.firstSeenAt || new Date().toISOString(),
        lastSeenAt: new Date().toISOString(),
      });
    };
    for (const row of Array.isArray(rows) ? rows : []) {
      if (!String(row?.venue || "").toLowerCase().startsWith("solana")) continue;
      const rawObj = parseRaw(row?.raw);
      const tx = rawObj?.tx || rawObj || {};
      const meta = tx?.result?.meta || tx?.meta || rawObj?.meta || {};
      const pair = String(row?.symbol || row?.raw_symbol || row?.symbol_venue || "").trim();
      const parts = pair ? pair.split("-").map((s) => s.trim()).filter(Boolean) : [];
      const symbols = parts.map(maybeTokenSymbol);
      const topPairs = [[row?.base_mint || row?.swap_base_mint || null, symbols[0] || ""],[row?.quote_mint || row?.swap_quote_mint || null, symbols[1] || ""]];
      for (const [addr,sym] of topPairs) pushSuggestion(addr,sym,null,row);
      const balances=[...(meta?.preTokenBalances||[]),...(meta?.postTokenBalances||[])];
      const mintToDecimals={};
      for (const tb of balances){ const mint=String(tb?.mint||"").trim(); const dec=Number(tb?.uiTokenAmount?.decimals); if(mint&&Number.isFinite(dec)&&!(mint in mintToDecimals)) mintToDecimals[mint]=dec; }
      const mintHints=[]; const rawSym=String(row?.raw_symbol || pair || "").trim();
      for (const p of rawSym.split("-").map((s)=>s.trim()).filter(Boolean)) if (isMintish(p)) mintHints.push(p);
      const mints=Array.from(new Set([row?.base_mint,row?.quote_mint,row?.swap_base_mint,row?.swap_quote_mint,...mintHints,...Object.keys(mintToDecimals)].filter(Boolean).map((s)=>String(s).trim())));
      for (const mint of mints){ let sym=regSym(mint); if(!sym&&parts.length===2){ if(mint===topPairs[0][0]) sym=symbols[0]||sym; if(mint===topPairs[1][0]) sym=symbols[1]||sym; } pushSuggestion(mint,sym,mintToDecimals[mint],row); }
    }
    const next=Array.from(byAddress.values()).filter((it)=>it&&it.address&&it.address!==SOL_MINT).sort((a,b)=>String(b.lastSeenAt||"").localeCompare(String(a.lastSeenAt||"")));
    writeJsonLS(LS_SOLANA_DETECTED_TOKENS_KEY,next);
  }, [solanaTokenRegistryMap]);

  const aoRaw = Array.isArray(allOrders) ? allOrders : [];

  useEffect(() => {
    if (tab !== "allOrders") return;
    if (!Array.isArray(aoRaw) || !aoRaw.length) return;
    _detectSolanaTokenSuggestionsFromRows(aoRaw);
  }, [tab, aoRaw, _detectSolanaTokenSuggestionsFromRows]);

  // Unified "Status" filter state (client-side; persists)
  const [aoStatusFilter, setAoStatusFilter] = useState(() => {
    const raw = localStorage.getItem(LS_AO_STATUS_FILTER_KEY) || "";
    const parsed = safeParseJson(raw);
    if (typeof parsed === "string") return parsed;
    if (raw && raw[0] !== "{" && raw[0] !== "[" && raw[0] !== '"' && raw[0] !== "'") return String(raw);
    return "";
  });

  useEffect(() => {
    try {
      localStorage.setItem(LS_AO_STATUS_FILTER_KEY, JSON.stringify(String(aoStatusFilter || "")));
    } catch {
      // ignore
    }
  }, [aoStatusFilter]);

  const aoFiltered = useMemo(() => {
    if (!hideCancelledUnified) return aoRaw;

    const fallbackIsCanceled = (s) => {
      const sl = normalizeStatusLower(s);
      return sl === "canceled" || sl === "cancelled" || sl.includes("canceled") || sl.includes("cancelled");
    };

    return aoRaw.filter((o) => {
      const st = normalizeStatus(pickOrderStatus(o));
      if (typeof isCanceledStatus === "function") return !isCanceledStatus(st);
      return !fallbackIsCanceled(st);
    });
  }, [aoRaw, hideCancelledUnified, isCanceledStatus]);

  const aoStatusTokens = useMemo(() => parseStatusFilterTokens(aoStatusFilter), [aoStatusFilter]);

  const aoVisible = useMemo(() => {
    if (!aoStatusTokens.length) return aoFiltered;

    return aoFiltered.filter((o) => {
      const stL = normalizeStatusLower(pickOrderStatus(o));
      if (!stL) return false;

      for (const tok of aoStatusTokens) {
        if (!tok) continue;
        if (stL.includes(tok)) return true;
      }
      return false;
    });
  }, [aoFiltered, aoStatusTokens]);



  // Bridge path for manual Market-field Apply from App.jsx:
  // App's setActiveMarket() can still push a friendly Solana pair into aoSymbol directly.
  // When that happens, migrate it into the local Solana alias filter here so All Orders
  // does not depend on backend CEX-style symbol matching for Solana rows.
  useEffect(() => {
    const raw = String(aoSymbol || "").trim();
    if (!raw) return;

    const shouldLocalize = shouldUseLocalSolanaSymbolFilter(raw, aoVenue || venue || "");
    if (!shouldLocalize) return;

    if (String(aoLocalSymbolFilter || "").trim() !== raw) {
      setAoLocalSymbolFilter(raw);
    }
    if (String(aoSymbol || "").trim()) {
      setAoSymbol?.("");
    }
    setAoPage?.(1);

    try {
      if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
        window.requestAnimationFrame(() => {
          window.requestAnimationFrame(() => {
            try { doLoadAllOrders?.(); } catch {}
          });
        });
      } else {
        window.setTimeout(() => {
          try { doLoadAllOrders?.(); } catch {}
        }, 0);
      }
    } catch {
      // ignore
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [aoSymbol, aoVenue, venue]);

  const aoVisibleClient = useMemo(() => {
    const q = String(aoLocalSymbolFilter || "").trim();
    if (!q) return aoVisible;
    const qU = q.toUpperCase();
    const regMap = solanaTokenRegistryMap && typeof solanaTokenRegistryMap === "object" ? solanaTokenRegistryMap : {};
    const regGet = (v) => {
      const s = String(v || "").trim();
      if (!s) return "";
      return String(regMap[s] || regMap[s.toLowerCase()] || regMap[s.toUpperCase()] || "").trim();
    };
    const looksMintish = (v) => /^[1-9A-HJ-NP-Za-km-z]{32,120}$/.test(String(v || "").trim());
    const normPart = (part) => {
      const s = String(part || "").trim();
      if (!s) return "";
      if (s === SOL_MINT || s.toUpperCase() === "SOL") return "SOL";
      const reg = regGet(s);
      if (reg) return String(reg).trim().toUpperCase();
      if (looksMintish(s)) return s;
      return s.toUpperCase();
    };
    const buildAliases = (o) => {
      const out = new Set();
      const add = (v) => {
        const s = String(v || "").trim();
        if (!s) return;
        out.add(s);
        out.add(s.toUpperCase());
        const parts = s.split("-").map((x) => String(x || "").trim()).filter(Boolean);
        if (parts.length === 2) out.add(`${normPart(parts[0])}-${normPart(parts[1])}`.toUpperCase());
      };
      add(o?.symbol);
      add(o?.raw_symbol);
      add(o?.symbol_venue);
      add(o?.symbol_canon);
      const baseMint = o?.base_mint || o?.swap_base_mint || o?.input_mint || "";
      const quoteMint = o?.quote_mint || o?.swap_quote_mint || o?.output_mint || "";
      if (baseMint && quoteMint) add(`${baseMint}-${quoteMint}`);
      return out;
    };
    return aoVisible.filter((o) => {
      const aliases = buildAliases(o);
      for (const a of aliases) if (String(a).toUpperCase() === qU) return true;
      return false;
    });
  }, [aoVisible, aoLocalSymbolFilter, solanaTokenRegistryMap]);

  const aoCounts = useMemo(() => {
    let openN = 0;
    let termN = 0;
    for (const o of aoVisibleClient) {
      const b = orderBucket(o);
      if (b === "terminal") termN += 1;
      else openN += 1;
    }
    return { open: openN, terminal: termN };
  }, [aoVisibleClient]);

  // ─────────────────────────────────────────────────────────────
  // Audible notification: new FILLED order detected (All Orders)
  // ─────────────────────────────────────────────────────────────
  const [aoFillSoundEnabled, setAoFillSoundEnabled] = useState(() => readBoolLS(LS_AO_FILL_SOUND_ENABLED_KEY, false));
  const [aoFillSoundType, setAoFillSoundType] = useState(() => readStrLS(LS_AO_FILL_SOUND_TYPE_KEY, "chime"));
  const [aoFillSoundVolume, setAoFillSoundVolume] = useState(() => {
    const v = readNumLS(LS_AO_FILL_SOUND_VOL_KEY, 0.7);
    return Number.isFinite(v) ? Math.min(1, Math.max(0, v)) : 0.7;
  });

// Fill toasts (UI notifications)
const [aoFillToastEnabled, setAoFillToastEnabled] = useState(() => {
  return readBoolLS(LS_AO_FILL_TOAST_ENABLED_KEY, true);
});
const [fillToasts, setFillToasts] = useState([]);
const toastTimersRef = useRef(new Map()); // id -> timeout


  useEffect(() => writeBoolLS(LS_AO_FILL_SOUND_ENABLED_KEY, !!aoFillSoundEnabled), [aoFillSoundEnabled]);
  useEffect(() => writeStrLS(LS_AO_FILL_SOUND_TYPE_KEY, aoFillSoundType), [aoFillSoundType]);
  useEffect(() => writeNumLS(LS_AO_FILL_SOUND_VOL_KEY, aoFillSoundVolume), [aoFillSoundVolume]);

useEffect(() => writeBoolLS(LS_AO_FILL_TOAST_ENABLED_KEY, !!aoFillToastEnabled), [aoFillToastEnabled]);

// Cleanup toast timers on unmount
useEffect(() => {
  return () => {
    try {
      for (const t of toastTimersRef.current.values()) clearTimeout(t);
    } catch {}
    toastTimersRef.current.clear();
  };
}, []);




// ─────────────────────────────────────────────────────────────
// All Orders: Tax withholding (Mode A: UI-only)
// ─────────────────────────────────────────────────────────────
const [aoTaxWithholdEnabled, setAoTaxWithholdEnabled] = useState(() =>
  readBoolLS(LS_AO_TAX_WITHHOLD_ENABLED_KEY, false)
);

const [aoTaxFedPct, setAoTaxFedPct] = useState(() => {
  const v = readNumLS(LS_AO_TAX_FED_PCT_KEY, 0);
  return Number.isFinite(v) ? clamp(v, 0, 100) : 0;
});

const [aoTaxStatePct, setAoTaxStatePct] = useState(() => {
  const v = readNumLS(LS_AO_TAX_STATE_PCT_KEY, 0);
  return Number.isFinite(v) ? clamp(v, 0, 100) : 0;
});


const [aoTaxAssumeNetWhenGainUnknown, setAoTaxAssumeNetWhenGainUnknown] = useState(() =>
  readBoolLS(LS_AO_TAX_ASSUME_NET_WHEN_UNKNOWN_KEY, false)
);
const [aoBackendRealizedEnabled, setAoBackendRealizedEnabled] = useState(true);
const [aoBackendRealizedBusy, setAoBackendRealizedBusy] = useState(false);
const [aoBackendRealizedLoaded, setAoBackendRealizedLoaded] = useState(false);

  // All Orders: Tax settings floating window (in-page)
  const [aoTaxWinOpen, setAoTaxWinOpen] = useState(false);
  const [aoTaxWinX, setAoTaxWinX] = useState(readNumLS(LS_AO_TAX_WIN_X_KEY, 60));
  const [aoTaxWinY, setAoTaxWinY] = useState(readNumLS(LS_AO_TAX_WIN_Y_KEY, 120));

useEffect(
  () => writeBoolLS(LS_AO_TAX_WITHHOLD_ENABLED_KEY, !!aoTaxWithholdEnabled),
  [aoTaxWithholdEnabled]
);
useEffect(() => writeNumLS(LS_AO_TAX_FED_PCT_KEY, aoTaxFedPct), [aoTaxFedPct]);
useEffect(() => writeNumLS(LS_AO_TAX_STATE_PCT_KEY, aoTaxStatePct), [aoTaxStatePct]);
useEffect(() => writeBoolLS(LS_AO_TAX_ASSUME_NET_WHEN_UNKNOWN_KEY, !!aoTaxAssumeNetWhenGainUnknown), [aoTaxAssumeNetWhenGainUnknown]);
useEffect(() => writeNumLS(LS_AO_TAX_WIN_X_KEY, aoTaxWinX), [aoTaxWinX]);
useEffect(() => writeNumLS(LS_AO_TAX_WIN_Y_KEY, aoTaxWinY), [aoTaxWinY]);

const loadAoRuntimeFlags = useCallback(async () => {
  try {
    const js = await fetchJSONMaybe("/api/trade/runtime_flags");
    if (typeof js?.realized_fields_enabled === "boolean") {
      setAoBackendRealizedEnabled(!!js.realized_fields_enabled);
    } else {
      setAoBackendRealizedEnabled(true);
    }
  } catch {
    setAoBackendRealizedEnabled(true);
  } finally {
    setAoBackendRealizedLoaded(true);
  }
}, []);

const saveAoRuntimeFlags = useCallback(async (nextEnabled) => {
  setAoBackendRealizedBusy(true);
  try {
    const tok = (typeof getAuthToken === "function") ? getAuthToken() : null;
    const headers = { "Content-Type": "application/json" };
    if (tok) headers["Authorization"] = `Bearer ${tok}`;
    const resp = await fetch("/api/trade/runtime_flags", {
      method: "POST",
      headers,
      body: JSON.stringify({ realized_fields_enabled: !!nextEnabled }),
    });
    let js = {};
    try { js = await resp.json(); } catch { js = {}; }
    if (!resp.ok) {
      const msg = (js && (js.detail || js.error)) ? (js.detail || js.error) : `Request failed (${resp.status})`;
      throw new Error(msg);
    }
    setAoBackendRealizedEnabled(typeof js?.realized_fields_enabled === "boolean" ? !!js.realized_fields_enabled : !!nextEnabled);
  } finally {
    setAoBackendRealizedBusy(false);
  }
}, []);

useEffect(() => {
  if (aoTaxWinOpen && !aoBackendRealizedLoaded) {
    loadAoRuntimeFlags();
  }
}, [aoTaxWinOpen, aoBackendRealizedLoaded, loadAoRuntimeFlags]);

  // All Orders: Ledger sync (lot-journal) settings
  function clampLedgerLimit(n, fallback = 5000) {
    const x = Number(n);
    if (!Number.isFinite(x)) return fallback;
    return Math.min(5000, Math.max(1, Math.floor(x)));
  }

  const [aoLedgerSyncOnSyncLoad, setAoLedgerSyncOnSyncLoad] = useState(() =>
    readBoolLS(LS_AO_LEDGER_SYNC_ON_SYNCLOAD_KEY, true)
  );
  useEffect(
    () => writeBoolLS(LS_AO_LEDGER_SYNC_ON_SYNCLOAD_KEY, !!aoLedgerSyncOnSyncLoad),
    [aoLedgerSyncOnSyncLoad]
  );

  const [aoLedgerSyncWalletId, setAoLedgerSyncWalletId] = useState(() =>
    readStrLS(LS_AO_LEDGER_SYNC_WALLET_ID_KEY, "default")
  );
  useEffect(
    () => writeStrLS(LS_AO_LEDGER_SYNC_WALLET_ID_KEY, String(aoLedgerSyncWalletId || "default")),
    [aoLedgerSyncWalletId]
  );

  const [aoLedgerSyncMode, setAoLedgerSyncMode] = useState(() =>
    readStrLS(LS_AO_LEDGER_SYNC_MODE_KEY, "ALL")
  );
  useEffect(
    () => writeStrLS(LS_AO_LEDGER_SYNC_MODE_KEY, String(aoLedgerSyncMode || "ALL")),
    [aoLedgerSyncMode]
  );

  const [aoLedgerSyncLimit, setAoLedgerSyncLimit] = useState(() =>
    clampLedgerLimit(readNumLS(LS_AO_LEDGER_SYNC_LIMIT_KEY, 5000))
  );
  useEffect(() => writeNumLS(LS_AO_LEDGER_SYNC_LIMIT_KEY, clampLedgerLimit(aoLedgerSyncLimit)), [aoLedgerSyncLimit]);

  const [aoLedgerSyncDryRun, setAoLedgerSyncDryRun] = useState(() =>
    readBoolLS(LS_AO_LEDGER_SYNC_DRY_RUN_KEY, false)
  );
  useEffect(() => writeBoolLS(LS_AO_LEDGER_SYNC_DRY_RUN_KEY, !!aoLedgerSyncDryRun), [aoLedgerSyncDryRun]);

  const [aoLedgerSyncBusy, setAoLedgerSyncBusy] = useState(false);
  const [aoLedgerSyncLast, setAoLedgerSyncLast] = useState(null);
  const [aoLedgerSyncErr, setAoLedgerSyncErr] = useState(null);

  const runLedgerSync = useCallback(
    async ({ alsoReloadAllOrders = false } = {}) => {
      if (aoLedgerSyncBusy) return null;

      const walletId = String(aoLedgerSyncWalletId || "default").trim() || "default";
      const mode = String(aoLedgerSyncMode || "ALL").trim() || "ALL";
      const limit = clampLedgerLimit(aoLedgerSyncLimit, 5000);
      const dryRun = !!aoLedgerSyncDryRun;

      const qs = new URLSearchParams({
        wallet_id: walletId,
        mode,
        limit: String(limit),
        dry_run: dryRun ? "true" : "false",
      }).toString();

      setAoLedgerSyncBusy(true);
      setAoLedgerSyncErr(null);

      try {
        const res = await fetch(`/api/ledger/sync?${qs}`, {
          method: "POST",
        });

        let data = null;
        const ct = res.headers?.get?.("content-type") || "";
        if (ct.includes("application/json")) {
          data = await res.json();
        } else {
          const t = await res.text();
          data = t ? { detail: t } : {};
        }

        if (!res.ok) {
          const msg =
            (data && (data.detail || data.message || data.error)) ||
            `HTTP ${res.status} ${res.statusText}`;
          throw new Error(String(msg));
        }

        setAoLedgerSyncLast({ at: Date.now(), ...data });

        if (alsoReloadAllOrders && typeof doLoadAllOrders === "function") {
          await doLoadAllOrders({ venue: "*" });
        }

        return data;
      } finally {
        setAoLedgerSyncBusy(false);
      }
    },
    [
      aoLedgerSyncBusy,
      aoLedgerSyncWalletId,
      aoLedgerSyncMode,
      aoLedgerSyncLimit,
      aoLedgerSyncDryRun,
      doLoadAllOrders,
    ]
  );

  const doAllOrdersSyncAndLoad = useCallback(async () => {
    // Sync+Load orders first, then (optionally) run ledger sync so realized/tax fields refresh automatically.
    let syncErr = null;

    try {
      await doSyncAndLoadAllOrders?.({ venue: "*" });
    } catch (e) {
      syncErr = e;
      setAoLedgerSyncErr(e?.message ? String(e.message) : String(e));
    }

    // Auto-run ledger sync when explicitly enabled OR when tax withholding is enabled (since tax fields depend on it).
    const shouldAutoLedgerSync = !!aoLedgerSyncOnSyncLoad || !!aoTaxWithholdEnabled;

    if (shouldAutoLedgerSync) {
      try {
        // After syncing orders, run ledger sync and then reload All Orders so realized fields/tax can populate.
        await runLedgerSync({ alsoReloadAllOrders: true });
      } catch (e) {
        // Do not block Sync+Load on ledger sync errors; surface and continue.
        setAoLedgerSyncErr(e?.message ? String(e.message) : String(e));
      }
    }

    // If Sync+Load failed, we already surfaced the error above; keep the function resolved so UI doesn't wedge.
    return syncErr ? null : true;
  }, [aoLedgerSyncOnSyncLoad, aoTaxWithholdEnabled, runLedgerSync, doSyncAndLoadAllOrders]);


  const doAllOrdersLedgerSyncOnly = useCallback(async () => {
    try {
      await runLedgerSync({ alsoReloadAllOrders: true });
    } catch (e) {
      setAoLedgerSyncErr(e?.message ? String(e.message) : String(e));
    }
  }, [runLedgerSync]);



const aoTaxCombinedPct = useMemo(() => {
  const a = Number(aoTaxFedPct);
  const b = Number(aoTaxStatePct);
  const aa = Number.isFinite(a) ? a : 0;
  const bb = Number.isFinite(b) ? b : 0;
  // Inputs are already clamped 0..100.
  return aa + bb;
}, [aoTaxFedPct, aoTaxStatePct]);

  // Draggable floating tax window (position fixed; persisted)
  const aoTaxDragRef = useRef(null);

  const startAoTaxDrag = useCallback(
    (e) => {
      if (!e) return;
      try {
        e.preventDefault?.();
        e.stopPropagation?.();
      } catch {
        // ignore
      }

      const startX = e.clientX;
      const startY = e.clientY;
      const baseX = Number(aoTaxWinX) || 0;
      const baseY = Number(aoTaxWinY) || 0;

      aoTaxDragRef.current = { startX, startY, baseX, baseY };

      const onMove = (ev) => {
        const d = aoTaxDragRef.current;
        if (!d) return;
        const dx = (ev?.clientX ?? d.startX) - d.startX;
        const dy = (ev?.clientY ?? d.startY) - d.startY;
        setAoTaxWinX(Math.max(0, Math.round(d.baseX + dx)));
        setAoTaxWinY(Math.max(0, Math.round(d.baseY + dy)));
      };

      const onUp = () => {
        aoTaxDragRef.current = null;
        try {
          window.removeEventListener("mousemove", onMove);
          window.removeEventListener("mouseup", onUp);
        } catch {
          // ignore
        }
      };

      try {
        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup", onUp);
      } catch {
        // ignore
      }
    },
    [aoTaxWinX, aoTaxWinY]
  );

  const renderAoTaxWindow = useCallback(() => {
    if (!aoTaxWinOpen) return null;
    if (typeof document === "undefined") return null;

    const WIN_W = 380;
    const WIN_H = 290;

    let left = Number(aoTaxWinX) || 0;
    let top = Number(aoTaxWinY) || 0;
    if (typeof window !== "undefined") {
      const maxX = Math.max(0, (window.innerWidth || 0) - WIN_W - 8);
      const maxY = Math.max(0, (window.innerHeight || 0) - WIN_H - 8);
      left = Math.max(0, Math.min(left, maxX));
      top = Math.max(0, Math.min(top, maxY));
    }

    const combined = Number.isFinite(Number(aoTaxCombinedPct)) ? Number(aoTaxCombinedPct) : 0;

    return createPortal(
      <div
        data-no-drag="1"
        style={{
          position: "fixed",
          left,
          top,
          width: WIN_W,
          height: WIN_H,
          zIndex: 9999,
          background: "var(--utt-surface-1, #14181f)",
          border: "1px solid var(--utt-border-1, rgba(255,255,255,0.08))",
          borderRadius: 12,
          boxShadow: "0 12px 32px rgba(0,0,0,0.55)",
          overflow: "hidden",
        }}
      >
        <div
          onMouseDown={startAoTaxDrag}
          style={{
            cursor: "move",
            userSelect: "none",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "10px 12px",
            borderBottom: "1px solid var(--utt-border-1, rgba(255,255,255,0.08))",
            background: "var(--utt-surface-2, #10141a)",
          }}
        >
          <div style={{ fontWeight: 700 }}>Taxes</div>
          <button
            style={btn?.(false) ?? smallBtn(false)}
            onClick={() => setAoTaxWinOpen(false)}
            title="Close"
          >
            Close
          </button>
        </div>

        <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10 }}>
          <label style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <input
              type="checkbox"
              checked={!!aoTaxWithholdEnabled}
              onChange={(e) => setAoTaxWithholdEnabled(!!e.target.checked)}
            />
            <div style={{ display: "flex", flexDirection: "column", lineHeight: 1.1 }}>
              <div style={{ fontWeight: 600 }}>Enable tax withholding</div>
              <div style={{ opacity: 0.8, fontSize: 12 }}>
                Fallback UI tax for FILLED SELL orders when backend tax is missing.
              </div>
            </div>
          </label>

          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <label style={{ display: "flex", flexDirection: "column", gap: 4, flex: 1 }}>
              <div style={{ opacity: 0.85, fontSize: 12 }}>Fed %</div>
              <input
                value={String(aoTaxFedPct)}
                onChange={(e) => setAoTaxFedPct(parsePctInput(e.target.value))}
                style={sx.pillInput}
                inputMode="decimal"
              />
            </label>
            <label style={{ display: "flex", flexDirection: "column", gap: 4, flex: 1 }}>
              <div style={{ opacity: 0.85, fontSize: 12 }}>State %</div>
              <input
                value={String(aoTaxStatePct)}
                onChange={(e) => setAoTaxStatePct(parsePctInput(e.target.value))}
                style={sx.pillInput}
                inputMode="decimal"
              />
            </label>
          </div>

          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <div style={{ opacity: 0.85 }}>Combined</div>
            <div style={{ fontWeight: 700 }}>{combined.toFixed(2)}%</div>
          </div>

          <label style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <input
              type="checkbox"
              checked={!!aoTaxAssumeNetWhenGainUnknown}
              onChange={(e) => setAoTaxAssumeNetWhenGainUnknown(!!e.target.checked)}
            />
            <div style={{ display: "flex", flexDirection: "column", lineHeight: 1.1 }}>
              <div style={{ fontWeight: 600 }}>Tax net when gain unknown</div>
              <div style={{ opacity: 0.8, fontSize: 12 }}>
                When realized gain is unknown, still withhold on net.
              </div>
            </div>
          </label>

          <label style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <input
              type="checkbox"
              checked={!!aoBackendRealizedEnabled}
              disabled={!!aoBackendRealizedBusy}
              onChange={async (e) => {
                const next = !!e.target.checked;
                setAoBackendRealizedEnabled(next);
                try {
                  await saveAoRuntimeFlags(next);
                } catch {
                  setAoBackendRealizedEnabled(!next);
                }
              }}
            />
            <div style={{ display: "flex", flexDirection: "column", lineHeight: 1.1 }}>
              <div style={{ fontWeight: 600 }}>Enable backend realized gain fields</div>
              <div style={{ opacity: 0.8, fontSize: 12 }}>
                Controls backend realized proceeds, basis, pnl, and tax support. Defaults to on.
              </div>
            </div>
          </label>
        </div>
      </div>,
      document.body
    );
  }, [
    aoTaxWinOpen,
    aoTaxWinX,
    aoTaxWinY,
    aoTaxCombinedPct,
    aoTaxWithholdEnabled,
    aoTaxFedPct,
    aoTaxStatePct,
    aoTaxAssumeNetWhenGainUnknown,
    aoBackendRealizedEnabled,
    aoBackendRealizedBusy,
    saveAoRuntimeFlags,
    btn,
    smallBtn,
    sx.pillInput,
    startAoTaxDrag,
  ]);

function parsePctInput(v) {
  const s = String(v ?? "").trim();
  if (!s) return 0;
  const n = Number(s);
  return Number.isFinite(n) ? clamp(n, 0, 100) : 0;
}

function isFilledSellOrder(o) {
  const sideRaw = o?.side ?? o?.order_side ?? o?.orderSide ?? "";
  const side = String(sideRaw || "").trim().toLowerCase();
  if (side !== "sell") return false;

  const stLower = normalizeStatusLower(pickOrderStatus(o));
  const kind = classifyStatusKind(stLower, "");
  return kind === "filled";
}

  const audioCtxRef = useRef(null);

const dismissFillToast = useCallback((id) => {
  setFillToasts((prev) => prev.filter((t) => t.id !== id));
  const tm = toastTimersRef.current.get(id);
  if (tm) clearTimeout(tm);
  toastTimersRef.current.delete(id);
}, []);

const pushFillToast = useCallback(
  (toast) => {
    if (!aoFillToastEnabled) return;

    const id = toast?.id || `${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const ttlMs = Number.isFinite(toast?.ttlMs) ? toast.ttlMs : 8000;

    setFillToasts((prev) => {
      const next = [{ ...toast, id }, ...prev];
      return next.slice(0, 8);
    });

    const tm = setTimeout(() => dismissFillToast(id), Math.max(1500, ttlMs));
    toastTimersRef.current.set(id, tm);
  },
  [aoFillToastEnabled, dismissFillToast]
);

const pushFillToastFromOrder = useCallback(
  (o) => {
    if (!o) return;
    const venue = o?.venue ?? o?.exchange ?? o?.source ?? "unknown";
    const symbol = o?.symbol ?? o?.product_id ?? o?.productId ?? o?.pair ?? "";
    const sideRaw = o?.side ?? o?.order_side ?? o?.orderSide ?? "";
    const side = String(sideRaw || "").toLowerCase();
    const qty =
      o?.filled_qty ??
      o?.filledQuantity ??
      o?.qty_filled ??
      o?.qtyFilled ??
      o?.filled ??
      o?.filled_size ??
      o?.filledSize ??
      o?.size_filled ??
      "";
    const price =
      o?.avg_price ??
      o?.avgPrice ??
      o?.average_price ??
      o?.fill_price ??
      o?.fillPrice ??
      o?.executed_price ??
      o?.executedPrice ??
      o?.price ??
      "";

    const lines = [
      `${venue} · ${symbol}${side ? " · " + side.toUpperCase() : ""}`,
      `${qty ? "Qty " + qty : ""}${qty && price ? " @ " : ""}${price ? "Price " + price : ""}`.trim(),
    ].filter(Boolean);

    pushFillToast({
      title: "Order filled",
      lines,
      kind: side === "sell" ? "sell" : side === "buy" ? "buy" : "info",
    });
  },
  [pushFillToast]
);

function renderFillToasts() {
  if (!aoFillToastEnabled || !Array.isArray(fillToasts) || fillToasts.length === 0) return null;

  const stackSx = {
    position: "fixed",
    left: 16,
    bottom: 16,
    right: "auto",
    zIndex: 99999,
    display: "flex",
    flexDirection: "column",
    gap: 10,
    pointerEvents: "none",
    maxWidth: 420,
  };

  const cardBase = {
    pointerEvents: "auto",
    borderRadius: 12,
    border: "1px solid var(--utt-border, rgba(255,255,255,0.14))",
    background: "var(--utt-surface-2, rgba(20,20,20,0.92))",
    color: "var(--utt-text, rgba(255,255,255,0.9))",
    boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
    padding: "10px 12px",
  };

  const titleSx = { fontWeight: 700, fontSize: 13, marginBottom: 4 };
  const lineSx = { fontSize: 12, opacity: 0.9, lineHeight: 1.25 };

  const dotSx = (kind) => {
    const c =
      kind === "buy"
        ? "rgba(0, 200, 120, 0.95)"
        : kind === "sell"
        ? "rgba(255, 80, 80, 0.95)"
        : "rgba(255, 190, 40, 0.95)";
    return {
      width: 10,
      height: 10,
      borderRadius: 999,
      background: c,
      marginRight: 10,
      flex: "0 0 auto",
    };
  };

  const rowTop = { display: "flex", alignItems: "flex-start" };
  const xBtn = {
    marginLeft: 12,
    background: "transparent",
    border: "1px solid rgba(255,255,255,0.18)",
    color: "inherit",
    borderRadius: 10,
    padding: "2px 8px",
    cursor: "pointer",
    fontSize: 12,
    opacity: 0.85,
  };

  if (typeof document === "undefined" || !document.body) return null;

  return createPortal(
    <div style={stackSx}>
      {fillToasts.map((t) => (
        <div key={t.id} style={cardBase}>
          <div style={rowTop}>
            <div style={dotSx(t.kind)} />
            <div style={{ flex: "1 1 auto", minWidth: 0 }}>
              <div style={titleSx}>{t.title || "Fill"}</div>
              {Array.isArray(t.lines) &&
                t.lines.map((ln, i) => (
                  <div key={i} style={lineSx}>
                    {ln}
                  </div>
                ))}
            </div>
            <button style={xBtn} onClick={() => dismissFillToast(t.id)} title="Dismiss">
              ×
            </button>
          </div>
        </div>
      ))}
    </div>,
    document.body
  );
}

  const seenFilledKeysRef = useRef(new Set());
  const filledKeyQueueRef = useRef([]); // FIFO eviction to cap memory growth
  const filledPrimedRef = useRef(false);

  function ensureAudioContext() {
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return null;
      if (!audioCtxRef.current) audioCtxRef.current = new Ctx();
      const ctx = audioCtxRef.current;
      if (ctx && ctx.state === "suspended") {
        // Some browsers require a user gesture; this will simply no-op if blocked.
        ctx.resume?.().catch?.(() => {});
      }
      return ctx;
    } catch {
      return null;
    }
  }

  function scheduleTone({ ctx, t0, freq, durMs, oscType, vol }) {
    const t = t0 ?? ctx.currentTime;
    const dur = Math.max(0.01, (durMs ?? 140) / 1000);
    const volume = Math.min(1, Math.max(0, Number(vol ?? 0.7)));

    const osc = ctx.createOscillator();
    const gain = ctx.createGain();

    osc.type = oscType || "sine";
    osc.frequency.setValueAtTime(Number(freq ?? 880), t);

    // Simple envelope to avoid clicks
    const a = 0.006;
    const r = 0.14;
    gain.gain.setValueAtTime(0.0001, t);
    gain.gain.linearRampToValueAtTime(Math.max(0.0001, 0.7 * volume), t + a);
    gain.gain.exponentialRampToValueAtTime(0.0001, t + Math.min(dur, a + r));

    osc.connect(gain);
    gain.connect(ctx.destination);

    osc.start(t);
    osc.stop(t + dur);

    // Cleanup
    osc.onended = () => {
      try {
        osc.disconnect();
        gain.disconnect();
      } catch {
        // ignore
      }
    };
  }

  function playFillSound({ test = false } = {}) {
    const ctx = ensureAudioContext();
    if (!ctx) return;

    const vol = Math.min(1, Math.max(0, Number(aoFillSoundVolume ?? 0.7)));
    const t0 = ctx.currentTime + 0.01;

    const kind = String(aoFillSoundType || "chime").toLowerCase();
    if (kind === "beep") {
      scheduleTone({ ctx, t0, freq: 880, durMs: 140, oscType: "sine", vol });
    } else if (kind === "ding") {
      scheduleTone({ ctx, t0, freq: 1175, durMs: 130, oscType: "triangle", vol });
      scheduleTone({ ctx, t0: t0 + 0.10, freq: 1568, durMs: 190, oscType: "triangle", vol: vol * 0.9 });
    } else if (kind === "pop") {
      scheduleTone({ ctx, t0, freq: 220, durMs: 90, oscType: "square", vol: vol * 0.55 });
      scheduleTone({ ctx, t0: t0 + 0.06, freq: 440, durMs: 70, oscType: "square", vol: vol * 0.35 });
    } else {
      // "chime" (default)
      scheduleTone({ ctx, t0, freq: 1046, durMs: 140, oscType: "sine", vol });
      scheduleTone({ ctx, t0: t0 + 0.12, freq: 784, durMs: 170, oscType: "sine", vol: vol * 0.9 });
    }

    // If the browser blocks audio until a gesture, this will fail silently.
    if (test) {
      // no-op; explicit param left for future UX if you want "unlock audio" hints
    }
  }

  function orderStableKey(o) {
    const venue = String(o?.venue || "").trim().toLowerCase();
    const id = o?.order_id ?? o?.id ?? o?.client_order_id ?? o?.client_oid ?? o?.clientOid;
    if (id !== null && id !== undefined && String(id).trim()) return `${venue}:${String(id).trim()}`;
    const sym = String(o?.symbol_canon ?? o?.symbol ?? "").trim().toUpperCase();
    const side = String(o?.side ?? "").trim().toLowerCase();
    const price = String(o?.price ?? o?.limit_price ?? "").trim();
    const qty = String(o?.size ?? o?.qty ?? o?.amount ?? "").trim();
    const ts = String(o?.created_at ?? o?.createdAt ?? o?.ts ?? o?.time ?? "").trim();
    return `${venue}:${sym}:${side}:${price}:${qty}:${ts}`;
  }

  function isFilledOrder(o) {
    const stLower = normalizeStatusLower(o?.status ?? o?.order_status ?? o?.state ?? "");
    const kind = classifyStatusKind(stLower, "");
    return kind === "filled";
  }

  useEffect(() => {
    // Detect new filled orders from the raw (unfiltered) dataset to avoid false positives
    // when the user changes table filters/sorts.
    const rows = Array.isArray(aoRaw) ? aoRaw : [];
    if (!rows.length) return;

    const seen = seenFilledKeysRef.current;
    const queue = filledKeyQueueRef.current;

    const newlySeen = [];
    const newlyFilledOrders = [];
    for (const o of rows) {
      if (!isFilledOrder(o)) continue;
      const k = orderStableKey(o);
      if (!k) continue;
      if (!seen.has(k)) {
        newlySeen.push(k);
        newlyFilledOrders.push(o);
      }
    }

    // Prime on first non-empty snapshot: do NOT alert.
    if (!filledPrimedRef.current) {
      for (const k of newlySeen) {
        seen.add(k);
        queue.push(k);
      }
      filledPrimedRef.current = true;
      return;
    }

    if (newlySeen.length) {
      // Record first to prevent duplicate sounds if play triggers rerenders/errors.
      for (const k of newlySeen) {
        seen.add(k);
        queue.push(k);
      }

      // Cap memory
      const MAX_KEYS = 5000;
      while (queue.length > MAX_KEYS) {
        const old = queue.shift();
        if (old) seen.delete(old);
      }

      if (aoFillSoundEnabled) playFillSound();

      if (aoFillToastEnabled) {
        // Only toast the single most-recent fill detected in this refresh cycle.
        // (Prevents a burst of toasts when the table loads/sorts.)
        const toMs = (v) => {
          if (v === null || v === undefined) return 0;
          if (typeof v === "number") {
            // Heuristic: seconds vs ms
            if (v > 1e12) return v;
            if (v > 1e9) return v * 1000;
            return v;
          }
          const s = String(v).trim();
          if (!s) return 0;
          const n = Number(s);
          if (Number.isFinite(n)) {
            if (n > 1e12) return n;
            if (n > 1e9) return n * 1000;
            return n;
          }
          const d = Date.parse(s);
          return Number.isFinite(d) ? d : 0;
        };

        const orderEventMs = (o) => {
          // Prefer "closed/filled" timestamps, then updated/created.
          return Math.max(
            toMs(o?.closed_at ?? o?.closedAt),
            toMs(o?.filled_at ?? o?.filledAt),
            toMs(o?.done_at ?? o?.doneAt),
            toMs(o?.updated_at ?? o?.updatedAt),
            toMs(o?.created_at ?? o?.createdAt),
            toMs(o?.time ?? o?.ts)
          );
        };

        let latest = null;
        let latestMs = -1;
        for (const o of newlyFilledOrders) {
          const ms = orderEventMs(o);
          if (ms >= latestMs) {
            latest = o;
            latestMs = ms;
          }
        }
        if (!latest && newlyFilledOrders.length) latest = newlyFilledOrders[newlyFilledOrders.length - 1];
        if (latest) pushFillToastFromOrder(latest);
      }
    }
  }, [aoRaw, aoFillSoundEnabled, aoFillSoundType, aoFillSoundVolume, aoFillToastEnabled, pushFillToastFromOrder]);


  // ─────────────────────────────────────────────────────────────
  // Renders
  // ─────────────────────────────────────────────────────────────

  function renderBalances() {
    const row = { display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" };

    return (
      <>
        <div style={row}>
          <div style={{ fontSize: 16, fontWeight: 800 }}>Balances</div>

                    <button
            data-no-drag="1"
            style={
              btn?.(loadingBalances || balancesRefreshingLocal) ??
              smallBtn(loadingBalances || balancesRefreshingLocal)
            }
            disabled={!!loadingBalances || !!balancesRefreshingLocal}
            onClick={() => safeRefreshBalancesFromUI()}
            title="Refresh balances and USD valuations"
          >
            {(loadingBalances || balancesRefreshingLocal) ? "Refreshing…" : "Refresh"}
          </button>

          <label data-no-drag="1" style={sx.pill} title="Hide balances table view">
            <input data-no-drag="1" type="checkbox" checked={!!hideBalancesView} onChange={(e) => setHideBalancesView?.(e.target.checked)} />
            <span>Hide balances</span>
          </label>

          <label data-no-drag="1" style={sx.pill} title="Hide zero-total balances">
            <input data-no-drag="1" type="checkbox" checked={!!hideZeroBalances} onChange={(e) => setHideZeroBalances?.(e.target.checked)} />
            <span>Hide zeros</span>
          </label>

          <div data-no-drag style={sx.pill} title="Filter balances by asset symbol (e.g., BTC, ETH, DOGE)">
            <span>Search</span>
            <input
              data-no-drag="1"
              value={balancesSymbolQuery}
              onChange={(e) => setBalancesSymbolQuery(e.target.value)}
              placeholder="e.g. BTC"
              style={{ ...sx.input, width: 140 }}
            />
            {!!String(balancesSymbolQuery || "").trim() && (
              <button
                data-no-drag="1"
                type="button"
                style={smallBtn(false)}
                onClick={() => setBalancesSymbolQuery("")}
                title="Clear"
              >
                Clear
              </button>
            )}
          </div>

          <div style={{ marginLeft: "auto", ...sx.muted, fontSize: 13 }}>
            Portfolio Total:{" "}
            <b style={{ color: pal.text }}>
              ${
                hideTableDataGlobal
                  ? "••••"
                  : isSolanaVenue
                      ? solanaPortfolioTotalUsd === null
                        ? "—"
                        : fmtUsd?.(solanaPortfolioTotalUsd)
                      : portfolioTotalUsd === null
                        ? "—"
                        : fmtUsd?.(portfolioTotalUsd)
              }
            </b>
          </div>
        </div>
        {balancesBanner.open && (
          <div
            data-no-drag="1"
            style={{
              marginTop: 10,
              border: `1px solid ${pal.border}`,
              background: pal.panelBg,
              borderRadius: 12,
              padding: "10px 12px",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 10,
              boxShadow: `0 10px 24px ${pal.shadow}`,
            }}
          >
            <div style={{ fontSize: 12, color: pal.text }}>
              <b style={{ color: balancesBanner.kind === "error" ? pal.danger : pal.warn }}>
                {balancesBanner.kind === "error" ? "Balances refresh failed" : "Balances notice"}
              </b>
              <span style={{ marginLeft: 8, color: pal.muted }}>
                {hideTableDataGlobal ? "••••" : balancesBanner.msg}
              </span>
            </div>

            <button
              data-no-drag="1"
              style={btn?.(false) ?? smallBtn(false)}
              onClick={() => setBalancesBanner((p) => ({ ...p, open: false }))}
              title="Dismiss"
            >
              Dismiss
            </button>
          </div>
        )}


        {!hideBalancesView && (
          <table style={sx.table}>
            <thead>
              <tr>
                {venue === ALL_VENUES_VALUE && <th style={sx.th}>Venue</th>}
                <th style={{ ...sx.th, ...sx.linkyHeader }} onClick={() => toggleBalanceSort?.("asset")}>
                  Asset {balSortKey === "asset" ? (balSortDir === "asc" ? "▲" : "▼") : ""}
                </th>
                <th style={{ ...sx.th, ...sx.linkyHeader }} onClick={() => toggleBalanceSort?.("total")}>
                  Total {balSortKey === "total" ? (balSortDir === "asc" ? "▲" : "▼") : ""}
                </th>
                <th style={{ ...sx.th, ...sx.linkyHeader }} onClick={() => toggleBalanceSort?.("available")}>
                  Available {balSortKey === "available" ? (balSortDir === "asc" ? "▲" : "▼") : ""}
                </th>
                <th style={{ ...sx.th, ...sx.linkyHeader }} onClick={() => toggleBalanceSort?.("hold")}>
                  Hold {balSortKey === "hold" ? (balSortDir === "asc" ? "▲" : "▼") : ""}
                </th>
                <th style={sx.th}>Px USD</th>
                <th style={{ ...sx.th, ...sx.linkyHeader }} onClick={() => toggleBalanceSort?.("total_usd")}>
                  Total USD {balSortKey === "total_usd" ? (balSortDir === "asc" ? "▲" : "▼") : ""}
                </th>
                <th style={sx.th}>USD Src</th>
              </tr>
            </thead>
            <tbody>
              {(balancesFiltered || []).map((b, i) => (
                <tr key={`${b.venue || ""}:${b.asset || ""}:${i}`}>
                  {venue === ALL_VENUES_VALUE && <td style={sx.td}>{hideVenueNames ? "••••" : b.venue || "—"}</td>}

                  {/* UPDATED: Asset cell is clickable + hover overlib */}
                  {renderBalanceAssetCell(b)}

                  {/* UPDATED: high-precision numeric formatting */}
                  <td style={sx.td}>{mask?.(fmtBal(b.total))}</td>
                  <td style={sx.td}>{mask?.(fmtBal(b.available))}</td>
                  <td style={sx.td}>{mask?.(fmtBal(b.hold))}</td>


                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : b.px_usd === null ? "—" : fmtPxUsd?.(b.px_usd)}</td>
                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : b.total_usd === null ? "—" : fmtUsd?.(b.total_usd)}</td>
                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : b.usd_source_symbol || "—"}</td>
                </tr>
              ))}

              {(balancesFiltered || []).length === 0 && (
                <tr>
                  <td style={sx.td} colSpan={balancesColSpan || 7}>
                    <span style={sx.muted}>
                      {loadingBalances
                        ? "Loading…"
                        : (balancesSymbolQuery || "").trim()
                        ? "No balances match that symbol filter."
                        : "No balances loaded."}
                    </span>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}

        {isSolanaVenue && (
          <div style={{ marginTop: 12, borderTop: "1px solid rgba(255,255,255,0.08)", paddingTop: 10 }}>
            <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
              <div style={{ fontWeight: 800 }}>On-chain (Solana)</div>
              <span style={{ ...sx.muted, fontSize: 12 }}>Source: Solana RPC (live)</span>
              <button
                data-no-drag="1"
                style={btn?.(solanaOnchain.loading) ?? smallBtn(solanaOnchain.loading)}
                disabled={solanaOnchain.loading}
                onClick={() => refreshSolanaOnchainBalances()}
              >
                {solanaOnchain.loading ? "Loading…" : solanaOnchain.address ? "Refresh on-chain" : "Connect + Load"}
              </button>

              <button
                data-no-drag="1"
                style={btn?.(false) ?? smallBtn(false)}
                onClick={() => setShowSolanaOnchainDetails((v) => !v)}
              >
                {showSolanaOnchainDetails ? "Hide details" : "Show details"}
              </button>

              {solanaOnchain.address && (
                <span style={{ ...sx.muted, fontSize: 12 }}>
                  {solanaOnchain.address.slice(0, 6)}…{solanaOnchain.address.slice(-4)}
                </span>
              )}

              {solanaOnchain.err && <span style={{ ...sx.badge, background: "rgba(255,80,80,0.18)" }}>{solanaOnchain.err}</span>}
            </div>

            {showSolanaOnchainDetails ? (
              <div style={{ marginTop: 8 }}>
                <table style={{ ...sx.table, fontSize: 13 }}>
                  <thead>
                    <tr>
                      <th style={sx.th}>Token</th>
                      <th style={sx.th}>Mint</th>
                      <th style={sx.thRight}>Amount</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(solanaOnchain.items || [])
                      .filter((it) => (hideZeroBalances ? Number(it?.amount || 0) !== 0 : true))
                      .map((it, idx) => (
                        <tr key={it.mint || it.asset || idx}>
                          <td style={sx.td}>{it.symbol || it.asset || "—"}</td>
                          <td style={sx.td}>{hideTableDataGlobal ? "••••" : (it.mint ? `${String(it.mint).slice(0, 6)}…` : "—")}</td>
                          <td style={sx.tdRight}>{hideTableDataGlobal ? "••••" : fmtBal(it.amount)}</td>
                        </tr>
                      ))}

                    {(solanaOnchain.items || []).length === 0 && (
                      <tr>
                        <td style={sx.td} colSpan={3}>
                          <span style={sx.muted}>{solanaOnchain.loading ? "Loading…" : "No on-chain balances loaded."}</span>
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            ) : null}
          </div>
        )}
      </>
    );
  }

  function renderLocalOrders() {
    const row = { display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" };
    return (
      <>
        <div style={row}>
          <div style={{ fontSize: 16, fontWeight: 800 }}>Orders (Local DB)</div>

          <button data-no-drag="1" style={btn?.(loadingOrders) ?? smallBtn(loadingOrders)} disabled={loadingOrders} onClick={() => doLoadOrders?.()}>
            {loadingOrders ? "Loading…" : "Load"}
          </button>

          <div data-no-drag="1" style={sx.pill}>
            <span>Status</span>
            <select
              data-no-drag="1"
              style={{ ...sx.select, width: 160 }}
              value={statusFilter || ""}
              onChange={(e) => setStatusFilter?.(e.target.value)}
              title="Optional status filter"
            >
              <option value="">(any)</option>
              <option value="open">open</option>
              <option value="acked">acked</option>
              <option value="filled">filled</option>
              <option value="canceled">canceled</option>
              <option value="rejected">rejected</option>
            </select>
          </div>

          <div data-no-drag="1" style={sx.pill}>
            <span>Symbol</span>
            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 180 }}
              value={localSymbolFilter || ""}
              onChange={(e) => setLocalSymbolFilter?.(e.target.value)}
              placeholder="e.g. BTC-USD"
            />
          </div>

          <label data-no-drag="1" style={sx.pill} title="Hide canceled/acked/rejected (unless filtered to them)">
            <input
              data-no-drag="1"
              type="checkbox"
              checked={!!hideCancelledLocal}
              disabled={!!statusIsCanceledLocal}
              onChange={(e) => setHideCancelledLocal?.(e.target.checked)}
            />
            <span>Hide canceled</span>
          </label>

          <div style={{ marginLeft: "auto", ...sx.muted, fontSize: 12 }}>
            Rows: <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : (orders || []).length}</b>
          </div>
        </div>

        <table style={sx.table}>
          <thead>
            <tr>
              <th style={sx.th}>Created</th>
              <th style={sx.th}>Venue</th>
              <th style={sx.th}>Symbol</th>
              <th style={sx.th}>Side</th>
              <th style={sx.th}>Type</th>
              <th style={sx.th}>Qty</th>
              <th style={sx.th}>Limit</th>
              <th style={sx.th}>Status</th>
              <th style={sx.th}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {(orders || []).map((o, idx) => {
              const st = normalizeStatus(o?.status || "");
              const kind = classifyStatusKind(normalizeStatusLower(st), "");
              const isTerminalLike = kind === "filled" || kind === "canceled" || kind === "rejected" || kind === "terminal";

              const cancelRef = String(o?.cancel_ref || o?.cancelRef || "").trim();
              const hasPreferredCancel = !!cancelRef && typeof doCancelUnifiedOrder === "function";
              const hasIdCancel = o?.id != null && typeof doCancelOrder === "function";

              const statusAllows = isCancelableStatusAnyCase(st);
              const canCancel = !isTerminalLike && statusAllows && (hasPreferredCancel || hasIdCancel);

              const k = rowCancelKey(o);
              const isCanceling = !!cancelingKeys[k];

              const disabled = !!hideTableDataGlobal || !canCancel || isCanceling;

              const title = hideTableDataGlobal
                ? "Hidden"
                : isCanceling
                  ? "Canceling…"
                  : isTerminalLike
                    ? "Order is terminal"
                    : !statusAllows
                      ? "Order status is not cancelable"
                      : hasPreferredCancel
                        ? "Cancel (uses cancel_ref via unified cancel endpoint)"
                        : hasIdCancel
                          ? "Cancel (local DB id)"
                          : "Missing cancel_ref and local id (cannot cancel from this row)";

              return (
                <tr key={o.id || o.client_order_id || o.order_id || `${o.created_at}-${o.symbol}-${idx}`} style={localOrderRowStyle(o)}>
                  <td style={sx.td}>{maskMaybe?.(o.created_at ? fmtTime?.(o.created_at) : "—")}</td>
                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : o.venue || "—"}</td>

                  {renderClickableSymbolCell({
                    symbolCanon: o.symbol_canon || o.symbol || "",
                    venueMaybe: o.venue || "",
                  })}

                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : o.side || "—"}</td>
                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : o.type || "—"}</td>

                  {/* UPDATED: high-precision qty + limit */}
                  <td style={sx.td}>{mask?.(fmtEcoHi(o.qty ?? o.filled_qty))}</td>
                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : o.limit_price !== null && o.limit_price !== undefined ? fmtEcoHi(o.limit_price) : "—"}</td>

                  <td style={sx.td}>{hideTableDataGlobal ? "••••" : o.status || "—"}</td>
                  <td style={sx.td}>
                    <button
                      data-no-drag="1"
                      style={btn?.(disabled) ?? smallBtn(disabled)}
                      disabled={disabled}
                      onClick={() => openCancelModalLocal(o)}
                      title={title}
                    >
                      {isCanceling ? "Canceling…" : "Cancel"}
                    </button>
                  </td>
                </tr>
              );
            })}

            {(orders || []).length === 0 && (
              <tr>
                <td style={sx.td} colSpan={9}>
                  <span style={sx.muted}>{loadingOrders ? "Loading…" : "No local orders."}</span>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </>
    );
  }

  function renderAllOrders() {
    const row = { display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" };

    const applyAoSymbol = () => {
      const v = String(aoSymbolDraft || "").trim();
      // Important: manual entry in All Orders must honor BOTH the explicit All Orders venue
      // filter and the main app venue selector, otherwise Solana-friendly pairs like
      // UTTT-USDC / USDC-SOL fall back to backend CEX-only symbol search when aoVenue is blank.
      const looksFriendlySolanaPair = shouldUseLocalSolanaSymbolFilter(v, aoVenue || venue || "");

      const kickReload = () => {
        try {
          if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
            window.requestAnimationFrame(() => {
              window.requestAnimationFrame(() => {
                try { doLoadAllOrders?.(); } catch {}
              });
            });
          } else {
            window.setTimeout(() => {
              try { doLoadAllOrders?.(); } catch {}
            }, 0);
          }
        } catch {
          // ignore
        }
      };

      if (!v) {
        setAoLocalSymbolFilter("");
        setAoSymbol?.("");
        setAoPage?.(1);
        kickReload();
        return;
      }
      if (looksFriendlySolanaPair) {
        setAoLocalSymbolFilter(v);
        // Always clear backend symbol for friendly Solana local-filter mode.
        setAoSymbol?.("");
        setAoPage?.(1);
        kickReload();
        return;
      }
      setAoLocalSymbolFilter(v);
      setAoSymbol?.(v);
      setAoPage?.(1);
      kickReload();
    };

    return (
      <>
        <div style={row}>
          <div style={{ fontSize: 16, fontWeight: 800 }}>All Orders (Unified)</div>

          <button
            data-no-drag="1"
            style={btn?.(loadingAll) ?? smallBtn(loadingAll)}
            disabled={loadingAll}
            onClick={() => doLoadAllOrders?.()}
            title="Load from unified view (no venue ingestion)"
          >
            {loadingAll ? "Loading…" : "Load"}
          </button>

		  <button
			data-no-drag="1"
			style={btn?.(loadingAll) ?? smallBtn(loadingAll)}
			disabled={loadingAll}
            onClick={doAllOrdersSyncAndLoad}
			title="Refresh venue orders then load unified view"
		  >
			{loadingAll ? "Syncing…" : "Sync+Load"}
		  </button>

          <div data-no-drag="1" style={sx.pill}>
            <label
              style={sx.chkLabel}
              title="When enabled, runs /api/ledger/sync after Sync+Load so realized lots/journals (and tax fields) are up to date."
            >
              <input
                data-no-drag="1"
                type="checkbox"
                checked={aoLedgerSyncOnSyncLoad}
                disabled={aoLedgerSyncBusy}
                onChange={(e) => setAoLedgerSyncOnSyncLoad(!!e.target.checked)}
              />
              <span style={{ marginLeft: 6 }}>Ledger sync</span>
            </label>

            <button
              data-no-drag="1"
              style={btn?.(aoLedgerSyncBusy) ?? smallBtn(aoLedgerSyncBusy)}
              disabled={aoLedgerSyncBusy}
              onClick={doAllOrdersLedgerSyncOnly}
              title="Run /api/ledger/sync now (creates lots + consumes sells into LotJournal), then reload All Orders"
            >
              {aoLedgerSyncBusy ? "Syncing…" : "Sync Ledger"}
            </button>

            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 110 }}
              value={aoLedgerSyncWalletId}
              onChange={(e) => setAoLedgerSyncWalletId(e.target.value)}
              title="wallet_id (default: default)"
            />

            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 70 }}
              value={aoLedgerSyncLimit}
              onChange={(e) => setAoLedgerSyncLimit(e.target.value)}
              title="limit (max 5000)"
            />

            <label style={sx.chkLabel} title="Preview only (no DB mutations).">
              <input
                data-no-drag="1"
                type="checkbox"
                checked={aoLedgerSyncDryRun}
                disabled={aoLedgerSyncBusy}
                onChange={(e) => setAoLedgerSyncDryRun(!!e.target.checked)}
              />
              <span style={{ marginLeft: 6 }}>dry_run</span>
            </label>

            {aoLedgerSyncLast ? (
              <span
                title={`last: lots=${aoLedgerSyncLast.created_lots ?? 0}, sells=${aoLedgerSyncLast.consumed_sells ?? 0}, skipped=${aoLedgerSyncLast.skipped ?? 0}`}
                style={{ opacity: 0.85, fontSize: 12 }}
              >
                lots:{aoLedgerSyncLast.created_lots ?? 0} / sells:{aoLedgerSyncLast.consumed_sells ?? 0}
              </span>
            ) : null}
            {aoLedgerSyncErr ? (
              <span title={String(aoLedgerSyncErr)} style={{ opacity: 0.9, fontSize: 12 }}>
                (ledger: {String(aoLedgerSyncErr)})
              </span>
            ) : null}
          </div>

          <div data-no-drag="1" style={sx.pill}>
            <span>Source</span>
            <select
              data-no-drag="1"
              style={{ ...sx.select, width: 140 }}
              value={aoSource || ""}
              onChange={(e) => {
                setAoSource?.(e.target.value);
                setAoPage?.(1);
              }}
            >
              <option value="">ALL</option>
              <option value="LOCAL">LOCAL</option>
              <option value="VENUES">VENUES</option>
            </select>
          </div>

          <div data-no-drag="1" style={sx.pill}>
            <span>Venue</span>
            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 120 }}
              value={aoVenue || ""}
              onChange={(e) => {
                setAoVenue?.(e.target.value);
                setAoPage?.(1);
              }}
              placeholder="(any)"
            />
          </div>

          <div data-no-drag="1" style={sx.pill}>
            <span>Bucket</span>
            <select
              data-no-drag="1"
              style={{ ...sx.select, width: 140 }}
              value={aoStatusBucketEff || ""}
              onChange={(e) => {
                const nextBucket = normalizeStatusBucket(e.target.value); // "" | "open" | "terminal"
                setAoStatusBucket?.(nextBucket);
                setAoPage?.(1);

                // Requested default sort behavior
                applyDefaultSortForBucket(nextBucket);
              }}
            >
              <option value="">ALL</option>
              <option value="open">open</option>
              <option value="terminal">terminal</option>
            </select>
          </div>

          {/* Status filter (client-side) */}
          <div
            data-no-drag="1"
            style={sx.pill}
            title="Filter by status text (case-insensitive). Separate multiple values by commas/spaces (OR). Example: filled,rejected"
          >
            <span>Status</span>
            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 160 }}
              value={aoStatusFilter || ""}
              onChange={(e) => setAoStatusFilter(e.target.value)}
              list="uttAoStatusHints"
              placeholder="(any)"
              onKeyDown={(e) => {
                if (e.key === "Enter") setAoPage?.(1);
              }}
            />
            {!!String(aoStatusFilter || "").trim() && (
              <button
                data-no-drag="1"
                style={btn?.(false) ?? smallBtn(false)}
                onClick={() => {
                  setAoStatusFilter("");
                  setAoPage?.(1);
                }}
                title="Clear status filter"
              >
                Clear
              </button>
            )}
          </div>

          <datalist id="uttAoStatusHints">
            <option value="open" />
            <option value="OPEN" />
            <option value="acked" />
            <option value="ACKED" />
            <option value="filled" />
            <option value="FILLED" />
            <option value="rejected" />
            <option value="REJECTED" />
            <option value="canceled" />
            <option value="cancelled" />
            <option value="CANCELED" />
            <option value="CANCELLED" />
            <option value="pending" />
            <option value="live" />
            <option value="partially_filled" />
            <option value="partial" />
          </datalist>

          <div data-no-drag="1" style={sx.pill}>
            <span>Symbol</span>
            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 160 }}
              value={aoSymbolDraft}
              onChange={(e) => setAoSymbolDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") applyAoSymbol();
              }}
              placeholder="(any)"
            />
            <button
              data-no-drag="1"
              style={btn?.(false) ?? smallBtn(false)}
              disabled={false}
              onClick={() => applyAoSymbol()}
              title={String(aoSymbolDraft || "").trim() ? "Apply symbol filter" : "Clear symbol filter (show all)"}
            >
              Apply
            </button>
          </div>

          <label data-no-drag="1" style={sx.pill} title="Hide canceled orders in unified view">
            <input data-no-drag="1" type="checkbox" checked={!!hideCancelledUnified} onChange={(e) => setHideCancelledUnified?.(e.target.checked)} />
            <span>Hide canceled</span>
          </label>


          <label data-no-drag="1" style={sx.pill} title="Play an audible alert when a new filled order is detected in All Orders">
            <input
              data-no-drag="1"
              type="checkbox"
              checked={!!aoFillSoundEnabled}
              onChange={(e) => {
                const on = !!e.target.checked;
                setAoFillSoundEnabled(on);
                if (on) {
                  // Attempt to "unlock" audio on a user gesture
                  playFillSound({ test: true });
                }
              }}
            />
            <span>Sound on fill</span>
          </label>

<label data-no-drag="1" style={sx.pill} title="Show a popup toast when a new fill is detected">
  <input
    data-no-drag="1"
    type="checkbox"
    checked={!!aoFillToastEnabled}
    onChange={(e) => setAoFillToastEnabled(!!e.target.checked)}
  />
  <span>Toast on fill</span>
</label>


          {aoFillSoundEnabled && (
            <div data-no-drag="1" style={sx.pill} title="Select the fill alert sound and volume">
              <span>Sound</span>
              <select
                data-no-drag="1"
                style={{ ...sx.select, width: 140 }}
                value={aoFillSoundType}
                onChange={(e) => setAoFillSoundType(e.target.value)}
              >
                <option value="chime">Chime</option>
                <option value="beep">Beep</option>
                <option value="ding">Ding</option>
                <option value="pop">Pop</option>
              </select>

              <span style={{ ...sx.muted, fontSize: 12 }}>Vol</span>
              <input
                data-no-drag="1"
                type="range"
                min="0"
                max="1"
                step="0.05"
                value={aoFillSoundVolume}
                onChange={(e) => setAoFillSoundVolume(Number(e.target.value))}
                style={{ width: 90 }}
              />

              <button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => playFillSound({ test: true })}>
                Test
              </button>
            </div>
          )}

          {/* Taxes (moved into floating window) */}
          <button
            data-no-drag="1"
            style={btn?.(aoTaxWinOpen) ?? smallBtn(aoTaxWinOpen)}
            onClick={() => setAoTaxWinOpen((v) => !v)}
            title={
              "Open tax settings. Combined = Fed + State. " +
              (aoTaxWithholdEnabled ? "Withholding is ENABLED." : "Withholding is OFF.")
            }
          >
            Taxes {Number(aoTaxCombinedPct || 0).toFixed(2)}%
          </button>


<button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => setColMgrOpen((v) => !v)} title="Show/hide column manager">
            {colMgrOpen ? "Hide Columns" : "Columns"}
          </button>

          <div style={{ marginLeft: "auto", ...sx.muted, fontSize: 12 }}>
            Showing <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : aoVisibleClient.length}</b> /{" "}
            <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : allTotal}</b>{" "}
            <span style={{ marginLeft: 10 }}>
              Open: <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : aoCounts.open}</b> • Terminal:{" "}
              <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : aoCounts.terminal}</b>
            </span>
          </div>
        </div>

        {colMgrOpen && (
          <div style={{ marginTop: 10, display: "flex", gap: 10, alignItems: "flex-start", flexWrap: "wrap" }}>
            <div style={{ ...sx.pill, alignItems: "flex-start" }}>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                <div style={{ fontWeight: 800, fontSize: 12 }}>Columns</div>

                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => setPreset("preferred")}>
                    Preferred
                  </button>
                  <button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => setPreset("legacy")}>
                    Legacy
                  </button>
                  <button
                    data-no-drag="1"
                    style={btn?.(false) ?? smallBtn(false)}
                    onClick={() => resetColumnsToPreferred({ clearStorage: true })}
                    title="Resets to Preferred and clears the saved column order in localStorage"
                  >
                    Reset Columns
                  </button>
                  <button
                    data-no-drag="1"
                    style={btn?.(!columns.includes(COLS.viewed)) ?? smallBtn(!columns.includes(COLS.viewed))}
                    disabled={!columns.includes(COLS.viewed)}
                    onClick={() => moveViewedNearLeft()}
                    title="Moves the Viewed column next to Created/Closed/Actions so it cannot disappear off-screen"
                  >
                    Move Viewed Left
                  </button>
                </div>

                <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                  <select
                    data-no-drag="1"
                    style={{ ...sx.select, width: 220 }}
                    defaultValue=""
                    onChange={(e) => {
                      const v = e.target.value;
                      if (v) addColumn(v);
                      e.target.value = "";
                    }}
                  >
                    <option value="">Add column…</option>
                    {availableToAdd.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>

                  <span style={{ ...sx.muted, fontSize: 12 }}>Preset: {columnPreset}</span>
                </div>

                <div style={{ ...sx.muted, fontSize: 12, marginTop: 6 }}>
                  Note: <b>Actions</b> is required (Cancel buttons) and cannot be removed.
                </div>
              </div>
            </div>

            <div style={{ flex: 1, minWidth: 340 }}>
              <table style={sx.table}>
                <thead>
                  <tr>
                    <th style={sx.th}>Column</th>
                    <th style={sx.th}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {columns.map((c) => {
                    const isRequired = c === COLS.actions;
                    return (
                      <tr key={c}>
                        <td style={sx.td}>{c}</td>
                        <td style={sx.td}>
                          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                            <button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => moveColumn(c, -1)}>
                              Up
                            </button>
                            <button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => moveColumn(c, +1)}>
                              Down
                            </button>
                            <button
                              data-no-drag="1"
                              style={btn?.(isRequired) ?? smallBtn(isRequired)}
                              disabled={isRequired}
                              onClick={() => removeColumn(c)}
                              title={isRequired ? "Actions is required and cannot be removed." : "Remove column"}
                            >
                              Remove
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}

                  {columns.length === 0 && (
                    <tr>
                      <td style={sx.td} colSpan={2}>
                        <span style={sx.muted}>No columns selected.</span>
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        <div style={{ marginTop: 10 }}>
          <table style={sx.table}>
            <thead>
              <tr>{columns.map((c) => renderAllOrdersHeader(c))}</tr>
            </thead>
            <tbody>
              {aoVisibleClient.map((o, idx) => (
                <tr key={o.view_key || o.id || `${o.created_at || ""}-${idx}`} style={orderRowStyle(o)}>
                  {columns.map((c) => renderAllOrdersCell(o, c))}
                </tr>
              ))}

              {aoVisibleClient.length === 0 && (
                <tr>
                  <td style={sx.td} colSpan={Math.max(1, columns.length)}>
                    <span style={sx.muted}>
                      {loadingAll
                        ? "Loading…"
                        : aoStatusTokens.length
                          ? "No unified orders match current filters (including Status)."
                          : "No unified orders for current filters."}
                    </span>
                  </td>
                </tr>
              )}
            </tbody>
          </table>

          <div style={{ marginTop: 10, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <button
              data-no-drag="1"
              style={btn?.(aoPage <= 1) ?? smallBtn(aoPage <= 1)}
              disabled={aoPage <= 1}
              onClick={() => setAoPage?.(Math.max(1, Number(aoPage || 1) - 1))}
            >
              Prev
            </button>

            <div style={{ ...sx.muted, fontSize: 12 }}>
              Page <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : aoPage}</b> /{" "}
              <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : aoTotalPages}</b>
            </div>

            <button
              data-no-drag="1"
              style={btn?.(aoPage >= aoTotalPages) ?? smallBtn(aoPage >= aoTotalPages)}
              disabled={aoPage >= aoTotalPages}
              onClick={() => setAoPage?.(Math.min(Number(aoTotalPages || 1), Number(aoPage || 1) + 1))}
            >
              Next
            </button>

            <div data-no-drag="1" style={sx.pill}>
              <span>Size</span>
              <select
                data-no-drag="1"
                style={{ ...sx.select, width: 90 }}
                value={String(aoPageSize || 50)}
                onChange={(e) => {
                  setAoPageSize?.(Number(e.target.value));
                  setAoPage?.(1);
                }}
              >
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
                <option value="200">200</option>
              </select>
            </div>
          </div>
        </div>
      </>
    );
  }

  // ─────────────────────────────────────────────────────────────
  // Discover: local fallback state for days filter (in case App.jsx not yet updated)
  // ─────────────────────────────────────────────────────────────
  const [discDaysLocal, setDiscDaysLocal] = useState(() => {
    const v = safeParseJson(localStorage.getItem(LS_DISC_DAYS_KEY) || "");
    const n = Number(v);
    return Number.isFinite(n) ? n : 7;
  });

  const effectiveDiscDays = discDays !== undefined && discDays !== null ? discDays : discDaysLocal;
  const setEffectiveDiscDays = typeof setDiscDays === "function" ? setDiscDays : setDiscDaysLocal;

  useEffect(() => {
    try {
      localStorage.setItem(LS_DISC_DAYS_KEY, JSON.stringify(effectiveDiscDays));
    } catch {
      // ignore
    }
  }, [effectiveDiscDays]);


  // Discovery-enabled venues (UI allow-list)
  // We only show venues that are BOTH supported by the app AND in this allow-list.
  const discoveryAllowSet = useMemo(() => {
    // If App.jsx passes an explicit allow-list, use it. Otherwise use a sane default.
    const raw =
      Array.isArray(discoveryVenuesProp) && discoveryVenuesProp.length
        ? discoveryVenuesProp
        : DEFAULT_DISCOVERY_ALLOW_VENUES;

    return new Set(
      raw
        .map((vv) => String(vv || "").trim().toLowerCase())
        .filter(Boolean)
    );
  }, [discoveryVenuesProp]);

  // Discovery dropdown venue list:
  // Prefer backend-provided discovery venues (passed from App.jsx), otherwise fall back to enabled venues,
  // otherwise fall back to supported venues. Always normalize/dedupe, remove the ALL option, and apply allow-list.
  const discoveryVenueOptions = useMemo(() => {
    const src =
      Array.isArray(discoveryVenuesProp) && discoveryVenuesProp.length
        ? discoveryVenuesProp
        : Array.isArray(enabledVenues) && enabledVenues.length
          ? enabledVenues
          : supportedVenues || [];

    const out = [];
    const seen = new Set();
    const allKey = String(ALL_VENUES_VALUE || "").trim().toLowerCase();

    for (const v of src) {
      const vv = String(v || "").trim().toLowerCase();
      if (!vv) continue;
      if (allKey && vv === allKey) continue;
      if (seen.has(vv)) continue;
      if (!discoveryAllowSet.has(vv)) continue;
      seen.add(vv);
      out.push(vv);
    }

    // Final fallback: keep the UI functional even if src was empty.
    if (!out.length) {
      const fb = (supportedVenues || ["gemini"]).map((vv) => String(vv || "").trim().toLowerCase());
      for (const vv of fb) {
        if (!vv) continue;
        if (allKey && vv === allKey) continue;
        if (seen.has(vv)) continue;
        if (!discoveryAllowSet.has(vv)) continue;
        seen.add(vv);
        out.push(vv);
      }
    }

    return out;
  }, [discoveryVenuesProp, enabledVenues, supportedVenues, ALL_VENUES_VALUE, discoveryAllowSet]);


  function renderDiscover() {
    const v = String(discVenue || "").trim().toLowerCase();
    const discoveryVenues = discoveryVenueOptions.length ? discoveryVenueOptions : ["gemini"];



    const venueOk = discoveryVenues.includes(v);
    const effectiveV = venueOk ? v : discoveryVenues.includes("gemini") ? "gemini" : discoveryVenues[0] || "gemini";

    const eps = Number(discEps);
    const epsOk = Number.isFinite(eps) && eps > 0;

    const daysN = Number(effectiveDiscDays);
    const daysOk = Number.isFinite(daysN) && daysN > 0;
    const daysParam = daysOk ? Math.max(1, Math.round(daysN)) : null;

    const newItems = Array.isArray(discNew) ? discNew : [];
    const unheldItems = Array.isArray(discUnheld) ? discUnheld : [];

    const row = { display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" };

    const metaText = hideTableDataGlobal
      ? "••••"
      : `New: ${Number(discMeta?.new_count ?? newItems.length)} • Unheld: ${Number(discMeta?.unheld_count ?? unheldItems.length)}${
          daysParam ? ` • Days: ${daysParam}` : ""
        }`;

    const showVenue = hideVenueNames ? "••••" : effectiveV;

    const applyRow = (r) => {
      const sym = pickSymbolCanon(r);
      if (!sym) return;
      applySymbolFromDiscover?.(sym);
    };

    const timeMaybe = (t) => {
      if (!t) return "—";
      try {
        return fmtTime ? fmtTime(t) : String(t);
      } catch {
        return String(t);
      }
    };

    const renderViewedCell = (r) => {
      const sym = String(pickSymbolCanon(r) || "").trim().toUpperCase();
      const disabled = !sym || !toggleDiscoverySymbolViewed || !isDiscoverySymbolViewed;

      const checked = disabled ? false : !!isDiscoverySymbolViewed(effectiveV, sym);

      return (
        <td style={sx.td}>
          {disabled ? (
            <span style={sx.muted}>—</span>
          ) : (
            <input
              data-no-drag="1"
              type="checkbox"
              checked={checked}
              onChange={() => toggleDiscoverySymbolViewed(effectiveV, sym)}
              title="Mark this discovered symbol as viewed/verified (local)"
            />
          )}
        </td>
      );
    };

    const renderTable = ({ title, items, emptyText }) => {
      return (
        <div style={{ marginTop: 12 }}>
          <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontSize: 16, fontWeight: 800 }}>{title}</div>
            <div style={{ ...sx.muted, fontSize: 12 }}>{hideTableDataGlobal ? "••••" : `Rows: ${items.length}`}</div>
          </div>

          <table style={sx.table}>
            <thead>
              <tr>
                <th style={sx.th}>Viewed</th>
                <th style={sx.th}>Symbol</th>
                <th style={sx.th}>Venue Symbol</th>
                <th style={sx.th}>First Seen (UTC)</th>
                <th style={sx.th}>Action</th>
              </tr>
            </thead>
            <tbody>
              {items.map((r, idx) => {
                const sym = pickSymbolCanon(r);
                const venueSym = pickVenueSymbol(r);
                const first = pickFirstSeen(r);

                return (
                  <tr key={`${sym || venueSym || "row"}-${idx}`}>
                    {renderViewedCell(r)}
                    {renderClickableSymbolCell({ symbolCanon: sym, venueMaybe: effectiveV })}
                    <td style={sx.td}>{hideTableDataGlobal ? "••••" : venueSym || "—"}</td>
                    <td style={sx.td}>{hideTableDataGlobal ? "••••" : timeMaybe(first)}</td>
                    <td style={sx.td}>
                      <button
                        data-no-drag="1"
                        style={smallBtn(!sym)}
                        disabled={!sym}
                        onClick={() => applyRow(r)}
                        title={sym ? "Apply symbol to market (chart/orderbook)" : "Missing symbol"}
                      >
                        Apply
                      </button>
                    </td>
                  </tr>
                );
              })}

              {items.length === 0 && (
                <tr>
                  <td style={sx.td} colSpan={5}>
                    <span style={sx.muted}>{loadingDiscover ? "Loading…" : emptyText}</span>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      );
    };

    const discoveryVenuesText = hideTableDataGlobal ? "••••" : discoveryVenues.length ? discoveryVenues.join(", ") : "gemini";

    const loadDiscover = ({ refreshFirst }) => {
      doLoadDiscover?.({ refreshFirst: !!refreshFirst, days: daysParam });
    };

    return (
      <>
        <div style={row}>
          <div style={{ fontSize: 16, fontWeight: 800 }}>Discover (New Listings)</div>

          <div data-no-drag="1" style={sx.pill}>
            <span>Venue</span>
            <select
              data-no-drag="1"
              style={{ ...sx.select, width: 140 }}
              value={effectiveV}
              onChange={(e) => setDiscVenue?.(e.target.value)}
              title="Venue to query for symbol discovery"
            >
              {(discoveryVenues.length ? discoveryVenues : ["gemini"]).map((vv) => (
                <option key={vv} value={vv}>
                  {hideVenueNames ? "••••" : vv}
                </option>
              ))}
            </select>
          </div>

          <div
            data-no-drag="1"
            style={sx.pill}
            title="Only show newly-discovered symbols whose First Seen date is within the last X days. Set blank/0 to disable."
          >
            <span>Last</span>
            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 90 }}
              type="number"
              min="0"
              max="3650"
              value={String(effectiveDiscDays ?? "")}
              onChange={(e) => setEffectiveDiscDays(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") loadDiscover({ refreshFirst: false });
              }}
              placeholder="7"
            />
            <span>days</span>
            <span style={{ ...sx.muted, fontSize: 12 }}>{daysParam ? "" : "off"}</span>
          </div>

          <div data-no-drag="1" style={sx.pill} title="EPS threshold for considering a balance 'held' vs 'unheld'">
            <span>EPS</span>
            <input
              data-no-drag="1"
              style={{ ...sx.input, width: 120 }}
              value={String(discEps ?? "")}
              onChange={(e) => setDiscEps?.(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") loadDiscover({ refreshFirst: false });
              }}
              placeholder="1e-8"
            />
            <span style={{ ...sx.muted, fontSize: 12 }}>{epsOk ? "" : "invalid"}</span>
          </div>

          <button
            data-no-drag="1"
            style={btn?.(loadingDiscover) ?? smallBtn(loadingDiscover)}
            onClick={() => loadDiscover({ refreshFirst: false })}
            disabled={loadingDiscover}
            title="Reload New + Unheld New lists (GET only)"
          >
            {loadingDiscover ? "Loading…" : "Load"}
          </button>

          <button
            data-no-drag="1"
            style={btn?.(loadingDiscover) ?? smallBtn(loadingDiscover)}
            onClick={() => loadDiscover({ refreshFirst: true })}
            disabled={loadingDiscover}
            title="Refresh symbol snapshot first (POST), then reload lists"
          >
            {loadingDiscover ? "Refreshing…" : "Refresh discovery"}
          </button>

          <div style={{ marginLeft: "auto", ...sx.muted, fontSize: 13 }}>
            Venue: <b style={{ color: pal.text }}>{showVenue}</b> • {metaText}
          </div>
        </div>

        <div style={{ ...sx.muted, fontSize: 12, marginTop: 8 }}>
          Notes: “Load” does not create a new snapshot; “Refresh discovery” does (then reloads lists). Discover is manual-only.
          <br />
          Filtering: “Last X days” uses <b>First Seen (UTC)</b> (the earliest snapshot time your terminal observed the symbol).
          <br />
          Discovery venues enabled: {discoveryVenuesText}
        </div>

        {renderTable({
          title: "New Listings",
          items: newItems,
          emptyText: "No new listings detected (diff is empty).",
        })}

        {renderTable({
          title: "Unheld New Listings",
          items: unheldItems,
          emptyText: "No unheld-new listings (either none are new, or you already hold them above EPS).",
        })}
      </>
    );
  }

  const dockHint = dockBelowChart ? "Docked under chart" : "Floating";

  function renderManualCancelModal() {
    if (!showManualCancelModal) return null;

    const overlay = {
      position: "absolute",
      left: 0,
      top: 0,
      width: "100%",
      height: "100%",
      background: "rgba(0,0,0,0.55)",
      zIndex: 110,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      padding: 12,
    };

    const card = {
      width: "min(560px, 100%)",
      border: `1px solid ${pal.border}`,
      background: pal.widgetBg,
      borderRadius: 12,
      boxShadow: `0 18px 40px ${pal.shadow}`,
      padding: 16,
      color: pal.text,
    };

    const disableSubmit = !!manualCancelBusy || !String(manualCancelOrderId || "").trim();

    return (
      <div
        data-no-drag="1"
        style={overlay}
        onPointerDown={(e) => {
          e.stopPropagation();
          if (manualCancelBusy) return;
          setShowManualCancelModal?.(false);
        }}
      >
        <div
          data-no-drag="1"
          style={card}
          onPointerDown={(e) => {
            e.stopPropagation();
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, marginBottom: 12 }}>
            <div>
              <div style={{ fontSize: 18, fontWeight: 800 }}>Manual Cancel</div>
              <div style={{ fontSize: 12, opacity: 0.75, marginTop: 4 }}>
                Cancel by order id. Solana-Jupiter is wired now; other venues can be added to this same flow later.
              </div>
            </div>
            <button
              type="button"
              onClick={() => {
                if (manualCancelBusy) return;
                setShowManualCancelModal?.(false);
              }}
              style={{ ...sx.button, padding: "6px 10px" }}
            >
              Close
            </button>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "120px 1fr", gap: 10, alignItems: "center" }}>
            <div style={{ fontSize: 12, opacity: 0.8, fontWeight: 700 }}>Venue</div>
            <select
              value={manualCancelVenue}
              onChange={(e) => setManualCancelVenue?.(e.target.value)}
              style={sx.select}
            >
              <option value="solana_jupiter">Solana-Jupiter</option>
              <option value="coinbase">Coinbase (later)</option>
              <option value="kraken">Kraken (later)</option>
              <option value="robinhood">Robinhood (later)</option>
              <option value="dex_trade">Dex-Trade (later)</option>
            </select>

            <div style={{ fontSize: 12, opacity: 0.8, fontWeight: 700 }}>Order ID</div>
            <input
              value={manualCancelOrderId}
              onChange={(e) => setManualCancelOrderId?.(e.target.value)}
              placeholder={manualCancelVenue === "solana_jupiter" ? "Paste Jupiter order id" : "Paste order id"}
              style={{ ...sx.input, width: "100%" }}
            />
          </div>

          <div style={{ fontSize: 12, opacity: 0.72, marginTop: 10 }}>
            {manualCancelVenue === "solana_jupiter"
              ? "Use the Jupiter order account id, not the transaction signature."
              : "This venue is not wired yet, but the modal is ready for future order-id cancel flows."}
          </div>

          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 16 }}>
            <button
              type="button"
              onClick={() => {
                if (manualCancelBusy) return;
                setShowManualCancelModal?.(false);
              }}
              style={sx.button}
            >
              Close
            </button>
            <button
              type="button"
              onClick={submitManualCancelOrder}
              disabled={disableSubmit}
              style={{ ...sx.button, ...(disableSubmit ? sx.buttonDisabled : {}), fontWeight: 700 }}
            >
              {manualCancelBusy ? "Canceling…" : "Cancel Order"}
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Cancel confirmation modal (rendered over widget)
  function renderCancelConfirmModal() {
    if (!cancelModal.open) return null;

    const o = cancelModal.row || {};
    const sym = pickSymbolCanon(o) || o.symbol || "—";
    const ven = String(o.venue || o.venue_name || aoVenue || (venue === ALL_VENUES_VALUE ? "" : venue) || "").trim() || "—";
    const src = String(o.source || "").trim() || (cancelModal.kind === "local" ? "LOCAL" : "—");
    const st = normalizeStatus(pickOrderStatus(o)) || (cancelModal.kind === "local" ? String(o.status || "") : "") || "—";
    const created = o.created_at ? fmtTime?.(o.created_at) : o.createdAt ? fmtTime?.(o.createdAt) : "—";
    const cancelRef = String(o.cancel_ref || o.cancelRef || "").trim();

    const overlay = {
      position: "absolute",
      left: 0,
      top: 0,
      width: "100%",
      height: "100%",
      background: "rgba(0,0,0,0.55)",
      zIndex: 100,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      padding: 12,
    };

    const card = {
      width: "min(520px, 100%)",
      border: `1px solid ${pal.border}`,
      background: pal.widgetBg,
      borderRadius: 12,
      boxShadow: `0 18px 40px ${pal.shadow}`,
      padding: 14,
      color: pal.text,
    };

    const line = { display: "flex", gap: 10, flexWrap: "wrap", fontSize: 12, color: pal.muted };

    return (
      <div
        data-no-drag="1"
        style={overlay}
        onPointerDown={(e) => {
          e.stopPropagation();
          closeCancelModal();
        }}
      >
        <div
          data-no-drag="1"
          style={card}
          onPointerDown={(e) => {
            e.stopPropagation();
          }}
        >
          <div style={{ fontSize: 16, fontWeight: 900, marginBottom: 10 }}>{cancelModal.title || "Confirm cancel?"}</div>

          <div style={{ ...sx.muted, fontSize: 12, marginBottom: 10 }}>
            This action will submit a cancel request. If the venue has already filled the order, the cancel may be rejected.
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 12 }}>
            <div style={line}>
              <span style={sx.muted}>Symbol:</span> <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : sym}</b>
              <span style={{ marginLeft: 10, ...sx.muted }}>Venue:</span> <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : ven}</b>
            </div>
            <div style={line}>
              <span style={sx.muted}>Source:</span> <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : src || "—"}</b>
              <span style={{ marginLeft: 10, ...sx.muted }}>Status:</span> <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : st}</b>
            </div>
            <div style={line}>
              <span style={sx.muted}>Created:</span> <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : created}</b>
              {cancelRef ? (
                <>
                  <span style={{ marginLeft: 10, ...sx.muted }}>cancel_ref:</span> <b style={{ color: pal.text }}>{hideTableDataGlobal ? "••••" : cancelRef}</b>
                </>
              ) : null}
            </div>
          </div>

          <div style={{ display: "flex", gap: 10, justifyContent: "flex-end", flexWrap: "wrap" }}>
            <button data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => closeCancelModal()}>
              Cancel
            </button>

            <button id="uttCancelModalConfirmBtn" data-no-drag="1" style={btn?.(false) ?? smallBtn(false)} onClick={() => confirmCancelFromModal()} title="Confirm cancel">
              Confirm
            </button>
          </div>
        </div>
      </div>
    );
  }

  // NEW: Custom theme editor UI (only shown when themeKey === "custom")
  function renderCustomThemeEditor() {
    if (themeKey !== "custom") return null;
    if (!customThemeEditorOpen) return null;

    // Users expect this editor to be visible immediately when selecting "Custom".
    // If it is rendered after large tables, it can be pushed far below the fold.
    // We render it near the top of the body (see return block) and also provide
    // a ref so we can scroll it into view on theme selection.

    const row = { display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" };
    const grid = {
      display: "grid",
      gridTemplateColumns: "repeat(3, minmax(180px, 1fr))",
      gap: 10,
      width: "100%",
    };

    const item = {
      border: `1px solid ${pal.border}`,
      background: pal.panelBg,
      borderRadius: 12,
      padding: 10,
      display: "flex",
      gap: 10,
      alignItems: "center",
      justifyContent: "space-between",
    };

    const label = { fontSize: 12, fontWeight: 800, color: pal.text };
    const hint = { fontSize: 11, color: pal.muted, marginTop: 2 };

    const colorField = (key, title) => {
      const v = isHexColor(customTheme?.[key]) ? customTheme[key] : "#000000";
      return (
        <div style={item} title={title} key={key}>
          <div style={{ minWidth: 0 }}>
            <div style={label}>{title}</div>
            <div style={hint}>{key}</div>
          </div>
          <input
            data-no-drag="1"
            type="color"
            value={v}
            onChange={(e) => {
              const next = String(e.target.value || "").trim();
              if (!isHexColor(next)) return;
              setCustomTheme((p) => ({ ...(p || {}), [key]: next }));
            }}
            style={{ width: 44, height: 28, padding: 0, border: `1px solid ${pal.border}`, background: pal.widgetBg2, borderRadius: 8 }}
          />
        </div>
      );
    };

    const shadowColor = isHexColor(customTheme?.shadowColor) ? customTheme.shadowColor : "#000000";
    const shadowAlpha = Number(customTheme?.shadowAlpha);
    const shadowAlphaOk = Number.isFinite(shadowAlpha) ? clamp(shadowAlpha, 0, 1) : 0.35;

    return (
      <div
        ref={customThemeEditorRef}
        data-no-drag="1"
        style={{ marginTop: 10, border: `1px solid ${pal.border}`, background: pal.widgetBg2, borderRadius: 12, padding: 10 }}
      >
        <div style={row}>
          <div style={{ fontWeight: 900 }}>Custom Theme</div>
          <div style={{ ...sx.muted, fontSize: 12 }}>These persist in localStorage and apply immediately to this widget.</div>
          <div style={{ marginLeft: "auto", display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              data-no-drag="1"
              style={btn?.(false) ?? smallBtn(false)}
              onClick={() => setCustomThemeEditorOpen(false)}
              title="Hide Custom theme editor (will re-open next time you select Custom from the theme dropdown)"
            >
              Hide
            </button>
            <button
              data-no-drag="1"
              style={btn?.(false) ?? smallBtn(false)}
              onClick={() => {
                // reset custom palette to PALETTES.custom defaults
                const base = PALETTES.custom;
                setCustomTheme((p) => ({
                  ...(p || {}),
                  widgetBg: base.widgetBg,
                  widgetBg2: base.widgetBg2,
                  panelBg: base.panelBg,
                  border: base.border,
                  border2: base.border2,
                  text: base.text,
                  muted: base.muted,
                  link: base.link,
                  warn: base.warn,
                  danger: base.danger,
                  good: base.good,
                  shadowColor: "#000000",
                  shadowAlpha: 0.35,
                }));
              }}
              title="Reset Custom palette to defaults"
            >
              Reset Custom
            </button>
          </div>
        </div>

        <div style={{ marginTop: 10, ...grid }}>
          {colorField("widgetBg", "Widget Background")}
          {colorField("widgetBg2", "Widget Background 2")}
          {colorField("panelBg", "Panel Background")}

          {colorField("border", "Border")}
          {colorField("border2", "Border 2")}
          {colorField("text", "Text")}

          {colorField("muted", "Muted Text")}
          {colorField("link", "Link / Accent")}
          {colorField("warn", "Warn")}

          {colorField("danger", "Danger")}
          {colorField("good", "Good")}
          <div style={item} title="Drop shadow controls">
            <div style={{ minWidth: 0 }}>
              <div style={label}>Shadow</div>
              <div style={hint}>shadowColor + shadowAlpha</div>
            </div>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <input
                data-no-drag="1"
                type="color"
                value={shadowColor}
                onChange={(e) => {
                  const next = String(e.target.value || "").trim();
                  if (!isHexColor(next)) return;
                  setCustomTheme((p) => ({ ...(p || {}), shadowColor: next }));
                }}
                style={{ width: 44, height: 28, padding: 0, border: `1px solid ${pal.border}`, background: pal.widgetBg2, borderRadius: 8 }}
              />
              <input
                data-no-drag="1"
                type="number"
                min="0"
                max="1"
                step="0.05"
                value={String(shadowAlphaOk)}
                onChange={(e) => {
                  const n = Number(e.target.value);
                  if (!Number.isFinite(n)) return;
                  setCustomTheme((p) => ({ ...(p || {}), shadowAlpha: clamp(n, 0, 1) }));
                }}
                style={{ ...sx.input, width: 90 }}
              />
            </div>
          </div>
        </div>
      </div>
    );
  }

  // ─────────────────────────────────────────────────────────────
  // Widget shell / tabs
  // ─────────────────────────────────────────────────────────────
  const shell = {
    // Keep viewport coordinates in a single reference frame.
    // Switching to `absolute` on lock was reinterpreting the same x/y inside the tables pane,
    // which is what shoved the widget to the lower-left / bottom on lock.
    position: "fixed",
    left: geom.x,
    top: geom.y,
    width: geom.w,
    height: geom.h,
    background: pal.widgetBg,
    border: `1px solid ${pal.border}`,
    borderRadius: 12,
    boxShadow: `0 18px 40px ${pal.shadow}`,
    overflow: "hidden",
    zIndex: locked ? 40 : 10000,
  };

  const headerBar = {
    display: "flex",
    alignItems: "center",
    gap: 10,
    padding: "10px 10px",
    borderBottom: `1px solid ${pal.border}`,
    background: pal.widgetBg2,
    userSelect: "none",
    cursor: locked ? "default" : "grab",
  };

  const tabsRow = {
    display: "flex",
    gap: 8,
    alignItems: "center",
    flexWrap: "wrap",
  };

  const tabBtn = (active) => ({
    ...sx.button,
    padding: "6px 10px",
    fontSize: 12,
    fontWeight: active ? 900 : 700,
    borderColor: active ? pal.border2 : pal.border,
    background: active ? pal.panelBg : pal.widgetBg2,
  });

  const body = {
    position: "absolute",
    left: 0,
    top: 52,
    right: 0,
    bottom: 0,
    overflow: "auto",
    padding: 10,
  };

  const resizeHandleBottom = {
    position: "absolute",
    left: 0,
    bottom: 0,
    width: "100%",
    height: 8,
    cursor: locked ? "default" : "ns-resize",
    background: "transparent",
  };

  const resizeHandleTop = {
    position: "absolute",
    left: 0,
    top: 0,
    width: "100%",
    height: 8,
    cursor: locked ? "default" : "ns-resize",
    background: "transparent",
  };

  const resizeHandleRight = {
    position: "absolute",
    top: 0,
    right: 0,
    width: 8,
    height: "100%",
    cursor: locked ? "default" : "ew-resize",
    background: "transparent",
  };

  const resizeHandleCorner = {
    position: "absolute",
    right: 0,
    bottom: 0,
    width: 16,
    height: 16,
    cursor: locked ? "default" : "nwse-resize",
    background: "transparent",
  };

  const themeSelect = (
    <div data-no-drag="1" style={{ ...sx.pill, marginLeft: "auto" }}>
      <span>Theme</span>
      <select
        data-no-drag="1"
        style={{ ...sx.select, width: 180 }}
        value={themeKey}
        onChange={(e) => setThemeKey(e.target.value)}
        title="Tables widget theme"
      >
        {Object.entries(PALETTES).map(([k, v]) => (
          <option key={k} value={k}>
            {v.name || k}
          </option>
        ))}
      </select>
    </div>
  );

  const tabKey = String(tab || "balances");
  const setTabSafe = (t) => {
    if (typeof setTab === "function") setTab(t);
  };

  function renderBodyForTab() {
    if (hideTableDataGlobal) {
      // still render UI; data masking happens per-cell, but this gives a subtle heads-up.
    }

    if (tabKey === "localOrders") return renderLocalOrders();
    if (tabKey === "allOrders") return renderAllOrders();
    if (tabKey === "discover") return renderDiscover();
    return renderBalances();
  }

  return (
    <div ref={widgetRef} style={shell}>
      {/* Drag header */}
      <div style={headerBar} onPointerDown={startDrag} title={locked ? "Locked" : "Drag to move (or adjust dock offset when docked)"}>
        <div style={{ fontWeight: 900, color: pal.text }}>Tables</div>

        <div data-no-drag="1" style={tabsRow}>
          <button data-no-drag="1" style={tabBtn(tabKey === "balances")} onClick={() => setTabSafe("balances")}>
            Balances
          </button>
          <button data-no-drag="1" style={tabBtn(tabKey === "localOrders")} onClick={() => setTabSafe("localOrders")}>
            Local Orders
          </button>
          <button data-no-drag="1" style={tabBtn(tabKey === "allOrders")} onClick={() => setTabSafe("allOrders")}>
            All Orders
          </button>
          <button data-no-drag="1" style={tabBtn(tabKey === "discover")} onClick={() => setTabSafe("discover")}>
            Discover
          </button>
        </div>

        <div data-no-drag="1" style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <button
            data-no-drag="1"
            type="button"
            onClick={() => setShowManualCancelModal?.(true)}
            style={{ ...sx.button, fontWeight: 700 }}
            title="Cancel an order by venue + order id"
          >
            Cancel Order
          </button>

          <label data-no-drag="1" style={sx.pill} title="Lock widget position/size">
            <input data-no-drag="1" type="checkbox" checked={!!locked} onChange={(e) => handleLockedChange(e.target.checked)} />
            <span>Lock</span>
          </label>

          <label data-no-drag="1" style={sx.pill} title="Dock widget below chart (follows chart x/w, uses offset for y)">
            <input data-no-drag="1" type="checkbox" checked={!!dockBelowChart} onChange={(e) => setDockBelowChart(e.target.checked)} />
            <span>Dock</span>
          </label>

          <div style={{ ...sx.muted, fontSize: 12 }}>{dockHint}</div>
        </div>

        {themeSelect}
      </div>

      {/* Body */}
      <div style={body}>
        {renderBalancesOverlib()}
        {renderCustomThemeEditor()}
        {renderBodyForTab()}
      </div>

      {/* Modal overlays */}
      {renderManualCancelModal()}
      {renderCancelConfirmModal()}

      {/* Floating windows */}
      {renderAoTaxWindow()}

      {/* Toast stack (fixed-position; render once at widget root) */}
      {typeof renderFillToasts === "function" ? renderFillToasts() : null}

      {/* Resize handles */}
      <div style={resizeHandleTop} onPointerDown={startResizeTop} />
      <div style={resizeHandleBottom} onPointerDown={startResizeBottom} />
      <div style={resizeHandleRight} onPointerDown={startResizeRight} />
      <div style={resizeHandleCorner} onPointerDown={startResizeCorner} />
    </div>
  );
}
