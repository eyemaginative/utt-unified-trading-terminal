// frontend/src/app/constants.js

// Shared constants used by the App shell + windowed tools.
// Keep this file “pure” (no React imports).

export const ALL_VENUES_VALUE = "ALL";

// NOTE: This list is used for *core trading/balances polling* fallback.
// Do not restrict it to “discovery-capable” venues.
export const DEFAULT_SUPPORTED_VENUES = ["gemini", "coinbase", "kraken", "robinhood", "dex_trade"];

// Arb venues (preferred cross-venue scan list)
export const ARB_VENUES = ["coinbase", "kraken", "gemini", "robinhood", "dex_trade"];

// Default polling
export const DEFAULT_POLL_ENABLED = true;
export const DEFAULT_POLL_SECONDS = 300;

// LocalStorage keys (mirrors what you currently have in App.jsx)
// Keep these stable so you do not “lose” existing user prefs.
export const LS_KEYS = Object.freeze({
  VISIBLE_WIDGETS: "utt_visible_widgets_v1",

  POLL_ENABLED: "utt_poll_enabled_v1",
  POLL_SECONDS: "utt_poll_seconds_v1",

  HIDE_CANCELLED_LOCAL: "utt_hide_cancelled_local_v1",
  // NOTE: bumped key to v2 to avoid inheriting the old default that hid canceled orders by default.
  HIDE_CANCELLED_UNIFIED: "utt_hide_cancelled_unified_v2",

  DISCOVER_VENUE: "utt_discover_venue_v1",
  DISCOVER_EPS: "utt_discover_eps_v1",
  DISCOVERY_VIEWED_MAP: "utt_discovery_viewed_symbols_v1",

  // New: windowed tools layout/state (not used yet)
  WINDOWS_STATE: "utt_windows_state_v1",
});

// Window ids (stable internal keys)
export const WINDOW_IDS = Object.freeze({
  ARB: "arb",
  TOP_GAINERS: "top_gainers",
  MARKET_CAP: "market_cap",
  VOLUME: "volume",
  LOSERS: "losers",
});

// Default per-window settings.
// Later you can expand this to include dimensions/positions if you want.
export const DEFAULT_WINDOWS_STATE = Object.freeze({
  [WINDOW_IDS.ARB]: { open: false, refresh_enabled: true, refresh_seconds: 300 },
  [WINDOW_IDS.TOP_GAINERS]: { open: false, refresh_enabled: true, refresh_seconds: 300 },
  [WINDOW_IDS.MARKET_CAP]: { open: false, refresh_enabled: true, refresh_seconds: 300 },
  [WINDOW_IDS.VOLUME]: { open: false, refresh_enabled: true, refresh_seconds: 300 },
  [WINDOW_IDS.LOSERS]: { open: false, refresh_enabled: true, refresh_seconds: 300 },
});

// Safety clamp used by UI controls
export function clampInt(n, min, max) {
  const x = Math.trunc(Number(n));
  if (!Number.isFinite(x)) return min;
  if (x < min) return min;
  if (x > max) return max;
  return x;
}
