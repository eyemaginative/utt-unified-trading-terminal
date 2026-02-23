// frontend/src/utils/theme.js

// Theme candidates (localStorage keys written by TerminalTablesWidget or legacy variants)
export const UTT_THEME_CANDIDATES = [
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

export function canonThemeStr(s) {
  return String(s || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "");
}

// Maps the Tables palette names (free-form) to our stable header preset keys above.
export function mapThemeName(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";

  const lc = s.toLowerCase();
  const cc = canonThemeStr(s);

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

  if (cc.includes("midnight") && cc.includes("blue")) return "Midnight Blue";
  if (cc.includes("graphite")) return "Graphite";
  if (cc.includes("oled") || cc.includes("trueblack") || cc.includes("pitchblack")) return "OLED";
  if (cc.includes("dark")) return "Dark";

  return "";
}

// Public normalizer used by runtime updates; defaults to Dark for safety.
export function normalizeThemeName(raw) {
  return mapThemeName(raw) || "Dark";
}

export function tryParseThemeValue(raw) {
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

export function detectThemeNameFromLocalStorage() {
  try {
    for (const k of UTT_THEME_CANDIDATES) {
      const v = localStorage.getItem(k);
      if (v === null || v === undefined) continue;

      const parsed = tryParseThemeValue(v);
      const mapped = mapThemeName(parsed);
      if (mapped) return mapped;
    }

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

export function applyHeaderThemeVars(headerEl, presetKey) {
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
export function applyShellThemeVars(appEl, presetKey) {
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

export function extractThemeNameFromBusPayload(payload) {
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
