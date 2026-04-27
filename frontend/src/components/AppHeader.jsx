// frontend/src/components/AppHeader.jsx
import { useEffect, useMemo, useRef, useState } from "react";
import ArbChip from "../ArbChip";
import TopGainersWindow from "../features/scanners/TopGainersWindow";
import uttBanner from "../assets/utt-banner.jpg";
import { sharedFetchJSON } from "../lib/sharedFetch";
import QRCodeLib from "qrcode";

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

const LS_THEME_KEY = "utt_tables_theme_v1";
const LS_THEME_CUSTOM_KEY = "utt_tables_theme_custom_v1";

// Header banner (user-uploaded)
const LS_BANNER_KEY = "utt_header_banner_v1";
const BANNER_MAX_BYTES = 2 * 1024 * 1024; // 2MB
const BANNER_RECOMMENDED_W = 1920;
const BANNER_RECOMMENDED_H = 120; // compact default height (px)
const BANNER_EDIT_H = 200; // expanded height when Fit controls are open

// Donate (read-only config; only "hide addresses" is user-local)
const LS_DONATE_HIDE_ADDRS_KEY = "utt_donate_hide_addrs_v1";

// Header media player (user-local)
const LS_MEDIA_URL_KEY = "utt_header_media_url_v1";
const LS_MEDIA_VOL_KEY = "utt_header_media_vol_v1";

// Optional auth UI (local-only unless wired to backend)
const LS_AUTH_TOKEN_KEY = "utt_auth_token_v1";
const LS_AUTH_USER_KEY = "utt_auth_user_v1";

// Popover positioning
const POP_MARGIN = 8;

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

const textInputStyle = { width: '100%' };

function isHexColor(s) {
  return /^#[0-9a-fA-F]{6}$/.test(String(s || "").trim());
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

// Format epoch-seconds (UTC) into local date/time string for UI display.
const fmtEpochSec = (sec) => {
  if (sec === null || sec === undefined) return "";
  const n = Number(sec);
  if (!Number.isFinite(n) || n <= 0) return String(sec);
  const d = new Date(n * 1000);
  if (Number.isNaN(d.getTime())) return String(sec);
  return d.toLocaleString();
};


// -----------------------------
// Local-only QR (no deps)
//
// We embed a tiny QR encoder (Nayuki's qrcodegen, trimmed) to render otpauth://...
// as an inline SVG (no external QR services and no new npm deps).
//
// Notes:
// - Version selection is automatic.
// - Error correction: Low (L) is enough for otpauth URLs and keeps QR smaller.
// - Output: React <svg> with a quiet zone.
// -----------------------------

function qrSvgFromText(text, sizePx = 150) {
  const s = String(text || "").trim();
  if (!s) return null;
  try {
    const qr = QRCode.encodeText(s, QRCode.Ecc.L);
    const border = 2;
    const n = qr.size;
    const dim = n + border * 2;
    const scale = Math.max(1, Math.floor(sizePx / dim));
    const w = dim * scale;
    const h = dim * scale;
    const rects = [];
    for (let y = 0; y < n; y++) {
      for (let x = 0; x < n; x++) {
        if (qr.getModule(x, y)) {
          rects.push(
            <rect
              key={`${x}-${y}`}
              x={(x + border) * scale}
              y={(y + border) * scale}
              width={scale}
              height={scale}
            />
          );
        }
      }
    }
    return (
      <svg
        width={w}
        height={h}
        viewBox={`0 0 ${w} ${h}`}
        role="img"
        aria-label="QR code"
        style={{
          display: "block",
          borderRadius: 10,
          background: "#fff",
          padding: 6,
          boxShadow: "0 0 0 1px rgba(255,255,255,0.12) inset",
        }}
      >
        <rect x="0" y="0" width={w} height={h} fill="#fff" />
        <g fill="#000">{rects}</g>
      </svg>
    );
  } catch (e) {
    return null;
  }
}

// Minimal qrcodegen (https://www.nayuki.io/page/qr-code-generator-library)
// MIT licensed. Trimmed to what's needed for encodeText() + SVG module access.
class QRCode {
  static Ecc = {
    L: { ordinal: 0, formatBits: 1 },
    M: { ordinal: 1, formatBits: 0 },
    Q: { ordinal: 2, formatBits: 3 },
    H: { ordinal: 3, formatBits: 2 },
  };

  static encodeText(text, ecl) {
    const segs = QRSegment.makeSegments(text);
    return QRCode.encodeSegments(segs, ecl);
  }

  static encodeSegments(segs, ecl) {
    // Auto version range.
    let minVer = 1;
    let maxVer = 40;
    let mask = -1;
    for (let ver = minVer; ver <= maxVer; ver++) {
      const dataUsedBits = QRSegment.getTotalBits(segs, ver);
      if (dataUsedBits === null) continue;
      const dataCapBits = QRCode.getNumDataCodewords(ver, ecl) * 8;
      if (dataUsedBits <= dataCapBits) {
        return new QRCode(ver, ecl, QRCode.encodeSegmentsToCodewords(segs, ver, dataCapBits), mask);
      }
    }
    throw new Error("Data too long");
  }

  static encodeSegmentsToCodewords(segs, ver, dataCapBits) {
    const bb = [];
    const appendBits = (val, len) => {
      for (let i = len - 1; i >= 0; i--) bb.push((val >>> i) & 1);
    };
    for (const seg of segs) {
      appendBits(seg.mode.modeBits, 4);
      appendBits(seg.numChars, seg.mode.numCharCountBits(ver));
      for (const b of seg.data) bb.push(b);
    }
    // terminator
    const terminator = Math.min(4, dataCapBits - bb.length);
    appendBits(0, terminator);
    while (bb.length % 8 !== 0) bb.push(0);
    // pad bytes
    const pad0 = 0xec;
    const pad1 = 0x11;
    const out = [];
    for (let i = 0; i < bb.length; i += 8) {
      let v = 0;
      for (let j = 0; j < 8; j++) v = (v << 1) | bb[i + j];
      out.push(v);
    }
    let pad = 0;
    while (out.length * 8 < dataCapBits) {
      out.push(pad % 2 === 0 ? pad0 : pad1);
      pad++;
    }
    return out;
  }

  constructor(version, ecl, dataCodewords, mask) {
    this.version = version;
    this.errorCorrectionLevel = ecl;
    this.mask = mask;
    this.size = version * 4 + 17;
    this.modules = Array.from({ length: this.size }, () => Array(this.size).fill(false));
    this.isFunction = Array.from({ length: this.size }, () => Array(this.size).fill(false));
    this.drawFunctionPatterns();
    const allCodewords = this.addEccAndInterleave(dataCodewords);
    this.drawCodewords(allCodewords);
    this.applyBestMask();
    this.drawFormatBits();
  }

  getModule(x, y) {
    return this.modules[y][x];
  }

  // ---- internals (trimmed) ----

  static getNumDataCodewords(ver, ecl) {
    return Math.floor((QRCode.getNumRawDataModules(ver) / 8) - QRCode.getNumEccCodewords(ver, ecl));
  }

  static getNumRawDataModules(ver) {
    const result = (16 * ver + 128) * ver + 64;
    if (ver >= 2) {
      const numAlign = Math.floor(ver / 7) + 2;
      const numAlignModules = (numAlign - 1) * (numAlign - 1) * 25;
      const numTimingModules = (numAlign * 2 - 1) * 10;
      return result - numAlignModules - numTimingModules;
    }
    return result - 192;
  }

  static getNumEccCodewords(ver, ecl) {
    return QRCode.ECC_CODEWORDS_PER_BLOCK[ecl.ordinal][ver] * QRCode.NUM_ERROR_CORRECTION_BLOCKS[ecl.ordinal][ver];
  }

  drawFunctionPatterns() {
    // Finder patterns + separators
    const drawFinder = (x, y) => {
      for (let dy = -1; dy <= 7; dy++) {
        for (let dx = -1; dx <= 7; dx++) {
          const xx = x + dx;
          const yy = y + dy;
          if (0 <= xx && xx < this.size && 0 <= yy && yy < this.size) {
            const on = (0 <= dx && dx <= 6 && (dy === 0 || dy === 6)) || (0 <= dy && dy <= 6 && (dx === 0 || dx === 6)) || (2 <= dx && dx <= 4 && 2 <= dy && dy <= 4);
            this.modules[yy][xx] = on;
            this.isFunction[yy][xx] = true;
          }
        }
      }
    };
    drawFinder(0, 0);
    drawFinder(this.size - 7, 0);
    drawFinder(0, this.size - 7);

    // Timing patterns
    for (let i = 0; i < this.size; i++) {
      this.setFunctionModule(6, i, i % 2 === 0);
      this.setFunctionModule(i, 6, i % 2 === 0);
    }

    // Alignment patterns
    const alignPatPos = QRCode.getAlignmentPatternPositions(this.version);
    const numAlign = alignPatPos.length;
    for (let i = 0; i < numAlign; i++) {
      for (let j = 0; j < numAlign; j++) {
        if ((i === 0 && j === 0) || (i === 0 && j === numAlign - 1) || (i === numAlign - 1 && j === 0)) continue;
        this.drawAlignmentPattern(alignPatPos[i], alignPatPos[j]);
      }
    }

    // Dark module
    this.setFunctionModule(8, this.size - 8, true);

    // Reserve format info area
    for (let i = 0; i < 9; i++) {
      if (i !== 6) {
        this.isFunction[8][i] = true;
        this.isFunction[i][8] = true;
      }
    }
    for (let i = 0; i < 8; i++) {
      this.isFunction[this.size - 1 - i][8] = true;
      this.isFunction[8][this.size - 1 - i] = true;
    }
    this.isFunction[8][this.size - 8] = true;
  }

  setFunctionModule(x, y, isBlack) {
    this.modules[y][x] = isBlack;
    this.isFunction[y][x] = true;
  }

  static getAlignmentPatternPositions(ver) {
    if (ver === 1) return [];
    const numAlign = Math.floor(ver / 7) + 2;
    const step = numAlign === 2 ? ver * 4 + 10 : Math.ceil((ver * 4 + 10) / (numAlign - 1) / 2) * 2;
    const res = [6];
    for (let pos = ver * 4 + 10; res.length < numAlign; pos -= step) res.splice(1, 0, pos);
    return res;
  }

  drawAlignmentPattern(x, y) {
    for (let dy = -2; dy <= 2; dy++) {
      for (let dx = -2; dx <= 2; dx++) {
        this.setFunctionModule(x + dx, y + dy, Math.max(Math.abs(dx), Math.abs(dy)) !== 1);
      }
    }
  }

  addEccAndInterleave(data) {
    const ver = this.version;
    const ecl = this.errorCorrectionLevel;
    const numBlocks = QRCode.NUM_ERROR_CORRECTION_BLOCKS[ecl.ordinal][ver];
    const blockEccLen = QRCode.ECC_CODEWORDS_PER_BLOCK[ecl.ordinal][ver];
    const rawCodewords = Math.floor(QRCode.getNumRawDataModules(ver) / 8);
    const numShortBlocks = numBlocks - (rawCodewords % numBlocks);
    const shortBlockLen = Math.floor(rawCodewords / numBlocks);
    const blocks = [];
    let k = 0;
    for (let i = 0; i < numBlocks; i++) {
      const datLen = shortBlockLen - blockEccLen + (i < numShortBlocks ? 0 : 1);
      const dat = data.slice(k, k + datLen);
      k += datLen;
      const ecc = QRCode.reedSolomonComputeRemainder(dat, QRCode.reedSolomonComputeDivisor(blockEccLen));
      blocks.push(dat.concat(ecc));
    }
    const result = [];
    for (let i = 0; i < blocks[0].length; i++) {
      for (let j = 0; j < blocks.length; j++) {
        if (i !== shortBlockLen - blockEccLen || j >= numShortBlocks) result.push(blocks[j][i]);
      }
    }
    return result;
  }

  static reedSolomonComputeDivisor(degree) {
    let res = [1];
    for (let i = 0; i < degree; i++) {
      res = QRCode.reedSolomonMultiply(res, [1, QRCode.reedSolomonExp(i)]);
    }
    return res;
  }

  static reedSolomonComputeRemainder(data, divisor) {
    let res = Array(divisor.length).fill(0);
    for (const b of data) {
      const factor = b ^ res.shift();
      res.push(0);
      for (let i = 0; i < res.length; i++) res[i] ^= QRCode.reedSolomonMultiplyPoly(divisor[i], factor);
    }
    return res.slice(0, res.length - 1);
  }

  static reedSolomonMultiplyPoly(x, y) {
    if (x === 0 || y === 0) return 0;
    return QRCode.reedSolomonLogExp[(QRCode.reedSolomonExpLog[x] + QRCode.reedSolomonExpLog[y]) % 255];
  }

  static reedSolomonMultiply(p, q) {
    const res = Array(p.length + q.length - 1).fill(0);
    for (let i = 0; i < p.length; i++) {
      for (let j = 0; j < q.length; j++) {
        res[i + j] ^= QRCode.reedSolomonMultiplyPoly(p[i], q[j]);
      }
    }
    return res;
  }

  static reedSolomonExp(i) {
    return QRCode.reedSolomonLogExp[i];
  }

  drawCodewords(data) {
    let i = 0;
    for (let right = this.size - 1; right >= 1; right -= 2) {
      if (right === 6) right--;
      for (let vert = 0; vert < this.size; vert++) {
        for (let j = 0; j < 2; j++) {
          const x = right - j;
          const y = ((right + 1) & 2) === 0 ? this.size - 1 - vert : vert;
          if (!this.isFunction[y][x] && i < data.length * 8) {
            const bit = (data[Math.floor(i / 8)] >>> (7 - (i & 7))) & 1;
            this.modules[y][x] = bit !== 0;
            i++;
          }
        }
      }
    }
  }

  applyBestMask() {
    let minPenalty = Infinity;
    let bestMask = 0;
    for (let m = 0; m < 8; m++) {
      this.applyMask(m);
      this.drawFormatBits(m);
      const penalty = this.getPenaltyScore();
      if (penalty < minPenalty) {
        minPenalty = penalty;
        bestMask = m;
      }
      this.applyMask(m); // undo
    }
    this.applyMask(bestMask);
    this.mask = bestMask;
  }

  applyMask(mask) {
    for (let y = 0; y < this.size; y++) {
      for (let x = 0; x < this.size; x++) {
        if (!this.isFunction[y][x]) {
          let invert = false;
          switch (mask) {
            case 0:
              invert = (x + y) % 2 === 0;
              break;
            case 1:
              invert = y % 2 === 0;
              break;
            case 2:
              invert = x % 3 === 0;
              break;
            case 3:
              invert = (x + y) % 3 === 0;
              break;
            case 4:
              invert = (Math.floor(x / 3) + Math.floor(y / 2)) % 2 === 0;
              break;
            case 5:
              invert = ((x * y) % 2 + (x * y) % 3) === 0;
              break;
            case 6:
              invert = (((x * y) % 2 + (x * y) % 3) % 2) === 0;
              break;
            case 7:
              invert = (((x + y) % 2 + (x * y) % 3) % 2) === 0;
              break;
            default:
              break;
          }
          if (invert) this.modules[y][x] = !this.modules[y][x];
        }
      }
    }
  }

  drawFormatBits(mask = this.mask) {
    const data = (this.errorCorrectionLevel.formatBits << 3) | mask;
    let rem = data;
    for (let i = 0; i < 10; i++) rem = (rem << 1) ^ (((rem >>> 9) & 1) * 0x537);
    const bits = ((data << 10) | rem) ^ 0x5412;
    for (let i = 0; i <= 5; i++) this.setFunctionModule(8, i, ((bits >>> i) & 1) !== 0);
    this.setFunctionModule(8, 7, ((bits >>> 6) & 1) !== 0);
    this.setFunctionModule(8, 8, ((bits >>> 7) & 1) !== 0);
    this.setFunctionModule(7, 8, ((bits >>> 8) & 1) !== 0);
    for (let i = 9; i < 15; i++) this.setFunctionModule(14 - i, 8, ((bits >>> i) & 1) !== 0);
    for (let i = 0; i < 8; i++) this.setFunctionModule(this.size - 1 - i, 8, ((bits >>> i) & 1) !== 0);
    for (let i = 8; i < 15; i++) this.setFunctionModule(8, this.size - 15 + i, ((bits >>> i) & 1) !== 0);
    this.setFunctionModule(8, this.size - 8, true);
  }

  getPenaltyScore() {
    const n = this.size;
    let result = 0;
    // Adjacent modules in row/column in same color
    for (let y = 0; y < n; y++) {
      let runColor = false;
      let runLen = 0;
      for (let x = 0; x < n; x++) {
        const color = this.modules[y][x];
        if (x === 0 || color !== runColor) {
          if (runLen >= 5) result += 3 + (runLen - 5);
          runColor = color;
          runLen = 1;
        } else {
          runLen++;
        }
      }
      if (runLen >= 5) result += 3 + (runLen - 5);
    }
    for (let x = 0; x < n; x++) {
      let runColor = false;
      let runLen = 0;
      for (let y = 0; y < n; y++) {
        const color = this.modules[y][x];
        if (y === 0 || color !== runColor) {
          if (runLen >= 5) result += 3 + (runLen - 5);
          runColor = color;
          runLen = 1;
        } else {
          runLen++;
        }
      }
      if (runLen >= 5) result += 3 + (runLen - 5);
    }
    // 2x2 blocks
    for (let y = 0; y < n - 1; y++) {
      for (let x = 0; x < n - 1; x++) {
        const c = this.modules[y][x];
        if (c === this.modules[y][x + 1] && c === this.modules[y + 1][x] && c === this.modules[y + 1][x + 1]) result += 3;
      }
    }
    return result;
  }
}

// Precompute GF(2^8) tables for RS.
QRCode.reedSolomonLogExp = (() => {
  const res = Array(255);
  let x = 1;
  for (let i = 0; i < 255; i++) {
    res[i] = x;
    x <<= 1;
    if (x & 0x100) x ^= 0x11d;
  }
  return res;
})();
QRCode.reedSolomonExpLog = (() => {
  const res = Array(256).fill(0);
  for (let i = 0; i < 255; i++) res[QRCode.reedSolomonLogExp[i]] = i;
  return res;
})();

// Tables (index by [ecl.ordinal][version])
QRCode.ECC_CODEWORDS_PER_BLOCK = [
  [0, 7, 10, 15, 20, 26, 18, 20, 24, 30, 18, 20, 24, 26, 30, 22, 24, 28, 30, 28, 28, 28, 28, 30, 30, 26, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30], // L
  [0, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26, 30, 22, 22, 24, 24, 28, 28, 26, 26, 26, 26, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28], // M
  [0, 13, 22, 18, 26, 18, 24, 18, 22, 20, 24, 28, 26, 24, 20, 30, 24, 28, 28, 26, 30, 28, 30, 30, 30, 30, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30], // Q
  [0, 17, 28, 22, 16, 22, 28, 26, 26, 24, 28, 24, 28, 22, 24, 24, 30, 28, 28, 26, 28, 30, 24, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30], // H
];
QRCode.NUM_ERROR_CORRECTION_BLOCKS = [
  [0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 4, 4, 4, 4, 4, 6, 6, 6, 6, 7, 8, 8, 9, 9, 10, 12, 12, 12, 13, 14, 15, 16, 17, 18, 19, 19, 20, 21, 22, 24, 25],
  [0, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5, 5, 8, 9, 9, 10, 10, 11, 13, 14, 16, 17, 17, 18, 20, 21, 23, 25, 26, 28, 29, 31, 33, 35, 37, 38, 40, 43, 45, 47, 49],
  [0, 1, 1, 2, 2, 4, 4, 6, 6, 8, 8, 8, 10, 12, 16, 12, 17, 16, 18, 21, 20, 23, 23, 25, 27, 29, 34, 34, 35, 38, 40, 43, 45, 48, 51, 53, 56, 59, 62, 65, 68],
  [0, 1, 1, 2, 4, 4, 4, 5, 6, 8, 8, 11, 11, 16, 16, 18, 16, 19, 21, 25, 25, 25, 34, 30, 32, 35, 37, 40, 42, 45, 48, 51, 54, 57, 60, 63, 66, 70, 74, 77, 81],
];

class QRSegment {
  static Mode = {
    BYTE: {
      modeBits: 0x4,
      numCharCountBits: (ver) => (ver <= 9 ? 8 : ver <= 26 ? 16 : 16),
    },
  };

  constructor(mode, numChars, dataBits) {
    this.mode = mode;
    this.numChars = numChars;
    this.data = dataBits;
  }

  static makeSegments(text) {
    return [QRSegment.makeBytes(QRSegment.toUtf8Bytes(text))];
  }

  static makeBytes(data) {
    const bits = [];
    for (const b of data) {
      for (let i = 7; i >= 0; i--) bits.push((b >>> i) & 1);
    }
    return new QRSegment(QRSegment.Mode.BYTE, data.length, bits);
  }

  static toUtf8Bytes(str) {
    // TextEncoder is available in modern browsers; fallback keeps it local.
    if (typeof TextEncoder !== "undefined") return Array.from(new TextEncoder().encode(str));
    const utf8 = unescape(encodeURIComponent(str));
    return Array.from(utf8).map((c) => c.charCodeAt(0));
  }

  static getTotalBits(segs, ver) {
    let result = 0;
    for (const seg of segs) {
      const ccbits = seg.mode.numCharCountBits(ver);
      if (seg.numChars >= (1 << ccbits)) return null;
      result += 4 + ccbits + seg.data.length;
    }
    return result;
  }
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
      posX: Number.isFinite(Number(v.posX)) ? Number(v.posX) : 50,
      posY: Number.isFinite(Number(v.posY)) ? Number(v.posY) : 50,
      fitMode: (v.fitMode === "contain" || v.fitMode === "cover") ? v.fitMode : "cover",
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

function readMediaUrlFromStorage() {
  try {
    return String(localStorage.getItem(LS_MEDIA_URL_KEY) || "").trim();
  } catch {
    return "";
  }
}

function writeMediaUrlToStorage(v) {
  try {
    const s = String(v || "").trim();
    if (s) localStorage.setItem(LS_MEDIA_URL_KEY, s);
    else localStorage.removeItem(LS_MEDIA_URL_KEY);
  } catch {
    // ignore
  }
}

function readMediaVolFromStorage() {
  try {
    const n = Number(localStorage.getItem(LS_MEDIA_VOL_KEY));
    return Number.isFinite(n) ? Math.max(0, Math.min(1, n)) : 0.85;
  } catch {
    return 0.85;
  }
}

function writeMediaVolToStorage(v) {
  try {
    const n = Number(v);
    localStorage.setItem(LS_MEDIA_VOL_KEY, String(Number.isFinite(n) ? Math.max(0, Math.min(1, n)) : 0.85));
  } catch {
    // ignore
  }
}


function readAuthTokenFromStorage() {
  try {
    const raw = localStorage.getItem(LS_AUTH_TOKEN_KEY);
    const t = String(raw || "").trim();
    return t ? t : "";
  } catch {
    return "";
  }
}

function readAuthUserFromStorage() {
  try {
    const raw = localStorage.getItem(LS_AUTH_USER_KEY);
    const u = String(raw || "").trim();
    return u ? u : "";
  } catch {
    return "";
  }
}

function writeAuthToStorage(token, userLabel) {
  try {
    if (token) localStorage.setItem(LS_AUTH_TOKEN_KEY, String(token));
    else localStorage.removeItem(LS_AUTH_TOKEN_KEY);
  } catch {
    // ignore
  }
  try {
    if (userLabel) localStorage.setItem(LS_AUTH_USER_KEY, String(userLabel));
    else localStorage.removeItem(LS_AUTH_USER_KEY);
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


function LockIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" style={{ display: "block" }}>
      <path
        d="M7.5 11V8.5A4.5 4.5 0 0 1 12 4a4.5 4.5 0 0 1 4.5 4.5V11"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M6.5 11h11A2 2 0 0 1 19.5 13v5.5A2 2 0 0 1 17.5 20.5h-11A2 2 0 0 1 4.5 18.5V13A2 2 0 0 1 6.5 11Z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function UserIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" style={{ display: "block" }}>
      <path d="M12 12.5a4 4 0 1 0 0-8 4 4 0 0 0 0 8Z" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
      <path d="M4.5 20a7.5 7.5 0 0 1 15 0" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
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


function BuyIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" style={{ display: "block" }}>
      <path d="M6 7h15l-2 8H8L6 7Z" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
      <path d="M6 7 5 4H2" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M9 20a1 1 0 1 0 0-2 1 1 0 0 0 0 2Z" stroke="currentColor" strokeWidth="1.8" />
      <path d="M18 20a1 1 0 1 0 0-2 1 1 0 0 0 0 2Z" stroke="currentColor" strokeWidth="1.8" />
    </svg>
  );
}


function MediaIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" aria-hidden="true" focusable="false" style={{ display: "block" }}>
      <path d="M4 9a2 2 0 0 1 2-2h3.2L13 4v16l-3.8-3H6a2 2 0 0 1-2-2V9Z" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
      <path d="M17 9.5a4.5 4.5 0 0 1 0 5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      <path d="M19.5 7a8 8 0 0 1 0 10" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

function BuyUtttIcon({ size = 16 }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
      focusable="false"
      style={{ display: "block" }}
    >
      <defs>
        <linearGradient id="uttGradBuyBtn" x1="0" y1="0" x2="24" y2="0">
          <stop offset="0" stopColor="#4ef0ff" />
          <stop offset="0.45" stopColor="#5f7cff" />
          <stop offset="1" stopColor="#b45cff" />
        </linearGradient>
      </defs>

      {/* horns */}
      <path
        d="M6 7c1.8 0 3.2 1 4 2.2C10.8 8 12.2 7 14 7c1.6 0 3 .7 4 1.8"
        stroke="url(#uttGradBuyBtn)"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.95"
      />

      {/* head/body hint */}
      <path
        d="M9.5 20c-1.8-1.3-3-3.3-3-5.7 0-2.9 1.8-5.2 4.4-6
           0.7 1.5 1.8 2.2 3.1 2.2s2.4-0.7 3.1-2.2
           2.4 2.1 2.4 5c0 2.4-1.2 4.4-3 5.7"
        stroke="url(#uttGradBuyBtn)"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.9"
      />

      {/* upward arrow */}
      <path
        d="M13.5 17.5l4-4m0 0h-3m3 0v3"
        stroke="url(#uttGradBuyBtn)"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}


function AirdropIcon({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <defs>
        <linearGradient id="uttGradAir" x1="0" y1="0" x2="24" y2="0">
          <stop offset="0" stopColor="#4ef0ff" />
          <stop offset="0.45" stopColor="#5f7cff" />
          <stop offset="1" stopColor="#b45cff" />
        </linearGradient>
      </defs>
      <path d="M12 3c3.9 0 7 3.1 7 7v1" stroke="url(#uttGradAir)" strokeWidth="2" strokeLinecap="round" />
      <path d="M5 11v-1c0-3.9 3.1-7 7-7" stroke="url(#uttGradAir)" strokeWidth="2" strokeLinecap="round" />
      <path d="M7 11c1.6 0 2.6 1 3 2.2.4-1.2 1.4-2.2 3-2.2s2.6 1 3 2.2c.4-1.2 1.4-2.2 3-2.2" stroke="url(#uttGradAir)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" opacity="0.9"/>
      <path d="M9 14v4" stroke="url(#uttGradAir)" strokeWidth="2" strokeLinecap="round" />
      <path d="M15 14v4" stroke="url(#uttGradAir)" strokeWidth="2" strokeLinecap="round" />
      <path d="M9 18c0 1.7 1.3 3 3 3s3-1.3 3-3" stroke="url(#uttGradAir)" strokeWidth="2" strokeLinecap="round" />
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
  venuesRaw,
  venueOverrides,
  setVenueOverride,
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
  const [bannerFitOpen, setBannerFitOpen] = useState(false);
  const [bannerAutoFitPreviewOpen, setBannerAutoFitPreviewOpen] = useState(false);
  const [bannerAutoFitDraft, setBannerAutoFitDraft] = useState(null);


  // Banner display height stays stable so the header does not jump when editing fit/crop.
  const bannerDisplayHeight = BANNER_RECOMMENDED_H;
  const bannerFrameRadius = 14;

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
  // Media player
  const [mediaOpen, setMediaOpen] = useState(false);
  const [mediaBusy, setMediaBusy] = useState(false);
  const [mediaMsg, setMediaMsg] = useState("");
  const [mediaUrlInput, setMediaUrlInput] = useState(() => (typeof window === "undefined" ? "" : readMediaUrlFromStorage()));
  const [mediaSource, setMediaSource] = useState("");
  const [mediaResolvedUrl, setMediaResolvedUrl] = useState("");
  const [mediaNowPlaying, setMediaNowPlaying] = useState("");
  const [mediaIsPlaying, setMediaIsPlaying] = useState(false);
  const [mediaVolume, setMediaVolume] = useState(() => (typeof window === "undefined" ? 0.85 : readMediaVolFromStorage()));
  const [mediaPos, setMediaPos] = useState(null);
  const mediaBtnRef = useRef(null);
  const mediaPopRef = useRef(null);
  const mediaAudioRef = useRef(null);
  const mediaFileRef = useRef(null);
  const mediaObjectUrlRef = useRef("");
  // Profile panel (account / password / 2FA / API keys)
  const [profileOpen, setProfileOpen] = useState(false);
  const profileBtnRef = useRef(null);
  const profilePopRef = useRef(null);
  const [profile2faBusy, setProfile2faBusy] = useState(false);
  const [profile2faMsg, setProfile2faMsg] = useState("");
  const [profile2faSecret, setProfile2faSecret] = useState("");
  const [profile2faOtpAuth, setProfile2faOtpAuth] = useState("");
  const [profile2faShowQr, setProfile2faShowQr] = useState(false);
  const [profile2faQrSvg, setProfile2faQrSvg] = useState("");
  const [profile2faCode, setProfile2faCode] = useState("");
  const [profile2faResetPw, setProfile2faResetPw] = useState("");
  const [profile2faResetCode, setProfile2faResetCode] = useState("");
  const [profilePwMsg, setProfilePwMsg] = useState("");
  const [profilePwBusy, setProfilePwBusy] = useState(false);
  const [profilePwCurrent, setProfilePwCurrent] = useState("");
  const [profilePwNew, setProfilePwNew] = useState("");
  const [profilePwNew2, setProfilePwNew2] = useState("");
  const [profilePwTotp, setProfilePwTotp] = useState("");
  // API key vault UI (write-only; secrets never displayed again)
  const [profileKeysMsg, setProfileKeysMsg] = useState("");
  const [profileKeysBusy, setProfileKeysBusy] = useState(false);
  const [profileKeysItems, setProfileKeysItems] = useState([]);
  const [profileKeyVenue, setProfileKeyVenue] = useState("");
  const [profileKeyLabel, setProfileKeyLabel] = useState("");
  const [profileKeyApiKey, setProfileKeyApiKey] = useState("");
  const [profileKeyApiSecret, setProfileKeyApiSecret] = useState("");
  const [profileKeyPassphrase, setProfileKeyPassphrase] = useState("");
  const [profileKeyTotp, setProfileKeyTotp] = useState("");
  const [profileBackupBusy, setProfileBackupBusy] = useState(false);
  const [profileBackupMsg, setProfileBackupMsg] = useState("");
  const [profileAutoBackupOnLogout, setProfileAutoBackupOnLogout] = useState(true);
  const [profileBackupPrefBusy, setProfileBackupPrefBusy] = useState(false);
  const [profileStayLoggedIn, setProfileStayLoggedIn] = useState(false);
  const [profileSessionPrefBusy, setProfileSessionPrefBusy] = useState(false);
  const [profileSessionMsg, setProfileSessionMsg] = useState("");

  // Auth UI (optional; local-only unless backend is wired)
  const [authOpen, setAuthOpen] = useState(false);
  const [authToken, setAuthToken] = useState(() => (typeof window === "undefined" ? "" : readAuthTokenFromStorage()));
  const [authUser, setAuthUser] = useState(() => (typeof window === "undefined" ? "" : readAuthUserFromStorage()));

  const [authTotpEnabled, setAuthTotpEnabled] = useState(false);
  const [authTotpProvisioned, setAuthTotpProvisioned] = useState(false);
  const [authBusy, setAuthBusy] = useState(false);
  const [authSignupOpen, setAuthSignupOpen] = useState(false);
  const [authBootstrapRequired, setAuthBootstrapRequired] = useState(false);
  const [authMsg, setAuthMsg] = useState("");
  const [authForm, setAuthForm] = useState({ user: "", pass: "", pass2: "", otp: "", want2fa: true });

  useEffect(() => {
    if (authOpen) return;
    const msg = String(authMsg || "").trim();
    if (!msg) return;

    const lower = msg.toLowerCase();
    const shouldAutoHide =
      lower === "signed in." ||
      lower === "logged out." ||
      lower === "account created / signed in." ||
      lower === "account created / signed in. finish 2fa in profile when ready.";
    if (!shouldAutoHide) return;

    const t = window.setTimeout(() => setAuthMsg(""), 2500);
    return () => window.clearTimeout(t);
  }, [authMsg, authOpen]);
  const clearAuthSensitive = () => setAuthForm((s) => ({ ...s, pass: "", pass2: "", otp: "" }));

  const refreshAuthBootstrapStatus = async () => {
    try {
      const base = tgTrimApiBase(API_BASE);
      const url = `${base}/api/auth/bootstrap_status`;
      const r = await fetch(url, { method: "GET" });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok) return;
      setAuthSignupOpen(Boolean(data?.signup_open));
      setAuthBootstrapRequired(Boolean(data?.first_user_bootstrap_required));
    } catch {
      setAuthSignupOpen(false);
      setAuthBootstrapRequired(false);
    }
  };
  const submitAuth = async () => {
    setAuthMsg("");
    const u = String(authForm.user || "").trim();
    const p = String(authForm.pass || "");
    const p2 = String(authForm.pass2 || "");
    const otp = String(authForm.otp || "").trim();
    if (!u || !p) {
      setAuthMsg("Missing username or password.");
      return;
    }

    const base = tgTrimApiBase(API_BASE);
    setAuthBusy(true);
    try {
      const didSignup = !!(authSignupOpen || authBootstrapRequired);

      if (didSignup) {
        if (!p2) {
          setAuthMsg("Confirm password is required.");
          return;
        }
        if (p !== p2) {
          setAuthMsg("Passwords do not match.");
          return;
        }

        const signupUrl = `${base}/api/auth/signup`;
        const rs = await fetch(signupUrl, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ username: u, password: p, want_2fa: Boolean(authForm.want2fa) }),
        });
        const cts = String(rs.headers.get("content-type") || "");
        const signupData = cts.includes("application/json") ? await rs.json() : { detail: await rs.text() };
        if (!rs.ok) {
          const msg = typeof signupData?.detail === "string" ? signupData.detail : JSON.stringify(signupData?.detail || signupData);
          setAuthMsg(msg || `Create account failed (${rs.status})`);
          await refreshAuthBootstrapStatus();
          return;
        }
      }

      const url = `${base}/api/auth/login`;
      const r = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username: u, password: p, totp: otp || null, remember_me: !!profileStayLoggedIn }),
      });

      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok) {
        const msg = typeof data?.detail === "string" ? data.detail : JSON.stringify(data?.detail || data);
        setAuthMsg(msg || `Login failed (${r.status})`);
        return;
      }

      const token = String(data?.token || data?.access_token || "").trim();
      const label = String(data?.user || data?.username || u).trim();
      if (!token) {
        setAuthMsg("Login endpoint returned no token (expected token/access_token).");
        return;
      }

      writeAuthToStorage(token, label);
      setAuthToken(token);
      setAuthUser(label);
      setAuthSignupOpen(false);
      setAuthBootstrapRequired(false);
      clearAuthSensitive();
      setAuthOpen(false);
      setAuthMsg(
        didSignup
          ? (authForm.want2fa
              ? "Account created / signed in. Finish 2FA in Profile when ready."
              : "Account created / signed in.")
          : "Signed in."
      );
    } catch (e) {
      setAuthMsg("Auth not configured yet (missing auth endpoints) or network error.");
    } finally {
      setAuthBusy(false);
    }
  };

  const callBackupDatabase = async ({ quiet = false } = {}) => {
    const base = tgTrimApiBase(API_BASE);
    const tok = String(authToken || "").trim();
    if (!tok) {
      if (!quiet) setProfileBackupMsg("Login required.");
      return { ok: false, skipped: true, reason: "no_token" };
    }

    setProfileBackupBusy(true);
    if (!quiet) setProfileBackupMsg("");
    try {
      const r = await fetch(`${base}/api/auth/backup_db`, {
        method: "POST",
        headers: { Authorization: `Bearer ${tok}` },
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok || data?.ok === false) {
        const msg = typeof data?.detail === "string" ? data.detail : JSON.stringify(data?.detail || data);
        if (!quiet) setProfileBackupMsg(msg || `Backup failed (${r.status}).`);
        return { ok: false, error: msg || `Backup failed (${r.status}).` };
      }
      const detail = [data?.filename, data?.size_bytes != null ? `${Number(data.size_bytes).toLocaleString()} bytes` : "", data?.backup_path].filter(Boolean).join(" • ");
      if (!quiet) setProfileBackupMsg(detail ? `Backup created: ${detail}` : "Backup created.");
      return data;
    } catch (e) {
      const msg = String(e?.message || e || "Backup failed.");
      if (!quiet) setProfileBackupMsg(msg);
      return { ok: false, error: msg };
    } finally {
      setProfileBackupBusy(false);
    }
  };

  const logoutWithBackup = async () => {
    const base = tgTrimApiBase(API_BASE);
    const tok = String(authToken || "").trim();
    let backupNotice = "";

    if (tok) {
      if (profileAutoBackupOnLogout) {
        const backup = await callBackupDatabase({ quiet: true });
        if (backup?.ok) {
          backupNotice = backup?.filename ? `Backup created: ${backup.filename}` : "Backup created before logout.";
        } else if (backup && !backup.skipped) {
          backupNotice = `Backup before logout failed: ${String(backup.error || "unknown error")}`;
        }
      }

      try {
        await fetch(`${base}/api/auth/logout`, {
          method: "POST",
          headers: { Authorization: `Bearer ${tok}` },
        });
      } catch {
        // best-effort only; logout is client-side token clearing
      }
    }

    writeAuthToStorage("", "");
    setAuthToken("");
    setAuthUser("");
    clearAuthSensitive();
    setAuthTotpEnabled(false);
    setAuthTotpProvisioned(false);
    setAuthOpen(false);
    setProfileOpen(false);
    setAuthMsg(backupNotice || "Logged out.");
    setProfileBackupMsg(backupNotice);
  };

  const loadBackupPrefs = async () => {
    const base = tgTrimApiBase(API_BASE);
    const tok = String(authToken || "").trim();
    if (!tok) return;
    try {
      const r = await fetch(`${base}/api/auth/backup_prefs`, {
        method: "GET",
        headers: { Authorization: `Bearer ${tok}` },
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok || data?.ok === false) return;
      setProfileAutoBackupOnLogout(Boolean(data?.auto_backup_on_logout));
    } catch {
      // ignore
    }
  };

  const saveBackupPrefs = async (enabled) => {
    const base = tgTrimApiBase(API_BASE);
    const tok = String(authToken || "").trim();
    if (!tok) return false;
    setProfileBackupPrefBusy(true);
    try {
      const r = await fetch(`${base}/api/auth/backup_prefs`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${tok}`,
        },
        body: JSON.stringify({ auto_backup_on_logout: Boolean(enabled) }),
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok || data?.ok === false) {
        const msg = typeof data?.detail === "string" ? data.detail : JSON.stringify(data?.detail || data);
        setProfileBackupMsg(msg || `Backup preference save failed (${r.status}).`);
        return false;
      }
      setProfileAutoBackupOnLogout(Boolean(data?.auto_backup_on_logout));
      setProfileBackupMsg(`Auto-backup before logout ${Boolean(data?.auto_backup_on_logout) ? "enabled" : "disabled"}.`);
      return true;
    } catch (e) {
      setProfileBackupMsg(String(e?.message || e || "Backup preference save failed."));
      return false;
    } finally {
      setProfileBackupPrefBusy(false);
    }
  };


  const loadSessionPrefs = async () => {
    const base = tgTrimApiBase(API_BASE);
    const tok = String(authToken || "").trim();
    if (!tok) return;
    try {
      const r = await fetch(`${base}/api/auth/session_prefs`, {
        method: "GET",
        headers: { Authorization: `Bearer ${tok}` },
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok || data?.ok === false) return;
      setProfileStayLoggedIn(Boolean(data?.remember_login));
    } catch {
      // ignore
    }
  };

  const saveSessionPrefs = async (enabled) => {
    const base = tgTrimApiBase(API_BASE);
    const tok = String(authToken || "").trim();
    if (!tok) return false;
    setProfileSessionPrefBusy(true);
    try {
      const r = await fetch(`${base}/api/auth/session_prefs`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${tok}`,
        },
        body: JSON.stringify({ remember_login: Boolean(enabled) }),
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok || data?.ok === false) {
        const msg = typeof data?.detail === "string" ? data.detail : JSON.stringify(data?.detail || data);
        setProfileSessionMsg(msg || `Session preference save failed (${r.status}).`);
        return false;
      }
      setProfileStayLoggedIn(Boolean(data?.remember_login));
      const nextToken = String(data?.token || "").trim();
      const nextUser = String(data?.user || authUser || "").trim();
      if (nextToken) {
        writeAuthToStorage(nextToken, nextUser);
        setAuthToken(nextToken);
        if (nextUser) setAuthUser(nextUser);
      }
      setProfileSessionMsg(`Stay logged in on this device ${Boolean(data?.remember_login) ? "enabled" : "disabled"}.`);
      return true;
    } catch (e) {
      setProfileSessionMsg(String(e?.message || e || "Session preference save failed."));
      return false;
    } finally {
      setProfileSessionPrefBusy(false);
    }
  };


  const refreshAuthFromServer = async (tok, opts = {}) => {
    const token = String(tok || authToken || "").trim();
    if (!token) return;
    const { silent = false, openPromptOnExpire = false } = opts || {};
    try {
      const base = tgTrimApiBase(API_BASE);
      const url = `${base}/api/auth/me`;
      const r = await fetch(url, {
        method: "GET",
        headers: { authorization: `Bearer ${token}` },
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };

      if (!r.ok) {
        if (r.status === 401 || r.status === 403) {
          writeAuthToStorage("", "");
          setAuthToken("");
          setAuthUser("");
          setAuthTotpEnabled(false);
          setAuthTotpProvisioned(false);
          setProfileOpen(false);
          setProfileStayLoggedIn(false);
          if (!silent) {
            setAuthMsg("Session expired. Please log in again.");
            if (openPromptOnExpire) {
              setAuthOpen(true);
              setTimeout(() => placeAuthNearButton(), 0);
            }
          }
        }
        return;
      }

      if (data && data.auth === true) {
        if (Object.prototype.hasOwnProperty.call(data || {}, "totp_enabled")) {
          setAuthTotpEnabled(Boolean(data.totp_enabled));
        }
        if (Object.prototype.hasOwnProperty.call(data || {}, "totp_provisioned")) {
          setAuthTotpProvisioned(Boolean(data.totp_provisioned));
        }
        if (Object.prototype.hasOwnProperty.call(data || {}, "auto_backup_on_logout")) {
          setProfileAutoBackupOnLogout(Boolean(data.auto_backup_on_logout));
        }
        if (Object.prototype.hasOwnProperty.call(data || {}, "remember_login")) {
          setProfileStayLoggedIn(Boolean(data.remember_login));
        }
        const label = String(data.user || data.username || authUser || "local").trim();
        if (label && label !== authUser) setAuthUser(label);
      } else if (data && data.auth === false) {
        writeAuthToStorage("", "");
        setAuthToken("");
        setAuthUser("");
        setAuthTotpEnabled(false);
        setAuthTotpProvisioned(false);
        setProfileOpen(false);
        setProfileStayLoggedIn(false);
        if (!silent) {
          setAuthMsg("Session expired. Please log in again.");
          if (openPromptOnExpire) {
            setAuthOpen(true);
            setTimeout(() => placeAuthNearButton(), 0);
          }
        }
      }
    } catch {
      // ignore network errors; keep local state
    }
  };

  useEffect(() => {
    // reconcile local token with backend identity
    refreshAuthFromServer(undefined, { silent: true, openPromptOnExpire: false });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken]);

  useEffect(() => {
    if (!authOpen || authToken) return;
    refreshAuthBootstrapStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authOpen, authToken]);

  useEffect(() => {
    if (!authToken) return;

    const check = () => refreshAuthFromServer(undefined, { silent: false, openPromptOnExpire: true });
    const onFocus = () => check();
    const onVisible = () => {
      if (document.visibilityState === "visible") check();
    };

    const id = window.setInterval(check, 5 * 60 * 1000);
    window.addEventListener("focus", onFocus);
    document.addEventListener("visibilitychange", onVisible);

    return () => {
      window.clearInterval(id);
      window.removeEventListener("focus", onFocus);
      document.removeEventListener("visibilitychange", onVisible);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken]);
  const [buyUtttMsg, setBuyUtttMsg] = useState("");
  const [airdropMsg, setAirdropMsg] = useState("");
  const [airdropOpen, setAirdropOpen] = useState(false);
  const [airdropBusy, setAirdropBusy] = useState(false);
  const [airdropRegisterBusy, setAirdropRegisterBusy] = useState(false);
  const [airdropStatus, setAirdropStatus] = useState(null);
  const [airdropWallet, setAirdropWallet] = useState("");
  const [airdropWalletDraft, setAirdropWalletDraft] = useState("");
  const [airdropErr, setAirdropErr] = useState("");
  const [airdropChallenge, setAirdropChallenge] = useState(null);
  const airdropBtnRef = useRef(null);
  const airdropPopRef = useRef(null);
  const [airdropPos, setAirdropPos] = useState(null);
  // Profile: 2FA wiring (DB mode preferred; shared mode returns provisioning + note)
  const clearProfile2faUi = () => {
    setProfile2faMsg("");
    setProfile2faSecret("");
    setProfile2faOtpAuth("");
    setProfile2faShowQr(false);
    setProfile2faQrSvg("");
    setProfile2faCode("");
    setProfile2faResetPw("");
    setProfile2faResetCode("");

    // Password change UI
    setProfilePwMsg("");
    setProfilePwBusy(false);
    setProfilePwCurrent("");
    setProfilePwNew("");
    setProfilePwNew2("");
    setProfilePwTotp("");

    // API Keys UI
    setProfileKeysMsg("");
    setProfileKeysBusy(false);
    setProfileKeysItems([]);
    setProfileKeyVenue("");
    setProfileKeyLabel("");
    setProfileKeyApiKey("");
    setProfileKeyApiSecret("");
    setProfileKeyPassphrase("");
    setProfileKeyTotp("");
    setProfileBackupBusy(false);
    setProfileBackupPrefBusy(false);
    setProfileBackupMsg("");
    setProfileSessionMsg("");
  };
  // When opening Profile, refresh auth state (so totp_enabled is current) and ensure we never
  // re-display a previously generated secret/QR once 2FA is enabled.
  useEffect(() => {
    if (!profileOpen) return;
    // Refresh /api/auth/me (updates authTotpEnabled/authTotpProvisioned)
    refreshAuthFromServer(authToken, { silent: true, openPromptOnExpire: false });
    loadBackupPrefs();
    loadSessionPrefs();
    // Load API key metadata (never secrets)
    callProfileApiKeysList();
  }, [profileOpen]);

  useEffect(() => {
    if (authTotpEnabled) {
      // If 2FA is enabled, do not keep provisioning material in UI state.
      clearProfile2faUi();
    }
  }, [authTotpEnabled]);


  // Generate QR SVG (local-only) when shown. Uses `qrcode` library for reliable scanning in Google Authenticator.
  useEffect(() => {
    let cancelled = false;
    const s = String(profile2faOtpAuth || "").trim();
    if (!profile2faShowQr || !s) {
      setProfile2faQrSvg("");
      return () => {
        cancelled = true;
      };
    }

    (async () => {
      try {
        const svg = await QRCodeLib.toString(s, {
          type: "svg",
          errorCorrectionLevel: "M",
          margin: 4,
          width: 260,
        });
        if (!cancelled) setProfile2faQrSvg(String(svg || ""));
      } catch (_e) {
        if (!cancelled) setProfile2faQrSvg("");
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [profile2faShowQr, profile2faOtpAuth]);


  const callProfile2fa = async (op, bodyObj) => {
    const base = tgTrimApiBase(API_BASE);
    const url = `${base}/api/auth/2fa/${op}`;
    const tok = String(authToken || "").trim();
    if (!tok) {
      setProfile2faMsg("Login required.");
      return;
    }
    setProfile2faBusy(true);
    setProfile2faMsg("");
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${tok}`,
        },
        body: JSON.stringify(bodyObj || {}),
      });

      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok) {
        const msg = typeof data?.detail === "string" ? data.detail : JSON.stringify(data?.detail || data);
        setProfile2faMsg(msg || `2FA ${op} failed (${r.status})`);
        return;
      }

      const prov = data?.totp_provisioning || data?.provisioning || data;
      const secret = String(prov?.secret || "").trim();
      const otpauth = String(prov?.otpauth_url || prov?.otpauth || "").trim();
      // Setup/reset always yields provisioning material and implies 2FA is currently disabled until enabled.
      setAuthTotpEnabled(false);
      setAuthTotpProvisioned(true);
      if (secret) setProfile2faSecret(secret);
      if (otpauth) setProfile2faOtpAuth(otpauth);
      setProfile2faMsg(op === "reset" ? "2FA secret reset. Scan/copy and then enable with a code." : "2FA secret generated. Scan/copy and then enable with a code.");
      setProfile2faCode("");
    setProfile2faResetPw("");
    setProfile2faResetCode("");

    // Password change UI
    setProfilePwMsg("");
    setProfilePwBusy(false);
    setProfilePwCurrent("");
    setProfilePwNew("");
    setProfilePwNew2("");
    setProfilePwTotp("");
    } catch (e) {
      setProfile2faMsg(String(e?.message || e || "2FA request failed."));
    } finally {
      setProfile2faBusy(false);
    }
  };
  const callProfilePasswordChange = async () => {
    const base = tgTrimApiBase(API_BASE);
    const url = `${base}/api/auth/password/change`;
    const tok = String(authToken || "").trim();
    if (!tok) {
      setProfilePwMsg("Login required.");
      return;
    }

    const cur = String(profilePwCurrent || "");
    const n1 = String(profilePwNew || "");
    const n2 = String(profilePwNew2 || "");
    const totp = String(profilePwTotp || "").trim();

    if (!cur.trim()) {
      setProfilePwMsg("Current password is required.");
      return;
    }
    if (!n1.trim()) {
      setProfilePwMsg("New password is required.");
      return;
    }
    if (n1 !== n2) {
      setProfilePwMsg("New passwords do not match.");
      return;
    }
    if (authTotpEnabled && totp.length < 6) {
      setProfilePwMsg("2FA code is required to change password (when 2FA is enabled).");
      return;
    }

    setProfilePwBusy(true);
    setProfilePwMsg("");
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${tok}`,
        },
        body: JSON.stringify({
          current_password: cur,
          new_password: n1,
          totp: authTotpEnabled ? totp : null,
        }),
      });

      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok || data?.ok === false) {
        const msg = typeof data?.detail === "string" ? data.detail : (data?.message ? String(data.message) : JSON.stringify(data?.detail || data));
        setProfilePwMsg(msg || `Password change failed (${r.status})`);
        return;
      }

      setProfilePwMsg("Password updated.");
      setProfilePwCurrent("");
      setProfilePwNew("");
      setProfilePwNew2("");
      setProfilePwTotp("");
    } catch (e) {
      setProfilePwMsg(String(e?.message || e));
    } finally {
      setProfilePwBusy(false);
    }
  };
  const callProfileApiKeysList = async () => {
    const base = tgTrimApiBase(API_BASE);
    const url = `${base}/api/auth/api_keys`;
    const tok = String(authToken || "").trim();
    if (!tok) {
      setProfileKeysItems([]);
      return;
    }
    setProfileKeysBusy(true);
    setProfileKeysMsg("");
    try {
      const r = await fetch(url, {
        method: "GET",
        headers: { Authorization: `Bearer ${tok}` },
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { ok: false, detail: await r.text() };
      if (!r.ok || !data?.ok) {
        setProfileKeysItems([]);
        setProfileKeysMsg(data?.detail || `API keys load failed (${r.status}).`);
        return;
      }
      setProfileKeysItems(Array.isArray(data?.items) ? data.items : []);
    } catch (e) {
      setProfileKeysItems([]);
      setProfileKeysMsg(`API keys load failed: ${String(e?.message || e)}`);
    } finally {
      setProfileKeysBusy(false);
    }
  };

  const callProfileApiKeysSave = async () => {
    const base = tgTrimApiBase(API_BASE);
    const url = `${base}/api/auth/api_keys`;
    const tok = String(authToken || "").trim();
    if (!tok) {
      setProfileKeysMsg("Login required.");
      return;
    }

    const venue = String(profileKeyVenue || "").trim();
    const label = String(profileKeyLabel || "").trim();
    const api_key = String(profileKeyApiKey || "").trim();
    const api_secret = String(profileKeyApiSecret || "").trim();
    const passphrase = String(profileKeyPassphrase || "").trim();
    const totp = String(profileKeyTotp || "").trim();

    if (!venue) {
      setProfileKeysMsg("Venue is required.");
      return;
    }
    if (!api_key) {
      setProfileKeysMsg("API key is required.");
      return;
    }
    if (authTotpEnabled && totp.length < 6) {
      setProfileKeysMsg("2FA code is required to save API keys.");
      return;
    }

    setProfileKeysBusy(true);
    setProfileKeysMsg("");
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${tok}`,
        },
        body: JSON.stringify({
          venue,
          label: label || null,
          api_key,
          api_secret: api_secret || null,
          passphrase: passphrase || null,
          totp: totp || null,
        }),
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { ok: false, detail: await r.text() };
      if (!r.ok || !data?.ok) {
        setProfileKeysMsg(data?.detail || `API key save failed (${r.status}).`);
        return;
      }
      // Write-only behavior: clear secrets from UI immediately
      setProfileKeyApiKey("");
      setProfileKeyApiSecret("");
      setProfileKeyPassphrase("");
      setProfileKeyTotp("");
      setProfileKeysMsg("Saved.");
      await callProfileApiKeysList();
    } catch (e) {
      setProfileKeysMsg(`API key save failed: ${String(e?.message || e)}`);
    } finally {
      setProfileKeysBusy(false);
    }
  };

  const callProfileApiKeysDelete = async (id) => {
    const base = tgTrimApiBase(API_BASE);
    const url = `${base}/api/auth/api_keys/${encodeURIComponent(String(id))}`;
    const tok = String(authToken || "").trim();
    if (!tok) {
      setProfileKeysMsg("Login required.");
      return;
    }
    const totp = String(profileKeyTotp || "").trim();
    if (authTotpEnabled && totp.length < 6) {
      setProfileKeysMsg("2FA code is required to delete API keys.");
      return;
    }

    setProfileKeysBusy(true);
    setProfileKeysMsg("");
    try {
      const r = await fetch(url, {
        method: "DELETE",
        headers: {
          Authorization: `Bearer ${tok}`,
          ...(totp ? { "X-UTT-TOTP": totp } : {}),
        },
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { ok: false, detail: await r.text() };
      if (!r.ok || !data?.ok) {
        setProfileKeysMsg(data?.detail || `API key delete failed (${r.status}).`);
        return;
      }
      setProfileKeysMsg("Deleted.");
      await callProfileApiKeysList();
    } catch (e) {
      setProfileKeysMsg(`API key delete failed: ${String(e?.message || e)}`);
    } finally {
      setProfileKeysBusy(false);
    }
  };



  const enableProfile2fa = async () => {
    const base = tgTrimApiBase(API_BASE);
    const url = `${base}/api/auth/2fa/enable`;
    const tok = String(authToken || "").trim();
    const code = String(profile2faCode || "").trim();
    if (!tok) {
      setProfile2faMsg("Login required.");
      return;
    }
    if (!code) {
      setProfile2faMsg("Enter the 6-digit code from your authenticator.");
      return;
    }
    setProfile2faBusy(true);
    setProfile2faMsg("");
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          Authorization: `Bearer ${tok}`,
        },
        body: JSON.stringify({ totp: code }),
      });

      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok) {
        const msg = typeof data?.detail === "string" ? data.detail : JSON.stringify(data?.detail || data);
        setProfile2faMsg(msg || `2FA enable failed (${r.status})`);
        return;
      }
      const enabledNow = Object.prototype.hasOwnProperty.call(data || {}, "totp_enabled") ? Boolean(data.totp_enabled) : true;
      setAuthTotpEnabled(enabledNow);
      setAuthTotpProvisioned(true);
      // Do not keep provisioning material around once enabled.
      clearProfile2faUi();
      setProfile2faMsg("2FA enabled for your account.");
    setProfile2faResetPw("");
    setProfile2faResetCode("");

    // Password change UI
    setProfilePwMsg("");
    setProfilePwBusy(false);
    setProfilePwCurrent("");
    setProfilePwNew("");
    setProfilePwNew2("");
    setProfilePwTotp("");
    } catch (e) {
      setProfile2faMsg(String(e?.message || e || "2FA enable failed."));
    } finally {
      setProfile2faBusy(false);
    }
  };


  const [buyHover, setBuyHover] = useState(false);
  const [buyDown, setBuyDown] = useState(false);
  const [airHover, setAirHover] = useState(false);
  const [airDown, setAirDown] = useState(false);

  const authBtnRef = useRef(null);
  const authPopRef = useRef(null);

  // Venue enable/disable manager (UI-local overrides)
  const [venueMgrOpen, setVenueMgrOpen] = useState(false);
  const venueMgrBtnRef = useRef(null);
  const venueMgrPopRef = useRef(null);


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

  // Banner controls are kept in a compact dock so the banner itself can stay stable.
  const bannerCtlWrapStyle = {
    position: "absolute",
    right: 12,
    top: 12,
    display: "flex",
    flexDirection: "column",
    alignItems: "stretch",
    gap: 8,
    width: "min(820px, calc(100% - 24px))",
    maxWidth: 820,
    padding: "8px 10px",
    borderRadius: 12,
    background: "linear-gradient(180deg, rgba(8,11,16,0.92), rgba(8,11,16,0.82))",
    border: "1px solid color-mix(in srgb, var(--utt-hdr-border, rgba(255,255,255,0.12)) 78%, rgba(95,124,255,0.28))",
    backdropFilter: "blur(12px)",
    WebkitBackdropFilter: "blur(12px)",
    boxShadow: "0 16px 34px rgba(0,0,0,0.45), 0 0 0 1px rgba(78,240,255,0.05) inset",
  };

  const bannerCtlRowStyle = {
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
    gap: 10,
    rowGap: 8,
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

  const bannerFrameStyle = {
    position: "relative",
    width: "100%",
    height: bannerDisplayHeight,
    overflow: "hidden",
    borderRadius: bannerFrameRadius,
    border: "1px solid color-mix(in srgb, var(--utt-hdr-border, rgba(255,255,255,0.10)) 82%, rgba(78,240,255,0.14))",
    background: "linear-gradient(180deg, rgba(7,10,18,0.94), rgba(10,12,16,0.98))",
    boxShadow: "0 14px 34px rgba(0,0,0,0.28), 0 0 0 1px rgba(95,124,255,0.05) inset",
  };

  const bannerBgImgStyle = {
    position: "absolute",
    inset: 0,
    width: "100%",
    height: "100%",
    objectFit: "cover",
    objectPosition: "50% 50%",
    filter: "blur(18px) saturate(1.04)",
    transform: "scale(1.08)",
    opacity: 0.34,
  };

  const bannerGlowOverlayStyle = {
    position: "absolute",
    inset: 0,
    background:
      "linear-gradient(180deg, rgba(0,0,0,0.08), rgba(0,0,0,0.18)), radial-gradient(circle at 18% 50%, rgba(78,240,255,0.10), transparent 32%), radial-gradient(circle at 82% 50%, rgba(180,92,255,0.10), transparent 34%)",
    pointerEvents: "none",
  };

  const bannerForegroundImgStyle = {
    position: "absolute",
    inset: 0,
    width: "100%",
    height: "100%",
    objectFit: (banner?.fitMode || "cover") === "contain" ? "contain" : "cover",
    objectPosition: `${banner?.posX ?? 50}% ${banner?.posY ?? 50}%`,
  };

  const bannerFitPanelStyle = {
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
    gap: 12,
    rowGap: 8,
    flexWrap: "wrap",
    padding: "8px 10px",
    borderRadius: 10,
    background: "rgba(0,0,0,0.28)",
    border: "1px solid rgba(255,255,255,0.10)",
    marginTop: 2,
    maxWidth: "100%",
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


  const authBtnStyle = {
    ...donateBtnStyle,
    background: "var(--utt-hdr-btn-bg, rgba(255,255,255,0.04))",
    border: "1px solid var(--utt-hdr-btn-border, rgba(255,255,255,0.12))",
    fontWeight: 800,
    letterSpacing: 0.2,
  };


  
const buyUtttBtnStyle = {
  display: "inline-flex",
  alignItems: "center",
  gap: 8,
  height: 34, // keep consistent with existing header buttons
  padding: "0 12px",
  borderRadius: 10,
  cursor: "pointer",
  userSelect: "none",
  fontWeight: 800,
  letterSpacing: 0.6,
  textTransform: "uppercase",
  fontSize: 12,
  lineHeight: "34px",
  whiteSpace: "nowrap",

  border: "1px solid transparent",
  background:
    "linear-gradient(#0b0d10, #0b0d10) padding-box," +
    "linear-gradient(90deg, #4ef0ff 0%, #5f7cff 45%, #b45cff 100%) border-box",

  color: "var(--utt-hdr-fg, #e8eef8)",
  boxShadow: "0 0 0 1px rgba(78,240,255,0.10) inset, 0 8px 18px rgba(0,0,0,0.55)",
};

const softBtnStyle = authBtnStyle;

const buyUtttBtnHoverStyle = {
  filter: "brightness(1.07)",
  boxShadow:
    "0 0 0 1px rgba(78,240,255,0.20) inset," +
    "0 10px 22px rgba(0,0,0,0.60)," +
    "0 0 16px rgba(78,240,255,0.15)",
};

const buyUtttBtnActiveStyle = {
  transform: "translateY(1px)",
  filter: "brightness(1.03)",
};



  // ---------------------------
  // Auth (Login) popover: FIXED positioning under the Login button + viewport clamp
  // (matches Donate popover behavior and avoids transparency/blur)
  // ---------------------------
  const AUTH_MARGIN = 10;
  const AUTH_GAP = 10;
  const AUTH_MAX_W = 420;

  const [authPos, setAuthPos] = useState(null); // { x, y, w }

  const computeAuthWidth = () => {
    const vw = Math.max(320, window.innerWidth || 0);
    const w = Math.min(AUTH_MAX_W, Math.floor(vw * 0.92));
    return Math.max(320, w);
  };

  const clampAuthPos = (x, y, w) => {
    const vw = Math.max(320, window.innerWidth || 0);
    const vh = Math.max(320, window.innerHeight || 0);

    const ww = Math.min(Math.max(320, w || computeAuthWidth()), vw - AUTH_MARGIN * 2);
    const maxX = Math.max(AUTH_MARGIN, vw - ww - AUTH_MARGIN);

    // clamp Y so the header remains reachable (we don't know panel height precisely)
    const maxY = Math.max(AUTH_MARGIN, vh - 80);

    const cx = clamp(x, AUTH_MARGIN, maxX);
    const cy = clamp(y, AUTH_MARGIN, maxY);

    return { x: cx, y: cy, w: ww };
  };

  const placeAuthNearButton = () => {
    const btn = authBtnRef.current;
    if (!btn) return;

    const rect = btn.getBoundingClientRect();
    const w = computeAuthWidth();

    const desiredX = rect.right - w;
    const desiredY = rect.bottom + AUTH_GAP;

    setAuthPos(clampAuthPos(desiredX, desiredY, w));
  };

  useEffect(() => {
    if (!authOpen) return;

    // On open: place it under the Login button
    placeAuthNearButton();

    // Clamp on resize
    const onResize = () => {
      setAuthPos((p) => {
        const w = computeAuthWidth();
        if (!p) return clampAuthPos(AUTH_MARGIN, 120, w);
        return clampAuthPos(p.x, p.y, Math.min(p.w || w, w));
      });
    };

    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authOpen]);

  const authPanelStyle = {
    position: "fixed",
    left: authPos?.x ?? AUTH_MARGIN,
    top: authPos?.y ?? 120,
    zIndex: 20000,
    width: authPos?.w ?? 360,
    maxWidth: "92vw",

    color: "var(--utt-hdr-fg, #e8eef8)",

    // fully opaque (match Donate popover behavior)
    backgroundColor: "rgba(10, 12, 14, 0.96)",
    border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
    borderRadius: 14,
    boxShadow: "var(--utt-hdr-shadow, 0 10px 24px rgba(0,0,0,0.35))",
    overflow: "hidden",
    opacity: 1,
  };

  const authPanelHeaderStyle = {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 10,
    padding: "10px 12px",
    borderBottom: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.10))",
  };

  const authPanelBodyStyle = {
    display: "flex",
    flexDirection: "column",
    gap: 10,
    padding: 12,
  };

  const authInputStyle = {
    width: "100%",
    padding: "8px 10px",
    borderRadius: 10,
    border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
    background: "rgba(0,0,0,0.25)",
    color: "var(--utt-hdr-fg, #e8eef8)",
    outline: "none",
    fontSize: 12,
  };

  const authRowStyle = { display: "flex", gap: 10, alignItems: "center" };
  const authPrimaryBtnStyle = {
    ...donatePrimaryBtnStyle,
    width: "100%",
    justifyContent: "center",
  };
  const authSecondaryBtnStyle = { ...donateSmallBtnStyle };


  // ---------------------------
  // Donate popover: FIXED positioning + viewport clamp (prevents header clipping)
  // ---------------------------
  const DONATE_MARGIN = 10;
  const DONATE_GAP = 10;
  const DONATE_MAX_W = 560;

  const [donatePos, setDonatePos] = useState(null); // { x, y, w }
  const [profilePos, setProfilePos] = useState(null); // { x, y, w }

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


  const PROFILE_W = 560;
  const placeProfileNearButton = () => {
    const btn = profileBtnRef.current;
    if (!btn) return;

    const r = btn.getBoundingClientRect();
    const w = Math.min(PROFILE_W, Math.max(360, Math.floor(window.innerWidth * 0.92)));
    let x = Math.round(r.left);
    let y = Math.round(r.bottom + 8);

    // clamp to viewport
    const maxX = Math.max(POP_MARGIN, window.innerWidth - w - POP_MARGIN);
    x = Math.min(Math.max(POP_MARGIN, x), maxX);

    const maxY = Math.max(POP_MARGIN, window.innerHeight - 140 - POP_MARGIN);
    y = Math.min(Math.max(POP_MARGIN, y), maxY);

    setProfilePos({ x, y, w });
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

  // ---------------------------
  // Venues Manager popover: FIXED positioning near the Manage button + viewport clamp
  // ---------------------------
  const VENUE_MGR_MARGIN = 10;
  const VENUE_MGR_GAP = 10;
  const VENUE_MGR_MAX_W = 420;

  const [venueMgrPos, setVenueMgrPos] = useState(null); // { x, y, w }

  const computeVenueMgrWidth = () => {
    const vw = Math.max(320, window.innerWidth || 0);
    const w = Math.min(VENUE_MGR_MAX_W, Math.floor(vw * 0.92));
    return Math.max(300, w);
  };

  const clampVenueMgrPos = (x, y, w) => {
    const vw = Math.max(320, window.innerWidth || 0);
    const vh = Math.max(320, window.innerHeight || 0);

    const ww = Math.min(Math.max(300, w || computeVenueMgrWidth()), vw - VENUE_MGR_MARGIN * 2);
    const maxX = Math.max(VENUE_MGR_MARGIN, vw - ww - VENUE_MGR_MARGIN);

    const maxY = Math.max(VENUE_MGR_MARGIN, vh - 80);
    const cx = clamp(x, VENUE_MGR_MARGIN, maxX);
    const cy = clamp(y, VENUE_MGR_MARGIN, maxY);

    return { x: cx, y: cy, w: ww };
  };

  const placeVenueMgrNearButton = () => {
    const btn = venueMgrBtnRef.current;
    if (!btn) return;

    const rect = btn.getBoundingClientRect();
    const w = computeVenueMgrWidth();

    const desiredX = rect.left; // open beside the Manage button
    const desiredY = rect.bottom + VENUE_MGR_GAP;

    setVenueMgrPos(clampVenueMgrPos(desiredX, desiredY, w));
  };

  useEffect(() => {
    if (!venueMgrOpen) return;

    // On open: place it under the Manage button
    placeVenueMgrNearButton();

    const onResize = () => {
      setVenueMgrPos((p) => {
        const w = computeVenueMgrWidth();
        if (!p) return clampVenueMgrPos(VENUE_MGR_MARGIN, 120, w);
        return clampVenueMgrPos(p.x, p.y, Math.min(p.w || w, w));
      });
    };

    // Track scroll too (fixed popover stays put relative to viewport, but button may move if user scrolls)
    const onScroll = () => placeVenueMgrNearButton();

    window.addEventListener("resize", onResize);
    window.addEventListener("scroll", onScroll, true);

    return () => {
      window.removeEventListener("resize", onResize);
      window.removeEventListener("scroll", onScroll, true);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [venueMgrOpen]);

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


  const profilePanelStyle = {
    position: "fixed",
    left: profilePos?.x ?? POP_MARGIN,
    top: profilePos?.y ?? 120,
    zIndex: 9999,
    width: profilePos?.w ?? 560,
    maxWidth: "92vw",
    borderRadius: 14,
    border: "1px solid rgba(255,255,255,0.12)",
    background: "rgba(10, 12, 14, 0.96)",
    boxShadow: "0 18px 40px rgba(0,0,0,0.65)",
    padding: 14,
    pointerEvents: "auto",
    maxHeight: "82vh",
    overflowY: "auto",
    overflowX: "auto",
    overscrollBehavior: "contain",
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
    setBannerFitOpen(false);
  };

  const setBannerPos = (posX, posY) => {
    if (!banner) return;
    const next = { ...banner, posX: Number(posX), posY: Number(posY), at: new Date().toISOString() };
    try {
      localStorage.setItem(LS_BANNER_KEY, JSON.stringify(next));
    } catch {
      // ignore
    }
    setBanner(next);
  };

  const setBannerFitMode = (fitMode) => {
    if (!banner) return;
    const mode = String(fitMode || "").toLowerCase();
    const nextMode = mode === "contain" ? "contain" : "cover";
    const next = { ...banner, fitMode: nextMode, at: new Date().toISOString() };
    try {
      localStorage.setItem(LS_BANNER_KEY, JSON.stringify(next));
    } catch {
      // ignore
    }
    setBanner(next);
  };


const autoFitBanner = async () => {
  if (!banner?.dataUrl) return;

  setBannerMsg("");
  try {
    const src = String(banner.dataUrl || "");
    const img = await new Promise((resolve, reject) => {
      const im = new Image();
      im.onload = () => resolve(im);
      im.onerror = () => reject(new Error("Image load failed"));
      im.src = src;
    });

    const targetW = BANNER_RECOMMENDED_W;
    const targetH = BANNER_RECOMMENDED_H;

    const canvas = document.createElement("canvas");
    canvas.width = targetW;
    canvas.height = targetH;
    const ctx = canvas.getContext("2d");
    if (!ctx) throw new Error("Canvas not supported");

    // ── Background: cover + blur/dim (full-bleed fill)
    const iw = img.naturalWidth || img.width || 0;
    const ih = img.naturalHeight || img.height || 0;
    if (!iw || !ih) throw new Error("Bad image dimensions");

    const scaleCover = Math.max(targetW / iw, targetH / ih);
    const bw = iw * scaleCover;
    const bh = ih * scaleCover;
    const bx = (targetW - bw) / 2;
    const by = (targetH - bh) / 2;

    ctx.clearRect(0, 0, targetW, targetH);
    ctx.save();
    ctx.filter = "blur(18px)";
    ctx.globalAlpha = 0.55;
    ctx.drawImage(img, bx, by, bw, bh);
    ctx.restore();

    // subtle dark wash to keep text/UI readable
    ctx.save();
    ctx.globalAlpha = 0.22;
    ctx.fillStyle = "#000";
    ctx.fillRect(0, 0, targetW, targetH);
    ctx.restore();
    // ── Foreground: cover (fills width/height), using current X/Y as an anchor (object-position-like)
    const posX = typeof banner?.posX === "number" ? banner.posX : 50;
    const posY = typeof banner?.posY === "number" ? banner.posY : 50;

    const maxOffX = targetW - bw; // <= 0 when bw >= targetW
    const maxOffY = targetH - bh; // <= 0 when bh >= targetH

    // 0% = left/top, 50% = center, 100% = right/bottom
    const fx = maxOffX * (Math.min(100, Math.max(0, posX)) / 100);
    const fy = maxOffY * (Math.min(100, Math.max(0, posY)) / 100);

    ctx.save();
    ctx.globalAlpha = 1;
    ctx.filter = "none";
    ctx.drawImage(img, fx, fy, bw, bh);
    ctx.restore();

    // Prefer webp if available (smaller); fall back to jpeg.
    let outUrl = "";
    try {
      outUrl = canvas.toDataURL("image/webp", 0.92);
    } catch {
      outUrl = "";
    }
    if (!outUrl || !outUrl.startsWith("data:image/")) {
      outUrl = canvas.toDataURL("image/jpeg", 0.92);
    }

    const next = {
      ...banner,
      dataUrl: outUrl,
      width: targetW,
      height: targetH,
      posX: 50,
      posY: 50,
      fitMode: "cover", // the exported image is already banner-shaped
      at: new Date().toISOString(),
      autoFit: true,
    };

    // Preview first: don’t apply until user confirms.
    setBannerAutoFitDraft(next);
    setBannerAutoFitPreviewOpen(true);
    setBannerMsg(`Auto-fit preview ready (${targetW}×${targetH}). Click Apply to save or Cancel.`);
  } catch (e) {
    setBannerMsg(`Auto-fit failed: ${String(e?.message || e)}`);
  }
};

  const applyAutoFitBanner = () => {
    if (!bannerAutoFitDraft) {
      setBannerAutoFitPreviewOpen(false);
      return;
    }
    try {
      localStorage.setItem(LS_BANNER_KEY, JSON.stringify(bannerAutoFitDraft));
    } catch {
      // ignore
    }
    setBanner(bannerAutoFitDraft);
    setBannerAutoFitDraft(null);
    setBannerAutoFitPreviewOpen(false);
    setBannerFitOpen(false);
    setBannerMsg(`Auto-fit applied.`);
  };

  const cancelAutoFitBanner = () => {
    setBannerAutoFitPreviewOpen(false);
    setBannerAutoFitDraft(null);
    setBannerMsg(`Auto-fit canceled.`);
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
      posX: 50,
      posY: 50,
      fitMode: "cover",
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

  // Venue manager list: show all venues from the registry (including disabled) plus any known supportedVenues.
  const venueMgrRows = useMemo(() => {
    const byId = new Map();

    const addRow = (idRaw, row) => {
      const id = normalizeVenue(idRaw);
      if (!id) return;
      if (!byId.has(id)) byId.set(id, { id, row: row && typeof row === "object" ? row : null });
      else if (row && typeof row === "object") byId.set(id, { id, row });
    };

    for (const r of Array.isArray(venuesRaw) ? venuesRaw : []) {
      const id = normalizeVenue(r?.venue ?? r?.id ?? r?.slug ?? r?.key ?? r?.code ?? r?.name ?? "");
      addRow(id, r);
    }
    for (const v of Array.isArray(supportedVenues) ? supportedVenues : []) addRow(v, null);

    const rows = Array.from(byId.values());

    const getLabel = (id, row) => {
      const label =
        row?.display_name ??
        row?.displayName ??
        row?.label ??
        row?.title ??
        row?.name ??
        row?.venue ??
        row?.id ??
        "";
      return String(label || "").trim() || (typeof labelVenueOption === "function" ? labelVenueOption(id) : id);
    };

    const isEnabled = (id, row) => {
      const k = normalizeVenue(id);
      if (k && venueOverrides && Object.prototype.hasOwnProperty.call(venueOverrides, k)) return !!venueOverrides[k];
      return row?.enabled !== false;
    };

    return rows
      .map(({ id, row }) => ({
        id,
        label: getLabel(id, row),
        enabled: isEnabled(id, row),
        backendEnabled: row?.enabled !== false,
      }))
      .sort((a, b) => String(a.label).localeCompare(String(b.label)));
  }, [venuesRaw, supportedVenues, venueOverrides, labelVenueOption]);

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

  // Venue manager popover behavior (close on outside click / ESC)
  useEffect(() => {
    if (!venueMgrOpen) return;

    const onDown = (e) => {
      const btn = venueMgrBtnRef.current;
      const pop = venueMgrPopRef.current;
      const t = e?.target;
      if (!t) return;
      if (btn && btn.contains(t)) return;
      if (pop && pop.contains(t)) return;
      setVenueMgrOpen(false);
    };

    const onKey = (e) => {
      if (!e) return;
      if (String(e.key || "").toLowerCase() === "escape") {
        setVenueMgrOpen(false);
      }
    };

    document.addEventListener("mousedown", onDown, true);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousedown", onDown, true);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [venueMgrOpen]);

  const MEDIA_MARGIN = 10;
  const MEDIA_GAP = 10;
  const MEDIA_MAX_W = 520;

  const computeMediaWidth = () => {
    const vw = Math.max(320, window.innerWidth || 0);
    const w = Math.min(MEDIA_MAX_W, Math.floor(vw * 0.92));
    return Math.max(320, w);
  };

  const clampMediaPos = (x, y, w) => {
    const vw = Math.max(320, window.innerWidth || 0);
    const vh = Math.max(320, window.innerHeight || 0);
    const ww = Math.min(Math.max(320, w || computeMediaWidth()), vw - MEDIA_MARGIN * 2);
    const maxX = Math.max(MEDIA_MARGIN, vw - ww - MEDIA_MARGIN);
    const maxY = Math.max(MEDIA_MARGIN, vh - 80);
    return { x: clamp(x, MEDIA_MARGIN, maxX), y: clamp(y, MEDIA_MARGIN, maxY), w: ww };
  };

  const placeMediaNearButton = () => {
    const btn = mediaBtnRef.current;
    if (!btn) return;
    const rect = btn.getBoundingClientRect();
    const w = computeMediaWidth();
    const desiredX = rect.right - w;
    const desiredY = rect.bottom + MEDIA_GAP;
    setMediaPos(clampMediaPos(desiredX, desiredY, w));
  };

  const clearMediaMsgSoon = (ms = 3000) => {
    window.clearTimeout(window.__utt_media_msg_to);
    window.__utt_media_msg_to = window.setTimeout(() => setMediaMsg(""), ms);
  };

  const mediaSetMessage = (msg, ms = 3000) => {
    setMediaMsg(String(msg || ""));
    if (msg) clearMediaMsgSoon(ms);
  };

  const cleanupMediaObjectUrl = () => {
    try {
      if (mediaObjectUrlRef.current) URL.revokeObjectURL(mediaObjectUrlRef.current);
    } catch {
      // ignore
    }
    mediaObjectUrlRef.current = "";
  };

  const mediaLabelFromSource = (src, resolved = "") => {
    const s = String(src || resolved || "").trim();
    if (!s) return "—";
    try {
      const u = new URL(s);
      const path = String(u.pathname || "").trim();
      const name = path.split("/").filter(Boolean).pop() || u.hostname || s;
      return name;
    } catch {
      const parts = s.split(/[\\/]/).filter(Boolean);
      return parts[parts.length - 1] || s;
    }
  };

  const resolveMediaUrl = async (rawUrl) => {
    const s = String(rawUrl || "").trim();
    if (!s) return { resolvedUrl: "", displayName: "" };
    if (/\.pls(\?|$)/i.test(s) || /listen\.pls(\?|$)/i.test(s)) {
      const r = await fetch(s, { method: "GET" });
      const txt = await r.text();
      const m = txt.match(/^File1=(.+)$/im);
      if (!m || !m[1]) throw new Error("Could not parse stream URL from .pls");
      const resolved = String(m[1]).trim();
      const titleMatch = txt.match(/^Title1=(.+)$/im);
      return {
        resolvedUrl: resolved,
        displayName: String(titleMatch?.[1] || mediaLabelFromSource(s, resolved)).trim(),
      };
    }
    return { resolvedUrl: s, displayName: mediaLabelFromSource(s, s) };
  };

  const loadMediaSource = async (rawUrl, opts = {}) => {
    const autoPlay = opts?.autoPlay !== false;
    const s = String(rawUrl || "").trim();
    if (!s) {
      mediaSetMessage("Enter a media URL or load a local file.");
      return;
    }
    setMediaBusy(true);
    try {
      const audio = mediaAudioRef.current;
      if (!audio) throw new Error("Media player not ready");
      const { resolvedUrl, displayName } = await resolveMediaUrl(s);
      if (!resolvedUrl) throw new Error("No playable media URL found");
      cleanupMediaObjectUrl();
      setMediaSource(s);
      setMediaResolvedUrl(resolvedUrl);
      setMediaNowPlaying(displayName || mediaLabelFromSource(s, resolvedUrl));
      writeMediaUrlToStorage(s);
      audio.src = resolvedUrl;
      audio.load();
      if (autoPlay) await audio.play();
      setMediaIsPlaying(!audio.paused);
      mediaSetMessage("Media loaded.");
    } catch (e) {
      mediaSetMessage(`Media load failed: ${String(e?.message || e)}`, 4000);
    } finally {
      setMediaBusy(false);
    }
  };

  const handleMediaFilePicked = async (file) => {
    if (!file) return;
    cleanupMediaObjectUrl();
    const objectUrl = URL.createObjectURL(file);
    mediaObjectUrlRef.current = objectUrl;
    setMediaSource(String(file.name || "local file"));
    setMediaResolvedUrl(objectUrl);
    setMediaNowPlaying(String(file.name || "Local file"));
    const audio = mediaAudioRef.current;
    if (!audio) return;
    audio.src = objectUrl;
    audio.load();
    try {
      await audio.play();
      setMediaIsPlaying(true);
      mediaSetMessage("Local media loaded.");
    } catch {
      setMediaIsPlaying(false);
      mediaSetMessage("Local media loaded. Press Play to start.");
    }
  };

  const AIRDROP_MARGIN = 10;
  const AIRDROP_GAP = 10;
  const AIRDROP_MAX_W = 520;

  const computeAirdropWidth = () => {
    const vw = Math.max(320, window.innerWidth || 0);
    const w = Math.min(AIRDROP_MAX_W, Math.floor(vw * 0.92));
    return Math.max(320, w);
  };

  const clampAirdropPos = (x, y, w) => {
    const vw = Math.max(320, window.innerWidth || 0);
    const vh = Math.max(320, window.innerHeight || 0);
    const ww = Math.min(Math.max(320, w || computeAirdropWidth()), vw - AIRDROP_MARGIN * 2);
    const maxX = Math.max(AIRDROP_MARGIN, vw - ww - AIRDROP_MARGIN);
    const maxY = Math.max(AIRDROP_MARGIN, vh - 80);
    return { x: clamp(x, AIRDROP_MARGIN, maxX), y: clamp(y, AIRDROP_MARGIN, maxY), w: ww };
  };

  const placeAirdropNearButton = () => {
    const btn = airdropBtnRef.current;
    if (!btn) return;
    const rect = btn.getBoundingClientRect();
    const w = computeAirdropWidth();
    const desiredX = rect.right - w;
    const desiredY = rect.bottom + AIRDROP_GAP;
    setAirdropPos(clampAirdropPos(desiredX, desiredY, w));
  };

  const getConnectedSolanaProvider = () => {
    try {
      const candidates = [
        window?.solflare,
        window?.phantom?.solana,
        window?.solana,
      ].filter(Boolean);

      const seen = new Set();
      for (const provider of candidates) {
        if (!provider || seen.has(provider)) continue;
        seen.add(provider);
        const pk = String(provider?.publicKey?.toString?.() || "").trim();
        if (pk) {
          return provider;
        }
      }
      return null;
    } catch {
      return null;
    }
  };

  const guessConnectedSolanaWallet = () => {
    try {
      const provider = getConnectedSolanaProvider();
      const pk = String(provider?.publicKey?.toString?.() || "").trim();
      return pk;
    } catch {
      return "";
    }
  };

  const shortWalletLabel = (wallet) => {
    const s = String(wallet || "").trim();
    if (!s) return "—";
    if (s.length <= 12) return s;
    return `${s.slice(0, 4)}…${s.slice(-4)}`;
  };

  const resolveConnectedSolanaWallet = async () => {
    try {
      const provider = getConnectedSolanaProvider();
      const current = String(provider?.publicKey?.toString?.() || "").trim();
      return current || "";
    } catch {
      return "";
    }
  };

  const callAirdropStatus = async (walletOverride = "") => {
    const wallet = String(walletOverride || airdropWalletDraft || airdropWallet || guessConnectedSolanaWallet() || "").trim();
    setAirdropErr("");
    setAirdropBusy(true);
    try {
      const base = tgTrimApiBase(API_BASE);
      const qs = new URLSearchParams();
      if (wallet) qs.set("wallet", wallet);
      const headers = { Accept: "application/json" };
      if (String(authToken || "").trim()) headers.Authorization = `Bearer ${String(authToken || "").trim()}`;
      const r = await fetch(`${base}/api/airdrop/status${qs.toString() ? `?${qs.toString()}` : ""}`, {
        method: "GET",
        headers,
      });
      const ct = String(r.headers.get("content-type") || "");
      const data = ct.includes("application/json") ? await r.json() : { detail: await r.text() };
      if (!r.ok || data?.ok === false) {
        const msg = typeof data?.detail === "string" ? data.detail : JSON.stringify(data?.detail || data);
        setAirdropErr(msg || `Airdrop status failed (${r.status})`);
        setAirdropStatus(null);
        return;
      }
      setAirdropStatus(data || null);
      const resolvedWallet = String(data?.wallet || wallet).trim();
      if (resolvedWallet) {
        setAirdropWallet(resolvedWallet);
        setAirdropWalletDraft("");
      }
    } catch (e) {
      setAirdropErr(String(e?.message || e || "Airdrop status failed."));
      setAirdropStatus(null);
    } finally {
      setAirdropBusy(false);
    }
  };

  const bytesToBase64 = (bytesLike) => {
    try {
      const bytes = bytesLike instanceof Uint8Array ? bytesLike : new Uint8Array(bytesLike || []);
      let binary = "";
      const chunk = 0x8000;
      for (let i = 0; i < bytes.length; i += chunk) {
        binary += String.fromCharCode(...bytes.slice(i, i + chunk));
      }
      return window.btoa(binary);
    } catch {
      return "";
    }
  };

  const registerAirdropWallet = async () => {
    const connected = await resolveConnectedSolanaWallet();
    const wallet = String(airdropWalletDraft || airdropWallet || connected || "").trim();
    if (!wallet) {
      setAirdropErr("Connect a Solana wallet first.");
      return;
    }
    if (!connected) {
      setAirdropErr("No connected Solana wallet detected for signing.");
      return;
    }
    if (connected !== wallet) {
      setAirdropErr("The pasted wallet must match the connected Solana wallet before registration.");
      return;
    }
    const signingProvider = getConnectedSolanaProvider();
    if (!signingProvider?.signMessage) {
      setAirdropErr("The connected Solana wallet does not support signMessage.");
      return;
    }

    setAirdropErr("");
    setAirdropRegisterBusy(true);
    try {
      const base = tgTrimApiBase(API_BASE);
      const headers = { "content-type": "application/json", Accept: "application/json" };
      if (String(authToken || "").trim()) headers.Authorization = `Bearer ${String(authToken || "").trim()}`;

      const r1 = await fetch(`${base}/api/airdrop/register_challenge`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          wallet,
          authUser: String(authUser || "").trim() || null,
        }),
      });
      const ct1 = String(r1.headers.get("content-type") || "");
      const data1 = ct1.includes("application/json") ? await r1.json() : { detail: await r1.text() };
      if (!r1.ok || data1?.ok === false) {
        const msg = typeof data1?.detail === "string" ? data1.detail : JSON.stringify(data1?.detail || data1);
        setAirdropErr(msg || `Airdrop registration challenge failed (${r1.status})`);
        return;
      }

      if (data1?.alreadyRegistered) {
        setAirdropChallenge(null);
        setAirdropMsg("Wallet already registered for this campaign.");
        if (data1?.status) {
          setAirdropStatus(data1.status);
          setAirdropWallet(String(wallet || challenge?.wallet || "").trim());
          setAirdropWalletDraft("");
        } else {
          await callAirdropStatus(wallet);
        }
        return;
      }

      const challenge = {
        wallet: String(data1?.wallet || wallet).trim(),
        nonce: String(data1?.nonce || "").trim(),
        message: String(data1?.message || "").trim(),
        campaignId: String(data1?.campaignId || "").trim(),
      };
      setAirdropChallenge(challenge);
      if (!challenge.nonce || !challenge.message) {
        setAirdropErr("Airdrop challenge response was incomplete.");
        return;
      }

      const encoded = new TextEncoder().encode(challenge.message);
      const signed = await signingProvider.signMessage(encoded, "utf8");
      const sigBytes = signed?.signature || signed;
      const signature = bytesToBase64(sigBytes);
      if (!signature) {
        setAirdropErr("Wallet signature capture failed.");
        return;
      }

      const r2 = await fetch(`${base}/api/airdrop/register_verify`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          wallet: challenge.wallet,
          nonce: challenge.nonce,
          message: challenge.message,
          signature,
          authUser: String(authUser || "").trim() || null,
        }),
      });
      const ct2 = String(r2.headers.get("content-type") || "");
      const data2 = ct2.includes("application/json") ? await r2.json() : { detail: await r2.text() };
      if (!r2.ok || data2?.ok === false) {
        const msg = typeof data2?.detail === "string" ? data2.detail : JSON.stringify(data2?.detail || data2);
        setAirdropErr(msg || `Airdrop registration verify failed (${r2.status})`);
        return;
      }

      setAirdropMsg(data2?.alreadyRegistered ? "Wallet already registered." : "Wallet registered for airdrop review.");
      window.clearTimeout(window.__utt_airdrop_msg_to);
      window.__utt_airdrop_msg_to = window.setTimeout(() => setAirdropMsg(""), 4000);
      setAirdropChallenge(null);
      if (data2?.status) {
        setAirdropStatus(data2.status);
        setAirdropWallet(String(data2?.status?.wallet || wallet).trim());
        setAirdropWalletDraft("");
      } else {
        await callAirdropStatus(wallet);
      }
    } catch (e) {
      setAirdropErr(String(e?.message || e || "Airdrop registration failed."));
    } finally {
      setAirdropRegisterBusy(false);
    }
  };

  useEffect(() => {
    writeMediaUrlToStorage(mediaUrlInput);
  }, [mediaUrlInput]);

  useEffect(() => {
    writeMediaVolToStorage(mediaVolume);
    try {
      if (mediaAudioRef.current) mediaAudioRef.current.volume = Number(mediaVolume);
    } catch {
      // ignore
    }
  }, [mediaVolume]);

  useEffect(() => {
    const audio = mediaAudioRef.current;
    if (!audio) return;
    audio.volume = Number(mediaVolume);
    const onPlay = () => setMediaIsPlaying(true);
    const onPause = () => setMediaIsPlaying(false);
    const onEnded = () => setMediaIsPlaying(false);
    const onLoadedMeta = () => {
      const current = String(mediaNowPlaying || "").trim();
      if (!current) setMediaNowPlaying(mediaLabelFromSource(mediaSource, mediaResolvedUrl));
    };
    audio.addEventListener("play", onPlay);
    audio.addEventListener("pause", onPause);
    audio.addEventListener("ended", onEnded);
    audio.addEventListener("loadedmetadata", onLoadedMeta);
    return () => {
      audio.removeEventListener("play", onPlay);
      audio.removeEventListener("pause", onPause);
      audio.removeEventListener("ended", onEnded);
      audio.removeEventListener("loadedmetadata", onLoadedMeta);
    };
  }, [mediaVolume, mediaNowPlaying, mediaResolvedUrl, mediaSource]);

  useEffect(() => {
    if (!mediaOpen) return;
    placeMediaNearButton();
    const onResize = () => {
      setMediaPos((p) => {
        const w = computeMediaWidth();
        if (!p) return clampMediaPos(MEDIA_MARGIN, 120, w);
        return clampMediaPos(p.x, p.y, Math.min(p.w || w, w));
      });
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, [mediaOpen]);

  useEffect(() => {
    if (!mediaOpen) return;
    const onDown = (e) => {
      const btn = mediaBtnRef.current;
      const pop = mediaPopRef.current;
      const t = e?.target;
      if (btn && btn.contains(t)) return;
      if (pop && pop.contains(t)) return;
      setMediaOpen(false);
    };
    const onKey = (e) => {
      if (e?.key === "Escape") setMediaOpen(false);
    };
    document.addEventListener("mousedown", onDown, true);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousedown", onDown, true);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [mediaOpen]);

  useEffect(() => {
    return () => cleanupMediaObjectUrl();
  }, []);

  useEffect(() => {
    if (!airdropOpen) return;
    const guessed = guessConnectedSolanaWallet();
    if (guessed && !String(airdropWallet || "").trim()) setAirdropWallet(guessed);
    setAirdropWalletDraft("");
    setAirdropChallenge(null);
    placeAirdropNearButton();
    callAirdropStatus(guessed || airdropWallet || "");
    const onResize = () => {
      setAirdropPos((p) => {
        const w = computeAirdropWidth();
        if (!p) return clampAirdropPos(AIRDROP_MARGIN, 120, w);
        return clampAirdropPos(p.x, p.y, Math.min(p.w || w, w));
      });
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, [airdropOpen]);

  useEffect(() => {
    if (!airdropOpen) return;
    const onDown = (e) => {
      const btn = airdropBtnRef.current;
      const pop = airdropPopRef.current;
      const t = e?.target;
      if (btn && btn.contains(t)) return;
      if (pop && pop.contains(t)) return;
      setAirdropOpen(false);
      setAirdropErr("");
    };
    const onKey = (e) => {
      if (e?.key === "Escape") {
        setAirdropOpen(false);
        setAirdropErr("");
      }
    };
    document.addEventListener("mousedown", onDown, true);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousedown", onDown, true);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [airdropOpen]);

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
// const POP_MARGIN = 8; // moved to module scope (avoid TDZ)
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

        <div style={bannerFrameStyle}>
          <img
            src={banner?.dataUrl || uttBanner}
            alt=""
            style={bannerBgImgStyle}
            draggable={false}
          />
          <div style={bannerGlowOverlayStyle} />
          <img
            src={banner?.dataUrl || uttBanner}
            alt="Unified Trading Terminal"
            style={bannerForegroundImgStyle}
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

            <button
              type="button"
              style={{
                ...bannerBtnSecondaryStyle,
                opacity: banner ? 1 : 0.55,
                cursor: banner ? "pointer" : "not-allowed",
              }}
              onClick={() => (banner ? setBannerFitOpen((v) => !v) : null)}
              disabled={!banner}
              title={banner ? "Adjust banner crop position" : "No custom banner set"}
            >
              Fit
            </button>

<button
  type="button"
  style={{
    ...bannerBtnSecondaryStyle,
    opacity: banner ? 1 : 0.55,
    cursor: banner ? "pointer" : "not-allowed",
  }}
  onClick={() => (banner ? autoFitBanner() : null)}
  disabled={!banner}
  title={banner ? "Auto-fit: export a banner-shaped image (no distortion) so any upload looks right" : "No custom banner set"}
>
  Auto-fit
</button>


            <div style={bannerReqStyle} title="Recommended banner size and limits">
              Recommended: {BANNER_RECOMMENDED_W}×{BANNER_RECOMMENDED_H}+ • Max {(BANNER_MAX_BYTES / (1024 * 1024)).toFixed(0)}MB • JPG/PNG/WebP
            </div>
          </div>

          {bannerFitOpen && banner && (
            <div style={bannerFitPanelStyle}>
              <div style={{ fontSize: 12, opacity: 0.9, minWidth: 70 }}>Position</div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ fontSize: 12, opacity: 0.8 }}>X</div>
                <input
                  type="range"
                  min="0"
                  max="100"
                  value={banner.posX ?? 50}
                  onChange={(e) => setBannerPos(e.target.value, banner.posY ?? 50)}
                />
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ fontSize: 12, opacity: 0.8 }}>Y</div>
                <input
                  type="range"
                  min="0"
                  max="100"
                  value={banner.posY ?? 50}
                  onChange={(e) => setBannerPos(banner.posX ?? 50, e.target.value)}
                />
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12, opacity: 0.85 }}>
                <span title="Cover fills the banner (cropped). Contain fits the whole image (letterboxed).">
                  object-fit:
                </span>
                <button
                  type="button"
                  onClick={() => setBannerFitMode("cover")}
                  style={{
                    padding: "3px 8px",
                    borderRadius: 999,
                    border: "1px solid rgba(255,255,255,0.16)",
                    background: (banner?.fitMode || "cover") === "cover" ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.25)",
                    color: "rgba(255,255,255,0.92)",
                    cursor: "pointer",
                  }}
                  title="cover (fills banner; crops edges)"
                >
                  cover
                </button>
                <button
                  type="button"
                  onClick={() => setBannerFitMode("contain")}
                  style={{
                    padding: "3px 8px",
                    borderRadius: 999,
                    border: "1px solid rgba(255,255,255,0.16)",
                    background: (banner?.fitMode || "cover") === "contain" ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.25)",
                    color: "rgba(255,255,255,0.92)",
                    cursor: "pointer",
                  }}
                  title="contain (no crop; adds letterbox)"
                >
                  contain
                </button>
              </div>
            </div>
          )}

          {bannerAutoFitPreviewOpen && bannerAutoFitDraft && (
            <div
              style={{
                position: "fixed",
                inset: 0,
                background: "rgba(0,0,0,0.55)",
                zIndex: 9999,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                padding: 18,
              }}
              onClick={cancelAutoFitBanner}
            >
              <div
                style={{
                  width: "min(980px, 96vw)",
                  borderRadius: 14,
                  border: "1px solid rgba(255,255,255,0.14)",
                  background: "rgba(18,20,26,0.92)",
                  boxShadow: "0 24px 80px rgba(0,0,0,0.55)",
                  overflow: "hidden",
                }}
                onClick={(e) => e.stopPropagation()}
              >
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 14px" }}>
                  <div style={{ fontWeight: 700, letterSpacing: 0.2 }}>Auto-fit preview</div>
                  <div style={{ display: "flex", gap: 8 }}>
                    <button type="button" style={bannerBtnSecondaryStyle} onClick={cancelAutoFitBanner}>
                      Cancel
                    </button>
                    <button type="button" style={bannerBtnStyle} onClick={applyAutoFitBanner}>
                      Apply
                    </button>
                  </div>
                </div>

                <div style={{ padding: 14 }}>
                  <div
                    style={{
                      borderRadius: 12,
                      overflow: "hidden",
                      border: "1px solid rgba(255,255,255,0.10)",
                      background: "rgba(0,0,0,0.25)",
                    }}
                  >
                    <img
                      src={bannerAutoFitDraft.dataUrl}
                      alt="Auto-fit banner preview"
                      style={{
                        display: "block",
                        width: "100%",
                        height: "auto",
                      }}
                    />
                  </div>

                  <div style={{ marginTop: 10, fontSize: 12, opacity: 0.85, display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <span>Exported: {bannerAutoFitDraft.width}×{bannerAutoFitDraft.height}</span>
                    <span style={{ opacity: 0.6 }}>•</span>
                    <span>Mode: contain foreground + blurred fill</span>
                    <span style={{ opacity: 0.6 }}>•</span>
                    <span>Click outside to cancel</span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {!!bannerMsg && <div style={bannerWarnStyle}>{bannerMsg}</div>}
          {!!buyUtttMsg && <div style={bannerWarnStyle}>{buyUtttMsg}</div>}
          {!!airdropMsg && <div style={bannerWarnStyle}>{airdropMsg}</div>}
          {!!authMsg && !authOpen && <div style={bannerWarnStyle}>{authMsg}</div>}

          {/* Donate button lives under the banner upload controls */}
          <div style={{ position: "relative", alignSelf: "flex-end", display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", justifyContent: "flex-end" }}>
            <button
              ref={authBtnRef}
              type="button"
              style={authBtnStyle}
              onClick={async () => {
                if (authToken) {
                  await logoutWithBackup();
                  return;
                }
                setAuthOpen((v) => {
                  const next = !v;
                  if (next) {
                    clearAuthSensitive();
                    refreshAuthBootstrapStatus();
                    // place under button after render
                    setTimeout(() => placeAuthNearButton(), 0);
                  }
                  return next;
                });
                setAuthMsg("");
              }}
              title={authToken ? "Logout (local)" : "Login"}
              aria-label={authToken ? "Logout" : "Login"}
            >
              {authToken ? <UserIcon size={14} /> : <LockIcon size={14} />}
              <span>{authToken ? "LOGOUT" : "LOGIN"}</span>
            </button>

            



            <button
              type="button"
              style={{
                ...buyUtttBtnStyle,
                ...(buyHover ? buyUtttBtnHoverStyle : null),
                ...(buyDown ? buyUtttBtnActiveStyle : null),
              }}
              onMouseEnter={() => setBuyHover(true)}
              onMouseLeave={() => {
                setBuyHover(false);
                setBuyDown(false);
              }}
              onMouseDown={() => setBuyDown(true)}
              onMouseUp={() => setBuyDown(false)}
              onClick={() => {
                try {
                  const targetVenue = "solana_jupiter";
                  const targetSymbol = "UTTT-USDC";
                  setVenue?.(targetVenue);
                  setApplyMarketToTab?.(true);

                  const runApply = () => {
                    try {
                      setMarketInput?.(targetSymbol);
                      applyMarketSymbol?.(targetSymbol);
                    } catch {
                      // ignore
                    }
                  };

                  // The venue switch appears to trigger a later state sync that can restore the
                  // previous market. Re-assert the intended market a few times across the next
                  // settled UI ticks so the explicit BUY UTTT target wins.
                  const scheduleApply = (delayMs) => {
                    window.setTimeout(() => {
                      if (typeof window.requestAnimationFrame === "function") {
                        window.requestAnimationFrame(runApply);
                      } else {
                        runApply();
                      }
                    }, delayMs);
                  };

                  window.clearTimeout(window.__utt_buy_apply_to_0);
                  window.clearTimeout(window.__utt_buy_apply_to_1);
                  window.clearTimeout(window.__utt_buy_apply_to_2);

                  runApply();
                  window.__utt_buy_apply_to_0 = window.setTimeout(() => scheduleApply(0), 40);
                  window.__utt_buy_apply_to_1 = window.setTimeout(() => scheduleApply(0), 140);
                  window.__utt_buy_apply_to_2 = window.setTimeout(() => scheduleApply(0), 320);

                  setBuyUtttMsg(`BUY UTTT ready: ${targetVenue} • ${targetSymbol}`);
                } catch {
                  setBuyUtttMsg("BUY UTTT shortcut failed to initialize.");
                }
                window.clearTimeout(window.__utt_buy_msg_to);
                window.__utt_buy_msg_to = window.setTimeout(() => setBuyUtttMsg(""), 4000);
              }}
              title="Buy UTTT via Solana/Jupiter using UTTT-USDC"
              aria-label="Buy UTTT"
            >
              <BuyUtttIcon size={14} />
              <span>BUY UTTT</span>
            </button>

            <button
              type="button"
              style={{
                ...buyUtttBtnStyle,
                ...(airHover ? buyUtttBtnHoverStyle : null),
                ...(airDown ? buyUtttBtnActiveStyle : null),
              }}
              onMouseEnter={() => setAirHover(true)}
              onMouseLeave={() => {
                setAirHover(false);
                setAirDown(false);
              }}
              onMouseDown={() => setAirDown(true)}
              onMouseUp={() => setAirDown(false)}
              ref={airdropBtnRef}
              onClick={() => {
                const next = !airdropOpen;
                setAirdropOpen(next);
                setAirdropErr("");
                if (next) setTimeout(() => placeAirdropNearButton(), 0);
              }}
              title="Airdrop status / eligibility"
              aria-label="Airdrop"
            >
              <AirdropIcon size={14} />
              <span>AIRDROP</span>
            </button>

            <button
              ref={profileBtnRef}
              type="button"
              style={donateBtnStyle}
              onClick={() => {
                const next = !profileOpen;
                // Always clear Profile transient UI (messages / 2FA fields) on close/open.
                clearProfile2faUi();
                setProfileOpen(next);
                if (!profileOpen && next) setTimeout(() => placeProfileNearButton(), 0);
              }}
              title="Profile"
              aria-label="Profile"
            >
              <span style={{ fontWeight: 800, letterSpacing: 0.5 }}>PROFILE</span>
            </button>
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

            <button
              ref={mediaBtnRef}
              type="button"
              style={donateBtnStyle}
              onClick={() => {
                const next = !mediaOpen;
                setMediaOpen(next);
                if (next) setTimeout(() => placeMediaNearButton(), 0);
              }}
              title="Media player"
              aria-label="Media player"
            >
              <MediaIcon size={14} />
              <span>MEDIA</span>
            </button>
          </div>

        </div>
      </div>
    </div>

      {authOpen && !authToken && (
        <div ref={authPopRef} style={authPanelStyle}>
          <div style={authPanelHeaderStyle}>
            <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 0 }}>
              <div style={{ fontWeight: 900, fontSize: 13, lineHeight: 1.1 }}>Sign in</div>
              <div style={{ fontSize: 11, opacity: 0.75, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {authSignupOpen || authBootstrapRequired ? "Create the first local account for this install" : "Sign in with your local UTT account"}
              </div>
            </div>

            <button
              type="button"
              style={authSecondaryBtnStyle}
              onClick={() => {
                setAuthOpen(false);
                clearAuthSensitive();
                setAuthMsg("");
              }}
              title="Close"
            >
              Close
            </button>
          </div>

          <div style={authPanelBodyStyle}>
            {!!authMsg && <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-warn, var(--utt-warn, #f7b955))" }}>{authMsg}</div>}

            <input
              style={authInputStyle}
              placeholder="Username / email"
              value={authForm.user}
              onChange={(e) => setAuthForm((s) => ({ ...s, user: e.target.value }))}
              autoComplete="username"
            />

            <input
              style={authInputStyle}
              placeholder="Password"
              type="password"
              value={authForm.pass}
              onChange={(e) => setAuthForm((s) => ({ ...s, pass: e.target.value }))}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  submitAuth();
                }
              }}
              autoComplete="current-password"
            />

            {(authSignupOpen || authBootstrapRequired) && (
              <input
                style={authInputStyle}
                placeholder="Confirm password"
                type="password"
                value={authForm.pass2}
                onChange={(e) => setAuthForm((s) => ({ ...s, pass2: e.target.value }))}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    submitAuth();
                  }
                }}
                autoComplete="new-password"
              />
            )}

            <div style={authRowStyle}>
              <input
                style={{ ...authInputStyle, flex: 1 }}
                placeholder={authSignupOpen || authBootstrapRequired ? "2FA code (leave blank on first sign-in)" : "2FA code (optional)"}
                value={authForm.otp}
                onChange={(e) => setAuthForm((s) => ({ ...s, otp: e.target.value.replace(/[^0-9]/g, "").slice(0, 8) }))}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    submitAuth();
                  }
                }}
                inputMode="numeric"
              />
            </div>

            {(authSignupOpen || authBootstrapRequired) && (
              <label style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, opacity: 0.9, cursor: "pointer", userSelect: "none" }}>
                <input
                  type="checkbox"
                  checked={!!authForm.want2fa}
                  onChange={(e) => setAuthForm((s) => ({ ...s, want2fa: !!e.target.checked }))}
                />
                <span>Provision 2FA during first account setup</span>
              </label>
            )}

            <label style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, opacity: 0.9, cursor: "pointer", userSelect: "none" }}>
              <input
                type="checkbox"
                checked={!!profileStayLoggedIn}
                onChange={(e) => setProfileStayLoggedIn(!!e.target.checked)}
              />
              <span>Stay logged in on this device</span>
            </label>

            <button
              type="button"
              style={{ ...authPrimaryBtnStyle, opacity: authBusy ? 0.75 : 1 }}
              onClick={submitAuth}
              disabled={authBusy}
            >
              {authSignupOpen || authBootstrapRequired ? (authBusy ? "Creating..." : "Create first account") : (authBusy ? "Signing in..." : "Sign in")}
            </button>

            <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.25 }}>
              {authSignupOpen || authBootstrapRequired
                ? <>No local users exist yet. This first-account flow is only available while the user table is empty.</>
                : <>Local auth is backed by the backend auth routes. It does not change any existing venue behavior.</>}
            </div>
          </div>
        </div>
      )}

      {/* FIXED popover (outside of banner stack) so it cannot be clipped */}

      {profileOpen && (
        <div ref={profilePopRef} style={profilePanelStyle}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
            <div>
              <div style={{ fontSize: 14, fontWeight: 900 }}>Profile</div>
              <div style={{ fontSize: 12, opacity: 0.8 }}>Account • Password • 2FA • API Keys</div>
            </div>
            <button
              type="button"
              style={donateSmallBtnStyle}
              onClick={() => setProfileOpen(false)}
              title="Close"
            >
              Close
            </button>
          </div>

          {!authToken ? (
            <div style={{ fontSize: 13, opacity: 0.9 }}>
              You must be logged in to use Profile settings.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              <div>
                <div style={{ fontWeight: 800, marginBottom: 6 }}>Account</div>
                <div style={{ fontSize: 13, opacity: 0.9 }}>Signed in as: <b>{authUser || "local"}</b></div>
                <label style={{ display: "inline-flex", alignItems: "center", gap: 8, marginTop: 8, fontSize: 12, opacity: profileSessionPrefBusy ? 0.7 : 0.92, cursor: profileSessionPrefBusy ? "not-allowed" : "pointer", userSelect: "none" }}>
                  <input
                    type="checkbox"
                    checked={!!profileStayLoggedIn}
                    disabled={profileSessionPrefBusy}
                    onChange={async (e) => {
                      const next = !!e.target.checked;
                      const prev = !!profileStayLoggedIn;
                      setProfileStayLoggedIn(next);
                      const ok = await saveSessionPrefs(next);
                      if (!ok) setProfileStayLoggedIn(prev);
                    }}
                  />
                  <span>Stay logged in on this device</span>
                </label>
                {!!profileSessionMsg && (
                  <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-link, #9ad)", lineHeight: 1.35, wordBreak: "break-word", marginTop: 6 }}>
                    {profileSessionMsg}
                  </div>
                )}
                <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.25, marginTop: 4 }}>
                  Default session lifetime is controlled by the backend. Enabling this requests a longer-lived session and refreshes the current login token immediately.
                </div>
              </div>
              <div style={{ height: 1, background: "rgba(255,255,255,0.08)" }} />
              <div>
                <div style={{ fontWeight: 800, marginBottom: 6 }}>Backup / Recovery</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {!!profileBackupMsg && (
                    <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-link, #9ad)", lineHeight: 1.35, wordBreak: "break-word" }}>
                      {profileBackupMsg}
                    </div>
                  )}
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                    <button
                      type="button"
                      style={donateSmallBtnStyle}
                      disabled={profileBackupBusy}
                      onClick={() => callBackupDatabase({ quiet: false })}
                      title="Create a timestamped backup of the active SQLite database"
                    >
                      {profileBackupBusy ? "Backing up..." : "Backup Database"}
                    </button>
                  </div>
                  <label style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, opacity: profileBackupPrefBusy ? 0.7 : 0.9, cursor: profileBackupPrefBusy ? "not-allowed" : "pointer", userSelect: "none" }}>
                    <input
                      type="checkbox"
                      checked={!!profileAutoBackupOnLogout}
                      disabled={profileBackupPrefBusy}
                      onChange={async (e) => {
                        const next = !!e.target.checked;
                        const prev = !!profileAutoBackupOnLogout;
                        setProfileAutoBackupOnLogout(next);
                        const ok = await saveBackupPrefs(next);
                        if (!ok) setProfileAutoBackupOnLogout(prev);
                      }}
                    />
                    <span>Auto-backup database before logout</span>
                  </label>
                  <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.25 }}>
                    You can back up manually here, and choose whether logout should create a backup automatically.
                  </div>
                </div>
              </div>
              <div style={{ height: 1, background: "rgba(255,255,255,0.08)" }} />
              <div>
                <div style={{ fontWeight: 800, marginBottom: 6 }}>Password</div>

                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {!!profilePwMsg && (
                    <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-link, #9ad)" }}>
                      {profilePwMsg}
                    </div>
                  )}

                  <input
                    style={donateInputStyle}
                    type="password"
                    value={profilePwCurrent}
                    onChange={(e) => setProfilePwCurrent(e.target.value)}
                    placeholder="Current password"
                    autoComplete="current-password"
                  />

                  <input
                    style={donateInputStyle}
                    type="password"
                    value={profilePwNew}
                    onChange={(e) => setProfilePwNew(e.target.value)}
                    placeholder="New password"
                    autoComplete="new-password"
                  />

                  <input
                    style={donateInputStyle}
                    type="password"
                    value={profilePwNew2}
                    onChange={(e) => setProfilePwNew2(e.target.value)}
                    placeholder="Confirm new password"
                    autoComplete="new-password"
                  />

                  <input
                    style={donateInputStyle}
                    value={profilePwTotp}
                    onChange={(e) => setProfilePwTotp(e.target.value.replace(/[^0-9]/g, "").slice(0, 8))}
                    placeholder={authTotpEnabled ? "2FA code (required)" : "2FA code (optional)"}
                    inputMode="numeric"
                  />

                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <button
                      type="button"
                      style={donateSmallBtnStyle}
                      disabled={profilePwBusy}
                      onClick={callProfilePasswordChange}
                      title="Change password (requires current password; requires 2FA if enabled)"
                    >
                      {profilePwBusy ? "Updating..." : "Update password"}
                    </button>

                    <button
                      type="button"
                      style={donateSmallBtnStyle}
                      disabled={profilePwBusy}
                      onClick={() => {
                        setProfilePwMsg("");
                        setProfilePwCurrent("");
                        setProfilePwNew("");
                        setProfilePwNew2("");
                        setProfilePwTotp("");
                      }}
                      title="Clear password fields"
                    >
                      Clear
                    </button>
                  </div>

                  <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.25 }}>
                    Password changes require your current password. If 2FA is enabled, a current 2FA code is required (step-up).
                  </div>
                </div>
              </div>
              <div style={{ height: 1, background: "rgba(255,255,255,0.08)" }} />
              <div>
                <div style={{ fontWeight: 800, marginBottom: 6 }}>2FA</div>
                <div style={{ fontSize: 13, opacity: 0.85 }}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      {!authTotpEnabled && !authTotpProvisioned && (
                        <button
                          type="button"
                          style={donateSmallBtnStyle}
                          disabled={profile2faBusy}
                          onClick={() => {
                            clearProfile2faUi();
                            callProfile2fa("setup", {});
                          }}
                          title="Provision 2FA (one-time). Does not enable until verified."
                        >
                          Setup 2FA
                        </button>
                      )}

                      {!authTotpEnabled && authTotpProvisioned && (
                        <button
                          type="button"
                          style={donateSmallBtnStyle}
                          disabled={profile2faBusy}
                          onClick={() => {
                            clearProfile2faUi();
                            callProfile2fa("setup", {});
                          }}
                          title="Show existing pending 2FA provisioning (does not rotate)."
                        >
                          Show 2FA
                        </button>
                      )}

                      <button
                        type="button"
                        style={donateSmallBtnStyle}
                        disabled={profile2faBusy}
                        onClick={() => {
                          // Keep reset confirmation fields; clear only provisioning UI.
                          setProfile2faMsg("");
                          setProfile2faSecret("");
                          setProfile2faOtpAuth("");
                          setProfile2faShowQr(false);
                          setProfile2faQrSvg("");
                          setProfile2faCode("");
                          callProfile2fa("reset", { password: profile2faResetPw || "", totp: profile2faResetCode || "" });
                        }}
                        title="Reset (rotate) your 2FA secret"
                      >
                        Reset 2FA
                      </button>
                    </div>

                    {authTotpEnabled && (
                      <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 8 }}>
                        <div style={{ fontSize: 12, opacity: 0.9 }}>
                          2FA is enabled. To reset (rotate) your 2FA secret, confirm your password and a current 2FA code.
                        </div>
                        <input
                          style={donateInputStyle}
                          type="password"
                          value={profile2faResetPw}
                          onChange={(e) => setProfile2faResetPw(e.target.value)}
                          placeholder="Password (required to reset 2FA)"
                        />
                        <input
                          style={donateInputStyle}
                          value={profile2faResetCode}
                          onChange={(e) => setProfile2faResetCode(e.target.value)}
                          placeholder="Current 2FA code"
                        />
                      </div>
                    )}

                    {!!profile2faMsg && (
                      <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-link, #9ad)" }}>
                        {profile2faMsg}
                      </div>
                    )}

                    {!authTotpEnabled && !!profile2faSecret && (
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <input
                          style={donateInputStyle}
                          value={profile2faSecret}
                          readOnly
                          onFocus={(e) => e.target.select()}
                          title="2FA secret (copy to authenticator if not using QR)"
                        />
                        <button
                          type="button"
                          style={donateSmallBtnStyle}
                          onClick={async () => {
                            const ok = await copyText(profile2faSecret);
                            setProfile2faMsg(ok ? "Secret copied." : "Copy failed.");
                          }}
                        >
                          Copy
                        </button>
                      </div>
                    )}

                    {!authTotpEnabled && !!profile2faOtpAuth && (
                      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <input
                            style={donateInputStyle}
                            value={profile2faOtpAuth}
                            readOnly
                            onFocus={(e) => e.target.select()}
                            title="otpauth URL (QR source)"
                          />
                          <button
                            type="button"
                            style={donateSmallBtnStyle}
                            onClick={async () => {
                              const ok = await copyText(profile2faOtpAuth);
                              setProfile2faMsg(ok ? "otpauth URL copied." : "Copy failed.");
                            }}
                          >
                            Copy
                          </button>

                          <button
                            type="button"
                            style={donateSmallBtnStyle}
                            onClick={() => setProfile2faShowQr((v) => !v)}
                            title="Toggle QR visibility (local-only)"
                          >
                            {profile2faShowQr ? "Hide QR" : "Show QR"}
                          </button>
                        </div>

                        {profile2faShowQr && (
                          <>
                            <div style={{ display: "flex", justifyContent: "center" }}>
                              {profile2faQrSvg ? (
                                <div
                                  style={{ width: 260, maxWidth: "100%" }}
                                  dangerouslySetInnerHTML={{ __html: profile2faQrSvg }}
                                />
                              ) : (
                                <div style={{ fontSize: 12, opacity: 0.7 }}>Generating QR…</div>
                              )}
                            </div>
                            <div style={{ fontSize: 11, opacity: 0.7, textAlign: "center", lineHeight: 1.25 }}>
                              Scan this QR with Google Authenticator / 1Password / Authy, then enter the 6-digit code and click <b>Enable</b>.
                            </div>
                          </>
                        )}
                      </div>
                    )}

                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <input
                        style={donateInputStyle}
                        value={profile2faCode}
                        onChange={(e) => setProfile2faCode(e.target.value)}
                        placeholder="6-digit code"
                        inputMode="numeric"
                        title="Enter current 2FA code to enable"
                      />
                      <button
                        type="button"
                        style={donateSmallBtnStyle}
                        disabled={profile2faBusy}
                        onClick={enableProfile2fa}
                        title="Enable 2FA after scanning/copying the secret"
                      >
                        Enable
                      </button>
                    </div>

                    <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.25 }}>
                      Tip: Setup/Reset generates a secret, but 2FA is enforced only after you click <b>Enable</b> with a valid code.
                    </div>
                  </div>
                </div>
              </div>
              <div style={{ height: 1, background: "rgba(255,255,255,0.08)" }} />
              <div>
                <div style={{ fontWeight: 800, marginBottom: 6 }}>API Keys (write-only)</div>
                <div style={{ fontSize: 13, opacity: 0.85, marginBottom: 10 }}>
                  Keys entered here will be encrypted at rest and will <b>never</b> be displayed again in the UI. Save your secrets before submitting.
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "150px 1fr", gap: 8, alignItems: "center" }}>
                  <div style={{ fontSize: 12, opacity: 0.75 }}>Venue</div>
                  <input
                    value={profileKeyVenue}
                    onChange={(e) => setProfileKeyVenue(e.target.value)}
                    placeholder="coinbase / kraken / solana_dex / etc"
                    style={textInputStyle}
                  />

                  <div style={{ fontSize: 12, opacity: 0.75 }}>Label (optional)</div>
                  <input
                    value={profileKeyLabel}
                    onChange={(e) => setProfileKeyLabel(e.target.value)}
                    placeholder="e.g. trading / read-only"
                    style={textInputStyle}
                  />

                  <div style={{ fontSize: 12, opacity: 0.75 }}>API key</div>
                  <input
                    value={profileKeyApiKey}
                    onChange={(e) => setProfileKeyApiKey(e.target.value)}
                    placeholder="paste key"
                    style={textInputStyle}
                  />

                  <div style={{ fontSize: 12, opacity: 0.75 }}>API secret</div>
                  <input
                    value={profileKeyApiSecret}
                    onChange={(e) => setProfileKeyApiSecret(e.target.value)}
                    placeholder="paste secret"
                    style={textInputStyle}
                  />

                  <div style={{ fontSize: 12, opacity: 0.75 }}>Passphrase</div>
                  <input
                    value={profileKeyPassphrase}
                    onChange={(e) => setProfileKeyPassphrase(e.target.value)}
                    placeholder="(if required by venue)"
                    style={textInputStyle}
                  />

                  {authTotpEnabled && (
                    <>
                      <div style={{ fontSize: 12, opacity: 0.75 }}>2FA code (required)</div>
                      <input
                        value={profileKeyTotp}
                        onChange={(e) => setProfileKeyTotp(e.target.value)}
                        placeholder="6-digit code"
                        style={textInputStyle}
                      />
                    </>
                  )}
                </div>

                <div style={{ display: "flex", gap: 10, marginTop: 10, alignItems: "center" }}>
                  <button
                    type="button"
                    onClick={callProfileApiKeysSave}
                    disabled={profileKeysBusy}
                    style={{ ...softBtnStyle, opacity: profileKeysBusy ? 0.6 : 1 }}
                  >
                    Save key
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setProfileKeyVenue("");
                      setProfileKeyLabel("");
                      setProfileKeyApiKey("");
                      setProfileKeyApiSecret("");
                      setProfileKeyPassphrase("");
                      setProfileKeyTotp("");
                      setProfileKeysMsg("");
                    }}
                    disabled={profileKeysBusy}
                    style={{ ...softBtnStyle, opacity: profileKeysBusy ? 0.6 : 1 }}
                  >
                    Clear
                  </button>

                  <div style={{ marginLeft: "auto", fontSize: 12, opacity: 0.8 }}>
                    {profileKeysMsg ? profileKeysMsg : profileKeysBusy ? "Working..." : ""}
                  </div>
                </div>

                <div style={{ height: 1, background: "rgba(255,255,255,0.08)", margin: "12px 0" }} />

                <div style={{ fontSize: 12, opacity: 0.75, marginBottom: 6 }}>Saved keys (metadata only)</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {Array.isArray(profileKeysItems) && profileKeysItems.length > 0 ? (
                    profileKeysItems.map((k) => (
                      <div
                        key={k.id}
                        style={{
                          display: "grid",
                          gridTemplateColumns: "1fr auto",
                          gap: 10,
                          alignItems: "center",
                          padding: "8px 10px",
                          borderRadius: 10,
                          border: "1px solid rgba(255,255,255,0.10)",
                          background: "rgba(255,255,255,0.03)",
                        }}
                      >
                        <div style={{ minWidth: 0 }}>
                          <div style={{ fontWeight: 700, fontSize: 13, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                            {k.venue}{k.label ? ` — ${k.label}` : ""}
                          </div>
                          <div style={{ fontSize: 12, opacity: 0.75 }}>
                            {k.hint ? `key: ${k.hint}` : "key: (hidden)"} {k.updated_at ? ` • updated: ${fmtEpochSec(k.updated_at)}` : ""}
                          </div>
                        </div>
                        <button
                          type="button"
                          onClick={() => callProfileApiKeysDelete(k.id)}
                          disabled={profileKeysBusy}
                          style={{ ...softBtnStyle, opacity: profileKeysBusy ? 0.6 : 1 }}
                        >
                          Delete
                        </button>
                      </div>
                    ))
                  ) : (
                    <div style={{ fontSize: 12, opacity: 0.6 }}>No saved keys yet.</div>
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {airdropOpen && (
        <div
          ref={airdropPopRef}
          style={{
            position: "fixed",
            left: airdropPos?.x ?? 10,
            top: airdropPos?.y ?? 120,
            zIndex: 20000,
            width: airdropPos?.w ?? 460,
            maxWidth: "92vw",
            color: "var(--utt-hdr-fg, #e8eef8)",
            backgroundColor: "rgba(10, 12, 14, 0.96)",
            border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
            borderRadius: 14,
            boxShadow: "var(--utt-hdr-shadow, 0 10px 24px rgba(0,0,0,0.35))",
            overflow: "hidden",
            opacity: 1,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10, padding: "10px 12px", borderBottom: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.10))" }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 0 }}>
              <div style={{ fontWeight: 900, fontSize: 13, lineHeight: 1.1 }}>Airdrop Registration / Status</div>
              <div style={{ fontSize: 11, opacity: 0.75 }}>Register wallet with a Solana signature, then check campaign status.</div>
            </div>
            <button type="button" style={donateSmallBtnStyle} onClick={() => setAirdropOpen(false)} title="Close">Close</button>
          </div>

          <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10, maxHeight: "min(70vh, 620px)", overflowY: "auto" }}>
            {!!airdropErr && <div style={{ fontSize: 12, opacity: 0.95, color: "var(--utt-hdr-warn, var(--utt-warn, #f7b955))" }}>{airdropErr}</div>}
            {!!airdropMsg && <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-link, #9ad)" }}>{airdropMsg}</div>}

            <div style={{ display: "grid", gridTemplateColumns: "110px 1fr auto auto", gap: 8, alignItems: "center" }}>
              <div style={{ fontSize: 12, opacity: 0.75 }}>Wallet</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                <input
                  style={donateInputStyle}
                  value={airdropWalletDraft}
                  onChange={(e) => setAirdropWalletDraft(e.target.value)}
                  placeholder="Paste Solana wallet to check / register"
                  onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); callAirdropStatus(); } }}
                />
                <div style={{ fontSize: 11, opacity: 0.72, lineHeight: 1.25 }}>
                  Detected wallet: <b>{shortWalletLabel(airdropStatus?.wallet || airdropWallet || guessConnectedSolanaWallet())}</b>
                </div>
              </div>
              <button
                type="button"
                style={donateSmallBtnStyle}
                onClick={async () => {
                  const guessed = await resolveConnectedSolanaWallet();
                  if (!guessed) {
                    setAirdropErr("No already-connected Solana wallet detected. Connect the same wallet you use for swaps first.");
                    return;
                  }
                  setAirdropWallet(guessed);
                  setAirdropWalletDraft("");
                  setAirdropMsg("Connected wallet detected.");
                  window.clearTimeout(window.__utt_airdrop_msg_to);
                  window.__utt_airdrop_msg_to = window.setTimeout(() => setAirdropMsg(""), 3000);
                  await callAirdropStatus(guessed);
                }}
                title="Use connected Solana wallet"
              >
                Use connected
              </button>
              <button type="button" style={donateSmallBtnStyle} disabled={airdropBusy} onClick={() => callAirdropStatus()}>{airdropBusy ? "Checking…" : "Refresh"}</button>
            </div>

            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <button
                type="button"
                style={{
                  ...donatePrimaryBtnStyle,
                  opacity: (!airdropStatus?.campaignActive || !airdropStatus?.registrationOpen) ? 0.65 : 1,
                  cursor: (!airdropStatus?.campaignActive || !airdropStatus?.registrationOpen) ? "not-allowed" : donatePrimaryBtnStyle.cursor,
                }}
                disabled={
                  airdropRegisterBusy ||
                  airdropBusy ||
                  !String(airdropWalletDraft || airdropWallet || guessConnectedSolanaWallet() || "").trim() ||
                  !airdropStatus?.campaignActive ||
                  !airdropStatus?.registrationOpen
                }
                onClick={() => {
                  if (!airdropStatus?.campaignActive || !airdropStatus?.registrationOpen) {
                    setAirdropMsg("Official UTTT airdrop registration will open in a future release.");
                    window.clearTimeout(window.__utt_airdrop_msg_to);
                    window.__utt_airdrop_msg_to = window.setTimeout(() => setAirdropMsg(""), 4000);
                    return;
                  }
                  registerAirdropWallet();
                }}
                title={
                  (!airdropStatus?.campaignActive || !airdropStatus?.registrationOpen)
                    ? "Official UTTT airdrop registration will open in a future release"
                    : "Register this connected wallet for the campaign using a wallet signature"
                }
              >
                {(!airdropStatus?.campaignActive || !airdropStatus?.registrationOpen)
                  ? "Future release"
                  : (airdropRegisterBusy ? "Registering…" : "Register wallet")}
              </button>

              <button
                type="button"
                style={donateSmallBtnStyle}
                disabled={!String(airdropStatus?.wallet || airdropWallet || guessConnectedSolanaWallet() || "").trim()}
                onClick={async () => {
                  const ok = await copyText(String(airdropStatus?.wallet || airdropWallet || guessConnectedSolanaWallet() || "").trim());
                  setAirdropMsg(ok ? "Wallet copied." : "Copy failed.");
                  window.clearTimeout(window.__utt_airdrop_msg_to);
                  window.__utt_airdrop_msg_to = window.setTimeout(() => setAirdropMsg(""), 3000);
                }}
                title="Copy current wallet"
              >
                Copy wallet
              </button>
            </div>

            {!!airdropChallenge?.nonce && (
              <div style={{ fontSize: 11, opacity: 0.75, lineHeight: 1.35 }}>
                Challenge issued for <b>{String(airdropChallenge.wallet || "")}</b>. Wallet will sign a one-time registration message for campaign <b>{String(airdropChallenge.campaignId || "")}</b>.
              </div>
            )}

            <div style={{ display: "grid", gridTemplateColumns: "150px 1fr", gap: 8, alignItems: "center" }}>
              <div style={{ fontSize: 12, opacity: 0.75 }}>Campaign</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{String(airdropStatus?.campaignName || airdropStatus?.campaignId || "UTTT Airdrop")}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Campaign active</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{airdropStatus?.campaignActive ? "Yes" : "No"}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Registration open</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{airdropStatus?.registrationOpen ? "Yes" : "No"}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Connected</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{(airdropStatus?.connected || !!String(airdropWallet || guessConnectedSolanaWallet() || "").trim()) ? "Yes" : "No"}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Wallet ID</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{shortWalletLabel(airdropStatus?.wallet || airdropWallet || guessConnectedSolanaWallet())}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Registered</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{airdropStatus?.registered ? `Yes${airdropStatus?.registeredAt ? ` • ${airdropStatus.registeredAt}` : ""}` : "No"}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Eligibility</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>
                {!airdropStatus?.campaignActive
                  ? "Future release"
                  : (airdropStatus?.eligible ? "Yes" : "No")}
              </div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Claimable</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{String(airdropStatus?.claimableAmount ?? "0")} {String(airdropStatus?.tokenSymbol || "UTTT")}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Claimed</div>
              <div style={{ fontSize: 13, fontWeight: 700 }}>{airdropStatus?.claimed ? `Yes${airdropStatus?.claimedAt ? ` • ${airdropStatus.claimedAt}` : ""}` : "No"}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Reason</div>
              <div style={{ fontSize: 13, lineHeight: 1.35 }}>
                {!airdropStatus?.campaignActive
                  ? "Official UTTT airdrop registration is not active in this build yet."
                  : String(airdropStatus?.reason || "Connect a Solana wallet and register to begin.")}
              </div>
            </div>

            <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.4 }}>
              {!airdropStatus?.campaignActive ? (
                <>Official UTTT airdrop registration will be enabled in a future release. This build keeps the wallet-check/status shell visible, but eligibility is not finalized here.</>
              ) : (
                <>Registration requires a Solana wallet signature and is limited to one wallet per campaign. In this first pass, a wallet only becomes eligible once it is both <b>registered</b> and present in the private operator snapshot.</>
              )}
            </div>
          </div>
        </div>
      )}

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

      <audio ref={mediaAudioRef} preload="none" style={{ display: "none" }} />

      {mediaOpen && (
        <div
          ref={mediaPopRef}
          style={{
            position: "fixed",
            left: mediaPos?.x ?? 10,
            top: mediaPos?.y ?? 120,
            zIndex: 20000,
            width: mediaPos?.w ?? 460,
            maxWidth: "92vw",
            color: "var(--utt-hdr-fg, #e8eef8)",
            backgroundColor: "rgba(10, 12, 14, 0.96)",
            border: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.12))",
            borderRadius: 14,
            boxShadow: "var(--utt-hdr-shadow, 0 10px 24px rgba(0,0,0,0.35))",
            overflow: "hidden",
            opacity: 1,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10, padding: "10px 12px", borderBottom: "1px solid var(--utt-hdr-border, rgba(255,255,255,0.10))" }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 0 }}>
              <div style={{ fontWeight: 900, fontSize: 13, lineHeight: 1.1 }}>Media Player</div>
              <div style={{ fontSize: 11, opacity: 0.75 }}>Load a local media file or enter a direct stream / playlist URL.</div>
            </div>
            <button type="button" style={donateSmallBtnStyle} onClick={() => setMediaOpen(false)} title="Close">Close</button>
          </div>

          <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10, maxHeight: "min(70vh, 620px)", overflowY: "auto" }}>
            {!!mediaMsg && <div style={{ fontSize: 12, opacity: 0.9, color: "var(--utt-hdr-link, #9ad)" }}>{mediaMsg}</div>}

            <div style={{ display: "grid", gridTemplateColumns: "80px 1fr auto", gap: 8, alignItems: "center" }}>
              <div style={{ fontSize: 12, opacity: 0.75 }}>Source</div>
              <input
                style={donateInputStyle}
                value={mediaUrlInput}
                onChange={(e) => setMediaUrlInput(e.target.value)}
                placeholder="Enter direct stream URL or .pls playlist URL"
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    loadMediaSource(mediaUrlInput, { autoPlay: true });
                  }
                }}
              />
              <button
                type="button"
                style={donatePrimaryBtnStyle}
                disabled={mediaBusy || !String(mediaUrlInput || "").trim()}
                onClick={() => loadMediaSource(mediaUrlInput, { autoPlay: true })}
                title="Load and play URL"
              >
                {mediaBusy ? "Loading…" : "Load"}
              </button>
            </div>

            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
              <input
                ref={mediaFileRef}
                type="file"
                accept="audio/*,video/*,.pls,.m3u,.m3u8"
                style={{ display: "none" }}
                onChange={async (e) => {
                  const f = e.target?.files?.[0] || null;
                  e.target.value = "";
                  if (f) {
                    if (/\.pls$/i.test(String(f.name || ""))) {
                      try {
                        const txt = await f.text();
                        const m = txt.match(/^File1=(.+)$/im);
                        const resolved = String(m?.[1] || "").trim();
                        if (!resolved) throw new Error("Could not parse stream URL from local .pls");
                        setMediaUrlInput(resolved);
                        await loadMediaSource(resolved, { autoPlay: true });
                      } catch (err) {
                        mediaSetMessage(`Playlist load failed: ${String(err?.message || err)}`, 4000);
                      }
                    } else {
                      await handleMediaFilePicked(f);
                    }
                  }
                }}
              />

              <button
                type="button"
                style={donateSmallBtnStyle}
                onClick={() => mediaFileRef.current?.click?.()}
                title="Load local audio/video file"
              >
                Load file
              </button>

              <button
                type="button"
                style={donateSmallBtnStyle}
                disabled={!mediaResolvedUrl}
                onClick={async () => {
                  const audio = mediaAudioRef.current;
                  if (!audio) return;
                  try {
                    if (audio.paused) await audio.play();
                    else audio.pause();
                  } catch (e) {
                    mediaSetMessage(`Playback failed: ${String(e?.message || e)}`, 4000);
                  }
                }}
                title={mediaIsPlaying ? "Pause" : "Play"}
              >
                {mediaIsPlaying ? "Pause" : "Play"}
              </button>

              <button
                type="button"
                style={donateSmallBtnStyle}
                disabled={!mediaResolvedUrl}
                onClick={() => {
                  const audio = mediaAudioRef.current;
                  if (!audio) return;
                  try {
                    audio.pause();
                    audio.currentTime = 0;
                    setMediaIsPlaying(false);
                  } catch {
                    // ignore
                  }
                }}
                title="Stop"
              >
                Stop
              </button>

              <button
                type="button"
                style={donateSmallBtnStyle}
                disabled={!mediaResolvedUrl}
                onClick={async () => {
                  const s = String(mediaResolvedUrl || mediaSource || "").trim();
                  if (!s) return;
                  const ok = await copyText(s);
                  mediaSetMessage(ok ? "Media source copied." : "Copy failed.");
                }}
                title="Copy current media source"
              >
                Copy source
              </button>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "80px 1fr auto", gap: 8, alignItems: "center" }}>
              <div style={{ fontSize: 12, opacity: 0.75 }}>Volume</div>
              <input
                type="range"
                min="0"
                max="1"
                step="0.01"
                value={Number(mediaVolume)}
                onChange={(e) => setMediaVolume(Number(e.target.value))}
              />
              <div style={{ fontSize: 12, opacity: 0.85, minWidth: 36, textAlign: "right" }}>{Math.round(Number(mediaVolume) * 100)}%</div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "110px 1fr", gap: 8, alignItems: "center" }}>
              <div style={{ fontSize: 12, opacity: 0.75 }}>Now Playing</div>
              <div style={{ fontSize: 13, fontWeight: 700, wordBreak: "break-word" }}>{String(mediaNowPlaying || "—")}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Resolved URL</div>
              <div style={{ fontSize: 12, opacity: 0.88, wordBreak: "break-all" }}>{String(mediaResolvedUrl || "—")}</div>

              <div style={{ fontSize: 12, opacity: 0.75 }}>Status</div>
              <div style={{ fontSize: 12, opacity: 0.9 }}>{mediaIsPlaying ? "Playing" : (mediaResolvedUrl ? "Loaded" : "Idle")}</div>
            </div>

            <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.4 }}>
              Browser media can play direct audio/video URLs and many stream endpoints. “Now Playing” reliably shows the local file name or resolved source label. Live stream track metadata depends on what the remote server exposes and may not always be available due to stream/CORS limits.
            </div>
          </div>
        </div>
      )}

      {venueMgrOpen && (
        <div
          ref={venueMgrPopRef}
          style={{
            position: "fixed",
            left: venueMgrPos?.x ?? 10,
            top: venueMgrPos?.y ?? 80,
            zIndex: 50,
            width: venueMgrPos?.w ?? 360,
            maxWidth: "calc(100vw - 24px)",
            border: "1px solid var(--utt-border-1, rgba(255,255,255,0.14))",
            background: "var(--utt-surface-2, #151515)",
            borderRadius: 12,
            padding: 10,
            boxShadow: "var(--utt-shadow, 0 10px 24px rgba(0,0,0,0.40))",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10, marginBottom: 8 }}>
            <div style={{ fontWeight: 900, fontSize: 13 }}>Venues</div>
            <button type="button" style={{ ...headerStyles.button, padding: "6px 8px" }} onClick={() => setVenueMgrOpen(false)}>
              Close
            </button>
          </div>

          <div style={{ fontSize: 12, opacity: 0.75, marginBottom: 8 }}>
            These toggles are a <b>UI-local override</b> (stored in your browser). Backend env stays unchanged.
          </div>

          <div style={{ maxHeight: 340, overflow: "auto", paddingRight: 6 }}>
            {(venueMgrRows || []).map((r) => {
              const disabledByBackend = r.backendEnabled === false && (venueOverrides == null || !Object.prototype.hasOwnProperty.call(venueOverrides, r.id));
              const checked = !!r.enabled;

              return (
                <label
                  key={r.id}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    gap: 10,
                    padding: "6px 6px",
                    borderRadius: 10,
                    cursor: "pointer",
                    border: "1px solid transparent",
                  }}
                  title={disabledByBackend ? "Disabled by backend registry (env). Enabling here is a UI override." : ""}
                >
                  <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                    <div style={{ fontSize: 12, fontWeight: 850 }}>{r.label}</div>
                    <div style={{ fontSize: 11, opacity: 0.65 }}>{r.id}</div>
                  </div>

                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => {
                      const next = !!e.target.checked;
                      if (typeof setVenueOverride === "function") setVenueOverride(r.id, next);
                      if (!next && normalizeVenue(venue) === normalizeVenue(r.id)) {
                        try {
                          setVenue(ALL_VENUES_VALUE);
                        } catch {
                          // ignore
                        }
                      }
                    }}
                  />
                </label>
              );
            })}
          </div>

          <div style={{ display: "flex", gap: 8, marginTop: 10, justifyContent: "flex-end" }}>
            <button
              type="button"
              style={{ ...headerStyles.button, padding: "6px 8px" }}
              onClick={() => {
                try {
                  localStorage.removeItem("utt_venue_overrides_v1");
                } catch {
                  // ignore
                }
                try {
                  if (typeof setVenueOverride === "function") {
                    // best-effort: clear by forcing a reload; App.jsx will re-init overrides state on reload.
                    window.location.reload();
                  }
                } catch {
                  // ignore
                }
              }}
              title="Clear local overrides (reloads the page)"
            >
              Reset overrides
            </button>
          </div>
        </div>
      )}


      <div style={headerStyles.toolbar}>
        <div style={headerStyles.pill}>
          <span>Venue</span>
          <select
            style={{
              ...headerStyles.select,
              color: "var(--utt-hdr-fg, #f2f2f2)",
              background: "var(--utt-hdr-ctl-bg, #111)",
            }}
            value={venue}
            onChange={(e) => setVenue(e.target.value)}
          >
            <option
              value={ALL_VENUES_VALUE}
              style={{ backgroundColor: "#111", color: "#f2f2f2" }}
            >
              {labelVenueOption(ALL_VENUES_VALUE)}
            </option>
            {(supportedVenues || []).map((v) => (
              <option
                key={v}
                value={v}
                style={{ backgroundColor: "#111", color: "#f2f2f2" }}
              >
                {labelVenueOption(v)}
              </option>
            ))}
          </select>
          <button
            ref={venueMgrBtnRef}
            type="button"
            style={{ ...headerStyles.button, padding: "6px 8px" }}
            onClick={() => {
              const next = !venueMgrOpen;
              setVenueMgrOpen(next);
              if (next) setTimeout(() => placeVenueMgrNearButton(), 0);
            }}
            title="Enable/disable venues (local UI override)"
          >
            Manage
          </button>
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
          <button style={btnHeader(!marketInput.trim())} disabled={!marketInput.trim()} onClick={() => applyMarketSymbol()}>
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
