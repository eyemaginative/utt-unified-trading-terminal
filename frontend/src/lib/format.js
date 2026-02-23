// frontend/src/lib/format.js

/**
 * Format helpers.
 *
 * Goals:
 * - Never show scientific notation in UI (e.g., "5.4e-8" => "0.000000054")
 * - Allow per-column precision control
 * - Trim trailing zeros by default to avoid noisy decimals
 *
 * NOTE: Even with a perfect formatter, HTML inputs will still display scientific notation
 * if you pass a Number into <input value={...}> (JS Number -> toString()).
 * For OrderTicketWidget, keep price input state as a STRING and use expandExponential()
 * (or fmtPriceInput()) for display.
 */

function isNil(v) {
  return v === null || v === undefined;
}

function normalizeNumString(input) {
  if (isNil(input)) return "";
  if (typeof input === "number") {
    // Avoid number.toString() scientific display for small numbers by using a high-precision fixed,
    // then trimming later. 20 is safe-ish for most UI price/qty use.
    if (!Number.isFinite(input)) return "";
    const abs = Math.abs(input);
    if (abs !== 0 && (abs < 1e-6 || abs >= 1e21)) {
      // May become scientific, but we'll expand it below.
      return String(input);
    }
    return input.toFixed(20); // trimmed later
  }
  return String(input).trim();
}

/**
 * Expand scientific notation string to a plain decimal string.
 * Examples:
 *  "5.4e-8"  -> "0.000000054"
 *  "-1.2E+3" -> "-1200"
 *
 * Exported alias: expandExponential()
 */
function expandScientific(s) {
  const str = String(s || "").trim();
  if (!str) return "";

  if (!/[eE]/.test(str)) return str;

  const m = str.match(/^([+-]?)(\d+(?:\.\d+)?)[eE]([+-]?\d+)$/);
  if (!m) return str;

  const sign = m[1] || "";
  const base = m[2];
  const exp = parseInt(m[3], 10);
  if (!Number.isFinite(exp)) return str;

  const parts = base.split(".");
  const intPart = parts[0] || "0";
  const fracPart = parts[1] || "";
  const digits = (intPart + fracPart).replace(/^0+(?=\d)/, ""); // keep at least one digit
  const digitsSafe = digits.length ? digits : "0";

  const decPos0 = intPart.length; // decimal position in the concatenated digits
  const decPos = decPos0 + exp;

  if (decPos <= 0) {
    const zeros = "0".repeat(Math.abs(decPos));
    return `${sign}0.${zeros}${digitsSafe}`;
  }

  if (decPos >= digitsSafe.length) {
    const zeros = "0".repeat(decPos - digitsSafe.length);
    return `${sign}${digitsSafe}${zeros}`;
  }

  const left = digitsSafe.slice(0, decPos);
  const right = digitsSafe.slice(decPos);
  return `${sign}${left}.${right}`;
}

function stripLeadingPlus(s) {
  return s && s[0] === "+" ? s.slice(1) : s;
}

function trimTrailingZeros(s) {
  const str = String(s);
  if (!str.includes(".")) return str;
  const neg = str[0] === "-";
  const core = neg ? str.slice(1) : str;

  const [a, bRaw] = core.split(".");
  const b = (bRaw || "").replace(/0+$/, "");
  const out = b.length ? `${a}.${b}` : a;
  return neg ? `-${out}` : out;
}

function ensureMinDecimals(s, minDecimals) {
  if (!minDecimals || minDecimals <= 0) return s;
  const str = String(s);
  const neg = str[0] === "-";
  const core = neg ? str.slice(1) : str;

  const parts = core.split(".");
  const a = parts[0] || "0";
  const b = parts[1] || "";
  if (b.length >= minDecimals) return str;

  const pad = "0".repeat(minDecimals - b.length);
  const out = `${a}.${b}${pad}`;
  return neg ? `-${out}` : out;
}

/**
 * Round a plain decimal string (no scientific) to maxDecimals.
 * String-safe rounding with carry. Handles negative numbers.
 */
function roundDecimalString(s, maxDecimals) {
  const str0 = String(s || "").trim();
  if (!str0) return "";

  const neg = str0[0] === "-";
  const str = neg ? str0.slice(1) : stripLeadingPlus(str0);

  if (maxDecimals === null || maxDecimals === undefined) return (neg ? "-" : "") + str;
  const md = Math.max(0, Math.floor(Number(maxDecimals)));

  const parts = str.split(".");
  const a = parts[0] || "0";
  const b = parts[1] || "";

  if (md === 0) {
    if (!b.length) return (neg ? "-" : "") + a;
    const first = b[0];
    if (first < "5") return (neg ? "-" : "") + a;

    const arr = a.split("").map((ch) => ch.charCodeAt(0) - 48);
    let i = arr.length - 1;
    let carry = 1;
    while (i >= 0 && carry) {
      const v = arr[i] + carry;
      arr[i] = v % 10;
      carry = v >= 10 ? 1 : 0;
      i -= 1;
    }
    const outInt = (carry ? "1" : "") + arr.join("");
    return (neg ? "-" : "") + outInt.replace(/^0+(?=\d)/, "");
  }

  if (b.length <= md) {
    const out = b.length ? `${a}.${b}` : a;
    return (neg ? "-" : "") + out.replace(/^0+(?=\d)/, "");
  }

  const keep = b.slice(0, md);
  const nextDigit = b[md];

  if (nextDigit < "5") {
    const out = `${a}.${keep}`;
    return (neg ? "-" : "") + out.replace(/^0+(?=\d)/, "");
  }

  const fracArr = keep.split("").map((ch) => ch.charCodeAt(0) - 48);
  let i = fracArr.length - 1;
  let carry = 1;
  while (i >= 0 && carry) {
    const v = fracArr[i] + carry;
    fracArr[i] = v % 10;
    carry = v >= 10 ? 1 : 0;
    i -= 1;
  }

  let intArr = a.split("").map((ch) => ch.charCodeAt(0) - 48);
  if (carry) {
    let j = intArr.length - 1;
    let c = 1;
    while (j >= 0 && c) {
      const v = intArr[j] + c;
      intArr[j] = v % 10;
      c = v >= 10 ? 1 : 0;
      j -= 1;
    }
    if (c) intArr = [1, ...intArr];
  }

  const out = `${intArr.join("").replace(/^0+(?=\d)/, "")}.${fracArr.join("")}`;
  return (neg ? "-" : "") + out;
}

/**
 * Exported: Expand scientific/exponential notation into plain decimals.
 * This is the helper you should use for OrderTicketWidget input display.
 */
export function expandExponential(value) {
  if (isNil(value)) return "";
  const s = String(value).trim();
  if (!s) return "";
  return expandScientific(s);
}

/**
 * Exported: For input fields (price/qty) where you want:
 * - no scientific notation
 * - optional decimal clamp
 * - NO trimming by default (inputs should preserve user intent)
 *
 * Use this for displaying an input value that may include "e-".
 */
export function fmtNumInput(value, { maxDecimals = null, trim = false, minDecimals = 0 } = {}) {
  if (isNil(value)) return "";

  // 1) normalize and expand sci
  let s = normalizeNumString(value);
  if (!s) return "";
  s = expandScientific(s);
  s = s.replace(/,/g, "").trim();

  if (!s || s === "-" || s === "+") return "";

  // allow partial input states (e.g., "0.", ".5")? we keep conservative:
  // For display, we only coerce if it is numeric-ish.
  // If it's not numeric-ish, return raw trimmed to avoid breaking typing.
  if (!/^[-+]?\d*(\.\d*)?$/.test(s)) return String(value);

  // 2) clamp (round) if requested and value is a complete numeric like 123 or 123.45
  const isComplete = /^[-+]?\d+(\.\d+)?$/.test(s);
  if (isComplete && maxDecimals !== null && maxDecimals !== undefined) {
    s = roundDecimalString(s, maxDecimals);
  }

  // 3) trimming is off by default for inputs
  if (trim) s = trimTrailingZeros(s);
  s = ensureMinDecimals(s, minDecimals);

  return s;
}

/**
 * Primary numeric formatter used across the app.
 *
 * Back-compat signatures:
 * - fmtNum(value, 10)
 * - fmtNum(value, { maxDecimals: 10, minDecimals: 0, trim: true, grouping: false })
 */
export function fmtNum(value, optsOrMaxDecimals = 8) {
  if (isNil(value)) return "—";

  // Normalize args
  let opts = {};
  if (typeof optsOrMaxDecimals === "number") {
    opts = { maxDecimals: optsOrMaxDecimals };
  } else if (typeof optsOrMaxDecimals === "object" && optsOrMaxDecimals) {
    opts = { ...optsOrMaxDecimals };
  } else {
    opts = { maxDecimals: 8 };
  }

  const maxDecimals = opts.maxDecimals ?? 8;
  const minDecimals = opts.minDecimals ?? 0;
  const trim = opts.trim !== false; // default true
  const grouping = opts.grouping === true; // default false

  let s = normalizeNumString(value);
  if (!s) return "—";

  s = expandScientific(s);

  s = s.replace(/,/g, "").trim();
  if (!s || s === "-" || s === "+") return "—";

  if (!/^[-+]?\d+(\.\d+)?$/.test(s)) {
    const n = Number(s);
    if (!Number.isFinite(n)) return String(value);
    s = expandScientific(String(n));
  }

  s = roundDecimalString(s, maxDecimals);

  if (trim) s = trimTrailingZeros(s);
  s = ensureMinDecimals(s, minDecimals);

  if (grouping) {
    try {
      const n = Number(s);
      if (Number.isFinite(n)) {
        const dec = (s.split(".")[1] || "").length;
        return n.toLocaleString(undefined, {
          useGrouping: true,
          minimumFractionDigits: dec,
          maximumFractionDigits: dec,
        });
      }
    } catch {
      // ignore
    }
  }

  return s;
}

/**
 * Balances formatter (UI display):
 * - Never scientific notation
 * - Dynamic precision for dust
 * - Avoids noisy long decimals for typical balances
 *
 * Use this for balances tables: total/available/hold.
 */
export function fmtBal(value, { maxDecimals = 12, dustThreshold = 1e-6 } = {}) {
  if (isNil(value)) return "—";

  // Normalize to a plain decimal string (no exponent), then apply formatting rules.
  let s = normalizeNumString(value);
  if (!s) return "—";
  s = expandScientific(s);
  s = s.replace(/,/g, "").trim();
  if (!s || s === "-" || s === "+") return "—";

  // If it still isn't numeric-ish, last-chance Number parse.
  if (!/^[-+]?\d+(\.\d+)?$/.test(s)) {
    const n = Number(s);
    if (!Number.isFinite(n)) return String(value);
    s = expandScientific(String(n));
    s = s.replace(/,/g, "").trim();
  }

  const n = Number(s);
  if (!Number.isFinite(n)) return String(value);

  const abs = Math.abs(n);

  // Render dust as plain decimals with significant digits (no exponent).
  // Example: 5.7e-7 -> "0.00000057"
  if (abs !== 0 && abs < dustThreshold) {
    // Convert via Intl in standard notation, without grouping.
    // Then run through expandScientific just in case the environment still emits exponent.
    const out = new Intl.NumberFormat("en-US", {
      notation: "standard",
      useGrouping: false,
      maximumSignificantDigits: 12,
    }).format(n);

    return trimTrailingZeros(expandScientific(out));
  }

  // For normal balances, use your string-safe pipeline.
  // Keep more decimals for < 1, fewer for larger, but never exponent.
  let md = maxDecimals;
  if (abs >= 1) md = Math.min(8, maxDecimals);
  if (abs >= 1000) md = Math.min(4, maxDecimals);

  s = roundDecimalString(s, md);
  s = trimTrailingZeros(s);

  return s;
}

/** Convenience: quantities (keep precision, trim zeros, no sci) */
export function fmtQty(value) {
  return fmtNum(value, { maxDecimals: 10, trim: true, grouping: false });
}

/** Convenience: prices (avoid sci, up to 10 decimals, trim zeros) */
export function fmtPrice(value) {
  return fmtNum(value, { maxDecimals: 10, trim: true, grouping: false });
}

/**
 * Decide decimals for "money-like" values that may be USD or crypto quote amounts.
 * We cannot reliably know the quote currency here, so we use magnitude-based precision.
 */
function _moneyDecimalsByMagnitude(n) {
  const abs = Math.abs(n);

  // Default for "USD-ish" values is 4 decimals (what you had),
  // but for BTC/ETH-quoted totals, we need more precision.
  if (abs === 0) return 4;

  // Small quote amounts (typical BTC totals) need more decimals.
  if (abs < 0.0001) return 10;
  if (abs < 0.01) return 8;

  // Normal “cash-like” values.
  if (abs < 1) return 6;

  // Larger values: 4 is fine (keeps UI clean).
  return 4;
}

/** Convenience: money totals (Gross/Net) */
export function fmtMoney(value) {
  if (isNil(value)) return "—";
  const n = Number(value);
  const md = Number.isFinite(n) ? _moneyDecimalsByMagnitude(n) : 8;
  return fmtNum(value, { maxDecimals: md, trim: true, grouping: false });
}

/**
 * Fees can be even smaller than gross/net (especially maker fees in quote units).
 * Use slightly higher precision for tiny values.
 */
function _feeDecimalsByMagnitude(n) {
  const abs = Math.abs(n);
  if (abs === 0) return 6;
  if (abs < 0.0001) return 12;
  if (abs < 0.01) return 10;
  if (abs < 1) return 8;
  return 6;
}

/** Convenience: fees */
export function fmtFee(value) {
  if (isNil(value)) return "—";
  const n = Number(value);
  const md = Number.isFinite(n) ? _feeDecimalsByMagnitude(n) : 10;
  return fmtNum(value, { maxDecimals: md, trim: true, grouping: false });
}

/** Time formatting used in tables */
export function fmtTime(ts) {
  if (!ts) return "—";
  try {
    const d = typeof ts === "string" || typeof ts === "number" ? new Date(ts) : ts;
    if (!(d instanceof Date) || isNaN(d.getTime())) return String(ts);
    return d.toISOString().replace("T", " ").replace("Z", "Z");
  } catch {
    return String(ts);
  }
}

/** Order cancelability helper used in App.jsx/TerminalTablesWidget */
export function isCancelableStatus(st) {
  const s = String(st || "").trim().toLowerCase();
  if (!s) return false;
  return (
    s === "open" ||
    s === "acked" ||
    s === "pending" ||
    s === "new" ||
    s === "live" ||
    s === "active" ||
    s === "working" ||
    s.includes("await") ||
    s.includes("queue") ||
    s.includes("accept") ||
    s.includes("partial")
  );
}
