// frontend/src/utils/orders.js

export function isTerminalBucket(bucket) {
  return String(bucket || "").toLowerCase() === "terminal";
}

export function isTerminalStatus(status) {
  const s = String(status || "").toLowerCase();
  return s === "filled" || s === "canceled" || s === "cancelled" || s === "rejected" || s === "done" || s === "closed";
}

export function isCanceledStatus(status) {
  const s = String(status || "").toLowerCase();
  return s === "canceled" || s === "cancelled";
}

// unified hide predicate (for Local + Unified tables)
export function isHiddenByHideCancelled(status) {
  const s = String(status || "").toLowerCase();
  return s === "canceled" || s === "cancelled";
}

// All Orders scope model (Design A)
export function normalizeScope(v) {
  const s = String(v || "").trim().toUpperCase();
  if (s === "LOCAL") return "LOCAL";
  if (s === "VENUES") return "VENUES";
  return ""; // ALL
}

export function parseMaybeTime(v) {
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

export function sortValueForField(o, field) {
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

export function compareBySort(a, b, field, dir) {
  const dAsc = String(dir || "desc").toLowerCase() === "asc";
  const d = dAsc ? 1 : -1;

  const av = sortValueForField(a, field);
  const bv = sortValueForField(b, field);

  const aMissing = av === null || av === undefined || av === "";
  const bMissing = bv === null || bv === undefined || bv === "";

  if (aMissing && bMissing) {
    const at = parseMaybeTime(a?.created_at) ?? 0;
    const bt = parseMaybeTime(b?.created_at) ?? 0;
    if (at !== bt) return (at - bt) * -1; // tie-break: created_at DESC
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
