// frontend/src/utils/formatters.js

export function fmtEco(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 8 });
}

export function fmtUsd(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export function fmtPxUsd(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 8 });
}

// Price formatter for ArbChip (kept separate so App.jsx can stay slim)
export function fmtPrice(n) {
  if (n === null || n === undefined) return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toLocaleString(undefined, { maximumFractionDigits: 10 });
}

export function calcGrossTotal(o) {
  const fq = Number(o?.filled_qty);
  const ap = Number(o?.avg_fill_price);
  if (Number.isFinite(fq) && Number.isFinite(ap) && fq > 0 && ap > 0) return fq * ap;

  const q = Number(o?.qty);
  const lp = Number(o?.limit_price);
  if (Number.isFinite(q) && Number.isFinite(lp) && q > 0 && lp > 0) return q * lp;

  return null;
}

export function calcFee(o) {
  const fee = Number(o?.fee);
  return Number.isFinite(fee) ? fee : null;
}

export function calcNetTotal(o) {
  const taf = Number(o?.total_after_fee);
  if (Number.isFinite(taf)) return taf;

  const gross = calcGrossTotal(o);
  const fee = calcFee(o);
  if (Number.isFinite(gross) && Number.isFinite(fee)) return gross - fee;

  return null;
}
