// frontend/src/utils/venues.js

export function normalizeVenue(v) {
  return String(v || "").trim().toLowerCase();
}

export function normalizeSymbolCanon(s) {
  const sym = String(s || "").trim().toUpperCase();
  return sym;
}

export function normalizeVenueList(input) {
  // Accepts many shapes:
  // - ["gemini","kraken"]
  // - { venues: ["gemini", ...] }
  // - { venues: [{ venue: "gemini", enabled: true }, ...] }
  // - [{ venue: "gemini" }, { id: "coinbase" }]
  // Returns: unique, lowercased venue strings.
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
    const v = String(pick(x) ?? "").trim().toLowerCase();
    if (!v) continue;
    if (v === "[object object]") continue;
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

// Local cache key: `${venue}|${symbolCanon}`
export function discoveryKey(venue, symbolCanon) {
  const v = String(venue || "").trim().toLowerCase();
  const s = String(symbolCanon || "").trim().toUpperCase();
  if (!v || !s) return "";
  return `${v}|${s}`;
}

// Server view_key: `{venue}:{symbol_canon}`
export function discoveryViewKey(venue, symbolCanon) {
  const v = String(venue || "").trim().toLowerCase();
  const s = String(symbolCanon || "").trim().toUpperCase();
  if (!v || !s) return "";
  return `${v}:${s}`;
}

// Utility: best-effort titlecase fallback for unknown venues
export function prettyVenueName(v) {
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

// Converts many possible backend response shapes into: [{ venue, symbolCanon }]
export function normalizeVenueMarketsResponse(v, asset, res) {
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
