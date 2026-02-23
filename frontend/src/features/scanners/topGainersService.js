// frontend/src/features/scanners/topGainersService.js
import { sharedFetchJSON } from "../../lib/sharedFetch";

function clampSeconds(n, fallback = 300) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(10, Math.floor(x));
}

function trimApiBase(apiBase) {
  const s = String(apiBase || "").trim();
  return s.replace(/\/+$/, "");
}

function toNum(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
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

function symbolFromAsset(asset) {
  const a = String(asset || "").trim().toUpperCase();
  if (!a) return "";
  return `${a}-USD`;
}

function canonicalizeSymbol(symRaw) {
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

function pickNum(r, keys) {
  if (!r || typeof r !== "object") return null;
  for (const k of keys) {
    if (r[k] !== undefined && r[k] !== null) {
      const n = toNum(r[k]);
      if (n !== null) return n;
    }
  }
  return null;
}

function pickStr(r, keys, fallback = "") {
  if (!r || typeof r !== "object") return fallback;
  for (const k of keys) {
    const v = r[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") return String(v);
  }
  return fallback;
}

const NUM_FIELD_KEYS = {
  change_1h: [
    "change_1h",
    "change1h",
    "1h_change",
    "1hChange",
    "pct_change_1h",
    "pctChange1h",
    "percent_change_1h",
    "percentChange1h",
  ],
  change_1d: [
    "change_1d",
    "change1d",
    "1d_change",
    "1dChange",
    "change_24h",
    "change24h",
    "pct_change_24h",
    "pctChange24h",
    "percent_change_24h",
    "percentChange24h",
  ],
  change_1w: [
    "change_1w",
    "change1w",
    "1w_change",
    "1wChange",
    "change_7d",
    "change7d",
    "pct_change_7d",
    "pctChange7d",
    "percent_change_7d",
    "percentChange7d",
  ],
};

const LS_PREFIX = "utt:scanner:top_gainers";
const lsKey = (suffix) => `${LS_PREFIX}:${suffix}`;
const CACHE_KEY = lsKey("cache_v1");

function lsGet(key) {
  try {
    return window?.localStorage?.getItem(key);
  } catch {
    return null;
  }
}

function lsSet(key, val) {
  try {
    window?.localStorage?.setItem(key, String(val));
  } catch {
    // ignore
  }
}

function readBoolLS(key, fallback) {
  const v = lsGet(key);
  if (v === null || v === undefined) return fallback;
  const s = String(v).trim().toLowerCase();
  if (s === "1" || s === "true" || s === "yes" || s === "on") return true;
  if (s === "0" || s === "false" || s === "no" || s === "off") return false;
  return fallback;
}

function readIntLS(key, fallback) {
  const v = lsGet(key);
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function readJsonLS(key) {
  const s = lsGet(key);
  if (!s) return null;
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function writeJsonLS(key, obj) {
  try {
    lsSet(key, JSON.stringify(obj));
  } catch {
    // ignore
  }
}

async function fetchBalancesLatestOne(base, venueOpt, signal) {
  const params = new URLSearchParams();
  params.set("with_prices", "true");
  if (venueOpt) params.set("venue", venueOpt);
  const url = `${base}/api/balances/latest?${params.toString()}`;
  const json = await sharedFetchJSON(url, { signal, ttlMs: 1200 });
  return Array.isArray(json?.items) ? json.items : [];
}

async function fetchScannerTopGainers(base, venuesArr, signal) {
  const p = new URLSearchParams();
  p.set("limit", "250");
  const vv = (Array.isArray(venuesArr) ? venuesArr : []).map(normalizeVenue).filter(Boolean);
  if (vv.length) for (const v of vv) p.append("venues", v);
  const url = `${base}/api/scanners/top_gainers?${p.toString()}`;
  const json = await sharedFetchJSON(url, { signal, ttlMs: 1200 });
  return Array.isArray(json?.items) ? json.items : [];
}

function buildRowsFromBalancesItems(items, allowedVenuesSet) {
  const out = [];
  const heldSymbols = new Set();

  const arr = Array.isArray(items) ? items : [];
  for (const b of arr) {
    if (!b || typeof b !== "object") continue;

    const v = normalizeVenue(b.venue);
    if (allowedVenuesSet && allowedVenuesSet.size > 0) {
      if (!v || !allowedVenuesSet.has(v)) continue;
    }

    const asset = String(b.asset || "").trim().toUpperCase();
    if (!asset || asset === "USD") continue;

    const qty = toNum(b.total) ?? 0;
    if (!qty || Math.abs(qty) <= 0) continue;

    const sym = symbolFromAsset(asset);
    if (!sym) continue;

    heldSymbols.add(sym);

    out.push({
      venue: v,
      asset,
      symbol: sym,
      total: qty,
      px_usd: toNum(b.px_usd),
      total_usd: toNum(b.total_usd),
      change_1h: null,
      change_1d: null,
      change_1w: null,
    });
  }

  return { rows: out, heldSymbols: Array.from(heldSymbols).sort() };
}

function enrichRowsWithScanner(rows, scannerItems) {
  const exactMap = new Map();
  const symMap = new Map();

  for (const it of scannerItems || []) {
    if (!it || typeof it !== "object") continue;

    const venueGuess =
      normalizeVenue(it.venue) ||
      normalizeVenue(it.exchange) ||
      normalizeVenue(it.venue_id) ||
      normalizeVenue(it.source) ||
      "";

    const symRaw =
      pickStr(it, ["symbol", "pair", "market"], "") || symbolFromAsset(pickStr(it, ["asset", "base", "ticker"], ""));
    const sym = canonicalizeSymbol(symRaw);
    if (!sym) continue;

    const payload = {
      change_1h: pickNum(it, NUM_FIELD_KEYS.change_1h),
      change_1d: pickNum(it, NUM_FIELD_KEYS.change_1d),
      change_1w: pickNum(it, NUM_FIELD_KEYS.change_1w),
    };

    if (payload.change_1h === null && payload.change_1d === null && payload.change_1w === null) continue;

    if (venueGuess) exactMap.set(`${venueGuess}:${sym}`, payload);

    const prev = symMap.get(sym);
    if (!prev) symMap.set(sym, payload);
    else {
      const prev1d = toNum(prev.change_1d);
      const next1d = toNum(payload.change_1d);
      if (prev1d === null && next1d !== null) symMap.set(sym, payload);
      else if (prev1d !== null && next1d !== null && next1d > prev1d) symMap.set(sym, payload);
    }
  }

  const nextRows = (rows || []).map((r0) => {
    const v0 = normalizeVenue(r0.venue);
    const s0 = canonicalizeSymbol(r0.symbol);
    const exact = exactMap.get(`${v0}:${s0}`);
    const fallback = symMap.get(s0);
    const ch = exact || fallback;
    if (!ch) return r0;
    return {
      ...r0,
      change_1h: ch.change_1h ?? r0.change_1h,
      change_1d: ch.change_1d ?? r0.change_1d,
      change_1w: ch.change_1w ?? r0.change_1w,
    };
  });

  return { rows: nextRows, exactMap, symMap };
}

function computeTopFromRows(rows) {
  const arr = Array.isArray(rows) ? rows : [];
  let best = null;
  for (const r of arr) {
    const c1d = toNum(r?.change_1d);
    if (c1d === null) continue;
    if (!best || c1d > best.change_1d) {
      best = { asset: r.asset || null, symbol: r.symbol || null, change_1d: c1d };
    }
  }
  return best;
}

/**
 * Singleton service:
 * - start(config) begins polling (deduped)
 * - subscribe(cb) receives snapshots
 * - refreshNow() forces refresh
 * - writes cache to LS so windows can hydrate instantly when opened
 */
const TopGainersService = (() => {
  let listeners = new Set();

  let config = {
    apiBase: "",
    enabledVenues: [],
    venueFilter: "",
    autoRefresh: readBoolLS(lsKey("autoRefresh"), true),
    refreshSeconds: clampSeconds(readIntLS(lsKey("refreshSeconds"), 300), 300),
  };

  let timer = null;
  let inFlight = false;
  let abortCtrl = null;
  let lastRunAt = 0;

  let snapshot = (() => {
    const cached = readJsonLS(CACHE_KEY);
    if (cached && typeof cached === "object") return cached;
    return {
      ts: null,
      lastUpdated: null,
      top: null,
      rows: [],
      heldSymbols: [],
      balancesWarn: null,
      scanErr: null,
      heldErr: null,
      loading: false,
      venue_filter: "",
      enabledVenues: [],
    };
  })();

  function emit(next) {
    snapshot = next;
    for (const cb of listeners) {
      try {
        cb(snapshot);
      } catch {
        // ignore listener errors
      }
    }
  }

  function writeCache(next) {
    writeJsonLS(CACHE_KEY, next);
  }

  async function doFetchOnce({ reason } = {}) {
    if (inFlight) return;
    const base = trimApiBase(config.apiBase);
    if (!base) return;

    inFlight = true;
    lastRunAt = Date.now();

    try {
      abortCtrl?.abort?.();
    } catch {
      // ignore
    }
    abortCtrl = new AbortController();

    const enabledVenuesNorm = (config.enabledVenues || []).map(normalizeVenue).filter(Boolean).sort();
    const vf = normalizeVenueFilterValue(config.venueFilter);

    const vList = vf ? [vf] : enabledVenuesNorm.slice();
    const allowedSet = new Set(vList.map(normalizeVenue).filter(Boolean));

    const starting = {
      ...snapshot,
      loading: reason !== "silent",
      heldErr: null,
      scanErr: null,
      balancesWarn: null,
      venue_filter: vf || "",
      enabledVenues: enabledVenuesNorm,
    };
    emit(starting);

    // 1) balances driver (merge per venue when vf is blank)
    let mergedBalanceItems = [];
    let balancesWarn = null;

    if (vf) {
      mergedBalanceItems = await fetchBalancesLatestOne(base, vf, abortCtrl.signal);
    } else if (!enabledVenuesNorm.length) {
      mergedBalanceItems = await fetchBalancesLatestOne(base, "", abortCtrl.signal);
    } else {
      const results = await Promise.allSettled(
        enabledVenuesNorm.map(async (v) => {
          const items = await fetchBalancesLatestOne(base, v, abortCtrl.signal);
          return { venue: v, items };
        })
      );

      const okItems = [];
      const failed = [];
      for (const r of results) {
        if (r.status === "fulfilled") okItems.push(...(Array.isArray(r.value?.items) ? r.value.items : []));
        else failed.push(r.reason);
      }

      mergedBalanceItems = okItems;
      if (failed.length) balancesWarn = "Some venues failed to load balances (non-fatal).";

      if (!mergedBalanceItems.length && failed.length) {
        throw new Error("Failed to load balances for enabled venues.");
      }
    }

    const { rows: baseRows, heldSymbols } = buildRowsFromBalancesItems(mergedBalanceItems, allowedSet);

    // 2) enrich (best effort)
    let scanErr = null;
    let enrichedRows = baseRows;

    if (baseRows.length) {
      try {
        const venuesForScan = vf ? [vf] : enabledVenuesNorm.slice();
        const scannerItems = await fetchScannerTopGainers(base, venuesForScan, abortCtrl.signal);
        const enriched = enrichRowsWithScanner(baseRows, scannerItems);
        enrichedRows = enriched.rows;

        const anyEnriched = baseRows.some((r0) => {
          const v0 = normalizeVenue(r0.venue);
          const s0 = canonicalizeSymbol(r0.symbol);
          return enriched.exactMap.has(`${v0}:${s0}`) || enriched.symMap.has(s0);
        });

        if (!anyEnriched) {
          scanErr =
            "Scanner returned no matching change fields for held symbols. (Non-fatal; check /api/scanners/top_gainers payload and symbol formats.)";
        }
      } catch {
        scanErr = "Scanner request failed. (Non-fatal; balances table remains correct.)";
      }
    }

    const top = computeTopFromRows(enrichedRows);

    const next = {
      ts: Date.now(),
      lastUpdated: new Date().toISOString(),
      top,
      rows: enrichedRows,
      heldSymbols,
      balancesWarn,
      scanErr,
      heldErr: null,
      loading: false,
      venue_filter: vf || "",
      enabledVenues: enabledVenuesNorm,
    };

    emit(next);
    writeCache(next);
  }

  function clearTimer() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
  }

  function scheduleLoop() {
    clearTimer();
    if (!config.autoRefresh) return;

    const ms = clampSeconds(config.refreshSeconds, 300) * 1000;
    const jitterMs = Math.floor(Math.random() * 800);

    const tick = async () => {
      // If we very recently refreshed (e.g., config change + immediate tick), do not double-fire.
      if (Date.now() - lastRunAt < 800) {
        timer = setTimeout(tick, ms);
        return;
      }
      await doFetchOnce({ reason: "interval" });
      timer = setTimeout(tick, ms);
    };

    // IMPORTANT: first tick is quick (jitter only), not ms + jitter.
    timer = setTimeout(tick, jitterMs);
  }

  return {
    start(nextConfig) {
      config = {
        ...config,
        ...(nextConfig || {}),
        venueFilter: normalizeVenueFilterValue(nextConfig?.venueFilter ?? config.venueFilter),
      };

      // persist the knobs the user expects to persist
      lsSet(lsKey("autoRefresh"), config.autoRefresh ? "1" : "0");
      lsSet(lsKey("refreshSeconds"), String(clampSeconds(config.refreshSeconds, 300)));

      // schedule polling (and do a one-time immediate fetch if we have apiBase)
      scheduleLoop();

      // Do an immediate fetch when apiBase/enabledVenues change (fast, shows life).
      // Keep it "silent" if we already have rows to avoid UI flicker.
      const silent = Array.isArray(snapshot?.rows) && snapshot.rows.length > 0;
      if (trimApiBase(config.apiBase)) doFetchOnce({ reason: silent ? "silent" : "deps" });
    },

    stop() {
      clearTimer();
      try {
        abortCtrl?.abort?.();
      } catch {
        // ignore
      }
    },

    subscribe(cb) {
      if (typeof cb !== "function") return () => {};
      listeners.add(cb);
      // immediate push of current snapshot
      try {
        cb(snapshot);
      } catch {
        // ignore
      }
      return () => {
        listeners.delete(cb);
      };
    },

    refreshNow() {
      return doFetchOnce({ reason: "manual" });
    },

    readCache() {
      return readJsonLS(CACHE_KEY);
    },

    setAutoRefresh(next) {
      config.autoRefresh = !!next;
      lsSet(lsKey("autoRefresh"), config.autoRefresh ? "1" : "0");
      scheduleLoop();
    },

    setRefreshSeconds(next) {
      config.refreshSeconds = clampSeconds(next, 300);
      lsSet(lsKey("refreshSeconds"), String(config.refreshSeconds));
      scheduleLoop();
    },

    setVenueFilter(next) {
      config.venueFilter = normalizeVenueFilterValue(next);
      scheduleLoop();
      if (trimApiBase(config.apiBase)) doFetchOnce({ reason: "deps" });
    },

    getConfig() {
      return { ...config };
    },
  };
})();

export default TopGainersService;
