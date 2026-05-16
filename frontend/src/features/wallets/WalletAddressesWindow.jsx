// frontend/src/features/wallets/WalletAddressesWindow.jsx
import React, { useEffect, useMemo, useState } from "react";

/**
 * WalletAddressesWindow (MVP)
 *
 * Backend endpoints (current):
 *  - GET    /api/wallet_addresses?asset=&network=&limit=
 *  - POST   /api/wallet_addresses
 *  - DELETE /api/wallet_addresses/{address_id}
 *  - GET    /api/wallet_addresses/balances/latest?limit=
 *  - POST   /api/wallet_addresses/balances/refresh
 *  - POST   /api/wallet_addresses/tx/ingest
 *
 * Notes:
 *  - This is intentionally balances/snapshots-focused (Blockchair polling),
 *    because tx-ingestion / onchain networks is the next backend step.
 *
 * Props (optional):
 *  - apiBase: string (e.g. "http://localhost:8000") or "" for same-origin
 *  - hideTableData: boolean (redacts addresses)
 *  - onClose: () => void
 */
export default function WalletAddressesWindow({ apiBase = "", hideTableData = false, onClose }) {
  const [tab, setTab] = useState("addresses"); // addresses | balances
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const [addresses, setAddresses] = useState([]);
  const [balances, setBalances] = useState([]);

  const [flt, setFlt] = useState({
    asset: "",
    wallet_id: "",
    network: "",
    limit: 500,
  });

  const [form, setForm] = useState({
    asset: "BTC",
    wallet_id: "",
    network: "mainnet",
    address: "",
    label: "",
    owner_scope: "user",
  });

  const [editingId, setEditingId] = useState(null);

  const [withPrices, setWithPrices] = useState(true);
  const [balLimit, setBalLimit] = useState(2000);

  // Balances refresh debug/status
  const [balLastRefresh, setBalLastRefresh] = useState(null);

  // Tx ingest → ledger controls
  const [txWriteLedger, setTxWriteLedger] = useState(true);
  const [txLimitPerAddress, setTxLimitPerAddress] = useState(200);
  const [txLastResult, setTxLastResult] = useState(null);

  const txStats = useMemo(() => deriveTxStats(txLastResult), [txLastResult]);

  const redacted = (v) => (hideTableData ? "••••••••" : v);

  function applyHydrationWalletMode() {
    setForm((p) => ({
      ...p,
      asset: "ALL",
      wallet_id: "polkadot_hydration",
      network: "hydration",
      label: p.label || "Hydration SubWallet",
    }));
  }


  function _asInt(v) {
    const n = Number(v);
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }

  function _asCount(v, fallback = null) {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }

  function deriveTxStats(res) {
    if (!res) return null;

    // Accept multiple backend shapes; prefer explicit fields when present.
    const linkedDeposits =
      res.linked_deposits ??
      res.deposits_linked ??
      res.depositsLinked ??
      res?.linked?.deposits ??
      res?.linked?.deposit ??
      res?.ledger?.linked_deposits ??
      res?.ledger?.deposits;

    const linkedWithdrawals =
      res.linked_withdrawals ??
      res.withdrawals_linked ??
      res.withdrawalsLinked ??
      res?.linked?.withdrawals ??
      res?.linked?.withdrawal ??
      res?.ledger?.linked_withdrawals ??
      res?.ledger?.withdrawals;

    const pending =
      res.pending ??
      res.pending_rows ??
      res.pendingCount ??
      res?.backlog?.pending ??
      res?.stats?.pending;

    const rawByReason =
      res.skipped_by_reason ??
      res.skips_by_reason ??
      res.skip_reasons ??
      res?.skip?.by_reason ??
      res?.skips?.by_reason ??
      res?.stats?.skipped_by_reason;

    let skippedByReason = null;
    if (rawByReason && typeof rawByReason === "object") {
      if (Array.isArray(rawByReason)) {
        // [{reason, n}] or [{k,v}]
        const m = {};
        for (const r of rawByReason) {
          const reason = r?.reason ?? r?.k ?? r?.key ?? r?.name;
          const n = r?.n ?? r?.count ?? r?.v ?? r?.value;
          if (reason != null) m[String(reason)] = _asCount(n, 0);
        }
        skippedByReason = m;
      } else {
        // {reason: n}
        skippedByReason = {};
        for (const [k, v] of Object.entries(rawByReason)) skippedByReason[String(k)] = _asCount(v, 0);
      }
    }

    let skippedTotal =
      res.skipped_total ??
      res.skipped ??
      res.skips_total ??
      res?.skip?.total ??
      res?.stats?.skipped_total;

    if (skippedTotal == null && skippedByReason) {
      skippedTotal = Object.values(skippedByReason).reduce((a, b) => a + (Number.isFinite(Number(b)) ? Number(b) : 0), 0);
    }

    const out = {
      linkedDeposits: _asInt(linkedDeposits),
      linkedWithdrawals: _asInt(linkedWithdrawals),
      skippedTotal: _asInt(skippedTotal),
      skippedByReason,
      pending: _asInt(pending),
    };

    // If everything is missing, return null so UI can hide summary.
    const any =
      out.linkedDeposits != null ||
      out.linkedWithdrawals != null ||
      out.skippedTotal != null ||
      out.pending != null ||
      (out.skippedByReason && Object.keys(out.skippedByReason).length > 0);

    return any ? out : null;
  }


  function normBase(b) {
    if (!b) return "";
    return b.endsWith("/") ? b.slice(0, -1) : b;
  }

  function _protocolHint(url) {
    try {
      const pageProto = window?.location?.protocol;
      const u = new URL(url, window.location.href);
      if (pageProto === "https:" && u.protocol === "http:") {
        return " (Mixed content: https page -> http API blocked by browser. Use https API or serve frontend over http.)";
      }
      return "";
    } catch {
      return "";
    }
  }

  async function api(path, opts = {}) {
    const base = normBase(apiBase);
    const url = `${base}${path}`;
    const method = opts.method || "GET";

    const headers = {
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    };

    const init = { method, headers };
    if (opts.body !== undefined) init.body = JSON.stringify(opts.body);

    let res;
    let text = "";
    try {
      res = await fetch(url, init);
      text = await res.text();
    } catch (e) {
      // This is where "Failed to fetch" comes from (CORS, mixed content, network down, etc.)
      const hint = _protocolHint(url);
      console.error("[WalletAddressesWindow.api] fetch threw:", { url, method, init, error: e });
      const msg = (e && e.message) ? e.message : String(e);
      throw new Error(`Fetch failed: ${method} ${url}${hint} :: ${msg}`);
    }

    let data;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }

    if (!res.ok) {
      const msg =
        (data && (data.detail || data.error || data.message)) ||
        `HTTP ${res.status} ${res.statusText}`;
      throw new Error(`${msg} (${method} ${url})`);
    }
    return data;
  }

  const listQuery = useMemo(() => {
    const p = new URLSearchParams();
    if (flt.asset?.trim()) p.set("asset", flt.asset.trim());
    if (flt.wallet_id?.trim()) p.set("wallet_id", flt.wallet_id.trim());
    if (flt.network?.trim()) p.set("network", flt.network.trim());
    p.set("limit", String(flt.limit || 500));
    const qs = p.toString();
    return qs ? `?${qs}` : "";
  }, [flt]);

  async function loadAddresses() {
    setBusy(true);
    setErr("");
    try {
      const rows = await api(`/api/wallet_addresses${listQuery}`);
      setAddresses(Array.isArray(rows) ? rows : rows?.items || []);
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  async function loadBalances() {
    setBusy(true);
    setErr("");
    try {
      const qs = new URLSearchParams();
      qs.set("with_prices", withPrices ? "1" : "0");
      qs.set("limit", String(balLimit || 2000));
      const res = await api(`/api/wallet_addresses/balances/latest?${qs.toString()}`);
      const items = Array.isArray(res) ? res : res?.items || [];
      setBalances(items);
      return items;
    } catch (e) {
      setErr(e?.message || String(e));
      return [];
    } finally {
      setBusy(false);
    }
  }

  function isHydrationAddressRow(row) {
    const walletId = String(row?.wallet_id ?? row?.walletId ?? row?.venue ?? row?.venue_override ?? "").trim().toLowerCase();
    const network = String(row?.network ?? row?.chain ?? row?.network_name ?? "").trim().toLowerCase();
    return (
      walletId === "polkadot_hydration" ||
      walletId === "hydration" ||
      walletId.includes("hydration") ||
      network === "hydration" ||
      network === "polkadot_hydration" ||
      network.includes("hydration")
    );
  }


  function firstHydrationOrderbookPrice(levels) {
    const arr = Array.isArray(levels) ? levels : [];
    for (const lvl of arr) {
      const px = Array.isArray(lvl)
        ? Number(lvl?.[0] ?? lvl?.price)
        : Number(lvl?.price ?? lvl?.px ?? lvl?.rate ?? lvl?.limit ?? lvl?.p);
      if (Number.isFinite(px) && px > 0) return px;
    }
    return null;
  }

  function hydrationOrderbookMid(data) {
    const direct = Number(
      data?.mid ??
      data?.midPrice ??
      data?.mid_price ??
      data?.price ??
      data?.markPrice ??
      data?.mark_price ??
      data?.spotPrice ??
      data?.spot_price ??
      data?.pool?.spotPrice ??
      data?.pool?.spot_price
    );
    if (Number.isFinite(direct) && direct > 0) return direct;

    const rawBid = Number(data?.bestBid ?? data?.best_bid ?? data?.bid ?? data?.bids?.[0]?.price);
    const rawAsk = Number(data?.bestAsk ?? data?.best_ask ?? data?.ask ?? data?.asks?.[0]?.price);
    const bid = Number.isFinite(rawBid) && rawBid > 0 ? rawBid : firstHydrationOrderbookPrice(data?.bids);
    const ask = Number.isFinite(rawAsk) && rawAsk > 0 ? rawAsk : firstHydrationOrderbookPrice(data?.asks);
    if (bid !== null && ask !== null && bid > 0 && ask > 0) return (bid + ask) / 2;
    if (bid !== null && bid > 0) return bid;
    if (ask !== null && ask > 0) return ask;
    return null;
  }

  async function fetchHydrationOrderbookMid(symbol) {
    const sym = String(symbol || "").trim().toUpperCase();
    if (sym !== "UTTT-HDX") return null;
    try {
      const data = await api(`/api/polkadot_dex/hydration/orderbook?symbol=${encodeURIComponent(sym)}&depth=5&route_mode=manual_xyk`);
      if (data?.ok === false) return null;
      const mid = hydrationOrderbookMid(data);
      return mid !== null && mid > 0 ? mid : null;
    } catch {
      return null;
    }
  }

  async function fetchHydrationDerivedUsdPrices() {
    const out = {
      prices: { USDT: 1, USDC: 1, HOLLAR: 1 },
      sources: { USDT: "stable", USDC: "stable", HOLLAR: "stable" },
    };

    const asHydrationPriceNum = (v) => {
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    try {
      const data = await api("/api/polkadot_dex/hydration/prices?assets=HDX,DOT,USDT,UTTT,HOLLAR&refresh=true");
      const maps = [
        data?.prices_usd,
        data?.usd_prices,
        data?.pricesUsd,
        data?.usdPrices,
        data?.prices,
        data?.price_map,
        data?.priceMap,
        data?.usd,
      ];
      const sourceMaps = [
        data?.priceSources,
        data?.price_sources,
        data?.usd_sources,
        data?.usdSources,
        data?.sources,
      ];
      const symbols = ["HDX", "DOT", "UTTT", "USDT", "USDC", "HOLLAR"];

      for (const sym of symbols) {
        for (const m of maps) {
          if (!m || typeof m !== "object") continue;
          const entry = m?.[sym] ?? m?.[sym.toLowerCase()];
          const val = asHydrationPriceNum(
            entry && typeof entry === "object"
              ? entry?.px_usd ?? entry?.price_usd ?? entry?.priceUsd ?? entry?.usd_price ?? entry?.usdPrice ?? entry?.price ?? entry?.usd
              : entry
          );
          if (val !== null) {
            out.prices[sym] = val;
            break;
          }
        }

        for (const sm of sourceMaps) {
          if (!sm || typeof sm !== "object") continue;
          const src = sm?.[sym] ?? sm?.[sym.toLowerCase()];
          if (src !== undefined && src !== null && String(src).trim()) {
            out.sources[sym] = String(src);
            break;
          }
        }
      }
    } catch {
      // Keep stablecoin defaults and avoid falling back to generic Hydration orderbook pairs.
    }

    return out;
  }

  function applyHydrationDerivedUsdPricesToSnapshotRows(items, derived) {
    const prices = derived?.prices || {};
    const sources = derived?.sources || {};
    const asNum = (v) => {
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    return (items || []).map((it) => {
      const sym = String(it?.asset || it?.symbol || "").trim().toUpperCase();
      const balance = asNum(it?.balance ?? it?.total ?? it?.available) ?? 0;
      const existingPx = asNum(it?.usd_price ?? it?.px_usd ?? it?.price_usd);
      const existingUsd = asNum(it?.usd_value ?? it?.total_usd ?? it?.value_usd);
      const derivedPx = asNum(prices?.[sym]);
      const px = existingPx !== null ? existingPx : derivedPx;
      const usdValue = existingUsd !== null ? existingUsd : (px !== null ? balance * px : null);

      return {
        ...it,
        usd_price: px !== null ? px : it?.usd_price ?? "",
        usd_value: usdValue !== null ? usdValue : it?.usd_value ?? "",
        usd_source_symbol: it?.usd_source_symbol && it.usd_source_symbol !== "—"
          ? it.usd_source_symbol
          : (px !== null ? (sources?.[sym] || "derived") : it?.usd_source_symbol || ""),
      };
    });
  }

  function normalizeHydrationBalanceRows(data, sourceRow) {
    const address = String(sourceRow?.address || data?.address || "").trim();
    const label = sourceRow?.label || "Hydration SubWallet";
    const fetchedAt = data?.fetched_at || data?.fetchedAt || new Date().toISOString();

    const asNum = (v) => {
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    const canonicalHydrationSymbol = (symbol, assetId) => {
      const s = String(symbol || "").trim();
      const id = String(assetId || "").trim();
      const key = (s || id).toLowerCase();
      const byId = {
        "0": "HDX",
        native: "HDX",
        hdx: "HDX",
        "5": "DOT",
        dot: "DOT",
        "10": "USDT",
        usdt: "USDT",
        "222": "HOLLAR",
        hollar: "HOLLAR",
        "1001331": "UTTT",
        uttt: "UTTT",
      };
      return byId[key] || s || id;
    };

    const getHydrationUsdPrice = (symbol, assetId, item = null) => {
      const direct = asNum(
        item?.px_usd ??
        item?.price_usd ??
        item?.priceUsd ??
        item?.usd_price ??
        item?.usdPrice ??
        item?.priceUSD ??
        item?.usd
      );
      if (direct !== null) return direct;

      const sym = String(symbol || "").trim().toUpperCase();
      const id = String(assetId || "").trim();
      if (sym === "USDT" || sym === "USDC" || sym === "HOLLAR") return 1;

      const maps = [
        data?.prices_usd,
        data?.pricesUsd,
        data?.usd_prices,
        data?.usdPrices,
        data?.prices,
        data?.price_map,
        data?.priceMap,
        data?.usd,
      ];
      const keys = [sym, sym.toLowerCase(), id, id.toLowerCase()].filter(Boolean);
      for (const m of maps) {
        if (!m || typeof m !== "object") continue;
        for (const k of keys) {
          const entry = m?.[k];
          const val = asNum(
            typeof entry === "object"
              ? entry?.px_usd ?? entry?.price_usd ?? entry?.priceUsd ?? entry?.usd_price ?? entry?.usdPrice ?? entry?.price ?? entry?.usd
              : entry
          );
          if (val !== null) return val;
        }
      }

      return asNum(
        sym === "HDX" ? (data?.hdx_usd ?? data?.hdxUsd ?? data?.hdx_price_usd ?? data?.hdxPriceUsd) :
        sym === "DOT" ? (data?.dot_usd ?? data?.dotUsd ?? data?.dot_price_usd ?? data?.dotPriceUsd) :
        sym === "UTTT" ? (data?.uttt_usd ?? data?.utttUsd ?? data?.uttt_price_usd ?? data?.utttPriceUsd) :
        null
      );
    };

    let rawItems = Array.isArray(data?.items)
      ? data.items
      : Array.isArray(data?.balances)
        ? data.balances
        : Array.isArray(data?.tokens)
          ? data.tokens
          : Array.isArray(data?.assets)
            ? data.assets
            : data?.balances && typeof data.balances === "object"
              ? Object.entries(data.balances).map(([asset, value]) => ({ asset, ...(value && typeof value === "object" ? value : { total: value }) }))
              : data?.items && typeof data.items === "object"
                ? Object.entries(data.items).map(([asset, value]) => ({ asset, ...(value && typeof value === "object" ? value : { total: value }) }))
                : [];

    const hasNativeHdx = (rawItems || []).some((it) => {
      const sym = canonicalHydrationSymbol(
        it?.symbol ?? it?.asset ?? it?.ticker ?? it?.currency ?? it?.assetSymbol ?? it?.asset_symbol,
        it?.asset_id ?? it?.assetId ?? it?.id ?? it?.token_id ?? it?.tokenId
      );
      return String(sym || "").toUpperCase() === "HDX";
    });

    if (!hasNativeHdx) {
      const nativeObj =
        (data?.native && typeof data.native === "object" ? data.native : null) ||
        (data?.native_balance && typeof data.native_balance === "object" ? data.native_balance : null) ||
        (data?.nativeBalance && typeof data.nativeBalance === "object" ? data.nativeBalance : null) ||
        (data?.hdx && typeof data.hdx === "object" ? data.hdx : null) ||
        (data?.hdx_balance && typeof data.hdx_balance === "object" ? data.hdx_balance : null) ||
        (data?.hdxBalance && typeof data.hdxBalance === "object" ? data.hdxBalance : null) ||
        null;
      const nativeNumber = asNum(
        data?.hdx_ui ??
        data?.hdxUi ??
        data?.hdx_balance_ui ??
        data?.hdxBalanceUi ??
        data?.native_ui ??
        data?.nativeUi ??
        data?.native_balance_ui ??
        data?.nativeBalanceUi ??
        data?.hdx_balance ??
        data?.hdxBalance ??
        data?.native_balance ??
        data?.nativeBalance
      );
      const candidate = nativeObj
        ? { asset: "HDX", symbol: "HDX", asset_id: "native", ...nativeObj }
        : nativeNumber !== null
          ? { asset: "HDX", symbol: "HDX", asset_id: "native", total: nativeNumber, available: nativeNumber }
          : null;
      if (candidate) rawItems = [candidate, ...(rawItems || [])];
    }

    const out = [];
    for (const it of rawItems || []) {
      const rawAsset = String(
        it?.symbol ??
        it?.asset ??
        it?.ticker ??
        it?.currency ??
        it?.assetSymbol ??
        it?.asset_symbol ??
        ""
      ).trim();
      const assetId = String(
        it?.asset_id ??
        it?.assetId ??
        it?.id ??
        it?.token_id ??
        it?.tokenId ??
        ""
      ).trim();
      const asset = canonicalHydrationSymbol(rawAsset, assetId);

      const free = asNum(
        it?.free_ui ??
        it?.freeUi ??
        it?.available_ui ??
        it?.availableUi ??
        it?.available ??
        it?.free ??
        it?.spendable ??
        it?.amount_ui ??
        it?.amountUi ??
        it?.uiAmount ??
        it?.ui_amount
      );

      const reserved = asNum(
        it?.reserved_ui ??
        it?.reservedUi ??
        it?.reserved ??
        it?.hold_ui ??
        it?.holdUi ??
        it?.hold ??
        it?.locked ??
        0
      );

      const totalExplicit = asNum(
        it?.total_ui ??
        it?.totalUi ??
        it?.balance_ui ??
        it?.balanceUi ??
        it?.total ??
        it?.balance ??
        it?.amount
      );

      const balance = totalExplicit ?? ((free ?? 0) + (reserved ?? 0));
      const usdPrice = getHydrationUsdPrice(asset, assetId, it);
      const usdValue = asNum(it?.total_usd ?? it?.usd_value ?? it?.usdValue ?? it?.value_usd ?? it?.valueUsd) ??
        (usdPrice != null && balance != null ? balance * usdPrice : null);

      if (!asset && balance == null) continue;

      out.push({
        id: `hydration:${sourceRow?.id || address}:${asset || out.length}`,
        asset: asset || "Hydration",
        network: "hydration",
        address,
        label,
        balance: balance ?? "",
        usd_price: usdPrice ?? "",
        usd_value: usdValue ?? "",
        fetched_at: fetchedAt,
        source: data?.source || data?.venue || "polkadot_dex/balances",
      });
    }
    return out;
  }

  async function refreshHydrationBalanceRow(row) {
    const address = String(row?.address || "").trim();
    if (!address) throw new Error("Hydration wallet row is missing an address.");
    const res = await api(`/api/polkadot_dex/balances?address=${encodeURIComponent(address)}`);
    if (res?.ok === false) throw new Error(res?.detail || res?.error || "Hydration balances failed.");

    const normalizedRows = normalizeHydrationBalanceRows(res, row);
    try {
      const derived = await fetchHydrationDerivedUsdPrices();
      return applyHydrationDerivedUsdPricesToSnapshotRows(normalizedRows, derived);
    } catch {
      return normalizedRows;
    }
  }

  async function refreshBalances(ids = null) {
    setBusy(true);
    setErr("");
    try {
      const selectedRows = Array.isArray(ids) && ids.length
        ? (addresses || []).filter((a) => ids.includes(a.id))
        : (addresses || []);

      const hydrationRows = selectedRows.filter((a) => isHydrationAddressRow(a));
      const legacyRows = selectedRows.filter((a) => !isHydrationAddressRow(a));

      const hydrationLiveRows = [];
      const hydrationErrors = [];
      for (const row of hydrationRows) {
        try {
          const rows = await refreshHydrationBalanceRow(row);
          hydrationLiveRows.push(...rows);
        } catch (e) {
          hydrationErrors.push({
            id: row?.id,
            asset: row?.asset,
            network: row?.network,
            address: row?.address,
            error: e?.message || String(e),
          });
        }
      }

      const shouldRunLegacyRefresh = Array.isArray(ids) && ids.length
        ? legacyRows.length > 0
        : hydrationRows.length === 0 || legacyRows.length > 0;

      let res = null;
      let latest = [];
      if (shouldRunLegacyRefresh) {
        const body = Array.isArray(ids) && ids.length
          ? { ids: legacyRows.map((a) => a.id).filter(Boolean) }
          : hydrationRows.length && legacyRows.length
            ? { ids: legacyRows.map((a) => a.id).filter(Boolean) }
            : {};

        if (!Array.isArray(body.ids) || body.ids.length) {
          res = await api(`/api/wallet_addresses/balances/refresh`, { method: "POST", body });
          latest = await loadBalances();
        }
      }

      const mergedErrors = [
        ...(Array.isArray(res?.errors) ? res.errors : []),
        ...hydrationErrors,
      ];

      const refreshed = Number(res?.refreshed || 0) + hydrationRows.length - hydrationErrors.length;
      const finalResult = {
        ...(res || {}),
        refreshed,
        errors: mergedErrors,
        hydration_live: {
          attempted: hydrationRows.length,
          refreshed: hydrationRows.length - hydrationErrors.length,
          rows: hydrationLiveRows.length,
          endpoint: "/api/polkadot_dex/balances",
        },
      };

      // surface refresh result in the UI
      setBalLastRefresh(finalResult);

      if (hydrationLiveRows.length) {
        setBalances([...(hydrationLiveRows || []), ...(latest || [])]);
        setTab("balances");
      }

      if (mergedErrors.length) {
        console.error("[wallet_balances_refresh] errors:", mergedErrors);
        setErr(`Balances refresh completed: refreshed ${refreshed}, errors ${mergedErrors.length} (see console).`);
      }
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  async function ingestTx(ids = null) {
    setBusy(true);
    setErr("");
    setTxLastResult(null);
    try {
      const body = {
        limit_per_address: Number(txLimitPerAddress || 200),
        write_ledger: txWriteLedger ? 1 : 0,
      };
      if (Array.isArray(ids) && ids.length) body.ids = ids;

      const res = await api(`/api/wallet_addresses/tx/ingest`, { method: "POST", body });
      setTxLastResult(res || { ok: true });

      if (tab === "balances") {
        await loadBalances();
      } else {
        await loadAddresses();
      }
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  async function createAddress() {
    setBusy(true);
    setErr("");
    try {
      const walletId = String(form.wallet_id || "").trim();
      const network = String(form.network || "").trim();
      const isHydrationWallet =
        walletId.toLowerCase() === "polkadot_hydration" ||
        network.toLowerCase() === "hydration";
      const asset = String(form.asset || "").trim().toUpperCase() || (isHydrationWallet ? "ALL" : "");

      const payload = {
        asset,
        wallet_id: walletId || null,
        network,
        address: String(form.address || "").trim(),
        label: String(form.label || "").trim() || null,
        owner_scope: String(form.owner_scope || "user").trim().toLowerCase(),
      };

      if (!payload.address) throw new Error("Address is required.");
      if (!payload.asset) throw new Error("Asset is required. Use ALL for Hydration/SubWallet all-asset detection.");
      if (!payload.network) throw new Error("Network is required.");

      if (editingId) {
        await api(`/api/wallet_addresses/${editingId}`, { method: "PATCH", body: payload });
      } else {
        await api(`/api/wallet_addresses`, { method: "POST", body: payload });
      }
      await loadAddresses();
      setForm((p) => ({ ...p, address: "", label: "" }));
      setEditingId(null);
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  function beginEdit(row) {
    setEditingId(row.id);
    setForm({
      asset: row.asset || "",
      wallet_id: row.wallet_id || "",
      network: row.network || "",
      address: row.address || "",
      label: row.label || "",
      owner_scope: row.owner_scope || "user",
    });
  }

  async function deleteAddress(id) {
    if (!window.confirm("Delete this wallet address record?")) return;
    setBusy(true);
    setErr("");
    try {
      await api(`/api/wallet_addresses/${id}`, { method: "DELETE" });
      await loadAddresses();
    } catch (e) {
      setErr(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    loadAddresses();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div style={{ padding: 12, fontFamily: "system-ui, sans-serif", fontSize: 13 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <div style={{ fontWeight: 700, fontSize: 16 }}>Wallet Addresses</div>
        <div style={{ opacity: 0.7 }}>({tab})</div>
        <div style={{ flex: 1 }} />
        <button onClick={() => (tab === "balances" ? loadBalances() : loadAddresses())} disabled={busy}>
          Refresh
        </button>
        {typeof onClose === "function" && <button onClick={onClose}>Close</button>}
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 10 }}>
        <button onClick={() => setTab("addresses")} disabled={tab === "addresses"}>
          Addresses
        </button>
        <button
          onClick={() => {
            setTab("balances");
            loadBalances();
          }}
          disabled={tab === "balances"}
        >
          Balances
        </button>
        <div style={{ flex: 1 }} />
        {busy && <div style={{ opacity: 0.8 }}>Working…</div>}
      </div>

      {err && (
        <div
          style={{
            marginBottom: 10,
            padding: 8,
            background: "#ffecec",
            border: "1px solid #ffb3b3",
            borderRadius: 8,
            color: "#111",
            fontWeight: 600,
          }}
        >
          <b>Error:</b> {err}
        </div>
      )}

      {tab === "addresses" && (
        <>
          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Add Wallet Address (MVP)</div>

            <div style={{ marginBottom: 10, display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
              <div style={{ fontWeight: 600, opacity: 0.9 }}>Hydration/SubWallet:</div>
              <button type="button" onClick={applyHydrationWalletMode} disabled={busy}>
                Use Hydration wallet
              </button>
              <div style={{ flexBasis: "100%", opacity: 0.75 }}>
                Use one account-level Hydration row: <b>Asset</b> <code>ALL</code>, <b>Venue</b> <code>polkadot_hydration</code>, and <b>Network</b> <code>hydration</code>. The same SubWallet/Substrate address can be scanned for all supported Hydration assets; no per-asset address rows are needed.
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>Asset / scope</label>
              <input
                placeholder="e.g. BTC, SOL, UTTT, or ALL for Hydration"
                value={form.asset}
                onChange={(e) => setForm((p) => ({ ...p, asset: e.target.value.toUpperCase() }))}
              />

              <label>Venue</label>
              <input
                placeholder="e.g. polkadot_hydration, robinhood, dex-trade (blank = self-custody)"
                value={form.wallet_id}
                onChange={(e) => setForm((p) => ({ ...p, wallet_id: e.target.value }))}
              />

              <label>Network</label>
              <input placeholder="e.g. hydration, solana, mainnet" value={form.network} onChange={(e) => setForm((p) => ({ ...p, network: e.target.value }))} />

              <label>Address</label>
              <input placeholder="SubWallet/Substrate or chain address" value={form.address} onChange={(e) => setForm((p) => ({ ...p, address: e.target.value }))} />

              <label>Label</label>
              <input value={form.label} onChange={(e) => setForm((p) => ({ ...p, label: e.target.value }))} />

              <label>Owner scope</label>
              <input value={form.owner_scope} onChange={(e) => setForm((p) => ({ ...p, owner_scope: e.target.value }))} />
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center" }}>
              <button onClick={createAddress} disabled={busy || !String(form.address || "").trim()}>
                {editingId ? "Save" : "Create"}
              </button>
              {editingId ? (
                <button
                  onClick={() => {
                    setEditingId(null);
                    setForm({ asset: "BTC", wallet_id: "", network: "mainnet", address: "", label: "", owner_scope: "user" });
                    setErr("");
                  }}
                  disabled={busy}
                >
                  Cancel
                </button>
              ) : null}
              <div style={{ opacity: 0.75 }}>
                Hydration/SubWallet rows should normally use <b>Asset</b> <code>ALL</code>, <b>Venue</b> <code>polkadot_hydration</code>, and <b>Network</b> <code>hydration</code>. Blank venue remains self-custody.
              </div>
            </div>
          </div>

          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Tx Ingest → Deposits/Withdrawals</div>

            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>Write to ledger</label>
              <input type="checkbox" checked={txWriteLedger} onChange={(e) => setTxWriteLedger(e.target.checked)} />

              <label>Limit / address</label>
              <input
                type="number"
                value={txLimitPerAddress}
                onChange={(e) => setTxLimitPerAddress(Number(e.target.value || 200))}
                min={1}
                max={5000}
              />
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center" }}>
              <button
                onClick={() => {
                  if (!window.confirm("Run on-chain tx ingest for ALL wallet addresses?")) return;
                  ingestTx(null);
                }}
                disabled={busy}
              >
                Ingest txs (all)
              </button>
              <div style={{ opacity: 0.75 }}>
                Uses <code>/api/wallet_addresses/tx/ingest</code>. Enforces policy (skip coinbase; deposits-only robinhood/dex-trade; self-custody both). Hydration wallet rows use <code>ALL</code> + <code>polkadot_hydration</code> + <code>hydration</code>; backend asset scanning/tx ingest support remains endpoint-dependent.
              </div>
            </div>

            {txLastResult ? (
              <div style={{ marginTop: 10, padding: 8, background: "#111", border: "1px solid #333", borderRadius: 8, color: "#eee" }}>
                <div style={{ fontWeight: 700, marginBottom: 4 }}>Last ingest result</div>
                {txStats ? (
                  <div style={{ marginBottom: 8, padding: 8, background: "#171717", border: "1px solid #2a2a2a", borderRadius: 8 }}>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }}>
                      <div>
                        Linked deposits (selected addresses): <b>{txStats.linkedDeposits != null ? txStats.linkedDeposits : "—"}</b>
                      </div>
                      <div>
                        Linked withdrawals (selected addresses): <b>{txStats.linkedWithdrawals != null ? txStats.linkedWithdrawals : "—"}</b>
                      </div>
                      <div>
                        Skipped (selected addresses): <b>{txStats.skippedTotal != null ? txStats.skippedTotal : "—"}</b>
                      </div>
                      <div>
                        Pending (selected addresses): <b>{txStats.pending != null ? txStats.pending : "—"}</b>
                      </div>
                    </div>

                    {txStats.skippedByReason && Object.keys(txStats.skippedByReason).length > 0 ? (
                      <div style={{ marginTop: 8, opacity: 0.95 }}>
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>Skipped by reason</div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                          {Object.entries(txStats.skippedByReason)
                            .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
                            .map(([reason, n]) => (
                              <div
                                key={reason}
                                style={{
                                  padding: "2px 8px",
                                  borderRadius: 999,
                                  border: "1px solid #333",
                                  background: "#0f0f0f",
                                  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
                                  fontSize: 12,
                                }}
                              >
                                {reason}: <b>{String(n)}</b>
                              </div>
                            ))}
                        </div>
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <div style={{ marginBottom: 8, opacity: 0.85 }}>
                    Counters: <span style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" }}>n/a</span>{" "}
                    (selected addresses; backend response did not include linked/skipped/pending fields)
                  </div>
                )}
                <pre style={{ margin: 0, whiteSpace: "pre-wrap", color: "#eee" }}>{JSON.stringify(txLastResult, null, 2)}</pre>
              </div>
            ) : null}
          </div>

          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Filters</div>
            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>Asset</label>
              <input value={flt.asset} onChange={(e) => setFlt((p) => ({ ...p, asset: e.target.value.toUpperCase() }))} />

              <label>Venue</label>
              <input
                placeholder="e.g. polkadot_hydration, robinhood, dex-trade"
                value={flt.wallet_id}
                onChange={(e) => setFlt((p) => ({ ...p, wallet_id: e.target.value }))}
              />

              <label>Network</label>
              <input value={flt.network} onChange={(e) => setFlt((p) => ({ ...p, network: e.target.value }))} />

              <label>Limit</label>
              <input
                type="number"
                value={flt.limit}
                onChange={(e) => setFlt((p) => ({ ...p, limit: Number(e.target.value || 500) }))}
                min={1}
                max={2000}
              />
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <button onClick={loadAddresses} disabled={busy}>
                Apply
              </button>
              <button
                type="button"
                onClick={() => setFlt((p) => ({ ...p, wallet_id: "polkadot_hydration", network: "hydration" }))}
                disabled={busy}
              >
                Filter Hydration
              </button>
              <button type="button" onClick={() => setFlt((p) => ({ ...p, asset: "", wallet_id: "", network: "" }))} disabled={busy}>
                Clear filters
              </button>
            </div>
          </div>

          <div style={{ padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Wallet Addresses</div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ borderCollapse: "collapse", width: "100%" }}>
                <thead>
                  <tr>
                    {["id", "asset", "venue", "network", "address", "label", "owner_scope", "created_at", "actions"].map((h) => (
                      <th key={h} style={{ textAlign: "left", borderBottom: "1px solid #ccc", padding: 6 }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(addresses || []).map((a) => (
                    <tr key={a.id}>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.id}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.asset || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.wallet_id || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.network || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee", fontFamily: "monospace" }}>{redacted(a.address || "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.label || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.owner_scope || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{a.created_at || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>
                        <button
                          onClick={() => {
                            if (!window.confirm("Refresh balance snapshot for this address?")) return;
                            refreshBalances([a.id]);
                          }}
                          disabled={busy}
                          style={{ marginRight: 6 }}
                        >
                          Balance
                        </button>
                        <button
                          onClick={() => {
                            if (!window.confirm("Ingest on-chain tx history for this address?")) return;
                            ingestTx([a.id]);
                          }}
                          disabled={busy}
                          style={{ marginRight: 6 }}
                        >
                          Txs
                        </button>
                        <button onClick={() => beginEdit(a)} disabled={busy} style={{ marginRight: 6 }}>
                          Edit
                        </button>
                        <button onClick={() => deleteAddress(a.id)} disabled={busy}>
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                  {(addresses || []).length === 0 && (
                    <tr>
                      <td colSpan={9} style={{ padding: 8, opacity: 0.7 }}>
                        No addresses yet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {tab === "balances" && (
        <>
          <div style={{ marginBottom: 12, padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Balances Controls</div>
            <div style={{ display: "grid", gridTemplateColumns: "140px 1fr 140px 1fr", gap: 8 }}>
              <label>With prices</label>
              <input type="checkbox" checked={withPrices} onChange={(e) => setWithPrices(e.target.checked)} />

              <label>Limit</label>
              <input
                type="number"
                value={balLimit}
                onChange={(e) => setBalLimit(Number(e.target.value || 2000))}
                min={1}
                max={5000}
              />
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <button onClick={loadBalances} disabled={busy}>
                Load latest
              </button>
              <button onClick={() => refreshBalances(null)} disabled={busy}>
                Refresh all balances
              </button>

              {balLastRefresh ? (
                <div style={{ opacity: 0.85 }}>
                  Last refresh: <b>{Number(balLastRefresh.refreshed || 0)}</b> refreshed •{" "}
                  <b>{Array.isArray(balLastRefresh.errors) ? balLastRefresh.errors.length : 0}</b> errors
                  {balLastRefresh?.hydration_live?.attempted ? (
                    <span>
                      {" "}• Hydration live: <b>{Number(balLastRefresh.hydration_live.refreshed || 0)}</b> wallets / <b>{Number(balLastRefresh.hydration_live.rows || 0)}</b> assets
                    </span>
                  ) : null}
                </div>
              ) : (
                <div style={{ opacity: 0.75 }}>
                  Explorer refresh may take time per address. Hydration/SubWallet <code>ALL</code> rows use the live Polkadot-Hydration balance endpoint instead of the legacy explorer snapshot path.
                </div>
              )}
            </div>

            {balLastRefresh && Array.isArray(balLastRefresh.errors) && balLastRefresh.errors.length > 0 ? (
              <div style={{ marginTop: 10, paddingTop: 10, borderTop: "1px solid #eee" }}>
                <div style={{ fontWeight: 600, marginBottom: 6, opacity: 0.9 }}>Refresh errors</div>
                <div style={{ maxHeight: 140, overflow: "auto", fontSize: 12, opacity: 0.95 }}>
                  {balLastRefresh.errors.map((er, idx) => {
                    const asset = er?.asset || "";
                    const network = er?.network || "";
                    const addr = er?.address || "";
                    const msg = er?.error || er?.message || er?.detail || JSON.stringify(er);
                    return (
                      <div key={(er?.id || "") + idx} style={{ marginBottom: 8 }}>
                        <div>
                          <b>{asset}</b> {network ? `(${network})` : ""} {addr ? `• ${redacted(addr)}` : ""}
                        </div>
                        <div style={{ opacity: 0.85, marginTop: 2, whiteSpace: "pre-wrap" }}>{String(msg)}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : null}
          </div>

          <div style={{ padding: 10, border: "1px solid #ddd" }}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Latest Balances</div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ borderCollapse: "collapse", width: "100%" }}>
                <thead>
                  <tr>
                    {["id", "asset", "network", "address", "label", "balance", "usd_price", "usd_value", "usd_source_symbol", "fetched_at"].map((h) => (
                      <th key={h} style={{ textAlign: "left", borderBottom: "1px solid #ccc", padding: 6 }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(balances || []).map((b) => (
                    <tr key={`${b.id}:${b.fetched_at || ""}`}>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.id}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.asset || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.network || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee", fontFamily: "monospace" }}>{redacted(b.address || "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.label || ""}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.balance ?? "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.usd_price ?? "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.usd_value ?? "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{String(b.usd_source_symbol ?? b.usd_source ?? b.price_source ?? "")}</td>
                      <td style={{ padding: 6, borderBottom: "1px solid #eee" }}>{b.fetched_at || ""}</td>
                    </tr>
                  ))}
                  {(balances || []).length === 0 && (
                    <tr>
                      <td colSpan={9} style={{ padding: 8, opacity: 0.7 }}>
                        No balance snapshots yet (try “Refresh all” first).
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
