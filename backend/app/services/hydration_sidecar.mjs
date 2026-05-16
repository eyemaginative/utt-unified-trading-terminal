#!/usr/bin/env node

import http from 'node:http';
import process from 'node:process';

function jsonReplacer(_key, value) {
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Uint8Array) return `0x${Buffer.from(value).toString('hex')}`;
  return value;
}

function send(res, status, obj) {
  const body = `${JSON.stringify(obj, jsonReplacer)}\n`;
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  });
  res.end(body);
}

function redactWs(url) {
  const u = String(url || '').trim();
  if (!u) return '';
  const parts = u.replace(/\/+$/, '').split('/');
  if (parts.length >= 4) {
    parts[parts.length - 1] = '***';
    return parts.join('/');
  }
  return u;
}

function trace(stage, extra = {}) {
  try {
    process.stderr.write(JSON.stringify({ stage, t: new Date().toISOString(), ...extra }, jsonReplacer) + "\n");
  } catch (_e) {}
}

function stageTimeoutMs(req) {
  const n = Number(req?.stepTimeoutS ?? 30);
  return Number.isFinite(n) && n > 0 ? Math.max(1000, Math.trunc(n * 1000)) : 30000;
}

function boolFromReq(v) {
  if (typeof v === 'boolean') return v;
  if (typeof v === 'number') return v !== 0;
  const s = String(v ?? '').trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'y' || s === 'on';
}

function withStageTimeout(promise, ms, stage, extra = {}) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const err = new Error(`Hydration sidecar stage timed out: ${stage}`);
      err.code = 'hydration_sidecar_stage_timeout';
      err.stage = stage;
      err.timeoutMs = ms;
      err.extra = extra;
      reject(err);
    }, ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function parseAtomicBigInt(value) {
  const s = String(value ?? '').trim();
  if (!s) throw new Error('amountInAtomic is required');
  if (!/^[0-9]+$/.test(s)) throw new Error(`amountInAtomic must be a non-negative integer string, got: ${s}`);
  return BigInt(s);
}

function toSerializable(v, seen = new WeakSet()) {
  if (v == null) return v;
  if (typeof v === 'bigint') return v.toString();
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
  if (v instanceof Uint8Array) return `0x${Buffer.from(v).toString('hex')}`;
  if (Array.isArray(v)) return v.slice(0, 25).map((x) => toSerializable(x, seen));
  if (typeof v === 'object') {
    if (seen.has(v)) return '[Circular]';
    seen.add(v);
    const out = {};
    for (const [k, val] of Object.entries(v).slice(0, 80)) {
      if (typeof val === 'function') continue;
      out[k] = toSerializable(val, seen);
    }
    return out;
  }
  return String(v);
}

function encodedDataToHex(v) {
  if (v == null) return null;
  try {
    if (typeof v === 'string') return v.startsWith('0x') ? v : `0x${v}`;
    if (v instanceof Uint8Array) return `0x${Buffer.from(v).toString('hex')}`;
    if (typeof v?.toHex === 'function') return v.toHex();
    if (typeof v?.asBytes === 'function') return `0x${Buffer.from(v.asBytes()).toString('hex')}`;
    if (papiPkg?.Binary && typeof papiPkg.Binary.toHex === 'function') {
      try { return papiPkg.Binary.toHex(v); } catch (_e) {}
    }
    const s = typeof v?.toString === 'function' ? v.toString() : '';
    if (s && s !== '[object Object]') return s.startsWith('0x') ? s : `0x${s}`;
  } catch (_e) {}
  return null;
}

function safeToHuman(v) {
  try {
    if (v && typeof v.toHuman === 'function') return v.toHuman();
  } catch (_e) {}
  return null;
}

function safeToString(v) {
  try {
    if (v == null) return null;
    if (typeof v === 'bigint') return v.toString();
    if (typeof v === 'number') return Number.isFinite(v) ? String(v) : null;
    if (typeof v === 'string') return v;
    if (typeof v.toString === 'function') {
      const s = v.toString();
      if (s && s !== '[object Object]') return s;
    }
  } catch (_e) {}
  return null;
}

function amountString(v, depth = 0) {
  if (depth > 4 || v == null) return null;
  if (typeof v === 'bigint') return v.toString();
  if (typeof v === 'number') return Number.isFinite(v) ? String(Math.trunc(v)) : null;
  if (typeof v === 'string') {
    const m = v.replace(/,/g, '').match(/-?\d+(?:\.\d+)?/);
    return m ? m[0] : null;
  }
  if (typeof v === 'object') {
    const direct = safeToString(v);
    if (direct && direct !== '[object Object]') {
      const m = direct.replace(/,/g, '').match(/-?\d+(?:\.\d+)?/);
      if (m) return m[0];
    }
    for (const k of ['amountOut', 'outAmount', 'outputAmount', 'calculatedOut', 'amount', 'value', 'raw', 'balance', 'free', 'num']) {
      if (Object.prototype.hasOwnProperty.call(v, k)) {
        const got = amountString(v[k], depth + 1);
        if (got != null) return got;
      }
    }
  }
  return null;
}

function parseHumanAmount(v) {
  if (v == null) return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string') {
    const m = v.replace(/,/g, '').match(/-?\d+(?:\.\d+)?/);
    return m ? Number(m[0]) : null;
  }
  if (Array.isArray(v)) {
    for (const x of v) {
      const got = parseHumanAmount(x);
      if (got != null) return got;
    }
  }
  if (typeof v === 'object') {
    for (const k of ['amountOut', 'outAmount', 'outputAmount', 'amount', 'value']) {
      if (Object.prototype.hasOwnProperty.call(v, k)) {
        const got = parseHumanAmount(v[k]);
        if (got != null) return got;
      }
    }
  }
  return null;
}

function pickAmountOut(trade, human) {
  const candidates = [
    trade?.amountOut,
    trade?.outAmount,
    trade?.outputAmount,
    trade?.calculatedOut,
    trade?.to?.amountOut,
    trade?.to?.amount,
    trade?.amounts?.out,
    trade?.trade?.amountOut,
    trade?.result?.amountOut,
  ];
  for (const c of candidates) {
    const got = amountString(c);
    if (got != null) return got;
  }
  const humanCandidates = [
    human?.amountOut,
    human?.outAmount,
    human?.outputAmount,
    human?.to?.amount,
    human?.result?.amountOut,
  ];
  for (const c of humanCandidates) {
    const got = parseHumanAmount(c);
    if (got != null) return { human: got };
  }
  return null;
}

function isUnsupportedSdkAssetError(error) {
  const msg = String(error?.message || error || '');
  return /not supported asset/i.test(msg);
}

function unsupportedSdkAssetDetail(error, ctx = {}) {
  const human = ctx.human || safeToHuman(ctx.trade);
  const stage = ctx.stage || 'swap_tx_build';
  const isRouterStage = /get_best|quote|router/i.test(stage);
  const code = isRouterStage
    ? 'hydration_custom_asset_unsupported_by_sdk_router'
    : 'hydration_custom_asset_unsupported_by_sdk_tx_builder';
  const message = isRouterStage
    ? 'Hydration SDK router quote rejected one of the routed assets as unsupported. The pool can exist on-chain, but sdk-next has not admitted this custom asset into its supported routing registry, so UTT must use a manual Hydration call builder or alternate quote path for this pair.'
    : 'Hydration SDK tx.trade rejected one of the routed assets as unsupported. Quotes can still work for some paths, but this SDK transaction builder cannot build the custom-asset swap transaction.';
  return {
    ok: false,
    error: code,
    message,
    status: 501,
    sdkMessage: String(error?.message || error),
    stage,
    provider: 'galactic_sdk_next_sidecar',
    priceCache: boolFromReq(ctx.priceCache),
    wsUrl: redactWs(currentWsUrl),
    assetInId: ctx.assetInId,
    assetOutId: ctx.assetOutId,
    amountMode: ctx.amountMode,
    amountInAtomic: ctx.amountInAtomic || null,
    amountOutAtomic: ctx.amountOutAtomic || null,
    beneficiary: ctx.beneficiary || null,
    slippageBps: ctx.slippageBps ?? null,
    sidecar: { initCount, supportedAssetsCached: !!supportedAssetsCache },
    quoteFallbackRequired: isRouterStage,
    manualFallbackRequired: true,
    nextRequired: isRouterStage
      ? 'Stop sdk-next swap_tx tests for this custom pair. Decode one successful Hydration UI UTTT-HDX swap and add a manual custom-asset quote/swap builder fallback instead of sdk.api.router.getBestSell/getBestBuy + sdk.tx.trade.'
      : 'Decode one successful Hydration UI swap for this custom asset pair, then add a manual router extrinsic builder fallback instead of sdk.tx.trade(trade).',
    human,
    rawTrade: toSerializable(ctx.trade),
    stack: String(error?.stack || '').split('\n').slice(0, 8).join('\n'),
  };
}

function throwUnsupportedSdkAssetError(error, ctx = {}) {
  if (!isUnsupportedSdkAssetError(error)) throw error;
  const detail = unsupportedSdkAssetDetail(error, ctx);
  const err = new Error(detail.message);
  err.status = 501;
  err.detail = detail;
  throw err;
}

function extractId(asset) {
  if (!asset || typeof asset !== 'object') return null;
  for (const k of ['id', 'assetId', 'asset_id', 'tokenId', 'key']) {
    if (asset[k] != null) {
      const n = Number(asset[k]);
      if (Number.isFinite(n)) return n;
    }
  }
  return null;
}

async function resolveSdkAssetId(sdkObj, meta) {
  const assetId = String(meta?.assetId ?? '').trim();
  const symbol = String(meta?.symbol || '').trim().toUpperCase();

  if (assetId.toLowerCase() === 'native') {
    const fallback = Number(meta?.sdkAssetIdFallback ?? 0);
    if (!Number.isFinite(fallback)) throw new Error(`No SDK asset id fallback for native asset ${symbol || assetId}`);
    return fallback;
  }

  if (assetId) {
    const n = Number(assetId);
    if (!Number.isFinite(n)) throw new Error(`Invalid Hydration SDK asset id for ${meta?.symbol || 'asset'}: ${assetId}`);
    return n;
  }

  const assets = await getSupportedAssets(sdkObj, { stepTimeoutS: 30 });
  if (Array.isArray(assets)) {
    for (const a of assets) {
      const sym = String(a?.symbol || a?.ticker || a?.name || '').trim().toUpperCase();
      if (sym === symbol) {
        const id = extractId(a);
        if (id != null) return id;
      }
    }
  }

  const fallback = Number(meta?.sdkAssetIdFallback ?? 0);
  if (!Number.isFinite(fallback)) throw new Error(`No SDK asset id fallback for asset ${symbol || assetId}`);
  return fallback;
}

function compactAsset(asset) {
  if (!asset || typeof asset !== 'object') return asset;
  const out = {};
  for (const k of ['id', 'assetId', 'asset_id', 'tokenId', 'key', 'symbol', 'ticker', 'name', 'decimals', 'isExternal', 'type']) {
    if (asset[k] != null) out[k] = toSerializable(asset[k]);
  }
  return out;
}

function compactPool(pool) {
  if (!pool || typeof pool !== 'object') return pool;
  const out = {};
  for (const k of ['id', 'address', 'type', 'pool', 'assetId', 'assetIds', 'tokens', 'tokenIn', 'tokenOut', 'shares']) {
    if (pool[k] != null) out[k] = toSerializable(pool[k]);
  }
  return Object.keys(out).length ? out : toSerializable(pool);
}

async function inspectStep(req, stage, fn, extra = {}) {
  const ms = stageTimeoutMs(req);
  trace(`${stage}_start`, extra);
  try {
    const result = await withStageTimeout(Promise.resolve().then(fn), ms, stage, extra);
    trace(`${stage}_done`, { kind: Array.isArray(result) ? 'array' : typeof result, count: Array.isArray(result) ? result.length : null });
    return { ok: true, result: toSerializable(result) };
  } catch (e) {
    trace(`${stage}_failed`, { code: e?.code || null, message: String(e?.message || e).slice(0, 240) });
    return {
      ok: false,
      error: e?.code || 'hydration_inspect_step_failed',
      stage,
      message: String(e?.message || e),
      timeout_s: e?.timeoutMs ? Number(e.timeoutMs) / 1000 : undefined,
      extra: e?.extra || extra,
    };
  }
}

let sdkPkg;
let papiPkg;
let papiWsProviderPkg;
let papiWsProviderSpec = '';
let sdk;
let client;
let currentWsUrl = '';
let initPromise = null;
let supportedAssetsCache = null;
let initCount = 0;

async function loadPackages() {
  if (sdkPkg && papiPkg && papiWsProviderPkg) return;
  trace('sidecar_import_start');
  sdkPkg = await import('@galacticcouncil/sdk-next');
  papiPkg = await import('polkadot-api');

  // polkadot-api export paths differ by package version. Try the Hydration
  // example path first, then fall back to the installed v2.1.x-compatible path.
  let lastProviderImportError = null;
  for (const spec of ['polkadot-api/ws-provider/node', 'polkadot-api/ws']) {
    try {
      const mod = await import(spec);
      if (typeof mod?.getWsProvider === 'function') {
        papiWsProviderPkg = mod;
        papiWsProviderSpec = spec;
        trace('sidecar_import_ws_provider_done', { spec, keys: Object.keys(mod || {}).slice(0, 20) });
        break;
      }
    } catch (providerError) {
      lastProviderImportError = providerError;
      trace('sidecar_import_ws_provider_failed', { spec, message: String(providerError?.message || providerError).slice(0, 240) });
    }
  }
  if (!papiWsProviderPkg) {
    throw lastProviderImportError || new Error('No polkadot-api getWsProvider export found.');
  }

  trace('sidecar_import_done', {
    sdkKeys: Object.keys(sdkPkg || {}).slice(0, 20),
    papiKeys: Object.keys(papiPkg || {}).slice(0, 20),
    wsProviderSpec: papiWsProviderSpec,
    wsProviderKeys: Object.keys(papiWsProviderPkg || {}).slice(0, 20),
  });
}

async function destroyContext() {
  try { await sdk?.destroy?.(); } catch (_e) {}
  try { client?.destroy?.(); } catch (_e) {}
  sdk = null;
  client = null;
  currentWsUrl = '';
  supportedAssetsCache = null;
}

async function ensureContext(wsUrl) {
  const cleanWs = String(wsUrl || '').trim();
  if (!cleanWs) throw new Error('wsUrl is required');
  if (sdk && client && currentWsUrl === cleanWs) return { sdk, client, reused: true };

  if (initPromise) {
    await initPromise;
    if (sdk && client && currentWsUrl === cleanWs) return { sdk, client, reused: true };
  }

  initPromise = (async () => {
    await loadPackages();
    if (sdk || client) await destroyContext();

    const { createSdkContext } = sdkPkg;
    const { createClient } = papiPkg;
    const { getWsProvider } = papiWsProviderPkg;

    trace('sidecar_provider_start', { wsUrl: redactWs(cleanWs), provider: papiWsProviderSpec });
    const provider = getWsProvider(cleanWs);
    trace('sidecar_provider_done', { provider: papiWsProviderSpec });
    client = createClient(provider);
    trace('sidecar_client_created');
    sdk = await createSdkContext(client);
    currentWsUrl = cleanWs;
    supportedAssetsCache = null;
    initCount += 1;
    trace('sidecar_sdk_context_created', { initCount, sdkKeys: Object.keys(sdk || {}).slice(0, 30), apiKeys: Object.keys(sdk?.api || {}).slice(0, 30) });
  })();

  try {
    await initPromise;
  } finally {
    initPromise = null;
  }
  return { sdk, client, reused: false };
}

async function getSupportedAssets(sdkObj, req) {
  if (supportedAssetsCache) return supportedAssetsCache;
  const ms = stageTimeoutMs(req);
  supportedAssetsCache = await withStageTimeout(
    sdkObj?.client?.asset?.getSupported?.(true),
    ms,
    'asset_get_supported',
  );
  return supportedAssetsCache;
}


function manualCustomSwapReq(req) {
  const m = req?.manualCustomSwap;
  return !!(m && typeof m === 'object' && boolFromReq(m.enabled));
}

function requiresRouterQuotes(req) {
  const mode = String(req?.mode || '').trim();
  if (mode === 'price_spot' || mode === 'price_spot_direct') return true;
  if (mode === 'quote_sell') return !manualCustomSwapReq(req);
  if (mode === 'swap_tx') return !manualCustomSwapReq(req);
  return false;
}

function pickSpotPrice(spot, human) {
  const candidates = [
    spot?.price,
    spot?.spotPrice,
    spot?.amount,
    spot?.value,
    spot?.n,
    spot?.result,
    human?.price,
    human?.spotPrice,
    human?.amount,
    human?.value,
  ];
  for (const c of candidates) {
    const got = parseHumanAmount(c);
    if (got != null && Number.isFinite(got) && got > 0) return got;
  }
  const s = safeToString(spot);
  const got = parseHumanAmount(s);
  if (got != null && Number.isFinite(got) && got > 0) return got;
  return null;
}

function routerQuotesDisabledDetail(req, mode = req?.mode) {
  return {
    ok: false,
    error: 'hydration_router_quotes_disabled',
    message: 'Hydration SDK router quote calls are disabled to protect RPC quota. Manual custom swap builders remain available for configured routes.',
    status: 503,
    node: process.versions.node,
    provider: 'galactic_sdk_next_sidecar',
    mode,
    rawSymbol: req?.rawSymbol || null,
    resolvedSymbol: req?.resolvedSymbol || null,
    manualCustomSwap: manualCustomSwapReq(req),
    priceCache: boolFromReq(req?.priceCache),
    enableRouterQuotes: boolFromReq(req?.enableRouterQuotes),
    wsUrl: redactWs(currentWsUrl || req?.wsUrl || ''),
    sidecar: { initCount, supportedAssetsCached: !!supportedAssetsCache },
    blockedMethods: [
      'sdk.api.router.getSpotPrice',
      'sdk-next direct context getSpotPrice',
      'sdk.api.router.getBestSell',
      'sdk.api.router.getBestBuy',
      'sdk.api.router.getRoutes',
      'sdk.api.router.getPools',
      'sdk.api.router.getTradeableAssets',
    ],
    nextRequired: 'Leave UTT_HYDRATION_ENABLE_ROUTER_QUOTES=0 until a lighter quote source, cached price layer, or safe SDK reset strategy is implemented.',
  };
}

function throwRouterQuotesDisabled(req, mode = req?.mode) {
  const detail = routerQuotesDisabledDetail(req, mode);
  const err = new Error(detail.message);
  err.status = 503;
  err.detail = detail;
  throw err;
}


function directAssetIdFromMeta(meta) {
  const assetId = String(meta?.assetId ?? '').trim();
  const symbol = String(meta?.symbol || '').trim().toUpperCase();
  if (assetId.toLowerCase() === 'native') {
    const fallback = Number(meta?.sdkAssetIdFallback ?? 0);
    if (!Number.isFinite(fallback)) throw new Error(`No SDK asset id fallback for native asset ${symbol || assetId}`);
    return fallback;
  }
  if (assetId) {
    const n = Number(assetId);
    if (!Number.isFinite(n)) throw new Error(`Invalid Hydration SDK asset id for ${symbol || 'asset'}: ${assetId}`);
    return n;
  }
  const fallback = Number(meta?.sdkAssetIdFallback ?? NaN);
  if (Number.isFinite(fallback)) return fallback;
  throw new Error(`No direct SDK asset id for asset ${symbol || assetId}`);
}

async function createDirectSdkClient({ sdkPkg, papiPkg, papiWsProviderPkg, wsUrl, tracePrefix = 'direct' }) {
  const apiMod = sdkPkg?.api;
  if (apiMod && typeof apiMod.getWs === 'function') {
    trace(`${tracePrefix}_api_get_ws_start`, { wsUrl: redactWs(wsUrl) });
    const got = await Promise.resolve(apiMod.getWs(wsUrl));
    trace(`${tracePrefix}_api_get_ws_done`, { kind: typeof got, keys: Object.keys(got || {}).slice(0, 30) });
    if (got && (typeof got.getUnsafeApi === 'function' || typeof got.destroy === 'function')) {
      return { client: got, provider: null, source: '@galacticcouncil/sdk-next/api.getWs:client' };
    }
    if (papiPkg && typeof papiPkg.createClient === 'function') {
      try {
        const client = papiPkg.createClient(got);
        return { client, provider: got, source: '@galacticcouncil/sdk-next/api.getWs:provider+polkadot-api/createClient' };
      } catch (e) {
        trace(`${tracePrefix}_api_get_ws_create_client_failed`, { message: String(e?.message || e).slice(0, 240) });
      }
    }
  }

  if (!papiWsProviderPkg || typeof papiWsProviderPkg.getWsProvider !== 'function' || !papiPkg || typeof papiPkg.createClient !== 'function') {
    throw new Error('No direct SDK client factory is available. Expected sdk-next api.getWs or polkadot-api getWsProvider/createClient.');
  }
  trace(`${tracePrefix}_fallback_provider_start`, { wsUrl: redactWs(wsUrl) });
  const provider = papiWsProviderPkg.getWsProvider(wsUrl);
  const client = papiPkg.createClient(provider);
  trace(`${tracePrefix}_fallback_provider_done`);
  return { client, provider, source: 'polkadot-api/ws-provider+createClient' };
}

async function runDirectTradeRouterSpot({ sdkPkg, papiPkg, papiWsProviderPkg, wsUrl, assetInId, assetOutId, timeoutMs, priceCache }) {
  if (!sdkPkg?.api?.getWs || !sdkPkg?.createSdkContext || !papiPkg?.createClient) {
    throw new Error(`sdk-next context spot exports missing. sdk keys: ${Object.keys(sdkPkg || {}).slice(0, 40).join(', ')} papi keys: ${Object.keys(papiPkg || {}).slice(0, 40).join(', ')}`);
  }

  let provider = null;
  let directClient = null;
  let sdkContext = null;
  const source = '@galacticcouncil/sdk-next/api.getWs+createSdkContext';
  try {
    trace('direct_context_provider_start', { assetInId, assetOutId, wsUrl: redactWs(wsUrl), source });
    provider = sdkPkg.api.getWs(wsUrl);
    directClient = papiPkg.createClient(provider);
    trace('direct_context_client_created', { source });

    sdkContext = await withStageTimeout(
      Promise.resolve().then(() => sdkPkg.createSdkContext(directClient)),
      timeoutMs,
      'direct_create_sdk_context',
      { assetInId, assetOutId, source },
    );
    trace('direct_context_sdk_created', {
      source,
      sdkKeys: Object.keys(sdkContext || {}).slice(0, 30),
      apiKeys: Object.keys(sdkContext?.api || {}).slice(0, 30),
      ctxKeys: Object.keys(sdkContext?.ctx || {}).slice(0, 30),
    });

    const router = sdkContext?.api?.router;
    if (!router || typeof router.getSpotPrice !== 'function') {
      throw new Error(`sdkContext.api.router.getSpotPrice is not available. api keys: ${Object.keys(sdkContext?.api || {}).slice(0, 40).join(', ')}`);
    }

    trace('direct_context_get_spot_price_start', { assetInId, assetOutId, timeoutMs, source });
    const spot = await withStageTimeout(
      Promise.resolve().then(() => router.getSpotPrice(assetInId, assetOutId)),
      timeoutMs,
      'direct_get_spot_price',
      { assetInId, assetOutId, source },
    );
    trace('direct_context_get_spot_price_done', { spotKeys: Object.keys(spot || {}).slice(0, 30), source });
    const human = safeToHuman(spot);
    const spotPrice = pickSpotPrice(spot, human);
    return {
      ok: true,
      mode: 'price_spot_direct',
      provider: 'galactic_sdk_next_context_spot',
      directClientSource: source,
      priceCache: boolFromReq(priceCache),
      wsUrl: redactWs(wsUrl),
      assetInId,
      assetOutId,
      spotPrice,
      price: spotPrice,
      human,
      raw: toSerializable(spot),
    };
  } finally {
    try { await sdkContext?.destroy?.(); } catch (_e) {}
    try { await directClient?.destroy?.(); } catch (_e) {}
    try { await provider?.destroy?.(); } catch (_e) {}
  }
}

function toBigIntString(value, field) {
  const s = String(value ?? '').replace(/,/g, '').trim();
  if (!/^[0-9]+$/.test(s)) throw new Error(`${field} must be a non-negative integer string, got: ${s}`);
  return BigInt(s);
}

function manualRouteVariants(route) {
  const legs = Array.isArray(route) ? route : [];
  const normalized = legs.map((leg) => ({
    pool: String(leg?.pool || 'XYK').trim() || 'XYK',
    assetIn: Number(leg?.assetIn ?? leg?.asset_in),
    assetOut: Number(leg?.assetOut ?? leg?.asset_out),
  }));
  return [
    normalized.map((leg) => ({ pool: { type: leg.pool, value: undefined }, assetIn: leg.assetIn, assetOut: leg.assetOut })),
    normalized.map((leg) => ({ pool: { type: leg.pool }, assetIn: leg.assetIn, assetOut: leg.assetOut })),
    normalized.map((leg) => ({ pool: leg.pool, assetIn: leg.assetIn, assetOut: leg.assetOut })),
    normalized.map((leg) => ({ pool: { type: leg.pool, value: undefined }, asset_in: leg.assetIn, asset_out: leg.assetOut })),
    normalized.map((leg) => ({ pool: leg.pool, asset_in: leg.assetIn, asset_out: leg.assetOut })),
  ];
}

async function txLikeToEncodedHex(txLike) {
  const tx = await Promise.resolve(txLike);
  const candidates = [];
  if (tx != null) candidates.push(tx);
  if (typeof tx?.build === 'function') candidates.push(await Promise.resolve(tx.build()));
  if (typeof tx?.get === 'function') candidates.push(await Promise.resolve(tx.get()));
  for (const c of candidates) {
    if (!c) continue;
    if (typeof c?.getEncodedData === 'function') {
      const got = await Promise.resolve(c.getEncodedData());
      const hex = encodedDataToHex(got);
      if (hex) return hex;
    }
    const hex = encodedDataToHex(c?.encodedData || c?.callData || c?.transactionData || c);
    if (hex) return hex;
  }
  return null;
}

async function buildManualHydrationRouterSwapTx({ client, req, assetInId, assetOutId }) {
  const manual = req?.manualCustomSwap || {};
  const method = String(manual.method || '').trim().toLowerCase();
  if (!['sell', 'buy'].includes(method)) throw new Error(`Unsupported manual router method: ${method}`);
  const beneficiary = String(req.beneficiary || req.userPubkey || '').trim();
  if (!beneficiary) throw new Error('beneficiary/userPubkey is required for manual Hydration swap builder.');

  const unsafeApi = client?.getUnsafeApi?.();
  if (!unsafeApi?.tx) throw new Error('polkadot-api unsafe tx builder is not available.');
  const routerPallet = unsafeApi.tx.Router || unsafeApi.tx.router;
  const fn = routerPallet?.[method];
  if (typeof fn !== 'function') {
    throw new Error(`Hydration Router.${method} tx builder is not available in unsafe API. tx pallets: ${Object.keys(unsafeApi.tx || {}).slice(0, 50).join(', ')}`);
  }

  const routeVariants = manualRouteVariants(manual.route);
  const attempts = [];
  const callVariants = [];
  const aIn = Number(manual.assetInId ?? assetInId);
  const aOut = Number(manual.assetOutId ?? assetOutId);

  for (const route of routeVariants) {
    if (method === 'sell') {
      const amountIn = toBigIntString(manual.amountInAtomic, 'manualCustomSwap.amountInAtomic');
      const minAmountOut = toBigIntString(manual.minAmountOutAtomic, 'manualCustomSwap.minAmountOutAtomic');
      callVariants.push({ kind: 'snake-object', value: { asset_in: aIn, asset_out: aOut, amount_in: amountIn, min_amount_out: minAmountOut, route } });
      callVariants.push({ kind: 'camel-object', value: { assetIn: aIn, assetOut: aOut, amountIn, minAmountOut, route } });
      callVariants.push({ kind: 'positional', value: [aIn, aOut, amountIn, minAmountOut, route] });
    } else {
      const amountOut = toBigIntString(manual.amountOutAtomic, 'manualCustomSwap.amountOutAtomic');
      const maxAmountIn = toBigIntString(manual.maxAmountInAtomic, 'manualCustomSwap.maxAmountInAtomic');
      callVariants.push({ kind: 'snake-object', value: { asset_in: aIn, asset_out: aOut, amount_out: amountOut, max_amount_in: maxAmountIn, route } });
      callVariants.push({ kind: 'camel-object', value: { assetIn: aIn, assetOut: aOut, amountOut, maxAmountIn, route } });
      callVariants.push({ kind: 'positional', value: [aIn, aOut, amountOut, maxAmountIn, route] });
    }
  }

  for (const variant of callVariants) {
    try {
      const built = Array.isArray(variant.value)
        ? await Promise.resolve(fn.apply(routerPallet, variant.value))
        : await Promise.resolve(fn.call(routerPallet, variant.value));
      const encodedHex = await txLikeToEncodedHex(built);
      if (encodedHex) {
        return {
          ok: true,
          mode: 'swap_tx',
          provider: 'manual_papi_router',
          manualCustomSwap: true,
          manualCustomSwapReason: manual.reason || 'manual custom-asset Router fallback',
          method,
          section: 'router',
          assetInId: aIn,
          assetOutId: aOut,
          amountMode: manual.amountMode || req.amountMode || req.amount_mode || null,
          amountInAtomic: manual.amountInAtomic || null,
          amountOutAtomic: manual.amountOutAtomic || null,
          minAmountOutAtomic: manual.minAmountOutAtomic || null,
          maxAmountInAtomic: manual.maxAmountInAtomic || null,
          estimatedAmountOutAtomic: manual.estimatedAmountOutAtomic || null,
          estimatedAmountOutUi: manual.estimatedAmountOutUi ?? null,
          estimatedAmountInAtomic: manual.estimatedAmountInAtomic || null,
          estimatedAmountInUi: manual.estimatedAmountInUi ?? null,
          route: manual.route || [],
          beneficiary,
          slippageBps: manual.slippageBps ?? req.slippageBps ?? null,
          encodedCallData: encodedHex,
          transactionData: encodedHex,
          signed: false,
          submitted: false,
          builderVariant: variant.kind,
          args: toSerializable(variant.value),
        };
      }
      attempts.push({ kind: variant.kind, encoded: false, built: toSerializable(built) });
    } catch (e) {
      attempts.push({ kind: variant.kind, error: String(e?.message || e).slice(0, 500) });
    }
  }

  const err = new Error('Manual Hydration Router call builder could not encode call data.');
  err.status = 502;
  err.detail = {
    ok: false,
    error: 'hydration_manual_router_call_build_failed',
    message: err.message,
    provider: 'manual_papi_router',
    method,
    section: 'router',
    assetInId: aIn,
    assetOutId: aOut,
    route: manual.route || [],
    attempts: attempts.slice(0, 25),
    txPalletKeys: Object.keys(unsafeApi.tx || {}).slice(0, 80),
    routerKeys: Object.keys(routerPallet || {}).slice(0, 80),
    nextRequired: 'Use the attempts/routerKeys output to adjust the PAPI unsafe Router call shape, or switch to explicit SCALE call encoding once the Hydration Router pallet/call indexes are confirmed.',
  };
  throw err;
}

async function handleInspect(req) {
  const { sdk: sdkObj, reused } = await ensureContext(req.wsUrl);

  trace('sidecar_resolve_asset_in_start', { assetIn: req.assetIn });
  const assetInId = await resolveSdkAssetId(sdkObj, req.assetIn);
  trace('sidecar_resolve_asset_in_done', { assetInId });
  trace('sidecar_resolve_asset_out_start', { assetOut: req.assetOut });
  const assetOutId = await resolveSdkAssetId(sdkObj, req.assetOut);
  trace('sidecar_resolve_asset_out_done', { assetOutId });

  const router = sdkObj?.api?.router;
  const assetClient = sdkObj?.client?.asset;

  const supportedRaw = await inspectStep(req, 'asset_get_supported', () => getSupportedAssets(sdkObj, req));
  const supported = supportedRaw.ok && Array.isArray(supportedRaw.result)
    ? supportedRaw.result.map(compactAsset).slice(0, 250)
    : supportedRaw;
  const symbolHits = Array.isArray(supported)
    ? supported.filter((a) => {
        const sym = String(a?.symbol || a?.ticker || a?.name || '').trim().toUpperCase();
        return sym === String(req.assetIn?.symbol || '').trim().toUpperCase()
          || sym === String(req.assetOut?.symbol || '').trim().toUpperCase();
      })
    : [];

  const heavyInspect = boolFromReq(req.enableHeavyInspect);
  const inspectModeRaw = String(req.inspectMode || (heavyInspect ? 'full' : 'light')).trim().toLowerCase();
  const inspectMode = ['light', 'spot', 'routes', 'full'].includes(inspectModeRaw) ? inspectModeRaw : 'light';
  const skipped = (reason) => ({ ok: false, skipped: true, reason });

  if (!heavyInspect || inspectMode === 'light') {
    return {
      ok: true,
      mode: 'inspect',
      inspectMode: 'light',
      provider: 'galactic_sdk_next_sidecar',
      wsProviderImport: papiWsProviderSpec,
      wsUrl: redactWs(currentWsUrl),
      sidecar: { reused, initCount, supportedAssetsCached: !!supportedAssetsCache },
      heavyInspectSkipped: true,
      message: 'Heavy Hydration router inspection is disabled. Set enableHeavyInspect=true / UTT_HYDRATION_ENABLE_HEAVY_INSPECT=1 and inspectMode=spot/routes/full to run selected router checks.',
      requested: {
        symbol: req.rawSymbol || req.resolvedSymbol || null,
        assetIn: req.assetIn,
        assetOut: req.assetOut,
      },
      resolved: { assetInId, assetOutId },
      routerKeys: Object.keys(router || {}).slice(0, 60),
      assetClientKeys: Object.keys(assetClient || {}).slice(0, 60),
      supportedAssets: supported,
      symbolHits,
      tradeableAssets: skipped('heavy_inspect_disabled'),
      pools: skipped('heavy_inspect_disabled'),
      routesForward: skipped('heavy_inspect_disabled'),
      routesReverse: skipped('heavy_inspect_disabled'),
      spotForward: skipped('heavy_inspect_disabled'),
      spotReverse: skipped('heavy_inspect_disabled'),
    };
  }

  let tradeable = skipped(`${inspectMode}_inspect_mode`);
  let pools = skipped(`${inspectMode}_inspect_mode`);
  let routesForward = skipped(`${inspectMode}_inspect_mode`);
  let routesReverse = skipped(`${inspectMode}_inspect_mode`);
  let spotForward = skipped(`${inspectMode}_inspect_mode`);
  let spotReverse = skipped(`${inspectMode}_inspect_mode`);

  if (inspectMode === 'routes' || inspectMode === 'full') {
    tradeable = await inspectStep(req, 'router_get_tradeable_assets', () => router?.getTradeableAssets?.());
    const poolsRaw = await inspectStep(req, 'router_get_pools', () => router?.getPools?.());
    pools = poolsRaw.ok && Array.isArray(poolsRaw.result)
      ? { ok: true, result: poolsRaw.result.map(compactPool).slice(0, 80), count: poolsRaw.result.length }
      : poolsRaw;
    routesForward = await inspectStep(req, 'router_get_routes_forward', () => router?.getRoutes?.(assetInId, assetOutId), { assetInId, assetOutId });
    routesReverse = await inspectStep(req, 'router_get_routes_reverse', () => router?.getRoutes?.(assetOutId, assetInId), { assetInId: assetOutId, assetOutId: assetInId });
  }

  if (inspectMode === 'spot' || inspectMode === 'full') {
    spotForward = await inspectStep(req, 'router_get_spot_price_forward', () => router?.getSpotPrice?.(assetInId, assetOutId), { assetInId, assetOutId });
    spotReverse = await inspectStep(req, 'router_get_spot_price_reverse', () => router?.getSpotPrice?.(assetOutId, assetInId), { assetInId: assetOutId, assetOutId: assetInId });
  }

  return {
    ok: true,
    mode: 'inspect',
    inspectMode,
    provider: 'galactic_sdk_next_sidecar',
    wsUrl: redactWs(currentWsUrl),
    wsProviderImport: papiWsProviderSpec,
    sidecar: { reused, initCount, supportedAssetsCached: !!supportedAssetsCache },
    requested: {
      symbol: req.rawSymbol || req.resolvedSymbol || null,
      assetIn: req.assetIn,
      assetOut: req.assetOut,
    },
    resolved: { assetInId, assetOutId },
    routerKeys: Object.keys(router || {}).slice(0, 60),
    assetClientKeys: Object.keys(assetClient || {}).slice(0, 60),
    supportedAssets: supported,
    symbolHits,
    tradeableAssets: tradeable,
    pools,
    routesForward,
    routesReverse,
    spotForward,
    spotReverse,
  };
}

async function handlePriceSpot(req) {
  if (!boolFromReq(req.enableRouterQuotes)) {
    throwRouterQuotesDisabled(req, 'price_spot');
  }

  const { sdk: sdkObj, reused } = await ensureContext(req.wsUrl);

  const assetInId = await resolveSdkAssetId(sdkObj, req.assetIn);
  const assetOutId = await resolveSdkAssetId(sdkObj, req.assetOut);
  const quoteTimeoutMs = stageTimeoutMs(req);

  trace('sidecar_get_spot_price_start', { assetInId, assetOutId, quoteTimeoutMs });
  let spot;
  try {
    spot = await withStageTimeout(
      sdkObj.api.router.getSpotPrice(assetInId, assetOutId),
      quoteTimeoutMs,
      'get_spot_price',
      { assetInId, assetOutId },
    );
  } catch (e) {
    throwUnsupportedSdkAssetError(e, {
      assetInId,
      assetOutId,
      amountMode: 'spot',
      amountInAtomic: null,
      amountOutAtomic: null,
      beneficiary: null,
      slippageBps: null,
      trade: null,
      stage: 'get_spot_price',
      priceCache: boolFromReq(req.priceCache),
    });
  }
  trace('sidecar_get_spot_price_done', { spotKeys: Object.keys(spot || {}).slice(0, 30) });

  const human = safeToHuman(spot);
  const spotPrice = pickSpotPrice(spot, human);
  return {
    ok: true,
    mode: 'price_spot',
    provider: 'galactic_sdk_next_sidecar',
    priceCache: boolFromReq(req.priceCache),
    wsUrl: redactWs(currentWsUrl),
    wsProviderImport: papiWsProviderSpec,
    sidecar: { reused, initCount, supportedAssetsCached: !!supportedAssetsCache },
    assetInId,
    assetOutId,
    spotPrice,
    price: spotPrice,
    human,
    raw: toSerializable(spot),
  };
}


async function handlePriceSpotDirect(req) {
  if (!boolFromReq(req.enableRouterQuotes)) {
    throwRouterQuotesDisabled(req, 'price_spot_direct');
  }
  await loadPackages();

  const wsUrl = String(req.wsUrl || currentWsUrl || '').trim();
  if (!wsUrl) {
    const err = new Error('wsUrl is required for price_spot_direct.');
    err.status = 422;
    err.detail = { ok: false, error: 'missing_ws_url', message: err.message, status: 422 };
    throw err;
  }
  const assetInId = directAssetIdFromMeta(req.assetIn);
  const assetOutId = directAssetIdFromMeta(req.assetOut);
  const quoteTimeoutMs = stageTimeoutMs(req);
  try {
    return await runDirectTradeRouterSpot({
      sdkPkg,
      papiPkg,
      papiWsProviderPkg,
      wsUrl,
      assetInId,
      assetOutId,
      timeoutMs: quoteTimeoutMs,
      priceCache: req.priceCache,
    });
  } catch (e) {
    throwUnsupportedSdkAssetError(e, {
      assetInId,
      assetOutId,
      amountMode: 'spot',
      amountInAtomic: null,
      amountOutAtomic: null,
      beneficiary: null,
      slippageBps: null,
      trade: null,
      stage: 'direct_get_spot_price',
      priceCache: boolFromReq(req.priceCache),
    });
  }
}

async function handleQuoteSell(req) {
  if (!manualCustomSwapReq(req) && !boolFromReq(req.enableRouterQuotes)) {
    throwRouterQuotesDisabled(req, 'quote_sell');
  }

  const { sdk: sdkObj, reused } = await ensureContext(req.wsUrl);

  const assetInId = await resolveSdkAssetId(sdkObj, req.assetIn);
  const assetOutId = await resolveSdkAssetId(sdkObj, req.assetOut);

  if (manualCustomSwapReq(req)) {
    if (!boolFromReq(req.enableSwapTx)) {
      const err = new Error('Hydration manual swap transaction building is disabled.');
      err.status = 503;
      err.detail = { ok: false, error: 'hydration_swap_tx_disabled', message: err.message, status: 503, resolved: { assetInId, assetOutId }, manualCustomSwap: true };
      throw err;
    }
    const built = await buildManualHydrationRouterSwapTx({ client, req, assetInId, assetOutId });
    return {
      ...built,
      wsUrl: redactWs(currentWsUrl),
      wsProviderImport: papiWsProviderSpec,
      sidecar: { reused, initCount, supportedAssetsCached: !!supportedAssetsCache },
    };
  }

  if (!boolFromReq(req.enableRouterQuotes)) {
    const err = new Error('Hydration router quote calls are disabled to avoid hammering RPC while rate-limited.');
    err.status = 503;
    err.detail = {
      ok: false,
      error: 'hydration_router_quotes_disabled',
      message: err.message,
      status: 503,
      node: process.versions.node,
      provider: 'galactic_sdk_next_sidecar',
      wsProviderImport: papiWsProviderSpec,
      sidecar: { reused, initCount, supportedAssetsCached: !!supportedAssetsCache },
      resolved: { assetInId, assetOutId },
      wsUrl: redactWs(currentWsUrl),
    };
    throw err;
  }

  const amountInAtomic = String(req.amountInAtomic || '').trim();
  if (!amountInAtomic) {
    const err = new Error('amountInAtomic is required.');
    err.status = 422;
    err.detail = { ok: false, error: 'missing_amount_in_atomic', message: err.message, status: 422 };
    throw err;
  }
  const amountInAtomicBigInt = parseAtomicBigInt(amountInAtomic);
  const quoteTimeoutMs = stageTimeoutMs(req);

  trace('sidecar_get_best_sell_start', {
    assetInId,
    assetOutId,
    amountInAtomic,
    amountType: 'bigint',
    quoteTimeoutMs,
  });
  let trade;
  try {
    trade = await withStageTimeout(
      sdkObj.api.router.getBestSell(assetInId, assetOutId, amountInAtomicBigInt),
      quoteTimeoutMs,
      'get_best_sell',
      { assetInId, assetOutId, amountInAtomic },
    );
  } catch (e) {
    throwUnsupportedSdkAssetError(e, {
      assetInId,
      assetOutId,
      amountMode: 'exact_in',
      amountInAtomic,
      amountOutAtomic: null,
      beneficiary: null,
      slippageBps: null,
      trade: null,
      stage: 'get_best_sell',
    });
  }
  trace('sidecar_get_best_sell_done', { tradeKeys: Object.keys(trade || {}).slice(0, 30) });

  const human = safeToHuman(trade);
  const picked = pickAmountOut(trade, human);

  let amountOutAtomic = null;
  let amountOutUi = null;
  if (typeof picked === 'string') {
    amountOutAtomic = picked;
  } else if (picked && typeof picked === 'object' && picked.human != null) {
    amountOutUi = picked.human;
  }

  return {
    ok: true,
    mode: 'quote_sell',
    provider: 'galactic_sdk_next_sidecar',
    wsUrl: redactWs(currentWsUrl),
    wsProviderImport: papiWsProviderSpec,
    sidecar: { reused, initCount, supportedAssetsCached: !!supportedAssetsCache },
    assetInId,
    assetOutId,
    amountInAtomic,
    amountInUi: req.amountInUi ?? null,
    amountOutAtomic,
    amountOutUi,
    human,
    raw: toSerializable(trade),
  };
}

async function handleSwapTx(req) {
  if (!manualCustomSwapReq(req) && !boolFromReq(req.enableRouterQuotes)) {
    throwRouterQuotesDisabled(req, 'swap_tx');
  }

  const { sdk: sdkObj, reused } = await ensureContext(req.wsUrl);

  const assetInId = await resolveSdkAssetId(sdkObj, req.assetIn);
  const assetOutId = await resolveSdkAssetId(sdkObj, req.assetOut);

  if (!boolFromReq(req.enableRouterQuotes)) {
    const err = new Error('Hydration router quotes are disabled; cannot build a swap transaction without a fresh route quote.');
    err.status = 503;
    err.detail = { ok: false, error: 'hydration_router_quotes_disabled', message: err.message, status: 503, resolved: { assetInId, assetOutId } };
    throw err;
  }
  if (!boolFromReq(req.enableSwapTx)) {
    const err = new Error('Hydration swap transaction building is disabled.');
    err.status = 503;
    err.detail = { ok: false, error: 'hydration_swap_tx_disabled', message: err.message, status: 503, resolved: { assetInId, assetOutId } };
    throw err;
  }

  const amountMode = String(req.amountMode || req.amount_mode || 'exact_in').trim().toLowerCase();
  if (!['exact_in', 'exact_out'].includes(amountMode)) {
    const err = new Error('amountMode must be exact_in or exact_out.');
    err.status = 422;
    err.detail = { ok: false, error: 'invalid_amount_mode', message: err.message, status: 422, amountMode };
    throw err;
  }
  if (amountMode === 'exact_out' && !boolFromReq(req.enableExactBuy)) {
    const err = new Error('Hydration exact-out BUY/getBestBuy is temporarily disabled while sidecar timeout behavior is isolated.');
    err.status = 503;
    err.detail = { ok: false, error: 'hydration_exact_buy_disabled', message: err.message, status: 503, amountMode };
    throw err;
  }

  const amountInAtomic = String(req.amountInAtomic || '').trim();
  const requestedAmountOutAtomic = String(req.amountOutAtomic || '').trim();
  if (amountMode === 'exact_in' && !amountInAtomic) {
    const err = new Error('amountInAtomic is required for exact_in swaps.');
    err.status = 422;
    err.detail = { ok: false, error: 'missing_amount_in_atomic', message: err.message, status: 422, amountMode };
    throw err;
  }
  if (amountMode === 'exact_out' && !requestedAmountOutAtomic) {
    const err = new Error('amountOutAtomic is required for exact_out swaps.');
    err.status = 422;
    err.detail = { ok: false, error: 'missing_amount_out_atomic', message: err.message, status: 422, amountMode };
    throw err;
  }
  const beneficiary = String(req.beneficiary || req.userPubkey || '').trim();
  if (!beneficiary) {
    const err = new Error('beneficiary/userPubkey is required.');
    err.status = 422;
    err.detail = { ok: false, error: 'missing_beneficiary', message: err.message, status: 422 };
    throw err;
  }

  const quoteTimeoutMs = stageTimeoutMs(req);
  const slippageBps = Number(req.slippageBps ?? 100);
  const slippagePct = Number.isFinite(slippageBps) ? slippageBps / 100 : 1;

  let trade;
  if (amountMode === 'exact_out') {
    const amountOutAtomicBigInt = parseAtomicBigInt(requestedAmountOutAtomic);
    trace('sidecar_swap_get_best_buy_start', { assetInId, assetOutId, amountOutAtomic: requestedAmountOutAtomic, amountType: 'bigint', quoteTimeoutMs });
    try {
      trade = await withStageTimeout(
        sdkObj.api.router.getBestBuy(assetInId, assetOutId, amountOutAtomicBigInt),
        quoteTimeoutMs,
        'swap_get_best_buy',
        { assetInId, assetOutId, amountOutAtomic: requestedAmountOutAtomic },
      );
    } catch (e) {
      throwUnsupportedSdkAssetError(e, {
        assetInId,
        assetOutId,
        amountMode,
        amountInAtomic,
        amountOutAtomic: requestedAmountOutAtomic,
        beneficiary,
        slippageBps,
        trade: null,
        stage: 'swap_get_best_buy',
      });
    }
    trace('sidecar_swap_get_best_buy_done', { tradeKeys: Object.keys(trade || {}).slice(0, 30) });
  } else {
    const amountInAtomicBigInt = parseAtomicBigInt(amountInAtomic);
    trace('sidecar_swap_get_best_sell_start', { assetInId, assetOutId, amountInAtomic, amountType: 'bigint', quoteTimeoutMs });
    try {
      trade = await withStageTimeout(
        sdkObj.api.router.getBestSell(assetInId, assetOutId, amountInAtomicBigInt),
        quoteTimeoutMs,
        'swap_get_best_sell',
        { assetInId, assetOutId, amountInAtomic },
      );
    } catch (e) {
      throwUnsupportedSdkAssetError(e, {
        assetInId,
        assetOutId,
        amountMode,
        amountInAtomic,
        amountOutAtomic: requestedAmountOutAtomic,
        beneficiary,
        slippageBps,
        trade: null,
        stage: 'swap_get_best_sell',
      });
    }
    trace('sidecar_swap_get_best_sell_done', { tradeKeys: Object.keys(trade || {}).slice(0, 30) });
  }

  if (!sdkObj?.tx || typeof sdkObj.tx.trade !== 'function') {
    const err = new Error('Hydration SDK tx.trade builder is not available.');
    err.status = 502;
    err.detail = { ok: false, error: 'hydration_tx_trade_builder_missing', message: err.message, status: 502, txKeys: Object.keys(sdkObj?.tx || {}).slice(0, 30) };
    throw err;
  }

  const unsupportedCtxBase = {
    assetInId,
    assetOutId,
    amountMode,
    amountInAtomic,
    amountOutAtomic: requestedAmountOutAtomic,
    beneficiary,
    slippageBps,
    trade,
  };

  trace('sidecar_swap_build_start', { beneficiary, slippageBps, slippagePct });
  let txBuilder;
  try {
    txBuilder = sdkObj.tx.trade(trade);
  } catch (e) {
    throwUnsupportedSdkAssetError(e, { ...unsupportedCtxBase, stage: 'sdk_tx_trade' });
  }
  try {
    if (typeof txBuilder?.withBeneficiary === 'function') txBuilder = txBuilder.withBeneficiary(beneficiary);
    if (typeof txBuilder?.withSlippage === 'function') txBuilder = txBuilder.withSlippage(slippagePct);
  } catch (e) {
    throwUnsupportedSdkAssetError(e, { ...unsupportedCtxBase, stage: 'sdk_tx_trade_options' });
  }

  trace('sidecar_swap_tx_build_call_start', { builderKeys: Object.keys(txBuilder || {}).slice(0, 30) });
  let built;
  try {
    built = await withStageTimeout(
      Promise.resolve(typeof txBuilder?.build === 'function' ? txBuilder.build() : txBuilder),
      quoteTimeoutMs,
      'swap_tx_build',
      { assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps },
    );
  } catch (e) {
    throwUnsupportedSdkAssetError(e, { ...unsupportedCtxBase, stage: 'swap_tx_build' });
  }
  trace('sidecar_swap_tx_build_call_done', { builtKeys: Object.keys(built || {}).slice(0, 30) });

  trace('sidecar_swap_tx_get_start', { hasGet: typeof built?.get === 'function' });
  let txObj;
  try {
    txObj = await withStageTimeout(
      Promise.resolve(typeof built?.get === 'function' ? built.get() : built),
      quoteTimeoutMs,
      'swap_tx_get',
      { assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps },
    );
  } catch (e) {
    throwUnsupportedSdkAssetError(e, { ...unsupportedCtxBase, stage: 'swap_tx_get' });
  }
  trace('sidecar_swap_tx_get_done', { txKeys: Object.keys(txObj || {}).slice(0, 30) });

  trace('sidecar_swap_tx_get_encoded_data_start', { hasGetEncodedData: typeof txObj?.getEncodedData === 'function' });
  let encoded;
  try {
    encoded = await withStageTimeout(
      Promise.resolve(typeof txObj?.getEncodedData === 'function' ? txObj.getEncodedData() : txObj?.encodedData || txObj?.callData || null),
      quoteTimeoutMs,
      'swap_tx_get_encoded_data',
      { assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps },
    );
  } catch (e) {
    throwUnsupportedSdkAssetError(e, { ...unsupportedCtxBase, stage: 'swap_tx_get_encoded_data' });
  }
  const encodedHex = encodedDataToHex(encoded);
  trace('sidecar_swap_build_done', { builtKeys: Object.keys(built || {}).slice(0, 30), txKeys: Object.keys(txObj || {}).slice(0, 30), hasEncodedHex: !!encodedHex });

  if (!encodedHex) {
    const err = new Error('Hydration SDK built a trade transaction, but no encoded call data could be extracted.');
    err.status = 502;
    err.detail = {
      ok: false,
      error: 'hydration_swap_tx_encoded_data_missing',
      message: err.message,
      status: 502,
      built: toSerializable(built),
      tx: toSerializable(txObj),
    };
    throw err;
  }

  const human = safeToHuman(trade);
  const picked = pickAmountOut(trade, human);
  let amountOutAtomic = null;
  let amountOutUi = null;
  if (typeof picked === 'string') amountOutAtomic = picked;
  else if (picked && typeof picked === 'object' && picked.human != null) amountOutUi = picked.human;

  return {
    ok: true,
    mode: 'swap_tx',
    provider: 'galactic_sdk_next_sidecar',
    wsUrl: redactWs(currentWsUrl),
    wsProviderImport: papiWsProviderSpec,
    sidecar: { reused, initCount, supportedAssetsCached: !!supportedAssetsCache },
    assetInId,
    assetOutId,
    amountMode,
    amountInAtomic: amountInAtomic || null,
    amountInUi: req.amountInUi ?? null,
    amountOutAtomic: amountMode === 'exact_out' ? requestedAmountOutAtomic : (amountOutAtomic || null),
    amountOutUi: req.amountOutUi ?? amountOutUi ?? null,
    quotedAmountOutAtomic: amountOutAtomic || null,
    quotedAmountOutUi: amountOutUi,
    beneficiary,
    slippageBps,
    slippagePct,
    encodedCallData: encodedHex,
    transactionData: encodedHex,
    signed: false,
    submitted: false,
    human,
    rawTrade: toSerializable(trade),
  };
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 2_000_000) {
        reject(new Error('request body too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      try {
        resolve(JSON.parse(body || '{}'));
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

const port = Number(process.env.UTT_HYDRATION_SIDECAR_PORT || 8787);
const host = process.env.UTT_HYDRATION_SIDECAR_HOST || '127.0.0.1';

const server = http.createServer(async (req, res) => {
  try {
    const path = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`).pathname;

    if (req.method === 'GET' && path === '/health') {
      send(res, 200, {
        ok: true,
        service: 'hydration_sidecar',
        node: process.versions.node,
        initialized: !!sdk,
        initCount,
        wsUrl: redactWs(currentWsUrl),
        supportedAssetsCached: !!supportedAssetsCache,
        wsProviderImport: papiWsProviderSpec,
      });
      return;
    }

    if (req.method !== 'POST') {
      send(res, 405, { ok: false, error: 'method_not_allowed' });
      return;
    }

    const body = await readJsonBody(req);

    if (path === '/inspect') {
      const out = await handleInspect({ ...body, mode: 'inspect' });
      send(res, 200, out);
      return;
    }

    if (path === '/price_spot') {
      const out = await handlePriceSpot({ ...body, mode: 'price_spot' });
      send(res, 200, out);
      return;
    }

    if (path === '/price_spot_direct') {
      const out = await handlePriceSpotDirect({ ...body, mode: 'price_spot_direct' });
      send(res, 200, out);
      return;
    }

    if (path === '/quote_sell') {
      const out = await handleQuoteSell({ ...body, mode: 'quote_sell' });
      send(res, 200, out);
      return;
    }

    if (path === '/swap_tx') {
      const out = await handleSwapTx({ ...body, mode: 'swap_tx' });
      send(res, 200, out);
      return;
    }

    send(res, 404, { ok: false, error: 'not_found', path });
  } catch (e) {
    const status = Number(e?.status || e?.detail?.status || 502);
    if (e?.code === 'hydration_sidecar_stage_timeout') {
      const timedOutWsUrl = redactWs(currentWsUrl);
      trace('sidecar_stage_timeout_reset_start', { stage: e.stage || 'unknown' });
      try {
        await destroyContext();
        trace('sidecar_stage_timeout_reset_done', { stage: e.stage || 'unknown' });
      } catch (resetError) {
        trace('sidecar_stage_timeout_reset_failed', {
          stage: e.stage || 'unknown',
          message: String(resetError?.message || resetError).slice(0, 240),
        });
      }
      send(res, 504, {
        ok: false,
        error: 'hydration_sidecar_stage_timeout',
        message: String(e?.message || e),
        stage: e.stage || 'unknown',
        timeout_s: Number(e.timeoutMs || 0) / 1000,
        extra: e.extra || {},
        wsUrl: timedOutWsUrl,
        sidecarReset: true,
      });
      return;
    }
    send(res, status, e?.detail || {
      ok: false,
      error: 'hydration_sidecar_error',
      message: String(e?.message || e),
      stack: String(e?.stack || '').split('\n').slice(0, 6).join('\n'),
    });
  }
});

server.listen(port, host, () => {
  trace('sidecar_listening', { host, port, node: process.versions.node });
  process.stderr.write(`Hydration sidecar listening on http://${host}:${port}\n`);
});

async function shutdown() {
  trace('sidecar_shutdown_start');
  try { server.close(); } catch (_e) {}
  await destroyContext();
  trace('sidecar_shutdown_done');
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
