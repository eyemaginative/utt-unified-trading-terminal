#!/usr/bin/env node

import process from 'node:process';

let papiPkg;

function readStdin() {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => { data += chunk; });
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

function jsonReplacer(_key, value) {
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Uint8Array) return `0x${Buffer.from(value).toString('hex')}`;
  return value;
}

function emit(obj, code = 0) {
  process.stdout.write(`${JSON.stringify(obj, jsonReplacer)}\n`);
  process.exit(code);
}

function fail(error, message, extra = {}, code = 1) {
  emit({ ok: false, error, message, ...extra }, code);
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
      const err = new Error(`Hydration helper stage timed out: ${stage}`);
      err.code = 'hydration_helper_stage_timeout';
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

function unsupportedSdkAssetPayload(error, ctx = {}) {
  const human = ctx.human || safeToHuman(ctx.trade);
  const stage = ctx.stage || 'swap_tx_build';
  const isRouterStage = /get_best|quote|router/i.test(stage);
  return {
    status: 501,
    node: process.versions.node,
    provider: 'galactic_sdk_next',
    wsProviderImport: ctx.wsProviderImport || null,
    wsUrl: redactWs(ctx.wsUrl || ''),
    sdkMessage: String(error?.message || error),
    stage,
    assetInId: ctx.assetInId,
    assetOutId: ctx.assetOutId,
    amountMode: ctx.amountMode,
    amountInAtomic: ctx.amountInAtomic || null,
    amountOutAtomic: ctx.amountOutAtomic || null,
    beneficiary: ctx.beneficiary || null,
    slippageBps: ctx.slippageBps ?? null,
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

function failUnsupportedSdkAsset(error, ctx = {}) {
  const stage = ctx.stage || 'swap_tx_build';
  const isRouterStage = /get_best|quote|router/i.test(stage);
  fail(
    isRouterStage ? 'hydration_custom_asset_unsupported_by_sdk_router' : 'hydration_custom_asset_unsupported_by_sdk_tx_builder',
    isRouterStage
      ? 'Hydration SDK router quote rejected one of the routed assets as unsupported. The pool can exist on-chain, but sdk-next has not admitted this custom asset into its supported routing registry, so UTT must use a manual Hydration call builder or alternate quote path for this pair.'
      : 'Hydration SDK tx.trade rejected one of the routed assets as unsupported. Quotes can still work for some paths, but this SDK transaction builder cannot build the custom-asset swap transaction.',
    unsupportedSdkAssetPayload(error, ctx),
    1,
  );
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

async function resolveSdkAssetId(sdk, meta) {
  const assetId = String(meta?.assetId ?? '').trim();
  const symbol = String(meta?.symbol || '').trim().toUpperCase();

  // Native HDX is already known by the router/helper contract.
  // Do not call asset.getSupported(true) for native here; that SDK query can hang
  // and we only need the configured fallback for native Hydration asset id.
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

  try {
    const assets = await sdk?.client?.asset?.getSupported?.(true);
    if (Array.isArray(assets)) {
      for (const a of assets) {
        const sym = String(a?.symbol || a?.ticker || a?.name || '').trim().toUpperCase();
        if (sym === symbol) {
          const id = extractId(a);
          if (id != null) return id;
        }
      }
    }
  } catch (_e) {}

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
    const result = await withStageTimeout(
      Promise.resolve().then(fn),
      ms,
      stage,
      extra,
    );
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


function manualCustomSwapReq(req) {
  const m = req?.manualCustomSwap;
  return !!(m && typeof m === 'object' && boolFromReq(m.enabled));
}

function routerQuotesDisabledDetail(req, mode = req?.mode) {
  return {
    ok: false,
    error: 'hydration_router_quotes_disabled',
    message: 'Hydration SDK router quote calls are disabled to protect RPC quota. Manual custom swap builders remain available for configured routes.',
    status: 503,
    node: process.versions.node,
    provider: 'galactic_sdk_next',
    mode,
    rawSymbol: req?.rawSymbol || null,
    resolvedSymbol: req?.resolvedSymbol || null,
    manualCustomSwap: manualCustomSwapReq(req),
    priceCache: boolFromReq(req?.priceCache),
    enableRouterQuotes: boolFromReq(req?.enableRouterQuotes),
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

async function main() {
  const inputRaw = await readStdin();
  let req;
  try {
    req = JSON.parse(inputRaw || '{}');
  } catch (e) {
    fail('invalid_json', 'Helper stdin must be JSON.', { message: String(e?.message || e) });
  }

  const major = Number(String(process.versions.node || '0').split('.')[0]);
  trace("parsed_request", { mode: req.mode, symbol: req.rawSymbol || req.resolvedSymbol || null, node: process.versions.node });
  if (major < 25) {
    fail(
      'node_version_below_sdk_minimum',
      'Current Galactic SDK README lists Node.js 25+ as the prerequisite for sdk-next. Upgrade Node before relying on live Hydration quotes.',
      { node: process.versions.node, requiredNodeMajor: 25 },
      1,
    );
  }

  const wsUrl = String(req.wsUrl || '').trim();
  if (!wsUrl) fail('missing_ws_url', 'wsUrl is required.');
  trace("ws_ready", { wsUrl: redactWs(wsUrl) });

  if (requiresRouterQuotes(req) && !boolFromReq(req.enableRouterQuotes)) {
    const detail = routerQuotesDisabledDetail(req, req.mode);
    fail(detail.error, detail.message, { ...detail, wsUrl: redactWs(wsUrl) }, 1);
  }

  let sdkPkg;
  papiPkg = null;
  let papiWsProviderPkg;
  let papiWsProviderSpec = '';
  try {
    trace("import_start");
    sdkPkg = await import('@galacticcouncil/sdk-next');
    trace("import_sdk_next_done", { keys: Object.keys(sdkPkg || {}).slice(0, 20) });
    papiPkg = await import('polkadot-api');
    trace("import_polkadot_api_done", { keys: Object.keys(papiPkg || {}).slice(0, 20) });

    // polkadot-api export paths differ by package version. The Hydration example
    // uses polkadot-api/ws-provider/node, while installed v2.1.x may only export
    // polkadot-api/ws. Try the example path first, then fall back.
    let lastProviderImportError = null;
    for (const spec of ['polkadot-api/ws-provider/node', 'polkadot-api/ws']) {
      try {
        const mod = await import(spec);
        if (typeof mod?.getWsProvider === 'function') {
          papiWsProviderPkg = mod;
          papiWsProviderSpec = spec;
          trace("import_papi_ws_provider_done", { spec, keys: Object.keys(mod || {}).slice(0, 20) });
          break;
        }
      } catch (providerError) {
        lastProviderImportError = providerError;
        trace("import_papi_ws_provider_failed", { spec, message: String(providerError?.message || providerError).slice(0, 240) });
      }
    }
    if (!papiWsProviderPkg) {
      throw lastProviderImportError || new Error('No polkadot-api getWsProvider export found.');
    }
  } catch (e) {
    fail(
      'missing_hydration_sdk_dependencies',
      'Install/upgrade Hydration helper dependencies in the backend folder: npm install @galacticcouncil/sdk-next polkadot-api',
      { node: process.versions.node, message: String(e?.message || e) },
      1,
    );
  }

  if (req.mode === 'price_spot_direct') {
    try {
      const assetInId = directAssetIdFromMeta(req.assetIn);
      const assetOutId = directAssetIdFromMeta(req.assetOut);
      const quoteTimeoutMs = stageTimeoutMs(req);
      const result = await runDirectTradeRouterSpot({
        sdkPkg,
        papiPkg,
        papiWsProviderPkg,
        wsUrl,
        assetInId,
        assetOutId,
        timeoutMs: quoteTimeoutMs,
        priceCache: req.priceCache,
      });
      emit({
        ...result,
        wsProviderImport: papiWsProviderSpec,
      });
    } catch (e) {
      if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { wsProviderImport: papiWsProviderSpec, wsUrl, amountMode: 'spot', amountInAtomic: null, amountOutAtomic: null, beneficiary: null, slippageBps: null, trade: null, stage: 'direct_get_spot_price' });
      fail('hydration_direct_spot_failed', 'Direct sdk-next context getSpotPrice failed.', {
        status: e?.status || 502,
        node: process.versions.node,
        provider: 'galactic_sdk_next_context_spot',
        message: String(e?.message || e),
        stack: String(e?.stack || '').split('\n').slice(0, 8).join('\n'),
        wsUrl: redactWs(wsUrl),
      }, 1);
    }
  }

  const { createSdkContext } = sdkPkg;
  const { createClient } = papiPkg;
  const { getWsProvider } = papiWsProviderPkg;
  let sdk;
  let client;
  try {
    trace("provider_start", { provider: papiWsProviderSpec });
    const provider = getWsProvider(wsUrl);
    trace("provider_done", { provider: papiWsProviderSpec });
    client = createClient(provider);
    trace("client_created");
    sdk = await createSdkContext(client);
    trace("sdk_context_created", { sdkKeys: Object.keys(sdk || {}).slice(0, 30), apiKeys: Object.keys(sdk?.api || {}).slice(0, 30) });

    if (req.mode !== 'price_spot' && req.mode !== 'price_spot_direct' && req.mode !== 'quote_sell' && req.mode !== 'inspect' && req.mode !== 'swap_tx') {
      fail('unsupported_mode', 'Only mode=price_spot, mode=price_spot_direct, mode=quote_sell, mode=inspect, or mode=swap_tx is implemented in this helper pass.', { mode: req.mode }, 1);
    }

    trace("resolve_asset_in_start", { assetIn: req.assetIn });
    const assetInId = await resolveSdkAssetId(sdk, req.assetIn);
    trace("resolve_asset_in_done", { assetInId });
    trace("resolve_asset_out_start", { assetOut: req.assetOut });
    const assetOutId = await resolveSdkAssetId(sdk, req.assetOut);
    trace("resolve_asset_out_done", { assetOutId });

    if (req.mode === 'price_spot') {
      const quoteTimeoutMs = stageTimeoutMs(req);
      trace("get_spot_price_start", { assetInId, assetOutId, quoteTimeoutMs });
      let spot;
      try {
        spot = await withStageTimeout(
          sdk.api.router.getSpotPrice(assetInId, assetOutId),
          quoteTimeoutMs,
          "get_spot_price",
          { assetInId, assetOutId },
        );
      } catch (e) {
        if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { wsProviderImport: papiWsProviderSpec, wsUrl, assetInId, assetOutId, amountMode: 'spot', amountInAtomic: null, amountOutAtomic: null, beneficiary: null, slippageBps: null, trade: null, stage: 'get_spot_price' });
        throw e;
      }
      trace("get_spot_price_done", { spotKeys: Object.keys(spot || {}).slice(0, 30) });
      const human = safeToHuman(spot);
      const spotPrice = pickSpotPrice(spot, human);
      emit({
        ok: true,
        mode: 'price_spot',
        provider: 'galactic_sdk_next',
        priceCache: boolFromReq(req.priceCache),
        wsProviderImport: papiWsProviderSpec,
        wsUrl: redactWs(wsUrl),
        assetInId,
        assetOutId,
        spotPrice,
        price: spotPrice,
        human,
        raw: toSerializable(spot),
      });
    }

    if (req.mode === 'inspect') {
      const router = sdk?.api?.router;
      const assetClient = sdk?.client?.asset;

      const supportedRaw = await inspectStep(req, 'asset_get_supported', () => assetClient?.getSupported?.(true));
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
        emit({
          ok: true,
          mode: 'inspect',
          inspectMode: 'light',
          provider: 'galactic_sdk_next',
          wsProviderImport: papiWsProviderSpec,
          wsUrl: redactWs(wsUrl),
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
        });
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

      emit({
        ok: true,
        mode: 'inspect',
        inspectMode,
        provider: 'galactic_sdk_next',
        wsProviderImport: papiWsProviderSpec,
        wsUrl: redactWs(wsUrl),
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
      });
    }

    if (req.mode === 'swap_tx') {
      if (manualCustomSwapReq(req)) {
        if (!boolFromReq(req.enableSwapTx)) {
          fail('hydration_swap_tx_disabled', 'Hydration manual swap transaction building is disabled.', {
            status: 503,
            node: process.versions.node,
            provider: 'manual_papi_router',
            resolved: { assetInId, assetOutId },
            wsUrl: redactWs(wsUrl),
            manualCustomSwap: true,
          }, 1);
        }
        try {
          const built = await buildManualHydrationRouterSwapTx({ client, req, assetInId, assetOutId });
          emit({
            ...built,
            wsProviderImport: papiWsProviderSpec,
            wsUrl: redactWs(wsUrl),
          });
        } catch (e) {
          if (e?.detail) fail(e.detail.error || 'hydration_manual_router_call_build_failed', e.detail.message || String(e?.message || e), e.detail, 1);
          fail('hydration_manual_router_call_build_failed', 'Manual Hydration Router call builder failed.', {
            node: process.versions.node,
            message: String(e?.message || e),
            stack: String(e?.stack || '').split('\n').slice(0, 8).join('\n'),
            wsUrl: redactWs(wsUrl),
          }, 1);
        }
      }

      if (!boolFromReq(req.enableRouterQuotes)) {
        fail('hydration_router_quotes_disabled', 'Hydration router quote calls are disabled; cannot build a swap transaction without a fresh route quote.', {
          status: 503,
          node: process.versions.node,
          provider: 'galactic_sdk_next',
          resolved: { assetInId, assetOutId },
          wsUrl: redactWs(wsUrl),
        }, 1);
      }
      if (!boolFromReq(req.enableSwapTx)) {
        fail('hydration_swap_tx_disabled', 'Hydration swap transaction building is disabled.', {
          status: 503,
          node: process.versions.node,
          provider: 'galactic_sdk_next',
          resolved: { assetInId, assetOutId },
          wsUrl: redactWs(wsUrl),
        }, 1);
      }

      const amountMode = String(req.amountMode || req.amount_mode || 'exact_in').trim().toLowerCase();
      if (!['exact_in', 'exact_out'].includes(amountMode)) {
        fail('invalid_amount_mode', 'amountMode must be exact_in or exact_out.', { amountMode }, 1);
      }
      if (amountMode === 'exact_out' && !boolFromReq(req.enableExactBuy)) {
        fail('hydration_exact_buy_disabled', 'Hydration exact-out BUY/getBestBuy is temporarily disabled while sidecar timeout behavior is isolated.', { amountMode, status: 503 }, 1);
      }
      const amountInAtomic = String(req.amountInAtomic || '').trim();
      const requestedAmountOutAtomic = String(req.amountOutAtomic || '').trim();
      if (amountMode === 'exact_in' && !amountInAtomic) fail('missing_amount_in_atomic', 'amountInAtomic is required for exact_in swaps.', { amountMode }, 1);
      if (amountMode === 'exact_out' && !requestedAmountOutAtomic) fail('missing_amount_out_atomic', 'amountOutAtomic is required for exact_out swaps.', { amountMode }, 1);
      const beneficiary = String(req.beneficiary || req.userPubkey || '').trim();
      if (!beneficiary) fail('missing_beneficiary', 'beneficiary/userPubkey is required.', {}, 1);
      const quoteTimeoutMs = stageTimeoutMs(req);
      const slippageBps = Number(req.slippageBps ?? 100);
      const slippagePct = Number.isFinite(slippageBps) ? slippageBps / 100 : 1;

      let trade;
      if (amountMode === 'exact_out') {
        const amountOutAtomicBigInt = parseAtomicBigInt(requestedAmountOutAtomic);
        trace("swap_get_best_buy_start", { assetInId, assetOutId, amountOutAtomic: requestedAmountOutAtomic, amountType: "bigint", quoteTimeoutMs });
        try {
          trade = await withStageTimeout(
            sdk.api.router.getBestBuy(assetInId, assetOutId, amountOutAtomicBigInt),
            quoteTimeoutMs,
            "swap_get_best_buy",
            { assetInId, assetOutId, amountOutAtomic: requestedAmountOutAtomic },
          );
        } catch (e) {
          if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { wsProviderImport: papiWsProviderSpec, wsUrl, assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps, trade: null, stage: 'swap_get_best_buy' });
          throw e;
        }
        trace("swap_get_best_buy_done", { tradeKeys: Object.keys(trade || {}).slice(0, 30) });
      } else {
        const amountInAtomicBigInt = parseAtomicBigInt(amountInAtomic);
        trace("swap_get_best_sell_start", { assetInId, assetOutId, amountInAtomic, amountType: "bigint", quoteTimeoutMs });
        try {
          trade = await withStageTimeout(
            sdk.api.router.getBestSell(assetInId, assetOutId, amountInAtomicBigInt),
            quoteTimeoutMs,
            "swap_get_best_sell",
            { assetInId, assetOutId, amountInAtomic },
          );
        } catch (e) {
          if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { wsProviderImport: papiWsProviderSpec, wsUrl, assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps, trade: null, stage: 'swap_get_best_sell' });
          throw e;
        }
        trace("swap_get_best_sell_done", { tradeKeys: Object.keys(trade || {}).slice(0, 30) });
      }

      if (!sdk?.tx || typeof sdk.tx.trade !== 'function') {
        fail('hydration_tx_trade_builder_missing', 'Hydration SDK tx.trade builder is not available.', { txKeys: Object.keys(sdk?.tx || {}).slice(0, 30) }, 1);
      }

      const unsupportedCtxBase = {
        wsProviderImport: papiWsProviderSpec,
        wsUrl,
        assetInId,
        assetOutId,
        amountMode,
        amountInAtomic,
        amountOutAtomic: requestedAmountOutAtomic,
        beneficiary,
        slippageBps,
        trade,
      };

      trace("swap_build_start", { beneficiary, slippageBps, slippagePct });
      let txBuilder;
      try {
        txBuilder = sdk.tx.trade(trade);
      } catch (e) {
        if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { ...unsupportedCtxBase, stage: 'sdk_tx_trade' });
        throw e;
      }
      try {
        if (typeof txBuilder?.withBeneficiary === 'function') txBuilder = txBuilder.withBeneficiary(beneficiary);
        if (typeof txBuilder?.withSlippage === 'function') txBuilder = txBuilder.withSlippage(slippagePct);
      } catch (e) {
        if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { ...unsupportedCtxBase, stage: 'sdk_tx_trade_options' });
        throw e;
      }

      trace("swap_tx_build_call_start", { builderKeys: Object.keys(txBuilder || {}).slice(0, 30) });
      let built;
      try {
        built = await withStageTimeout(Promise.resolve(typeof txBuilder?.build === 'function' ? txBuilder.build() : txBuilder), quoteTimeoutMs, "swap_tx_build", { assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps });
      } catch (e) {
        if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { ...unsupportedCtxBase, stage: 'swap_tx_build' });
        throw e;
      }
      trace("swap_tx_build_call_done", { builtKeys: Object.keys(built || {}).slice(0, 30) });

      trace("swap_tx_get_start", { hasGet: typeof built?.get === 'function' });
      let txObj;
      try {
        txObj = await withStageTimeout(Promise.resolve(typeof built?.get === 'function' ? built.get() : built), quoteTimeoutMs, "swap_tx_get", { assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps });
      } catch (e) {
        if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { ...unsupportedCtxBase, stage: 'swap_tx_get' });
        throw e;
      }
      trace("swap_tx_get_done", { txKeys: Object.keys(txObj || {}).slice(0, 30) });

      trace("swap_tx_get_encoded_data_start", { hasGetEncodedData: typeof txObj?.getEncodedData === 'function' });
      let encoded;
      try {
        encoded = await withStageTimeout(Promise.resolve(typeof txObj?.getEncodedData === 'function' ? txObj.getEncodedData() : txObj?.encodedData || txObj?.callData || null), quoteTimeoutMs, "swap_tx_get_encoded_data", { assetInId, assetOutId, amountMode, amountInAtomic, amountOutAtomic: requestedAmountOutAtomic, beneficiary, slippageBps });
      } catch (e) {
        if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { ...unsupportedCtxBase, stage: 'swap_tx_get_encoded_data' });
        throw e;
      }
      const encodedHex = encodedDataToHex(encoded);
      trace("swap_build_done", { builtKeys: Object.keys(built || {}).slice(0, 30), txKeys: Object.keys(txObj || {}).slice(0, 30), hasEncodedHex: !!encodedHex });

      if (!encodedHex) {
        fail('hydration_swap_tx_encoded_data_missing', 'Hydration SDK built a trade transaction, but no encoded call data could be extracted.', { built: toSerializable(built), tx: toSerializable(txObj) }, 1);
      }

      const human = safeToHuman(trade);
      const picked = pickAmountOut(trade, human);
      let amountOutAtomic = null;
      let amountOutUi = null;
      if (typeof picked === 'string') amountOutAtomic = picked;
      else if (picked && typeof picked === 'object' && picked.human != null) amountOutUi = picked.human;

      emit({
        ok: true,
        mode: 'swap_tx',
        provider: 'galactic_sdk_next',
        wsProviderImport: papiWsProviderSpec,
        wsUrl: redactWs(wsUrl),
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
      });
    }

    if (!boolFromReq(req.enableRouterQuotes)) {
      fail('hydration_router_quotes_disabled', 'Hydration router quote calls are disabled to avoid hammering RPC while rate-limited. Set enableRouterQuotes=true / UTT_HYDRATION_ENABLE_ROUTER_QUOTES=1 to run live router quotes.', {
        status: 503,
        node: process.versions.node,
        provider: 'galactic_sdk_next',
        resolved: { assetInId, assetOutId },
        wsUrl: redactWs(wsUrl),
      }, 1);
    }

    const amountInAtomic = String(req.amountInAtomic || '').trim();
    if (!amountInAtomic) fail('missing_amount_in_atomic', 'amountInAtomic is required.', {}, 1);
    const amountInAtomicBigInt = parseAtomicBigInt(amountInAtomic);
    const quoteTimeoutMs = stageTimeoutMs(req);

    trace("get_best_sell_start", {
      assetInId,
      assetOutId,
      amountInAtomic,
      amountType: "bigint",
      quoteTimeoutMs,
    });
    let trade;
    try {
      trade = await withStageTimeout(
        sdk.api.router.getBestSell(assetInId, assetOutId, amountInAtomicBigInt),
        quoteTimeoutMs,
        "get_best_sell",
        { assetInId, assetOutId, amountInAtomic }
      );
    } catch (e) {
      if (isUnsupportedSdkAssetError(e)) failUnsupportedSdkAsset(e, { wsProviderImport: papiWsProviderSpec, wsUrl, assetInId, assetOutId, amountMode: 'exact_in', amountInAtomic, amountOutAtomic: null, beneficiary: null, slippageBps: null, trade: null, stage: 'get_best_sell' });
      throw e;
    }
    trace("get_best_sell_done", { tradeKeys: Object.keys(trade || {}).slice(0, 30) });
    const human = safeToHuman(trade);
    const picked = pickAmountOut(trade, human);

    let amountOutAtomic = null;
    let amountOutUi = null;
    if (typeof picked === 'string') {
      amountOutAtomic = picked;
    } else if (picked && typeof picked === 'object' && picked.human != null) {
      amountOutUi = picked.human;
    }

    emit({
      ok: true,
      mode: 'quote_sell',
      provider: 'galactic_sdk_next',
      priceCache: boolFromReq(req.priceCache),
      wsProviderImport: papiWsProviderSpec,
      wsUrl: redactWs(wsUrl),
      assetInId,
      assetOutId,
      amountInAtomic,
      amountInUi: req.amountInUi ?? null,
      amountOutAtomic,
      amountOutUi,
      human,
      raw: toSerializable(trade),
    });
  } catch (e) {
    if (e?.code === 'hydration_helper_stage_timeout') {
      fail('hydration_helper_stage_timeout', 'Hydration helper stage timed out before returning.', {
        node: process.versions.node,
        stage: e.stage || 'unknown',
        timeout_s: Number(e.timeoutMs || 0) / 1000,
        message: String(e?.message || e),
        extra: e.extra || {},
        wsUrl: redactWs(wsUrl),
      }, 1);
    }
    fail('hydration_sdk_quote_failed', 'Hydration SDK quote failed.', {
      node: process.versions.node,
      message: String(e?.message || e),
      stack: String(e?.stack || '').split('\n').slice(0, 6).join('\n'),
      wsUrl: redactWs(wsUrl),
    }, 1);
  } finally {
    trace("cleanup_start");
    try { await sdk?.destroy?.(); } catch (_e) {}
    try { client?.destroy?.(); } catch (_e) {}
    trace("cleanup_done");
  }
}

main().catch((e) => {
  fail('hydration_helper_unhandled_error', 'Unhandled helper error.', { message: String(e?.message || e), stack: String(e?.stack || '') }, 1);
});
