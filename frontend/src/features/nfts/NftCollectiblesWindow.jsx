// frontend/src/features/nfts/NftCollectiblesWindow.jsx
import { useEffect, useMemo, useRef, useState } from "react";

const LS_UNISAT_ADDR_KEY = "utt_nft_unisat_address_v1";
const LS_COUNTERPARTY_METADATA_CACHE_KEY = "utt_nft_counterparty_metadata_cache_v1";
const COUNTERPARTY_METADATA_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const COUNTERPARTY_METADATA_CACHE_MAX_ASSETS = 500;
const LS_NFT_WINDOW_LAYOUT_KEY = "utt_nft_collectibles_window_layout_v1";

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function finiteNumberOrNull(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function maskMiddle(v, left = 8, right = 6) {
  const s = String(v || "").trim();
  if (!s) return "—";
  if (s.length <= left + right + 1) return s;
  return `${s.slice(0, left)}…${s.slice(-right)}`;
}

function safeLower(v) {
  return String(v || "").trim().toLowerCase();
}

function fmtSats(v) {
  const n = finiteNumberOrNull(v);
  if (n === null) return "—";
  return `${Math.round(n).toLocaleString()} sats`;
}

function fmtBtcFromSats(v) {
  const n = finiteNumberOrNull(v);
  if (n === null) return "—";
  return `${(n / 100000000).toFixed(8)} BTC`;
}

function fmtMarketQty(v) {
  const n = finiteNumberOrNull(v);
  if (n === null) return "—";
  const abs = Math.abs(n);
  const digits = abs >= 1000 ? 4 : abs >= 1 ? 8 : 10;
  return n.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function fmtMarketPrice(v, quoteAsset = "") {
  const n = finiteNumberOrNull(v);
  if (n === null) return "—";
  const abs = Math.abs(n);
  const digits = abs >= 1000 ? 4 : abs >= 1 ? 8 : 10;
  const suffix = quoteAsset ? ` ${quoteAsset}` : "";
  return `${n.toLocaleString(undefined, { maximumFractionDigits: digits })}${suffix}`;
}

function compactMarketRows(rowsMaybe, limit = 6) {
  return asArray(rowsMaybe).slice(0, Math.max(1, Number(limit) || 6));
}

function fmtTimeMaybe(v) {
  if (v === null || v === undefined || v === "") return "—";
  const n = Number(v);
  if (Number.isFinite(n) && n > 0) {
    const ms = n > 100000000000 ? n : n * 1000;
    const d = new Date(ms);
    if (!Number.isNaN(d.getTime())) return d.toLocaleString();
  }
  const d = new Date(v);
  if (!Number.isNaN(d.getTime())) return d.toLocaleString();
  return String(v);
}

function copyTextSafe(text) {
  const s = String(text || "").trim();
  if (!s) return;
  try {
    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(s).catch(() => {});
      return;
    }
  } catch {
    // ignore
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = s;
    ta.setAttribute("readonly", "readonly");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
  } catch {
    // ignore
  }
}

function getUnisatProvider() {
  try {
    return typeof window !== "undefined" ? window.unisat : null;
  } catch {
    return null;
  }
}

async function fetchJsonMaybe(apiBase, path) {
  const base = String(apiBase || "").replace(/\/+$/, "");
  const p = String(path || "");
  const url = p.startsWith("http") ? p : `${base}${p.startsWith("/") ? p : `/${p}`}`;
  const r = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store" });
  const data = await r.json().catch(() => ({}));
  if (!r.ok || data?.ok === false) throw new Error(data?.detail || data?.error || `HTTP ${r.status}`);
  return data;
}

function looksLikeExternalMetadataPointer(v) {
  const s = String(v || "").trim();
  if (!s) return false;
  if (/^(https?:\/\/|ipfs:\/\/|ar:\/\/|arweave:\/\/|\/\/)/i.test(s)) return true;
  return /^[A-Za-z0-9.-]+\.[A-Za-z]{2,}(\/|$)/.test(s) && (s.includes("/") || /\.(json|png|jpe?g|gif|webp|html?)(\?|$)/i.test(s));
}

function metadataHasRetryableMediaFailure(metadataMaybe) {
  const metadata = metadataMaybe && typeof metadataMaybe === "object" ? metadataMaybe : {};
  const media = metadata.media && typeof metadata.media === "object" ? metadata.media : null;
  if (media?.ok) return false;
  const external = metadata.external_metadata && typeof metadata.external_metadata === "object" ? metadata.external_metadata : {};
  const err = String(metadata.media_error || external.error || "").trim().toLowerCase();
  const hasExternalPointer = !!metadata.external_metadata_url || !!external.url || looksLikeExternalMetadataPointer(metadata.description);
  if (!hasExternalPointer) {
    const desc = String(metadata.description || "").trim().toLowerCase();
    const asset = String(metadata.asset || "").trim().toUpperCase();
    // Older Counterparty collectibles often have no on-chain media pointer,
    // but registry probes can still enrich them later. Do not let a stale
    // localStorage no-media row suppress TokenScan/registry retries.
    if (asset.endsWith("CARD") || asset.endsWith("CD") || ` ${desc} `.includes(" card") || desc.includes("sog") || desc.includes("spells of genesis")) {
      return true;
    }
    return false;
  }
  if (["no_media_url_in_metadata", "no_media_url_in_override", "no_media_url_in_orbital_metadata"].includes(err)) return false;
  // Any failed external metadata result should be retried instead of being
  // preserved in localStorage as a permanent no-image record.
  if (err) return true;
  // Older cached entries may have a bare-domain description but no
  // external_metadata_url because earlier code did not normalize bare URLs.
  return true;
}

function uniqueStrings(...vals) {
  const out = [];
  const push = (v) => {
    if (Array.isArray(v)) {
      v.forEach(push);
      return;
    }
    const s = String(v || "").trim();
    if (s && !out.includes(s)) out.push(s);
  };
  vals.forEach(push);
  return out;
}

function arweaveMediaPathFallbacks(urlMaybe) {
  const raw = String(urlMaybe || "").trim();
  if (!raw) return [];
  try {
    const u = new URL(raw);
    const host = String(u.hostname || "").toLowerCase();
    const parts = String(u.pathname || "").split("/").filter(Boolean);
    const out = [];
    const add = (candidate) => {
      const s = String(candidate || "").trim();
      if (s && !out.includes(s)) out.push(s);
    };
    if (parts.length >= 2) {
      const rootPath = `/${parts[0]}${u.search || ""}`;
      if (host.endsWith(".arweave.net") || ["arweave.net", "permagate.io", "ar-io.net"].includes(host)) {
        // Key Counterparty/ORBital case:
        //   .../<data-id>/<name>_image.png
        // often resolves in-browser as:
        //   .../<data-id>
        add(`${u.protocol}//${u.host}${rootPath}`);
        add(`https://arweave.net/${parts[0]}${u.search || ""}`);
        add(`https://permagate.io/${parts[0]}${u.search || ""}`);
        add(`https://ar-io.net/${parts[0]}${u.search || ""}`);
      }
      if (["ipfs.io", "cloudflare-ipfs.com", "gateway.pinata.cloud"].includes(host) && parts[0].toLowerCase() === "ipfs" && parts[1]) {
        add(`https://ipfs.io/ipfs/${parts[1]}${u.search || ""}`);
        add(`https://cloudflare-ipfs.com/ipfs/${parts[1]}${u.search || ""}`);
        add(`https://gateway.pinata.cloud/ipfs/${parts[1]}${u.search || ""}`);
      }
    }
    return out;
  } catch {
    return [];
  }
}

function mediaUrlCandidates(primary, ...candidateGroups) {
  const base = uniqueStrings(primary, ...candidateGroups);
  const expanded = [];
  base.forEach((u) => {
    if (!expanded.includes(u)) expanded.push(u);
    arweaveMediaPathFallbacks(u).forEach((candidate) => {
      if (!expanded.includes(candidate)) expanded.push(candidate);
    });
  });
  return expanded;
}

function ResilientMediaImage({ src, candidates, alt, style, fallbackStyle, loading = "lazy" }) {
  const urls = useMemo(() => mediaUrlCandidates(src, candidates), [src, candidates]);
  const [idx, setIdx] = useState(0);
  useEffect(() => {
    setIdx(0);
  }, [urls.join("|")]);

  const current = urls[idx] || "";
  if (!current) {
    return <div style={fallbackStyle || style}>Image unavailable</div>;
  }
  if (idx >= urls.length) {
    return <div style={fallbackStyle || style}>Image unavailable</div>;
  }

  return (
    <img
      alt={alt || "media preview"}
      src={current}
      loading={loading}
      referrerPolicy="no-referrer"
      onError={() => setIdx((prev) => prev + 1)}
      style={style}
    />
  );
}

function readCounterpartyMetadataCache(assetsMaybe = []) {
  try {
    if (typeof window === "undefined" || !window.localStorage) return {};
    const wanted = new Set((assetsMaybe || []).map((a) => String(a || "").trim().toUpperCase()).filter(Boolean));
    const raw = window.localStorage.getItem(LS_COUNTERPARTY_METADATA_CACHE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    const items = parsed?.items && typeof parsed.items === "object" ? parsed.items : parsed;
    const now = Date.now();
    const out = {};
    for (const [assetRaw, entryRaw] of Object.entries(items || {})) {
      const asset = String(assetRaw || "").trim().toUpperCase();
      if (!asset || (wanted.size && !wanted.has(asset))) continue;
      const entry = entryRaw && typeof entryRaw === "object" ? entryRaw : {};
      const ts = Number(entry.ts || entry.updatedAt || entry.cachedAt || 0);
      const metadata = entry.metadata && typeof entry.metadata === "object" ? entry.metadata : entry;
      if (!metadata || typeof metadata !== "object") continue;
      if (metadataHasRetryableMediaFailure(metadata)) continue;
      if (ts && now - ts > COUNTERPARTY_METADATA_CACHE_TTL_MS) continue;
      out[asset] = { ...metadata, asset: metadata.asset || asset };
    }
    return out;
  } catch {
    return {};
  }
}

function writeCounterpartyMetadataCache(metadataMapMaybe = {}) {
  try {
    if (typeof window === "undefined" || !window.localStorage) return;
    const incoming = metadataMapMaybe && typeof metadataMapMaybe === "object" ? metadataMapMaybe : {};
    if (!Object.keys(incoming).length) return;
    let items = {};
    try {
      const raw = window.localStorage.getItem(LS_COUNTERPARTY_METADATA_CACHE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        items = parsed?.items && typeof parsed.items === "object" ? parsed.items : (parsed && typeof parsed === "object" ? parsed : {});
      }
    } catch {
      items = {};
    }
    const now = Date.now();
    for (const [assetRaw, metadataRaw] of Object.entries(incoming)) {
      const asset = String(assetRaw || metadataRaw?.asset || "").trim().toUpperCase();
      if (!asset || !metadataRaw || typeof metadataRaw !== "object") continue;
      if (metadataHasRetryableMediaFailure(metadataRaw)) continue;
      items[asset] = {
        ts: now,
        metadata: { ...metadataRaw, asset },
      };
    }
    const sorted = Object.entries(items)
      .filter(([asset, entry]) => asset && entry && typeof entry === "object")
      .sort((a, b) => Number(b[1]?.ts || 0) - Number(a[1]?.ts || 0))
      .slice(0, COUNTERPARTY_METADATA_CACHE_MAX_ASSETS);
    window.localStorage.setItem(LS_COUNTERPARTY_METADATA_CACHE_KEY, JSON.stringify({ version: 1, updatedAt: now, items: Object.fromEntries(sorted) }));
  } catch {
    // localStorage quota/private mode should never block NFT display
  }
}

function readNftWindowLayout() {
  try {
    if (typeof window === "undefined" || !window.localStorage) return {};
    const raw = window.localStorage.getItem(LS_NFT_WINDOW_LAYOUT_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function writeNftWindowLayout(layoutMaybe = {}) {
  try {
    if (typeof window === "undefined" || !window.localStorage) return;
    const layout = layoutMaybe && typeof layoutMaybe === "object" ? layoutMaybe : {};
    window.localStorage.setItem(LS_NFT_WINDOW_LAYOUT_KEY, JSON.stringify(layout));
  } catch {
    // ignore localStorage persistence failures
  }
}

function normalizeInscriptionsPayload(payload) {
  const root = payload && typeof payload === "object" ? payload : {};
  const rawItems = Array.isArray(payload)
    ? payload
    : Array.isArray(root.list)
      ? root.list
      : Array.isArray(root.items)
        ? root.items
        : Array.isArray(root.inscriptions)
          ? root.inscriptions
          : Array.isArray(root.data)
            ? root.data
            : [];

  return rawItems.map((it, idx) => {
    const obj = it && typeof it === "object" ? it : {};
    const inscriptionId = String(obj.inscriptionId || obj.inscription_id || obj.id || "").trim();
    const contentType = String(obj.contentType || obj.content_type || obj.mimeType || obj.mime_type || obj.type || "").trim();
    const preview = String(obj.preview || obj.previewUrl || obj.preview_url || "").trim();
    const content = String(obj.content || obj.contentUrl || obj.content_url || "").trim();
    const location = String(obj.location || obj.utxo || obj.output || "").trim();
    const number = obj.inscriptionNumber ?? obj.inscription_number ?? obj.number ?? null;
    const outputValue = obj.outputValue ?? obj.output_value ?? obj.value ?? obj.sats ?? null;

    return {
      ...obj,
      _idx: idx,
      sourceKind: "ordinal",
      inscriptionId,
      inscriptionNumber: number,
      contentType,
      preview,
      content,
      location,
      outputValue,
      address: String(obj.address || "").trim(),
      timestamp: obj.timestamp ?? obj.time ?? obj.createdAt ?? obj.created_at ?? null,
      genesisTransaction: String(obj.genesisTransaction || obj.genesis_tx || obj.genesisTx || "").trim(),
    };
  });
}

function normalizeInscriptionsTotal(payload, items) {
  if (payload && typeof payload === "object" && !Array.isArray(payload)) {
    for (const k of ["total", "totalCount", "total_count", "count"]) {
      const n = finiteNumberOrNull(payload[k]);
      if (n !== null) return n;
    }
  }
  return items.length;
}

function extractCounterpartyBalanceRows(payload) {
  if (Array.isArray(payload)) return payload.filter((x) => x && typeof x === "object");
  if (!payload || typeof payload !== "object") return [];

  const containers = [
    // Prefer backend-normalized balance rows when present.  The raw
    // Counterparty Core payload is still preserved under payload.raw for audit,
    // but the UI should consume quantity_normalized / quantity_atomic / decimals.
    payload.items,
    payload.balances,
    payload.rows,
    payload.records,
    payload.raw,
    payload.result,
    payload.data,
  ];

  for (const c of containers) {
    if (Array.isArray(c)) return c.filter((x) => x && typeof x === "object");
    if (c && typeof c === "object") {
      const nested = extractCounterpartyBalanceRows(c);
      if (nested.length) return nested;
    }
  }

  return [];
}

function normalizeCounterpartyBalancesPayload(payload) {
  const rows = extractCounterpartyBalanceRows(payload);
  return rows
    .map((row, idx) => {
      const asset = String(row?.asset || row?.asset_name || row?.assetName || row?.symbol || "").trim().toUpperCase();
      if (!asset) return null;
      const longname = String(row?.asset_longname || row?.assetLongname || row?.longname || row?.asset_long_name || "").trim();
      const quantity = finiteNumberOrNull(
        row?.quantity_normalized ??
          row?.normalized_quantity ??
          row?.quantityNormalized ??
          row?.balance_normalized ??
          row?.balanceNormalized ??
          row?.quantity ??
          row?.balance ??
          row?.qty ??
          row?.amount
      );
      const utxo = String(row?.utxo || row?.location || row?.output || "").trim();
      const utxoAddress = String(row?.utxo_address || row?.utxoAddress || row?.address || "").trim();
      const divisible = row?.divisible;
      return {
        ...row,
        _idx: idx,
        sourceKind: "counterparty",
        asset,
        assetLongname: longname,
        quantity,
        contentType: divisible === false ? "counterparty/non-divisible-asset" : "counterparty/asset",
        location: utxo || utxoAddress,
        address: utxoAddress || String(row?.address || "").trim(),
      };
    })
    .filter(Boolean);
}

function normalizeCounterpartyMetadataItems(payload) {
  const root = payload && typeof payload === "object" ? payload : {};
  const rawItems = Array.isArray(payload)
    ? payload
    : Array.isArray(root.items)
      ? root.items
      : Array.isArray(root.data)
        ? root.data
        : Array.isArray(root.results)
          ? root.results
          : [];

  const out = {};
  for (const it of rawItems || []) {
    const obj = it && typeof it === "object" ? it : {};
    const meta = obj.metadata && typeof obj.metadata === "object" ? obj.metadata : obj;
    const asset = String(obj.asset || meta.asset || meta.asset_name || meta.symbol || "").trim().toUpperCase();
    if (!asset) continue;
    const media = meta.media && typeof meta.media === "object" ? meta.media : null;
    const externalMetadata = meta.external_metadata && typeof meta.external_metadata === "object" ? meta.external_metadata : null;
    out[asset] = {
      ...meta,
      asset,
      asset_longname: meta.asset_longname || meta.assetLongname || meta.longname || meta.asset_long_name || null,
      issuer: meta.issuer || meta.owner || meta.source || meta.issuer_address || meta.owner_address || null,
      description: meta.description || meta.desc || meta.memo || null,
      media_name: meta.media_name || media?.name || null,
      media_description: meta.media_description || media?.description || null,
      media,
      external_metadata: externalMetadata,
      external_metadata_url: meta.external_metadata_url || media?.metadata_url || externalMetadata?.url || null,
      media_error: meta.media_error || null,
      divisible: meta.divisible,
      locked: meta.locked,
      callable: meta.callable,
      reset: meta.reset,
      supply: meta.supply,
      supply_source: meta.supply_source || null,
      source_path: obj.source_path || meta.source_path || null,
    };
  }
  return out;
}

function boolLabel(v) {
  if (v === true) return "Yes";
  if (v === false) return "No";
  return "—";
}

function firstText(...vals) {
  for (const v of vals || []) {
    const s = String(v ?? "").trim();
    if (s) return s;
  }
  return "";
}

function counterpartyMetadataFor(row, metadataMap) {
  const asset = String(row?.asset || "").trim().toUpperCase();
  return asset ? (metadataMap?.[asset] || null) : null;
}

function counterpartyMediaFor(row, metadataMap) {
  const meta = counterpartyMetadataFor(row, metadataMap) || row?.assetMetadata || null;
  const media = meta?.media && typeof meta.media === "object" ? meta.media : null;
  if (!media?.content_url && !media?.preview_url && !media?.image_url && !media?.animation_url) return null;
  const contentUrl = firstText(media.content_url, media.animation_url, media.image_url, media.preview_url);
  const previewUrl = firstText(media.preview_url, media.image_url, media.content_url, media.animation_url);
  const imageCandidates = mediaUrlCandidates(media.image_url, media.image_url_candidates);
  const animationCandidates = mediaUrlCandidates(media.animation_url, media.animation_url_candidates);
  const audioCandidates = mediaUrlCandidates(media.audio_url, media.audio_url_candidates);
  const contentCandidates = mediaUrlCandidates(contentUrl, media.content_url_candidates, animationCandidates, imageCandidates, audioCandidates);
  const previewCandidates = mediaUrlCandidates(previewUrl, media.preview_url_candidates, imageCandidates, contentCandidates);
  return {
    ...media,
    content_url: contentUrl,
    preview_url: previewUrl,
    image_url_candidates: imageCandidates,
    animation_url_candidates: animationCandidates,
    audio_url_candidates: audioCandidates,
    content_url_candidates: contentCandidates,
    preview_url_candidates: previewCandidates,
    content_type: media.content_type || "",
  };
}

function uniqueCounterpartyAssets(rowsMaybe) {
  const seen = new Set();
  const out = [];
  for (const row of rowsMaybe || []) {
    const asset = String(row?.asset || "").trim().toUpperCase();
    if (!asset || seen.has(asset)) continue;
    seen.add(asset);
    out.push(asset);
  }
  return out;
}

function chunkArray(items, size) {
  const n = Math.max(1, Number(size) || 1);
  const out = [];
  for (let i = 0; i < (items || []).length; i += n) out.push(items.slice(i, i + n));
  return out;
}

function prioritizeCounterpartyAssets(rowsMaybe, assetsMaybe, queryMaybe, selectedMaybe) {
  const rows = rowsMaybe || [];
  const assetIndex = new Map();
  rows.forEach((row, idx) => {
    const asset = String(row?.asset || "").trim().toUpperCase();
    if (asset && !assetIndex.has(asset)) assetIndex.set(asset, idx);
  });
  const q = String(queryMaybe || "").trim().toLowerCase();
  const selectedAsset = String(selectedMaybe?.asset || "").trim().toUpperCase();

  function score(asset) {
    const idx = assetIndex.has(asset) ? assetIndex.get(asset) : 9999;
    const row = rows[idx] || null;
    let s = -idx;
    if (selectedAsset && asset === selectedAsset) s += 100000;
    if (q && rowSearchText(row).includes(q)) s += 50000;
    if (q && String(asset || "").toLowerCase().includes(q)) s += 25000;
    return s;
  }

  return [...(assetsMaybe || [])].sort((a, b) => score(b) - score(a));
}

function withCounterpartyMedia(row, metadataMap) {
  const meta = counterpartyMetadataFor(row, metadataMap);
  const media = counterpartyMediaFor(row, metadataMap);
  if (!media) return { ...row, assetMetadata: meta };
  const ct = media.content_type || row?.contentType || "counterparty/asset";
  return {
    ...row,
    assetMetadata: meta,
    media,
    contentType: ct,
    preview: media.preview_url || row?.preview || "",
    content: media.content_url || row?.content || "",
    mediaName: media.name || meta?.media_name || "",
    mediaDescription: media.description || meta?.media_description || "",
  };
}

function fmtQuantityMaybe(v) {
  const n = finiteNumberOrNull(v);
  if (n === null) return "—";
  return n.toLocaleString(undefined, { maximumFractionDigits: 8 });
}

function rowSearchText(row) {
  if (!row) return "";
  return [
    row.sourceKind,
    row.inscriptionId,
    row.inscriptionNumber,
    row.contentType,
    row.location,
    row.address,
    row.genesisTransaction,
    row.asset,
    row.assetLongname,
    row.asset_longname,
    row.assetMetadata?.asset_longname,
    row.assetMetadata?.issuer,
    row.assetMetadata?.description,
    row.assetMetadata?.media_name,
    row.assetMetadata?.media_description,
    row.media?.name,
    row.media?.description,
    row.utxo,
    row.utxo_address,
  ].map((x) => String(x || "").toLowerCase()).join(" ");
}

function contentTypeBucket(contentType) {
  const ct = safeLower(contentType);
  if (!ct) return "unknown";
  if (ct.startsWith("image/") && ct !== "image/svg+xml") return "image";
  if (ct.startsWith("video/")) return "video";
  if (ct.startsWith("audio/")) return "audio";
  if (ct === "text/plain" || ct.startsWith("text/plain")) return "text";
  if (ct.includes("json")) return "json";
  if (ct.includes("html") || ct === "image/svg+xml") return "external";
  if (ct.startsWith("counterparty/")) return "counterparty";
  return "other";
}

function renderPreview(item, styles) {
  if (styles?.hidden) {
    return (
      <div style={{
        width: 74,
        height: 58,
        borderRadius: 10,
        border: "1px solid rgba(255,255,255,0.12)",
        background: "rgba(255,255,255,0.04)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 13,
        fontWeight: 900,
        color: "var(--utt-muted, rgba(255,255,255,0.66))",
      }}>••••</div>
    );
  }
  const ct = safeLower(item?.contentType);
  const bucket = contentTypeBucket(ct);
  const previewUrl = String(item?.preview || item?.content || "").trim();
  const boxStyle = {
    width: 74,
    height: 58,
    borderRadius: 10,
    border: "1px solid rgba(255,255,255,0.12)",
    background: "rgba(255,255,255,0.04)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    overflow: "hidden",
    fontSize: 11,
    color: "var(--utt-muted, rgba(255,255,255,0.66))",
  };

  if (previewUrl && bucket === "image") {
    return (
      <ResilientMediaImage
        alt="inscription preview"
        src={previewUrl}
        candidates={item?.media?.preview_url_candidates || item?.media?.image_url_candidates || item?.media?.content_url_candidates}
        loading="lazy"
        style={{ ...boxStyle, objectFit: "cover" }}
        fallbackStyle={{ ...boxStyle, padding: 4, textAlign: "center" }}
      />
    );
  }
  if (previewUrl && bucket === "video") {
    return <video src={previewUrl} muted preload="metadata" style={{ ...boxStyle, objectFit: "cover" }} />;
  }
  if (previewUrl && bucket === "audio") {
    return <div style={boxStyle}>Audio</div>;
  }
  if (bucket === "text") return <div style={boxStyle}>TXT</div>;
  if (bucket === "json") return <div style={boxStyle}>{"{ }"}</div>;
  if (bucket === "external") return <div style={boxStyle}>Open</div>;
  if (bucket === "counterparty") return <div style={boxStyle}>{styles?.fallbackLabel || "XCP"}</div>;
  return <div style={boxStyle}>{styles?.fallbackLabel || "NFT"}</div>;
}

function typeBadgeStyle(bucket) {
  const base = {
    display: "inline-flex",
    alignItems: "center",
    gap: 4,
    padding: "2px 7px",
    borderRadius: 999,
    border: "1px solid rgba(255,255,255,0.12)",
    fontSize: 11,
    fontWeight: 800,
    whiteSpace: "nowrap",
  };
  if (bucket === "image") return { ...base, color: "#9ad", background: "rgba(120,160,255,0.12)" };
  if (bucket === "video" || bucket === "audio") return { ...base, color: "#f7b955", background: "rgba(247,185,85,0.12)" };
  if (bucket === "text" || bucket === "json") return { ...base, color: "#55e38c", background: "rgba(85,227,140,0.10)" };
  if (bucket === "external") return { ...base, color: "#ffb4e6", background: "rgba(255,180,230,0.10)" };
  if (bucket === "counterparty") return { ...base, color: "#f7b955", background: "rgba(247,185,85,0.12)" };
  return { ...base, color: "rgba(255,255,255,0.78)", background: "rgba(255,255,255,0.06)" };
}

export default function NftCollectiblesWindow({ apiBase = "", hideTableData = false, height = 620 }) {
  const initialLayoutRef = useRef(null);
  if (!initialLayoutRef.current) initialLayoutRef.current = readNftWindowLayout();

  const [providerPresent, setProviderPresent] = useState(false);
  const [providerInfo, setProviderInfo] = useState(null);
  const [accounts, setAccounts] = useState([]);
  const [address, setAddress] = useState(() => {
    try {
      return localStorage.getItem(LS_UNISAT_ADDR_KEY) || "";
    } catch {
      return "";
    }
  });
  const [network, setNetwork] = useState("");
  const [chain, setChain] = useState(null);
  const [btcBalance, setBtcBalance] = useState(null);
  const [cursor, setCursor] = useState(0);
  const [pageSize, setPageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [items, setItems] = useState([]);
  const [counterpartyItems, setCounterpartyItems] = useState([]);
  const [counterpartyLoading, setCounterpartyLoading] = useState(false);
  const [counterpartyErr, setCounterpartyErr] = useState("");
  const [counterpartyUpdatedAt, setCounterpartyUpdatedAt] = useState(null);
  const [counterpartyMetadataMap, setCounterpartyMetadataMap] = useState({});
  const [counterpartyMetadataLoading, setCounterpartyMetadataLoading] = useState(false);
  const [counterpartyMetadataErr, setCounterpartyMetadataErr] = useState("");
  const [counterpartyMarketByAsset, setCounterpartyMarketByAsset] = useState({});
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [selected, setSelected] = useState(null);
  const [updatedAt, setUpdatedAt] = useState(null);
  const [isFullscreen, setIsFullscreen] = useState(!!initialLayoutRef.current?.isFullscreen);
  const [detailPaneWidth, setDetailPaneWidth] = useState(() => {
    const n = finiteNumberOrNull(initialLayoutRef.current?.detailPaneWidth);
    return n === null ? 320 : Math.max(280, Math.min(720, n));
  });
  const counterpartyMetadataLoadSeqRef = useRef(0);
  const counterpartyMarketLoadSeqRef = useRef(0);
  const splitLayoutRef = useRef(null);

  useEffect(() => {
    writeNftWindowLayout({
      isFullscreen: !!isFullscreen,
      detailPaneWidth: Math.round(detailPaneWidth || 320),
    });
  }, [isFullscreen, detailPaneWidth]);

  useEffect(() => {
    if (!isFullscreen) return undefined;
    const onKeyDown = (ev) => {
      if (ev.key === "Escape") setIsFullscreen(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isFullscreen]);

  function startSplitResize(ev) {
    ev.preventDefault();
    ev.stopPropagation();
    const startX = Number(ev.clientX || 0);
    const startWidth = Number(detailPaneWidth || 320);
    const onMove = (moveEv) => {
      const containerWidth = splitLayoutRef.current?.getBoundingClientRect?.().width || 0;
      const maxWidth = Math.max(320, Math.min(860, containerWidth - 320));
      const next = Math.max(280, Math.min(maxWidth, startWidth - (Number(moveEv.clientX || 0) - startX)));
      setDetailPaneWidth(next);
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }

  useEffect(() => {
    setProviderPresent(!!getUnisatProvider());
    fetchJsonMaybe(apiBase, "/api/counterparty/wallet_provider/unisat")
      .then((data) => setProviderInfo(data))
      .catch(() => setProviderInfo(null));
  }, [apiBase]);

  async function readWalletState({ prompt = false } = {}) {
    const provider = getUnisatProvider();
    setProviderPresent(!!provider);
    if (!provider) throw new Error("UniSat wallet provider not found. Install/open UniSat in this browser profile.");

    const acctList = prompt && typeof provider.requestAccounts === "function"
      ? await provider.requestAccounts()
      : typeof provider.getAccounts === "function"
        ? await provider.getAccounts()
        : [];

    const arr = asArray(acctList).map((x) => String(x || "").trim()).filter(Boolean);
    const addr = arr[0] || "";
    setAccounts(arr);
    setAddress(addr);
    try {
      if (addr) localStorage.setItem(LS_UNISAT_ADDR_KEY, addr);
    } catch {
      // ignore
    }

    try {
      const n = typeof provider.getNetwork === "function" ? await provider.getNetwork() : "";
      setNetwork(String(n || ""));
    } catch {
      setNetwork("");
    }

    try {
      const c = typeof provider.getChain === "function" ? await provider.getChain() : null;
      setChain(c || null);
    } catch {
      setChain(null);
    }

    try {
      const b = typeof provider.getBalance === "function" ? await provider.getBalance() : null;
      setBtcBalance(b || null);
    } catch {
      setBtcBalance(null);
    }

    return addr;
  }

  async function loadCounterpartyAssetMetadata(rowsMaybe = counterpartyItems) {
    const rows = rowsMaybe || [];
    const assets = prioritizeCounterpartyAssets(rows, uniqueCounterpartyAssets(rows).slice(0, 100), query, selected);
    const loadSeq = counterpartyMetadataLoadSeqRef.current + 1;
    counterpartyMetadataLoadSeqRef.current = loadSeq;

    if (!assets.length) {
      setCounterpartyMetadataMap({});
      return {};
    }

    const cached = readCounterpartyMetadataCache(assets);
    const cachedAssets = new Set(Object.keys(cached || {}));
    const assetsToFetch = assets.filter((asset) => !cachedAssets.has(String(asset || "").trim().toUpperCase()));
    const merged = { ...(cached || {}) };
    const errors = [];

    async function fetchAssets(assetSubset) {
      const subset = (assetSubset || []).map((a) => String(a || "").trim().toUpperCase()).filter(Boolean);
      if (!subset.length) return {};
      const payload = await fetchJsonMaybe(apiBase, `/api/counterparty/assets/metadata?assets=${encodeURIComponent(subset.join(","))}&limit=${Math.max(1, subset.length)}`);
      return normalizeCounterpartyMetadataItems(payload);
    }

    function mergeChunk(normalized) {
      if (counterpartyMetadataLoadSeqRef.current !== loadSeq) return;
      Object.assign(merged, normalized || {});
      writeCounterpartyMetadataCache(normalized || {});
      setCounterpartyMetadataMap((prev) => ({ ...(prev || {}), ...(normalized || {}) }));
    }

    setCounterpartyMetadataLoading(true);
    setCounterpartyMetadataErr("");
    setCounterpartyMetadataMap(cached || {});
    try {
      // Hydrate from local cache immediately, then fetch only missing/stale
      // assets.  This makes media previews appear immediately after the first
      // successful metadata run while preserving incremental network refreshes.
      if (!assetsToFetch.length) {
        return merged;
      }

      // Load priority rows one asset at a time first. This prevents one slow or
      // broken Counterparty metadata lookup from holding the whole NFT window at
      // 43 / 0, and lets searched/selected rows like FREESPIN render as soon as
      // their own metadata returns.
      const priorityAssets = assetsToFetch.slice(0, 8);
      const remainderAssets = assetsToFetch.slice(8);

      for (const asset of priorityAssets) {
        try {
          const normalized = await fetchAssets([asset]);
          mergeChunk(normalized);
        } catch (e) {
          errors.push(`${asset}: ${String(e?.message || e || "metadata failed")}`);
        }
      }

      for (const chunk of chunkArray(remainderAssets, 8)) {
        try {
          const normalized = await fetchAssets(chunk);
          mergeChunk(normalized);
        } catch (e) {
          errors.push(`${chunk.join(",")}: ${String(e?.message || e || "metadata failed")}`);
        }
      }

      if (errors.length && counterpartyMetadataLoadSeqRef.current === loadSeq) {
        setCounterpartyMetadataErr(`Some Counterparty metadata failed: ${errors.slice(0, 3).join(" | ")}${errors.length > 3 ? " ..." : ""}`);
      }
      return merged;
    } finally {
      if (counterpartyMetadataLoadSeqRef.current === loadSeq) {
        setCounterpartyMetadataLoading(false);
      }
    }
  }

  async function loadCounterpartyBalances(addrMaybe = address) {
    const addr = String(addrMaybe || "").trim();
    if (!addr) {
      setCounterpartyItems([]);
      return [];
    }

    setCounterpartyLoading(true);
    setCounterpartyErr("");
    try {
      const payload = await fetchJsonMaybe(apiBase, `/api/counterparty/address/${encodeURIComponent(addr)}/balances`);
      const normalized = normalizeCounterpartyBalancesPayload(payload);
      setCounterpartyItems(normalized);
      setCounterpartyUpdatedAt(new Date().toISOString());
      loadCounterpartyAssetMetadata(normalized).catch(() => {});
      return normalized;
    } catch (e) {
      setCounterpartyErr(String(e?.message || e || "Failed to load Counterparty assets."));
      return [];
    } finally {
      setCounterpartyLoading(false);
    }
  }

  async function loadCounterpartyMarketContext(assetMaybe, opts = {}) {
    const asset = String(assetMaybe || "").trim().toUpperCase();
    if (!asset) return null;

    const force = !!opts.force;
    const existing = counterpartyMarketByAsset?.[asset];
    if (!force && existing?.data && !existing?.error) return existing.data;

    const seq = counterpartyMarketLoadSeqRef.current + 1;
    counterpartyMarketLoadSeqRef.current = seq;
    setCounterpartyMarketByAsset((prev) => ({
      ...(prev || {}),
      [asset]: {
        ...(prev?.[asset] || {}),
        loading: true,
        error: "",
      },
    }));

    try {
      const payload = await fetchJsonMaybe(apiBase, `/api/counterparty/assets/${encodeURIComponent(asset)}/market_context?limit=25&open_only=true`);
      if (counterpartyMarketLoadSeqRef.current === seq) {
        setCounterpartyMarketByAsset((prev) => ({
          ...(prev || {}),
          [asset]: {
            data: payload,
            loading: false,
            error: "",
            updatedAt: new Date().toISOString(),
          },
        }));
      }
      return payload;
    } catch (e) {
      const msg = String(e?.message || e || "Failed to load Counterparty market context.");
      if (counterpartyMarketLoadSeqRef.current === seq) {
        setCounterpartyMarketByAsset((prev) => ({
          ...(prev || {}),
          [asset]: {
            ...(prev?.[asset] || {}),
            loading: false,
            error: msg,
          },
        }));
      }
      return null;
    }
  }

  useEffect(() => {
    const asset = String(selected?.sourceKind || "").toLowerCase() === "counterparty"
      ? String(selected?.asset || "").trim().toUpperCase()
      : "";
    if (!asset) return;
    loadCounterpartyMarketContext(asset).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiBase, selected?.sourceKind, selected?.asset]);

  async function loadInscriptions(nextCursor = 0, opts = {}) {
    const provider = getUnisatProvider();
    setProviderPresent(!!provider);
    if (!provider) {
      setErr("UniSat wallet provider not found.");
      return;
    }
    if (typeof provider.getInscriptions !== "function") {
      setErr("UniSat getInscriptions API is not available in this wallet version.");
      return;
    }

    setLoading(true);
    setErr("");
    try {
      let addr = address;
      if (opts.readWallet !== false) {
        addr = await readWalletState({ prompt: !!opts.prompt });
      }
      if (!addr) throw new Error("No UniSat account connected.");

      const payload = await provider.getInscriptions(Number(nextCursor) || 0, Number(pageSize) || 20);
      const normalized = normalizeInscriptionsPayload(payload);
      setItems(normalized);
      setTotal(normalizeInscriptionsTotal(payload, normalized));
      setCursor(Number(nextCursor) || 0);
      setUpdatedAt(new Date().toISOString());
      const counterpartyNormalized = await loadCounterpartyBalances(addr);
      if (!selected) {
        if (normalized.length) setSelected(normalized[0]);
        else if (counterpartyNormalized.length) setSelected(counterpartyNormalized[0]);
      }
    } catch (e) {
      setErr(String(e?.message || e || "Failed to load UniSat inscriptions."));
    } finally {
      setLoading(false);
    }
  }

  async function connectAndLoad() {
    await loadInscriptions(0, { prompt: true });
  }

  const displayRows = useMemo(() => {
    const q = String(query || "").trim().toLowerCase();
    const tf = String(typeFilter || "all").trim().toLowerCase();
    const sf = String(sourceFilter || "all").trim().toLowerCase();
    const ordinalRows = (items || []).map((it) => ({ ...it, sourceKind: "ordinal" }));
    const counterpartyRows = (counterpartyItems || []).map((it) => {
      const meta = counterpartyMetadataFor(it, counterpartyMetadataMap);
      return withCounterpartyMedia({
        ...it,
        sourceKind: "counterparty",
        assetMetadata: meta,
        assetLongname: it.assetLongname || meta?.asset_longname || "",
      }, counterpartyMetadataMap);
    });

    return [...ordinalRows, ...counterpartyRows].filter((row) => {
      const rowKind = String(row?.sourceKind || "").toLowerCase();
      if (sf === "ordinals" && rowKind !== "ordinal") return false;
      if (sf === "counterparty" && rowKind !== "counterparty") return false;

      const bucket = contentTypeBucket(row?.contentType);
      if (tf !== "all" && !(tf === "counterparty" && rowKind === "counterparty") && bucket !== tf) return false;
      if (!q) return true;
      return rowSearchText(row).includes(q);
    });
  }, [items, counterpartyItems, counterpartyMetadataMap, query, typeFilter, sourceFilter]);

  const summary = useMemo(() => {
    const out = { total: items.length + counterpartyItems.length, image: 0, video: 0, audio: 0, text: 0, json: 0, external: 0, other: 0, counterparty: counterpartyItems.length, metadata: Object.keys(counterpartyMetadataMap || {}).length };
    for (const it of items || []) {
      const b = contentTypeBucket(it?.contentType);
      if (out[b] === undefined) out.other += 1;
      else out[b] += 1;
    }
    for (const it of counterpartyItems || []) {
      const media = counterpartyMediaFor(it, counterpartyMetadataMap);
      if (!media) continue;
      const b = contentTypeBucket(media.content_type);
      if (out[b] === undefined) out.other += 1;
      else out[b] += 1;
    }
    return out;
  }, [items, counterpartyItems, counterpartyMetadataMap]);

  const pageCanPrev = cursor > 0;
  const pageCanNext = total > 0 ? cursor + pageSize < total : items.length >= pageSize;

  const panelStyle = {
    height: isFullscreen ? "calc(100vh - 16px)" : "100%",
    minHeight: isFullscreen ? "calc(100vh - 16px)" : 420,
    display: "flex",
    flexDirection: "column",
    gap: 10,
    padding: 12,
    boxSizing: "border-box",
    color: "var(--utt-page-fg, #e8eef8)",
    background: "var(--utt-surface-1, #101216)",
    border: "1px solid var(--utt-border-1, rgba(255,255,255,0.12))",
    borderRadius: 14,
    overflow: "hidden",
    position: isFullscreen ? "fixed" : "relative",
    inset: isFullscreen ? 8 : "auto",
    zIndex: isFullscreen ? 1000 : "auto",
    boxShadow: isFullscreen ? "0 24px 80px rgba(0,0,0,0.50)" : "none",
  };
  const cardStyle = {
    border: "1px solid var(--utt-border-1, rgba(255,255,255,0.12))",
    background: "rgba(255,255,255,0.035)",
    borderRadius: 12,
    padding: 10,
  };
  const buttonStyle = {
    border: "1px solid var(--utt-border-1, rgba(255,255,255,0.12))",
    background: "var(--utt-button-bg, rgba(255,255,255,0.06))",
    color: "var(--utt-page-fg, #e8eef8)",
    borderRadius: 10,
    padding: "6px 9px",
    cursor: "pointer",
    fontWeight: 800,
    whiteSpace: "nowrap",
  };
  const inputStyle = {
    border: "1px solid var(--utt-border-1, rgba(255,255,255,0.12))",
    background: "var(--utt-control-bg, rgba(0,0,0,0.28))",
    color: "var(--utt-page-fg, #e8eef8)",
    borderRadius: 10,
    padding: "6px 9px",
  };
  const thStyle = {
    textAlign: "left",
    position: "sticky",
    top: 0,
    zIndex: 2,
    background: "var(--utt-surface-2, #151922)",
    color: "var(--utt-page-fg, #e8eef8)",
    borderBottom: "1px solid var(--utt-border-1, rgba(255,255,255,0.12))",
    padding: "8px 9px",
    fontSize: 12,
    whiteSpace: "nowrap",
  };
  const tdStyle = {
    borderBottom: "1px solid var(--utt-row-border, rgba(255,255,255,0.08))",
    padding: "8px 9px",
    fontSize: 12,
    verticalAlign: "middle",
    whiteSpace: "nowrap",
  };
  const mutedStyle = { color: "var(--utt-muted, rgba(255,255,255,0.66))" };

  const selectedKind = String(selected?.sourceKind || "ordinal").toLowerCase();
  const selectedMetadata = selectedKind === "counterparty" ? counterpartyMetadataFor(selected, counterpartyMetadataMap) : null;
  const selectedCounterpartyMedia = selectedKind === "counterparty" ? counterpartyMediaFor(selected, counterpartyMetadataMap) : null;
  const selectedContentUrl = String(
    selectedKind === "counterparty"
      ? (selectedCounterpartyMedia?.content_url || selectedCounterpartyMedia?.preview_url || selected?.content || selected?.preview || "")
      : (selected?.content || selected?.preview || "")
  ).trim();
  const selectedBucket = contentTypeBucket(selectedKind === "counterparty" ? (selectedCounterpartyMedia?.content_type || selected?.contentType) : selected?.contentType);
  const selectedCounterpartyMarketState = selectedKind === "counterparty"
    ? (counterpartyMarketByAsset?.[String(selected?.asset || "").trim().toUpperCase()] || null)
    : null;
  const selectedCounterpartyMarket = selectedCounterpartyMarketState?.data || null;
  const selectedCounterpartyQuotes = asArray(selectedCounterpartyMarket?.quotes);
  const selectedCounterpartyOrders = compactMarketRows(selectedCounterpartyMarket?.orders, 6);
  const selectedCounterpartyDispensers = compactMarketRows(selectedCounterpartyMarket?.dispensers, 6);

  return (
    <div style={panelStyle}>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 10, flexWrap: "wrap" }}>
        <div>
          <div style={{ fontSize: 18, fontWeight: 950 }}>Bitcoin Assets → NFTs / Collectibles</div>
          <div style={{ ...mutedStyle, marginTop: 3, fontSize: 12 }}>
            UniSat Ordinals are loaded from the browser wallet. Counterparty assets are loaded from the read-only Counterparty API for the same Bitcoin address.
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", justifyContent: "flex-end" }}>
          <button
            type="button"
            style={{ ...buttonStyle, padding: "5px 8px" }}
            onClick={() => setIsFullscreen((prev) => !prev)}
            title={isFullscreen ? "Return to normal window size" : "Expand this window to full screen"}
          >
            {isFullscreen ? "Exit fullscreen" : "Fullscreen"}
          </button>
          <span style={{ ...typeBadgeStyle("text"), color: providerPresent ? "#55e38c" : "#ff6b6b" }}>
            UniSat {providerPresent ? "detected" : "not detected"}
          </span>
          <span style={typeBadgeStyle("external")}>Read-only</span>
          <span style={typeBadgeStyle("other")}>No send/sign/PSBT</span>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(6, minmax(120px, 1fr))", gap: 8 }}>
        <div style={cardStyle}><div style={mutedStyle}>Loaded</div><div style={{ fontWeight: 950, fontSize: 18 }}>{hideTableData ? "••••" : summary.total.toLocaleString()}</div></div>
        <div style={cardStyle}><div style={mutedStyle}>Ordinals</div><div style={{ fontWeight: 950, fontSize: 18 }}>{hideTableData ? "••••" : items.length.toLocaleString()}</div></div>
        <div style={cardStyle} title="Counterparty balances / metadata loaded"><div style={mutedStyle}>Counterparty</div><div style={{ fontWeight: 950, fontSize: 18 }}>{hideTableData ? "••••" : `${summary.counterparty.toLocaleString()} / ${summary.metadata.toLocaleString()}`}</div></div>
        <div style={cardStyle}><div style={mutedStyle}>Images</div><div style={{ fontWeight: 950, fontSize: 18 }}>{hideTableData ? "••••" : summary.image.toLocaleString()}</div></div>
        <div style={cardStyle}><div style={mutedStyle}>Text / JSON</div><div style={{ fontWeight: 950, fontSize: 18 }}>{hideTableData ? "••••" : (summary.text + summary.json).toLocaleString()}</div></div>
        <div style={cardStyle} title="UniSat getBalance result">
          <div style={mutedStyle}>Wallet BTC</div>
          <div style={{ fontWeight: 950, fontSize: 18 }}>
            {hideTableData ? "••••" : fmtBtcFromSats(btcBalance?.total ?? btcBalance?.confirmed ?? btcBalance?.amount ?? btcBalance)}
          </div>
        </div>
      </div>

      <div style={{ ...cardStyle, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
        <button type="button" style={buttonStyle} disabled={loading} onClick={connectAndLoad}>
          {loading ? "Loading…" : address ? "Reconnect UniSat" : "Connect UniSat"}
        </button>
        <button type="button" style={{ ...buttonStyle, opacity: address ? 1 : 0.55 }} disabled={loading || !address} onClick={() => loadInscriptions(cursor, { prompt: false })}>
          Refresh
        </button>
        <button type="button" style={{ ...buttonStyle, opacity: pageCanPrev ? 1 : 0.55 }} disabled={loading || !pageCanPrev} onClick={() => loadInscriptions(Math.max(0, cursor - pageSize), { prompt: false })}>
          Prev
        </button>
        <button type="button" style={{ ...buttonStyle, opacity: pageCanNext ? 1 : 0.55 }} disabled={loading || !pageCanNext} onClick={() => loadInscriptions(cursor + pageSize, { prompt: false })}>
          Next
        </button>
        <select value={pageSize} onChange={(e) => setPageSize(Number(e.target.value) || 20)} style={inputStyle}>
          <option value={10}>10/page</option>
          <option value={20}>20/page</option>
          <option value={50}>50/page</option>
        </select>
        <select value={sourceFilter} onChange={(e) => setSourceFilter(e.target.value)} style={inputStyle}>
          <option value="all">All sources</option>
          <option value="ordinals">Ordinals</option>
          <option value="counterparty">Counterparty</option>
        </select>
        <select value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)} style={inputStyle}>
          <option value="all">All content / asset types</option>
          <option value="image">Images</option>
          <option value="video">Video</option>
          <option value="audio">Audio</option>
          <option value="text">Text</option>
          <option value="json">JSON</option>
          <option value="counterparty">Counterparty assets</option>
          <option value="external">HTML/SVG external</option>
          <option value="other">Other</option>
        </select>
        <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search inscription #, asset, ID, UTXO…" style={{ ...inputStyle, minWidth: 230, flex: "1 1 240px" }} />
        <div style={{ marginLeft: "auto", ...mutedStyle, fontSize: 12 }}>
          Wallet: <b>{hideTableData ? "••••" : maskMiddle(address)}</b> {network ? `• ${network}` : ""} {chain?.enum ? `• ${chain.enum}` : ""}
        </div>
      </div>

      {err ? <div style={{ color: "#ff6b6b", fontSize: 12, whiteSpace: "pre-wrap" }}>{err}</div> : null}
      {counterpartyErr ? <div style={{ color: "#ffb86b", fontSize: 12, whiteSpace: "pre-wrap" }}>Counterparty assets: {counterpartyErr}</div> : null}
      {counterpartyMetadataErr ? <div style={{ color: "#ffb86b", fontSize: 12, whiteSpace: "pre-wrap" }}>Counterparty metadata: {counterpartyMetadataErr}</div> : null}

      <div
        ref={splitLayoutRef}
        style={{
          display: "grid",
          gridTemplateColumns: `minmax(0, 1fr) 10px minmax(280px, ${Math.round(detailPaneWidth)}px)`,
          gap: 0,
          minHeight: 0,
          flex: "1 1 auto",
          alignItems: "stretch",
        }}
      >
        <div style={{ ...cardStyle, padding: 0, overflow: "hidden", minHeight: 0, display: "flex", flexDirection: "column", marginRight: 5 }}>
          <div style={{ flex: "1 1 auto", minHeight: 0, overflow: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={thStyle}>Preview</th>
                  <th style={thStyle}>Type</th>
                  <th style={thStyle}>Name / Inscription #</th>
                  <th style={thStyle}>Standard</th>
                  <th style={thStyle}>Source</th>
                  <th style={thStyle}>ID</th>
                  <th style={thStyle}>Location / UTXO</th>
                  <th style={thStyle}>Value</th>
                  <th style={thStyle}>Seen</th>
                  <th style={thStyle}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {displayRows.length === 0 ? (
                  <tr>
                    <td style={{ ...tdStyle, ...mutedStyle }} colSpan={10}>
                      {loading || counterpartyLoading ? "Loading Bitcoin assets…" : address ? "No Ordinals or Counterparty assets returned for the current filters." : "Connect UniSat to load read-only Ordinals and Counterparty assets."}
                    </td>
                  </tr>
                ) : displayRows.map((it) => {
                  const sourceKind = String(it?.sourceKind || "ordinal").toLowerCase();
                  const isCounterparty = sourceKind === "counterparty";
                  const bucket = contentTypeBucket(it.contentType);
                  const active = isCounterparty
                    ? selected?.sourceKind === "counterparty" && selected?.asset === it.asset && selected?._idx === it._idx
                    : selected?.sourceKind !== "counterparty" && selected?._idx === it._idx && selected?.inscriptionId === it.inscriptionId;
                  const url = String(it.content || it.preview || "").trim();
                  const idText = isCounterparty ? (it.assetLongname || it.asset || "") : (it.inscriptionId || "");
                  const locationText = isCounterparty ? (it.location || it.utxo || it.address || "") : it.location;
                  return (
                    <tr key={`${sourceKind}:${idText || locationText || it._idx}`} onClick={() => setSelected(it)} style={{ background: active ? "rgba(120,160,255,0.08)" : "transparent", cursor: "pointer" }}>
                      <td style={tdStyle}>{renderPreview(it, { fallbackLabel: isCounterparty ? "XCP" : "ORD", hidden: hideTableData })}</td>
                      <td style={tdStyle}><span style={typeBadgeStyle(bucket)}>{isCounterparty ? "Counterparty asset" : (it.contentType || bucket)}</span></td>
                      <td style={tdStyle}>
                        <div style={{ fontWeight: 900 }}>{isCounterparty ? firstText(it.mediaName, it.asset) : (it.inscriptionNumber !== null && it.inscriptionNumber !== undefined ? `Inscription #${it.inscriptionNumber}` : "Inscription")}</div>
                        <div style={{ ...mutedStyle, fontSize: 11 }}>{isCounterparty ? (firstText(it.assetLongname, it.mediaDescription, it.assetMetadata?.issuer ? `Issuer ${maskMiddle(it.assetMetadata.issuer, 6, 4)}` : "", "Protocol balance")) : (bucket === "external" ? "External-open only" : "Safe preview eligible")}</div>
                      </td>
                      <td style={tdStyle}>{isCounterparty ? "Counterparty" : "Ordinals"}</td>
                      <td style={tdStyle}>{isCounterparty ? "Counterparty API" : "UniSat"}</td>
                      <td style={tdStyle} title={idText}>{hideTableData ? "••••" : maskMiddle(idText, 8, 8)}</td>
                      <td style={tdStyle} title={locationText}>{hideTableData ? "••••" : maskMiddle(locationText, 10, 8)}</td>
                      <td style={tdStyle}>{hideTableData ? "••••" : (isCounterparty ? fmtQuantityMaybe(it.quantity) : fmtSats(it.outputValue))}</td>
                      <td style={tdStyle}>{isCounterparty ? fmtTimeMaybe(counterpartyUpdatedAt || updatedAt) : fmtTimeMaybe(it.timestamp)}</td>
                      <td style={tdStyle}>
                        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                          <button type="button" style={{ ...buttonStyle, padding: "4px 7px", fontSize: 11 }} onClick={(e) => { e.stopPropagation(); setSelected(it); }}>Preview</button>
                          <button type="button" style={{ ...buttonStyle, padding: "4px 7px", fontSize: 11 }} onClick={(e) => { e.stopPropagation(); copyTextSafe(idText); }}>{isCounterparty ? "Copy Asset" : "Copy ID"}</button>
                          {!isCounterparty && url ? <a href={url} target="_blank" rel="noreferrer" style={{ ...buttonStyle, padding: "4px 7px", fontSize: 11, textDecoration: "none" }} onClick={(e) => e.stopPropagation()}>Open</a> : null}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <div
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize asset list and preview panes"
          onMouseDown={startSplitResize}
          style={{
            cursor: "col-resize",
            width: 10,
            minWidth: 10,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
          title="Drag to resize panes"
        >
          <div style={{ width: 4, height: "100%", borderRadius: 999, background: "rgba(255,255,255,0.12)" }} />
        </div>

        <div style={{ ...cardStyle, minHeight: 0, overflow: "auto", marginLeft: 5 }}>
          <div style={{ fontWeight: 950, marginBottom: 8 }}>Selected preview</div>
          {selected ? (
            <>
              <div style={{ display: "flex", justifyContent: "center", alignItems: "center", minHeight: 190, border: "1px solid rgba(255,255,255,0.10)", borderRadius: 12, background: "rgba(0,0,0,0.18)", overflow: "hidden" }}>
                {hideTableData ? (
                  <div style={{ padding: 16, textAlign: "center", ...mutedStyle, fontWeight: 900 }}>••••</div>
                ) : selectedKind === "counterparty" && selectedContentUrl && selectedBucket === "image" ? (
                  <ResilientMediaImage
                    alt="selected Counterparty asset media"
                    src={selectedContentUrl}
                    candidates={selectedCounterpartyMedia?.content_url_candidates || selectedCounterpartyMedia?.preview_url_candidates || selectedCounterpartyMedia?.image_url_candidates}
                    loading="eager"
                    style={{ maxWidth: "100%", maxHeight: 260, objectFit: "contain" }}
                    fallbackStyle={{ padding: 16, textAlign: "center", ...mutedStyle }}
                  />
                ) : selectedKind === "counterparty" && selectedContentUrl && selectedBucket === "video" ? (
                  <video src={selectedContentUrl} controls style={{ maxWidth: "100%", maxHeight: 260 }} />
                ) : selectedKind === "counterparty" && selectedContentUrl && selectedBucket === "audio" ? (
                  <audio src={selectedContentUrl} controls style={{ width: "92%" }} />
                ) : selectedKind === "counterparty" ? (
                  <div style={{ padding: 18, textAlign: "center" }}>
                    <div style={{ fontSize: 26, fontWeight: 950 }}>{firstText(selectedCounterpartyMedia?.name, selected.asset, "Counterparty")}</div>
                    <div style={{ marginTop: 8, ...mutedStyle }}>{selectedMetadata?.asset_longname || selectedCounterpartyMedia?.description || "Counterparty asset balance"}</div>
                    <div style={{ marginTop: 8, fontWeight: 900 }}>{fmtQuantityMaybe(selected.quantity)}</div>
                    <div style={{ marginTop: 8, display: "flex", gap: 6, justifyContent: "center", flexWrap: "wrap" }}>
                      <span style={typeBadgeStyle("counterparty")}>Divisible: {boolLabel(selectedMetadata?.divisible)}</span>
                      <span style={typeBadgeStyle(selectedMetadata?.locked ? "external" : "other")}>Locked: {boolLabel(selectedMetadata?.locked)}</span>
                    </div>
                  </div>
                ) : selectedContentUrl && selectedBucket === "image" ? (
                  <img alt="selected inscription" src={selectedContentUrl} referrerPolicy="no-referrer" style={{ maxWidth: "100%", maxHeight: 260, objectFit: "contain" }} />
                ) : selectedContentUrl && selectedBucket === "video" ? (
                  <video src={selectedContentUrl} controls style={{ maxWidth: "100%", maxHeight: 260 }} />
                ) : selectedContentUrl && selectedBucket === "audio" ? (
                  <audio src={selectedContentUrl} controls style={{ width: "92%" }} />
                ) : selectedBucket === "text" || selectedBucket === "json" ? (
                  <div style={{ padding: 16, textAlign: "center", ...mutedStyle }}>Text/JSON inscription. Use Open for full content.</div>
                ) : (
                  <div style={{ padding: 16, textAlign: "center", ...mutedStyle }}>External preview only for this content type.</div>
                )}
              </div>

              <div style={{ marginTop: 10, display: "grid", gap: 6, fontSize: 12 }}>
                {selectedKind === "counterparty" ? (
                  <>
                    <div><b>Standard:</b> Counterparty</div>
                    <div title={selected.asset}><b>Asset:</b> {hideTableData ? "••••" : selected.asset || "—"}</div>
                    <div title={selected.assetLongname || selected.asset_longname || selectedMetadata?.asset_longname}><b>Longname:</b> {hideTableData ? "••••" : (selected.assetLongname || selected.asset_longname || selectedMetadata?.asset_longname || "—")}</div>
                    <div><b>Quantity:</b> {hideTableData ? "••••" : fmtQuantityMaybe(selected.quantity)}</div>
                    <div title={selectedMetadata?.supply}><b>Supply:</b> {hideTableData ? "••••" : fmtQuantityMaybe(selectedMetadata?.supply)}</div>
                    <div title={selectedMetadata?.issuer}><b>Issuer / Owner:</b> {hideTableData ? "••••" : maskMiddle(selectedMetadata?.issuer, 10, 10)}</div>
                    <div><b>Divisible:</b> {hideTableData ? "••••" : boolLabel(selectedMetadata?.divisible)}</div>
                    <div><b>Locked:</b> {hideTableData ? "••••" : boolLabel(selectedMetadata?.locked)}</div>
                    <div title={selectedMetadata?.description}><b>Description:</b> {hideTableData ? "••••" : (selectedCounterpartyMedia?.description || selectedMetadata?.description || "—")}</div>
                    <div title={selectedCounterpartyMedia?.name}><b>Media Name:</b> {hideTableData ? "••••" : (selectedCounterpartyMedia?.name || "—")}</div>
                    <div title={selectedCounterpartyMedia?.content_url}><b>Media URL:</b> {hideTableData ? "••••" : maskMiddle(selectedCounterpartyMedia?.content_url, 18, 14)}</div>
                    <div title={selectedMetadata?.external_metadata_url}><b>Metadata URL:</b> {hideTableData ? "••••" : maskMiddle(selectedMetadata?.external_metadata_url, 18, 14)}</div>
                    <div title={selected.address}><b>Address:</b> {hideTableData ? "••••" : maskMiddle(selected.address, 10, 10)}</div>
                    <div title={selected.utxo || selected.location}><b>UTXO:</b> {hideTableData ? "••••" : maskMiddle(selected.utxo || selected.location, 10, 10)}</div>
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 4 }}>
                      <button type="button" style={{ ...buttonStyle, padding: "5px 8px" }} onClick={() => copyTextSafe(selected.asset)}>Copy Asset</button>
                      {selectedMetadata?.issuer ? <button type="button" style={{ ...buttonStyle, padding: "5px 8px" }} onClick={() => copyTextSafe(selectedMetadata.issuer)}>Copy Issuer</button> : null}
                      {selectedCounterpartyMedia?.content_url ? <a href={selectedCounterpartyMedia.content_url} target="_blank" rel="noreferrer" style={{ ...buttonStyle, padding: "5px 8px", textDecoration: "none" }}>Open media</a> : null}
                      {selectedMetadata?.external_metadata_url ? <a href={selectedMetadata.external_metadata_url} target="_blank" rel="noreferrer" style={{ ...buttonStyle, padding: "5px 8px", textDecoration: "none" }}>Open metadata</a> : null}
                    </div>

                    <div style={{ marginTop: 10, paddingTop: 10, borderTop: "1px solid rgba(255,255,255,0.10)", display: "grid", gap: 8 }}>
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, flexWrap: "wrap" }}>
                        <div>
                          <b>Market context</b>
                          <div style={{ ...mutedStyle, fontSize: 11 }}>Read-only orders / dispensers. No compose, sign, send, or broadcast.</div>
                        </div>
                        <button
                          type="button"
                          style={{ ...buttonStyle, padding: "4px 7px", fontSize: 11, opacity: selected?.asset ? 1 : 0.55 }}
                          disabled={!selected?.asset || selectedCounterpartyMarketState?.loading}
                          onClick={() => loadCounterpartyMarketContext(selected.asset, { force: true })}
                        >
                          {selectedCounterpartyMarketState?.loading ? "Loading…" : "Refresh market"}
                        </button>
                      </div>

                      {selectedCounterpartyMarketState?.error ? (
                        <div style={{ color: "#ffb86b", fontSize: 12, whiteSpace: "pre-wrap" }}>{selectedCounterpartyMarketState.error}</div>
                      ) : null}

                      {selectedCounterpartyMarket ? (
                        <>
                          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 6 }}>
                            <div style={{ ...cardStyle, padding: 8 }}><div style={mutedStyle}>Open Orders</div><div style={{ fontWeight: 900 }}>{hideTableData ? "••••" : (selectedCounterpartyMarket.summary?.open_orders ?? 0).toLocaleString()}</div></div>
                            <div style={{ ...cardStyle, padding: 8 }}><div style={mutedStyle}>Dispensers</div><div style={{ fontWeight: 900 }}>{hideTableData ? "••••" : (selectedCounterpartyMarket.summary?.open_dispensers ?? 0).toLocaleString()}</div></div>
                            <div style={{ ...cardStyle, padding: 8 }}><div style={mutedStyle}>Quotes</div><div style={{ fontWeight: 900 }}>{hideTableData ? "••••" : (selectedCounterpartyMarket.summary?.quote_count ?? selectedCounterpartyQuotes.length).toLocaleString()}</div></div>
                          </div>

                          {selectedCounterpartyQuotes.length ? (
                            <div style={{ display: "grid", gap: 4 }}>
                              <div style={{ fontWeight: 900 }}>Best bid / ask</div>
                              {selectedCounterpartyQuotes.slice(0, 4).map((q) => (
                                <div key={q.quote_asset || "quote"} style={{ display: "grid", gridTemplateColumns: "70px 1fr 1fr", gap: 6, fontSize: 12 }}>
                                  <span style={mutedStyle}>{q.quote_asset || "—"}</span>
                                  <span>Bid {hideTableData ? "••••" : fmtMarketPrice(q.best_bid, q.quote_asset)}</span>
                                  <span>Ask {hideTableData ? "••••" : fmtMarketPrice(q.best_ask, q.quote_asset)}</span>
                                </div>
                              ))}
                            </div>
                          ) : <div style={{ ...mutedStyle, fontSize: 12 }}>No computable bid/ask quote returned yet.</div>}

                          {selectedCounterpartyOrders.length ? (
                            <div style={{ overflowX: "auto" }}>
                              <div style={{ fontWeight: 900, marginBottom: 4 }}>Open / recent orders</div>
                              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                                <thead><tr><th style={thStyle}>Side</th><th style={thStyle}>Quote</th><th style={thStyle}>Price</th><th style={thStyle}>Qty Rem</th><th style={thStyle}>Status</th></tr></thead>
                                <tbody>
                                  {selectedCounterpartyOrders.map((o, idx) => (
                                    <tr key={`${o.tx_hash || "order"}:${idx}`}>
                                      <td style={tdStyle}>{String(o.side || "—").toUpperCase()}</td>
                                      <td style={tdStyle}>{o.quote_asset || "—"}</td>
                                      <td style={tdStyle}>{hideTableData ? "••••" : fmtMarketPrice(o.price, o.quote_asset)}</td>
                                      <td style={tdStyle}>{hideTableData ? "••••" : fmtMarketQty(o.base_remaining ?? o.base_quantity)}</td>
                                      <td style={tdStyle}>{o.status || "—"}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : <div style={{ ...mutedStyle, fontSize: 12 }}>No order rows returned for this asset.</div>}

                          {selectedCounterpartyDispensers.length ? (
                            <div style={{ overflowX: "auto" }}>
                              <div style={{ fontWeight: 900, marginBottom: 4 }}>Dispensers</div>
                              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                                <thead><tr><th style={thStyle}>Source</th><th style={thStyle}>Qty</th><th style={thStyle}>BTC / Unit</th><th style={thStyle}>Status</th></tr></thead>
                                <tbody>
                                  {selectedCounterpartyDispensers.map((d, idx) => (
                                    <tr key={`${d.tx_hash || "dispenser"}:${idx}`}>
                                      <td style={tdStyle} title={d.source || ""}>{hideTableData ? "••••" : maskMiddle(d.source, 6, 5)}</td>
                                      <td style={tdStyle}>{hideTableData ? "••••" : fmtMarketQty(d.give_remaining ?? d.give_quantity)}</td>
                                      <td style={tdStyle}>{hideTableData ? "••••" : fmtMarketPrice(d.price_btc_per_unit ?? d.price_btc, "BTC")}</td>
                                      <td style={tdStyle}>{d.status || "—"}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : <div style={{ ...mutedStyle, fontSize: 12 }}>No dispenser rows returned for this asset.</div>}
                        </>
                      ) : selectedCounterpartyMarketState?.loading ? (
                        <div style={{ ...mutedStyle, fontSize: 12 }}>Loading read-only market context…</div>
                      ) : (
                        <div style={{ ...mutedStyle, fontSize: 12 }}>Select Refresh market to load read-only order / dispenser context.</div>
                      )}
                    </div>
                  </>
                ) : (
                  <>
                    <div><b>Content Type:</b> {selected.contentType || "—"}</div>
                    <div title={selected.inscriptionId}><b>ID:</b> {hideTableData ? "••••" : maskMiddle(selected.inscriptionId, 10, 10)}</div>
                    <div title={selected.location}><b>Location:</b> {hideTableData ? "••••" : maskMiddle(selected.location, 10, 10)}</div>
                    <div><b>Output:</b> {hideTableData ? "••••" : fmtSats(selected.outputValue)}</div>
                    <div><b>Genesis TX:</b> {hideTableData ? "••••" : maskMiddle(selected.genesisTransaction, 10, 10)}</div>
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 4 }}>
                      <button type="button" style={{ ...buttonStyle, padding: "5px 8px" }} onClick={() => copyTextSafe(selected.inscriptionId)}>Copy ID</button>
                      {selectedContentUrl ? <a href={selectedContentUrl} target="_blank" rel="noreferrer" style={{ ...buttonStyle, padding: "5px 8px", textDecoration: "none" }}>Open content</a> : null}
                    </div>
                  </>
                )}
              </div>
            </>
          ) : (
            <div style={mutedStyle}>Select an inscription row to preview metadata.</div>
          )}

          <div style={{ marginTop: 12, paddingTop: 10, borderTop: "1px solid rgba(255,255,255,0.10)", ...mutedStyle, fontSize: 12, lineHeight: 1.45 }}>
            Safe inline preview is limited to image/video/audio/text/json. Counterparty media is read from external JSON metadata URLs when available. HTML, SVG, scripts, and unknown MIME types should be opened externally only.
          </div>
          {providerInfo?.utt_policy ? (
            <div style={{ marginTop: 8, ...mutedStyle, fontSize: 12 }}>Policy: {providerInfo.utt_policy}</div>
          ) : null}
          {updatedAt ? <div style={{ marginTop: 8, ...mutedStyle, fontSize: 11 }}>Ordinals updated: {fmtTimeMaybe(updatedAt)}</div> : null}
          {counterpartyUpdatedAt ? <div style={{ marginTop: 4, ...mutedStyle, fontSize: 11 }}>Counterparty updated: {fmtTimeMaybe(counterpartyUpdatedAt)}</div> : null}
          {counterpartyMetadataLoading ? <div style={{ marginTop: 4, ...mutedStyle, fontSize: 11 }}>Counterparty metadata loading…</div> : null}
        </div>
      </div>
    </div>
  );
}
