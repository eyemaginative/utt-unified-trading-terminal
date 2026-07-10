// frontend/src/features/nfts/NftCollectiblesWindow.jsx
import { useEffect, useMemo, useState } from "react";

const LS_UNISAT_ADDR_KEY = "utt_nft_unisat_address_v1";

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
    payload.raw,
    payload.result,
    payload.data,
    payload.items,
    payload.balances,
    payload.rows,
    payload.records,
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
    return <img alt="inscription preview" src={previewUrl} loading="lazy" referrerPolicy="no-referrer" style={{ ...boxStyle, objectFit: "cover" }} />;
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
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [selected, setSelected] = useState(null);
  const [updatedAt, setUpdatedAt] = useState(null);

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
      return normalized;
    } catch (e) {
      setCounterpartyErr(String(e?.message || e || "Failed to load Counterparty assets."));
      return [];
    } finally {
      setCounterpartyLoading(false);
    }
  }

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
    const counterpartyRows = (counterpartyItems || []).map((it) => ({ ...it, sourceKind: "counterparty" }));

    return [...ordinalRows, ...counterpartyRows].filter((row) => {
      const rowKind = String(row?.sourceKind || "").toLowerCase();
      if (sf === "ordinals" && rowKind !== "ordinal") return false;
      if (sf === "counterparty" && rowKind !== "counterparty") return false;

      const bucket = contentTypeBucket(row?.contentType);
      if (tf !== "all" && bucket !== tf) return false;
      if (!q) return true;
      return rowSearchText(row).includes(q);
    });
  }, [items, counterpartyItems, query, typeFilter, sourceFilter]);

  const summary = useMemo(() => {
    const out = { total: items.length + counterpartyItems.length, image: 0, video: 0, audio: 0, text: 0, json: 0, external: 0, other: 0, counterparty: counterpartyItems.length };
    for (const it of items || []) {
      const b = contentTypeBucket(it?.contentType);
      if (out[b] === undefined) out.other += 1;
      else out[b] += 1;
    }
    return out;
  }, [items, counterpartyItems]);

  const pageCanPrev = cursor > 0;
  const pageCanNext = total > 0 ? cursor + pageSize < total : items.length >= pageSize;

  const panelStyle = {
    height: "100%",
    minHeight: 420,
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
  const selectedContentUrl = String(selected?.content || selected?.preview || "").trim();
  const selectedBucket = contentTypeBucket(selected?.contentType);

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
        <div style={cardStyle}><div style={mutedStyle}>Counterparty</div><div style={{ fontWeight: 950, fontSize: 18 }}>{hideTableData ? "••••" : summary.counterparty.toLocaleString()}</div></div>
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

      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) 320px", gap: 10, minHeight: 0, flex: "1 1 auto" }}>
        <div style={{ ...cardStyle, padding: 0, overflow: "hidden", minHeight: 0 }}>
          <div style={{ maxHeight: Math.max(260, Number(height) - 250), overflow: "auto" }}>
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
                        <div style={{ fontWeight: 900 }}>{isCounterparty ? it.asset : (it.inscriptionNumber !== null && it.inscriptionNumber !== undefined ? `Inscription #${it.inscriptionNumber}` : "Inscription")}</div>
                        <div style={{ ...mutedStyle, fontSize: 11 }}>{isCounterparty ? (it.assetLongname || "Protocol balance") : (bucket === "external" ? "External-open only" : "Safe preview eligible")}</div>
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

        <div style={{ ...cardStyle, minHeight: 0, overflow: "auto" }}>
          <div style={{ fontWeight: 950, marginBottom: 8 }}>Selected preview</div>
          {selected ? (
            <>
              <div style={{ display: "flex", justifyContent: "center", alignItems: "center", minHeight: 190, border: "1px solid rgba(255,255,255,0.10)", borderRadius: 12, background: "rgba(0,0,0,0.18)", overflow: "hidden" }}>
                {hideTableData ? (
                  <div style={{ padding: 16, textAlign: "center", ...mutedStyle, fontWeight: 900 }}>••••</div>
                ) : selectedKind === "counterparty" ? (
                  <div style={{ padding: 18, textAlign: "center" }}>
                    <div style={{ fontSize: 26, fontWeight: 950 }}>{selected.asset || "Counterparty"}</div>
                    <div style={{ marginTop: 8, ...mutedStyle }}>Counterparty asset balance</div>
                    <div style={{ marginTop: 8, fontWeight: 900 }}>{fmtQuantityMaybe(selected.quantity)}</div>
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
                    <div title={selected.assetLongname || selected.asset_longname}><b>Longname:</b> {hideTableData ? "••••" : (selected.assetLongname || selected.asset_longname || "—")}</div>
                    <div><b>Quantity:</b> {hideTableData ? "••••" : fmtQuantityMaybe(selected.quantity)}</div>
                    <div title={selected.address}><b>Address:</b> {hideTableData ? "••••" : maskMiddle(selected.address, 10, 10)}</div>
                    <div title={selected.utxo || selected.location}><b>UTXO:</b> {hideTableData ? "••••" : maskMiddle(selected.utxo || selected.location, 10, 10)}</div>
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 4 }}>
                      <button type="button" style={{ ...buttonStyle, padding: "5px 8px" }} onClick={() => copyTextSafe(selected.asset)}>Copy Asset</button>
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
            Safe inline preview is limited to image/video/audio/text/json. Counterparty rows are balance metadata only. HTML, SVG, scripts, and unknown MIME types should be opened externally only.
          </div>
          {providerInfo?.utt_policy ? (
            <div style={{ marginTop: 8, ...mutedStyle, fontSize: 12 }}>Policy: {providerInfo.utt_policy}</div>
          ) : null}
          {updatedAt ? <div style={{ marginTop: 8, ...mutedStyle, fontSize: 11 }}>Ordinals updated: {fmtTimeMaybe(updatedAt)}</div> : null}
          {counterpartyUpdatedAt ? <div style={{ marginTop: 4, ...mutedStyle, fontSize: 11 }}>Counterparty updated: {fmtTimeMaybe(counterpartyUpdatedAt)}</div> : null}
        </div>
      </div>
    </div>
  );
}
