// frontend/src/lib/sharedFetch.js
// Coalesce identical GET requests across components/windows within the same SPA.
// Also provides a short TTL cache to avoid burst refresh when multiple windows poll together.

const inflight = new Map(); // url -> Promise
const cache = new Map(); // url -> { ts, data }

function nowMs() {
  return Date.now();
}

export async function sharedFetchJSON(url, { signal, ttlMs = 1500 } = {}) {
  const u = String(url || "");
  if (!u) throw new Error("sharedFetchJSON: url is required");

  const now = nowMs();
  const cached = cache.get(u);
  if (cached && now - cached.ts <= ttlMs) {
    return cached.data;
  }

  const existing = inflight.get(u);
  if (existing) return existing;

  const p = (async () => {
    const res = await fetch(u, { method: "GET", signal });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`GET ${u} failed (${res.status}): ${txt || res.statusText}`);
    }
    const json = await res.json().catch(() => null);
    cache.set(u, { ts: nowMs(), data: json });
    return json;
  })();

  inflight.set(u, p);

  try {
    return await p;
  } finally {
    inflight.delete(u);
  }
}

export function sharedFetchClear(urlPrefix = "") {
  const p = String(urlPrefix || "");
  for (const k of cache.keys()) {
    if (!p || k.startsWith(p)) cache.delete(k);
  }
}
