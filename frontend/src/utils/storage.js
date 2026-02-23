// frontend/src/utils/storage.js

export function safeParseJson(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

export function readBoolLS(key, defaultVal) {
  const raw = localStorage.getItem(key);
  if (raw === null || raw === undefined) return defaultVal;

  const parsed = safeParseJson(raw);
  if (parsed === null) {
    const s = String(raw).toLowerCase().trim();
    if (s === "true") return true;
    if (s === "false") return false;
    return defaultVal;
  }
  return !!parsed;
}

export function readNumLS(key, defaultVal) {
  const raw = localStorage.getItem(key);
  if (raw === null || raw === undefined) return defaultVal;
  const n = Number(raw);
  return Number.isFinite(n) ? n : defaultVal;
}
