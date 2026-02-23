// frontend/src/hooks/useLocalStorageState.js
import { useEffect, useState } from "react";

/**
 * useLocalStorageState(key, initialValue)
 * - Persists JSON to localStorage
 * - Safe if localStorage is blocked/unavailable
 */
export function useLocalStorageState(key, initialValue) {
  const [value, setValue] = useState(() => {
    try {
      const raw = localStorage.getItem(key);
      if (raw === null || raw === undefined) return initialValue;
      return JSON.parse(raw);
    } catch {
      return initialValue;
    }
  });

  useEffect(() => {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      // ignore quota/private mode/blocked storage
    }
  }, [key, value]);

  return [value, setValue];
}
