// frontend/src/hooks/useInterval.js
import { useEffect, useRef } from "react";

/**
 * useInterval(callback, delayMs, { immediate })
 *
 * - delayMs: number in milliseconds, or null/undefined to disable
 * - immediate: if true, runs once immediately when enabled
 */
export function useInterval(callback, delayMs, { immediate = false } = {}) {
  const cbRef = useRef(callback);

  // Always keep latest callback without resetting interval
  useEffect(() => {
    cbRef.current = callback;
  }, [callback]);

  useEffect(() => {
    if (delayMs === null || delayMs === undefined) return;

    const ms = Number(delayMs);
    if (!Number.isFinite(ms) || ms <= 0) return;

    if (immediate) {
      try {
        cbRef.current?.();
      } catch {
        // swallow; interval continues
      }
    }

    const id = setInterval(() => {
      try {
        cbRef.current?.();
      } catch {
        // swallow; interval continues
      }
    }, ms);

    return () => clearInterval(id);
  }, [delayMs, immediate]);
}
