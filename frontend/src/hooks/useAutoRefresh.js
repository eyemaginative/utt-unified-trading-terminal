// frontend/src/hooks/useAutoRefresh.js
import { useCallback, useMemo, useState } from "react";
import { useInterval } from "./useInterval";

/**
 * useAutoRefresh(asyncFn, options)
 *
 * Standardizes:
 * - enabled toggle
 * - interval seconds
 * - loading/error state
 * - lastUpdated timestamp
 *
 * asyncFn should be stable (useCallback in callers), or accept it being re-bound.
 */
export function useAutoRefresh(
  asyncFn,
  { enabledDefault = true, intervalSecDefault = 300 } = {}
) {
  const [enabled, setEnabled] = useState(!!enabledDefault);
  const [intervalSec, setIntervalSec] = useState(Number(intervalSecDefault) || 300);
  const [lastUpdated, setLastUpdated] = useState(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const run = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      await asyncFn?.();
      setLastUpdated(Date.now());
    } catch (e) {
      const msg =
        e?.response?.data?.detail ||
        e?.response?.data?.message ||
        e?.message ||
        "refresh failed";
      setError(String(msg));
    } finally {
      setLoading(false);
    }
  }, [asyncFn]);

  const delayMs = useMemo(() => {
    if (!enabled) return null;
    const sec = Number(intervalSec);
    if (!Number.isFinite(sec) || sec <= 0) return null;
    return sec * 1000;
  }, [enabled, intervalSec]);

  // Background ticking refresh
  useInterval(() => {
    run();
  }, delayMs);

  return {
    enabled,
    setEnabled,
    intervalSec,
    setIntervalSec,
    lastUpdated,
    loading,
    error,
    run,
  };
}
