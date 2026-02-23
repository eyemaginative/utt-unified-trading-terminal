// frontend/src/app/useAppState.js
import { useCallback, useMemo } from "react";
import { useLocalStorageState } from "../hooks/useLocalStorageState";

import {
  LS_KEYS,
  DEFAULT_POLL_ENABLED,
  DEFAULT_POLL_SECONDS,
  DEFAULT_WINDOWS_STATE,
  clampInt,
} from "./constants";

/**
 * useAppState
 *
 * Centralizes “App shell” state so App.jsx can become a thin view layer.
 * This file is intentionally conservative and does not assume your full current App.jsx structure.
 *
 * Nothing imports this yet (safe).
 */
export default function useAppState() {
  // Background polling preferences
  const [pollEnabled, setPollEnabled] = useLocalStorageState(
    LS_KEYS.POLL_ENABLED,
    DEFAULT_POLL_ENABLED
  );
  const [pollSecondsRaw, setPollSecondsRaw] = useLocalStorageState(
    LS_KEYS.POLL_SECONDS,
    DEFAULT_POLL_SECONDS
  );

  // Window open/refresh settings
  const [windowsState, setWindowsState] = useLocalStorageState(
    LS_KEYS.WINDOWS_STATE,
    DEFAULT_WINDOWS_STATE
  );

  const pollSeconds = useMemo(() => clampInt(pollSecondsRaw, 3, 3600), [pollSecondsRaw]);

  const setPollSeconds = useCallback(
    (next) => {
      // Allow typing "" for inputs; normalize on blur in UI later.
      if (next === "") {
        setPollSecondsRaw("");
        return;
      }
      setPollSecondsRaw(clampInt(next, 3, 3600));
    },
    [setPollSecondsRaw]
  );

  const openWindow = useCallback(
    (id) => {
      const key = String(id || "").trim();
      if (!key) return;
      setWindowsState((prev) => {
        const cur = prev && typeof prev === "object" ? prev : {};
        const row = cur[key] || {};
        return { ...cur, [key]: { ...row, open: true } };
      });
    },
    [setWindowsState]
  );

  const closeWindow = useCallback(
    (id) => {
      const key = String(id || "").trim();
      if (!key) return;
      setWindowsState((prev) => {
        const cur = prev && typeof prev === "object" ? prev : {};
        const row = cur[key] || {};
        return { ...cur, [key]: { ...row, open: false } };
      });
    },
    [setWindowsState]
  );

  const toggleWindow = useCallback(
    (id) => {
      const key = String(id || "").trim();
      if (!key) return;
      setWindowsState((prev) => {
        const cur = prev && typeof prev === "object" ? prev : {};
        const row = cur[key] || {};
        const nextOpen = !row.open;
        return { ...cur, [key]: { ...row, open: nextOpen } };
      });
    },
    [setWindowsState]
  );

  const setWindowRefresh = useCallback(
    (id, { refresh_enabled, refresh_seconds } = {}) => {
      const key = String(id || "").trim();
      if (!key) return;

      setWindowsState((prev) => {
        const cur = prev && typeof prev === "object" ? prev : {};
        const row = cur[key] || {};

        const next = { ...row };
        if (refresh_enabled !== undefined) next.refresh_enabled = !!refresh_enabled;
        if (refresh_seconds !== undefined) {
          if (refresh_seconds === "") next.refresh_seconds = "";
          else next.refresh_seconds = clampInt(refresh_seconds, 3, 3600);
        }

        return { ...cur, [key]: next };
      });
    },
    [setWindowsState]
  );

  return {
    // polling
    pollEnabled: !!pollEnabled,
    setPollEnabled,
    pollSeconds,
    pollSecondsRaw,
    setPollSeconds,

    // windows
    windowsState,
    setWindowsState,
    openWindow,
    closeWindow,
    toggleWindow,
    setWindowRefresh,
  };
}
