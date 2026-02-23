// frontend/src/components/common/RefreshControls.jsx
import React, { useMemo } from "react";

/**
 * RefreshControls
 *
 * Presentational control strip for auto-refresh behavior.
 * Intentionally UI-only; the parent owns state and handlers.
 *
 * Props:
 * - enabled: boolean
 * - seconds: number|string
 * - onToggleEnabled(nextBool)
 * - onChangeSeconds(nextNumber)
 * - onRefreshNow()
 *
 * Optional:
 * - minSeconds (default 3)
 * - maxSeconds (default 3600)
 * - disabled (default false)
 * - label (default "Refresh")
 * - showLastUpdated (default false)
 * - lastUpdatedText (string)
 * - compact (default false)
 */
export default function RefreshControls({
  enabled,
  seconds,
  onToggleEnabled,
  onChangeSeconds,
  onRefreshNow,

  minSeconds = 3,
  maxSeconds = 3600,
  disabled = false,
  label = "Refresh",
  showLastUpdated = false,
  lastUpdatedText = "",
  compact = false,
}) {
  const styles = useMemo(() => {
    const pillPad = compact ? "4px 6px" : "6px 8px";
    const ctlPad = compact ? "4px 6px" : "5px 8px";
    const btnPad = compact ? "5px 8px" : "6px 9px";

    return {
      wrap: {
        display: "flex",
        gap: 10,
        rowGap: 8,
        alignItems: "center",
        flexWrap: "wrap",
      },
      pill: {
        display: "inline-flex",
        alignItems: "center",
        gap: 8,
        padding: pillPad,
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 10,
        background: "var(--utt-surface-2, #151515)",
        color: "var(--utt-page-fg, #eee)",
      },
      select: {
        background: "var(--utt-control-bg, #0f0f0f)",
        color: "var(--utt-page-fg, #eee)",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 8,
        padding: ctlPad,
      },
      input: {
        width: 86,
        background: "var(--utt-control-bg, #0f0f0f)",
        color: "var(--utt-page-fg, #eee)",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 8,
        padding: ctlPad,
      },
      button: {
        background: "var(--utt-button-bg, #1b1b1b)",
        color: "var(--utt-page-fg, #eee)",
        border: "1px solid var(--utt-border-1, #2a2a2a)",
        borderRadius: 10,
        padding: btnPad,
        cursor: "pointer",
        whiteSpace: "nowrap",
      },
      buttonDisabled: {
        opacity: 0.55,
        cursor: "not-allowed",
      },
      muted: {
        opacity: 0.75,
        fontSize: compact ? 11 : 12,
      },
      label: {
        fontWeight: 700,
        fontSize: compact ? 12 : 13,
      },
      chkWrap: {
        display: "inline-flex",
        alignItems: "center",
        gap: 8,
      },
    };
  }, [compact]);

  const secNumber = normalizeSeconds(seconds, minSeconds, maxSeconds);

  const canInteract = !disabled;
  const canRefreshNow = canInteract && typeof onRefreshNow === "function";
  const canToggle = canInteract && typeof onToggleEnabled === "function";
  const canChangeSeconds = canInteract && typeof onChangeSeconds === "function";

  return (
    <div style={styles.wrap}>
      <div style={styles.pill}>
        <span style={styles.label}>{label}</span>

        <label style={styles.chkWrap} title="Enable automatic refresh">
          <input
            type="checkbox"
            checked={!!enabled}
            disabled={!canToggle}
            onChange={(e) => {
              if (!canToggle) return;
              onToggleEnabled(!!e.target.checked);
            }}
          />
          <span style={styles.muted}>Auto</span>
        </label>

        <span style={styles.muted}>Every</span>

        <input
          style={styles.input}
          type="number"
          min={minSeconds}
          max={maxSeconds}
          step={1}
          value={String(seconds ?? "")}
          disabled={!canChangeSeconds}
          onChange={(e) => {
            if (!canChangeSeconds) return;
            const raw = e.target.value;
            // Allow empty while typing; parent can decide how to store it.
            if (raw === "") {
              onChangeSeconds("");
              return;
            }
            const n = Number(raw);
            if (!Number.isFinite(n)) return;
            onChangeSeconds(clampInt(n, minSeconds, maxSeconds));
          }}
          onBlur={() => {
            // On blur, “normalize” if parent is keeping a string.
            if (!canChangeSeconds) return;
            onChangeSeconds(secNumber);
          }}
        />

        <span style={styles.muted}>sec</span>

        <button
          type="button"
          style={{ ...styles.button, ...(canRefreshNow ? null : styles.buttonDisabled) }}
          disabled={!canRefreshNow}
          onClick={() => {
            if (!canRefreshNow) return;
            onRefreshNow();
          }}
        >
          Refresh now
        </button>

        {showLastUpdated ? <span style={styles.muted}>{lastUpdatedText || ""}</span> : null}
      </div>
    </div>
  );
}

function clampInt(n, min, max) {
  const x = Math.trunc(Number(n));
  if (!Number.isFinite(x)) return min;
  if (x < min) return min;
  if (x > max) return max;
  return x;
}

function normalizeSeconds(value, minSeconds, maxSeconds) {
  const n = Number(value);
  if (!Number.isFinite(n)) return clampInt(minSeconds, minSeconds, maxSeconds);
  return clampInt(n, minSeconds, maxSeconds);
}
