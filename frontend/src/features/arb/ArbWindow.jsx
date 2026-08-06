// frontend/src/features/arb/ArbWindow.jsx
import ArbChip from "../../ArbChip";

export default function ArbWindow({
  styles,
  apiBase,
  symbolCanon,
  venues,
  fmtPrice,
  fetchArbSnapshot,
  hideTableData = false,
  hideVenueNames = false,
}) {
  return (
    <div
      style={{
        height: "100%",
        minHeight: 0,
        overflow: "hidden",
        background: "var(--utt-surface-1, #121212)",
      }}
    >
      <ArbChip
        apiBase={apiBase}
        symbol={symbolCanon}
        venues={venues}
        refreshMs={8000}
        fmtPrice={fmtPrice}
        hideTableData={hideTableData}
        hideVenueNames={hideVenueNames}
        styles={styles}
        thresholdPct={0.1}
        fetchArbSnapshot={fetchArbSnapshot}
        chipVariant="window"
        chipTitle="Arbitrage"
      />
    </div>
  );
}
