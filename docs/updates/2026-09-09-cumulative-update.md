# UTT — September 9, 2026 cumulative update

This checkpoint publishes the accepted UTT work completed after GitHub baseline `ecd735228ef57f85f4312a2908a5ca1e16f6438f`.


## Robinhood Chain Profile and startup persistence

- Robinhood Chain discovery and interactive quote limits are persisted in the authenticated Profile preference store rather than remaining fixed application ceilings.
- Persisted values survive backend restarts and remain separate from execution authorization.
- The Tables startup tab is database-backed through `ui.tables_startup_tab`, with supported All Orders, Balances, Local Orders, and Discover values.
- Startup rendering waits for the authenticated preference to resolve, avoiding an unintended transient balance load.
- The focused startup-preference surface contributes five accepted tests.

## Robinhood Chain catalog / SQLite writer-scope hardening

- Registry-discovery provider work was moved out of the repaired SQLite writer scope.
- Failed registration attempts retain clean rollback behavior.
- Final POOLS-USDG and VOLT-USDG selected-market refresh witnesses completed without the prior lock failure.
- Required background Ledger/FIFO synchronization remains enabled.

## Robinhood Chain order-book reliability

- Provider-backed synthetic books now use a bounded provider ladder rather than mixing bid/ask sides across providers.
- Structural failures remain fail-closed: exact-pair identity mismatch, unavailable direction, crossed market after the bounded recovery path, and required Registry identity failures are not papered over with unsafe fallback data.
- A first crossed Uniswap synthetic snapshot is discarded as a whole and may receive exactly one fresh complete paired resample. The first crossed snapshot is never exposed or mixed with the second snapshot.
- Accepted live stability work completed six normal-cadence BUY-USDG observations without an unsafe crossed book, followed by clean BUCKET-USDG, BACKED-USDG, and INDEX-USDG spot observations.
- Low-priced Robinhood Chain books now have adaptive frontend price precision. Normal books remain compact; when distinct visible raw levels would round to the same price, display precision increases only as far as needed, bounded at 12 decimals. Book-row click precision rises with the display precision so a visually distinct row does not collapse back to the same Ticket price.
- The VOLT-USDG root was proven with distinct raw synthetic levels: 8 decimals collided on both sides, 9 decimals still collided on bids, and 10 decimals separated the captured levels. The adaptive repair is installed and production-build green; the final naturally occurring multi-level UI/click witness remains a follow-up after this publication checkpoint.

## Robinhood Chain Order Ticket and wallet lifecycle

- Synthetic Book row metadata now carries the top-level Robinhood Chain `synthetic` / `quote_only` context into the Book-to-Ticket event path, preserving lane consistency and the accepted Ask → BUY / Bid → SELL mapping.
- A selected ask can bind BUY + Exact Spend without requiring the previous highest-bid workaround.
- Finite-approval receipt handling now has bounded automatic receipt polling: an initial delay followed by a bounded polling interval/attempt count. Manual receipt refresh remains available.
- The watcher is read-only with respect to wallet authority: it does not prepare a swap, request another wallet transaction, sign, broadcast, or open an automatic second transaction.
- Confirmed swap handling refreshes the relevant Robinhood Chain balance automatically; the manual balance Refresh remains available.
- A stale test assumption based on the global count of `automatic: true` was replaced with semantic receipt-watcher safety assertions. The canonical backend regression returned to `494 passed` plus `40 subtests`, with only the existing FastAPI `on_event` deprecation warning.
- The successful finite-approval automatic-receipt live witness remains a natural-future canary. No approval transaction should be manufactured solely for validation.

## Counterparty / XCP valuation repair

- XCP direct USD pricing now retains exact Registry identity (`coingecko:counterparty`) and can recover a missing exact ID with a bounded CoinGecko simple-price lookup when the normal markets request succeeds but omits that requested ID.
- Counterparty Book-derived accounting prices no longer accept a raw best bid or ask as fair value by itself.
- Stale, rate-limited, one-sided, crossed, non-finite, non-positive, or excessively wide Book evidence fails closed for accounting valuation.
- A valid derived fallback, when needed, requires a fresh coherent two-sided Book and uses a validated midpoint subject to the spread guard.
- Live acceptance recovered XCP around `5.1080063621 USD` from `coingecko:counterparty` while the factual XCP-BTC Book remained crossed; that crossed Book no longer controlled portfolio `px_usd`.

## Balance identity UX

- Robinhood Chain balance identity details moved from a browser-native disappearing `title` tooltip into the terminal's existing interactive balance overlib.
- Contract text is selectable and includes an explicit Copy contract action.
- The panel remains open while the pointer moves from the asset cell into the panel using the existing bounded hover-close mechanics.
- All-Venues grouped Robinhood Chain children use the same identity surface.
- Generic balance identity handling also recognizes supported `mint` fields, extending copyable mint behavior to Solana balance surfaces where that identity is present.

## Validation and safety state

- Canonical backend regression: `494 passed`, `40 subtests passed`, `0 failed`; one known FastAPI `on_event` deprecation warning remains.
- Current frontend production builds exit successfully; existing Vite warnings about mixed dynamic/static Solana imports and the large application chunk are non-fatal and remain visible rather than being suppressed.
- Multiple Robinhood Chain swaps have completed successfully during the current accepted operator workflow.
- This publication does not relax saved-wallet matching, exact Registry identity, finite approvals, slippage/minimum-output checks, provider-concurrency limits, browser-wallet confirmation, backend no-sign/no-broadcast rules, or the prohibition on automatic second transactions.

## Explicit follow-up after this checkpoint

- Complete the naturally occurring multi-level adaptive Book display/click witness on VOLT-USDG or another affected low-price Robinhood Chain market.
- Complete the next naturally required successful finite-approval receipt-auto-advance witness without forcing an approval.
- Resume the remaining engineering roadmap: legacy Ticket-surface cleanup, balance-snapshot performance, Registry price-metadata safety, balance-link closure, RPC redundancy, route-state synchronization, wallet-simulation divergence, USDG-ETH follow-up, cold-start work, auth hot-path work, balance Registry linkage, and execution discovery.
