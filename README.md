# UTT — Unified Trading Terminal

UTT (Unified Trading Terminal) is a local-first, multi-venue crypto trading workstation built with **FastAPI** and **React**. It unifies centralized-exchange workflows, selected DEX and browser-wallet execution paths, market-data tools, self-custody visibility, local ledger/basis context, and operator diagnostics in one dense terminal interface.

> **Documentation state — September 9, 2026:** this README describes the cumulative state published after GitHub baseline `ecd735228ef57f85f4312a2908a5ca1e16f6438f`. This checkpoint includes the accepted Robinhood Chain Profile-limit persistence, Tables startup preference, catalog/SQLite writer-scope hardening, Book reliability/fallback work, Book→Ticket exact-spend binding, bounded approval-receipt polling, automatic post-confirmation balance refresh, Counterparty/XCP valuation repair, interactive balance contract/mint copy UX, and the installed/static-green adaptive low-price Robinhood Chain Book precision repair. The adaptive multi-level Book click/display witness and the successful finite-approval auto-advance witness remain explicit follow-up canaries; neither is being forced merely to complete publication.

This README is the current operator-oriented summary for the September 9 publication checkpoint.

## September 9, 2026 cumulative update


### Robinhood Chain Profile limits and persisted operator preferences

Robinhood Chain quote/discovery limits are no longer treated as immutable hard-coded economic ceilings. The accepted Profile work moved those operator limits into the existing authenticated preference store:

- discovery and interactive quote limits are persisted per authenticated Profile subject;
- saved values survive backend restart and are read back from the database;
- historical capability/probe state does not silently become the final authority for a new current Ticket amount;
- quote, wallet, approval, signing, and broadcast safety boundaries remain separate from Profile persistence.

This preserves bounded operator control without converting a Profile preference into transaction authorization.

### Tables startup preference

The terminal's startup Tables tab is now an authenticated database-backed preference rather than an unconditional balance-tab startup:

- `ui.tables_startup_tab` is stored in the existing `utt_user_preferences` surface;
- supported values are All Orders, Balances, Local Orders, and Discover;
- the canonical auth-token key is used for preference resolution;
- the Tables surface is held in a neutral loading state until the saved preference resolves, preventing a transient unintended balance load before the selected startup tab is known;
- saving the startup/layout preference is section-scoped and does not overwrite Robinhood Chain Profile limits.

The focused startup-preference regression surface contributes five accepted tests.

### Robinhood Chain catalog / SQLite writer-scope hardening

The accepted catalog-lock campaign narrowed Registry-discovery database writer scope around provider-backed refresh work:

- provider/network work is not intentionally held inside the repaired Registry-discovery SQLite writer scope;
- failed registration attempts preserve rollback semantics rather than leaving partial objectives;
- final POOLS-USDG and VOLT-USDG selected-market refresh witnesses completed without the prior SQLite lock failure;
- background Ledger/FIFO synchronization remains enabled; the repair did not disable required accounting refresh to hide contention.

This does not claim SQLite can never contend. It records the accepted writer-scope repair and its final live closure.

### Robinhood Chain Book routing and reliability

- Synthetic Order Books use bounded provider selection/fallback and never mix bid and ask sides from different provider snapshots.
- Structural integrity failures remain fail-closed. Exact-pair mismatch, unavailable direction, required Registry identity mismatch, and a still-crossed market after the bounded recovery path are not converted into fabricated liquidity.
- A first crossed Uniswap synthetic snapshot is discarded as a whole. The service may take exactly one fresh complete paired resample; the first crossed snapshot is never exposed or combined with the second.
- Accepted live stability work completed six normal-cadence BUY-USDG observations without an unsafe crossed Book. Post-repair BUCKET-USDG, BACKED-USDG, and INDEX-USDG observations were also coherent.
- The Book keeps the factual provider state visible while transaction authority remains a separate concern.

### Robinhood Chain Book precision

The low-price `VOLT-USDG` Book exposed a frontend precision defect rather than a provider or backend price defect. Distinct raw synthetic prices collapsed under fixed frontend formatting:

```text
8 decimals  -> asks collide, bids collide
9 decimals  -> asks separate, bids still collide
10 decimals -> captured asks and bids separate
```

The installed repair is generic across Robinhood Chain markets:

- normal Books remain compact at the usual precision;
- visible asks and bids are examined together;
- if distinct raw prices would render identically, precision increases one digit at a time;
- the first collision-free precision is used consistently across the visible Book;
- adaptive precision is bounded at 12 decimals;
- Book-row click precision is raised to at least the selected display precision so visibly distinct rows do not collapse back into the same Ticket reference value;
- true raw duplicate prices remain true duplicates; UTT does not manufacture separation or sum synthetic samples as exchange liquidity.

The source/static/build contract is accepted. A naturally occurring multi-level live display/click witness remains follow-up work after this publication checkpoint.

### Robinhood Chain Book → Ticket binding

- Robinhood Chain synthetic/quote-only metadata is carried from the Book response into the row-pick event path.
- Side/lane consistency is validated at the Ticket bridge.
- Ask → BUY and Bid → SELL remain the accepted orientation.
- BUY rows bind into **Exact Spend** semantics without requiring the previous highest-bid workaround.
- The Book-pick path preserves exact current market identity instead of relying on ticker-only heuristics.

### Finite approvals and receipt handling

- ERC-20 approvals remain finite; unlimited approval is disabled.
- Approval and swap are separate wallet requests.
- After an explicit approval submission, a bounded read-only receipt watcher can poll for confirmation using the accepted delay/interval/attempt limits.
- Manual **Refresh Approval Receipt** remains available.
- The watcher cannot prepare a swap, request another wallet transaction, sign, broadcast, or open an automatic second transaction.
- A confirmed swap refreshes the relevant Robinhood Chain balance automatically; manual balance refresh remains available.
- A stale regression assertion based on the global count of `automatic: true` was replaced with semantic receipt-watcher safety checks.

The next naturally required successful finite approval will provide the remaining live auto-advance canary. UTT does not revoke allowance or manufacture an approval only to satisfy that witness.

### Counterparty / XCP valuation repair

XCP valuation is now fail-closed against pathological market evidence while retaining an exact direct-price path:

- XCP Registry identity remains explicit as `coingecko:counterparty`.
- If the normal CoinGecko markets response succeeds but omits the exact requested ID, Market Metrics can perform one bounded exact-ID simple-price lookup for the missing item.
- The secondary price lookup is not used as an uncontrolled retry after provider/transport failure.
- Raw best bid or best ask alone is no longer accounting fair value.
- Stale, rate-limited, one-sided, crossed, non-finite, non-positive, or excessively wide ASSET-BTC evidence is rejected for accounting valuation.
- A derived fallback, when valid, requires a fresh coherent two-sided Book and uses a validated midpoint subject to the configured spread guard.

Live acceptance recovered XCP at approximately `5.1080063621 USD` from `coingecko:counterparty` while the factual XCP-BTC Book remained crossed. The crossed Book stayed visible as market evidence but did not control `px_usd`.

### Balance contract and mint identity UX

Balance identity details are now usable rather than disappearing as a native browser tooltip:

- Robinhood Chain contract details are shown in the terminal's interactive balance overlib;
- the panel remains open when the pointer moves from the asset cell into the panel;
- contract text is selectable;
- **Copy contract** copies the exact address;
- All-Venues grouped Robinhood Chain child rows use the same identity surface;
- generic balance identity handling also recognizes supported `mint` fields, extending copyable mint behavior to Solana balance surfaces where a mint is available.

No provider, RPC, signing, wallet, or backend mutation authority was added for this UX change.

### Validation state at publication

- Canonical backend regression: **494 passed**, **40 subtests passed**, **0 failed**.
- One existing FastAPI `on_event` deprecation warning remains visible.
- Current production frontend builds exit successfully.
- Existing Vite warnings about mixed dynamic/static Solana imports and the large application chunk remain non-fatal and are not suppressed.
- Multiple Robinhood Chain swaps completed successfully in the current operator workflow.
- The latest adaptive Book precision repair is installed and production-build green; its final naturally occurring multi-level display/click witness remains pending as described above.

A machine-readable publication manifest is in [`docs/PUBLICATION_MANIFEST_2026-09-09.json`](docs/PUBLICATION_MANIFEST_2026-09-09.json). Release notes and the BBCode announcement are in [`docs/updates/`](docs/updates/).

---

## Core safety model

UTT is deliberately review-first and fail-closed around transaction authority.

### General

- UI visibility is not transaction authority.
- Passive reads do not imply permission to trade.
- Local ledger/FIFO mutation remains separate from normal balance/market-data reads.
- Runtime secrets, databases, backups, wallet seeds, private keys, API secrets, and signed transaction material do not belong in source control.

### Robinhood Chain

- Token and pair identity are Registry-backed rather than ticker-only.
- MetaMask/browser-wallet signing is initiated only by explicit operator action.
- The backend does not hold a wallet private key, sign transactions, or broadcast wallet transactions.
- Unlimited approval is disabled.
- Approval and swap are separate transactions when an approval is required.
- No automatic retry or automatic second wallet transaction is permitted.
- Provider plans are short-lived and bound to wallet, pair, direction, amount, destination, calldata, and current authority.
- Quoteability, Registry presence, or synthetic Book visibility does not automatically grant execution authority.

### Counterparty / UniSat

- The backend can read public Counterparty data and build unsigned compose previews.
- UniSat remains the explicit browser-wallet signing surface.
- PSBT signing and broadcast are separate operator actions.
- Automatic broadcast is disabled.
- The backend never holds the Bitcoin signing key.

---

## Capability overview

UTT currently spans these broad areas:

### Trading and execution

- CEX order-ticket routing and supported cancellation through integrated venue adapters.
- Robinhood Chain exact-input review and controlled browser-wallet execution.
- Counterparty dispenser/protocol-order compose review with explicit UniSat handoff.
- Solana Jupiter/Raydium wallet-aware routing.
- Polkadot/Hydration manual XYK and confirmed manual Router flows.

### Market data and routing context

- CEX Order Books where adapters expose them.
- DEX pseudo/synthetic books for route and quote context.
- Counterparty dispenser/protocol-order Book context.
- Robinhood Chain provider-backed synthetic Books with bounded fallback and structural fail-closed behavior.
- CoinGecko-backed Market Cap and Volume windows through backend caching.
- Managed read-only Arbitrage snapshots.

### Portfolio and balances

- Total / Available / Hold balance semantics where venue data permits.
- USD pricing enrichment and total portfolio context.
- Cost basis, average cost, basis status, lot drilldown, and gain fields where evidence exists.
- Self-custody Wallet Address snapshots and All-Venues aggregation.
- Interactive contract/mint identity copy in supported balance surfaces.

### Orders, lifecycle, and accounting context

- Local + venue order normalization in All Orders.
- DEX swap lifecycle reflection where supported.
- Receipt reconciliation and exact-once ownership for Robinhood Chain UTT vs external wallet activity.
- Deposits, withdrawals, missing-basis lots, transfer-link previews, FIFO lot-impact preview/apply workflows, and bridge-transfer planning.

---

## Venue and source coverage

Runtime configuration, credentials, Registry state, adapter capability flags, and backend safety gates remain authoritative if a local environment differs from this table.

| Venue / source | Canonical key | Type | Core UTT coverage |
|---|---|---|---|
| Coinbase | `coinbase` | CEX | markets, Book, balances, unified orders, live-gated ticket |
| Crypto.com Exchange | `cryptocom` | CEX | instruments, Book, balances, unified orders, live-gated ticket |
| Cexius | `cexius` | CEX | public market data, normalized balances, complete order snapshots, limit BUY/SELL, selected cancel |
| Dex-Trade | `dex_trade` | CEX | market/Book/balance/order workflow, live-gated where configured |
| Gemini | `gemini` | CEX | market/Book/balance/order workflow, live-gated where configured |
| Kraken | `kraken` | CEX | market/Book/balance/order workflow, live-gated where configured |
| OKX | `okx` | CEX | instruments, Book, rules, balances, order/fill diagnostics, live-gated submit/cancel |
| Robinhood Crypto | `robinhood` | brokerage / CEX-style | balances, pricing/order context, unified orders; separate custody from Robinhood Chain |
| Robinhood Chain | `robinhood_chain` | EVM DEX / chain | Registry identity, balances, synthetic Books, controlled browser-wallet execution, receipt reconciliation, wallet-history materialization |
| Counterparty / Bitcoin | `counterparty` | Bitcoin metaprotocol | balances, dispensers/orders, compose review, UniSat sign/broadcast handoff, accounting previews |
| Solana-Jupiter | `solana_jupiter` | Solana DEX | canonical user-facing Jupiter/Solana execution venue |
| Solana legacy | `solana_dex` | compatibility | historical/API compatibility; not the canonical user-facing venue |
| Raydium | under Solana flow | Solana route | route/provider context inside Solana workflow |
| Polkadot / Hydration | `polkadot_hydration` | Substrate DEX | manual/live UTTT-HDX, confirmed Router routes, balances, wallet-history/ledger tooling |
| Self-custody Wallet Addresses | `self_custody` grouping | custody/portfolio | cached on-chain balance/history evidence; not a generic trading venue |
| Derived/global metrics | global/Derived | market data | CoinGecko and derived market context; no trading authority |

Important distinctions:

- `robinhood` and `robinhood_chain` are separate venues and custody/accounting scopes.
- `solana_jupiter` is the canonical user-facing Solana execution venue; `solana_dex` remains compatibility infrastructure.
- All Venues is an aggregate portfolio mode, not a tradable venue.
- Self-custody is a custody/portfolio source, not execution authority.

---

## Robinhood Chain operator flow

A normal controlled workflow is:

```text
select robinhood_chain
→ select an exact Registry-backed pair
→ inspect synthetic Book / provider context
→ choose a Book row or enter an exact amount
→ review unsigned plan
→ if required, review and explicitly submit one finite approval
→ wait for confirmed allowance / receipt state
→ build a fresh post-approval plan
→ explicitly submit one separate swap request
→ reconcile receipt
→ refresh lifecycle / All Orders / focused balances
```

Key properties:

- Ask → BUY and Bid → SELL are preserved by the Book/Ticket bridge.
- BUY Book picks use Exact Spend semantics.
- Automatic approval-receipt polling is read-only and bounded.
- Confirmed swap balance refresh is automatic for the focused identity set.
- No approval confirmation automatically opens the swap wallet request.

Robinhood Chain USD balance pricing remains read-only and Registry-oriented. Registered assets can use stable mappings, explicit CoinGecko IDs, or exact registered quote context. Unsupported identities remain unpriced rather than receiving guessed ticker matches.

---

## Counterparty / UniSat operator flow

Typical safe workflow:

```text
configure Counterparty source address in Wallet Addresses
→ inspect BTC / protocol-asset balances
→ inspect dispenser or protocol-order context
→ build unsigned compose preview
→ verify address, quantity, BTC consideration, fee and funding evidence
→ explicitly request UniSat signPsbt
→ review signed-but-not-broadcast state
→ separately confirm pushPsbt only when live broadcast is intentionally enabled
→ verify transaction / All Orders / accounting preview
```

XCP and other Counterparty accounting values must fail closed when usable direct or coherent derived price evidence is unavailable.

---

## Solana and Hydration

### Solana

The current Solana workflow includes Jupiter Metis/Ultra/Trigger-related routing, Raydium route context, Registry/mint resolution, wallet-aware balances and Order Ticket behavior. `solana_jupiter` is the canonical user-facing venue.

### Polkadot / Hydration

Hydration support includes:

- Token Registry asset metadata;
- Route Registry manual/live pool metadata;
- UTTT-HDX manual XYK context;
- confirmed manual Router paths such as DOT-HDX / HDX-DOT where configured and accepted;
- external USD pricing and cache/status reporting;
- wallet-history ingestion, deposits/withdrawals, missing-basis and explicit FIFO workflows;
- Spread / Bridge planning with canonical-supply and read-only basis previews.

Generic SDK route polling remains disabled by default for the public-safe path unless intentionally enabled for diagnostics.

---

## Local configuration and secrets

UTT is designed so the public repository contains application code, not live operational secrets.

Recommended pattern:

```text
tracked repo
  code
  safe example env
  public assets

outside repo
  private backend.env
  runtime database
  backups
  API/RPC secrets
  wallet/private-key material
```

Use **Profile → API Keys** for supported venue/provider credentials. The credential vault requires separate persistent encryption/authentication secrets and explicit local owner binding. Do not commit those values.

A typical frontend local API setting is:

```env
VITE_API_BASE=http://127.0.0.1:8000
```

A typical backend launch from the backend directory is:

```powershell
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

---

## Quick start

```powershell
git clone https://github.com/eyemaginative/utt-unified-trading-terminal.git
cd utt-unified-trading-terminal

cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# separate terminal
cd frontend
npm install
npm run dev
```

Configure private runtime paths and credentials before starting live-gated venue workflows. A visible trading widget alone does not authorize a live transaction.

---

## Repository layout

```text
.
├── backend/
│   ├── app/
│   │   ├── adapters/
│   │   ├── routers/
│   │   ├── services/
│   │   ├── venues/
│   │   ├── config.py
│   │   ├── main.py
│   │   ├── models.py
│   │   └── schemas.py
│   └── tests/
├── frontend/
│   └── src/
│       ├── features/
│       ├── lib/
│       ├── App.jsx
│       ├── OrderBookWidget.jsx
│       ├── OrderTicketWidget.jsx
│       └── TerminalTablesWidget.jsx
├── docs/
│   ├── updates/
│   └── screenshots/
├── scripts/
├── backend.env.example
└── README.md
```

---

## Current explicit follow-up

This publication intentionally does **not** claim two outstanding natural/live witnesses as closed:

1. **Adaptive low-price Robinhood Chain Book witness** — the installed/static-green precision repair still needs a naturally occurring multi-level UI/click witness on VOLT-USDG or another affected market.
2. **Successful finite-approval auto-advance canary** — the next naturally required finite approval should confirm automatic receipt-state advance without an automatic second MetaMask request. Do not force an approval only for this test.

After this publication checkpoint, the engineering roadmap resumes with legacy Ticket-surface cleanup, balance-snapshot performance, Registry price-metadata safety, balance-link closure, RPC redundancy, route-state synchronization, wallet-simulation divergence, USDG-ETH follow-up, cold-start work, auth hot-path work, balance Registry linkage, and execution discovery.

---

## Security and disclaimer

This project interacts with trading infrastructure and browser wallets. Use separate test accounts/wallets where appropriate, keep secrets outside source control, review transaction details in the wallet, and review staged diffs before publication.

This software is provided for development/operator workflows. Use it at your own risk. Nothing in this repository is financial or investment advice, and no trading outcome is guaranteed.

## License

MIT. See [`LICENSE`](LICENSE).
