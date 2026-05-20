# UTT — Unified Trading Terminal

UTT (Unified Trading Terminal) is a local-first, multi-venue crypto trading terminal built with **FastAPI** on the backend and **React** on the frontend. It is designed to unify centralized exchange (CEX) workflows and selected decentralized exchange (DEX) flows under a single operator-focused interface.

At a high level, UTT provides one place to:

- connect and manage venue credentials
- inspect balances and portfolio state
- view orderbooks and pseudo-orderbooks
- submit and track orders across venues
- monitor scanners, discovery tools, wallet activity, market-cap data, and volume data
- work with local ledger and tax-related state
- plan cross-chain UTTT movements with bridge-transfer records, canonical supply context, and read-only basis previews
- integrate Solana and Polkadot / Hydration DEX routing and wallet-based execution alongside traditional exchange adapters

---

## What UTT is

UTT is a desktop-style browser application today, but architecturally it functions as a local trading workstation:

- **Backend:** FastAPI application providing venue adapters, market/order routes, auth/profile endpoints, wallet and ledger tooling, Solana DEX routing, and Polkadot / Hydration routing
- **Frontend:** React interface providing a modular multi-window trading terminal UI
- **Storage / local state:** local database and runtime state kept outside of public source control
- **Secrets model:** local or external environment loading for runtime config, plus profile-managed encrypted credential storage for exchange API keys

This repository contains the application code, not live credentials, private keys, or production database state.

---

## Major capabilities

### Centralized exchange workflow

UTT includes exchange adapter and routing layers for multiple venues, with a design centered on a unified terminal experience rather than isolated venue-specific apps.

Current functionality in the codebase includes:

- venue registry and adapter routing
- balances and account views
- order submission plumbing
- order status aggregation
- unified all-orders style data views
- auth, profile, and API-key management flows

### Solana DEX workflow

UTT also includes Solana DEX-specific functionality so on-chain trading can live inside the same interface.

Current architecture in this repository includes support for:

- **Jupiter Metis** swap pathing
- **Jupiter Ultra** order and execution flows
- **Jupiter Trigger** limit-order-related flows
- **Raydium** swap routing
- Solana wallet-aware order ticket behavior
- wallet selection and wallet-manager integration in the frontend
- token resolution, mint lookup, token registry support, and balance helpers

### Polkadot / Hydration DEX workflow

UTT includes a Polkadot / Hydration integration path focused on routing UTTT-HDX activity through the same terminal workflow as CEX and Solana DEX activity.

Current architecture in this repository includes support for:

- **Polkadot-Hydration** venue routing
- Hydration RPC access through profile-managed API keys
- Token Registry-managed Hydration asset metadata
- Token Registry-managed external price metadata
- Hydration Route Registry support for manual/live pool routes
- UTTT-HDX manual XYK orderbook generation from live pool reserves
- Hydration order-ticket execution plumbing for UTTT-HDX
- Hydration balances in terminal tables and wallet-address views
- external USD price enrichment for HDX and DOT
- UTTT/USD derivation from UTTT-HDX live route pricing and HDX/USD
- Hydration price-cache status reporting with clear fresh, stale, partial, and unavailable classifications
- persistent Hydration sidecar quote-cache / singleflight / backoff protection for controlled SDK diagnostics
- Hydration order-ticket execution plumbing for UTTT-HDX manual XYK sell/exact-in and buy/exact-out flows
- generic Hydration SDK router quote and orderbook paths blocked by default unless explicitly enabled
- compact Hydration UI price-status indicators in Order Book and Order Ticket using status-only backend polling
- Hydration swap recording into the unified all-orders flow through `swap_orders`
- Hydration wallet-history ingestion through an optional Subscan-backed provider path
- wallet-address transaction caching before deposit / withdrawal materialization
- Hydration-derived deposit and withdrawal materialization with source provenance
- missing-basis lot creation for detected deposits without inventing USD basis
- transfer-link preview and metadata-only linkage for internal transfer candidates
- explicit-only FIFO withdrawal lot-impact rebuilds with dry-run-first safety
- LP / Omnipool special handling for 2-POOL-style rows that should not be forced through normal FIFO

The public-safe Hydration path avoids generic SDK quote polling for pricing. Broad SDK router quotes are disabled by default while the UTTT-HDX route uses registry-backed manual/live pool metadata. External USD pricing and cache-only status endpoints are the normal price path; the persistent sidecar quote cache is used as a controlled diagnostic/protection layer, not as a UI polling loop.

### Market metrics workflow

UTT includes backend-cached market metrics for operator tool windows and AppHeader summaries.

Current architecture in this repository includes support for:

- AppHeader **Market Cap** and **Volume** tool-tab windows
- selected-asset market cap and volume summaries in the AppHeader tool tabs
- backend-cached CoinGecko market data with TTL, stale-cache behavior, and rate-limit backoff
- DB-owned / tracked asset discovery for market-metrics windows
- Token Registry-managed external price IDs as the preferred market-data resolver
- automatic CoinGecko symbol discovery for explicitly selected assets when no registry mapping exists
- graceful unavailable / placeholder rows for unsupported or unresolved assets

The frontend does not call CoinGecko directly. Market-cap and volume windows read normalized data from the backend market-metrics service, while the backend owns source selection, caching, rate-limit handling, and symbol-to-source resolution.

### Spread / Bridge planning workflow

UTT includes a Spread / Bridge dashboard for planning cross-chain UTTT movement without enabling bridge execution by default.

Current architecture in this repository includes support for:

- AppHeader-launched Spread / Bridge dashboard as a dedicated frontend feature component
- Solana-to-Hydration and Hydration-to-Solana planning context
- UTTT spread display using Solana UTTT/USD and Hydration-derived UTTT/USD
- Total Canonical Supply display for multichain UTTT supply context
- per-chain UTTT supply rows for Solana, Polkadot / Asset Hub, and Hydration route metadata
- Hydration UTTT metadata shown without double-counting Asset Hub-side canonical supply
- bridge transfer-record preview and local PLANNED record creation
- source-side and destination-side evidence linking for bridge-transfer records
- local transfer-record reconciliation without signing, submitting, or executing bridge transactions
- read-only basis / tax-treatment preview for reconciled bridge-transfer records
- read-only apply-basis-transfer preview that models future FIFO movement and inherited destination basis
- actual bridge execution and actual basis mutation intentionally disabled until a real transfer test is ready

The Spread / Bridge dashboard is a planning and accounting-safety surface. It does not execute bridge transfers, sign transactions, submit transactions, mutate deposits or withdrawals, consume FIFO lots, create inherited destination lots, or write lot-journal rows unless a future guarded apply endpoint is explicitly added and confirmed.

### Operator UI / terminal behavior

The frontend is built as a workstation terminal with independently managed panes and specialized windows.

Representative components include:

- `App.jsx` as the primary shell
- `WindowManager.jsx` for pane and window behavior
- `OrderBookWidget.jsx`
- `OrderTicketWidget.jsx`
- `TerminalTablesWidget.jsx`
- `features/bridge/SpreadBridgeDashboardChip.jsx` for Spread / Bridge planning
- feature windows such as token registry and scanner tooling

### Security / operational direction

UTT is intentionally structured so the code can be published while sensitive runtime material stays local.

That includes:

- external env-path loading for runtime configuration
- keeping live backend secrets outside the repo
- avoiding committed database and key files
- using **Profile → API Keys** for venue credentials instead of tracked env files
- storing user-entered venue keys in the app’s local encrypted credential store rather than plaintext repository files

---

## Repository layout

A simplified view of the current repository structure:

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
│   ├── alembic/
│   ├── data/
│   └── tools/
├── frontend/
│   ├── public/
│   └── src/
│       ├── app/
│       ├── components/
│       ├── features/
│       ├── hooks/
│       ├── lib/
│       ├── utils/
│       ├── App.jsx
│       ├── main.jsx
│       ├── OrderBookWidget.jsx
│       ├── OrderTicketWidget.jsx
│       └── TerminalTablesWidget.jsx
├── docs/
│   └── screenshots/
├── scripts/
├── backend.env.example
└── .gitignore
```

### Important directories

#### `backend/app/adapters/`
Venue-specific adapter logic and exchange integration helpers.

#### `backend/app/routers/`
FastAPI routers exposing backend functionality to the frontend and local operator workflows.

#### `backend/app/services/`
Shared service-layer logic such as aggregated order handling.

#### `backend/app/venues/`
Venue registration and integration mapping.

#### `backend/tools/`
Non-secret backend utility tooling. Generated keys, live credentials, and private key material should stay outside the repository.

#### `frontend/src/`
Main frontend application code, widgets, feature windows, hooks, and supporting libraries.

#### `docs/screenshots/`
Repository screenshots used in this README and on the public repo page.

#### `scripts/`
Utility scripts and local development helpers.

---

## Current frontend focus

The UI is built around a multi-pane trading terminal rather than a static page layout.

Current areas of focus include:

- right-lane tile and splitter behavior
- terminal-style window management
- order book and order ticket integration
- table, ledger, and order views
- Solana wallet-manager integration for DEX flows
- registry, scanner, Market Cap, Volume, and Spread / Bridge planning windows
- compact Hydration price-status UI that avoids triggering backend refresh or SDK quote paths
- AppHeader portfolio totals that include cached wallet-address / self-custody balances

In practical terms, the frontend favors:

- task-oriented windows
- local workflow efficiency
- keyboard and mouse hybrid usage
- dense operator information over marketing-style UI

---

## Current backend focus

The backend acts as the local orchestration layer for UTT. It is not just a thin API wrapper.

It is responsible for:

- venue adapter access
- wallet and market routing
- order creation and cancellation support
- unified order views
- token and symbol resolution
- backend-cached market metrics, external market-data resolution, and rate-limit-safe source caching
- wallet-address history ingestion, caching, and materialization workflows
- deposit, withdrawal, missing-basis, transfer-link, and FIFO lot-impact workflows
- bridge transfer-record planning, reconciliation, and read-only basis/tax-treatment preview workflows
- local auth and profile integration
- Solana DEX route construction and transaction preparation
- local environment and secret resolution patterns

The backend is the source of truth for trading-side behavior, while the frontend is the terminal for interacting with it.

---

## Supported and integrated areas in the codebase

The exact state of each venue may evolve over time, but the repository currently includes work across:

- Coinbase
- Crypto.com Exchange
- Dex-Trade
- Gemini
- Kraken
- Robinhood
- Solana DEX flows
  - Jupiter
  - Raydium
- Polkadot / Hydration DEX flows
  - Hydration UTTT-HDX manual/live route
  - Token Registry asset and price metadata
  - Route Registry live pool reserves
  - Hydration price-cache status and external USD pricing
  - persistent sidecar quote-cache diagnostics / backoff protection
  - manual UTTT-HDX orderbook and swap transaction preparation
  - inline Order Book and Order Ticket Hydration price-status UI
  - Hydration wallet-history ingestion and materialization
  - deposit / withdrawal provenance and missing-basis lot workflows
  - transfer-link diagnostics and explicit-only FIFO lot-impact handling
  - Spread / Bridge dashboard planning, canonical UTTT supply context, transfer-record reconciliation, and read-only basis preview

Supporting routes and tooling also include:

- auth and profile flows
- token registry
- wallet address handling
- all-orders aggregation
- scanner and discovery windows
- airdrop-related routing and tooling

---

## Screenshots

### Main Trading Terminal
![UTT Main UI](docs/screenshots/main-ui.png)

### Order Book and Order Ticket
![Order Book and Order Ticket](docs/screenshots/orderbook-orderticket.png)

### Token Registry
![Token Registry](docs/screenshots/token-registry.png)

### Solana DEX Wallet / Order Ticket
![Solana DEX Wallet](docs/screenshots/solana-dex-wallet.png)

### Tables / Balances View
![Tables and Balances](docs/screenshots/tables-balances.png)

### Profile / API Keys
![Profile API Keys](docs/screenshots/profile-api-keys.png)

---

## Quick start

> **Important:** UTT is designed to run with local configuration and local secrets. Do **not** paste real keys into tracked files. Keep runtime secrets outside the repository.

### Prerequisites

Recommended baseline:

- **Python 3.10+**
- **Node.js 18+** and npm
- Windows PowerShell for the Windows-oriented commands below
- a local Solana wallet extension if using Solana DEX features
- a Polkadot/Substrate wallet extension such as SubWallet if using Polkadot / Hydration flows
- venue access and any required API credentials for the venues you plan to test

### 1) Clone the repository

```powershell
git clone https://github.com/eyemaginative/utt-unified-trading-terminal.git
cd utt-unified-trading-terminal
```

### 2) Configure backend environment

The repository uses `backend.env.example` as the public, sanitized backend runtime template. The backend environment is for runtime configuration and local pathing, **not** for storing exchange API keys.

Relevant files:

- `backend.env.example` — safe public template committed to this repo
- `backend/.env` — local stub file that points the backend to your private env path
- `backend/app/config.py` — backend configuration loader

Recommended setup:

```powershell
# Example only; choose your own private location outside the repo.
Copy-Item backend.env.example C:\path\to\utt-secrets\backend.env
```

Then create or update `backend/.env` with only the external env pointer:

```env
UTT_ENV_PATH=C:\path\to\utt-secrets\backend.env
```

The private `backend.env` file lives outside the repo and contains local-only runtime configuration. Exchange API keys and RPC/API keys should be saved through **Profile → API Keys** whenever the app supports that venue.

For Polkadot / Hydration work, keep the real RPC/API key out of the repository. The recommended pattern is to save the Dwellir/Hydration key through **Profile → API Keys** using the Hydration venue key, while the private env keeps only non-secret runtime toggles and templates.

A safe local Hydration configuration uses placeholder/template values such as:

```env
UTT_HYDRATION_RPC_PROVIDER=dwellir
UTT_HYDRATION_RPC_URL_TEMPLATE=https://api-hydration.n.dwellir.com/{api_key}
UTT_HYDRATION_WS_URL_TEMPLATE=wss://api-hydration.n.dwellir.com/{api_key}
UTT_HYDRATION_RPC_URL=

UTT_HYDRATION_ENABLE_ROUTER_QUOTES=0
UTT_HYDRATION_ENABLE_SWAP_TX=1
UTT_HYDRATION_ENABLE_EXACT_BUY=1

UTT_HYDRATION_ENABLE_MANUAL_POOL_FALLBACK=1
UTT_HYDRATION_MANUAL_POOL_LIVE_RESERVES=1

UTT_HYDRATION_ENABLE_EXTERNAL_USD_PRICES=1
UTT_HYDRATION_EXTERNAL_USD_PRICE_SOURCE=coingecko
UTT_HYDRATION_ENABLE_SDK_PRICE_CACHE=1
UTT_HYDRATION_PRICE_CACHE_USE_SIDECAR=0
UTT_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK=0
UTT_HYDRATION_PRICE_CACHE_TTL_S=300
UTT_HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S=600
UTT_HYDRATION_EXTERNAL_USD_PRICE_TIMEOUT_S=5
UTT_HYDRATION_PRICE_CACHE_STRATEGY=spot_then_sell
UTT_HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION=direct

UTT_HYDRATION_USE_SIDECAR=0
UTT_HYDRATION_SIDECAR_URL=http://127.0.0.1:8787
UTT_HYDRATION_AUTOSTART_SIDECAR=0
UTT_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR=0
UTT_HYDRATION_SIDECAR_QUOTE_CACHE=1
UTT_HYDRATION_SIDECAR_QUOTE_CACHE_TTL_MS=30000
UTT_HYDRATION_SIDECAR_QUOTE_CACHE_STALE_TTL_MS=300000
UTT_HYDRATION_SIDECAR_QUOTE_BACKOFF_MS=120000
UTT_HYDRATION_SIDECAR_QUOTE_CACHE_MAX_ENTRIES=100

UTT_HYDRATION_HELPER_STEP_TIMEOUT_S=30
```

The persistent Hydration sidecar is optional and normally only needed for controlled SDK diagnostics. For the public-safe path, keep sidecar autostart disabled and leave sidecar usage off unless you intentionally start:

```powershell
cd backend
$env:UTT_HYDRATION_SIDECAR_HOST="127.0.0.1"
$env:UTT_HYDRATION_SIDECAR_PORT="8787"
node app\services\hydration_sidecar.mjs
```

If sidecar diagnostics are intentionally enabled, set `UTT_HYDRATION_USE_SIDECAR=1` and/or `UTT_HYDRATION_PRICE_CACHE_USE_SIDECAR=1` in your private env for that local test only.

Hydration asset IDs, decimals, external price IDs, and route/pool metadata are intended to be managed through the Token Registry and Route Registry rather than hardcoded into tracked env files.

### 3) Create and activate a backend virtual environment

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 4) Install backend dependencies

If the repo uses `requirements.txt`:

```powershell
pip install -r requirements.txt
```

If the repo uses `pyproject.toml`, install according to that project file instead.

The Hydration helper services also use Node-based backend dependencies. From the `backend` directory, install the backend JS helper dependencies when using Polkadot / Hydration features:

```powershell
npm install
```

These dependencies support helper-side Hydration tooling such as `hydration_quote.mjs` and `hydration_sidecar.mjs`.


### 5) Start the backend

A common local run command is:

```powershell
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### 6) Install frontend dependencies

In a separate terminal:

```powershell
cd frontend
npm install
```

### 7) Configure frontend env

A typical local setting is:

```env
VITE_API_BASE=http://127.0.0.1:8000
```

### 8) Start the frontend

```powershell
npm run dev
```

### 9) Add venue API keys in the app

Venue credentials are added inside the app through the user profile, not by editing tracked env files.

Open:

- **Profile**
- **API Keys**
- add the venue
- enter the required key material for that venue
- save it through the UI

The current codebase uses profile and API-key management flows with local encrypted secret-bundle handling rather than relying on committed backend files.

---

## Installation notes by environment

### Windows

The repository and current operator workflow are heavily Windows-tested and PowerShell-oriented. Windows is the easiest platform to start with.

### Linux / macOS

The backend and frontend stacks are portable in principle, but local pathing, shell scripts, wallet extension workflows, and some venue-specific tooling may need adaptation.

---

## Environment and secrets model

UTT is intentionally structured so that public source code can live in git while live credentials remain local.

### What belongs in the repository

- code
- schema and model definitions
- example env files such as `backend.env.example`
- non-sensitive defaults
- UI assets intended for publication
- utility scripts that do not contain secrets

### What does not belong in the repository

- real API keys
- private keys and PEM files
- local DB files
- production runtime logs
- wallet seed phrases and mnemonics
- local backup data
- locally generated venue key material

### Recommended practice

- keep private env files outside the repo
- use tracked stub files only
- add venue API credentials through **Profile → API Keys**
- scan staged diffs before every push
- keep wallet and account testing material separate from source control

---

## Solana DEX notes

The Solana side of UTT is designed to fit into the same terminal as the CEX workflows rather than being a separate application.

### Current flow areas in the codebase

- wallet-aware order ticket behavior
- Jupiter Metis route handling
- Jupiter Ultra order and execute support
- Jupiter Trigger and limit-related routing
- Raydium swap path construction
- token resolution and token-account-aware routing
- token registry lookups
- balance and wallet helper flows

### Wallet behavior

The frontend integrates wallet selection and wallet-manager behavior for supported Solana wallets. Because wallet and account state matters, two different wallet extensions can behave differently if they are connected to different actual addresses with different balances and token accounts.

A route succeeding in one wallet and failing in another does not necessarily indicate a code bug. It may indicate:

- different wallet address
- different token balances
- missing associated token account for a given mint
- router-specific account requirements

---

## Polkadot / Hydration DEX notes

The Polkadot / Hydration side of UTT is designed as an opt-in DEX venue path. It is intended to coexist with CEX and Solana DEX workflows without enabling broad SDK quote polling by default.

### Current flow areas in the codebase

- `polkadot_hydration` venue selection
- Hydration chain/RPC diagnostics
- Hydration balance retrieval
- Token Registry-based Hydration asset resolution
- Token Registry-based external price metadata
- Route Registry-based UTTT-HDX pool metadata
- live UTTT-HDX reserve lookup
- manual XYK pseudo-orderbook construction
- manual UTTT-HDX order-ticket transaction preparation for sell/exact-in and buy/exact-out flows
- compact Order Book and Order Ticket Hydration price-status UI driven by refresh-free status endpoints
- generic SDK router quotes blocked by default for non-manual pairs
- Hydration order-ticket pre-trade checks and execution path
- Hydration swap recording into `swap_orders`
- All Orders reflection for confirmed Hydration swaps
- price-cache status reporting for cache-only, live refresh, stale, partial, and error-backoff states
- persistent Hydration sidecar quote-cache / singleflight / backoff diagnostics
- Hydration wallet-history ingestion, materialization, missing-basis, transfer-link, and FIFO workflows
- Spread / Bridge dashboard pricing based on Solana UTTT/USD versus Hydration-derived UTTT/USD
- bridge transfer-record preview, local PLANNED record creation, evidence linking, reconciliation, and read-only basis/apply previews

### Token Registry requirements

Hydration assets should be configured through Token Registry rows rather than tracked env JSON. For example:

```text
HDX    hydration native   decimals 12   price source coingecko   price id hydradx
DOT    hydration 5        decimals 10   price source coingecko   price id polkadot
USDT   hydration 10       decimals 6    price source stable      price id stable
UTTT   hydration 1001331  decimals 6    price source derived     price id UTTT-HDX*HDX-USD
```

### Route Registry requirements

The UTTT-HDX route should be configured through the Hydration Route Registry with the route type, fee bps, and live pool account metadata. The intended safe path is:

```text
UTTT-HDX manual/live route
→ live pool reserves
→ manual XYK pseudo-orderbook
→ order ticket execution
→ record_submit
→ swap_orders
→ All Orders
```

### Pricing model

The public-safe Hydration pricing path uses:

```text
HDX/USD     = external price source from Token Registry, normally CoinGecko hydradx
DOT/USD     = external price source from Token Registry, normally CoinGecko polkadot
USDT/USD    = stable
USDC/USD    = stable
HOLLAR/USD  = stable
UTTT/HDX    = UTTT-HDX live route reserve ratio
UTTT/USD    = UTTT/HDX x HDX/USD
```

Hydration pricing endpoints expose cache status explicitly so the UI can distinguish safe polling from live refresh behavior:

```text
GET /api/polkadot_dex/hydration/prices/status
  -> status-only, refresh-free, sidecar-autostart-free, safe for UI polling

GET /api/polkadot_dex/hydration/prices?refresh=false
  -> cache-only response; may be fresh, stale, partial, or unavailable, but does not refresh

GET /api/polkadot_dex/hydration/prices?refresh=true
  -> controlled backend refresh using external USD prices and the UTTT-HDX manual/live route
```

The Order Book and Order Ticket use the status-only endpoint for UI status display. The Order Book keeps Hydration price status inline with the existing depth / auto / route row so switching venues does not resize the widget or push the asks/bids area downward. The Order Ticket shows the same pricing state as a compact `Prices` pill in the Polkadot controls area.

The price response includes `statusDetail` and cache classification fields such as:

```text
cache_only_fresh
cache_only_partial_stale
live_fresh
partial_stale
error_backoff
refresh_failed_stale
```

Generic Hydration SDK router quotes are disabled by default for the public-safe configuration. The persistent `hydration_sidecar.mjs` service includes quote-cache, singleflight, stale-serve, and per-key error-backoff protection for controlled SDK diagnostics, but SDK fallback remains disabled in the normal pricing path.

### Hydration wallet-history and ledger workflow

UTT now includes a Hydration wallet-history ingestion path intended to bring self-custody Hydration activity into the same local ledger workflow as venue and wallet-address activity.

Current Hydration history workflow areas include:

- optional Subscan-backed Hydration history provider support
- safe `provider=none` default behavior when no history provider is configured
- dry-run-first ingestion and coverage diagnostics
- page-windowed backfill controls for deeper history scans
- wallet-address transaction caching before any deposit / withdrawal materialization
- trusted amount parsing with `amount_v2` validation for integer-looking provider amounts
- materialization of cached Hydration wallet transactions into `AssetDeposit` and `AssetWithdrawal` rows
- raw provenance metadata such as provider, source type, source address, transaction hash, and network
- missing-basis lot creation for detected deposits without assigning fake USD basis
- transfer-link preview and metadata-only linking for likely internal transfers
- a withdrawal-before-deposit safety gate for transfer-link candidates
- explicit-only FIFO withdrawal lot-impact rebuilding
- strict default handling for insufficient inventory rather than partial or forced consumption
- opening-balance correction support for known missing historical inventory, with basis remaining unknown
- LP / Omnipool special handling for 2-POOL-style rows so they remain visible but are not forced through normal FIFO

This workflow is intentionally conservative. Normal eligible withdrawals can be applied to lots only through explicit dry-run / apply operations. Transfer-linked withdrawals are skipped by default, and LP / pool-token rows are classified for special handling instead of being consumed through normal inventory logic.

### Spread / Bridge transfer-record workflow

The Spread / Bridge dashboard is intended to prepare cross-chain UTTT movement before any bridge execution path is enabled.

Current safe sequence:

```text
preview transfer record
→ create local PLANNED bridge-transfer record
→ link source-side evidence
→ link destination-side evidence
→ reconcile the local record
→ preview basis / tax treatment
→ preview future apply-basis-transfer plan
```

This workflow is planning-only until the operator is ready for a real bridge test. It classifies source activity as a `TRANSFER_OUT` candidate and destination activity as a `TRANSFER_IN` candidate, but the actual basis-transfer apply endpoint remains intentionally absent. A future apply endpoint should require real linked `AssetWithdrawal` and `AssetDeposit` rows, reviewed basis availability, and explicit confirmation such as `confirm_apply_basis_transfer=true`.

## Auth, profile, and local credential handling

The codebase includes auth, profile, and local credential-management work. In practical terms, that means UTT is intended to be an operator workstation, not just a stateless public dashboard.

Examples of functionality reflected in the current repository include:

- profile and auth routing
- API-key management UI flows
- DB-backed and encrypted secret-bundle patterns in code
- local runtime settings and operator preferences

Venue API keys are added through the **Profile / API Keys** interface and stored in the application’s local credential store rather than being committed to backend files or repository env files.

---

## Token registry and wallet tooling

The repository includes token-registry-related backend and frontend work. This supports:

- symbol and mint resolution
- display-friendly token labeling
- registry-managed token metadata
- registry-managed external price source and price ID metadata
- market-metrics source resolution through Token Registry external price IDs
- Solana token tooling inside the operator UI
- Hydration asset ID, decimals, and external price metadata
- Hydration Route Registry metadata for manual/live UTTT-HDX pool routing

There is also wallet-address handling in the backend, which supports broader local wallet and workflow integration. Cached wallet-address snapshots can contribute to AppHeader portfolio totals, so self-custody balances can be represented without requiring the balances table to be opened first.

Hydration wallet-history ingestion extends this local wallet tooling by caching indexed wallet transactions, materializing them into local deposits and withdrawals, and linking them to the missing-basis, FIFO lot, and bridge transfer-record workflows without requiring live secrets or runtime database files to be committed.

---

## Orderbook and order-ticket model

UTT uses a unified terminal style where the order book, order ticket, tables, scanners, and other panes are all parts of one coordinated workstation.

### Order book

Current work includes:

- venue-aware order book display
- pseudo-orderbook behavior for DEX routes
- manual/live Hydration UTTT-HDX orderbook generation
- inline Hydration price-cache status display that does not create an extra notification row
- router-quote safety gating for generic Hydration SDK pairs
- right-lane terminal tile integration

### Order ticket

Current work includes:

- venue-aware order entry
- Solana wallet-manager integration
- Jupiter and Raydium route selection for DEX paths
- Hydration manual UTTT-HDX sell/exact-in and buy/exact-out transaction preparation
- compact Hydration `Prices` status pill using refresh-free backend status polling
- blocked generic Hydration swap-tx path when router quotes are disabled
- operator status and preflight behavior
- simplified widget controls with redundant Lock buttons and the Order Ticket top-left resize handle removed

---

## Data and runtime state

You may see empty tracked directories such as `backend/data/` that exist only to preserve folder structure. Non-secret backend helper scripts may live under `backend/tools/`.

That does not mean the repository is intended to contain live runtime data.

In general:

- keep runtime DB files out of source control
- keep generated key material out of source control
- keep tool scripts under `backend/tools/` only when they contain no secrets or generated private material
- keep local backups out of source control
- use `.gitignore` and external paths appropriately

---

## Troubleshooting

### Frontend starts but cannot reach backend

Check:

- backend is running
- `VITE_API_BASE` points to the correct backend URL
- backend host and port are reachable from the frontend

### Backend starts but venue requests fail

Check:

- local runtime env path is correct
- the venue API key was actually added and saved in **Profile → API Keys**
- the correct venue was configured in the profile
- no real credentials were placed into tracked files

### Solana wallet connects but a trade fails

Check:

- which wallet address is actually connected
- whether that address has the required input token balance
- whether that address has the required token account for the mint being used
- which router is selected (Metis, Ultra, or Raydium)

### Hydration balances or UTTT-HDX orderbook do not load

Check:

- the Dwellir/Hydration key is saved through **Profile → API Keys**
- Hydration Token Registry rows exist for HDX, DOT, USDT, and UTTT
- HDX and DOT have valid external price IDs
- the UTTT-HDX Route Registry row has live pool-account metadata
- broad router quotes are disabled unless intentionally debugging SDK behavior
- backend logs are not showing generic Hydration orderbook calls for pricing pairs such as `HDX-USDT`, `DOT-USDT`, or `UTTT-USDT`

The normal safe-path pricing flow should use `/api/polkadot_dex/hydration/prices`, `/api/polkadot_dex/hydration/prices/status`, and the UTTT-HDX manual/live route, not generic Hydration orderbook requests for USD pricing.

### Hydration price cache or status looks stale

The persistent sidecar does not need to stay open for normal UI polling. Keep it running only while intentionally testing protected SDK diagnostics or a private env configuration that explicitly enables sidecar usage.

Check:

- `/api/polkadot_dex/hydration/prices/status` is reachable and reports `safe_for_ui_polling=true`
- Order Book / Order Ticket status indicators are using the status endpoint rather than triggering `refresh=true`
- `/api/polkadot_dex/hydration/prices?refresh=false` is being used for cache-only reads
- `/api/polkadot_dex/hydration/prices?refresh=true` is used only for controlled backend refreshes
- `statusDetail.classification` is inspected before treating missing prices as failures
- SDK fallback remains disabled unless intentionally testing the protected sidecar path
- `UTT_HYDRATION_AUTOSTART_SIDECAR=0` and `UTT_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR=0` are used for public-safe operation

Expected safe states include:

```text
status_only
cache_only_fresh
cache_only_partial_stale
live_fresh
partial_stale
error_backoff
refresh_failed_stale
```

### Hydration generic orderbook or swap-tx requests are blocked

This is usually expected. Generic SDK-routed pairs such as `DOT-USDT` are blocked by default when router quotes are disabled. The expected protective response includes:

```text
hydration_router_quotes_disabled
hydration_swap_tx_requires_router_quotes
quoteStatus.status = disabled
```

Manual/live routes such as `UTTT-HDX` can still build pseudo-orderbooks and unsigned transaction payloads through the manual XYK route. That path supports sell/exact-in and buy/exact-out transaction preparation without reopening generic SDK router quote polling.

### Hydration wallet-history rows do not appear in deposits or withdrawals

Check:

- the Hydration history provider is configured intentionally, such as a Subscan API key saved through **Profile → API Keys** or a private env path
- `/api/hydration_wallet_history/status` reports the expected provider and key availability
- ingestion is first run with `dry_run=true`
- page-windowed coverage diagnostics show expected assets before cache/materialization is applied
- cached wallet-address transaction rows exist before materialization
- materialization is run separately from the provider fetch
- deposit rows that lack basis are rebuilt into missing-basis lots before withdrawal FIFO impact is applied

The safe sequence is:

```text
coverage dry run
→ cache trusted wallet tx rows
→ preview materialization
→ apply materialization
→ rebuild missing-basis lots
→ preview withdrawal lot impact
→ apply only clean assets explicitly
```

### Hydration withdrawal FIFO shows insufficient inventory

Check:

- whether deposits exist for the same venue, wallet, and asset
- whether missing-basis lots have been created for detected deposits
- whether withdrawals predate available deposit lots
- whether the asset needs an opening-balance correction for known historical inventory
- whether the row is transfer-linked and intentionally skipped
- whether the row is an LP / Omnipool-style asset such as 2-POOL and should be held for special handling

The FIFO rebuild path is explicit-only. Do not use `allow_partial=true`, `force_rebuild=true`, or broad all-asset application unless you are intentionally performing a controlled ledger repair.

### Market Cap or Volume windows show unavailable data

Check:

- backend is running and `/api/market_metrics/summary` is reachable
- the asset exists in local balance / wallet / registry state, or is explicitly selected in the UI
- Token Registry rows use `external_price_source` and `external_price_id` where deterministic mapping is needed
- CoinGecko rate limits have not forced the backend into temporary backoff
- backend cache files under `backend/data/` are local runtime data and should not be committed

The normal path is:

```text
DB-owned/tracked asset
→ Token Registry external price ID if configured
→ env override if configured
→ automatic CoinGecko symbol search for explicitly selected assets
→ small hardcoded fallback map for bootstrap assets
```

### UI layout looks wrong

The terminal UI uses pane and window logic with multiple specialized widgets. Layout issues are usually related to dependencies, recent layout changes, or stale frontend state after major UI updates.

For Hydration specifically, the Order Book price-cache indicator is intentionally inline with the existing depth / auto / route status row. It should not add a standalone notification row or change widget height when switching between CEX venues and `polkadot_hydration`.

The Order Book and Order Ticket no longer depend on visible Lock controls for normal operation. If old local UI state behaves oddly after upgrading, clear local widget state or reload the app so the current unlocked widget behavior is applied.

---

## Security notes

This project interacts with trading infrastructure and wallet and account workflows. Treat it accordingly.

### Recommended operator posture

- use local-only secrets
- review staged diffs before every push
- use separate accounts and wallets for testing
- avoid storing sensitive values in plaintext inside the repo
- keep local DB, backup, and key files outside version control

### Important disclaimer

This software is provided for operator workflows and development or testing purposes. Use it at your own risk. Nothing in this repository should be treated as financial advice, investment advice, or a guarantee of trading outcomes.

---

## Development philosophy

UTT is being developed as a practical operator terminal with an emphasis on:

- local-first workflows
- unified venue handling
- terminal-style density and control
- security-conscious secret handling
- incremental, surgical changes instead of destructive rewrites

---

## Contributing

Contributions are best when they are scoped, testable, and operationally safe.

The current contribution model is straightforward:

1. open an issue describing the change
2. discuss scope before major architectural changes
3. avoid committing secrets, runtime data, or local credential material
4. keep changes surgical and easy to review

---

## License

This project is licensed under the **MIT License**.

See the top-level [LICENSE](LICENSE) file for the full license text.

---

## Status

UTT is an actively evolving trading terminal codebase with ongoing work across:

- UI and layout refinement
- Solana wallet and router integration
- Polkadot / Hydration UTTT-HDX routing
- Hydration price-cache status, external USD pricing, and UTTT/USD derivation
- Hydration sidecar quote-cache / singleflight / backoff safety
- manual UTTT-HDX orderbook and buy/sell transaction preparation
- compact Hydration UI status indicators for Order Book and Order Ticket
- Hydration wallet-history ingestion and ledger materialization
- Spread / Bridge transfer-record planning, canonical supply context, and read-only basis/apply previews
- missing-basis lots, transfer-link previews, and explicit-only FIFO lot impact
- LP / Omnipool special handling for pool-token activity
- registry, scanner, Market Cap, and Volume tool windows
- auth, profile, and API-key handling
- venue adapter coverage
- wallet-address and self-custody portfolio visibility
- unified order and ledger workflows

Expect active iteration rather than a frozen, final product.
