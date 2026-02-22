# Build and Use Guide

> Replace placeholders with your real stack details once application code is added.

## Prerequisites

- Git
- Runtime(s): e.g., Node.js/Python/Rust/Go
- Package manager(s)

## Clone

```bash
git clone <YOUR_GITHUB_REPO_URL>
cd utt-unified-trading-terminal
```

## Configure environment

Create local environment file:

```bash
cp .env.example .env
```

Open `.env` and fill only your local values (never commit this file).

Minimum values to review:

- `UTT_TRADING_MODE` (`paper` recommended initially)
- `UTT_ENABLE_LIVE_TRADING=false`
- `UTT_BROKER_API_KEY` / `UTT_BROKER_API_SECRET`
- `UTT_DATABASE_URL`

## Install dependencies

Add your actual command(s), for example:

```bash
# npm install
# pip install -r requirements.txt
# cargo build
```

## Run locally

Add your real startup command(s), for example:

```bash
# npm run dev
# python app.py
# cargo run
```

## Basic usage flow

1. Start UTT services
2. Log in locally
3. Connect paper trading account
4. Load market data view
5. Place test order in simulation mode

## Safety recommendations

- Start in simulation mode
- Set conservative risk limits
- Log all order actions for auditing

## .env safety rules

- Keep `.env` local only (already ignored by `.gitignore`).
- Commit `.env.example` with placeholders only.
- If a secret is leaked, rotate it immediately.
