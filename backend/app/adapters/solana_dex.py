from __future__ import annotations

"""Back-compat shim for the Solana on-chain adapter.

Why this file exists:
- Your router is `routers/solana_dex.py`
- Having an adapter module with the same filename (`adapters/solana_dex.py`) is confusing.
- We canonicalize the implementation to `adapters/solana_onchain.py`, but keep this import path
  working so existing registry/imports do not break.
"""

from .solana_onchain import SolanaDexAdapter  # noqa: F401
