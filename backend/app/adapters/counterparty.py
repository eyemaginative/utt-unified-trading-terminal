from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx

from ..config import settings


class CounterpartyAdapter:
    """Read-only Counterparty / Bitcoin metaprotocol adapter.

    This adapter deliberately does not sign, compose, or broadcast transactions.
    Wallet operations should remain browser/wallet-mediated (UniSat now; other
    Bitcoin wallets later) until an explicit unsigned transaction compose tranche
    is added.
    """

    venue = "counterparty"

    def __init__(self, base_url: Optional[str] = None, timeout_s: Optional[float] = None):
        fn = getattr(settings, "counterparty_effective_base_url", None)
        resolved = base_url or (fn() if callable(fn) else None) or os.getenv("COUNTERPARTY_API_BASE_URL") or "https://api.counterparty.io:4000"
        self.base_url = str(resolved or "").strip().rstrip("/")
        try:
            self.timeout_s = float(timeout_s if timeout_s is not None else (os.getenv("COUNTERPARTY_TIMEOUT_S") or "15"))
        except Exception:
            self.timeout_s = 15.0

    # ------------------------------------------------------------------
    # Low-level HTTP helpers
    # ------------------------------------------------------------------

    def _url(self, path: str) -> str:
        p = str(path or "").strip()
        if not p.startswith("/"):
            p = "/" + p
        return f"{self.base_url}{p}"

    def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.base_url.startswith(("https://", "http://")):
            raise ValueError("COUNTERPARTY_API_BASE_URL must start with http:// or https://")
        with httpx.Client(timeout=self.timeout_s, headers={"accept": "application/json"}) as client:
            r = client.get(self._url(path), params=params or {})
        body_preview = (r.text or "")[:800]
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} from Counterparty API path={path!r} body={body_preview}")
        try:
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"Non-JSON from Counterparty API path={path!r} body={body_preview}") from e
        return data if isinstance(data, dict) else {"data": data}

    def _first_ok(self, candidates: List[Tuple[str, Optional[Dict[str, Any]]]]) -> Dict[str, Any]:
        errors: List[Dict[str, Any]] = []
        for path, params in candidates:
            try:
                data = self._get_json(path, params=params)
                return {"ok": True, "path": path, "params": params or {}, "raw": data}
            except Exception as e:
                errors.append({"path": path, "params": params or {}, "error": str(e)[:500]})
        return {"ok": False, "errors": errors}

    @staticmethod
    def _items_from_payload(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if not isinstance(payload, dict):
            return []

        # Common API response containers across Counterparty Core and explorers.
        for key in ("result", "data", "items", "balances", "records", "rows"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
            if isinstance(val, dict):
                nested = CounterpartyAdapter._items_from_payload(val)
                if nested:
                    return nested
        return []

    @staticmethod
    def _asset_matches(row: Dict[str, Any], asset: str) -> bool:
        a = str(asset or "").strip().upper()
        for key in ("asset", "asset_name", "assetName", "symbol"):
            if str(row.get(key) or "").strip().upper() == a:
                return True
        return False

    @staticmethod
    def _as_float(v: Any) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None

    @staticmethod
    def _as_int(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(str(v).replace(",", ""))
        except Exception:
            try:
                return int(float(str(v).replace(",", "")))
            except Exception:
                return None

    @staticmethod
    def _first_dict_from_payload(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            for key in ("result", "data", "asset", "info", "item", "metadata"):
                val = payload.get(key)
                if isinstance(val, dict):
                    nested = CounterpartyAdapter._first_dict_from_payload(val)
                    if nested:
                        return nested
            return payload
        return {}

    @staticmethod
    def _as_bool(v: Any) -> Optional[bool]:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in {"1", "true", "yes", "y", "on", "locked"}:
            return True
        if s in {"0", "false", "no", "n", "off", "unlocked"}:
            return False
        return None

    @staticmethod
    def _first_present(row: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
        for key in keys:
            if key in row and row.get(key) not in (None, ""):
                return row.get(key)
        return None

    @classmethod
    def _normalize_asset_metadata(cls, asset: str, raw_payload: Any, *, source_path: Optional[str] = None) -> Dict[str, Any]:
        row = cls._first_dict_from_payload(raw_payload)
        asset_norm = str(asset or cls._first_present(row, ("asset", "asset_name", "assetName", "symbol")) or "").strip().upper()
        longname = str(cls._first_present(row, ("asset_longname", "assetLongname", "asset_long_name", "longname", "long_name")) or "").strip()
        issuer = str(cls._first_present(row, ("issuer", "owner", "source", "issuer_address", "issuerAddress", "owner_address", "ownerAddress", "current_holder", "holder")) or "").strip()
        description = str(cls._first_present(row, ("description", "desc", "memo", "text", "message")) or "").strip()
        divisible = cls._as_bool(cls._first_present(row, ("divisible", "is_divisible", "isDivisible")))
        locked = cls._as_bool(cls._first_present(row, ("locked", "lock", "is_locked", "isLocked", "locked_status")))
        callable_ = cls._as_bool(cls._first_present(row, ("callable", "is_callable", "isCallable")))
        reset = cls._as_bool(cls._first_present(row, ("reset", "resettable", "is_resettable", "isResettable")))

        supply_source = None
        supply = None
        supply_atomic = None
        supply_decimals = 8 if divisible is True else 0
        normalized_supply_keys = (
            "supply_normalized",
            "total_supply_normalized",
            "quantity_normalized",
            "normalized_supply",
            "supplyNormalized",
            "totalSupplyNormalized",
        )
        raw_supply_keys = ("supply", "total_supply", "quantity", "issued", "issued_supply", "totalSupply")

        for key in normalized_supply_keys:
            supply = cls._as_float(row.get(key))
            if supply is not None:
                supply_source = key
                if divisible is True:
                    supply_atomic = int(round(float(supply) * 100000000))
                elif divisible is False:
                    supply_atomic = int(round(float(supply)))
                break
        if supply is None:
            for key in raw_supply_keys:
                raw_supply = cls._as_float(row.get(key))
                if raw_supply is not None:
                    supply_source = key
                    if divisible is True:
                        # Counterparty Core v2 asset metadata exposes `supply` for
                        # divisible assets in atomic/base units. Convert it to
                        # display units so XCP-like assets do not show 1e8-scaled
                        # supply in UTT.
                        supply_atomic = int(round(float(raw_supply)))
                        supply = float(raw_supply) / 100000000.0
                    else:
                        supply = float(raw_supply)
                        supply_atomic = int(round(float(raw_supply)))
                    break

        call_date = cls._first_present(row, ("call_date", "callDate"))
        call_price = cls._as_float(cls._first_present(row, ("call_price", "callPrice")))
        block_index = cls._first_present(row, ("block_index", "blockIndex", "block"))
        tx_hash = str(cls._first_present(row, ("tx_hash", "txHash", "txid", "transaction_hash")) or "").strip()

        return {
            "asset": asset_norm,
            "asset_longname": longname or None,
            "issuer": issuer or None,
            "description": description or None,
            "divisible": divisible,
            "locked": locked,
            "callable": callable_,
            "reset": reset,
            "supply": supply,
            "supply_atomic": supply_atomic,
            "supply_decimals": supply_decimals,
            "supply_source": supply_source,
            "call_date": call_date,
            "call_price": call_price,
            "block_index": block_index,
            "tx_hash": tx_hash or None,
            "source_path": source_path,
            "raw_item": row,
        }

    # ------------------------------------------------------------------
    # Read endpoints
    # ------------------------------------------------------------------

    def diagnostics(self) -> Dict[str, Any]:
        network_fn = getattr(settings, "counterparty_effective_network", None)
        provider_fn = getattr(settings, "counterparty_effective_wallet_provider", None)
        enabled_fn = getattr(settings, "counterparty_effective_enabled", None)
        candidates = [
            ("/v2/", None),
            ("/v2/healthz", None),
            ("/healthz", None),
            ("/", None),
        ]
        probe = self._first_ok(candidates)
        return {
            "ok": True,
            "venue": self.venue,
            "enabled": bool(enabled_fn() if callable(enabled_fn) else True),
            "base_url": self.base_url,
            "network": network_fn() if callable(network_fn) else "mainnet",
            "wallet_provider": provider_fn() if callable(provider_fn) else "unisat",
            "read_only": True,
            "signing": "external_wallet_required",
            "probe": probe,
        }

    def wallet_provider_info(self, provider: str = "unisat") -> Dict[str, Any]:
        p = str(provider or "unisat").strip().lower() or "unisat"
        if p != "unisat":
            return {
                "ok": False,
                "provider": p,
                "error": "unsupported_wallet_provider",
                "supported": ["unisat"],
            }
        return {
            "ok": True,
            "provider": "unisat",
            "browser_object": "window.unisat",
            "read_methods": ["requestAccounts", "getAccounts", "getChain", "getNetwork", "getPublicKey", "getBalance", "getInscriptions"],
            "write_methods_later": ["signMessage", "signPsbt", "signPsbts", "pushPsbt", "pushTx", "sendBitcoin"],
            "utt_policy": "read-only backend now; browser wallet signing/PSBT compose later",
        }

    def get_asset(self, asset: str) -> Dict[str, Any]:
        a_norm = str(asset or "").strip().upper()
        a = quote(a_norm, safe="")
        result = self._first_ok([
            (f"/v2/assets/{a}", None),
            (f"/v2/assets/{a}/info", None),
            (f"/api/assets/{a}", None),
            (f"/assets/{a}", None),
        ])
        if result.get("ok"):
            result["asset"] = a_norm
            result["metadata"] = self._normalize_asset_metadata(a_norm, result.get("raw"), source_path=result.get("path"))
        return result

    def get_assets_metadata(self, assets: List[str], limit: int = 100) -> Dict[str, Any]:
        seen = set()
        normalized: List[str] = []
        for asset in assets or []:
            a = str(asset or "").strip().upper()
            if not a or a in seen:
                continue
            seen.add(a)
            normalized.append(a)
            if len(normalized) >= max(1, min(int(limit or 100), 200)):
                break

        items: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for asset in normalized:
            result = self.get_asset(asset)
            if result.get("ok"):
                items.append({
                    "ok": True,
                    "asset": asset,
                    "metadata": result.get("metadata") or {},
                    "source_path": result.get("path"),
                    "raw": result.get("raw"),
                })
            else:
                errors.append({"asset": asset, "errors": result.get("errors") or []})

        return {
            "ok": True,
            "count": len(items),
            "requested_count": len(normalized),
            "items": items,
            "errors": errors,
            "read_only": True,
        }

    def get_address_balances(self, address: str) -> Dict[str, Any]:
        addr = quote(str(address or "").strip(), safe="")
        return self._first_ok([
            (f"/v2/addresses/{addr}/balances", None),
            (f"/v2/balances/{addr}", None),
            ("/v2/balances", {"address": str(address or "").strip()}),
            (f"/api/balances/{addr}", None),
            ("/api/balances", {"address": str(address or "").strip()}),
            (f"/balances/{addr}", None),
        ])

    def get_address_asset_balance(self, address: str, asset: str) -> Dict[str, Any]:
        asset_norm = str(asset or "").strip().upper()
        balances = self.get_address_balances(address)
        if not balances.get("ok"):
            return {
                "ok": False,
                "address": address,
                "asset": asset_norm,
                "quantity": 0.0,
                "quantity_atomic": 0,
                "decimals": 8,
                "errors": balances.get("errors") or [],
            }

        items = self._items_from_payload(balances.get("raw"))
        matched = None
        for row in items:
            if self._asset_matches(row, asset_norm):
                matched = row
                break

        if matched is None:
            return {
                "ok": True,
                "address": address,
                "asset": asset_norm,
                "quantity": 0.0,
                "quantity_atomic": 0,
                "decimals": 8,
                "source_path": balances.get("path"),
                "raw": balances.get("raw"),
            }

        divisible = matched.get("divisible")
        decimals = 8 if divisible is not False else 0

        normalized_keys = ("quantity_normalized", "normalized_quantity", "quantityNormalized", "balance_normalized", "balanceNormalized", "qty_normalized")
        explicit_atomic_keys = ("quantity_atomic", "balance_atomic", "raw_quantity", "rawQuantity", "quantity_raw", "balance_raw")
        display_quantity_keys = ("quantity", "balance", "qty", "amount")

        units = None
        quantity_source = None
        for k in normalized_keys:
            units = self._as_float(matched.get(k))
            if units is not None:
                quantity_source = k
                break

        atomic = None
        if units is None:
            # Counterparty Core v2 address-balance rows commonly expose `quantity`
            # as a display quantity already (for example, FREESPIN quantity=1500),
            # while older/explorer payloads may expose explicit atomic/raw fields.
            # Only divide by decimals when an explicitly atomic/raw key is present.
            for k in explicit_atomic_keys:
                atomic = self._as_int(matched.get(k))
                if atomic is not None:
                    quantity_source = k
                    break

            if atomic is not None:
                units = float(atomic) / (10 ** int(decimals or 0))
            else:
                for k in display_quantity_keys:
                    units = self._as_float(matched.get(k))
                    if units is not None:
                        quantity_source = k
                        break
                if units is None:
                    units = 0.0
                atomic = int(round(float(units) * (10 ** int(decimals or 0))))
        else:
            atomic = int(round(float(units) * (10 ** int(decimals or 0))))

        return {
            "ok": True,
            "address": address,
            "asset": asset_norm,
            "quantity": float(units or 0.0),
            "quantity_atomic": int(atomic or 0),
            "decimals": int(decimals or 0),
            "quantity_source": quantity_source,
            "source_path": balances.get("path"),
            "raw_item": matched,
            "raw": balances.get("raw"),
        }

    def get_address_sends(self, address: str, limit: int = 50) -> Dict[str, Any]:
        addr = quote(str(address or "").strip(), safe="")
        lim = max(1, min(int(limit or 50), 500))
        return self._first_ok([
            (f"/v2/addresses/{addr}/sends", {"limit": lim}),
            ("/v2/sends", {"address": str(address or "").strip(), "limit": lim}),
            (f"/api/sends/{addr}", {"limit": lim}),
        ])

    def get_orders(self, asset: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
        lim = max(1, min(int(limit or 50), 500))
        params: Dict[str, Any] = {"limit": lim}
        if asset:
            params["asset"] = str(asset or "").strip().upper()
        candidates: List[Tuple[str, Optional[Dict[str, Any]]]] = [("/v2/orders", params)]
        if asset:
            a = quote(str(asset or "").strip().upper(), safe="")
            candidates.append((f"/v2/assets/{a}/orders", {"limit": lim}))
        candidates.append(("/api/orders", params))
        return self._first_ok(candidates)
