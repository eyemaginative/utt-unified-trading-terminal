from __future__ import annotations

import json
import os
import re
import time
from decimal import Decimal, ROUND_FLOOR
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

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
    _MARKET_CONTEXT_CACHE: Dict[str, Dict[str, Any]] = {}

    def __init__(self, base_url: Optional[str] = None, timeout_s: Optional[float] = None):
        fn = getattr(settings, "counterparty_effective_base_url", None)
        resolved = base_url or (fn() if callable(fn) else None) or os.getenv("COUNTERPARTY_API_BASE_URL") or "https://api.counterparty.io:4000"
        self.base_url = str(resolved or "").strip().rstrip("/")
        try:
            self.timeout_s = float(timeout_s if timeout_s is not None else (os.getenv("COUNTERPARTY_TIMEOUT_S") or "15"))
        except Exception:
            self.timeout_s = 15.0
        # Small in-process helper cache used only for order/dispenser quantity
        # normalization.  It stores display decimals inferred from Counterparty
        # asset metadata and never stores secrets or wallet data.
        self._quantity_decimals_cache: Dict[str, int] = {"BTC": 8, "XCP": 8, "BITCRYSTALS": 8, "PEPECASH": 8}

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


    @staticmethod
    def _looks_like_external_metadata_pointer(raw: Any) -> bool:
        s = str(raw or "").strip()
        if not s:
            return False
        if s.startswith(("http://", "https://", "ipfs://", "ar://", "arweave://", "//")):
            return True
        # Counterparty-era assets often store bare domains in the on-chain
        # description, for example: xcp.coindaddy.io/BITCRYSTALS.json.
        # Treat only domain-like strings with a slash or metadata extension as
        # external pointers so plain descriptions are not accidentally fetched.
        return bool(re.match(r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,}(/|$)", s) and ("/" in s or re.search(r"\.(json|png|jpe?g|gif|webp|html?)(\?|$)", s, flags=re.IGNORECASE)))

    @staticmethod
    def _safe_external_url(url: Any) -> Optional[str]:
        raw = str(url or "").strip()
        if not raw:
            return None
        if raw.startswith("//"):
            raw = "https:" + raw
        if raw.startswith("ipfs://"):
            raw = "https://ipfs.io/ipfs/" + raw[len("ipfs://"):].lstrip("/")
        if raw.startswith("ar://"):
            raw = "https://arweave.net/" + raw[len("ar://"):].lstrip("/")
        if raw.startswith("arweave://"):
            raw = "https://arweave.net/" + raw[len("arweave://"):].lstrip("/")
        if "://" not in raw and CounterpartyAdapter._looks_like_external_metadata_pointer(raw):
            raw = "https://" + raw

        parsed = urlparse(raw)
        if parsed.scheme not in {"https", "http"}:
            return None
        host = str(parsed.hostname or "").strip().lower()
        if not host:
            return None
        if host in {"localhost", "0.0.0.0"} or host.endswith(".local"):
            return None
        if host.startswith("127.") or host.startswith("10.") or host.startswith("192.168."):
            return None
        if host.startswith("172."):
            parts = host.split(".")
            try:
                if len(parts) >= 2 and 16 <= int(parts[1]) <= 31:
                    return None
            except Exception:
                pass
        return raw

    @staticmethod
    def _infer_content_type_from_url(url: str, fallback: Optional[str] = None) -> Optional[str]:
        fb = str(fallback or "").strip().lower()
        if fb:
            return fb
        path = urlparse(str(url or "")).path.lower()
        if path.endswith((".png", ".apng")):
            return "image/png"
        if path.endswith((".jpg", ".jpeg")):
            return "image/jpeg"
        if path.endswith(".gif"):
            return "image/gif"
        if path.endswith(".webp"):
            return "image/webp"
        if path.endswith(".mp4"):
            return "video/mp4"
        if path.endswith(".webm"):
            return "video/webm"
        if path.endswith(".mp3"):
            return "audio/mpeg"
        if path.endswith(".wav"):
            return "audio/wav"
        if path.endswith(".json"):
            return "application/json"
        return None

    @classmethod
    def _normalize_media_url(cls, url: Any, *, base_url: Optional[str] = None) -> Optional[str]:
        raw = str(url or "").strip()
        if not raw:
            return None
        if base_url and raw.startswith(("./", "../", "/")):
            raw = urljoin(base_url, raw)
        return cls._safe_external_url(raw)

    @classmethod
    def _media_url_candidates(cls, url: Any) -> List[str]:
        """Return browser-facing fallback candidates for immutable media URLs.

        Some Arweave registry records point at URLs shaped like:
            https://<manifest>.arweave.net/<data-id>/<filename>.png

        In several Counterparty/ORBital datasets the browser-resolvable media is
        the data-id URL without the trailing filename.  Keep the original first
        and add stripped/gateway variants as fallbacks.  This does not fetch or
        execute the media server-side.
        """
        safe_url = cls._safe_external_url(url)
        if not safe_url:
            return []

        candidates: List[str] = []

        def add(candidate: Any) -> None:
            safe = cls._safe_external_url(candidate)
            if safe and safe not in candidates:
                candidates.append(safe)

        add(safe_url)
        parsed = urlparse(safe_url)
        host = str(parsed.hostname or "").strip().lower()
        path_parts = [p for p in (parsed.path or "").split("/") if p]
        query = f"?{parsed.query}" if parsed.query else ""

        # Key case from ORBital/Arweave metadata:
        #   ...arweave.net/<data-id>/<name>_image.png
        # should also try:
        #   ...arweave.net/<data-id>
        #
        # Only apply the path-stripping fallback to immutable media gateways
        # where this behavior is known.  Do not strip ordinary web paths such
        # as rarepepes.com/wp-content/... down to rarepepes.com/wp-content.
        if len(path_parts) >= 2:
            root_path = "/" + path_parts[0] + query
            if host.endswith(".arweave.net") or host == "arweave.net":
                add(f"{parsed.scheme}://{parsed.netloc}{root_path}")
                add(f"https://arweave.net/{path_parts[0]}{query}")
                add(f"https://permagate.io/{path_parts[0]}{query}")
                add(f"https://ar-io.net/{path_parts[0]}{query}")
            elif host in {"permagate.io", "ar-io.net"}:
                add(f"{parsed.scheme}://{parsed.netloc}{root_path}")
                add(f"https://arweave.net/{path_parts[0]}{query}")
                add(f"https://permagate.io/{path_parts[0]}{query}")
                add(f"https://ar-io.net/{path_parts[0]}{query}")
            elif host in {"ipfs.io", "cloudflare-ipfs.com", "gateway.pinata.cloud"} and len(path_parts) >= 3 and path_parts[0].lower() == "ipfs":
                # /ipfs/<cid>/<filename> -> /ipfs/<cid>
                cid_path = f"/ipfs/{path_parts[1]}{query}"
                add(f"https://ipfs.io{cid_path}")
                add(f"https://cloudflare-ipfs.com{cid_path}")
                add(f"https://gateway.pinata.cloud{cid_path}")

        return candidates

    @classmethod
    def _with_media_url_candidates(cls, media: Dict[str, Any]) -> Dict[str, Any]:
        """Attach candidate URL arrays consumed by the NFT frontend fallback image loader."""
        if not isinstance(media, dict):
            return media

        out = dict(media)

        def assign(field: str) -> None:
            urls = cls._media_url_candidates(out.get(field))
            if urls:
                out[f"{field}_candidates"] = urls

        assign("image_url")
        assign("animation_url")
        assign("audio_url")
        assign("content_url")
        assign("preview_url")

        # Ensure content/preview fallbacks can use image fallbacks too.
        if out.get("image_url_candidates"):
            content_candidates = []
            for group in (out.get("content_url_candidates"), out.get("image_url_candidates")):
                for u in group or []:
                    if u and u not in content_candidates:
                        content_candidates.append(u)
            preview_candidates = []
            for group in (out.get("preview_url_candidates"), out.get("image_url_candidates"), out.get("content_url_candidates")):
                for u in group or []:
                    if u and u not in preview_candidates:
                        preview_candidates.append(u)
            if content_candidates:
                out["content_url_candidates"] = content_candidates
            if preview_candidates:
                out["preview_url_candidates"] = preview_candidates

        return out

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = str(os.getenv(name, "") or "").strip().lower()
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
        return bool(default)

    @staticmethod
    def _safe_json_object(raw: Any) -> Optional[Dict[str, Any]]:
        if isinstance(raw, dict):
            return raw
        s = str(raw or "").strip()
        if not s:
            return None
        try:
            data = json.loads(s)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    @classmethod
    def _extract_img_srcs_from_html(cls, html_text: Any) -> List[str]:
        txt = unescape(str(html_text or ""))
        if not txt:
            return []
        out: List[str] = []
        for match in re.finditer(r"<img\b[^>]*\bsrc\s*=\s*(['\"])(.*?)\1", txt, flags=re.IGNORECASE | re.DOTALL):
            safe = cls._safe_external_url(match.group(2))
            if safe and safe not in out:
                out.append(safe)
        # Some registries store escaped HTML or bare URLs rather than a clean img tag.
        for match in re.finditer(r"https?://[^\s'\"<>]+", txt):
            candidate = match.group(0).rstrip("),.;")
            if re.search(r"\.(png|apng|jpe?g|gif|webp)(\?|$)", candidate, flags=re.IGNORECASE):
                safe = cls._safe_external_url(candidate)
                if safe and safe not in out:
                    out.append(safe)
        return out

    @classmethod
    def _extract_media_from_registry_metadata(cls, asset: str, data: Dict[str, Any], *, source: str) -> Dict[str, Any]:
        meta = data if isinstance(data, dict) else {}
        image = cls._normalize_media_url(cls._first_present(meta, ("image_large_hd", "image_large", "image", "image_url", "imageUrl", "thumbnail", "preview")))
        video = cls._normalize_media_url(cls._first_present(meta, ("video", "video_url", "animation_url", "animationUrl")))
        audio = cls._normalize_media_url(cls._first_present(meta, ("audio", "audio_url")))
        if not image:
            for src in cls._extract_img_srcs_from_html(cls._first_present(meta, ("description", "desc", "html", "body"))):
                image = src
                break

        media_url = video or audio or image
        ctype = cls._infer_content_type_from_url(
            media_url or "",
            cls._first_present(meta, ("content_type", "contentType", "mime_type", "mimeType")),
        )
        if video and not ctype:
            ctype = "video/mp4"
        if audio and not ctype:
            ctype = "audio/mpeg"
        if image and not ctype:
            ctype = "image/*"

        media = {
            "ok": bool(media_url),
            "source": source,
            "asset": str(asset or cls._first_present(meta, ("asset", "name")) or "").strip().upper(),
            "name": cls._first_present(meta, ("name", "asset", "image_title", "title")),
            "description": cls._first_present(meta, ("description", "desc")),
            "image_url": image,
            "animation_url": video,
            "audio_url": audio,
            "content_url": media_url,
            "preview_url": image or media_url,
            "external_url": cls._normalize_media_url(cls._first_present(meta, ("website", "external_url", "externalUrl", "url"))),
            "content_type": ctype,
            "raw_metadata": meta,
        }
        return cls._with_media_url_candidates(media)

    def _asset_media_override(self, asset: str) -> Optional[Dict[str, Any]]:
        a_norm = str(asset or "").strip().upper()
        sources: List[Tuple[str, Optional[Dict[str, Any]]]] = []
        inline = self._safe_json_object(os.getenv("COUNTERPARTY_ASSET_MEDIA_OVERRIDES_JSON"))
        if inline:
            sources.append(("env_json", inline))
        path = str(os.getenv("COUNTERPARTY_ASSET_MEDIA_OVERRIDES_FILE") or "").strip()
        if path:
            try:
                file_data = self._safe_json_object(open(path, "r", encoding="utf-8").read())
                if file_data:
                    sources.append(("env_file", file_data))
            except Exception:
                pass

        for source, data in sources:
            if not isinstance(data, dict):
                continue
            row = None
            if a_norm in data and isinstance(data.get(a_norm), dict):
                row = data.get(a_norm)
            elif str(data.get("asset") or "").strip().upper() == a_norm:
                row = data
            if row:
                media = self._extract_media_from_registry_metadata(a_norm, row, source=f"override_{source}")
                return {
                    "ok": bool(media.get("ok")),
                    "source": f"override_{source}",
                    "asset": a_norm,
                    "raw": row,
                    "media": media if media.get("ok") else None,
                    "media_error": None if media.get("ok") else "no_media_url_in_override",
                }
        return None


    def _asset_media_probe_templates(self, asset: str, metadata: Optional[Dict[str, Any]] = None) -> List[str]:
        """Return opt-in/direct image probe templates for legacy registries.

        This is intentionally conservative.  It does not guess media for every
        Counterparty asset.  By default it only probes known legacy sources when
        the on-chain description points at that legacy source.  Operators can
        add more templates without code changes through:

            COUNTERPARTY_ASSET_MEDIA_PROBE_URL_TEMPLATES

        Template variables:
            {asset}, {asset_lower}, {asset_upper}
        """
        a_norm = str(asset or "").strip().upper()
        if not a_norm:
            return []

        meta = metadata if isinstance(metadata, dict) else {}
        desc = str(meta.get("description") or "").strip().lower()
        templates: List[str] = []

        def add(tpl: Any) -> None:
            s = str(tpl or "").strip()
            if s and s not in templates:
                templates.append(s)

        raw = str(os.getenv("COUNTERPARTY_ASSET_MEDIA_PROBE_URL_TEMPLATES") or "").strip()
        if raw:
            for tpl in re.split(r"[,\n]+", raw):
                add(tpl)

        # Rare Pepe / MyRarePepe legacy JSON endpoints are frequently parked or
        # unavailable now, while the canonical image assets often still live on
        # rarepepes.com by asset name.
        if "myrarepepe.com" in desc or "rarepepe" in desc or "rare pepe" in desc:
            add("https://rarepepes.com/wp-content/assets/rarepepe/{asset}.jpg")
            add("https://rarepepes.com/wp-content/assets/rarepepe/{asset}.png")
            add("https://rarepepes.com/wp-content/assets/rarepepe/{asset_lower}.jpg")
            add("https://rarepepes.com/wp-content/assets/rarepepe/{asset_lower}.png")

        # TokenScan CP20 hosts many Counterparty collectible/card images that
        # have only terse on-chain descriptions such as "SOG card" or no
        # direct metadata URL in Counterparty Core.  Probe only obvious
        # card/SoG-style rows and validate the response headers before using
        # the URL; this keeps the batch loader conservative and read-only.
        cardish = (
            a_norm.endswith("CARD")
            or a_norm.endswith("CD")
            or " card" in f" {desc} "
            or "sog" in desc
            or "spells of genesis" in desc
        )
        if cardish:
            add("https://cp20.tokenscan.io/img/cards/{asset}.jpg")
            add("https://cp20.tokenscan.io/img/cards/{asset}.png")
            add("https://cp20.tokenscan.io/img/cards/{asset_lower}.jpg")
            add("https://cp20.tokenscan.io/img/cards/{asset_lower}.png")

        return templates

    def _probe_registry_media_url(self, url: Any) -> Dict[str, Any]:
        safe_url = self._safe_external_url(url)
        if not safe_url:
            return {"ok": False, "url": str(url or ""), "error": "unsafe_or_unsupported_media_url"}

        timeout_s = self._env_float("COUNTERPARTY_ASSET_MEDIA_PROBE_TIMEOUT_S", 5.0, min_value=1.0, max_value=20.0)
        headers = {
            "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.2",
            "user-agent": "UTT Counterparty media probe/1.0",
            "range": "bytes=0-0",
        }
        try:
            with httpx.Client(timeout=timeout_s, headers=headers, follow_redirects=True) as client:
                # Streaming GET with a byte range lets us validate headers
                # without downloading the image payload.  Some legacy hosts do
                # not support HEAD consistently.
                with client.stream("GET", safe_url) as r:
                    ctype = str(r.headers.get("content-type") or "").split(";")[0].strip().lower()
                    final_url = str(r.url)
                    if r.status_code >= 400:
                        return {"ok": False, "url": safe_url, "final_url": final_url, "error": f"http_{r.status_code}", "content_type": ctype}
                    inferred = self._infer_content_type_from_url(final_url, ctype)
                    if not (str(inferred or "").startswith("image/") or str(ctype or "").startswith("image/")):
                        return {"ok": False, "url": safe_url, "final_url": final_url, "error": "probe_not_image", "content_type": ctype or inferred}
                    return {"ok": True, "url": safe_url, "final_url": final_url, "content_type": ctype or inferred or "image/*"}
        except Exception as e:
            return {"ok": False, "url": safe_url, "error": str(e)[:300]}

    def _fetch_asset_media_probe(self, asset: str, metadata: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        a_norm = str(asset or "").strip().upper()
        if not a_norm:
            return None

        attempts: List[Dict[str, Any]] = []
        templates = self._asset_media_probe_templates(a_norm, metadata)
        if not templates:
            return None

        for tpl in templates:
            try:
                url = str(tpl).format(
                    asset=quote(a_norm, safe=""),
                    asset_upper=quote(a_norm.upper(), safe=""),
                    asset_lower=quote(a_norm.lower(), safe=""),
                )
            except Exception:
                continue
            probe = self._probe_registry_media_url(url)
            attempts.append(probe)
            if not probe.get("ok"):
                continue

            image_url = self._safe_external_url(probe.get("final_url") or probe.get("url"))
            if not image_url:
                continue

            raw = {
                "asset": a_norm,
                "name": a_norm,
                "image": image_url,
                "image_url": image_url,
                "external_url": (
                    "https://rarepepes.com"
                    if "rarepepes.com" in image_url.lower()
                    else "https://cp20.tokenscan.io"
                    if "tokenscan.io" in image_url.lower()
                    else None
                ),
                "content_type": probe.get("content_type") or self._infer_content_type_from_url(image_url) or "image/*",
                "registry_probe_attempts": attempts,
            }
            media = self._extract_media_from_registry_metadata(a_norm, raw, source="registry_media_probe")
            if media.get("ok"):
                media["probe_attempts"] = attempts
                return {
                    "ok": True,
                    "source": "registry_media_probe",
                    "asset": a_norm,
                    "url": image_url,
                    "raw": raw,
                    "media": media,
                    "attempts": attempts,
                    "media_error": None,
                }

        return {
            "ok": False,
            "source": "registry_media_probe",
            "asset": a_norm,
            "error": attempts[-1].get("error") if attempts else "no_probe_templates",
            "attempts": attempts,
            "media": None,
            "media_error": attempts[-1].get("error") if attempts else "no_probe_templates",
        }

    def _orbital_asset_metadata_url_templates(self) -> List[str]:
        raw = str(os.getenv("COUNTERPARTY_ORBITAL_METADATA_URL_TEMPLATES") or "").strip()
        if raw:
            return [x.strip() for x in re.split(r"[,\n]+", raw) if x.strip()]
        # These are intentionally best-effort and may need adjustment if ORBital
        # changes or publishes a stable documented endpoint.  Keep this fallback
        # optional so the batch metadata window does not block on unknown APIs.
        return [
            "https://orbital.market/api/assets/{asset}",
            "https://orbital.market/api/asset/{asset}",
            "https://orbital.market/api/v1/assets/{asset}",
            "https://orbital.market/api/v1/asset/{asset}",
            "https://orbital.market/asset/{asset}.json",
        ]

    def _fetch_orbital_asset_metadata(self, asset: str) -> Dict[str, Any]:
        a_norm = str(asset or "").strip().upper()
        if not a_norm:
            return {"ok": False, "error": "missing_asset"}
        if not self._env_bool("COUNTERPARTY_ORBITAL_METADATA_ENABLED", False):
            return {"ok": False, "asset": a_norm, "skipped": True, "reason": "COUNTERPARTY_ORBITAL_METADATA_ENABLED is not enabled"}

        timeout_s = self._env_float("COUNTERPARTY_ORBITAL_METADATA_TIMEOUT_S", 8.0, min_value=2.0, max_value=30.0)
        max_bytes = self._env_int("COUNTERPARTY_ORBITAL_METADATA_MAX_BYTES", 512 * 1024, min_value=64 * 1024, max_value=2 * 1024 * 1024)
        attempts: List[Dict[str, Any]] = []
        headers = {
            "accept": "application/json,text/plain;q=0.9,*/*;q=0.2",
            "user-agent": "UTT Counterparty ORBital metadata reader/1.0",
        }
        for tpl in self._orbital_asset_metadata_url_templates():
            url = self._safe_external_url(str(tpl).format(asset=quote(a_norm, safe=""), asset_lower=quote(a_norm.lower(), safe="")))
            if not url:
                continue
            try:
                with httpx.Client(timeout=timeout_s, headers=headers, follow_redirects=True) as client:
                    r = client.get(url)
                ctype = str(r.headers.get("content-type") or "").split(";")[0].strip().lower()
                if r.status_code >= 400:
                    attempts.append({"url": url, "error": f"http_{r.status_code}"})
                    continue
                content = r.content or b""
                if len(content) > max_bytes:
                    attempts.append({"url": url, "error": "metadata_too_large", "content_length": len(content)})
                    continue
                try:
                    data = r.json()
                except Exception:
                    attempts.append({"url": url, "error": "metadata_not_json", "content_type": ctype, "preview": content.decode("utf-8", errors="replace")[:300]})
                    continue
                if not isinstance(data, dict):
                    attempts.append({"url": url, "error": "metadata_json_not_object", "content_type": ctype})
                    continue
                if data.get("success") is False:
                    attempts.append({"url": url, "error": str(data.get("error") or "success_false")[:200]})
                    continue
                media = self._extract_media_from_registry_metadata(a_norm, data, source="orbital")
                return {
                    "ok": bool(media.get("ok")),
                    "asset": a_norm,
                    "url": url,
                    "content_type": ctype,
                    "raw": data,
                    "media": media if media.get("ok") else None,
                    "media_error": None if media.get("ok") else "no_media_url_in_orbital_metadata",
                    "attempts": attempts,
                }
            except Exception as e:
                attempts.append({"url": url, "error": str(e)[:300]})
        return {"ok": False, "asset": a_norm, "error": attempts[-1].get("error") if attempts else "orbital_metadata_fetch_failed", "attempts": attempts}

    @staticmethod
    def _apply_registry_media(metadata: Dict[str, Any], registry_result: Optional[Dict[str, Any]], *, registry_key: str) -> None:
        if not registry_result:
            return
        metadata[registry_key] = registry_result
        media = registry_result.get("media") if isinstance(registry_result, dict) else None
        if isinstance(media, dict) and media.get("ok"):
            metadata["media"] = media
            metadata["media_error"] = None
            metadata["media_source"] = registry_result.get("source") or registry_key
            if media.get("description") and not metadata.get("media_description"):
                metadata["media_description"] = media.get("description")
            if media.get("name") and not metadata.get("media_name"):
                metadata["media_name"] = media.get("name")

    @staticmethod
    def _env_float(name: str, default: float, *, min_value: float, max_value: float) -> float:
        try:
            raw = os.getenv(name)
            n = float(raw) if raw not in (None, "") else float(default)
        except Exception:
            n = float(default)
        return max(float(min_value), min(float(max_value), n))

    @staticmethod
    def _env_int(name: str, default: int, *, min_value: int, max_value: int) -> int:
        try:
            raw = os.getenv(name)
            n = int(raw) if raw not in (None, "") else int(default)
        except Exception:
            n = int(default)
        return max(int(min_value), min(int(max_value), n))

    @staticmethod
    def _metadata_cache_enabled() -> bool:
        raw = str(os.getenv("COUNTERPARTY_ASSET_METADATA_CACHE_ENABLED", "1") or "").strip().lower()
        if raw in {"0", "false", "no", "n", "off"}:
            return False
        return True

    @staticmethod
    def _metadata_cache_ttl_s() -> int:
        try:
            return max(60, min(int(os.getenv("COUNTERPARTY_ASSET_METADATA_CACHE_TTL_S") or "604800"), 60 * 60 * 24 * 30))
        except Exception:
            return 604800

    @staticmethod
    def _metadata_cache_file() -> str:
        explicit = str(os.getenv("COUNTERPARTY_ASSET_METADATA_CACHE_FILE") or "").strip()
        if explicit:
            return explicit
        cache_dir = str(os.getenv("UTT_CACHE_DIR") or os.path.join(os.getcwd(), ".utt_cache")).strip()
        return os.path.join(cache_dir, "counterparty_asset_metadata_cache.json")

    def _metadata_cache_read(self) -> Dict[str, Any]:
        if not self._metadata_cache_enabled():
            return {"items": {}}
        path = self._metadata_cache_file()
        try:
            if not os.path.exists(path):
                return {"items": {}}
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {"items": {}}
            items = data.get("items") if isinstance(data.get("items"), dict) else {}
            return {"items": items}
        except Exception:
            return {"items": {}}

    def _metadata_cache_write(self, data: Dict[str, Any]) -> None:
        if not self._metadata_cache_enabled():
            return
        try:
            path = self._metadata_cache_file()
            os.makedirs(os.path.dirname(path), exist_ok=True)
            items = data.get("items") if isinstance(data.get("items"), dict) else {}
            max_items = self._env_int("COUNTERPARTY_ASSET_METADATA_CACHE_MAX_ITEMS", 1000, min_value=10, max_value=10000)
            sorted_items = dict(
                sorted(items.items(), key=lambda kv: int((kv[1] or {}).get("ts") or 0), reverse=True)[:max_items]
            )
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"version": 1, "items": sorted_items}, f, ensure_ascii=False)
            os.replace(tmp, path)
        except Exception:
            # Cache failures must not affect read-only Counterparty metadata.
            pass

    @classmethod
    def _metadata_has_retryable_media_failure(cls, metadata: Dict[str, Any]) -> bool:
        """Avoid persisting external-pointer failures as final no-media facts.

        If an asset has an on-chain external metadata pointer but no media, do
        not let one parked-domain HTML page, gateway timeout, 404, or stale
        no-media cache entry permanently suppress later retries.  Assets with no
        external pointer can still be cached as plain protocol balances.
        """
        if not isinstance(metadata, dict):
            return False
        media = metadata.get("media")
        if isinstance(media, dict) and media.get("ok"):
            return False

        external = metadata.get("external_metadata") if isinstance(metadata.get("external_metadata"), dict) else {}
        err = str(metadata.get("media_error") or "").strip().lower()
        external_err = str(external.get("error") or "").strip().lower()
        combined = " ".join([err, external_err]).strip()

        has_external_pointer = bool(
            metadata.get("external_metadata_url")
            or external.get("url")
            or cls._looks_like_external_metadata_pointer(metadata.get("description"))
        )
        if not has_external_pointer:
            # Several older Counterparty collectibles do not store a URL in the
            # on-chain description even though marketplace/wallet registries can
            # still have media for them.  Avoid long-lived "plain no-media"
            # cache entries for obvious collectible/card rows so future registry
            # probes can enrich them when a source is added.
            desc = str(metadata.get("description") or "").strip().lower()
            asset_name = str(metadata.get("asset") or "").strip().upper()
            if (
                asset_name.endswith("CARD")
                or " card" in f" {desc} "
                or asset_name.endswith("CD")
                or "sog" in desc
                or "spells of genesis" in desc
                or "rarepepe" in desc
                or "rare pepe" in desc
            ):
                return True
            return False

        stable_no_media = {
            "no_media_url_in_metadata",
            "no_media_url_in_override",
            "no_media_url_in_orbital_metadata",
        }
        if combined in stable_no_media:
            return False

        # Treat any failed external metadata lookup as retryable/stale.  This
        # includes metadata_not_json from parked legacy domains, 404/502 from
        # flaky Arweave gateways, and timeout/network errors.  It prevents
        # yesterday's failed no-media result from hiding today's available image.
        if combined:
            return True

        # Also retry older cached records that have a pointer-like description
        # but were normalized before bare-domain URL support existed.
        return True

    def _metadata_cache_get(self, asset: str) -> Optional[Dict[str, Any]]:
        a_norm = str(asset or "").strip().upper()
        if not a_norm:
            return None
        data = self._metadata_cache_read()
        entry = (data.get("items") or {}).get(a_norm)
        if not isinstance(entry, dict):
            return None
        ts = self._as_int(entry.get("ts")) or 0
        if ts and (int(__import__("time").time()) - ts) > self._metadata_cache_ttl_s():
            return None
        metadata = entry.get("metadata") if isinstance(entry.get("metadata"), dict) else None
        if not metadata:
            return None
        if self._metadata_has_retryable_media_failure(metadata):
            return None
        return {**metadata, "asset": metadata.get("asset") or a_norm, "cache_hit": True}

    def _metadata_cache_put(self, asset: str, metadata: Dict[str, Any]) -> None:
        a_norm = str(asset or "").strip().upper()
        if not a_norm or not isinstance(metadata, dict):
            return
        if self._metadata_has_retryable_media_failure(metadata):
            return
        data = self._metadata_cache_read()
        items = data.setdefault("items", {})
        # Store final normalized metadata, including media URLs and gateway
        # attempts, so future UI opens do not need to refetch slow Arweave JSON.
        items[a_norm] = {"ts": int(__import__("time").time()), "metadata": {**metadata, "asset": a_norm}}
        self._metadata_cache_write(data)

    @classmethod
    def _external_metadata_url_candidates(cls, url: Any) -> List[str]:
        safe_url = cls._safe_external_url(url)
        if not safe_url:
            return []

        parsed = urlparse(safe_url)
        host = str(parsed.hostname or "").strip().lower()
        suffix = parsed.path or "/"
        if parsed.query:
            suffix = f"{suffix}?{parsed.query}"

        candidates: List[str] = []

        def add(candidate: str) -> None:
            safe = cls._safe_external_url(candidate)
            if safe and safe not in candidates:
                candidates.append(safe)

        # Arweave subdomain gateways can encode the transaction/manifest id in
        # the host: https://<txid>.arweave.net/<manifest-path>.  Canonical
        # gateways need that host id restored into the path, otherwise a URL like
        # https://<txid>.arweave.net/foo.json is incorrectly retried as only
        # https://arweave.net/foo.json and will often 404.  Try the host+path
        # canonical form first, then the original subdomain form, then the older
        # path-only form for compatibility with plain arweave.net URLs that were
        # accidentally served through a subdomain gateway.
        if host.endswith(".arweave.net") and host != "arweave.net":
            arweave_host_id = host[: -len(".arweave.net")].strip(".")
            host_path = f"/{arweave_host_id}{suffix}" if arweave_host_id else suffix
            add(f"https://arweave.net{host_path}")
            add(safe_url)
            add(f"https://arweave.net{suffix}")
            add(f"https://ar-io.net{host_path}")
            add(f"https://permagate.io{host_path}")
            add(f"https://ar-io.net{suffix}")
            add(f"https://permagate.io{suffix}")
        elif host == "arweave.net":
            add(safe_url)
            add(f"https://ar-io.net{suffix}")
            add(f"https://permagate.io{suffix}")
        elif host == "ipfs.io" and suffix.startswith("/ipfs/"):
            add(safe_url)
            add(f"https://cloudflare-ipfs.com{suffix}")
            add(f"https://gateway.pinata.cloud{suffix}")
        else:
            add(safe_url)
            # Several older Counterparty metadata pointers are bare-domain or
            # HTTP-era URLs.  If HTTPS fails, try the HTTP form as a read-only
            # metadata fallback; if HTTP was supplied, also try HTTPS.
            if parsed.scheme == "https":
                add(f"http://{parsed.netloc}{suffix}")
            elif parsed.scheme == "http":
                add(f"https://{parsed.netloc}{suffix}")

        return candidates

    def _fetch_external_json_metadata(self, url: Any) -> Dict[str, Any]:
        candidates = self._external_metadata_url_candidates(url)
        if not candidates:
            return {"ok": False, "error": "unsafe_or_unsupported_metadata_url"}

        max_bytes = self._env_int("COUNTERPARTY_EXTERNAL_METADATA_MAX_BYTES", 512 * 1024, min_value=64 * 1024, max_value=2 * 1024 * 1024)
        timeout_s = self._env_float("COUNTERPARTY_EXTERNAL_METADATA_TIMEOUT_S", 18.0, min_value=3.0, max_value=45.0)
        attempts: List[Dict[str, Any]] = []
        headers = {
            "accept": "application/json,text/plain;q=0.9,*/*;q=0.2",
            "user-agent": "UTT Counterparty metadata reader/1.0",
        }

        for safe_url in candidates:
            try:
                with httpx.Client(timeout=timeout_s, headers=headers, follow_redirects=True) as client:
                    r = client.get(safe_url)
                ctype = str(r.headers.get("content-type") or "").split(";")[0].strip().lower()
                clen = r.headers.get("content-length")
                if clen:
                    try:
                        if int(clen) > max_bytes:
                            attempts.append({"url": safe_url, "error": "metadata_too_large", "content_length": int(clen)})
                            continue
                    except Exception:
                        pass
                if r.status_code >= 400:
                    attempts.append({"url": safe_url, "error": f"http_{r.status_code}"})
                    continue
                content = r.content or b""
                if len(content) > max_bytes:
                    attempts.append({"url": safe_url, "error": "metadata_too_large", "content_length": len(content)})
                    continue
                try:
                    data = r.json()
                except Exception:
                    text = content.decode("utf-8", errors="replace")[:2048]
                    attempts.append({"url": safe_url, "error": "metadata_not_json", "content_type": ctype, "preview": text[:500]})
                    continue
                if not isinstance(data, dict):
                    attempts.append({"url": safe_url, "error": "metadata_json_not_object", "content_type": ctype})
                    continue
                return {
                    "ok": True,
                    "url": safe_url,
                    "requested_url": candidates[0],
                    "content_type": ctype,
                    "data": data,
                    "attempts": attempts,
                }
            except Exception as e:
                attempts.append({"url": safe_url, "error": str(e)[:300]})

        last_error = attempts[-1].get("error") if attempts else "metadata_fetch_failed"
        return {
            "ok": False,
            "url": candidates[0],
            "error": last_error,
            "attempts": attempts,
        }

    @classmethod
    def _extract_media_from_external_metadata(cls, metadata_url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        meta = data if isinstance(data, dict) else {}
        props = meta.get("properties") if isinstance(meta.get("properties"), dict) else {}
        files = props.get("files") if isinstance(props.get("files"), list) else []

        # Support both conventional NFT JSON keys and ORBital/EasyAsset-style
        # registry JSON keys.  FREESPIN-like assets can return fields such as
        # image_large/image_large_hd/video/audio and HTML descriptions with an
        # embedded <img src=...>, not just image/animation_url.
        image = cls._normalize_media_url(
            cls._first_present(
                meta,
                (
                    "image_large_hd",
                    "image_large",
                    "image",
                    "image_url",
                    "imageUrl",
                    "imageURI",
                    "image_uri",
                    "thumbnail",
                    "preview",
                ),
            ),
            base_url=metadata_url,
        )
        animation = cls._normalize_media_url(
            cls._first_present(
                meta,
                ("video", "video_url", "animation_url", "animationUrl", "animation", "animationURI", "animation_uri"),
            ),
            base_url=metadata_url,
        )
        audio = cls._normalize_media_url(
            cls._first_present(meta, ("audio", "audio_url", "audioUrl")),
            base_url=metadata_url,
        )
        external_url = cls._normalize_media_url(
            cls._first_present(meta, ("website", "external_url", "externalUrl", "external", "url")),
            base_url=metadata_url,
        )

        if not image:
            for src in cls._extract_img_srcs_from_html(cls._first_present(meta, ("description", "desc", "html", "body"))):
                image = src
                break

        file_ctype = None
        if not image and files:
            for f in files:
                if not isinstance(f, dict):
                    continue
                f_url = cls._normalize_media_url(cls._first_present(f, ("uri", "url", "src")), base_url=metadata_url)
                f_type = str(cls._first_present(f, ("type", "mime", "mimeType", "content_type")) or "").strip()
                if f_url:
                    image = f_url
                    file_ctype = f_type or None
                    break

        media_url = animation or audio or image
        content_type = cls._infer_content_type_from_url(
            media_url or "",
            cls._first_present(meta, ("content_type", "contentType", "mime_type", "mimeType")) or file_ctype,
        )
        if animation and not content_type:
            content_type = "video/mp4"
        if audio and not content_type:
            content_type = "audio/mpeg"
        if image and not content_type:
            content_type = "image/*"

        media = {
            "ok": bool(media_url),
            "source": "external_metadata",
            "metadata_url": metadata_url,
            "name": cls._first_present(meta, ("name", "title", "asset", "image_title")),
            "description": cls._first_present(meta, ("description", "desc")),
            "image_url": image,
            "animation_url": animation,
            "audio_url": audio,
            "content_url": media_url,
            "preview_url": image or media_url,
            "external_url": external_url,
            "content_type": content_type,
            "attributes": meta.get("attributes") if isinstance(meta.get("attributes"), list) else None,
            "raw_metadata": meta,
        }
        return cls._with_media_url_candidates(media)

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
        external_metadata_url = cls._safe_external_url(description) if description else None

        return {
            "asset": asset_norm,
            "asset_longname": longname or None,
            "issuer": issuer or None,
            "description": description or None,
            "external_metadata_url": external_metadata_url,
            "media": None,
            "media_error": None,
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
    # Counterparty market/order/dispenser normalization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _status_text(row: Dict[str, Any]) -> str:
        raw = CounterpartyAdapter._first_present(
            row,
            (
                "status",
                "status_text",
                "statusText",
                "order_status",
                "orderStatus",
                "state",
                "dispense_status",
                "dispenser_status",
            ),
        )
        if raw is None:
            return ""
        # Counterparty Core dispenser status can be numeric in some payloads.
        # Keep the original value visible but provide useful labels for the
        # common open/closed cases.
        try:
            n = int(raw)
            if n == 0:
                return "open"
            if n == 10:
                return "closed"
        except Exception:
            pass
        return str(raw or "").strip()

    @staticmethod
    def _is_open_status(status: Any) -> bool:
        s = str(status or "").strip().lower()
        if not s:
            return True
        terminal_bits = (
            "cancel",
            "filled",
            "expired",
            "closed",
            "complete",
            "invalid",
            "drop",
            "dropped",
            "fail",
        )
        return not any(bit in s for bit in terminal_bits)

    @staticmethod
    def _decimals_from_divisible(divisible: Any, *, fallback: int = 8) -> int:
        parsed = CounterpartyAdapter._as_bool(divisible)
        if parsed is True:
            return 8
        if parsed is False:
            return 0
        return int(fallback)

    @classmethod
    def _divisible_hint_from_row(cls, row: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[bool]:
        for key in keys:
            if key in row and row.get(key) not in (None, ""):
                parsed = cls._as_bool(row.get(key))
                if parsed is not None:
                    return parsed
        return None

    def _asset_divisible_fast(self, asset: Any) -> Optional[bool]:
        """Return Counterparty divisibility without fetching external media.

        Balance normalization must be cheap and deterministic.  Calling
        get_asset() here would also run external metadata/media probes for
        collectible images, which is unnecessary and can make balance loads
        slow.  This helper only reads the protocol asset record.
        """
        a_norm = str(asset or "").strip().upper()
        if not a_norm:
            return None
        if a_norm in self._quantity_decimals_cache:
            return int(self._quantity_decimals_cache[a_norm]) > 0
        if a_norm in {"BTC", "XCP", "BITCRYSTALS", "PEPECASH"}:
            self._quantity_decimals_cache[a_norm] = 8
            return True
        try:
            a = quote(a_norm, safe="")
            result = self._first_ok([
                (f"/v2/assets/{a}", None),
                (f"/v2/assets/{a}/info", None),
                (f"/api/assets/{a}", None),
                (f"/assets/{a}", None),
            ])
            if result.get("ok"):
                row = self._first_dict_from_payload(result.get("raw"))
                divisible = self._as_bool(self._first_present(row, ("divisible", "is_divisible", "isDivisible")))
                if divisible is not None:
                    self._quantity_decimals_cache[a_norm] = 8 if divisible else 0
                    return bool(divisible)
        except Exception:
            pass
        return None

    @staticmethod
    def _balance_quantity_key_is_atomic(source_path: Optional[str], key: str) -> bool:
        """Counterparty Core v2 balance rows expose `quantity` in base units.

        Asset metadata/supply uses display normalization elsewhere, but address
        balance snapshots from /v2/addresses/.../balances return integer
        quantities for divisible assets.  Treat only known Counterparty Core
        balance paths this way so explorer-style display payloads are not
        accidentally divided.
        """
        k = str(key or "").strip()
        if k not in {"quantity", "balance", "qty", "amount"}:
            return False
        p = str(source_path or "").strip().lower()
        return p.startswith("/v2/addresses/") or p.startswith("/v2/balances")

    def _asset_display_decimals(self, asset: Any) -> int:
        a_norm = str(asset or "").strip().upper()
        if not a_norm:
            return 8
        if a_norm in self._quantity_decimals_cache:
            return int(self._quantity_decimals_cache[a_norm])
        divisible = self._asset_divisible_fast(a_norm)
        if divisible is True:
            return 8
        if divisible is False:
            return 0
        # Conservative fallback for unknown Counterparty assets in market rows.
        # Address balances use stricter source-aware logic below.  Do not cache
        # this fallback, because an unknown non-divisible asset should not poison
        # later balance normalization.
        return 8


    def _quantity_from_row(
        self,
        row: Dict[str, Any],
        asset: Any,
        normalized_keys: Tuple[str, ...],
        raw_keys: Tuple[str, ...],
        *,
        divisible_hint: Optional[bool] = None,
    ) -> Optional[float]:
        for key in normalized_keys:
            value = self._as_float(row.get(key))
            if value is not None:
                return float(value)

        raw_value = None
        for key in raw_keys:
            if key in row and row.get(key) not in (None, ""):
                raw_value = row.get(key)
                break
        if raw_value is None:
            return None

        # If a downstream explorer already returns a decimal string here, treat
        # it as display units.  Counterparty Core integer fields normally use
        # atomic units for divisible assets.
        raw_text = str(raw_value).strip()
        raw_number = self._as_float(raw_value)
        if raw_number is None:
            return None
        if "." in raw_text:
            return float(raw_number)

        decimals = self._decimals_from_divisible(divisible_hint, fallback=self._asset_display_decimals(asset)) if divisible_hint is not None else self._asset_display_decimals(asset)
        if decimals <= 0:
            return float(raw_number)
        return float(raw_number) / float(10 ** decimals)

    def _normalize_order_row(self, row: Dict[str, Any], asset: str) -> Dict[str, Any]:
        asset_norm = str(asset or "").strip().upper()
        give_asset = str(self._first_present(row, ("give_asset", "giveAsset", "base_asset", "baseAsset", "sell_asset")) or "").strip().upper()
        get_asset = str(self._first_present(row, ("get_asset", "getAsset", "quote_asset", "quoteAsset", "buy_asset")) or "").strip().upper()
        give_divisible_hint = self._divisible_hint_from_row(row, ("give_asset_divisible", "giveAssetDivisible", "give_divisible", "giveDivisible"))
        get_divisible_hint = self._divisible_hint_from_row(row, ("get_asset_divisible", "getAssetDivisible", "get_divisible", "getDivisible"))

        give_qty = self._quantity_from_row(
            row,
            give_asset,
            ("give_quantity_normalized", "giveQuantityNormalized", "give_normalized", "give_display_quantity"),
            ("give_quantity", "giveQuantity", "give_amount", "giveAmount"),
            divisible_hint=give_divisible_hint,
        )
        get_qty = self._quantity_from_row(
            row,
            get_asset,
            ("get_quantity_normalized", "getQuantityNormalized", "get_normalized", "get_display_quantity"),
            ("get_quantity", "getQuantity", "get_amount", "getAmount"),
            divisible_hint=get_divisible_hint,
        )
        give_remaining = self._quantity_from_row(
            row,
            give_asset,
            ("give_remaining_normalized", "giveRemainingNormalized", "give_remaining_display"),
            ("give_remaining", "giveRemaining"),
            divisible_hint=give_divisible_hint,
        )
        get_remaining = self._quantity_from_row(
            row,
            get_asset,
            ("get_remaining_normalized", "getRemainingNormalized", "get_remaining_display"),
            ("get_remaining", "getRemaining"),
            divisible_hint=get_divisible_hint,
        )

        side = "related"
        quote_asset = ""
        base_quantity = None
        quote_quantity = None
        base_remaining = None
        quote_remaining = None
        if give_asset == asset_norm:
            side = "ask"
            quote_asset = get_asset
            base_quantity = give_qty
            quote_quantity = get_qty
            base_remaining = give_remaining
            quote_remaining = get_remaining
        elif get_asset == asset_norm:
            side = "bid"
            quote_asset = give_asset
            base_quantity = get_qty
            quote_quantity = give_qty
            base_remaining = get_remaining
            quote_remaining = give_remaining

        explicit_price = self._as_float(self._first_present(row, ("price", "unit_price", "unitPrice", "rate")))
        price = explicit_price
        if price is None and base_quantity not in (None, 0) and quote_quantity is not None:
            try:
                price = float(quote_quantity) / float(base_quantity)
            except Exception:
                price = None

        status = self._status_text(row)
        tx_hash = str(self._first_present(row, ("tx_hash", "txHash", "txid", "order_hash", "hash")) or "").strip()
        source = str(self._first_present(row, ("source", "address", "source_address", "sourceAddress")) or "").strip()

        return {
            "asset": asset_norm,
            "side": side,
            "quote_asset": quote_asset or None,
            "price": price,
            "base_quantity": base_quantity,
            "quote_quantity": quote_quantity,
            "base_remaining": base_remaining,
            "quote_remaining": quote_remaining,
            "give_asset": give_asset or None,
            "give_quantity": give_qty,
            "give_remaining": give_remaining,
            "get_asset": get_asset or None,
            "get_quantity": get_qty,
            "get_remaining": get_remaining,
            "status": status,
            "is_open": self._is_open_status(status),
            "source": source or None,
            "tx_hash": tx_hash or None,
            "block_index": self._first_present(row, ("block_index", "blockIndex", "block")),
            "expiration": self._first_present(row, ("expiration", "expire_index", "expireIndex")),
            "raw_item": row,
        }

    def _normalize_dispenser_row(self, row: Dict[str, Any], asset: str) -> Dict[str, Any]:
        asset_norm = str(asset or self._first_present(row, ("asset", "give_asset", "giveAsset")) or "").strip().upper()
        give_asset = str(self._first_present(row, ("asset", "give_asset", "giveAsset")) or asset_norm).strip().upper()
        give_divisible_hint = self._divisible_hint_from_row(row, ("asset_divisible", "assetDivisible", "give_asset_divisible", "giveAssetDivisible", "divisible"))
        give_quantity = self._quantity_from_row(
            row,
            give_asset,
            ("give_quantity_normalized", "giveQuantityNormalized", "quantity_normalized", "quantityNormalized"),
            ("give_quantity", "giveQuantity", "quantity", "amount"),
            divisible_hint=give_divisible_hint,
        )
        escrow_quantity = self._quantity_from_row(
            row,
            give_asset,
            ("escrow_quantity_normalized", "escrowQuantityNormalized", "escrow_normalized"),
            ("escrow_quantity", "escrowQuantity"),
            divisible_hint=give_divisible_hint,
        )
        give_remaining = self._quantity_from_row(
            row,
            give_asset,
            ("give_remaining_normalized", "giveRemainingNormalized", "remaining_normalized"),
            ("give_remaining", "giveRemaining", "remaining", "remaining_quantity"),
            divisible_hint=give_divisible_hint,
        )
        satoshirate = self._as_int(self._first_present(row, ("satoshirate", "satoshi_rate", "satoshiRate", "rate", "price_sats")))
        price_btc = float(satoshirate) / 100000000.0 if satoshirate is not None else None
        price_btc_per_unit = None
        if price_btc is not None and give_quantity not in (None, 0):
            try:
                price_btc_per_unit = float(price_btc) / float(give_quantity)
            except Exception:
                price_btc_per_unit = None

        status = self._status_text(row)
        tx_hash = str(self._first_present(row, ("tx_hash", "txHash", "txid", "dispenser_tx_hash", "hash")) or "").strip()
        source = str(self._first_present(row, ("source", "address", "source_address", "sourceAddress")) or "").strip()

        return {
            "asset": asset_norm,
            "give_asset": give_asset or None,
            "give_quantity": give_quantity,
            "escrow_quantity": escrow_quantity,
            "give_remaining": give_remaining,
            "satoshirate": satoshirate,
            "price_btc": price_btc,
            "price_btc_per_unit": price_btc_per_unit,
            "quote_asset": "BTC" if satoshirate is not None else None,
            "status": status,
            "is_open": self._is_open_status(status),
            "source": source or None,
            "tx_hash": tx_hash or None,
            "oracle_address": self._first_present(row, ("oracle_address", "oracleAddress")),
            "block_index": self._first_present(row, ("block_index", "blockIndex", "block")),
            "raw_item": row,
        }

    @staticmethod
    def _market_items(result: Dict[str, Any]) -> List[Dict[str, Any]]:
        return CounterpartyAdapter._items_from_payload(result.get("raw") if isinstance(result, dict) else result)

    def _quote_summary(self, asset: str, orders: List[Dict[str, Any]], dispensers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        by_quote: Dict[str, Dict[str, Any]] = {}

        def bucket(quote_asset: Any) -> Dict[str, Any]:
            q = str(quote_asset or "").strip().upper() or "UNKNOWN"
            if q not in by_quote:
                by_quote[q] = {"quote_asset": q, "bids": [], "asks": []}
            return by_quote[q]

        for order in orders or []:
            if not order.get("is_open"):
                continue
            price = self._as_float(order.get("price"))
            if price is None or price <= 0:
                continue
            b = bucket(order.get("quote_asset"))
            if order.get("side") == "bid":
                b["bids"].append(order)
            elif order.get("side") == "ask":
                b["asks"].append(order)

        for disp in dispensers or []:
            if not disp.get("is_open"):
                continue
            price = self._as_float(disp.get("price_btc_per_unit")) or self._as_float(disp.get("price_btc"))
            if price is None or price <= 0:
                continue
            synthetic = {**disp, "side": "ask", "price": price, "quote_asset": "BTC", "source_type": "dispenser"}
            bucket("BTC")["asks"].append(synthetic)

        summaries: List[Dict[str, Any]] = []
        for q, data in sorted(by_quote.items(), key=lambda kv: kv[0]):
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            best_bid = max(bids, key=lambda x: float(x.get("price") or 0), default=None)
            best_ask = min(asks, key=lambda x: float(x.get("price") or 0), default=None)
            bid_px = self._as_float(best_bid.get("price")) if isinstance(best_bid, dict) else None
            ask_px = self._as_float(best_ask.get("price")) if isinstance(best_ask, dict) else None
            spread = ask_px - bid_px if bid_px is not None and ask_px is not None else None
            spread_pct = (spread / bid_px * 100.0) if spread is not None and bid_px not in (None, 0) else None
            summaries.append({
                "quote_asset": q,
                "best_bid": bid_px,
                "best_ask": ask_px,
                "spread": spread,
                "spread_pct": spread_pct,
                "bid_count": len(bids),
                "ask_count": len(asks),
                "best_bid_row": best_bid,
                "best_ask_row": best_ask,
            })
        return summaries


    # ------------------------------------------------------------------
    # Read-only orderbook helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _canonical_market_asset(asset: Any) -> str:
        a = str(asset or "").strip().upper()
        aliases = {
            "BCY": "BITCRYSTALS",
            "BITCRYSTAL": "BITCRYSTALS",
            "BITCRYSTALS": "BITCRYSTALS",
            "XCP": "XCP",
            "BTC": "BTC",
            "XBT": "BTC",
        }
        return aliases.get(a, a)

    @classmethod
    def _parse_orderbook_symbol(cls, symbol: Any) -> Tuple[str, str, str]:
        raw = str(symbol or "").strip().upper().replace("/", "-").replace("_", "-")
        if not raw or "-" not in raw:
            raise ValueError("Counterparty orderbook symbol must be BASE-QUOTE, e.g. XCP-BTC or BITCRYSTALS-XCP")
        parts = [p.strip() for p in raw.split("-") if p.strip()]
        if len(parts) != 2:
            raise ValueError("Counterparty orderbook symbol must contain exactly one base and quote asset")
        base = cls._canonical_market_asset(parts[0])
        quote_asset = cls._canonical_market_asset(parts[1])
        if not base or not quote_asset:
            raise ValueError("Counterparty orderbook base and quote are required")
        if base == quote_asset:
            raise ValueError("Counterparty orderbook base and quote must be different")
        return base, quote_asset, f"{base}-{quote_asset}"

    @staticmethod
    def _orderbook_level_size(row: Dict[str, Any]) -> Optional[float]:
        for key in ("base_remaining", "base_quantity", "give_remaining", "give_quantity", "size"):
            n = CounterpartyAdapter._as_float(row.get(key))
            if n is not None and n > 0:
                return float(n)
        return None

    @classmethod
    def _orderbook_level_from_order(cls, row: Dict[str, Any], *, side: str, quote_asset: str) -> Optional[Dict[str, Any]]:
        price = cls._as_float(row.get("price"))
        size = cls._orderbook_level_size(row)
        if price is None or price <= 0 or size is None or size <= 0:
            return None
        return {
            "price": float(price),
            "size": float(size),
            "side": side,
            "quote_asset": quote_asset,
            "source_type": "counterparty_order",
            "source": row.get("source"),
            "tx_hash": row.get("tx_hash"),
            "status": row.get("status"),
            "raw_order": row,
        }

    @classmethod
    def _orderbook_level_from_dispenser(cls, row: Dict[str, Any], *, quote_asset: str = "BTC") -> Optional[Dict[str, Any]]:
        price = cls._as_float(row.get("price_btc_per_unit")) or cls._as_float(row.get("price_btc"))
        size = None
        for key in ("give_remaining", "escrow_quantity", "give_quantity", "size"):
            n = cls._as_float(row.get(key))
            if n is not None and n > 0:
                size = float(n)
                break
        if price is None or price <= 0 or size is None or size <= 0:
            return None
        return {
            "price": float(price),
            "size": float(size),
            "side": "ask",
            "quote_asset": quote_asset,
            "source_type": "counterparty_dispenser",
            "source": row.get("source"),
            "tx_hash": row.get("tx_hash"),
            "status": row.get("status"),
            "satoshirate": row.get("satoshirate"),
            "raw_dispenser": row,
        }

    def get_orderbook(self, symbol: str, depth: int = 25, open_only: bool = True) -> Dict[str, Any]:
        """Build a read-only Counterparty order/dispenser book for BASE-QUOTE.

        This does not compose, sign, submit, or broadcast transactions.  It is a
        normalized view for the generic OrderBookWidget.  BTC-quoted asks can be
        sourced from open dispensers; protocol order rows supply asset/asset
        bids and asks when available.
        """
        base, quote_asset, symbol_canon = self._parse_orderbook_symbol(symbol)
        d = max(1, min(int(depth or 25), 200))
        lim = max(25, min(max(d * 4, 50), 500))

        orders_result = self.get_asset_orders(base, limit=lim, open_only=open_only)
        orders = orders_result.get("items") or []

        bids: List[Dict[str, Any]] = []
        asks: List[Dict[str, Any]] = []
        for row in orders:
            if open_only and not row.get("is_open"):
                continue
            q = self._canonical_market_asset(row.get("quote_asset"))
            if q != quote_asset:
                continue
            side = str(row.get("side") or "").strip().lower()
            if side == "bid":
                lvl = self._orderbook_level_from_order(row, side="bid", quote_asset=quote_asset)
                if lvl:
                    bids.append(lvl)
            elif side == "ask":
                lvl = self._orderbook_level_from_order(row, side="ask", quote_asset=quote_asset)
                if lvl:
                    asks.append(lvl)

        dispensers_result: Dict[str, Any] = {"ok": True, "items": [], "errors": []}
        if quote_asset == "BTC":
            dispensers_result = self.get_asset_dispensers(base, limit=lim, open_only=open_only)
            for row in dispensers_result.get("items") or []:
                if open_only and not row.get("is_open"):
                    continue
                lvl = self._orderbook_level_from_dispenser(row, quote_asset="BTC")
                if lvl:
                    asks.append(lvl)

        bids = sorted(bids, key=lambda x: float(x.get("price") or 0), reverse=True)[:d]
        asks = sorted(asks, key=lambda x: float(x.get("price") or 0))[:d]

        base_decimals = self._asset_display_decimals(base)
        quote_decimals = self._asset_display_decimals(quote_asset)
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        spread = (float(best_ask) - float(best_bid)) if best_bid is not None and best_ask is not None else None
        spread_pct = (spread / float(best_bid) * 100.0) if spread is not None and best_bid not in (None, 0) else None

        return {
            "ok": True,
            "venue": self.venue,
            "symbol": symbol_canon,
            "symbol_canon": symbol_canon,
            "baseAsset": base,
            "base_asset": base,
            "quoteAsset": quote_asset,
            "quote_asset": quote_asset,
            "depth": d,
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "spread_pct": spread_pct,
            "priceDecimals": max(0, min(int(quote_decimals or 8), 12)),
            "sizeDecimals": max(0, min(int(base_decimals or 0), 12)),
            "sources": {
                "orders": orders_result.get("source_path"),
                "dispensers": dispensers_result.get("source_path"),
            },
            "errors": {
                "orders": orders_result.get("errors") or [],
                "dispensers": dispensers_result.get("errors") or [],
            },
            "counts": {
                "orders": len(orders),
                "bids": len(bids),
                "asks": len(asks),
                "dispensers": len(dispensers_result.get("items") or []),
            },
            "open_only": bool(open_only),
            "read_only": True,
            "signing": "not_available_in_this_tranche",
            "compose": "later_unsigned_psbt_or_wallet_flow",
        }

    # ------------------------------------------------------------------
    # Unsigned compose preview helpers
    # ------------------------------------------------------------------

    def _post_json(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.base_url.startswith(("https://", "http://")):
            raise ValueError("COUNTERPARTY_API_BASE_URL must start with http:// or https://")
        with httpx.Client(timeout=self.timeout_s, headers={"accept": "application/json", "content-type": "application/json"}) as client:
            r = client.post(self._url(path), json=payload or {})
        body_preview = (r.text or "")[:800]
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} from Counterparty API path={path!r} body={body_preview}")
        try:
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"Non-JSON from Counterparty API path={path!r} body={body_preview}") from e
        return data if isinstance(data, dict) else {"data": data}

    @staticmethod
    def _decimal_or_none(value: Any) -> Optional[Decimal]:
        if value is None or value == "":
            return None
        try:
            d = Decimal(str(value).replace(",", "").strip())
        except Exception:
            return None
        if not d.is_finite():
            return None
        return d

    @staticmethod
    def _decimal_plain(value: Optional[Decimal], *, max_places: int = 18) -> Optional[str]:
        if value is None:
            return None
        try:
            q = value.quantize(Decimal(1) / (Decimal(10) ** max(0, min(int(max_places), 18))))
            s = format(q, "f")
            if "." in s:
                s = s.rstrip("0").rstrip(".")
            return s or "0"
        except Exception:
            try:
                return format(value, "f")
            except Exception:
                return str(value)

    def _display_quantity_to_atomic(self, asset: Any, quantity: Any) -> Optional[int]:
        d = self._decimal_or_none(quantity)
        if d is None or d < 0:
            return None
        decimals = max(0, min(int(self._asset_display_decimals(asset)), 18))
        scaled = (d * (Decimal(10) ** decimals)).to_integral_value(rounding=ROUND_FLOOR)
        try:
            return int(scaled)
        except Exception:
            return None

    @staticmethod
    def _selected_counterparty_book_row(row: Any) -> Dict[str, Any]:
        return row if isinstance(row, dict) else {}

    @staticmethod
    def _compose_level_price_decimal(row: Dict[str, Any]) -> Optional[Decimal]:
        if not isinstance(row, dict):
            return None
        for key in ("price", "displayPrice", "display_price", "limitPrice", "limit_price", "rate"):
            if row.get(key) not in (None, ""):
                d = CounterpartyAdapter._decimal_or_none(row.get(key))
                if d is not None and d > 0:
                    return d
        return None

    @staticmethod
    def _compose_level_is_dispenser(row: Any) -> bool:
        if not isinstance(row, dict):
            return False
        return bool(
            "dispenser" in str(row.get("source_type") or "").strip().lower()
            or isinstance(row.get("raw_dispenser"), dict)
        )

    @staticmethod
    def _compose_level_oracle_address(row: Any) -> Optional[str]:
        if not isinstance(row, dict):
            return None
        raw_dispenser = row.get("raw_dispenser") if isinstance(row.get("raw_dispenser"), dict) else {}
        oracle = row.get("oracle_address") or raw_dispenser.get("oracle_address") or raw_dispenser.get("oracleAddress")
        oracle_text = str(oracle or "").strip()
        return oracle_text or None

    def _compose_level_rejection_reasons(
        self,
        row: Any,
        *,
        side: str,
        limit_price: Decimal,
        quantity: Decimal,
    ) -> List[str]:
        """Return every reason a dispenser row is unsafe for this compose ticket."""
        if not isinstance(row, dict):
            return ["malformed_level"]

        reasons: List[str] = []
        px = self._compose_level_price_decimal(row)
        size_dec = self._decimal_or_none(row.get("size"))
        source = str(row.get("source") or "").strip()

        if px is None or px <= 0 or size_dec is None or size_dec <= 0 or not source:
            reasons.append("malformed_level")
            return reasons

        # Counterparty oracle dispensers require oracle-adjusted pricing rules.
        # Until those semantics are implemented and verified, their raw book
        # price must never be treated as a conventional fixed BTC price.
        if self._compose_level_oracle_address(row):
            reasons.append("oracle_price_unsupported")

        trade_side = str(side or "").strip().lower()
        if (trade_side == "buy" and px > limit_price) or (trade_side == "sell" and px < limit_price):
            reasons.append("price_outside_limit")

        if size_dec < quantity:
            reasons.append("insufficient_level_size")

        return reasons

    def _compose_level_diagnostic_row(self, row: Any, *, index: int, reasons: List[str]) -> Dict[str, Any]:
        if not isinstance(row, dict):
            return {"index": index, "reasons": list(reasons or ["malformed_level"])}
        px = self._compose_level_price_decimal(row)
        size_dec = self._decimal_or_none(row.get("size"))
        return {
            "index": index,
            "reasons": list(reasons or []),
            "price": self._decimal_plain(px, max_places=18) if px is not None else None,
            "size": self._decimal_plain(size_dec, max_places=18) if size_dec is not None else None,
            "source_type": str(row.get("source_type") or "").strip() or None,
            "source": str(row.get("source") or "").strip() or None,
            "tx_hash": str(row.get("tx_hash") or "").strip() or None,
            "oracle_address": self._compose_level_oracle_address(row),
        }

    def _find_compose_book_level(
        self,
        *,
        symbol: str,
        side: str,
        limit_price: Decimal,
        quantity: Decimal,
        depth: int = 100,
    ) -> Dict[str, Any]:
        """Find one safe conventional dispenser and explain every rejection.

        Automatic level selection exists only to recover a missing selected_level
        for BTC-quoted BUY dispenser previews.  Generic order rows continue to
        use compose/order and are never silently reclassified as dispensers.
        """
        sym = str(symbol or "").strip().upper()
        trade_side = str(side or "").strip().lower()
        diagnostics: Dict[str, Any] = {
            "attempted": True,
            "symbol": sym or None,
            "side": trade_side or None,
            "limit_price": self._decimal_plain(limit_price, max_places=18),
            "quantity": self._decimal_plain(quantity, max_places=18),
            "book_side": "asks" if trade_side == "buy" else "bids",
            "row_count": 0,
            "dispenser_row_count": 0,
            "ignored_non_dispenser_count": 0,
            "eligible_count": 0,
            "rejected_count": 0,
            "rejections": [],
            "selected": None,
            "reason": None,
        }

        if not sym or trade_side not in {"buy", "sell"}:
            diagnostics["reason"] = "invalid_symbol_or_side"
            return {"selected_level": None, "diagnostics": diagnostics}

        try:
            _base_asset, quote_asset, _symbol_canon = self._parse_orderbook_symbol(sym)
        except Exception as e:
            diagnostics["reason"] = "symbol_parse_failed"
            diagnostics["error"] = str(e)[:300]
            return {"selected_level": None, "diagnostics": diagnostics}

        # Dispensers are executable only as BTC-quoted BUY asks in this tranche.
        if trade_side != "buy" or quote_asset != "BTC":
            diagnostics["reason"] = "not_btc_buy_dispenser_shape"
            return {"selected_level": None, "diagnostics": diagnostics}

        try:
            book = self.get_orderbook(symbol=sym, depth=max(1, min(int(depth or 100), 200)), open_only=True)
        except Exception as e:
            diagnostics["reason"] = "orderbook_unavailable"
            diagnostics["error"] = str(e)[:300]
            return {"selected_level": None, "diagnostics": diagnostics}

        rows = book.get("asks") if trade_side == "buy" else book.get("bids")
        if not isinstance(rows, list) or not rows:
            diagnostics["reason"] = "no_book_rows"
            return {"selected_level": None, "diagnostics": diagnostics}

        diagnostics["row_count"] = len(rows)
        candidates: List[Tuple[Decimal, int, Dict[str, Any]]] = []
        rejected: List[Dict[str, Any]] = []

        for idx, row in enumerate(rows):
            if not self._compose_level_is_dispenser(row):
                diagnostics["ignored_non_dispenser_count"] += 1
                continue

            diagnostics["dispenser_row_count"] += 1
            reasons = self._compose_level_rejection_reasons(
                row,
                side=trade_side,
                limit_price=limit_price,
                quantity=quantity,
            )
            if reasons:
                rejected.append(self._compose_level_diagnostic_row(row, index=idx, reasons=reasons))
                continue

            px = self._compose_level_price_decimal(row)
            if px is None:
                rejected.append(self._compose_level_diagnostic_row(row, index=idx, reasons=["malformed_level"]))
                continue

            # BUY chooses the lowest qualifying conventional dispenser ask.
            candidates.append((px, idx, row))

        diagnostics["eligible_count"] = len(candidates)
        diagnostics["rejected_count"] = len(rejected)
        diagnostics["rejections"] = rejected

        if not candidates:
            diagnostics["reason"] = "no_safe_executable_dispenser"
            return {"selected_level": None, "diagnostics": diagnostics}

        candidates.sort(key=lambda x: (x[0], x[1]))
        selected_price, selected_index, selected_row = candidates[0]
        chosen = dict(selected_row)
        chosen.setdefault("selection_source", "auto_orderbook_match")
        diagnostics["selected"] = self._compose_level_diagnostic_row(chosen, index=selected_index, reasons=[])
        diagnostics["selected"]["price"] = self._decimal_plain(selected_price, max_places=18)
        diagnostics["reason"] = "selected_safe_executable_dispenser"
        return {"selected_level": chosen, "diagnostics": diagnostics}

    def _compose_try_candidates(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        errors: List[Dict[str, Any]] = []
        for candidate in candidates or []:
            method = str(candidate.get("method") or "GET").strip().upper()
            path = str(candidate.get("path") or "").strip()
            params = candidate.get("params") if isinstance(candidate.get("params"), dict) else {}
            json_payload = candidate.get("json") if isinstance(candidate.get("json"), dict) else {}
            if not path:
                continue
            try:
                if method == "POST":
                    raw = self._post_json(path, payload=json_payload or params)
                else:
                    raw = self._get_json(path, params=params)
                return {"ok": True, "candidate": candidate, "raw": raw}
            except Exception as e:
                errors.append({"candidate": candidate, "error": str(e)[:1000]})
        return {"ok": False, "errors": errors}

    def preview_compose(
        self,
        *,
        source_address: str,
        symbol: str,
        side: str,
        quantity: Any,
        limit_price: Any,
        selected_level: Optional[Dict[str, Any]] = None,
        attempt_upstream: bool = True,
    ) -> Dict[str, Any]:
        """Build a read-only unsigned Counterparty compose preview.

        This endpoint never signs and never broadcasts.  If attempt_upstream is
        true, it probes Counterparty Core compose endpoints and returns the
        unsigned compose response or the upstream errors for review.
        """
        source = str(source_address or "").strip()
        if not source:
            raise ValueError("source_address is required for Counterparty compose preview")

        trade_side = str(side or "").strip().lower()
        if trade_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")

        base, quote_asset, symbol_canon = self._parse_orderbook_symbol(symbol)
        qty_dec = self._decimal_or_none(quantity)
        px_dec = self._decimal_or_none(limit_price)
        if qty_dec is None or qty_dec <= 0:
            raise ValueError("quantity must be positive")
        if px_dec is None or px_dec <= 0:
            raise ValueError("limit_price must be positive")

        quote_qty_dec = qty_dec * px_dec
        base_atomic = self._display_quantity_to_atomic(base, qty_dec)
        quote_atomic = self._display_quantity_to_atomic(quote_asset, quote_qty_dec)
        btc_sats = self._display_quantity_to_atomic("BTC", quote_qty_dec) if quote_asset == "BTC" else None
        level = self._selected_counterparty_book_row(selected_level)
        selected_level_source = "provided" if level else "none"
        provided_level_validation: Optional[Dict[str, Any]] = None
        auto_selection_diagnostics: Dict[str, Any] = {
            "attempted": False,
            "reason": "selected_level_provided" if level else "not_started",
            "rejections": [],
        }

        # Enforce the same safety rules for a dispenser row supplied by the UI.
        # A clicked oracle-backed or undersized dispenser must not bypass the
        # backend auto-selection guard and reach compose/dispense.
        if level and trade_side == "buy" and quote_asset == "BTC" and self._compose_level_is_dispenser(level):
            provided_reasons = self._compose_level_rejection_reasons(
                level,
                side=trade_side,
                limit_price=px_dec,
                quantity=qty_dec,
            )
            provided_level_validation = self._compose_level_diagnostic_row(level, index=0, reasons=provided_reasons)
            provided_level_validation["accepted"] = not bool(provided_reasons)
            if provided_reasons:
                level = {}
                selected_level_source = "none"

        if not level:
            auto_match = self._find_compose_book_level(
                symbol=symbol_canon,
                side=trade_side,
                limit_price=px_dec,
                quantity=qty_dec,
            )
            auto_selection_diagnostics = auto_match.get("diagnostics") if isinstance(auto_match.get("diagnostics"), dict) else auto_selection_diagnostics
            auto_level = auto_match.get("selected_level") if isinstance(auto_match.get("selected_level"), dict) else None
            if auto_level:
                level = auto_level
                selected_level_source = "auto_orderbook_match"

        level_source_type = str(level.get("source_type") or "").strip().lower()
        level_source = str(level.get("source") or "").strip()
        level_tx_hash = str(level.get("tx_hash") or "").strip()
        level_size_dec = self._decimal_or_none(level.get("size"))
        dispenser_like = trade_side == "buy" and quote_asset == "BTC" and ("dispenser" in level_source_type or bool(level.get("raw_dispenser")))

        warnings = [
            "Unsigned compose preview only. UTT did not sign or broadcast this transaction.",
            "Review source address, assets, quantities, fee behavior, and selected order/dispenser details before enabling wallet signing.",
        ]

        if provided_level_validation and provided_level_validation.get("accepted") is False:
            rejected_reasons = ", ".join(provided_level_validation.get("reasons") or []) or "unsafe_level"
            warnings.append(
                f"The supplied Counterparty dispenser level was rejected for compose execution ({rejected_reasons}); UTT searched for a safe conventional replacement."
            )

        if selected_level_source == "auto_orderbook_match":
            warnings.append(
                "No eligible supplied book level was available; UTT auto-selected a current full-size, non-oracle Counterparty dispenser for compose preview."
            )
        elif auto_selection_diagnostics.get("attempted") and auto_selection_diagnostics.get("reason") == "no_safe_executable_dispenser":
            warnings.append(
                "No full-size, non-oracle Counterparty dispenser satisfied this ticket limit; UTT used the safe compose/order preview fallback."
            )

        if dispenser_like and level_size_dec is not None and qty_dec > level_size_dec:
            warnings.append(
                f"Requested quantity {self._decimal_plain(qty_dec, max_places=self._asset_display_decimals(base))} {base} exceeds selected dispenser level size {self._decimal_plain(level_size_dec, max_places=self._asset_display_decimals(base))} {base}. Compose preview may be rejected upstream unless quantity is reduced."
            )

        if trade_side == "buy":
            give_asset = quote_asset
            give_quantity_display = quote_qty_dec
            give_quantity_atomic = quote_atomic
            get_asset = base
            get_quantity_display = qty_dec
            get_quantity_atomic = base_atomic
        else:
            give_asset = base
            give_quantity_display = qty_dec
            give_quantity_atomic = base_atomic
            get_asset = quote_asset
            get_quantity_display = quote_qty_dec
            get_quantity_atomic = quote_atomic

        compose_kind = "dispenser_dispense" if dispenser_like else "order"
        escaped_source = quote(source, safe="")
        candidates: List[Dict[str, Any]] = []

        if compose_kind == "dispenser_dispense":
            dispenser_ref = level_source
            if not dispenser_ref:
                compose_kind = "order"
            else:
                # Counterparty Core v2 /compose/dispense accepts a narrow parameter set.
                # The prior preview sent extra human/audit fields (asset, btc_amount,
                # destination, quantity_normalized, satoshirate), and Core rejected them
                # as unrecognized.  Keep the upstream request minimal and preserve the
                # richer audit fields separately under preview_params.
                #
                # For a dispenser dispense, quantity is the BTC amount to send to the
                # dispenser in satoshis.  The received asset quantity remains visible
                # in preview_params/get_quantity for operator review.
                compact_dispense_params = {
                    "dispenser": dispenser_ref,
                    "quantity": btc_sats,
                }
                compact_dispense_params = {k: v for k, v in compact_dispense_params.items() if v not in (None, "")}
                preview_params = {
                    "dispenser": dispenser_ref,
                    "dispenser_tx_hash": level_tx_hash or None,
                    "asset": base,
                    "asset_quantity": base_atomic,
                    "asset_quantity_normalized": self._decimal_plain(qty_dec, max_places=self._asset_display_decimals(base)),
                    "btc_amount": btc_sats,
                    "btc_amount_normalized": self._decimal_plain(quote_qty_dec, max_places=8),
                    "satoshirate": level.get("satoshirate"),
                    "selected_level_size": self._decimal_plain(level_size_dec, max_places=self._asset_display_decimals(base)) if level_size_dec is not None else None,
                }
                preview_params = {k: v for k, v in preview_params.items() if v not in (None, "")}
                candidates.append({
                    "method": "GET",
                    "path": f"/v2/addresses/{escaped_source}/compose/dispense",
                    "params": compact_dispense_params,
                    "preview_params": preview_params,
                })
                if level_tx_hash:
                    tx_hash_params = dict(compact_dispense_params)
                    tx_hash_params["dispenser"] = level_tx_hash
                    candidates.append({
                        "method": "GET",
                        "path": f"/v2/addresses/{escaped_source}/compose/dispense",
                        "params": tx_hash_params,
                        "preview_params": {**preview_params, "dispenser": level_tx_hash, "dispenser_address": dispenser_ref},
                    })

        if compose_kind == "order":
            order_params = {
                "give_asset": give_asset,
                "give_quantity": give_quantity_atomic,
                "get_asset": get_asset,
                "get_quantity": get_quantity_atomic,
                "expiration": 1000,
                "fee_required": 0,
                "fee_provided": 0,
            }
            compact_order_params = {k: v for k, v in order_params.items() if v not in (None, "")}
            for path in (
                f"/v2/addresses/{escaped_source}/compose/order",
                f"/api/addresses/{escaped_source}/compose/order",
            ):
                candidates.append({"method": "GET", "path": path, "params": compact_order_params})

        compose_probe = {"ok": False, "skipped": True, "reason": "attempt_upstream=false"}
        if attempt_upstream:
            compose_probe = self._compose_try_candidates(candidates)

        return {
            "ok": True,
            "venue": self.venue,
            "symbol": symbol_canon,
            "symbol_canon": symbol_canon,
            "base_asset": base,
            "quote_asset": quote_asset,
            "side": trade_side,
            "source_address": source,
            "compose_kind": compose_kind,
            "quantity": self._decimal_plain(qty_dec, max_places=self._asset_display_decimals(base)),
            "limit_price": self._decimal_plain(px_dec, max_places=self._asset_display_decimals(quote_asset)),
            "quote_total": self._decimal_plain(quote_qty_dec, max_places=self._asset_display_decimals(quote_asset)),
            "give_asset": give_asset,
            "give_quantity": self._decimal_plain(give_quantity_display, max_places=self._asset_display_decimals(give_asset)),
            "give_quantity_atomic": give_quantity_atomic,
            "get_asset": get_asset,
            "get_quantity": self._decimal_plain(get_quantity_display, max_places=self._asset_display_decimals(get_asset)),
            "get_quantity_atomic": get_quantity_atomic,
            "selected_level": level or None,
            "selected_level_source": selected_level_source,
            "provided_level_validation": provided_level_validation,
            "auto_selection_diagnostics": auto_selection_diagnostics,
            "candidate_requests": candidates,
            "attempted_upstream": bool(attempt_upstream),
            "compose_ok": bool(compose_probe.get("ok")),
            "compose_result": compose_probe.get("raw") if compose_probe.get("ok") else None,
            "compose_candidate": compose_probe.get("candidate") if compose_probe.get("ok") else None,
            "compose_errors": compose_probe.get("errors") or [],
            "read_only": True,
            "unsigned_only": True,
            "signed": False,
            "broadcast": False,
            "signing": "not_performed",
            "broadcasting": "not_performed",
            "warnings": warnings,
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
        cached_metadata = self._metadata_cache_get(a_norm)
        if cached_metadata:
            return {
                "ok": True,
                "asset": a_norm,
                "path": cached_metadata.get("source_path"),
                "metadata": cached_metadata,
                "cached": True,
                "read_only": True,
            }

        a = quote(a_norm, safe="")
        result = self._first_ok([
            (f"/v2/assets/{a}", None),
            (f"/v2/assets/{a}/info", None),
            (f"/api/assets/{a}", None),
            (f"/assets/{a}", None),
        ])
        if result.get("ok"):
            result["asset"] = a_norm
            metadata = self._normalize_asset_metadata(a_norm, result.get("raw"), source_path=result.get("path"))
            ext_url = metadata.get("external_metadata_url")
            if ext_url:
                external = self._fetch_external_json_metadata(ext_url)
                if external.get("ok"):
                    metadata_url = external.get("url") or ext_url
                    media = self._extract_media_from_external_metadata(metadata_url, external.get("data") or {})
                    metadata["external_metadata"] = {
                        "ok": True,
                        "url": metadata_url,
                        "requested_url": ext_url,
                        "content_type": external.get("content_type"),
                        "raw": external.get("data"),
                        "attempts": external.get("attempts") or [],
                    }
                    metadata["media"] = media if media.get("ok") else None
                    metadata["media_error"] = None if media.get("ok") else "no_media_url_in_metadata"
                    if media.get("description") and not metadata.get("media_description"):
                        metadata["media_description"] = media.get("description")
                    if media.get("name") and not metadata.get("media_name"):
                        metadata["media_name"] = media.get("name")
                else:
                    metadata["external_metadata"] = external
                    metadata["media"] = None
                    metadata["media_error"] = external.get("error")

            # Some ORBital / EasyAsset-era Counterparty collectibles use a
            # separate off-chain registry/media record rather than the current
            # on-chain asset description URL.  Use local operator overrides
            # first; optional ORBital API probing is disabled by default to
            # avoid blocking batch metadata loads on undocumented endpoints.
            if not metadata.get("media"):
                self._apply_registry_media(metadata, self._asset_media_override(a_norm), registry_key="media_override")
            if not metadata.get("media"):
                self._apply_registry_media(metadata, self._fetch_asset_media_probe(a_norm, metadata), registry_key="registry_media_probe")
            if not metadata.get("media"):
                orbital = self._fetch_orbital_asset_metadata(a_norm)
                if orbital and not orbital.get("skipped"):
                    self._apply_registry_media(metadata, orbital, registry_key="orbital_metadata")
                elif orbital:
                    metadata["orbital_metadata"] = orbital

            result["metadata"] = metadata
            self._metadata_cache_put(a_norm, metadata)
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


    def _normalize_address_balance_row(self, row: Dict[str, Any], *, source_path: Optional[str] = None) -> Dict[str, Any]:
        asset = str(self._first_present(row, ("asset", "asset_name", "assetName", "symbol")) or "").strip().upper()
        longname = str(self._first_present(row, ("asset_longname", "assetLongname", "longname", "asset_long_name")) or "").strip()
        divisible = self._as_bool(self._first_present(row, ("divisible", "is_divisible", "isDivisible")))

        normalized_keys = (
            "quantity_normalized",
            "normalized_quantity",
            "quantityNormalized",
            "balance_normalized",
            "balanceNormalized",
            "qty_normalized",
        )
        explicit_atomic_keys = (
            "quantity_atomic",
            "balance_atomic",
            "raw_quantity",
            "rawQuantity",
            "quantity_raw",
            "balance_raw",
        )
        display_quantity_keys = ("quantity", "balance", "qty", "amount")

        raw_display_value = None
        for key in display_quantity_keys:
            if key in row and row.get(key) not in (None, ""):
                raw_display_value = row.get(key)
                break

        # Counterparty Core v2 address balances frequently omit `divisible` but
        # expose integer `quantity` in base units for divisible assets.  That is
        # why XCP and BITCRYSTALS can appear 1e8 too large if the UI treats
        # `quantity` as already-display units.  Infer divisibility cheaply for
        # known or obviously-atomic rows; otherwise preserve old display behavior
        # for small non-divisible collectibles.
        if divisible is None:
            if asset in self._quantity_decimals_cache:
                divisible = int(self._quantity_decimals_cache[asset]) > 0
            else:
                raw_int = self._as_int(raw_display_value)
                cardish = asset.endswith("CARD") or asset.endswith("CD")
                if raw_int is not None and abs(int(raw_int)) >= 100000000 and not cardish:
                    inferred = self._asset_divisible_fast(asset)
                    if inferred is not None:
                        divisible = bool(inferred)

        decimals = self._decimals_from_divisible(divisible, fallback=0)

        quantity_source = None
        units = None
        for key in normalized_keys:
            units = self._as_float(row.get(key))
            if units is not None:
                quantity_source = key
                break

        atomic = None
        if units is None:
            for key in explicit_atomic_keys:
                atomic = self._as_int(row.get(key))
                if atomic is not None:
                    quantity_source = key
                    break

            if atomic is not None:
                units = float(atomic) / float(10 ** int(decimals or 0)) if decimals > 0 else float(atomic)
            else:
                raw_units = None
                raw_key = None
                for key in display_quantity_keys:
                    raw_units = self._as_float(row.get(key))
                    if raw_units is not None:
                        raw_key = key
                        break

                if raw_units is None:
                    units = 0.0
                    atomic = 0
                    quantity_source = None
                else:
                    raw_text = str(row.get(raw_key) or "").strip()
                    raw_int = self._as_int(row.get(raw_key))
                    if decimals > 0 and raw_int is not None and "." not in raw_text and self._balance_quantity_key_is_atomic(source_path, str(raw_key or "")):
                        atomic = int(raw_int)
                        units = float(atomic) / float(10 ** int(decimals or 0))
                        quantity_source = f"{raw_key}_atomic_inferred"
                    else:
                        units = float(raw_units)
                        atomic = int(round(float(units) * float(10 ** int(decimals or 0)))) if decimals > 0 else int(round(float(units)))
                        quantity_source = raw_key
        else:
            atomic = int(round(float(units) * float(10 ** int(decimals or 0)))) if decimals > 0 else int(round(float(units)))

        return {
            **row,
            "asset": asset,
            "asset_longname": longname or row.get("asset_longname") or row.get("assetLongname"),
            "quantity": float(units or 0.0),
            "quantity_normalized": float(units or 0.0),
            "quantity_atomic": int(atomic or 0),
            "decimals": int(decimals or 0),
            "divisible": divisible,
            "quantity_source": quantity_source,
            "source_path": source_path,
            "raw_item": row,
        }


    def get_address_balances(self, address: str) -> Dict[str, Any]:
        addr = quote(str(address or "").strip(), safe="")
        result = self._first_ok([
            (f"/v2/addresses/{addr}/balances", None),
            (f"/v2/balances/{addr}", None),
            ("/v2/balances", {"address": str(address or "").strip()}),
            (f"/api/balances/{addr}", None),
            ("/api/balances", {"address": str(address or "").strip()}),
            (f"/balances/{addr}", None),
        ])
        if not result.get("ok"):
            return result

        raw_items = self._items_from_payload(result.get("raw"))
        items = [
            self._normalize_address_balance_row(row, source_path=result.get("path"))
            for row in raw_items
            if isinstance(row, dict)
        ]
        return {
            **result,
            "address": str(address or "").strip(),
            "count": len(items),
            "items": items,
            "read_only": True,
        }

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

        items = balances.get("items") if isinstance(balances.get("items"), list) else self._items_from_payload(balances.get("raw"))
        matched = None
        for row in items:
            if self._asset_matches(row, asset_norm):
                matched = row
                break

        if matched is None:
            decimals = self._asset_display_decimals(asset_norm)
            return {
                "ok": True,
                "address": address,
                "asset": asset_norm,
                "quantity": 0.0,
                "quantity_normalized": 0.0,
                "quantity_atomic": 0,
                "decimals": int(decimals or 0),
                "source_path": balances.get("path"),
                "raw": balances.get("raw"),
            }

        normalized = matched if matched.get("quantity_normalized") is not None else self._normalize_address_balance_row(matched, source_path=balances.get("path"))

        return {
            "ok": True,
            "address": address,
            "asset": asset_norm,
            "quantity": float(normalized.get("quantity_normalized") or normalized.get("quantity") or 0.0),
            "quantity_normalized": float(normalized.get("quantity_normalized") or normalized.get("quantity") or 0.0),
            "quantity_atomic": int(normalized.get("quantity_atomic") or 0),
            "decimals": int(normalized.get("decimals") or 0),
            "divisible": normalized.get("divisible"),
            "quantity_source": normalized.get("quantity_source"),
            "source_path": balances.get("path"),
            "raw_item": normalized.get("raw_item") if isinstance(normalized.get("raw_item"), dict) else matched,
            "raw": balances.get("raw"),
        }

    def get_address_balances_audit(self, address: str, assets: Optional[List[str]] = None) -> Dict[str, Any]:
        requested = [str(a or "").strip().upper() for a in (assets or []) if str(a or "").strip()]
        balances = self.get_address_balances(address)
        if not balances.get("ok"):
            return {"ok": False, "address": address, "items": [], "errors": balances.get("errors") or []}

        rows = balances.get("items") if isinstance(balances.get("items"), list) else []
        by_asset = {str(row.get("asset") or "").strip().upper(): row for row in rows if isinstance(row, dict)}
        wanted = requested or sorted(by_asset.keys())

        out = []
        for asset_name in wanted:
            row = by_asset.get(asset_name)
            if row:
                out.append({
                    "asset": asset_name,
                    "present": True,
                    "quantity": float(row.get("quantity_normalized") or row.get("quantity") or 0.0),
                    "quantity_normalized": float(row.get("quantity_normalized") or row.get("quantity") or 0.0),
                    "quantity_atomic": int(row.get("quantity_atomic") or 0),
                    "decimals": int(row.get("decimals") or 0),
                    "divisible": row.get("divisible"),
                    "quantity_source": row.get("quantity_source"),
                    "source_path": row.get("source_path") or balances.get("path"),
                    "raw_item": row.get("raw_item") if isinstance(row.get("raw_item"), dict) else row,
                })
            else:
                decimals = self._asset_display_decimals(asset_name)
                out.append({
                    "asset": asset_name,
                    "present": False,
                    "quantity": 0.0,
                    "quantity_normalized": 0.0,
                    "quantity_atomic": 0,
                    "decimals": int(decimals or 0),
                    "divisible": True if int(decimals or 0) == 8 else False if int(decimals or 0) == 0 else None,
                    "quantity_source": "missing",
                    "source_path": balances.get("path"),
                    "raw_item": None,
                })

        return {
            "ok": True,
            "address": str(address or "").strip(),
            "requested": wanted,
            "count": len(out),
            "items": out,
            "all_balance_count": len(rows),
            "source_path": balances.get("path"),
            "read_only": True,
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

    def get_asset_orders(self, asset: str, limit: int = 50, open_only: bool = False) -> Dict[str, Any]:
        a_norm = str(asset or "").strip().upper()
        lim = max(1, min(int(limit or 50), 500))
        a = quote(a_norm, safe="")
        status_params = {"status": "open"} if open_only else {}
        candidates: List[Tuple[str, Optional[Dict[str, Any]]]] = [
            (f"/v2/assets/{a}/orders", {"limit": lim, **status_params}),
            ("/v2/orders", {"asset": a_norm, "limit": lim, **status_params}),
            ("/v2/orders", {"give_asset": a_norm, "limit": lim, **status_params}),
            ("/v2/orders", {"get_asset": a_norm, "limit": lim, **status_params}),
            ("/api/orders", {"asset": a_norm, "limit": lim, **status_params}),
        ]
        result = self._first_ok(candidates)
        items = []
        if result.get("ok"):
            rows = self._market_items(result)
            for row in rows:
                give_asset = str(self._first_present(row, ("give_asset", "giveAsset", "base_asset", "baseAsset", "sell_asset")) or "").strip().upper()
                get_asset = str(self._first_present(row, ("get_asset", "getAsset", "quote_asset", "quoteAsset", "buy_asset")) or "").strip().upper()
                if a_norm and a_norm not in {give_asset, get_asset}:
                    continue
                normalized = self._normalize_order_row(row, a_norm)
                if open_only and not normalized.get("is_open"):
                    continue
                items.append(normalized)
        return {
            "ok": bool(result.get("ok")),
            "asset": a_norm,
            "count": len(items),
            "items": items,
            "source_path": result.get("path"),
            "errors": result.get("errors") or [],
            "raw": result.get("raw"),
            "open_only": bool(open_only),
            "read_only": True,
        }

    def get_asset_dispensers(self, asset: str, limit: int = 50, open_only: bool = False) -> Dict[str, Any]:
        a_norm = str(asset or "").strip().upper()
        lim = max(1, min(int(limit or 50), 500))
        a = quote(a_norm, safe="")
        # Counterparty Core uses numeric status=0 for open dispensers.  Some
        # explorer-style APIs use text status=open.  Try text first; if ignored,
        # the local open_only filter below still keeps the UI context focused.
        status_params = {"status": "open"} if open_only else {}
        candidates: List[Tuple[str, Optional[Dict[str, Any]]]] = [
            (f"/v2/assets/{a}/dispensers", {"limit": lim, **status_params}),
            ("/v2/dispensers", {"asset": a_norm, "limit": lim, **status_params}),
            ("/v2/dispensers", {"give_asset": a_norm, "limit": lim, **status_params}),
            ("/api/dispensers", {"asset": a_norm, "limit": lim, **status_params}),
            (f"/api/assets/{a}/dispensers", {"limit": lim, **status_params}),
        ]
        result = self._first_ok(candidates)
        items = []
        if result.get("ok"):
            rows = self._market_items(result)
            for row in rows:
                row_asset = str(self._first_present(row, ("asset", "give_asset", "giveAsset")) or "").strip().upper()
                if a_norm and row_asset and row_asset != a_norm:
                    continue
                normalized = self._normalize_dispenser_row(row, a_norm)
                if open_only and not normalized.get("is_open"):
                    continue
                items.append(normalized)
        return {
            "ok": bool(result.get("ok")),
            "asset": a_norm,
            "count": len(items),
            "items": items,
            "source_path": result.get("path"),
            "errors": result.get("errors") or [],
            "raw": result.get("raw"),
            "open_only": bool(open_only),
            "read_only": True,
        }

    @staticmethod
    def _market_context_cache_ttl_s() -> int:
        try:
            return max(0, min(int(os.getenv("COUNTERPARTY_MARKET_CONTEXT_CACHE_TTL_S") or "45"), 3600))
        except Exception:
            return 45

    @classmethod
    def _market_context_cache_get(cls, key: str) -> Optional[Dict[str, Any]]:
        ttl = cls._market_context_cache_ttl_s()
        if ttl <= 0:
            return None
        row = cls._MARKET_CONTEXT_CACHE.get(key)
        if not isinstance(row, dict):
            return None
        ts = float(row.get("ts") or 0)
        if not ts or time.time() - ts > ttl:
            try:
                cls._MARKET_CONTEXT_CACHE.pop(key, None)
            except Exception:
                pass
            return None
        data = row.get("data")
        if isinstance(data, dict):
            return {**data, "cache_hit": True, "cache_ttl_s": ttl}
        return None

    @classmethod
    def _market_context_cache_put(cls, key: str, data: Dict[str, Any]) -> None:
        ttl = cls._market_context_cache_ttl_s()
        if ttl <= 0 or not isinstance(data, dict):
            return
        try:
            # Keep this intentionally small; market context is UI acceleration,
            # not a durable ledger or trading cache.
            if len(cls._MARKET_CONTEXT_CACHE) > 250:
                oldest = sorted(cls._MARKET_CONTEXT_CACHE.items(), key=lambda kv: float((kv[1] or {}).get("ts") or 0))[:50]
                for k, _v in oldest:
                    cls._MARKET_CONTEXT_CACHE.pop(k, None)
            cls._MARKET_CONTEXT_CACHE[key] = {"ts": time.time(), "data": data}
        except Exception:
            pass

    def get_asset_market_context(self, asset: str, limit: int = 50, open_only: bool = True) -> Dict[str, Any]:
        a_norm = str(asset or "").strip().upper()
        lim = max(1, min(int(limit or 50), 200))
        cache_key = f"{a_norm}|{lim}|open:{1 if open_only else 0}"
        cached = self._market_context_cache_get(cache_key)
        if cached is not None:
            return cached

        orders = self.get_asset_orders(a_norm, limit=lim, open_only=open_only)
        dispensers = self.get_asset_dispensers(a_norm, limit=lim, open_only=open_only)
        order_items = orders.get("items") or []
        dispenser_items = dispensers.get("items") or []
        quotes = self._quote_summary(a_norm, order_items, dispenser_items)
        response = {
            "ok": True,
            "asset": a_norm,
            "orders": order_items,
            "orders_source_path": orders.get("source_path"),
            "orders_errors": orders.get("errors") or [],
            "dispensers": dispenser_items,
            "dispensers_source_path": dispensers.get("source_path"),
            "dispensers_errors": dispensers.get("errors") or [],
            "quotes": quotes,
            "summary": {
                "orders": len(order_items),
                "open_orders": sum(1 for x in order_items if x.get("is_open")),
                "dispensers": len(dispenser_items),
                "open_dispensers": sum(1 for x in dispenser_items if x.get("is_open")),
                "quote_count": len(quotes),
            },
            "open_only": bool(open_only),
            "read_only": True,
            "signing": "not_available_in_this_tranche",
            "compose": "later_unsigned_psbt_or_wallet_flow",
        }
        self._market_context_cache_put(cache_key, response)
        return response
