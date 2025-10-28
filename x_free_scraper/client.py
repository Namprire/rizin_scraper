from __future__ import annotations

import os
import logging
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class XClientError(RuntimeError):
    """Raised when the X API returns an error or the client is misconfigured."""


class XClient:
    """Thin wrapper around X (Twitter) v2 REST endpoints used by the scraper."""

    BASE_URL = "https://api.twitter.com/2"

    def __init__(
        self,
        bearer_token: Optional[str] = None,
        session: Optional[requests.Session] = None,
        timeout: int = 30,
        retries: int = 3,
        backoff_factor: float = 1.5,
    ) -> None:
        load_dotenv()
        token = bearer_token or os.getenv("TW_BEARER")
        if not token:
            raise XClientError(
                "TW_BEARER environment variable is required to call the X API."
            )

        self._logger = logging.getLogger("x_free_scraper.client")
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
            )
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)

        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "User-Agent": "x_free_scraper/1.0",
            }
        )

        retry_config = Retry(
            total=max(retries, 0),
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_config)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._timeout = timeout

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        self._logger.debug("GET %s params=%s", url, params)
        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            self._logger.error("Network error %s: %s", url, exc)
            raise XClientError(f"Network error calling X API: {exc}") from exc

        if response.status_code != 200:
            hints: Dict[str, Any] = {}
            retry_after = response.headers.get("retry-after")
            if retry_after:
                hints["retry_after_s"] = retry_after
            reset_hint = response.headers.get("x-rate-limit-reset")
            if reset_hint:
                with suppress(ValueError):
                    reset_ts = int(reset_hint)
                    hints["reset_utc"] = datetime.fromtimestamp(
                        reset_ts, timezone.utc
                    ).isoformat()

            error_payload: Any
            with suppress(ValueError):
                error_payload = response.json()
                self._logger.warning(
                    "X API non-200 %s -> %s payload=%s hints=%s",
                    url,
                    response.status_code,
                    error_payload,
                    hints or None,
                )
                message = f"X API error {response.status_code}: {error_payload}"
                if hints:
                    message += f" (hints: {hints})"
                raise XClientError(message)
            self._logger.warning(
                "X API non-200 %s -> %s text=%s hints=%s",
                url,
                response.status_code,
                response.text.strip(),
                hints or None,
            )
            message = f"X API error {response.status_code}: {response.text.strip()}"
            if hints:
                message += f" (hints: {hints})"
            raise XClientError(message)

        self._logger.info("X API %s -> %s", url, response.status_code)
        return response.json()

    def counts_recent(self, query: str, granularity: str = "hour") -> Dict[str, Any]:
        granularity = granularity or "hour"
        if granularity not in {"hour", "day"}:
            raise XClientError("granularity must be 'hour' or 'day'")

        params = {
            "query": query,
            "granularity": granularity,
        }
        return self._request("/tweets/counts/recent", params)

    def search_recent(self, query: str, max_results: int = 10) -> Dict[str, Any]:
        bounded_results = max(10, min(max_results or 10, 100))
        params = {
            "query": query,
            "max_results": bounded_results,
            "expansions": "author_id",
            "tweet.fields": "created_at,lang,public_metrics,conversation_id",
            "user.fields": "username,public_metrics,name",
        }
        return self._request("/tweets/search/recent", params)
