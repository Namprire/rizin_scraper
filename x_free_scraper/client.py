from __future__ import annotations

from typing import Any, Dict


class XClient:
    """Placeholder X API client.

    This stub exists so the CLI module can be imported without raising
    ImportError during early development stages. Replace its methods with
    real implementations once API wiring is ready.
    """

    def counts_recent(self, query: str, granularity: str = "hour") -> Dict[str, Any]:
        raise NotImplementedError("counts_recent must be implemented")

    def search_recent(self, query: str, max_results: int = 10) -> Dict[str, Any]:
        raise NotImplementedError("search_recent must be implemented")
