from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List


class XClient:
    """Deterministic, offline X API client.

    This lightweight client mimics a subset of Twitter's v2 endpoints so that the
    CLI can be exercised without real network access. Replace it with a genuine
    implementation once credentials and HTTP plumbing are ready.
    """

    _USERS: List[Dict[str, Any]] = [
        {
            "id": "1001",
            "username": "rizin_fan",
            "name": "RIZIN Fan",
            "public_metrics": {"followers_count": 1540, "tweet_count": 3200},
        },
        {
            "id": "1002",
            "username": "ufc_addict",
            "name": "UFC Addict",
            "public_metrics": {"followers_count": 2890, "tweet_count": 5400},
        },
        {
            "id": "1003",
            "username": "combat_journal",
            "name": "Combat Journal",
            "public_metrics": {"followers_count": 870, "tweet_count": 1220},
        },
        {
            "id": "1004",
            "username": "mma_polyglot",
            "name": "MMA Polyglot",
            "public_metrics": {"followers_count": 640, "tweet_count": 980},
        },
    ]

    def counts_recent(self, query: str, granularity: str = "hour") -> Dict[str, Any]:
        granularity = granularity or "hour"
        if granularity not in {"hour", "day"}:
            raise ValueError("granularity must be 'hour' or 'day'")

        steps = 24 if granularity == "hour" else 7
        step_delta = timedelta(hours=1 if granularity == "hour" else 24)
        anchor = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        data: List[Dict[str, Any]] = []
        total = 0
        for i in range(steps):
            start = anchor - step_delta * (steps - i)
            end = start + step_delta
            tweet_count = 12 + ((i * 3) % 9)
            total += tweet_count
            data.append(
                {
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "tweet_count": tweet_count,
                }
            )

        return {
            "data": data,
            "meta": {
                "query": query,
                "granularity": granularity,
                "total_tweet_count": total,
            },
        }

    def search_recent(self, query: str, max_results: int = 10) -> Dict[str, Any]:
        n = max(1, min(max_results or 10, 100))
        now = datetime.now(timezone.utc)

        tweets: List[Dict[str, Any]] = []
        included_user_ids: set[str] = set()

        for idx in range(n):
            user = self._USERS[idx % len(self._USERS)]
            included_user_ids.add(user["id"])
            tweet_id = f"{int(now.timestamp()) - idx}"
            created = now - timedelta(minutes=idx * 3)
            tweets.append(
                {
                    "id": tweet_id,
                    "author_id": user["id"],
                    "created_at": created.isoformat(),
                    "lang": ["en", "es", "pt", "fr", "ja"][idx % 5],
                    "text": (
                        f"[{idx+1}] Sample post referencing query {query[:60]}"
                        " (RIZIN vs UFC)."
                    ),
                    "public_metrics": {
                        "retweet_count": (idx * 2) % 7,
                        "reply_count": (idx * 3) % 5,
                        "like_count": 5 + (idx * 4) % 20,
                        "quote_count": idx % 3,
                    },
                    "conversation_id": tweet_id,
                }
            )

        includes = {
            "users": [user for user in self._USERS if user["id"] in included_user_ids]
        }

        return {
            "data": tweets,
            "includes": includes,
            "meta": {
                "result_count": len(tweets),
                "newest_id": tweets[0]["id"] if tweets else None,
                "oldest_id": tweets[-1]["id"] if tweets else None,
                "query": query,
            },
        }
