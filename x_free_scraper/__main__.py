from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from .client import XClient, XClientError
from .io_utils import (
    CLEAN_DIR,
    RAW_DIR,
    bump_monthly_count,
    ensure_dirs,
    guard_counts_rate,
    guard_monthly_quota,
    guard_search_rate,
    mark_counts_called,
    mark_search_called,
    normalize_search_json,
    quick_summary,
    read_state,
    read_yaml,
    save_jsonl,
    write_clean_csv,
    write_state,
)

PROJECT_ROOT = Path(__file__).resolve().parent


def cmd_status(_args: argparse.Namespace) -> int:
    state = read_state()
    print("month:", state["month"])
    print("monthly_count:", state["monthly_count"], "/ 100")
    print("last_counts_ts:", state["last_counts_ts"])
    print("last_search_ts:", state["last_search_ts"])
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    state = read_state()

    if args.what == "monthly":
        state["monthly_count"] = 0
    elif args.what == "all":
        state = {
            "month": datetime.now(timezone.utc).strftime("%Y-%m"),
            "monthly_count": 0,
            "last_counts_ts": None,
            "last_search_ts": None,
        }

    write_state(state)
    print("Reset done:", args.what)
    return 0


def read_query(query_key: str) -> str:
    query_path = PROJECT_ROOT / "queries.yaml"
    if not query_path.exists():
        # Support repository layouts where queries.yaml lives beside the package
        fallback = PROJECT_ROOT.parent / "queries.yaml"
        if fallback.exists():
            query_path = fallback
    if not query_path.exists():
        raise SystemExit("queries.yaml not found. Expected it in package or project root.")

    queries = read_yaml(query_path)

    if query_key not in queries:
        raise SystemExit(f"query-key '{query_key}' not found in queries.yaml")

    query = " ".join(queries[query_key].split())
    if len(query) > 512:
        raise SystemExit(f"query too long ({len(query)} chars). Free plan allows ≤512.")

    return query


def cmd_scout(args: argparse.Namespace) -> int:
    ok, message = guard_counts_rate()
    if not ok:
        print(message)
        return 2

    query = read_query(args.query_key)
    client = XClient()
    print(f"SCOUT '{args.query_key}'...")
    try:
        response = client.counts_recent(query=query, granularity=args.granularity)
    except XClientError as exc:
        print(f"Error: {exc}")
        return 4
    mark_counts_called()

    payload = {
        "query_key": args.query_key,
        "query": query,
        "granularity": args.granularity,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "response": response,
    }

    timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    raw_path = RAW_DIR / f"counts_{args.query_key}_{timestamp_tag}.jsonl"
    save_jsonl([payload], raw_path)

    total = sum(bucket.get("tweet_count", 0) for bucket in response.get("data", []))
    last_24 = sum(
        bucket.get("tweet_count", 0) for bucket in response.get("data", [])[-24:]
    )
    print(f"7d_total={total} | last24h={last_24} | saved={raw_path}")
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    expected = max(10, min(args.max_results, 100))

    ok, message = guard_monthly_quota(expected)
    if not ok:
        print(message)
        return 3

    ok, message = guard_search_rate()
    if not ok:
        print(message)
        return 2

    query = read_query(args.query_key)
    client = XClient()
    print(f"FETCH '{args.query_key}' (max_results={expected})...")
    try:
        response = client.search_recent(query=query, max_results=expected)
    except XClientError as exc:
        print(f"Error: {exc}")
        return 4
    mark_search_called()

    timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    raw_path = RAW_DIR / f"fetch_{args.query_key}_{timestamp_tag}.jsonl"
    save_jsonl([response], raw_path)

    load_dotenv()
    salt = os.getenv("PROJECT_SALT", "rizin-ufc-2025")
    rows = normalize_search_json(response, args.query_key, args.anonymize, salt)

    bump_monthly_count(len(rows))

    clean_path = CLEAN_DIR / f"fetch_{args.query_key}_{timestamp_tag}.csv"
    write_clean_csv(rows, clean_path)

    print(quick_summary(rows))
    print("raw:", raw_path)
    print("csv:", clean_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="x_free_scraper", description="X API Free-plan scout & fetch"
    )
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    status_parser = subparsers.add_parser("status", help="show quota & timestamps")
    status_parser.set_defaults(func=cmd_status)

    reset_parser = subparsers.add_parser("reset", help="reset counters")
    reset_parser.add_argument("--what", choices=["monthly", "all"], default="monthly")
    reset_parser.set_defaults(func=cmd_reset)

    scout_parser = subparsers.add_parser("scout", help="counts for one query-key")
    scout_parser.add_argument("--query-key", required=True)
    scout_parser.add_argument("--granularity", choices=["hour", "day"], default="hour")
    scout_parser.set_defaults(func=cmd_scout)

    fetch_parser = subparsers.add_parser("fetch", help="recent search for one query-key")
    fetch_parser.add_argument("--query-key", required=True)
    fetch_parser.add_argument("--max-results", type=int, default=10)
    fetch_parser.add_argument("--anonymize", action="store_true")
    fetch_parser.set_defaults(func=cmd_fetch)

    return parser


def main(argv: list[str] | None = None) -> int:
    ensure_dirs()
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
