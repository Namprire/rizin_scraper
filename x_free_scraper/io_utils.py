from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
STATE_PATH = PROJECT_ROOT / "state.json"

FIFTEEN_MIN = 15 * 60


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_yaml(path: Path) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _default_state() -> Dict[str, Any]:
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    return {
        "month": month,
        "monthly_count": 0,
        "last_counts_ts": None,
        "last_search_ts": None,
    }


def read_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        write_state(_default_state())
    st = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    cur_month = datetime.now(timezone.utc).strftime("%Y-%m")
    if st.get("month") != cur_month:
        st["month"] = cur_month
        st["monthly_count"] = 0
        write_state(st)
    return st


def write_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def seconds_since(ts_iso: str | None) -> int:
    if not ts_iso:
        return 10**9
    past = datetime.fromisoformat(ts_iso)
    delta = (datetime.now(timezone.utc) - past).total_seconds()
    return int(delta)


def guard_counts_rate() -> Tuple[bool, str]:
    st = read_state()
    elapsed = seconds_since(st.get("last_counts_ts"))
    if elapsed < FIFTEEN_MIN:
        return False, f"counts rate limit: {elapsed}s since last, need ≥900s"
    return True, "ok"


def guard_search_rate() -> Tuple[bool, str]:
    st = read_state()
    elapsed = seconds_since(st.get("last_search_ts"))
    if elapsed < FIFTEEN_MIN:
        return False, f"search rate limit: {elapsed}s since last, need ≥900s"
    return True, "ok"


def guard_monthly_quota(request_n: int) -> Tuple[bool, str]:
    st = read_state()
    used = st.get("monthly_count", 0)
    if used + request_n > 100:
        return (
            False,
            f"quota would be exceeded: used={used}, request={request_n}, limit=100",
        )
    return True, "ok"


def bump_monthly_count(n: int) -> None:
    st = read_state()
    st["monthly_count"] = int(st.get("monthly_count", 0)) + int(n)
    write_state(st)


def mark_counts_called() -> None:
    st = read_state()
    st["last_counts_ts"] = now_utc_iso()
    write_state(st)


def mark_search_called() -> None:
    st = read_state()
    st["last_search_ts"] = now_utc_iso()
    write_state(st)


def save_jsonl(records: List[Dict[str, Any]], path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _tokenize(text: str) -> List[str]:
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"[@#]\S+", " ", text)
    tokens = re.findall(r"[A-Za-zÀ-ÿ一-龥ぁ-ゔァ-ヴー0-9']+", text.lower())
    return tokens


def top_bigrams(texts: List[str], k: int = 5) -> List[Tuple[str, int]]:
    from collections import Counter

    counter: Counter[Tuple[str, str]] = Counter()
    for text in texts:
        tokens = _tokenize(text)
        for i in range(len(tokens) - 1):
            counter[(tokens[i], tokens[i + 1])] += 1
    return [(" ".join(bg), count) for bg, count in counter.most_common(k)]


def sha_id(s: str, salt: str) -> str:
    return hashlib.sha256((s + salt).encode("utf-8")).hexdigest()


def normalize_search_json(
    resp: Dict[str, Any],
    query_key: str,
    anonymize: bool,
    salt: str,
) -> List[Dict[str, Any]]:
    data = resp.get("data", []) or []
    includes = resp.get("includes", {}) or {}
    users = {user["id"]: user for user in includes.get("users", []) or []}
    rows: List[Dict[str, Any]] = []
    fetched_at = now_utc_iso()

    for tweet in data:
        author_id = tweet.get("author_id")
        user = users.get(author_id, {})
        public_metrics = tweet.get("public_metrics", {}) or {}
        user_metrics = user.get("public_metrics", {}) or {}

        tweet_id = tweet.get("id")
        username = user.get("username")
        display_name = user.get("name")

        retweets = public_metrics.get("retweet_count") or 0
        replies = public_metrics.get("reply_count") or 0
        likes = public_metrics.get("like_count") or 0
        quotes = public_metrics.get("quote_count") or 0
        engagement_total = retweets + replies + likes + quotes

        row: Dict[str, Any] = {
            "tweet_url": (
                f"https://x.com/{username}/status/{tweet_id}"
                if username and tweet_id
                else (f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None)
            ),
            "tweet_id": tweet_id,
            "posted_at_utc": tweet.get("created_at"),
            "tweet_language": tweet.get("lang"),
            "tweet_text": tweet.get("text"),
            "author_id": author_id,
            "author_username": username,
            "author_display_name": display_name,
            "author_followers": user_metrics.get("followers_count"),
            "engagement_likes": likes,
            "engagement_retweets": retweets,
            "engagement_replies": replies,
            "engagement_quotes": quotes,
            "engagement_total": engagement_total,
            "conversation_id": tweet.get("conversation_id"),
            "query_key": query_key,
            "fetched_at_utc": fetched_at,
        }

        if anonymize:
            row["author_id"] = sha_id(str(author_id or ""), salt)
            row["author_username"] = None
            row["author_display_name"] = None
            if tweet_id:
                row["tweet_url"] = f"https://x.com/i/web/status/{tweet_id}"

        rows.append(row)

    return rows


def write_clean_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    df = pd.DataFrame(rows)
    if not df.empty:
        sort_col = "posted_at_utc" if "posted_at_utc" in df.columns else None
        if sort_col:
            df = df.sort_values(by=sort_col, ascending=False, na_position="last")

    preferred_order = [
        "tweet_url",
        "tweet_id",
        "posted_at_utc",
        "tweet_language",
        "tweet_text",
        "author_username",
        "author_display_name",
        "author_id",
        "author_followers",
        "engagement_likes",
        "engagement_retweets",
        "engagement_replies",
        "engagement_quotes",
        "engagement_total",
        "conversation_id",
        "query_key",
        "fetched_at_utc",
    ]

    available_columns = [col for col in preferred_order if col in df.columns]
    remaining_columns = [col for col in df.columns if col not in available_columns]
    df = df.reindex(columns=available_columns + remaining_columns)

    df.to_csv(path, index=False, encoding="utf-8")


def _mean_or_zero(series: pd.Series) -> float:
    cleaned = series.dropna()
    if cleaned.empty:
        return 0.0
    value = cleaned.mean()
    return float(value) if pd.notna(value) else 0.0


def quick_summary(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "No records."

    df = pd.DataFrame(rows)
    lang_col = "tweet_language" if "tweet_language" in df else "lang"
    lang_dist = df[lang_col].value_counts(dropna=False).to_dict() if lang_col in df else {}

    likes_series = (
        df.get("engagement_likes")
        if "engagement_likes" in df
        else df.get("likes", pd.Series(dtype=float))
    )
    retweets_series = (
        df.get("engagement_retweets")
        if "engagement_retweets" in df
        else df.get("retweets", pd.Series(dtype=float))
    )

    avg_likes = _mean_or_zero(likes_series if likes_series is not None else pd.Series(dtype=float))
    avg_retweets = _mean_or_zero(
        retweets_series if retweets_series is not None else pd.Series(dtype=float)
    )

    text_col = "tweet_text" if "tweet_text" in df else "text"
    texts = df.get(text_col, pd.Series(dtype=str)).fillna("").tolist()
    bigrams = top_bigrams(texts, k=5)

    return (
        "records={n} | lang={lang} | avg_likes={likes:.2f} | "
        "avg_retweets={retweets:.2f} | top_bigrams={bigrams}".format(
            n=len(df),
            lang=lang_dist,
            likes=avg_likes,
            retweets=avg_retweets,
            bigrams=bigrams,
        )
    )
