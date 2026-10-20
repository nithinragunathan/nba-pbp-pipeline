"""Load landed files into bronze.raw_payloads (append-only).

A row is added when a game's stat-bearing hash differs from its latest row, so re-running is a
no-op and a late stat correction becomes a new row for the same game. A correction that is later
reverted (A, B, A) is three rows, so the reverted version becomes latest again. dbt reads the
latest row per (source, game_id) in bronze_latest and every version in bronze_payload_history.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from pbp import config

log = logging.getLogger(__name__)

DDL = """
create schema if not exists bronze;
create table if not exists bronze.raw_payloads (
    source          varchar not null,   -- nba | espn
    league          varchar not null,   -- nba | wnba | ncaam | ncaaw
    season          varchar not null,   -- '2025-26'
    game_id         varchar not null,   -- the source's own game id
    payload_format  varchar not null,   -- tells staging how to parse the payload
    extract_mode    varchar not null,   -- live | mirror
    source_url      varchar,
    extracted_at    timestamptz,
    ingested_at     timestamptz not null,
    payload_hash    varchar not null,   -- sha256 of the stat-bearing part of the payload
    payload         json not null
);
"""


def stat_bearing_part(payload_format: str, payload):
    """What the hash covers: every part staging reads, and nothing else. ESPN summaries also
    carry odds, news and standings that change between pulls without any stat changing; hashing
    only plays, box score and the header's scores keeps those from triggering rebuilds."""
    if payload_format == "espn_summary_v2":
        competitors = ((payload.get("header") or {}).get("competitions") or [{}])[0].get("competitors") or []
        return {"plays": payload.get("plays"), "boxscore": payload.get("boxscore"),
                "scores": [(c.get("id"), c.get("homeAway"), c.get("score")) for c in competitors]}
    return payload  # cdn_mirror_csv_rows: the rows themselves


def payload_hash(payload_format: str, payload) -> str:
    canonical = json.dumps(stat_bearing_part(payload_format, payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _partitions(path: Path) -> dict:
    return dict(part.split("=", 1) for part in path.parent.parts if "=" in part)


def load(paths: list[Path] | None = None, db_path: Path = config.DB_PATH) -> dict:
    if paths is None:
        paths = sorted(p for p in config.LANDING_DIR.rglob("*.json") if not p.name.endswith(".meta.json"))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    known = {
        (s, g): h for s, g, h in con.execute(
            """select source, game_id, arg_max(payload_hash, ingested_at)
               from bronze.raw_payloads group by all"""
        ).fetchall()
    }
    now = datetime.now(timezone.utc)
    summary = {"new": [], "changed": [], "unchanged": []}
    for path in paths:
        parts = _partitions(path)
        meta = json.loads(path.with_suffix(".meta.json").read_text())
        text = path.read_text()
        payload = json.loads(text)
        h = payload_hash(meta["payload_format"], payload)
        key = (parts["source"], path.stem)
        if known.get(key) == h:
            summary["unchanged"].append(key)
            continue
        summary["changed" if key in known else "new"].append(key)
        con.execute(
            "insert into bronze.raw_payloads values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [parts["source"], parts["league"], parts["season"], path.stem, meta["payload_format"],
             meta["extract_mode"], meta.get("url"), meta.get("extracted_at"), now, h, text],
        )
    con.close()
    log.info("bronze: %d new, %d changed, %d unchanged",
             len(summary["new"]), len(summary["changed"]), len(summary["unchanged"]))
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print({k: len(v) for k, v in load().items()})
