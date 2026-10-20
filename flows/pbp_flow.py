"""Prefect flow: extract every league -> load bronze -> dbt build -> reconciliation report.

    python flows/pbp_flow.py                              # every 2025-26 NBA game and 2026 WNBA game (ESPN only)
    python flows/pbp_flow.py --target nba:2025-26:ALL     # one league only
    python flows/pbp_flow.py --target nba:2025-26:WAS --target wnba:2026:WAS   # one team per league
    python flows/pbp_flow.py --target nba:2025-26:WAS --games 401809939 401809951

A target is league:season:team, where team ALL means the whole league. Each target is extracted
in its own subflow; loading, the dbt build and the report run once afterwards. The build covers only the leagues the run's targets
touched (a league-scoped build, see dbt/macros/league_scope.sql), or everything when the targets
cover every league in bronze.

A game that fails to extract (after its retries) does not stop the run: everything that landed is
loaded, built and reported, and the flow then fails with the list of what did not land.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone

import duckdb
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import exponential_backoff

from pbp import config, extract_espn, extract_nba, http, load_bronze, report

DEFAULT_TARGETS = ["nba:2025-26:ALL", "wnba:2026:ALL"]


def _retryable(task, task_run, state) -> bool:
    """Retry only failures another attempt can fix: timeouts, dropped connections, 5xx, 429."""
    try:
        state.result()
    except Exception as exc:
        return http.is_retryable(exc)
    return False


# The one retry policy for every extract task. Retries live here rather than in the HTTP helper
# so each attempt shows in Prefect; jitter keeps parallel workers from retrying in step.
RETRY = dict(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=5),
             retry_jitter_factor=0.5, retry_condition_fn=_retryable)


@task(**RETRY)
def list_espn_games(league: str, season: str, team: str) -> list[dict]:
    return extract_espn.list_team_games(league, season, team)


@task(**RETRY, tags=["espn-api"])
def extract_espn_game(league: str, season: str, espn_game_id: str, refresh: bool) -> str | None:
    path = extract_espn.extract_game(league, season, espn_game_id, refresh=refresh)
    return str(path) if path else None


@task(**RETRY)
def extract_nba_season(season: str, team: str, refresh: bool) -> int:
    """NBA Stats, from the community mirror. It ships one CSV per season, so this is one task, not
    one per game."""
    return len(extract_nba.extract_mirror(season, team, refresh=refresh))


@task
def load_bronze_task() -> dict:
    return load_bronze.load()


def build_scope(leagues: set[str]) -> list[str] | None:
    """The leagues to rebuild, or None for a full build (the run touched every loaded league)."""
    with duckdb.connect(str(config.DB_PATH), read_only=True) as con:
        loaded = {r[0] for r in con.execute("select distinct league from bronze.raw_payloads").fetchall()}
    return None if leagues >= loaded else sorted(leagues)


@task
def dbt_build(leagues: list[str] | None = None) -> None:
    """dbt build; with `leagues`, only those leagues' rows are rebuilt and tested."""
    logger = get_run_logger()
    dbt = str(config.REPO_ROOT / ".venv" / "bin" / "dbt")
    args = ["build", *(["--vars", json.dumps({"leagues": leagues})] if leagues else [])]
    proc = subprocess.run([dbt, "--no-use-colors", *args, "--profiles-dir", "."], cwd=config.DBT_DIR,
                          capture_output=True, text=True)
    tail = "\n".join(proc.stdout.splitlines()[-15:])
    logger.info("dbt %s\n%s", " ".join(args), tail)
    if proc.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} failed:\n{tail}\n{proc.stderr[-2000:]}")


@task
def reconciliation_report(bronze_summary: dict) -> str:
    md = report.build(bronze_summary=bronze_summary)
    path = report.write(report.build(run_details=False))
    create_markdown_artifact(markdown=md, key="pbp-reconciliation", description="NBA feed vs ESPN reconciliation")
    return str(path)


def _is_recent(start_utc: str, days: int) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return bool(days) and datetime.fromisoformat(start_utc.replace("Z", "+00:00")) >= cutoff


@flow(name="pbp-extract-league", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=4))
def extract_league(league: str, season: str, team: str, espn_game_ids: list[str] | None = None,
                   refresh_recent_days: int = 0) -> dict:
    """Land one team-season from ESPN and, for the NBA, the league's official feed (NBA Stats).
    The WNBA is ESPN only (see source_coverage.csv). Games played within refresh_recent_days are
    re-pulled even if already landed, so a late stat correction produces a new bronze version."""
    espn_games = list_espn_games(league, season, team)
    if espn_game_ids:
        espn_games = [g for g in espn_games if g["espn_game_id"] in set(espn_game_ids)]
    espn = [(g["espn_game_id"], extract_espn_game.submit(league, season, g["espn_game_id"],
                                                          _is_recent(g["start_utc"], refresh_recent_days)))
            for g in espn_games]

    official = []
    if league == "nba":
        refresh = any(_is_recent(g["start_utc"], refresh_recent_days) for g in espn_games)
        official = [(f"season {season}", extract_nba_season.submit(season, team, refresh))]

    # Wait for every game; a failed one is recorded, not raised, so the rest still get loaded.
    counts, failed = {"espn": 0, "official": 0}, []
    for kind, futures in (("espn", espn), ("official", official)):
        for game_id, future in futures:
            future.wait()
            if future.state.is_failed():
                failed.append(f"{league} {kind} {game_id}: {future.state.message}")
                continue
            landed = future.result()
            counts[kind] += landed if isinstance(landed, int) else int(landed is not None)
    official_note = f", official {counts['official']} games" if league == "nba" else " (ESPN only)"
    print(f"{league} {season} {team}: ESPN {counts['espn']} games{official_note} landed"
          + (f", {len(failed)} failed" if failed else ""))
    return {"counts": counts, "failed": failed}


@flow(name="pbp-unify", log_prints=True)
def pbp_flow(targets: list[str] = DEFAULT_TARGETS, espn_game_ids: list[str] | None = None,
             refresh_recent_days: int = 0) -> str:
    leagues, failed = set(), []
    for target in targets:
        league, season, team = target.split(":")
        state = extract_league(league, season, team, espn_game_ids, refresh_recent_days,
                               return_state=True)
        if state.is_failed():   # e.g. the schedule itself could not be listed
            failed.append(f"{target}: {state.message}")
        else:
            failed += state.result()["failed"]
        leagues.add(league)

    summary = load_bronze_task()
    print({k: len(v) for k, v in summary.items()})
    scope = build_scope(leagues)
    print(f"dbt build: {'leagues ' + ', '.join(scope) if scope else 'all leagues'}")
    dbt_build(scope)
    report_path = reconciliation_report(summary)
    if failed:
        raise RuntimeError(f"{len(failed)} extraction(s) failed; everything else was loaded and built:\n"
                           + "\n".join(failed))
    return report_path


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", action="append", dest="targets", metavar="LEAGUE:SEASON:TEAM",
                    help=f"repeatable; season '2025-26' for the NBA, '2026' for the WNBA, team ALL for every team (default: {' '.join(DEFAULT_TARGETS)})")
    ap.add_argument("--games", nargs="*", help="ESPN game ids to fetch (default: the target's whole regular season). "
                    "Filters ESPN only: the NBA mirror still lands every game of the target")
    ap.add_argument("--refresh-recent-days", type=int, default=0)
    a = ap.parse_args()
    print(pbp_flow(a.targets or DEFAULT_TARGETS, a.games, a.refresh_recent_days))
