"""Paths and season conventions shared by the extractors, loader and flow."""
from __future__ import annotations

import csv
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"        # downloaded mirror archives
LANDING_DIR = DATA_DIR / "landing"    # one raw file per (source, game), hive-style folders
DB_PATH = DATA_DIR / "pbp.duckdb"
DBT_DIR = REPO_ROOT / "dbt"
REPORTS_DIR = REPO_ROOT / "reports"
DIM_TEAM_SEED = DBT_DIR / "seeds" / "dim_team.csv"
ALL_TEAMS = "ALL"                     # team code for a league-wide target, e.g. nba:2025-26:ALL


def season_years(season: str) -> tuple[int, int]:
    """'2025-26' -> (2025, 2026). NBA mirrors key on the start year, ESPN on the end year.
    WNBA seasons sit inside one calendar year: '2026' -> (2026, 2026)."""
    start = int(season.split("-")[0])
    return (start, start + 1) if "-" in season else (start, start)


def landing_path(source: str, league: str, season: str, game_id: str) -> Path:
    return LANDING_DIR / f"source={source}" / f"league={league}" / f"season={season}" / f"{game_id}.json"


def write_landed(out: Path, payload: bytes | str, meta: dict) -> None:
    """Write a landing file and its .meta.json sidecar so an interrupted run never leaves a file
    that looks landed: the sidecar first, then the payload via a temporary file and a rename. An
    existing payload file therefore always has a sidecar and is never truncated."""
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".meta.json").write_text(json.dumps(meta))
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_bytes(payload.encode() if isinstance(payload, str) else payload)
    tmp.replace(out)


def team_lookup(nba_tricode: str, league: str = "nba") -> dict:
    """Row of the dim_team seed for a league tricode such as 'WAS' (Wizards in the NBA,
    Mystics in the WNBA)."""
    with open(DIM_TEAM_SEED) as f:
        for row in csv.DictReader(f):
            if row["nba_tricode"] == nba_tricode and row["league"] == league:
                return row
    raise KeyError(f"unknown {league} team {nba_tricode!r}; see {DIM_TEAM_SEED}")


def league_teams(league: str) -> list[str]:
    """Every tricode of a league in the dim_team seed."""
    with open(DIM_TEAM_SEED) as f:
        return [row["nba_tricode"] for row in csv.DictReader(f) if row["league"] == league]
