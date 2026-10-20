"""ESPN extractor: the public site API (live). Saves each game's summary response as-is.

ESPN covers the NBA, WNBA and men's and women's college basketball through the same
endpoint, so `league` is just a path segment here.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from pbp import config, http

log = logging.getLogger(__name__)

BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball"
LEAGUE_PATHS = {
    "nba": "nba",
    "wnba": "wnba",
    "ncaam": "mens-college-basketball",
    "ncaaw": "womens-college-basketball",
}
PAYLOAD_FORMAT = "espn_summary_v2"


def list_team_games(league: str, season: str, team_tricode: str) -> list[dict]:
    """Completed regular-season games for one team, from ESPN's team schedule. For ALL, every
    team's schedule, with each game once."""
    if team_tricode == config.ALL_TEAMS:
        games = {g["espn_game_id"]: g for t in config.league_teams(league)
                 for g in list_team_games(league, season, t)}
        return sorted(games.values(), key=lambda g: g["start_utc"])
    _, end_year = config.season_years(season)
    espn_abbrev = config.team_lookup(team_tricode, league)["espn_abbrev"].lower()
    url = f"{BASE}/{LEAGUE_PATHS[league]}/teams/{espn_abbrev}/schedule?season={end_year}&seasontype=2"
    events = http.get(url).json().get("events", [])
    games = [
        {"espn_game_id": e["id"], "start_utc": e["date"], "name": e["name"]}
        for e in events
        if e["competitions"][0]["status"]["type"].get("completed")
    ]
    log.info("ESPN schedule %s %s %s: %d completed games", league, season, team_tricode, len(games))
    return games


def extract_game(league: str, season: str, espn_game_id: str, *, refresh: bool = False) -> Path | None:
    """Download one game's summary to the landing zone. Skips the call if it is already landed.
    Returns None when ESPN has no summary for a completed game (it answers 404 for a few, e.g.
    two Lynx games of 2026). For the NBA the official feed then carries that game alone; for the
    WNBA, which is ESPN only here, the game is missing and the run logs it."""
    out = config.landing_path("espn", league, season, espn_game_id)
    if out.exists() and not refresh:
        return out
    url = f"{BASE}/{LEAGUE_PATHS[league]}/summary?event={espn_game_id}"
    try:
        resp = http.get(url)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            log.warning("ESPN has no summary for %s %s game %s; skipped", league, season, espn_game_id)
            return None
        raise
    resp.json()  # fail fast on a non-JSON body
    config.write_landed(out, resp.content, {"payload_format": PAYLOAD_FORMAT, "extract_mode": "live", "url": url,
                                            "extracted_at": datetime.now(timezone.utc).isoformat()})
    return out
