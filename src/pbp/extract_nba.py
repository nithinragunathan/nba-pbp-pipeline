"""NBA extractor: the community mirror of cdn.nba.com liveData play-by-play
(github.com/shufinskiy/nba_data). It ships one CSV per season with the liveData action fields
flattened into columns; each game's rows land as one file, with every value kept as the original
string. The pipeline never calls NBA servers itself.

There is deliberately no live mode. cdn.nba.com and stats.nba.com block scripted access from
most networks, and getting past that means disguising requests. In production this extractor is
replaced by the league's licensed feed: a new `payload_format` and a branch in base_nba_actions,
with nothing downstream changing.
"""
from __future__ import annotations

import json
import logging
import shutil
import tarfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from pbp import config, http

log = logging.getLogger(__name__)

MIRROR_URL = "https://raw.githubusercontent.com/shufinskiy/nba_data/main/datasets/cdnnba_{year}.tar.xz"


def nba_game_id(raw: str | int) -> str:
    """The mirror stores 22500086; the API uses the 10-character '0022500086'."""
    return str(int(raw)).zfill(10)


# ---------------------------------------------------------------- mirror

def mirror_csv(season: str, *, refresh: bool = False) -> Path:
    """Download and unpack the season CSV into the cache. The download is reused until a refresh
    finds that the remote file changed (its ETag or Last-Modified differs from the one saved),
    so a refresh picks up a corrected mirror and otherwise costs one HEAD request."""
    year, _ = config.season_years(season)
    url = MIRROR_URL.format(year=year)
    csv_path = config.CACHE_DIR / f"cdnnba_{year}.csv"
    archive = config.CACHE_DIR / f"cdnnba_{year}.tar.xz"
    validator_path = config.CACHE_DIR / f"cdnnba_{year}.validator"
    if refresh and archive.exists():
        head = http.head(url).headers
        remote = head.get("ETag") or head.get("Last-Modified")
        saved = validator_path.read_text() if validator_path.exists() else None
        if remote != saved:
            log.info("NBA mirror for %s changed upstream; downloading it again", season)
            archive.unlink()
            csv_path.unlink(missing_ok=True)
    if csv_path.exists():
        return csv_path
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not archive.exists():
        log.info("downloading NBA mirror for %s", season)
        # Download and unpack through temporary files, so an interrupted run never leaves a
        # truncated archive or CSV that later runs would take as complete.
        tmp_archive = archive.with_name(archive.name + ".tmp")
        with http.get(url, stream=True, timeout=120, retries=3) as resp, open(tmp_archive, "wb") as f:
            shutil.copyfileobj(resp.raw, f)
            validator = resp.headers.get("ETag") or resp.headers.get("Last-Modified")
        tmp_archive.replace(archive)
        if validator:
            validator_path.write_text(validator)
    tmp_csv = csv_path.with_name(csv_path.name + ".tmp")
    with tarfile.open(archive, "r:xz") as tar:
        member = next(m for m in tar.getmembers() if m.name.endswith(".csv") and m.isfile())
        with tar.extractfile(member) as src, open(tmp_csv, "wb") as dst:
            shutil.copyfileobj(src, dst)
    tmp_csv.replace(csv_path)
    return csv_path


def extract_mirror(season: str, team_tricode: str | None = None, game_ids: list[str] | None = None,
                   *, refresh: bool = False) -> list[Path]:
    """Split the season CSV into one landing file per game (a JSON array of row objects). For
    ALL, every regular-season game ('002' ids), to match ESPN's regular-season schedules."""
    csv_path = mirror_csv(season, refresh=refresh)
    con = duckdb.connect()
    rows = con.execute(
        "select * from read_csv(?, all_varchar = true, header = true)", [str(csv_path)]
    ).fetchdf()
    con.close()
    if game_ids:
        wanted = {nba_game_id(g) for g in game_ids}
    elif team_tricode == config.ALL_TEAMS:
        wanted = {g for g in map(nba_game_id, rows["gameId"].unique()) if g.startswith("002")}
    else:
        wanted = {nba_game_id(g) for g in rows.loc[rows["teamTricode"] == team_tricode, "gameId"].unique()}
    rows["_gid"] = rows["gameId"].map(nba_game_id)
    rows = rows[rows["_gid"].isin(wanted)]

    by_game: dict[str, list[dict]] = defaultdict(list)
    columns = [c for c in rows.columns if c != "_gid"]
    rows = rows.astype(object).where(rows.notna(), None)  # NaN -> null
    for d in rows.to_dict("records"):
        by_game[d["_gid"]].append({c: d[c] for c in columns})

    paths = []
    for gid, actions in sorted(by_game.items()):
        out = config.landing_path("nba", "nba", season, gid)
        if out.exists() and not refresh:
            paths.append(out)
            continue
        config.write_landed(out, json.dumps(actions, ensure_ascii=False),
                            _meta("cdn_mirror_csv_rows", "mirror", MIRROR_URL.format(year=config.season_years(season)[0])))
        paths.append(out)
    log.info("NBA mirror %s: %d games landed", season, len(paths))
    return paths


def _meta(payload_format: str, mode: str, url: str) -> dict:
    return {"payload_format": payload_format, "extract_mode": mode, "url": url,
            "extracted_at": datetime.now(timezone.utc).isoformat()}
