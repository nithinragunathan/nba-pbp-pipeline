"""Reconciliation report: what the run loaded and how well the sources agree."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from pbp import config


def _md_table(df) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |")
    return "\n".join(lines)


def build(db_path: Path = config.DB_PATH, bronze_summary: dict | None = None, run_details: bool = True) -> str:
    """The report as markdown. `run_details` adds what changes on every run (time, bronze load
    counts): wanted in the Prefect artifact, left out of the committed copy so it only changes
    when the data does."""
    con = duckdb.connect(str(db_path), read_only=True)
    q = lambda sql: con.execute(sql).fetchdf()

    games = q("""select league, count(*) as games, count(nba_game_id) as official, count(espn_game_id) as espn
                 from crosswalks.game_xwalk group by league order by league""")
    by_class = q("""
        select g.league, event_class,
               sum(n_events_a) as official_events, sum(n_events_b) as espn_events, sum(n_matched) as matched,
               round(sum(n_matched) / sum(n_events_a), 4) as official_match_rate,
               round(sum(n_matched) / sum(n_events_b), 4) as espn_match_rate,
               sum(n_only_a) as official_only, sum(n_only_b) as espn_only,
               sum(n_player_disagree) as player_disagree, sum(n_result_disagree) as result_disagree
        from marts.rpt_source_agreement a
        join crosswalks.game_xwalk g using (game_key)
        group by g.league, event_class
        order by g.league, event_class = 'all', event_class""")
    worst = q("""
        select game_key, n_only_a as official_only, n_only_b as espn_only,
               n_player_disagree as player_disagree, n_result_disagree as result_disagree
        from marts.rpt_source_agreement
        -- Games only one feed covers are all single-source by definition, so they would top the list.
        where game_key in (select game_key from crosswalks.game_xwalk
                           where nba_game_id is not null and espn_game_id is not null)
          and event_class = 'all' and (n_only_a + n_only_b + n_player_disagree + n_result_disagree) > 0
        order by n_only_a + n_only_b + n_player_disagree + n_result_disagree desc, game_key
        limit 10""")
    issues = q("select issue_id, source, game_key, test_name, note from ref.known_data_issues")
    con.close()

    parts = ["# Reconciliation report\n"]
    if run_details:
        parts.append(f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC.\n")
    parts.append("## Games\n\n" + _md_table(games))
    if run_details and bronze_summary:
        parts.append("## Bronze load\n\n" + "\n".join(f"- {k}: {len(v)}" for k, v in bronze_summary.items()))
    parts += [
        "## Official feed vs ESPN agreement by league and event class\n\n"
        "The official feed is NBA Stats (NBA only). The WNBA is ESPN only, so every WNBA event is single-source.\n\n" + _md_table(by_class),
        "## Games with the most disagreements\n\n" + _md_table(worst),
        "## Known source issues (reviewed, excluded from tests)\n\n" + _md_table(issues),
    ]
    return "\n\n".join(parts) + "\n"


def write(markdown: str, path: Path = config.REPORTS_DIR / "reconciliation.md") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown)
    return path


if __name__ == "__main__":
    print(write(build(run_details=False)))
