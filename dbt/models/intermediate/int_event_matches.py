"""Assign every source event a cluster_id (see src/pbp/matching.py for the method).

A Python model because the alignment is a small dynamic program per run of events between
score anchors, which is clear in Python and unreadable in SQL. The SQL models around it do
everything else.
"""


def model(dbt, session):
    import duckdb
    import pandas as pd
    from pbp.matching import match_events

    # dbt finds config keys by scanning for dbt.config.get() calls, and it does not look inside
    # expressions such as `dbt.config.get(...) or default`, so each call stands on its own line.
    max_clock_diff_s = dbt.config.get("match_max_clock_diff_s", 12)
    leagues_csv = dbt.config.get("leagues", "")

    # League-scoped builds (see macros/league_scope.sql): match only the selected leagues' games
    # and carry the other leagues' rows over from the current table.
    leagues = [lg for lg in leagues_csv.split(",") if lg]
    in_scope = "league in (" + ", ".join(f"'{lg}'" for lg in leagues) + ")"
    events = dbt.ref("stg_events")
    if leagues:
        events = events.filter(in_scope)
    events = events.df()
    out = match_events(events, max_clock_diff_s=float(max_clock_diff_s))
    out = out.merge(events[["game_key", "league"]].drop_duplicates(), on="game_key")
    if leagues:
        try:
            kept = session.sql(f"select * from {dbt.this} where not ({in_scope})").df()
        except duckdb.CatalogException:   # first build: nothing to keep
            kept = None
        if kept is not None:
            out = pd.concat([kept[out.columns], out], ignore_index=True)
    return out
