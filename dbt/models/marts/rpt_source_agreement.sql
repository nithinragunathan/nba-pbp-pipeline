-- How well NBA and ESPN agree, per game and event class, plus an 'all' row per game.
-- match_rate_* = share of that source's events that found a partner in the other source.
with c as (
    select * from {{ scoped(ref('int_clusters')) }}
    -- Only games both feeds cover: in a single-source game (every WNBA game here, the NBA Cup
    -- final) every event is single-source by definition, so there is nothing to compare.
    where game_key in (select game_key from {{ scoped(ref('game_xwalk')) }}
                       where nba_game_id is not null and espn_game_id is not null)
)

select
    league,
    game_key,
    'nba'                                                     as source_a,
    'espn'                                                    as source_b,
    coalesce(event_class, 'all')                              as event_class,
    count(nba_event_id)                                       as n_events_a,
    count(espn_event_id)                                      as n_events_b,
    count(*) filter (where n_sources = 2)                     as n_matched,
    round(count(*) filter (where n_sources = 2) / nullif(count(nba_event_id), 0), 4)  as match_rate_a,
    round(count(*) filter (where n_sources = 2) / nullif(count(espn_event_id), 0), 4) as match_rate_b,
    count(*) filter (where match_method = 'score_anchor')     as n_score_anchored,
    count(*) filter (where match_method = 'seq_align')        as n_seq_aligned,
    round(avg(abs(clock_diff)) / 10.0, 3)                     as avg_abs_clock_diff_s,
    count(*) filter (where nba_event_id is not null and espn_event_id is null) as n_only_a,
    count(*) filter (where espn_event_id is not null and nba_event_id is null) as n_only_b,
    count(*) filter (where n_sources = 2 and nba_player_key <> espn_player_key) as n_player_disagree,
    count(*) filter (where n_sources = 2 and nba_is_made <> espn_is_made)       as n_result_disagree,
    count(*) filter (where n_sources = 2 and nba_shot_value <> espn_shot_value) as n_value_disagree
from c
group by grouping sets ((league, game_key, event_class), (league, game_key))
