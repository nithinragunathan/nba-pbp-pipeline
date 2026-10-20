-- Per game, team and attempt type, NBA and ESPN attempt counts differ by at most
-- var('shot_count_tolerance'). Reviewed source errors listed in seeds/known_data_issues.csv are excluded.
with counts as (
    select game_key, team_key, event_class,
           count(*) filter (where source = 'nba')  as n_nba,
           count(*) filter (where source = 'espn') as n_espn
    from {{ scoped(ref('stg_events')) }}
    where event_class in ('shot', 'free_throw')
      and game_key in (select game_key from {{ scoped(ref('game_xwalk')) }}
                       where nba_game_id is not null and espn_game_id is not null)
    group by all
)

select * from counts
where abs(n_nba - n_espn) > {{ var('shot_count_tolerance') }}
  and game_key not in (
      select game_key from {{ ref('known_data_issues') }}
      where test_name = 'assert_shot_counts_within_tolerance')
