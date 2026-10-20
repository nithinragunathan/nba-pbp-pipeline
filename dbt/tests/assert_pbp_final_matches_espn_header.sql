{{ config(severity='warn') }}
-- Play-by-play final vs ESPN's game header (its official box-score final). Warn only: a
-- difference is a real data issue to report, not a pipeline bug. Known case: CLE at WAS on
-- 2025-11-07, where both feeds' plays end 114-148 and ESPN's header says 115-148.
select game_key, nba_final_home, nba_final_away, espn_final_home, espn_final_away
from {{ scoped(ref('game_xwalk')) }}
where (nba_final_home <> espn_final_home or nba_final_away <> espn_final_away)
  and game_key not in (
      select game_key from {{ ref('known_data_issues') }}
      where test_name = 'assert_pbp_final_matches_espn_header')
