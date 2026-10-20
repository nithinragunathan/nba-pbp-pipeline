-- The unified fct_shots must not double-count or drop a basket: points per game equal the NBA
-- final score (ESPN's header final where ESPN is the only feed). A made shot that the matcher
-- failed to pair would show up here twice. Reviewed
-- source errors listed in seeds/known_data_issues.csv are excluded.
select s.game_key, sum(case when s.is_made then s.shot_value else 0 end) as points,
       any_value(coalesce(g.nba_final_home + g.nba_final_away, g.espn_final_home + g.espn_final_away)) as final
from {{ scoped(ref('fct_shots')) }} s
join {{ scoped(ref('game_xwalk')) }} g using (game_key)
group by s.game_key
having sum(case when s.is_made then s.shot_value else 0 end) <> any_value(coalesce(g.nba_final_home + g.nba_final_away, g.espn_final_home + g.espn_final_away))
   and s.game_key not in (
       select game_key from {{ ref('known_data_issues') }}
       where test_name = 'assert_unified_points_equal_final_score')
