-- Points summed over possessions equal the NBA final score (ESPN's header final where ESPN is the
-- only feed): every basket sits in exactly one
-- possession, except technical free throws shot before a period's first possession, which belong
-- to none and are added separately. Reviewed source errors in seeds/known_data_issues.csv are excluded.
with outside as (
    select game_key, sum(shot_value) as points
    from {{ scoped(ref('fct_shots')) }}
    where possession_id is null and is_made and is_free_throw and coalesce(nba_description, espn_description) ilike '%technical%'
    group by game_key
)

select p.game_key, sum(p.points) as points, any_value(coalesce(o.points, 0)) as outside_points,
       any_value(coalesce(g.nba_final_home + g.nba_final_away, g.espn_final_home + g.espn_final_away)) as final
from {{ scoped(ref('fct_possessions')) }} p
join {{ scoped(ref('game_xwalk')) }} g using (game_key)
left join outside o using (game_key)
where p.game_key not in (
    select game_key from {{ ref('known_data_issues') }}
    where test_name = 'assert_possession_points_equal_final_score')
group by p.game_key
having sum(p.points) + any_value(coalesce(o.points, 0)) <> any_value(coalesce(g.nba_final_home + g.nba_final_away, g.espn_final_home + g.espn_final_away))
