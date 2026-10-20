-- For each source and game, points from made shots and free throws equal that source's own
-- final score, team by team. Catches dropped events and wrong shot values in staging.
-- Reviewed source errors listed in seeds/known_data_issues.csv are excluded.
with points as (
    select e.source, e.game_key, e.team_key, sum(e.shot_value) as points
    from {{ scoped(ref('stg_events')) }} e
    where e.event_class in ('shot', 'free_throw') and e.is_made
    group by all
),

finals as (
    select 'nba' as source, g.game_key, g.home_team_key as team_key, n.final_home as final
    from {{ scoped(ref('game_xwalk')) }} g join {{ scoped(ref('base_nba_games')) }} n on n.game_id = g.nba_game_id
    union all
    select 'nba', g.game_key, g.away_team_key, n.final_away
    from {{ scoped(ref('game_xwalk')) }} g join {{ scoped(ref('base_nba_games')) }} n on n.game_id = g.nba_game_id
    union all
    -- ESPN's final according to its own play-by-play (the last score it reports).
    select 'espn', g.game_key, g.home_team_key, max(p.score_home)
    from {{ scoped(ref('game_xwalk')) }} g join {{ scoped(ref('base_espn_plays')) }} p on p.game_id = g.espn_game_id group by all
    union all
    select 'espn', g.game_key, g.away_team_key, max(p.score_away)
    from {{ scoped(ref('game_xwalk')) }} g join {{ scoped(ref('base_espn_plays')) }} p on p.game_id = g.espn_game_id group by all
)

select f.*, coalesce(p.points, 0) as points
from finals f
left join points p using (source, game_key, team_key)
where coalesce(p.points, 0) <> f.final
  and not exists (
      select 1 from {{ ref('known_data_issues') }} k
      where k.test_name = 'assert_made_points_equal_final_score'
        and k.source = f.source and k.game_key = f.game_key)
