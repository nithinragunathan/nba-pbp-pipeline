-- One row per NBA game, derived from its actions. The feed has no explicit home/away flag,
-- so the home team is the one whose made baskets raise score_home.
with ordered as (
    select *,
        score_home - lag(score_home) over (partition by game_id order by order_number) as d_home,
        score_away - lag(score_away) over (partition by game_id order by order_number) as d_away
    from {{ scoped(ref('base_nba_actions')) }}
),

side_votes as (
    select game_id, team_id,
        sum(case when d_home > 0 then 1 else 0 end) as home_votes,
        sum(case when d_away > 0 then 1 else 0 end) as away_votes
    from ordered
    where shot_result = 'Made' and team_id is not null
    group by game_id, team_id
),

finals as (
    select game_id, league, season,
        min(time_actual) as first_action_utc,
        arg_max(score_home, order_number) as final_home,
        arg_max(score_away, order_number) as final_away
    from {{ scoped(ref('base_nba_actions')) }}
    group by game_id, league, season
)

select
    f.league, f.season, f.game_id, f.first_action_utc,
    max(case when v.home_votes > v.away_votes then v.team_id end) as home_team_id,
    max(case when v.away_votes > v.home_votes then v.team_id end) as away_team_id,
    f.final_home, f.final_away
from finals f
join side_votes v using (game_id)
group by all
