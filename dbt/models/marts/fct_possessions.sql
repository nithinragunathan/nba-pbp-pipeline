-- One row per possession: a run of consecutive events with the same team in possession. Boundaries
-- come from the NBA feed's possession field, or from ESPN's events where ESPN is the only feed
-- (boundary_source says which; see int_possessions).
with points as (
    select possession_id,
           sum(case when is_made then shot_value else 0 end) as points,
           count(*) filter (where not is_free_throw)           as fga,
           count(*) filter (where is_free_throw)               as fta
    from {{ scoped(ref('fct_shots')) }}
    group by 1
)

select
    s.league,
    s.possession_id,
    s.game_key,
    s.period,
    s.offense_team_key,
    s.start_clock_tenths,
    s.end_clock_tenths,
    s.n_actions,
    coalesce(p.points, 0) as points,
    coalesce(p.fga, 0)    as fga,
    coalesce(p.fta, 0)    as fta,
    s.boundary_source
from {{ scoped(ref('int_possession_spans')) }} s
left join points p using (possession_id)
