-- Possession time spans, used to place events only ESPN recorded in an NBA-feed game and as the
-- base of fct_possessions. boundary_source says which feed set the boundaries.
select
    league,
    possession_id,
    game_key,
    period,
    possession_team_key   as offense_team_key,
    any_value(source)     as boundary_source,
    max(clock_tenths)     as start_clock_tenths,
    min(clock_tenths)     as end_clock_tenths,
    count(*)              as n_actions
from {{ scoped(ref('int_possessions')) }}
group by league, possession_id, game_key, period, possession_team_key
