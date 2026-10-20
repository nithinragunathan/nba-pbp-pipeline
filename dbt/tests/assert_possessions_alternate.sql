-- Possessions alternate, so within a period the two teams' possession counts differ by at most 1.
with n as (
    select game_key, period, offense_team_key, count(*) as n
    from {{ scoped(ref('fct_possessions')) }}
    group by all
)

select game_key, period, max(n) - min(n) as diff
from n
group by game_key, period
having max(n) - min(n) > 1
