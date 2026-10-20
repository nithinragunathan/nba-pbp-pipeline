-- Events labelled with the possession they belong to. A possession is a run of consecutive events
-- with the same team in possession, so an offensive rebound continues it, as it should.
--   * Games the NBA feed covers: its `possession` team id, carried on every action (it equals the
--     acting team on every field goal, rebound and turnover of 2025-26).
--   * Games only ESPN covers (every WNBA game here, and the NBA Cup final): ESPN has no possession
--     field, so the team with the ball is set by shots, non-technical free throws, rebounds and
--     turnovers, and carried forward over fouls. A technical free throw does not change who has
--     the ball.
with nba as (
    select g.league, g.game_key, 'nba' as source, a.game_id || '-' || a.action_number as source_event_id,
           a.period, a.clock_tenths, a.order_number as seq, t.team_key as possession_team_key
    from {{ scoped(ref('base_nba_actions')) }} a
    join {{ scoped(ref('game_xwalk')) }} g on g.nba_game_id = a.game_id
    join {{ ref('dim_team') }} t on t.nba_team_id = a.possession_team_id and t.league = g.league
    where a.possession_team_id is not null and a.possession_team_id <> '0'
),

espn as (
    select e.league, e.game_key, 'espn' as source, e.source_event_id, e.period, e.clock_tenths,
           row_number() over w as seq,
           last_value(case when e.event_class in ('shot', 'rebound', 'turnover')
                             or (e.event_class = 'free_throw' and e.source_event_type not ilike '%technical%')
                           then e.team_key end ignore nulls)
               over (w rows unbounded preceding) as possession_team_key
    from {{ scoped(ref('stg_espn')) }} e
    where e.game_key in (select game_key from {{ scoped(ref('game_xwalk')) }} where nba_game_id is null)
    -- Game order as the matcher sees it: clock first, then ESPN's own sequence.
    window w as (partition by e.game_key, e.period order by e.clock_tenths desc, e.seq_in_period)
),

events as (
    select * from nba
    union all
    select * from espn where possession_team_key is not null
),

flagged as (
    select *,
        case when possession_team_key is distinct from
                  lag(possession_team_key) over (partition by game_key, period order by seq)
             then 1 else 0 end as starts_possession
    from events
)

select
    league,
    game_key,
    source,
    source_event_id,
    period,
    clock_tenths,
    seq,
    possession_team_key,
    game_key || ':P' || lpad(cast(sum(starts_possession) over (
        partition by game_key order by period, seq rows unbounded preceding) as varchar), 3, '0')
        as possession_id
from flagged
