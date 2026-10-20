-- NBA events in the shared schema (shots, free throws, rebounds, turnovers, fouls).
with a as (
    select * from {{ scoped(ref('base_nba_actions')) }}
)

select
    'nba'                                       as source,
    a.league,
    a.season,
    g.game_key,
    a.game_id || '-' || a.action_number         as source_event_id,
    a.period,
    a.clock_tenths,
    row_number() over (partition by a.game_id, a.period order by a.order_number) as seq_in_period,
    m.event_class,
    a.action_type || coalesce(':' || a.sub_type, '') as source_event_type,
    -- A missed end-of-quarter heave is logged as a team-level 'heave' action (it does not count
    -- against the player's shooting); a made heave is logged as a normal 3pt make.
    case when a.action_type = 'heave' then false
         when m.event_class in ('shot', 'free_throw') then a.shot_result = 'Made' end as is_made,
    case a.action_type when '3pt' then 3 when 'heave' then 3 when '2pt' then 2 when 'freethrow' then 1 end as shot_value,
    case when m.event_class = 'free_throw'
         then try_cast(regexp_extract(a.sub_type, '^(\d+) of', 1) as integer) end as ft_number,
    case when m.event_class = 'rebound' then a.sub_type end                   as rebound_type,
    case when m.event_class = 'rebound' then a.person_id is null end          as is_team_rebound,
    case when m.event_class = 'rebound' then a.qualifiers like '%deadball%' end as is_deadball,
    p.player_key,
    cast(a.person_id as varchar)                as source_player_id,
    t.team_key,
    -- Full-court percentages -> half-court feet, basket at (0, 0), y toward half court.
    -- `side` says which basket the shot was at, so the right-side basket is mirrored.
    -- The WNBA format has only the half-court legacy coordinates (tenths of a foot, same frame).
    case when m.event_class = 'shot' then coalesce(
        case a.side when 'right' then a.y_pct * 0.5 - 25 when 'left' then 25 - a.y_pct * 0.5 end,
        a.x_legacy / 10.0)
    end                                         as x_ft,
    case when m.event_class = 'shot' then coalesce(
        case a.side when 'right' then 88.75 - a.x_pct * 0.94 when 'left' then a.x_pct * 0.94 - 5.25 end,
        a.y_legacy / 10.0)
    end                                         as y_ft,
    a.x_legacy / 10.0                           as nba_x_legacy_ft,
    a.y_legacy / 10.0                           as nba_y_legacy_ft,
    a.score_home,
    a.score_away,
    case when m.event_class = 'rebound' and a.shot_action_number is not null
         then a.game_id || '-' || a.shot_action_number end as related_source_event_id,
    a.description,
    a.time_actual                               as wallclock
from a
join {{ ref('event_type_map') }} m
  on m.source = 'nba' and m.source_type = a.source_type
join {{ scoped(ref('game_xwalk')) }} g on g.nba_game_id = a.game_id
left join {{ ref('dim_team') }} t on t.nba_team_id = a.team_id
left join {{ scoped(ref('player_xwalk')) }} p on p.nba_person_id = cast(a.person_id as varchar)
where m.event_class <> 'ignore'
