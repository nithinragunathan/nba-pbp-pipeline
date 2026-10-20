-- ESPN events in the shared schema (shots, free throws, rebounds, turnovers, fouls).
select
    'espn'                                      as source,
    e.league,
    e.season,
    g.game_key,
    e.play_id                                   as source_event_id,
    e.period,
    e.clock_tenths,
    row_number() over (partition by e.game_id, e.period order by e.sequence_number) as seq_in_period,
    m.event_class,
    e.type_text                                 as source_event_type,
    case when m.event_class in ('shot', 'free_throw') then e.scoring_play end as is_made,
    case when m.event_class = 'free_throw' then 1
         when m.event_class = 'shot' then e.points_attempted end              as shot_value,
    case when m.event_class = 'free_throw'
         then try_cast(regexp_extract(e.type_text, '(\d+) of \d+', 1) as integer) end as ft_number,
    case e.type_id when '156' then 'offensive' when '155' then 'defensive' end as rebound_type,
    case when m.event_class = 'rebound' then e.athlete_id_1 is null end       as is_team_rebound,
    cast(null as boolean)                       as is_deadball,   -- ESPN does not flag dead-ball rebounds
    p.player_key,
    e.athlete_id_1                              as source_player_id,
    t.team_key,
    -- ESPN coordinates are already half-court feet: x 0-50 across, y from the basket line.
    case when m.event_class = 'shot' then e.coord_x - 25 end as x_ft,
    case when m.event_class = 'shot' then e.coord_y end      as y_ft,
    cast(null as double)                        as nba_x_legacy_ft,
    cast(null as double)                        as nba_y_legacy_ft,
    e.score_home,
    e.score_away,
    cast(null as varchar)                       as related_source_event_id,
    e.description,
    e.wallclock
from {{ scoped(ref('base_espn_plays')) }} e
join {{ ref('event_type_map') }} m
  on m.source = 'espn' and m.source_type = e.type_id
join {{ scoped(ref('game_xwalk')) }} g on g.espn_game_id = e.game_id
-- ESPN team ids are per league (16 is the Mystics in the WNBA and the Timberwolves in the NBA).
left join {{ ref('dim_team') }} t on t.espn_team_id = e.team_id and t.league = e.league
left join {{ scoped(ref('player_xwalk')) }} p on p.espn_athlete_id = e.athlete_id_1
where m.event_class <> 'ignore'
