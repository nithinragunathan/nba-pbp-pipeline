-- One row per real rebound (rebound_id = cluster_id), linked to the miss it came from.
-- related_shot_id uses the NBA feed's explicit link (shotActionNumber) when NBA has the
-- rebound; for a rebound only ESPN recorded it falls back to ESPN's previous missed attempt.
with c as (
    select * from {{ scoped(ref('int_clusters')) }} where event_class = 'rebound'
),

nba_link as (
    select c.cluster_id, m.cluster_id as related_shot_id
    from c
    join {{ scoped(ref('int_event_matches')) }} m
      on m.source = 'nba' and m.source_event_id = c.nba_related_event_id
),

espn_order as (
    -- ESPN events in game order, carrying forward the last missed shot or free throw.
    select m.cluster_id, e.event_class,
           last_value(case when e.event_class in ('shot', 'free_throw') and not e.is_made then m.cluster_id end ignore nulls)
               over (partition by e.game_key, e.period order by e.clock_tenths desc, e.seq_in_period
                     rows between unbounded preceding and 1 preceding) as previous_miss_cluster
    from {{ scoped(ref('stg_espn')) }} e
    join {{ scoped(ref('int_event_matches')) }} m on m.source = 'espn' and m.source_event_id = e.source_event_id
),

poss as (
    select source, source_event_id, possession_id from {{ scoped(ref('int_possessions')) }}
),

espn_only_poss as (
    -- A rebound only ESPN recorded takes the possession its team held at that moment. Filtered
    -- to ESPN-only rebounds before the range join so the join stays small.
    select c.cluster_id, any_value(sp.possession_id order by sp.start_clock_tenths) as possession_id
    from c
    join {{ scoped(ref('int_possession_spans')) }} sp
      on sp.game_key = c.game_key and sp.period = c.espn_period
     and sp.offense_team_key = c.espn_team_key
     and c.espn_clock_tenths between sp.end_clock_tenths and sp.start_clock_tenths
    where c.nba_event_id is null
    group by 1
)

select
    c.cluster_id                                                   as rebound_id,
    c.league,
    c.game_key,
    coalesce(poss.possession_id, poss_e.possession_id, sp.possession_id) as possession_id,
    coalesce(nl.related_shot_id, case when c.nba_event_id is null then eo.previous_miss_cluster end) as related_shot_id,
    case when nl.related_shot_id is not null then 'nba_link'
         when c.nba_event_id is null and eo.previous_miss_cluster is not null then 'espn_previous_miss' end as related_shot_method,
    coalesce(c.nba_period, c.espn_period)                          as period,
    coalesce(c.nba_clock_tenths, c.espn_clock_tenths)              as clock_tenths,
    coalesce(c.nba_team_key, c.espn_team_key)                      as team_key,
    coalesce(c.nba_player_key, c.espn_player_key)                  as player_key,
    case when c.nba_player_key is not null then 'nba'
         when c.espn_player_key is not null then 'espn' end        as player_key_source,
    coalesce(c.nba_rebound_type, c.espn_rebound_type)              as rebound_type,
    case when c.nba_rebound_type is not null then 'nba' else 'espn' end as rebound_type_source,
    coalesce(c.nba_is_team_rebound, c.espn_is_team_rebound)        as is_team_rebound,
    c.nba_is_deadball                                              as is_deadball,  -- only NBA flags dead-ball rebounds
    c.n_sources,
    c.match_method,
    c.clock_diff,
    case when c.n_sources = 2 and c.nba_player_key is not null and c.espn_player_key is not null
         then c.nba_player_key = c.espn_player_key end             as agree_player,
    case when c.n_sources = 2 then c.nba_is_team_rebound = c.espn_is_team_rebound end as agree_team_rebound,
    case when c.n_sources = 2 then c.nba_rebound_type = c.espn_rebound_type end       as agree_type,
    c.nba_event_id,
    c.espn_event_id,
    c.nba_description,
    c.espn_description
from c
left join nba_link nl on nl.cluster_id = c.cluster_id
-- ESPN's previous-miss fallback applies only to rebounds NBA lacks. That condition sits in the
-- select, not in this ON clause: a left-table-only condition there forced a nested-loop join.
left join espn_order eo on eo.cluster_id = c.cluster_id
left join poss on poss.source = 'nba' and poss.source_event_id = c.nba_event_id
-- Games only ESPN covers have possessions derived from ESPN's own events.
left join poss poss_e on poss_e.source = 'espn' and poss_e.source_event_id = c.espn_event_id
left join espn_only_poss sp on sp.cluster_id = c.cluster_id
