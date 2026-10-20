-- One row per real shot or free throw (shot_id = cluster_id).
-- Field-level precedence: each value comes from NBA when NBA has it, otherwise ESPN, and the
-- *_source column says which. Agreement flags are null when only one source saw the shot.
with c as (
    select * from {{ scoped(ref('int_clusters')) }} where event_class in ('shot', 'free_throw')
),

poss as (
    select source, source_event_id, possession_id from {{ scoped(ref('int_possessions')) }}
),

espn_only_poss as (
    -- A shot only ESPN recorded in an NBA-feed game takes the possession its team held at that moment.
    select c.cluster_id, any_value(p.possession_id order by p.start_clock_tenths) as possession_id
    from c
    join {{ scoped(ref('int_possession_spans')) }} p
      on p.game_key = c.game_key and p.period = c.espn_period
     and p.offense_team_key = c.espn_team_key
     and c.espn_clock_tenths between p.end_clock_tenths and p.start_clock_tenths
    where c.nba_event_id is null
    group by 1
)

select
    c.cluster_id                                                 as shot_id,
    c.league,
    c.game_key,
    coalesce(poss.possession_id, poss_e.possession_id, eo.possession_id) as possession_id,
    coalesce(c.nba_period, c.espn_period)                        as period,
    coalesce(c.nba_clock_tenths, c.espn_clock_tenths)            as clock_tenths,
    coalesce(c.nba_team_key, c.espn_team_key)                    as team_key,
    c.event_class = 'free_throw'                                 as is_free_throw,
    coalesce(c.nba_player_key, c.espn_player_key)                as player_key,
    case when c.nba_player_key is not null then 'nba'
         when c.espn_player_key is not null then 'espn' end      as player_key_source,
    coalesce(c.nba_shot_value, c.espn_shot_value)                as shot_value,
    coalesce(c.nba_is_made, c.espn_is_made)                      as is_made,
    case when c.nba_is_made is not null then 'nba' else 'espn' end as is_made_source,
    case when c.nba_x_ft is not null then c.nba_x_ft else c.espn_x_ft end as x_ft,
    case when c.nba_x_ft is not null then c.nba_y_ft else c.espn_y_ft end as y_ft,
    case when c.nba_x_ft is not null then 'nba'
         when c.espn_x_ft is not null then 'espn' end            as xy_source,
    c.n_sources,
    c.match_method,
    c.clock_diff,
    case when c.n_sources = 2 then c.nba_is_made = c.espn_is_made end         as agree_made,
    case when c.n_sources = 2 then c.nba_shot_value = c.espn_shot_value end   as agree_value,
    case when c.n_sources = 2 and c.nba_player_key is not null and c.espn_player_key is not null
         then c.nba_player_key = c.espn_player_key end                        as agree_player,
    case when c.n_sources = 2 and c.event_class = 'shot'
         then sqrt(power(c.nba_x_ft - c.espn_x_ft, 2) + power(c.nba_y_ft - c.espn_y_ft, 2)) end
                                                                              as xy_diff_ft,
    c.nba_event_id,
    c.espn_event_id,
    c.nba_description,
    c.espn_description
from c
left join poss on poss.source = 'nba' and poss.source_event_id = c.nba_event_id
-- Games only ESPN covers have possessions derived from ESPN's own events.
left join poss poss_e on poss_e.source = 'espn' and poss_e.source_event_id = c.espn_event_id
left join espn_only_poss eo on eo.cluster_id = c.cluster_id
