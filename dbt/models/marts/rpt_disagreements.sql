-- Every cluster where the sources disagree or only one source has the event: the list to hand-check.
with c as (
    select * from {{ scoped(ref('int_clusters')) }}
    -- Only games both feeds cover: in a single-source game (every WNBA game here, the NBA Cup
    -- final) every event is single-source by definition, so there is nothing to compare.
    where game_key in (select game_key from {{ scoped(ref('game_xwalk')) }}
                       where nba_game_id is not null and espn_game_id is not null)
)

select
    cluster_id,
    league,
    game_key,
    event_class,
    case
        when espn_event_id is null then 'nba_only'
        when nba_event_id is null then 'espn_only'
        when nba_is_made <> espn_is_made then 'result'
        when nba_shot_value <> espn_shot_value then 'shot_value'
        when nba_player_key <> espn_player_key then 'player'
        when nba_is_team_rebound <> espn_is_team_rebound then 'team_vs_player_rebound'
        when nba_rebound_type <> espn_rebound_type then 'rebound_type'
    end                                       as disagreement,
    coalesce(nba_period, espn_period)         as period,
    coalesce(nba_clock_tenths, espn_clock_tenths) / 10.0 as clock_s,
    nba_description,
    espn_description,
    nba_event_id,
    espn_event_id
from c
where espn_event_id is null
   or nba_event_id is null
   or nba_is_made <> espn_is_made
   or nba_shot_value <> espn_shot_value
   or nba_player_key <> espn_player_key
   or nba_is_team_rebound <> espn_is_team_rebound
   or nba_rebound_type <> espn_rebound_type
