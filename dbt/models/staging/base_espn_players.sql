-- Players listed in each ESPN box score (everyone on the roster for that game).
with latest as (
    select * from {{ scoped(ref('bronze_latest')) }} where source = 'espn' and payload_format = 'espn_summary_v2'
),

teams as (
    select league, game_id, unnest(from_json(payload -> 'boxscore' -> 'players', '["json"]')) as t
    from latest
),

athletes as (
    select league, game_id, t -> 'team' ->> 'id' as team_id,
           unnest(from_json(t -> 'statistics' -> 0 -> 'athletes', '["json"]')) as a
    from teams
)

select distinct
    league,
    game_id,
    team_id,
    a -> 'athlete' ->> 'id'          as athlete_id,
    a -> 'athlete' ->> 'displayName' as display_name,
    a -> 'athlete' ->> 'shortName'   as short_name,
    a -> 'athlete' ->> 'jersey'      as jersey
from athletes
where a -> 'athlete' ->> 'id' is not null   -- one box score has an empty roster slot
