-- One row per ESPN play, typed, all play types.
with latest as (
    select * from {{ scoped(ref('bronze_latest')) }} where source = 'espn' and payload_format = 'espn_summary_v2'
),

plays as (
    select league, season, game_id, unnest(from_json(payload -> 'plays', '["json"]')) as p
    from latest
)

select
    league,
    season,
    game_id,
    p ->> 'id'                                                as play_id,
    cast(p ->> 'sequenceNumber' as bigint)                    as sequence_number,
    p -> 'type' ->> 'id'                                      as type_id,
    replace(p -> 'type' ->> 'text', chr(10), ' ')             as type_text,
    p ->> 'text'                                              as description,
    cast(p -> 'period' ->> 'number' as integer)               as period,
    p -> 'clock' ->> 'displayValue'                           as clock_raw,
    {{ clock_to_tenths("p -> 'clock' ->> 'displayValue'") }}  as clock_tenths,
    cast(p ->> 'homeScore' as integer)                        as score_home,
    cast(p ->> 'awayScore' as integer)                        as score_away,
    cast(p ->> 'scoringPlay' as boolean)                      as scoring_play,
    cast(p ->> 'shootingPlay' as boolean)                     as shooting_play,
    cast(p ->> 'scoreValue' as integer)                       as score_value,
    cast(p ->> 'pointsAttempted' as integer)                  as points_attempted,
    p -> 'team' ->> 'id'                                      as team_id,
    p -> 'participants' -> 0 -> 'athlete' ->> 'id'            as athlete_id_1,
    p -> 'participants' -> 1 -> 'athlete' ->> 'id'            as athlete_id_2,
    -- ESPN uses a large negative sentinel for plays without a location (free throws).
    case when cast(p -> 'coordinate' ->> 'x' as double) between -10 and 100
         then cast(p -> 'coordinate' ->> 'x' as double) end   as coord_x,
    case when cast(p -> 'coordinate' ->> 'y' as double) between -10 and 100
         then cast(p -> 'coordinate' ->> 'y' as double) end   as coord_y,
    try_cast(p ->> 'wallclock' as timestamptz)                as wallclock
from plays
