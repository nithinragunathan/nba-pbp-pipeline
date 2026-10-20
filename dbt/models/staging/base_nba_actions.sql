-- One row per NBA liveData action, typed, all action types. The payload is the mirror format
-- (one game's rows of the season CSV, every value a string). A licensed league feed would be a
-- second payload_format unioned into `actions`.
with latest as (
    select * from {{ scoped(ref('bronze_latest')) }} where source = 'nba'
),

actions as (
    select league, season, game_id, unnest(from_json(payload, '["json"]')) as a
    from latest where payload_format = 'cdn_mirror_csv_rows'
),

cdn as (
    select
        league,
        season,
        game_id,
        cast(try_cast(a ->> 'actionNumber' as double) as integer)       as action_number,
        cast(try_cast(a ->> 'orderNumber' as double) as bigint)         as order_number,
        cast(try_cast(a ->> 'period' as double) as integer)             as period,
        a ->> 'clock'                                                   as clock_raw,
        {{ clock_to_tenths("a ->> 'clock'") }}                          as clock_tenths,
        a ->> 'actionType'                                              as action_type,
        a ->> 'subType'                                                 as sub_type,
        a ->> 'descriptor'                                              as descriptor,
        coalesce(a ->> 'qualifiers', '')                                as qualifiers,
        nullif(cast(try_cast(a ->> 'personId' as double) as bigint), 0) as person_id,
        a ->> 'playerNameI'                                             as player_name_i,
        cast(try_cast(a ->> 'teamId' as double) as bigint)::varchar     as team_id,
        a ->> 'teamTricode'                                             as team_tricode,
        cast(try_cast(a ->> 'possession' as double) as bigint)::varchar as possession_team_id,
        cast(try_cast(a ->> 'scoreHome' as double) as integer)          as score_home,
        cast(try_cast(a ->> 'scoreAway' as double) as integer)          as score_away,
        a ->> 'shotResult'                                              as shot_result,
        cast(try_cast(a ->> 'shotActionNumber' as double) as integer)   as shot_action_number,
        try_cast(a ->> 'x' as double)                                   as x_pct,
        try_cast(a ->> 'y' as double)                                   as y_pct,
        a ->> 'side'                                                    as side,
        try_cast(a ->> 'xLegacy' as double)                             as x_legacy,
        try_cast(a ->> 'yLegacy' as double)                             as y_legacy,
        try_cast(a ->> 'shotDistance' as double)                        as shot_distance_ft,
        a ->> 'description'                                             as description,
        try_cast(a ->> 'timeActual' as timestamptz)                     as time_actual,
        try_cast(a ->> 'edited' as timestamptz)                         as edited_at,
        {{ nba_source_type("a ->> 'actionType'", "a ->> 'subType'", "a ->> 'descriptor'") }} as source_type
    from actions
)

select * from cdn
