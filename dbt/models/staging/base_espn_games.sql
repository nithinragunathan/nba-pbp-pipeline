-- One row per ESPN game: teams, home/away and the official final score from the header.
with latest as (
    select * from {{ scoped(ref('bronze_latest')) }} where source = 'espn' and payload_format = 'espn_summary_v2'
),

competitors as (
    select
        league, season, game_id,
        payload -> 'header' -> 'competitions' -> 0 ->> 'date' as start_raw,
        unnest(from_json(payload -> 'header' -> 'competitions' -> 0 -> 'competitors', '["json"]')) as c
    from latest
)

select
    league,
    season,
    game_id,
    -- ESPN gives the tip-off time in UTC without seconds, e.g. 2025-10-23T00:00Z.
    cast(strptime(replace(any_value(start_raw), 'Z', ''), '%Y-%m-%dT%H:%M') as timestamp) as start_utc,
    max(case when c ->> 'homeAway' = 'home' then c -> 'team' ->> 'id' end)            as home_team_id,
    max(case when c ->> 'homeAway' = 'away' then c -> 'team' ->> 'id' end)            as away_team_id,
    max(case when c ->> 'homeAway' = 'home' then cast(c ->> 'score' as integer) end)  as final_home,
    max(case when c ->> 'homeAway' = 'away' then cast(c ->> 'score' as integer) end)  as final_away
from competitors
group by league, season, game_id
