-- Every event type either source sends must be listed in seeds/event_type_map.csv (as a class
-- or as 'ignore'). The APIs are undocumented, so a new code fails the build instead of being
-- silently dropped.
with seen as (
    select distinct 'nba' as source, source_type
    from {{ scoped(ref('base_nba_actions')) }}
    union
    select distinct 'espn', type_id from {{ scoped(ref('base_espn_plays')) }}
)

select s.*
from seen s
left join {{ ref('event_type_map') }} m using (source, source_type)
where m.event_class is null
