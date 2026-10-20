-- player_key may only be null for team-level events: team rebounds, team turnovers (e.g. shot
-- clock) and the NBA's team-level 'heave' record. Anything else is an unmatched player, unless it
-- is a reviewed source error listed in seeds/known_data_issues.csv.
select e.source, e.source_event_id, e.event_class, e.source_event_type, e.source_player_id, e.description
from {{ scoped(ref('stg_events')) }} e
where e.player_key is null
  and (
        e.source_player_id is not null                             -- a player id we could not resolve
     or e.event_class in ('free_throw', 'foul')
     or (e.event_class = 'shot' and e.source_event_type <> 'heave')
  )
  and not exists (
      select 1 from {{ ref('known_data_issues') }} k
      where k.test_name = 'assert_no_unexpected_null_player_key'
        and k.source = e.source and k.game_key = e.game_key)
