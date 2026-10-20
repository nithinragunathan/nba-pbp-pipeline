-- Players who appear as the primary actor in each NBA game.
select distinct league, game_id, team_id, person_id, player_name_i
from {{ scoped(ref('base_nba_actions')) }}
where person_id is not null and team_id is not null and player_name_i is not null
