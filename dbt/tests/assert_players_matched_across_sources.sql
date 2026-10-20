-- Every player who acted in a game both sources cover must exist in both sources.
select x.*
from {{ scoped(ref('player_xwalk')) }} x
where x.match_method = 'nba_only'
