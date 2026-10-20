-- Which source covers which league (from the seed), and how many games each source delivered here.
-- Always a full table: it summarizes every league, so a league-scoped build must not slice it.
{{ config(materialized='table') }}
with observed as (
    select league, 'nba' as source, count(nba_game_id) as n_games from {{ ref('game_xwalk') }} group by 1
    union all
    select league, 'espn', count(espn_game_id) from {{ ref('game_xwalk') }} group by 1
)

select c.league, c.source, c.status, coalesce(o.n_games, 0) as n_games_loaded, c.notes
from {{ ref('source_coverage') }} c
left join observed o using (league, source)
