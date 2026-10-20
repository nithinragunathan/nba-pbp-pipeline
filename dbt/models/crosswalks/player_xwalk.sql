-- One row per real player with each source's id. NBA play-by-play only carries short names
-- ('K. Middleton'), which happens to be ESPN's shortName format, so players are matched within
-- a game and team in three passes, each only for players the earlier passes left unmatched:
--   1. short_name:  normalized short names equal              ('K. Middleton' = 'K. Middleton')
--   2. last_name:   last name unique on both sides            ('P.J. Washington' vs 'PJ Washington')
--   3. name_tokens: NBA name tokens all inside ESPN full name ('G. Jackson' -> 'GG Jackson',
--                   'Yang' -> 'Yang Hansen'), initial agrees, and the candidate is unique
with games as (
    select game_key, league, nba_game_id, espn_game_id
    from {{ scoped(ref('game_xwalk')) }}
    where nba_game_id is not null and espn_game_id is not null
),

nba_p as (
    select g.game_key, t.team_key, p.person_id, p.player_name_i,
           {{ norm_name('p.player_name_i') }} as k_short
    from {{ scoped(ref('base_nba_players')) }} p
    join games g on g.nba_game_id = p.game_id
    join {{ ref('dim_team') }} t on t.nba_team_id = p.team_id
),

espn_p as (
    select g.game_key, t.team_key, a.athlete_id, a.display_name,
           {{ norm_name('a.short_name') }} as k_short,
           {{ norm_name('a.display_name') }} as k_full
    from {{ scoped(ref('base_espn_players')) }} a
    join games g on g.espn_game_id = a.game_id
    join {{ ref('dim_team') }} t on t.espn_team_id = a.team_id and t.league = g.league
),

with_last as (
    select 'nba' as src, game_key, team_key, cast(person_id as varchar) as id, k_short,
           regexp_replace(k_short, '^[a-z]+ ', '') as k_last,
           case when k_short like '% %' then split_part(k_short, ' ', 1) else '' end as initial,
           cast(null as varchar) as k_full
    from nba_p
    union all
    select 'espn', game_key, team_key, athlete_id, k_short, regexp_replace(k_short, '^[a-z]+ ', ''),
           null, k_full
    from espn_p
),

-- Last-name keys that identify exactly one player per side in that game and team.
unique_last as (
    select game_key, team_key, k_last
    from with_last
    group by game_key, team_key, k_last
    having count(*) filter (where src = 'nba') = 1 and count(*) filter (where src = 'espn') = 1
),

first_passes as (
    select n.game_key, n.id as person_id, e.id as athlete_id, 'short_name' as method
    from with_last n
    join with_last e
      on e.src = 'espn' and n.src = 'nba'
     and e.game_key = n.game_key and e.team_key = n.team_key and e.k_short = n.k_short
    union all
    select n.game_key, n.id, e.id, 'last_name'
    from with_last n
    join with_last e
      on e.src = 'espn' and n.src = 'nba'
     and e.game_key = n.game_key and e.team_key = n.team_key and e.k_last = n.k_last
     and e.k_short <> n.k_short
    join unique_last u
      on u.game_key = n.game_key and u.team_key = n.team_key and u.k_last = n.k_last
),

token_candidates as (
    select n.game_key, n.id as person_id, e.id as athlete_id,
           count(*) over (partition by n.game_key, n.id) as n_candidates
    from with_last n
    join with_last e
      on e.src = 'espn' and n.src = 'nba'
     and e.game_key = n.game_key and e.team_key = n.team_key
     and (' ' || e.k_full || ' ') like ('% ' || n.k_last || ' %')
     and (n.initial = '' or e.k_full like n.initial || '%')
    where not exists (select 1 from first_passes f where f.game_key = n.game_key and f.person_id = n.id)
      and not exists (select 1 from first_passes f where f.game_key = e.game_key and f.athlete_id = e.id)
),

game_matches as (
    select * from first_passes
    union all
    select game_key, person_id, athlete_id, 'name_tokens' from token_candidates where n_candidates = 1
),

pair_counts as (
    select person_id, athlete_id, count(distinct game_key) as n_games, min(method) as method
    from game_matches
    group by person_id, athlete_id
),

-- Keep the athlete each person matched most often; confidence is that share of matched games.
best as (
    select person_id, athlete_id, method, n_games,
           n_games / sum(n_games) over (partition by person_id) as match_confidence
    from pair_counts
    qualify row_number() over (partition by person_id order by n_games desc, athlete_id) = 1
),

nba_people as (
    select person_id::varchar as person_id, any_value(league) as league, mode(player_name_i) as nba_name
    from {{ scoped(ref('base_nba_players')) }} group by 1
),

espn_people as (
    select athlete_id, any_value(league) as league, mode(display_name) as espn_name
    from {{ scoped(ref('base_espn_players')) }} group by 1
)

select
    case when p.person_id is not null then 'nba_' || p.person_id else 'espn_' || e.athlete_id end as player_key,
    coalesce(p.league, e.league)                    as league,
    coalesce(e.espn_name, p.nba_name)               as display_name,
    p.person_id                                     as nba_person_id,
    e.athlete_id                                    as espn_athlete_id,
    case when b.person_id is not null then b.method
         when p.person_id is not null then 'nba_only'
         else 'espn_only' end                       as match_method,
    b.n_games                                       as n_games_matched,
    b.match_confidence
from nba_people p
left join best b on b.person_id = p.person_id
full outer join espn_people e on e.athlete_id = b.athlete_id
