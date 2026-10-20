-- Canonical game key plus each source's own game id (nullable where a source lacks the game).
-- NBA and ESPN games are paired on (home team, away team) and a tip-off within
-- 12 hours (teams sometimes play the same opponent at the same arena on consecutive days).
-- start_utc is a naive UTC timestamp; conversions name 'UTC' explicitly so the session time
-- zone of the machine running dbt never shifts a game's date.
with espn as (
    select g.league, g.season, g.game_id as espn_game_id, g.start_utc,
           h.team_key as home_team_key, a.team_key as away_team_key,
           g.final_home, g.final_away
    from {{ scoped(ref('base_espn_games')) }} g
    join {{ ref('dim_team') }} h on h.espn_team_id = g.home_team_id and h.league = g.league
    join {{ ref('dim_team') }} a on a.espn_team_id = g.away_team_id and a.league = g.league
),

nba as (
    select g.league, g.season, g.game_id as nba_game_id, timezone('UTC', g.first_action_utc) as start_utc,
           h.team_key as home_team_key, a.team_key as away_team_key,
           g.final_home, g.final_away
    from {{ scoped(ref('base_nba_games')) }} g
    join {{ ref('dim_team') }} h on h.nba_team_id = g.home_team_id
    join {{ ref('dim_team') }} a on a.nba_team_id = g.away_team_id
),

paired as (
    select
        coalesce(e.league, n.league)               as league,
        coalesce(e.season, n.season)               as season,
        coalesce(e.home_team_key, n.home_team_key) as home_team_key,
        coalesce(e.away_team_key, n.away_team_key) as away_team_key,
        coalesce(e.start_utc, n.start_utc)         as start_utc,
        n.nba_game_id,
        e.espn_game_id,
        n.final_home as nba_final_home, n.final_away as nba_final_away,
        e.final_home as espn_final_home, e.final_away as espn_final_away
    from espn e
    full outer join nba n
      on  n.league = e.league
      and n.home_team_key = e.home_team_key
      and n.away_team_key = e.away_team_key
      and abs(epoch(n.start_utc) - epoch(e.start_utc)) < 12 * 3600
)

select
    league || '_' || strftime(cast(timezone('America/New_York', timezone('UTC', start_utc)) as date), '%Y%m%d')
        || '_' || split_part(away_team_key, '_', 2) || '_at_' || split_part(home_team_key, '_', 2) as game_key,
    league,
    season,
    cast(timezone('America/New_York', timezone('UTC', start_utc)) as date) as game_date_et,
    start_utc,
    home_team_key,
    away_team_key,
    nba_game_id,
    espn_game_id,
    nba_final_home, nba_final_away,
    espn_final_home, espn_final_away
from paired
