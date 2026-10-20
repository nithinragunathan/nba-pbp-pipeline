-- One row per cluster with each source's version of the event side by side.
with m as (
    select m.cluster_id, m.match_method, m.clock_diff, e.*
    from {{ scoped(ref('int_event_matches')) }} m
    join {{ scoped(ref('stg_events')) }} e using (source, source_event_id)
)

select
    cluster_id,
    any_value(game_key)                                        as game_key,
    any_value(league)                                          as league,
    any_value(event_class)                                     as event_class,
    any_value(match_method)                                    as match_method,
    any_value(clock_diff)                                      as clock_diff,
    count(*)                                                   as n_sources,
    {% for src in ['nba', 'espn'] %}
    max(source_event_id) filter (where source = '{{ src }}')     as {{ src }}_event_id,
    max(period) filter (where source = '{{ src }}')              as {{ src }}_period,
    max(clock_tenths) filter (where source = '{{ src }}')        as {{ src }}_clock_tenths,
    max(seq_in_period) filter (where source = '{{ src }}')       as {{ src }}_seq_in_period,
    max(team_key) filter (where source = '{{ src }}')            as {{ src }}_team_key,
    max(player_key) filter (where source = '{{ src }}')          as {{ src }}_player_key,
    bool_or(is_made) filter (where source = '{{ src }}')         as {{ src }}_is_made,
    max(shot_value) filter (where source = '{{ src }}')          as {{ src }}_shot_value,
    max(rebound_type) filter (where source = '{{ src }}')        as {{ src }}_rebound_type,
    bool_or(is_team_rebound) filter (where source = '{{ src }}') as {{ src }}_is_team_rebound,
    bool_or(is_deadball) filter (where source = '{{ src }}')     as {{ src }}_is_deadball,
    max(x_ft) filter (where source = '{{ src }}')                as {{ src }}_x_ft,
    max(y_ft) filter (where source = '{{ src }}')                as {{ src }}_y_ft,
    max(related_source_event_id) filter (where source = '{{ src }}') as {{ src }}_related_event_id,
    max(description) filter (where source = '{{ src }}')         as {{ src }}_description,
    {% endfor %}
from m
group by cluster_id
