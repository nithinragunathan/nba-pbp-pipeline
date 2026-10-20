{#
  The NBA feed's key into seeds/event_type_map.csv, defined once for every payload format in
  base_nba_actions (which stg_nba and the event-type test read). NBA logs a double foul as two
  'personal' (or 'technical') fouls with descriptor 'double'; ESPN calls it a double foul
  outright.
#}
{% macro nba_source_type(action_type, sub_type, descriptor) -%}
    {#- Arguments are parenthesised: `a ->> 'x' = 'foul'` would parse as `a ->> ('x' = 'foul')`. -#}
    case when ({{ action_type }}) = 'foul' and ({{ descriptor }}) = 'double' then 'foul:double ' || ({{ sub_type }})
         when ({{ action_type }}) = 'foul' then 'foul:' || ({{ sub_type }})
         else ({{ action_type }}) end
{%- endmacro %}
