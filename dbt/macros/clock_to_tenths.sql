{#
  Game clock -> integer tenths of a second remaining in the period.
    NBA  'PT11M34.00S' -> 6940     'PT00M04.70S' -> 47
    ESPN '11:34'       -> 6940     '4.7'         -> 47   (ESPN drops the minutes in the last minute)
#}
{% macro clock_to_tenths(col) -%}
    case
        when {{ col }} like 'PT%'
            then cast(round((cast(regexp_extract({{ col }}, 'PT(\d+)M', 1) as integer) * 60
                       + cast(regexp_extract({{ col }}, 'M([\d.]+)S', 1) as double)) * 10) as integer)
        when {{ col }} like '%:%'
            then cast(round((cast(split_part({{ col }}, ':', 1) as integer) * 60
                       + cast(split_part({{ col }}, ':', 2) as double)) * 10) as integer)
        else cast(round(cast({{ col }} as double) * 10) as integer)
    end
{%- endmacro %}
