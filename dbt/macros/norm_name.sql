{# Name key used for player matching: no accents, lower case, no punctuation, no Jr./Sr./II/III/IV suffix. #}
{% macro norm_name(col) -%}
    trim(regexp_replace(
        regexp_replace(lower(strip_accents({{ col }})), '[^a-z ]', '', 'g'),
        ' (jr|sr|ii|iii|iv)$', ''))
{%- endmacro %}
