{#
  League-scoped builds. `dbt build --vars '{leagues: [wnba]}'` rebuilds only the WNBA rows of
  every model and leaves the other leagues' rows as they are; without the var every model is
  rebuilt in full. Each model reads its inputs through scoped(), and when the var is set the
  models are incremental: delete_league_slice() (a pre-hook) removes the selected leagues' rows,
  then the model's output for those leagues is appended. Leagues are independent all the way
  through (every key starts with the game, and a game belongs to one league), so a slice can
  be rebuilt on its own.
#}

{% macro league_in_scope(col='league') -%}
    {%- set leagues = var('leagues', none) -%}
    {%- if leagues -%}
        {{ col }} in ({% for l in leagues %}'{{ l }}'{{ ", " if not loop.last }}{% endfor %})
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}

{% macro scoped(relation) -%}
    {%- if var('leagues', none) -%}
        (select * from {{ relation }} where {{ league_in_scope() }})
    {%- else -%}
        {{ relation }}
    {%- endif -%}
{%- endmacro %}

{% macro delete_league_slice() -%}
    {%- if is_incremental() -%}
        delete from {{ this }} where {{ league_in_scope() }}
    {%- endif -%}
{%- endmacro %}
