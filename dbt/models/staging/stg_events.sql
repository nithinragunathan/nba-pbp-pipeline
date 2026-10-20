-- Every source's events in one shape. The matcher and all marts read from here.
select * from {{ scoped(ref('stg_nba')) }}
union all by name
select * from {{ scoped(ref('stg_espn')) }}
