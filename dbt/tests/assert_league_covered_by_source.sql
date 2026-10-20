-- No league may appear from a source that does not cover it (see seeds/source_coverage.csv).
select distinct e.league, e.source
from {{ scoped(ref('stg_events')) }} e
left join {{ ref('source_coverage') }} c
  on c.league = e.league and c.source = e.source and c.status = 'covered'
where c.league is null
