-- The court-frame flip (full-court percentages + side -> half-court feet) must agree with the
-- NBA's own half-court xLegacy/yLegacy coordinates to within half a foot.
select source_event_id, x_ft, nba_x_legacy_ft, y_ft, nba_y_legacy_ft
from {{ scoped(ref('stg_nba')) }}
where event_class = 'shot' and nba_x_legacy_ft is not null
  and (abs(x_ft - nba_x_legacy_ft) > 0.5 or abs(y_ft - nba_y_legacy_ft) > 0.5)
