-- Every rebound must link to a missed shot or free throw, except dead-ball team rebounds
-- (e.g. after the first of two free throws), which the NBA logs with no live ball to rebound.
select r.rebound_id, r.related_shot_id, s.is_made
from {{ scoped(ref('fct_rebounds')) }} r
left join {{ scoped(ref('fct_shots')) }} s on s.shot_id = r.related_shot_id
where not (r.is_team_rebound and coalesce(r.is_deadball, false))
  and (r.related_shot_id is null or s.is_made)
