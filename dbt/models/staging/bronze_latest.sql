-- The current version of each (source, game): the most recently ingested payload. Older
-- versions stay in bronze (see bronze_payload_history). A view, so the payloads are not copied:
-- readers apply scoped() themselves, and a league-scoped build never slices it. The newest
-- version is found from metadata alone and applied as a semi join, so the hash table holds one key
-- per (source, game), a few thousand, and payloads stream through (a window function, or a join
-- that hashed the payload side, ran out of memory in base_nba_actions).
{{ config(materialized='view') }}
with newest as (
    select source, game_id, max(ingested_at) as ingested_at
    from {{ source('bronze', 'raw_payloads') }}
    group by source, game_id
)

select b.*
from {{ source('bronze', 'raw_payloads') }} b
semi join newest n
  on n.source = b.source and n.game_id = b.game_id and n.ingested_at = b.ingested_at
