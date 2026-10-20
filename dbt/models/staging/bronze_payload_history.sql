-- Every version of each (source, game) payload and the window it was current in. Bronze is
-- append-only, so its history needs no snapshot: a version is current from its ingested_at
-- until the next version's. A view over all of bronze, never sliced by a league-scoped build.
{{ config(materialized='view') }}
select
    source || ':' || game_id                                                   as payload_key,
    source,
    league,
    season,
    game_id,
    payload_format,
    extract_mode,
    payload_hash,
    ingested_at                                                                as valid_from,
    lead(ingested_at) over (partition by source, game_id order by ingested_at) as valid_to,
    lead(ingested_at) over (partition by source, game_id order by ingested_at) is null as is_current
from {{ source('bronze', 'raw_payloads') }}
