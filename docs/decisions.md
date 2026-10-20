# Decision log

Every architecture decision in pbp-unify, what it was weighed against, and a verdict from a
review against the league-wide build (all 2025-26 NBA and 2026 WNBA games). The pipeline runs on
DuckDB locally, and each decision is also judged by how it would carry over to a cloud warehouse.

Each entry has the same parts:

- **Decision:** what the pipeline does.
- **Why:** the problem it answers.
- **Alternatives:** what else was possible.
- **Evaluation:** how it held up, including what the league-wide run showed.
- **Verdict:** keep, change or revisit, with any follow-up.

## Index

| # | Decision | Area | Verdict |
|---|---|---|---|
| D1 | DuckDB + dbt + Prefect, medallion layers | Platform | keep; add BigQuery port notes |
| D2 | Landing zone: raw files per game, as-is, hive folders | Ingestion | keep; protect bronze from `make clean` |
| D3 | Append-only bronze, versioned by a stat-bearing hash, plus a snapshot | Ingestion | keep bronze; replace snapshot with a view; widen ESPN hash |
| D4 | NBA official data from a community mirror | Sources | keep; make mirror refresh real; live mode removed (revised) |
| D5 | The WNBA from ESPN only: a single-source league | Sources | keep; a licensed WNBA feed would make it two-feed |
| D6 | ESPN's undocumented site API | Sources | keep; add a payload schema guard |
| D7 | Flow shape: subflow per target, task per game, one dbt build | Orchestration | keep; tolerate per-game failures; use prefect-dbt |
| D8 | Retries in the HTTP helper and in Prefect tasks; skip ESPN 404s | Orchestration | change: one Prefect-owned retry layer |
| D9 | One shared `stg_events` schema for every source | Modeling | keep; enforce a contract; `bronze_latest` as a view |
| D10 | `event_type_map` seed; an unmapped type fails the build | Modeling | keep; add pairing-rate check; one NBA type definition |
| D11 | Crosswalks: canonical game key, name-based player matching per game and team | Modeling | keep; warn on low match confidence |
| D12 | Cluster = one real event; gold is one row per cluster | Modeling | keep; link suspected counterparts |
| D13 | NBA as pivot; field-level precedence with `*_source` columns | Modeling | keep; document 3-source caveats |
| D14 | Matcher: score anchors, then order-preserving alignment | Matching | keep; known-pairs test; weight sweep |
| D15 | Matcher runs as a dbt Python model | Matching | keep here; document production design |
| D16 | Possessions from the NBA feed's `possession` field, or from ESPN's events | Modeling | keep; cross-check the ESPN rule on NBA games |
| D17 | League-scoped incremental builds via a dbt var | Build | keep; guard unscoped refs; commit equivalence check |
| D18 | dbt runs one model at a time (`threads: 1`) | Build | keep; fix the `fct_rebounds` nested-loop join |
| D19 | Tests encode basketball; known source errors live in a seed | Quality | keep; expiring, event-level exceptions; unit tests |
| D20 | Coverage, reconciliation and matcher reports | Quality | keep; classify disagreements; stop churn |

## D1. DuckDB + dbt + Prefect, medallion layers

**Decision:** Everything runs locally. Prefect orchestrates; landing files feed
`bronze.raw_payloads` in a DuckDB file; dbt (dbt-duckdb) builds silver (`staging`,
`crosswalks`, `intermediate`) and gold (`marts`).

**Why:** Anyone can run it with `make all`: no cloud account, credentials or cost. dbt and
Prefect are the target team's own tools, so the part that matters most, the dbt project,
carries over in structure: layers, tests, seeds, snapshot and macros.

**Alternatives:** BigQuery from the start (a rebuild was planned on 2026-10-18 and dropped the
next day as overkill); Postgres; plain Python without dbt.

**Evaluation:**
- Holds up: the league-wide build (1,560 games) runs end to end from a fresh clone in about 3
  minutes on a MacBook Pro, most of it ESPN downloads; `dbt build` takes 36 seconds.
- The SQL is tied to DuckDB's dialect (`->>`, `from_json`, `unnest`, `try_cast`, `qualify`);
  porting is roughly 150 mechanical edits.
- Single-node memory was the first limit the league-wide run hit (see D18). A warehouse removes
  it; locally the build now needs about 12 GB.
- The Python matcher model doesn't carry over cleanly: dbt-bigquery runs Python models on
  Dataproc (see D15).
- The plan to document the BigQuery port instead of building it was never carried out.

**Verdict:** Keep. Add a short "Running on BigQuery" section to the README covering dialect,
landing in GCS, bronze partitioning, where the matcher runs and league scoping, fed by the
verdicts on D3, D15 and D17.

**Revised (2026-10-20):** the README section was removed to keep the demo DuckDB-only. The port
notes stay in this log (D3, D15, D17).

## D2. Landing zone: raw files per game, hive folders

**Decision:** Each extractor writes one JSON file per (source, game) to
`data/landing/source=…/league=…/season=…/<game_id>.json`, with a `.meta.json` sidecar (format,
mode, URL, extraction time). An existing file means "already fetched", so landing doubles as
the extract cache. `refresh_recent_days` re-pulls recent games and overwrites their files.

**Why:** Fetching stays separate from loading; reruns fetch nothing new; a game is the unit
Prefect retries; hive folders read natively in DuckDB and as BigQuery external tables.

**Alternatives:** Load straight into bronze with no files; one file per season; immutable files
per pull (versioned landing).

**Evaluation:**
- Holds up: the cache made the league-wide iterations cheap (a rerun after a fix fetched only
  what was missing).
- Landing is not write-once: a refresh overwrites the file, so history lives only in bronze, and
  bronze sits in `data/pbp.duckdb`, which `make clean` deletes. After that, earlier versions
  cannot be replayed.
- "As-is" is not uniform. ESPN responses are saved verbatim; NBA mirror games are rows cut from a
  season CSV and re-serialised as JSON (values kept as strings).
- Versioned landing would only add files for re-pulled games (default window 0). Written on every
  re-pull, a nightly run with a 3-day window would keep about 4 files per game, roughly
  quadrupling landing; written only when the stat-bearing hash changes, versions equal real
  corrections.
- 1.6 GB of uncompressed JSON; compression would cut it about tenfold. Not worth it now.

**Verdict:** Keep overwriting landing in this version, because of local disk space, and make
bronze the protected record of history instead: `make clean` must no longer delete
`bronze.raw_payloads`. Say exactly what each source stores instead of "as-is". In production
(cheap object storage) landing may well be versioned, with a new file written only when the
stat-bearing hash changes, so that bronze can always be rebuilt from landing.

## D3. Append-only bronze, versioned by a stat-bearing hash, plus a snapshot

**Decision:** `load_bronze` appends a row when a game's stat-bearing hash differs from its latest
row; `bronze_latest` reads the newest row per game; the dbt snapshot `snap_bronze_payloads`
records version history from `bronze_game_versions`.

**Why:** A late stat correction must become a new version without overwriting the old one.
Hashing only the stat-bearing part keeps ESPN's changing odds and news from looking like
corrections.

**Alternatives:** Overwrite in place (upsert); hash the whole payload; history from the snapshot
only; history from bronze only.

**Evaluation:**
- Holds up: append-only with "latest wins" is the right core, and the stat-only hash works (the
  league-wide reruns load 2,790 payloads as unchanged). Since D2, bronze is the only record of
  history.
- The snapshot duplicates bronze. Bronze already keeps every version with `ingested_at`, so
  valid-from/valid-to is `lead(ingested_at)` in a view. The snapshot is also weaker: it only sees
  versions present at `dbt build` time, so a version loaded and superseded between two builds is
  never recorded. Snapshots fit sources that get overwritten; bronze doesn't.
- The ESPN hash misses a part staging reads: `base_espn_games` takes the final score from
  `header`, but the hash covers only `plays` and `boxscore`. A header-only correction (the CLE at
  WAS kind of error) would not create a version. Rule: the hash covers everything staging reads.
- The loader's docstring says a row is added only when (source, game_id, payload_hash) is new;
  the code compares with the latest hash, so A→B→A records three rows. The behaviour is right (a
  reverted correction must become latest again); the docstring is wrong.
- Check-then-insert is not a race locally (DuckDB allows one writing process), but would be on a
  warehouse.

**Verdict:** Keep append-only bronze and the stat-bearing hash. Replace the snapshot with a
`bronze_payload_history` view (`lead(ingested_at)` per game). Add ESPN's header scores to the
hash. Fix the docstring. On BigQuery, load with a `MERGE` keyed on (source, game_id,
payload_hash) against the latest version (README port notes). Dropping the project's only
snapshot is deliberate: a snapshot on an append-only source would be the weaker signal.

## D4. NBA official data from a community mirror

**Revised (2026-10-20):** live mode is removed: it was never tested, and cdn.nba.com refuses
scripted requests. The mirror is the only NBA path, and the extractor's docstring says what
replaces it in production (the licensed league feed, as a new `payload_format`). The rest of this
entry is the original review.

**Decision:** NBA Stats play-by-play comes from `shufinskiy/nba_data`, a GitHub mirror of
cdn.nba.com liveData (one compressed CSV per season). `--nba-mode live` read cdn.nba.com
directly but was never tested.

**Why:** cdn.nba.com and stats.nba.com block scripts: on 2026-10-19 cdn.nba.com returned 403 to a
home connection and stats.nba.com timed out. The mirror is the only working source of the
official feed.

**Alternatives:** Live cdn.nba.com (blocked); stats.nba.com via `nba_api` (blocked the same way);
ESPN only (no official pivot); a licensed league feed (Sportradar, Genius), which is what a team
would use.

**Evaluation:**
- Holds up: it is the official data (action numbers, ids, the `possession` field) at 20 MB a
  season, and all 1,230 games pair with ESPN at 99.99%.
- Refresh is a no-op in mirror mode: `mirror_csv()` reuses the cached CSV in `data/cache` and
  never downloads it again, so `refresh_recent_days` re-cuts landing files from stale data and a
  correction in the mirror never reaches bronze.
- The mirror lags: the 2025-26 file was last updated on 2026-04-25, after the regular season, and
  the repo updates in irregular batches. Fine for backfills, not for an in-season nightly run.
- Single maintainer, third party, no SLA. It also carries the 2025-26 playoffs and part of WNBA
  2026, unused here.
- Live mode is untested code behind a flag.

**Verdict:** Keep the mirror as the default here. On refresh, download the season file again
when the remote `ETag`/`Last-Modified` has changed, so refresh does what it says. Keep live mode,
labelled untested. In production the official feed would come from the team's licensed league
data, with the mirror only as a backfill stand-in.

## D5. The WNBA from ESPN only: a single-source league

**Decision:** WNBA games come from ESPN alone, through the same summary endpoint as the NBA, and
land as `source = 'espn'`, `league = 'wnba'`. The model treats the WNBA as a single-source league:
every event is its own cluster (`n_sources = 1`), possessions are derived from ESPN's events (D16),
and the agreement reports cover only games both feeds have.

**Why:** The WNBA's official stats have no public API, and ESPN covers the WNBA with the same
endpoint shape it uses for the NBA. A single-source league is also how college would arrive, so
it is worth handling as a normal case rather than an exception.

**Alternatives:** A licensed WNBA feed, which is what a team would use; the NBA mirror's WNBA 2026
files (incomplete, games 1-182); leaving the WNBA out.

**Evaluation:**
- Holds up: all 329 games ESPN has a summary for load, and every test passes. Nothing in the model
  is special-cased: the matcher has nothing to pair, and gold carries `n_sources = 1`.
- There is no cross-check. ESPN's errors in WNBA games can only be caught against ESPN itself
  (made baskets against its running score and its header final); two such games are in
  `known_data_issues`.
- ESPN answers 404 for two completed 2026 Lynx games, so those games are missing.
- `nba` as a source or column prefix means the NBA Stats feed, so the `nba_*` columns are null on
  WNBA rows.

**Verdict:** Keep. A licensed WNBA feed would make the WNBA a two-feed league like the NBA, with
nothing in the model changing.

## D6. ESPN's undocumented site API

**Decision:** ESPN data comes from `site.api.espn.com/.../summary?event=<id>`, one call per game,
saved verbatim. The game list comes from each team's schedule endpoint (for `ALL`, every team's
schedule merged, each game once). No auth. A 404 on a summary is logged and the game skipped.

**Why:** ESPN is the second feed that makes unifying several providers real. It covers the NBA,
WNBA and college through one endpoint shape, and it is open.

**Alternatives:** A second licensed provider next to the league feed; no second feed (a
single-source pipeline with nothing to reconcile).

**Evaluation:**
- Holds up: about 1,600 calls at 4 parallel workers ran without throttling. Clocks agree with the
  official feed to within 0.1 s on almost every event, so ESPN is likely built from the league's
  data: a strong cross-check rather than an independent observation. Reconciling against it found
  real errors, and it carries a game the NBA feed doesn't (the NBA Cup final), coordinates for
  heaves and the whole WNBA (D5).
- Its error modes are now well documented: running scores that end short (8 games), repeated plays
  (DEN at MIN), missing plays (OKC at NOP, LAC at CHI), text contradicting its own player ids
  (about 14 cases), untyped "No …" codes, out-of-order sequence numbers, and 404s for two completed
  games. All are handled; together they confirm ESPN must never win field precedence (D13).
- Undocumented and unversioned: a field rename would break staging. The `event_type_map` test
  (D10) catches new codes but not renamed or missing fields. Terms of use are a grey area.
- The `espn-api` concurrency tag has no limit configured; politeness rests on the 4-worker pool
  (see D8).

**Verdict:** Keep. Add a schema guard: a test that fails when summary payloads lack `plays`,
`boxscore` or `header`, so an API change fails loudly instead of yielding zero events.

## D7. Flow shape: subflow per target, task per game, one dbt build

**Decision:** `pbp_flow` runs one `extract_league` subflow per target, one after another. Inside
it, a 4-thread task runner runs one task per ESPN game and, for the NBA, one task for the mirror
season. Then one `load_bronze` task, one `dbt_build` task (a `dbt build` subprocess, scoped by
league when only some leagues were touched) and one report task that publishes a Prefect markdown
artifact.

**Why:** Per-game tasks let a failed call retry on its own. Loading, building and reporting cover
the whole project, so they run once.

**Alternatives:** One task per source; one flow per league; dbt through `prefect-dbt` (each dbt
node visible in Prefect); concurrent subflows; a Prefect deployment with a schedule.

**Evaluation:**
- Holds up: retry granularity worked (a dropped ESPN connection retried one game, not the season),
  and the single scoped build is simple and correct.
- One failed game blocks everything. In the first league-wide run, two ESPN 404s failed the WNBA
  subflow and the parent flow, so nothing that had landed was loaded or built.
  404s are handled now, but any other persistent per-game failure still blocks the run.
- dbt is a black box to Prefect: one subprocess, and on failure only the last 15 lines of output.
  `prefect-dbt` (`PrefectDbtRunner`, compatible with the installed Prefect 3.8 and dbt-core 1.12)
  runs dbt through its Python API and reports every model, test, seed and snapshot to Prefect
  with its own status, logs and lineage. It is the idiomatic integration for a team running dbt
  under Prefect; the cost is a pre-1.0 dependency whose API still moves.
- Running the two targets concurrently would save at most the shorter subflow (about 20 seconds)
  and only on a full backfill: nightly runs extract a handful of games. Both subflows call ESPN,
  so it would also double concurrent load on an undocumented API, and it complicates failure
  handling. Rejected.
- There is no deployment: each run starts a temporary local Prefect server, with no schedule,
  work pool or concurrency limits. Right for `make all`; production needs a deployment with a
  nightly schedule and `refresh_recent_days`.

**Verdict:** Keep the shape. Tolerate per-game failures: load and build whatever landed, then fail
the run at the end, listing the games that did not land. Run dbt through `prefect-dbt`. Concurrent
targets rejected. A scheduled deployment is a production note, not a change here.

## D8. Retries in the HTTP helper and in Prefect tasks; skip ESPN 404s

**Decision:** Two retry layers. `http.get` retries 4-5 times with 2/4/8/16 s backoff and a 45 s
timeout (since the league-wide run, not on 4xx other than 429). Each Prefect extract task retries
2-3 more times after 5/20/60 s. ESPN summary 404s are caught and the game skipped.

**Why:** Unofficial endpoints have occasional blips (dropped ESPN connections in the league-wide
run).

**Alternatives:** One layer only; Prefect's `retry_condition_fn` to retry only errors worth
retrying (available in the installed Prefect 3.8, with `retry_jitter_factor`); honouring
`Retry-After` on 429.

**Evaluation:**
- Holds up: transient failures recovered unnoticed.
- The layers multiply: one ESPN game can make 5 HTTP attempts x 4 task runs = 20 requests, with
  about 3½ minutes of sleeps plus up to 45 s of timeout each. The two ESPN 404s cost about that
  before failing. The 4xx fix applies only inside the HTTP layer; a 403 still gets retried by
  Prefect three more times.
- HTTP-level retries are invisible in Prefect (log warnings only); the UI shows "Completed" for a
  game that needed 20 attempts. Task-level retries appear with state history.
- No jitter and no `Retry-After` handling: four workers retry on the same schedule, so a throttled
  API sees synchronised bursts.

**Verdict:** One retry layer, owned by Prefect. `http.get` keeps its timeout and raises at once;
extract tasks get retries with backoff, `retry_jitter_factor` and a `retry_condition_fn` that
retries only timeouts, connection errors, 5xx and 429, waiting for `Retry-After` when sent. The
ESPN 404 skip stays. The mirror download, one large request, keeps a short HTTP-level retry.

## D9. One shared `stg_events` schema for every source

**Decision:** Each source has a `stg_<source>` model mapping its events to one 28-column shape
with five event classes (shot, free throw, rebound, turnover, foul). `stg_events` is
`stg_nba union all by name stg_espn`; the matcher and all marts read only `stg_events`.
Class-specific fields (`ft_number`, `rebound_type`, `x_ft`/`y_ft`) are nullable columns.

**Why:** The matcher needs one ordered stream per game and source, and a new source should only
need one model written against a known shape (the README's recipe, and the Second Spectrum plan).

**Alternatives:** One staging table per event class; source-native schemas mapped only at gold;
an enforced dbt contract on the shared shape.

**Evaluation:**
- Holds up: the WNBA plugged in with no downstream changes, and the shape carries about 1.1 M
  events across both leagues.
- The contract is implicit, and `union all by name` hides mistakes: a new source that misspells a
  column adds an extra column and leaves the real one null for that source, with nothing failing.
  dbt model contracts (`contract: {enforced: true}` with declared types, supported by dbt-duckdb)
  make the shape a checked interface.
- Source-specific columns leak into the shared shape (`nba_x_legacy_ft`/`nba_y_legacy_ft` for the
  NBA coordinate check, NBA-only `is_deadball`). Tolerable as nullable columns.
- Assists, steals and blocks are not modelled. NBA logs blocks and steals as separate actions
  (mapped `ignore`); ESPN embeds them as a second participant. The unified model cannot answer
  "assists by player", and the scope cut was not written down.
- Storage: every model is a table. `stg_nba` and `stg_espn` store the same 1.1 M events as
  `stg_events` (about 100-200 MB, each builds in about a second: not worth changing). The real
  duplication is `bronze_latest`, a table that copies every payload's full JSON, about 1.7 GB, as
  large as bronze itself. It is a cheap filter (newest row per game) over huge rows: the clearest
  case for a view.

**Verdict:** Keep the shared schema. Enforce a dbt contract on `stg_events`. Record assists,
steals and blocks as out of scope; the next step is attributes on the parent event
(`assist_player_key` on shots, `steal_player_key` on turnovers, `block_player_key` on missed
shots). Make `bronze_latest` a view; if carrying the payload through the window raises staging's
memory peak, pick the newest key from metadata columns first and join back for the payload. The
per-source models stay tables.

**Outcome:** The database went from 3.6 GB to 2.3 GB. The view needed two attempts. The window
version, and then a plain join that DuckDB chose to hash on the payload side, both ran
`base_nba_actions` out of memory; finding the newest version from metadata and applying it as a
semi join keeps the hash table at 2,790 keys and lets payloads stream. Building the NBA source type
(D10) by wrapping the model's final union in a CTE also tipped it over the limit, so the type is
computed in each branch through a macro instead.

## D10. `event_type_map` seed; an unmapped type fails the build

**Decision:** Every source event type is listed in `seeds/event_type_map.csv` with a class (shot,
free throw, rebound, turnover, foul or `ignore`). `assert_event_types_mapped` fails the build when
a feed sends a type the seed does not list.

**Why:** The APIs are undocumented; a new code should stop the build, not vanish.

**Alternatives:** Classify in SQL `case` statements; classify by description keywords; map
unknown types to `ignore` with a warning.

**Evaluation:**
- Holds up: the league-wide run stopped on 16 codes the Washington games never had, and each got
  a deliberate decision.
- The test checks that a type is listed, not that it is classified correctly. Two mistakes got
  through: ESPN's "No Turnover" mapped to `ignore` (it is an untyped turnover) and NBA double fouls
  missing the `double personal` entry (NBA marks them with a `double` descriptor). An `ignore`
  drops events silently; both surfaced only in a manual review of disagreements. A mis-mapped type
  has a clear signature: its events almost never pair with the other feed (58 of 58 NBA double
  fouls unpaired).
- The NBA type string (`'foul:' || sub_type`, plus the `double` descriptor rule) is built twice,
  in `stg_nba` and in the test, so the two can drift.

**Verdict:** Keep the seed and the fail-on-unmapped test. Add a report of pairing rate per
source type and a warn-level test for any mapped type with enough events and a low pairing rate.
Build the NBA type string once, as a column in `base_nba_actions`, read by both `stg_nba` and the
test.

## D11. Crosswalks: canonical game key, name-based player matching per game and team

**Decision:** Games: NBA and ESPN games pair on league, home team, away team and tip-off within
12 hours (full outer join); the key is readable, e.g. `nba_20251022_WAS_at_MIL` (league,
Eastern-time date, teams). Players: the feeds share no ids, so players are matched by name within
each game and team in three passes (short name, unique last name, name tokens); each NBA player's
match is the ESPN athlete they matched in the most games, with that share as
`match_confidence`. Keys are `nba_<person_id>`, or `espn_<athlete_id>` for ESPN-only players.

**Why:** Matching within a game and team means same-name players on different teams never
collide, and a traded player is matched per team.

**Alternatives:** A surrogate game key (official game id or a hash); one global name match; a
hand-maintained player id seed; fuzzy string matching.

**Evaluation:**
- Holds up: all 1,230 two-feed games paired (the NBA Cup final, ESPN only, correctly stays
  unpaired). Of 582 NBA players with events, 570 matched on short name, 10 on last name, 2 on name
  tokens.
- The vote earned its keep: Seth and Stephen Curry are both "S. Curry" on Golden State. In 3 of
  Seth's 10 games the per-game pass could not tell them apart; the vote resolved it (confidence
  0.7, the only match below 1.0), and events carry the right player.
- The game key is built from data that can change: a tip-off corrected across midnight Eastern, or
  a franchise changing tricode, changes the key and every `cluster_id` that contains it. A
  readable natural key is worth that low risk, but it was not written down.
- `match_confidence` is computed but nothing reads it; the Curry case surfaced only on inspection.
- WNBA players have no NBA-feed id, so their keys are `espn_<athlete_id>` (D5).

**Verdict:** Keep both crosswalks. Add a warn-level test for player matches with confidence below
0.9. Document the game key's stability trade-off and the Curry case.

## D12. Cluster = one real event; gold is one row per cluster

**Decision:** The matcher writes `int_event_matches` (one long row per source event with its
`cluster_id`). `int_clusters` pivots it to one wide row per cluster, with `nba_*` and `espn_*`
columns generated by a Jinja loop over the source list. Gold (`fct_shots`, `fct_rebounds`) has one
row per cluster with `n_sources`, agreement flags and both feeds' event ids. `cluster_id` is
`game_key:N<NBA action number>`, or `game_key:E<ESPN play id>` for an ESPN-only event.

**Why:** One row per thing that happened on the court is the grain users want, and every value
stays traceable to its source rows.

**Alternatives:** Gold from one source with the other as a lookup; a long gold table (one row per
source per event); clusters allowed to span event classes or teams.

**Evaluation:**
- Holds up: the grain is clear and tested (unique cluster ids; points sum to the final score per
  game). Disagreements are rows, not silent coalesces. Cluster ids are stable across reruns because
  both feeds' event ids are.
- A cluster holds at most one event per source, of one class and one team. Three real patterns
  cannot be expressed: the same play as a shot in one feed and a turnover in the other (9 pairs),
  a team rebound credited to opposite teams (8 pairs), and one-to-many events (ESPN's single
  double-foul row against NBA's two). Each becomes two singletons, so gold counts the play in both
  forms; for example 9 ESPN-only missed shots in `fct_shots` are turnovers in NBA, slightly
  inflating attempts.
- Adding a source is less free than the README says. The `int_clusters` pivot is one list entry,
  but precedence in `fct_shots` and `fct_rebounds` (about 30 `nba_*`/`espn_*` references each) and
  `rpt_disagreements` (20) would need edits. The tables don't change; the code does.

**Verdict:** Keep the cluster model; don't let clusters span classes or teams, which would
complicate every consumer. Add a suspected-counterpart link to `rpt_disagreements`: for each
singleton, the nearest singleton from the other feed in the same game within about 2 s, so a
two-singleton pair reads as one case and the inflated attempts can be counted. Correct the
README's claim that a new source leaves the backbone unchanged. Precedence as data is D13's
question.

## D13. NBA as pivot; field-level precedence with `*_source` columns

**Decision:** The official feed is the matcher's pivot: other sources pair against it and cluster
ids come from its action numbers. In gold, each field comes from the official feed when it has a
value, otherwise from ESPN; `player_key_source`, `is_made_source` and `xy_source` record which.
On an end-of-period heave NBA logs only a team-level action, so player and coordinates come from
ESPN.

**Why:** The official league feed is the record of truth for stats. A per-field rule fills one
feed's gaps from the other without hiding where each value came from.

**Alternatives:** Whole-row precedence; majority or confidence voting (needs 3+ sources);
precedence as data (a seed of source priority per field, applied by a macro).

**Evaluation:**
- Holds up: the league-wide ESPN error catalogue (running scores, repeated plays, text-vs-id
  contradictions, 2-vs-3 errors) is the case for official-first. Where one side was clearly wrong,
  it was ESPN, apart from buzzer shots NBA doesn't record; gold already takes NBA's value.
- Per-field mixing can produce a row neither feed asserted. Today it only fills NBA nulls
  (heaves), so the risk is theoretical; it grows with each source.
- The pivot doesn't extend cleanly to three sources: everything pairs against the official feed,
  so an event it missed cannot be matched between ESPN and Second Spectrum, and becomes two
  singletons. "Clusters get up to three members" holds only for events the official feed has.
- Precedence lives in code (about 30 references per fact table). With two sources a seed would be
  over-engineering; with a third, which would win coordinates while NBA wins results, it is the
  right shape.
- Only three fields have a `*_source` column. Shot value, team and clock come from ESPN only on
  ESPN-only rows, which `n_sources = 1` already shows.

**Verdict:** Keep official-first, per-field precedence. No code change. Document the three-source
caveats here and in the README's Second Spectrum section: non-pivot sources match only through
the pivot, and precedence should become data when a third source arrives.

## D14. Matcher: score anchors, then order-preserving alignment

**Decision:** Per game: (1) score anchors pair made baskets on (period, score after, class, team,
points), if clocks are within 12 s and the players are not different; (2) between anchors, each
event class is aligned by an order-preserving dynamic program (like `diff`) maximising a
hand-weighted similarity (same player, result, value, close clock); (3) a safety pass per period
over anything still unpaired.

**Why:** The score only moves on made baskets, so anchors are near-certain pairs that cut a game
into short stretches; order then separates fast sequences a clock window cannot (miss, offensive
rebound, putback miss, second rebound inside two seconds).

**Alternatives:** A clock window (the baseline); anchors on score alone; optimal assignment per
class (Hungarian) with a cost; a learned model.

**Evaluation:**
- Holds up: over 1,231 NBA games it leaves 7 made baskets unpaired, all shots ESPN doesn't have,
  against 13 (in 7 games) for the clock window and 167 (in 73) for score-only anchors. Both anchor
  rules exist because real data broke the simpler version (MIN at WAS; BOS at DET and MIN at DEN).
- No ground truth. Unpaired made baskets is a proxy for double counting and says nothing about
  wrong pairs; some of the ~112 player-credit disagreements could be adjacent same-class events
  paired in the wrong order (hand-check #7 is ambiguous this way). The synthetic evaluation was
  removed deliberately, so precision cannot be measured today.
- The weights are hand-set; their sensitivity is unknown.
- Known edge losses: PHI at NOP cross-paired two free throws (alignment allows a different
  shooter; the feeds ordered them differently); a Vučević miss logged 14 s apart exceeds the 12 s
  limit. A harder penalty on a different free-throw shooter would backfire: ESPN's text-vs-id
  errors would become unpaired singletons that double-count points.
- Nothing guards the anchor fixes against regression beyond the points tests catching it by
  chance.

**Verdict:** Keep the algorithm. Add a regression seed of known pairs (event pairs that must share
a cluster: BOS at DET Queta/Pritchard, MIN at DEN Jokić/Murray, the MIN at WAS Gobert/Middleton
collision and reviewed hand-checks) with a dbt test that each pair shares a `cluster_id`. Run a
sensitivity sweep of the weights and the 12 s limit, scored on both unpaired made baskets and the
known pairs: tuned on unpaired baskets alone, a sweep would reward over-aggressive pairing.

## D15. Matcher runs as a dbt Python model

**Decision:** `int_event_matches` is a dbt Python model: it reads `stg_events` into pandas,
calls `pbp.matching.match_events` from the installed package and returns the matches. For
league-scoped builds it filters the selected leagues and carries the other leagues' rows over
itself, because dbt-duckdb's incremental path for Python models fails on a local database.

**Why:** The alignment is about 40 lines of dynamic programming, clear in Python and unreadable in
SQL. As a dbt model it sits in one DAG with lineage, tests and docs, and the evaluation imports the
same function, so what is evaluated is what runs.

**Alternatives:** A Prefect task between two dbt builds, writing a table dbt reads as a source; a
SQL implementation (window functions for anchors, a greedy pass instead of the dynamic program);
a UDF.

**Evaluation:**
- Holds up: locally it is the simplest option: one `dbt build`, complete lineage, 1,560 games
  matched inside the normal build, and the evaluation cannot drift from production.
- On BigQuery this decision changes most. dbt-bigquery runs Python models on Dataproc (Spark):
  separate infrastructure and cost, minutes of start-up per run, and `src/pbp` packaged onto the
  cluster; `.df()` would pull all events to one driver.
- The league-scope carry-over (about 10 lines) works around a dbt-duckdb limitation.
- All events go into pandas in one process; fine at 1.2 M rows. Matching is independent per game,
  so it could stream game by game if it ever needs to.

**Verdict:** Keep the dbt Python model here; it is the right choice on DuckDB. Production design
(log and README BigQuery section): the matcher as a Prefect task between two dbt builds
(`dbt build -s +stg_events`, match per league or per game, write a partitioned table declared as a
dbt source, then build downstream), with `prefect-dbt` (D7) keeping both halves visible.

## D16. Possessions from the NBA feed's `possession` field, or from ESPN's events

**Decision:** In games the NBA feed covers, a possession is a run of consecutive actions with the
same `possession` team, reset each period; an offensive rebound continues it. In games only ESPN
covers (every WNBA game, the NBA Cup final) there is no such field, so the team with the ball is
set by shots, non-technical free throws, rebounds and turnovers and carried forward over fouls
(`boundary_source = 'espn'`). In NBA-feed games, events only ESPN recorded take their team's
possession at that clock; technical free throws before a period's first possession belong to
none. Ids are per-game sequence numbers (`game_key:P001`). Tests: the two teams' counts differ by
at most 1 per period, and possession points add up to the final.

**Why:** The NBA feed already knows who has the ball; deriving from it avoids re-implementing the
rulebook. Where ESPN is the only feed, the event rule is the simplest one that gets possessions
right.

**Alternatives:** The event rule everywhere; boundaries from ESPN everywhere; no possessions, pace
estimates instead.

**Evaluation:**
- Holds up: the NBA field matches the acting team on all 384,430 field goals, rebounds and
  turnovers. Counts pass a basketball sanity check: 101.6 possessions per team per game in the NBA
  and 80.3 in the WNBA, in line with league pace (overtime lifts the NBA figure slightly). Both
  tests pass in every game.
- The ESPN rule is not checked against an independent signal in the WNBA; the tests and the pace
  make it plausible, not proven. The NBA games are where it can be proven: there the NBA field is
  the answer.
- Ids are positional: a correction that inserts an action shifts every later id in that game.
  Gold is rebuilt in full, so nothing breaks here, but ids stored outside the warehouse would move.

**Verdict:** Keep. Cross-check the ESPN rule on the NBA games: derive possessions from ESPN's
events there too and compare them with the NBA field's. Document that possession ids are
positional.

## D17. League-scoped incremental builds via a dbt var

**Decision:** With `--vars '{leagues: [wnba]}'` every model switches from `table` to
`incremental` (append); a pre-hook deletes the selected leagues' rows and each model reads its
inputs through `scoped()`. Generic tests filter to the same leagues. `rpt_coverage` opts out (a
full table, since it summarises every league) and the Python matcher carries other leagues' rows
over itself. The flow picks the scope from the run's targets.

**Why:** Leagues never share a game, so each is an independent slice; rebuilding the WNBA should
not touch NBA rows.

**Alternatives:** Always a full build; dbt's standard incremental pattern (`is_incremental()` with
a `unique_key`, or `insert_overwrite` by partition on BigQuery); separate targets or schemas per
league; scoping by game or date.

**Evaluation:**
- Holds up: it works, and a scoped build was once checked table by table against a full build by
  content hash. Automatic scoping in the flow is convenient.
- The benefit is small at this scale: a full league-wide `dbt build` takes 36 seconds on a MacBook
  Pro, so a WNBA-only one saves seconds, and nightly runs touch both leagues. Scoping by game or
  date (rebuild only games whose payload changed) is what would pay off.
- The convention is not enforced: a model must read through `scoped()` or opt out explicitly. A
  model that forgets would duplicate other leagues' rows in scoped builds while passing full
  builds.
- It is home-grown. A dbt/BigQuery team would expect incremental models partitioned by game date
  and clustered by league and game, with `insert_overwrite` of the affected partitions, which
  covers league and game scope without a wrapper.
- The scoped-versus-full equivalence check is not in the repo; it was a one-off.

**Verdict:** Keep. Add a guard that fails when a model reads `ref()` or `source()` outside
`scoped()` without an explicit full-table opt-out. Commit the equivalence check as a script so
the README's claim is repeatable. Document date-partitioned `insert_overwrite` incrementals as the
BigQuery replacement and the route to game-scoped builds.

## D18. dbt runs one model at a time (`threads: 1`)

**Decision:** `profiles.yml` sets `threads: 1`, added after the first league-wide build ran out of
memory at DuckDB's 19 GB limit with four models in parallel.

**Why:** The staging models that parse raw JSON payloads are memory-heavy; several at once
exceeded the limit.

**Alternatives:** DuckDB settings (`memory_limit`, `preserve_insertion_order`); leaner heavy
models; four threads and the risk; parsing JSON before bronze.

**Evaluation** (measured model by model on the league-wide data):

| Model | Peak memory | Time |
|---|---|---|
| `base_nba_actions` | 7.6 GB | 21 s |
| `bronze_latest` (copies every payload) | 6.9 GB | 2 s |
| `base_espn_plays` | 5.6 GB | 5 s |
| `fct_rebounds` | | 71 s |
| `int_event_matches` | | 13 s |

- Holds up: one thread made the build reliable on a 24 GB machine in about 2 minutes. On a 16 GB
  machine DuckDB's default limit is about 12.8 GB, which the 7.6 GB peak fits.
- Little parallelism is given up: most time sits in models that run in sequence anyway.
- `fct_rebounds` spends 71 s, more than half the build, in a nested-loop join (about 390 CPU
  seconds across threads). The cause is a condition on the left table only
  (`c.nba_event_id is null`) inside a left join's ON clause, which stops DuckDB from hashing the
  join. The pattern appeared twice; the review first blamed the possession-span join, but the one
  that mattered was the join to ESPN's previous-miss ordering.
- Memory comes from parsing whole JSON documents, about 7x the payload size.
  `preserve_insertion_order = false` made no difference.

**Verdict:** Keep `threads: 1`. Move left-table-only conditions out of ON clauses in
`fct_rebounds`, and place ESPN-only rebounds in a filtered CTE the way `fct_shots` does. Re-measure
after `bronze_latest` becomes a view (D9) before considering more threads.

**Outcome:** `fct_rebounds` builds in 0.4 s instead of 71 s and the whole `dbt build` in about
50 s instead of about 2 minutes (36 s on a MacBook Pro from a fresh clone). Peak memory is
unchanged at about 12 GB, so `threads: 1` stays.

## D19. Tests encode basketball; known source errors live in a seed

**Decision:** Besides generic tests (unique, not_null, relationships, accepted_values), 12
singular tests check basketball invariants: made points equal the final (per source, in unified
gold and in possessions), every rebound follows a miss, possessions alternate, attempt counts agree
across feeds, every event type is mapped, the court-frame flip matches NBA's coordinates, every
player resolves, no league comes from a source that doesn't cover it, and ESPN's play-by-play final
matches its header (warn). A genuine source error does not loosen a test: it becomes a row in
`seeds/known_data_issues.csv` (source, game, test, note) and that test skips that game.

**Why:** The sources are undocumented, so the real specification is how basketball works.
Recording known errors as data keeps every test strict and every exception reviewable.

**Alternatives:** Looser thresholds; warn-only tests; per-event exclusions; dbt unit tests on fixed
inputs; Python unit tests for the matcher.

**Evaluation:**
- Holds up: the strongest part of the project. In the league-wide run the tests caught every real
  problem: ESPN's data errors, the cross-player anchor bug, double-counted free throws and missing
  possessions. The seed grew to 17 reviewed entries, each naming the plays involved.
- Exclusions are per (test, game), which is coarse: DEN at MIN is excluded from three tests, so a
  second, unrelated error in that game would be hidden.
- Known issues never expire: if a source corrects a game, bronze gets a new version (D3) but the
  exclusion stays and keeps hiding the test.
- Some exemptions are rules in YAML (the possession `not_null` filter: official events only,
  technical free throws exempt), documented only in the column description. As rules rather than
  per-game exceptions, YAML is the right place.
- Nothing is tested below the integration level: no dbt unit tests (available since dbt 1.8) and
  no Python tests for `matching.py` since the synthetic evaluation was removed. Clock parsing, name
  normalisation, possession derivation and the anchor rules are checked only through season-wide
  outcomes.

**Verdict:** Keep the approach. Add a check that every known issue still reproduces (warn when an
excluded failure has gone away, so stale rows get removed). Let known issues name specific events
for tests that report events, instead of excluding whole games. Add dbt unit tests for the
trickiest SQL logic (clock parsing, name normalisation, possession derivation) and pytest tests for `matching.py` built from small real-case fixtures (the anchor
collisions and the free-throw cases).

## D20. Coverage, reconciliation and matcher reports

**Decision:** `rpt_coverage` (which source covers which league, games delivered),
`rpt_source_agreement` and `rpt_disagreements` (match rates per game and class; one row per
cluster where the feeds disagree), `reports/reconciliation.md` (what the run loaded and how well
the sources agree, also published as a Prefect artifact) and `reports/matcher_evaluation.md` (the
three matchers on unpaired made baskets). All regenerate each run; the markdown reports are
committed.

**Why:** A reconciliation pipeline should show its evidence on every run, not only in the README.

**Alternatives:** dbt docs and exposures only; a dashboard; reports only as Prefect artifacts.

**Evaluation:**
- Holds up: the Prefect artifact is the right home for per-run evidence, and `rpt_disagreements`
  drove the whole league-wide review.
- That review lives only in prose: 293 disagreements were sorted by hand into about eight patterns
  (text vs id, shot vs turnover, buzzer shots, team rebound to opposite teams, …), and the next run
  starts from zero. Most patterns are mechanical to detect.
- Committed reports churn: every run rewrites the timestamp and the bronze-load counts, producing
  diffs with no content.
- The matcher evaluation covers NBA games only, which is all there is to evaluate: the WNBA has
  one feed.
- The reports overlap with each other and with `rpt_source_agreement`; tolerable, as one is about
  the run and the other about the matcher.

**Verdict:** Keep. Add a rule-based `pattern` column to `rpt_disagreements` from the league-wide
review, so only unclassified cases need a person (with D12's suspected-counterpart link). Drop the
timestamp and bronze-load counts from the committed
reports; the Prefect artifact keeps per-run detail.

## Codebase review (2026-10-20)

A read of every file after the follow-ups above, plus a league-scoped build (WNBA only; row
counts unchanged in both leagues) and a fresh-clone run (`make clean-all && make all`).

Fixed:
- Landing writes were not atomic. An interrupted run could leave a truncated payload or a payload
  without its `.meta.json`; it would "exist", be skipped by every later run and break
  `load_bronze`. The same held for a half-downloaded mirror archive. Every landing file now goes
  through `config.write_landed` (sidecar first, payload via a temporary file and a rename), and
  the mirror archive and CSV through temporary files.
- The reconciliation report's "games with the most disagreements" was topped by the games only
  one feed covers, which are single-source by definition; it now lists two-feed games only.
- The matcher evaluation counted the ESPN-only Cup final among "NBA games"; it now counts games
  with an official feed.
- `--games` filters ESPN only (the official feed still lands every game of the target); the help
  text now says so.
- README: the bronze key in the diagrams was `payload_hash`, which can repeat (A, B, A); a
  version is keyed by `ingested_at`. "The matcher and gold models don't change" when adding a
  source contradicted D12; corrected. The scoped-build equivalence claim now says it was a
  one-off check.

Open (added to the backlog):
- In live NBA mode a refresh re-downloads every game of the season, not only recent ones:
  `extract_live` gets the season-wide `refresh` flag. Untested code, so recorded, not changed
  (moot since live mode was removed, D4).

## Follow-up backlog

Every change agreed above, grouped by where it lands.

**Ingestion and sources**
- [x] `make clean` keeps `bronze.raw_payloads` (D2)
- [x] Describe exactly what each source stores instead of "as-is" (D2)
- [x] Replace `snap_bronze_payloads` with a `bronze_payload_history` view on `lead(ingested_at)` (D3)
- [x] Add ESPN header scores to the stat-bearing hash; fix the `load_bronze` docstring (D3)
- [x] Re-download the NBA mirror on refresh when its `ETag`/`Last-Modified` changed (D4)
- [x] The WNBA from ESPN only, as a single-source league (D5)
- [x] Remove the untested NBA live mode (D4, revised)
- [x] Pin every transitive dependency (`requirements.lock`): a fresh install pulled fastapi 0.155, which breaks Prefect 3.8.8
- [ ] Schema guard: fail when ESPN summaries lack `plays`, `boxscore` or `header` (D6)

**Orchestration**
- [x] Load and build whatever landed; fail at the end listing games that did not land (D7)
- [ ] Run dbt through `prefect-dbt` (D7)
- [x] One Prefect-owned retry layer with jitter, `retry_condition_fn` and `Retry-After` (D8)

**Modeling and matching**
- [ ] Enforced dbt contract on `stg_events` (D9)
- [x] `bronze_latest` as a view (D9)
- [ ] Pairing-rate per source type report and warn test (D10)
- [x] Build the NBA source type once, in `base_nba_actions` (D10)
- [ ] Warn test for player match confidence below 0.9 (D11)
- [ ] Suspected-counterpart link in `rpt_disagreements` (D12)
- [ ] Known-pairs regression seed and test; weight and clock-limit sensitivity sweep (D14)
- [x] Possessions for ESPN-only games (D16)
- [ ] Cross-check the ESPN possession rule on NBA games, against the NBA field (D16)
- [ ] Guard against unscoped refs; commit the scoped-versus-full equivalence check (D17)
- [x] Remove the nested-loop join in `fct_rebounds` (D18)

**Quality and reports**
- [ ] Known issues must still reproduce; event-level exclusions (D19)
- [ ] dbt unit tests for clock parsing, name normalisation, possession derivation (D19)
- [ ] pytest for `matching.py` from real-case fixtures (D19)
- [ ] Rule-based `pattern` column in `rpt_disagreements` (D20)
- [x] Drop volatile lines from committed reports (D20)

**From the codebase review**
- [x] ~~Live NBA mode: refresh only the recent games~~ moot: live mode removed

**Documentation only**
- [x] README "Running on BigQuery" section (later removed from the README to keep the demo DuckDB-only; the notes stay here): dialect, GCS landing (versioned in production), bronze
  `MERGE`, matcher as a Prefect task between dbt builds, date-partitioned `insert_overwrite`
  incrementals (D1, D2, D3, D15, D17)
- [x] Assists, steals and blocks out of scope; next step is attributes on parent events (D9)
- [x] Game key stability trade-off and the Curry case (D11)
- [x] Three-source caveats in the Second Spectrum section; correct "the backbone doesn't change" (D12, D13)
- [x] Possession ids are positional (D16)
