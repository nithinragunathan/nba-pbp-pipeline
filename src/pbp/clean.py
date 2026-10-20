"""Drop everything dbt built, keep bronze. Bronze is the only record of payload history (landing
files are overwritten on refresh), so `make clean` must not delete it; `make clean-all` does."""
from __future__ import annotations

import duckdb

from pbp import config

KEEP = {"bronze", "main", "information_schema", "pg_catalog"}


def main() -> None:
    if not config.DB_PATH.exists():
        return
    with duckdb.connect(str(config.DB_PATH)) as con:
        schemas = [r[0] for r in con.execute(
            "select schema_name from duckdb_schemas() where database_name = current_database() and not internal"
        ).fetchall()]
        for schema in schemas:
            if schema not in KEEP:
                con.execute(f'drop schema "{schema}" cascade')
    print(f"dropped dbt schemas from {config.DB_PATH.name}; bronze kept")


if __name__ == "__main__":
    main()
