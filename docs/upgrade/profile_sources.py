"""Read-only source inspection for the upgrade plan; emits aggregate evidence only.

Run from the repository root: .venv/Scripts/python.exe docs/upgrade/profile_sources.py
No warehouse tables or raw files are modified. SQL and results are recorded together.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import importlib.metadata
import json

import duckdb


def build_profile(root: Path) -> dict:
    con = duckdb.connect(":memory:")
    evidence = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Local CSV files in full; optional existing warehouse, read-only.",
        "caveats": [
            "Counts of distinct identifiers do not establish counts of business orders.",
            "Source provenance, anonymization and identifier semantics are unverified.",
            "Stored timestamps have no verified timezone; no timezone conversion applied.",
            "Warehouse results describe the existing materialization, not a fresh build.",
        ],
        "versions": {}, "sources": {}, "queries": [],
    }
    for package in ("duckdb", "dbt-core", "dbt-duckdb"):
        evidence["versions"][package] = importlib.metadata.version(package)

    def record(query_id, sql, connection=con):
        cursor = connection.execute(sql)
        names = [col[0] for col in cursor.description]
        rows = [dict(zip(names, row)) for row in cursor.fetchall()]
        evidence["queries"].append({"id": query_id, "sql": sql, "rows": rows})
        return rows

    for channel, order_col, created_col, sync_col in (
        ("tiktok", "order_id", "created_time", "updatedAt"),
        ("shopee", "order_sn", "create_time", "synced_at"),
    ):
        path = root / "data" / "raw" / f"{channel}_orders.csv"
        file_hash = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                file_hash.update(block)
        # Explicit string types preserve identifier digits and permit safe cast checks.
        load_sql = f"create temp table {channel} as select * from read_csv(?, all_varchar=true)"
        con.execute(load_sql, [str(path)])
        columns = [col[0] for col in con.execute(f"select * from {channel} limit 0").description]
        evidence["sources"][channel] = {
            "file": path.relative_to(root).as_posix(), "bytes": path.stat().st_size,
            "sha256": file_hash.hexdigest(), "columns": columns,
            "column_count": len(columns), "load_sql": load_sql,
            "load_parameters": [path.relative_to(root).as_posix()],
        }
        record(f"{channel}-summary", f"""
select count(*) as rows, count(distinct {order_col}) as distinct_order_ids,
 count(distinct (user_id, shop_name, {order_col})) as user_shop_order_keys,
 count(distinct pkId) as distinct_pk_ids, count(distinct shop_name) as shop_names,
 count(distinct user_id) as users,
 min(try_cast({created_col} as timestamp)) as min_created,
 max(try_cast({created_col} as timestamp)) as max_created,
 min(try_cast({sync_col} as timestamp)) as min_sync,
 max(try_cast({sync_col} as timestamp)) as max_sync,
 count(*) filter(where pkId is null) as null_pk,
 count(*) filter(where {order_col} is null) as null_order,
 count(*) filter(where {created_col} is not null and try_cast({created_col} as timestamp) is null) as invalid_created,
 count(*) filter(where {sync_col} is not null and try_cast({sync_col} as timestamp) is null) as invalid_sync,
 count(*) filter(where quantity is null or try_cast(quantity as integer) is null) as invalid_or_null_quantity,
 count(*) filter(where try_cast(quantity as integer)<=0) as nonpositive_quantity,
 count(*) filter(where regexp_matches({order_col}, '(?i)e[+-][0-9]+')) as scientific_order_ids
from {channel}""")
        for query_id, group_cols, condition in (
            ("duplicate-pk", "pkId", "count(*)>1"),
            ("pk-multiple-orders", "pkId", f"count(distinct {order_col})>1"),
            ("scoped-pk-multiple-orders", "user_id,shop_name,pkId", f"count(distinct {order_col})>1"),
            ("scoped-orders-multiple-created", f"user_id,shop_name,{order_col}", f"count(distinct {created_col})>1"),
            ("scoped-orders-multiple-status", f"user_id,shop_name,{order_col}", "count(distinct order_status)>1"),
        ):
            record(f"{channel}-{query_id}", f"""
select count(*) as affected_key_groups, coalesce(sum(n),0) as affected_rows
from (select {group_cols},count(*) as n from {channel}
      group by {group_cols} having {condition})""")
        record(f"{channel}-statuses", f"""
select order_status,count(*) as rows,count(distinct {order_col}) as distinct_order_ids
from {channel} group by 1 order by 2 desc""")
        record(f"{channel}-currency", f"select currency,count(*) as rows from {channel} group by 1 order by 2 desc")

    record("shopee-primary-row", "select is_primary_row,count(*) as rows from shopee group by 1 order by 2 desc")
    record("shopee-shopid-keys", "select count(distinct shop_id) as shop_ids,count(distinct (shop_id,order_sn)) as shop_order_keys from shopee")
    record("shared-sku-values", """
select count(*) as shared_literal_sku_values
from (select distinct seller_sku as sku from tiktok where seller_sku is not null) t
join (select distinct item_sku as sku from shopee where item_sku is not null) s using(sku)""")
    warehouse = root / "data" / "duckdb" / "ecom_warehouse.duckdb"
    try:
        with duckdb.connect(str(warehouse), read_only=True) as db:
            record("existing-fact-flags", """
select count(*) as fact_rows,sum(is_successful_revenue) as successful_flags,
sum(is_cancelled) as cancelled_flags,
count(*) filter(where order_status='COMPLETED') as completed_status_rows,
count(*) filter(where order_status='CANCELLED') as cancelled_status_rows
from main_intermediate.fct_orders""", db)
    except duckdb.Error as error:
        evidence["warehouse_read_error"] = type(error).__name__
    con.close()
    return evidence


if __name__ == "__main__":
    root = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / "dbt_transforms").is_dir())
    profile = build_profile(root)
    destination = root / "docs" / "upgrade" / "source_profile.json"
    destination.write_text(json.dumps(profile, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    for query in profile["queries"]:
        if query["id"].endswith("-summary") or query["id"] == "existing-fact-flags":
            print(query["id"], json.dumps(query["rows"], default=str))
    print(f"Saved {len(profile['queries'])} aggregate query results to {destination.relative_to(root)}")
