"""Audit the two local CSV exports without changing inputs or the warehouse.

Run from repo root with the project Python. Results contain aggregate evidence,
exact SQL and fingerprints, never raw customer records or original identifiers.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import csv
import hashlib
import json

import duckdb


CONFIG = {
    "tiktok": {
        "order": "order_id", "created": "created_time", "amount": "total_amount",
        "ids": ["pkId", "user_id", "order_id", "product_id", "sku_id", "seller_sku", "shop_name"],
        "dates": ["createdAt", "updatedAt", "created_time", "paid_time", "rts_time", "shipped_time", "delivered_time", "completed_time", "cancelled_time", "updated_time", "rts_sla_time", "tts_sla_time", "delivery_sla_time", "cancel_sla_time", "auto_cancel_time"],
        "numbers": ["quantity", "total_amount", "sub_total", "order_original_price", "shipping_fee", "original_shipping_fee", "tax_amount", "product_tax", "shipping_tax", "small_order_fee", "retail_delivery_fee", "insurance_fee", "seller_discount", "platform_discount", "shipping_seller_discount", "shipping_platform_discount", "item_sale_price", "item_original_price", "item_platform_disc", "item_seller_disc", "row_idx"],
    },
    "shopee": {
        "order": "order_sn", "created": "create_time", "amount": "buyer_total_amount",
        "ids": ["pkId", "user_id", "shop_id", "order_sn", "item_id", "model_id", "item_sku", "model_sku", "shop_name", "connection_id"],
        "dates": ["create_time", "pay_time", "shipped_time", "completed_time", "estimated_shipping_date", "synced_at", "cancel_time", "ship_by_date", "update_time"],
        "numbers": ["quantity", "original_price", "model_discounted_price", "item_weight", "total_order_value", "buyer_total_amount", "seller_discount", "shopee_discount", "voucher_from_seller", "voucher_from_shopee", "commission_fee", "service_fee", "transaction_fee", "escrow_amount", "actual_shipping_fee", "returned_quantity", "total_weight", "estimated_shipping_fee", "return_shipping_fee", "active_qty", "cancel_requested_qty", "cancelled_qty", "return_requested_qty"],
    },
}


def inspect_csv(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        prefix = stream.read(4)
        stream.seek(0)
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
        stream.seek(-1, 2)
        ends_newline = stream.read() == b"\n"
    widths = Counter()
    repeated_headers = embedded_newlines = blank_records = 0
    bad_rows = []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader)
        for row_number, row in enumerate(reader, 1):
            widths[len(row)] += 1
            repeated_headers += row == header
            blank_records += not row
            embedded_newlines += any("\n" in value or "\r" in value for value in row)
            if len(row) != len(header) and len(bad_rows) < 10:
                bad_rows.append(row_number)
        physical_lines = reader.line_num
    return {
        "sha256": digest.hexdigest(), "bytes": path.stat().st_size,
        "encoding_checked": "UTF-8, strict decoding", "utf8_bom": prefix.startswith(b"\xef\xbb\xbf"),
        "csv_strict_parse": "passed", "csv_rows": sum(widths.values()),
        "columns": header, "column_count": len(header),
        "duplicate_column_names": len(header) - len(set(header)),
        "row_width_counts": dict(widths), "bad_width_row_numbers": bad_rows,
        "repeated_headers": repeated_headers, "blank_records": blank_records,
        "records_with_embedded_newlines": embedded_newlines,
        "physical_lines": physical_lines, "ends_newline": ends_newline,
    }


def audit(root):
    evidence = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duckdb_version": duckdb.__version__, "sources": {}, "queries": [],
        "provenance": "User reports exporting CSV directly using DataGrip from vietnam_ecommerce on Xom Dataset; no custom extraction script.",
        "caveats": [
            "Masked IDs are not reliable business keys; counts of distinct masks are not counts of original entities.",
            "CSV validity does not establish completeness against the unavailable database.",
            "The location where masking was applied is unknown: source, export configuration or another earlier step.",
            "Candidate grouping by masked fields plus creation timestamp is diagnostic, not a validated order key.",
            "All anomaly counts refer to CSV rows or diagnostic key groups, not verified business orders.",
        ],
    }
    connection = duckdb.connect(":memory:")
    connection.execute("set threads=2")

    def record(query_id, sql):
        result = connection.execute(sql)
        rows = [dict(zip([col[0] for col in result.description], row)) for row in result.fetchall()]
        evidence["queries"].append({"id": query_id, "sql": sql, "rows": rows})
        return rows

    for channel, config in CONFIG.items():
        path = root / "data" / "raw" / f"{channel}_orders.csv"
        source = inspect_csv(path)
        source["file"] = path.relative_to(root).as_posix()
        load_sql = f"create temp table {channel} as select row_number() over() as __csv_row, * from read_csv(?, all_varchar=true, parallel=false)"
        connection.execute(load_sql, [str(path)])
        source["duckdb_load_sql"] = load_sql
        source["duckdb_load_parameters"] = [source["file"]]
        evidence["sources"][channel] = source
        oid, created, amount = config["order"], config["created"], config["amount"]
        record(f"{channel}-summary", f"""
select count(*) as rows,count(distinct pkId) as masked_pk_values,
 count(distinct {oid}) as masked_order_values,
 count(*)-count(distinct pkId) as rows_removed_by_masked_pk_dedup,
 min(try_cast({created} as timestamp)) as min_created,
 max(try_cast({created} as timestamp)) as max_created from {channel}""")
        assert source["csv_rows"] == connection.execute(f"select count(*) from {channel}").fetchone()[0]
        record(f"{channel}-exact-duplicates", f"""
select count(*) as duplicate_groups,coalesce(sum(n-1),0) as excess_duplicate_rows,
 coalesce(sum(n),0) as affected_rows
from (select * exclude(__csv_row),count(*) as n from {channel} group by all having count(*)>1)""")
        # These measurements expose only column-level counts, never cell values.
        pieces = []
        for column in source["columns"]:
            pieces.append(f"""select '{column}' as column_name,count(*) as rows,
 count(*) filter(where \"{column}\" is null) as null_rows,
 count(*) filter(where \"{column}\" is not null and trim(\"{column}\")='') as blank_rows,
 count(*) filter(where contains(\"{column}\",'*')) as masked_rows,
 count(distinct \"{column}\") as distinct_nonnull_values from {channel}""")
        record(f"{channel}-column-profile", "\nunion all\n".join(pieces))
        pieces = []
        for column in config["ids"]:
            pieces.append(f"""select '{column}' as column_name,count(\"{column}\") as nonnull_rows,
 count(*) filter(where contains(\"{column}\",'*')) as masked_rows,
 count(*) filter(where regexp_full_match(\"{column}\",'[^*]{{5}}\\*+')) as five_visible_then_stars,
 count(*) filter(where regexp_full_match(\"{column}\",'[0-9a-fA-F]{{5}}\\*{{31}}')) as hex_five_then_31_stars
 from {channel}""")
        record(f"{channel}-id-masking", "\nunion all\n".join(pieces))
        pieces = []
        for column in config["numbers"]:
            pieces.append(f"""select '{column}' as column_name,count(*) as rows,
 count(*) filter(where \"{column}\" is null) as null_rows,
 count(*) filter(where \"{column}\" is not null and try_cast(\"{column}\" as decimal(24,4)) is null) as invalid_numeric_rows,
 count(*) filter(where try_cast(\"{column}\" as decimal(24,4))<0) as negative_rows,
 count(*) filter(where try_cast(\"{column}\" as decimal(24,4))=0) as zero_rows,
 min(try_cast(\"{column}\" as decimal(24,4))) as minimum,
 max(try_cast(\"{column}\" as decimal(24,4))) as maximum from {channel}""")
        record(f"{channel}-numeric-profile", "\nunion all\n".join(pieces))
        pieces = []
        for column in config["dates"]:
            pieces.append(f"""select '{column}' as column_name,count(*) as rows,
 count(*) filter(where \"{column}\" is null) as null_rows,
 count(*) filter(where \"{column}\" is not null and try_cast(\"{column}\" as timestamp) is null) as invalid_timestamp_rows
 from {channel}""")
        record(f"{channel}-timestamp-profile", "\nunion all\n".join(pieces))
        record(f"{channel}-monthly-volume", f"select substr({created},1,7) as month,count(*) as rows from {channel} group by 1 order by 1")
        record(f"{channel}-statuses", f"select order_status,count(*) as rows from {channel} group by 1 order by 2 desc")
        record(f"{channel}-currencies", f"select currency,count(*) as rows from {channel} group by 1 order by 2 desc")
        key = f"user_id,shop_name,{oid},{created}"
        extra = (
            "count(*) as n,count(distinct row_idx) as distinct_idx,min(try_cast(row_idx as int)) as first_idx,max(try_cast(row_idx as int)) as last_idx"
            if channel == "tiktok" else "count(*) filter(where is_primary_row='true') as primaries"
        )
        checks = (
            "count(*) filter(where n<>distinct_idx) as repeated_rowidx_groups,count(*) filter(where first_idx=1 and last_idx=n and distinct_idx=n) as contiguous_1_to_n_groups"
            if channel == "tiktok" else "count(*) filter(where primaries=1) as exactly_one_primary_groups,count(*) filter(where primaries>1) as multiple_primary_groups,count(*) filter(where primaries=0) as no_primary_groups"
        )
        record(f"{channel}-candidate-grouping", f"""
select count(*) as candidate_groups,count(*) filter(where status_n>1) as multi_status_groups,
count(*) filter(where amount_n>1) as multi_header_amount_groups,{checks}
from (select {key},count(distinct order_status) as status_n,count(distinct {amount}) as amount_n,
{extra} from {channel} group by {key})""")

    record("tiktok-row-index", "select row_idx,count(*) as rows from tiktok group by 1 order by try_cast(row_idx as int)")
    record("tiktok-temporal-checks", """
select count(*) filter(where try_cast(paid_time as timestamp)<try_cast(created_time as timestamp)) as paid_before_created_rows,
 count(*) filter(where try_cast(delivered_time as timestamp)<try_cast(created_time as timestamp)) as delivered_before_created_rows,
 count(*) filter(where order_status='COMPLETED' and delivered_time is null) as completed_missing_delivery_rows,
 count(*) filter(where try_cast(updatedAt as timestamp)<try_cast(createdAt as timestamp)) as sync_update_before_create_rows
from tiktok""")
    record("tiktok-early-payment-distance", """
select count(*) as rows,min(date_diff('second',try_cast(paid_time as timestamp),try_cast(created_time as timestamp))) as min_seconds,
 max(date_diff('second',try_cast(paid_time as timestamp),try_cast(created_time as timestamp))) as max_seconds
from tiktok where try_cast(paid_time as timestamp)<try_cast(created_time as timestamp)""")
    record("shopee-temporal-checks", """
select count(*) filter(where try_cast(pay_time as timestamp)<try_cast(create_time as timestamp)) as paid_before_created_rows,
 count(*) filter(where try_cast(shipped_time as timestamp)<try_cast(create_time as timestamp)) as shipped_before_created_rows,
 count(*) filter(where try_cast(completed_time as timestamp)<try_cast(create_time as timestamp)) as completed_before_created_rows,
 count(*) filter(where order_status='COMPLETED' and completed_time is null) as completed_missing_completed_rows
from shopee""")
    record("shopee-negative-escrow", "select order_status,count(*) as rows from shopee where try_cast(escrow_amount as decimal(24,4))<0 group by 1")
    record("shopee-primary-money", """
select is_primary_row,count(*) as rows,
 count(*) filter(where try_cast(buyer_total_amount as decimal(24,4))>0) as positive_buyer_total_rows,
 count(*) filter(where try_cast(commission_fee as decimal(24,4))>0) as positive_commission_rows,
 count(*) filter(where try_cast(escrow_amount as decimal(24,4))<>0) as nonzero_escrow_rows
from shopee group by 1 order by 1""")
    record("shopee-payload", "select count(*) as rows,count(raw_payload) as populated_payload_rows from shopee")
    # Compare current fingerprints with the earlier independent profile.
    previous_path = root / "docs" / "upgrade" / "source_profile.json"
    if previous_path.exists():
        previous = json.loads(previous_path.read_text(encoding="utf-8"))
        evidence["matches_previous_profile_hashes"] = {
            channel: source["sha256"] == previous["sources"][channel]["sha256"]
            for channel, source in evidence["sources"].items()
        }
    evidence["inputs_unchanged_during_audit"] = {}
    for channel, source in evidence["sources"].items():
        with (root / source["file"]).open("rb") as stream:
            after_hash = hashlib.file_digest(stream, "sha256").hexdigest()
        unchanged = after_hash == source["sha256"]
        evidence["inputs_unchanged_during_audit"][channel] = unchanged
        if not unchanged:
            raise RuntimeError(f"Input changed during audit: {channel}")
    connection.close()
    return evidence


if __name__ == "__main__":
    root = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / "dbt_transforms").is_dir())
    result = audit(root)
    output = root / "docs" / "upgrade" / "csv_audit_evidence.json"
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")
    for query in result["queries"]:
        if query["id"].endswith(("-summary", "-exact-duplicates", "-candidate-grouping")):
            print(query["id"],json.dumps(query["rows"],default=str))
    print("Input hashes match earlier profile:",result.get("matches_previous_profile_hashes"))
    print("Saved aggregate evidence:",output.relative_to(root))
