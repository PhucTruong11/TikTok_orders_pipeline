"""Run dbt integration checks in a disposable project/database; never write dev DB.

    python scripts/validate_tiktok_dbt.py              # synthetic regression cases
    python scripts/validate_tiktok_dbt.py --real-data  # also check local TikTok CSV
"""
import argparse
from contextlib import nullcontext
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DBT_ENTRY = "import sys; from dbt.cli.main import dbtRunner; r=dbtRunner().invoke(sys.argv[1:]); sys.exit(0 if r.success else 1)"


def run_dbt(project, source, database, artifacts, expect_success=True):
    environment = os.environ.copy()
    environment.update(
        TIKTOK_CSV_PATH=source.as_posix(), DBT_DUCKDB_PATH=database.as_posix(),
        DBT_TARGET_PATH=artifacts.as_posix(), DBT_LOG_PATH=(artifacts / "logs").as_posix(),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false", DBT_USE_COLORS="false",
    )
    process = subprocess.run(
        [sys.executable, "-c", DBT_ENTRY, "build", "--full-refresh", "--target", "ci",
         "--project-dir", str(project), "--profiles-dir", str(project), "--no-partial-parse"],
        cwd=project, env=environment, capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=180,
    )
    (artifacts / "command_output.txt").parent.mkdir(parents=True, exist_ok=True)
    (artifacts / "command_output.txt").write_text(process.stdout + process.stderr, encoding="utf-8")
    if (process.returncode == 0) != expect_success:
        # Last lines include dbt's failing node/test, without dumping source records.
        raise AssertionError("Unexpected dbt result:\n" + "\n".join((process.stdout + process.stderr).splitlines()[-24:]))
    return json.loads((artifacts / "run_results.json").read_text(encoding="utf-8"))


def read_records(path):
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        return reader.fieldnames, list(reader)


def write_records(path, fieldnames, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def snapshot(database):
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute("""
            select observation_id, raw_order_status, order_status, standardized_status,
                   is_completed, is_cancelled, is_returned, _source_row_number
            from main_intermediate.fct_tiktok_observations order by _source_row_number
        """).fetchall()


def verify_fixture(database):
    rows = snapshot(database)
    assert len(rows) == 8 and len({row[0] for row in rows}) == 8, "Masked collisions or exact records were dropped"
    assert [row[7] for row in rows] == list(range(1, 9)), "CSV ordinals mishandled multiline record"
    assert sum(row[4] for row in rows) == 3, "Completed mapping regression"
    assert sum(row[5] for row in rows) == 1, "Canceled alias regression"
    assert sum(row[6] for row in rows) == 1, "Returned mapping regression"
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute("select count(*) from main_intermediate.fct_tiktok_observations where pk_id='abcde*******************************'").fetchone()[0] == 8
        assert connection.execute("select count(*) from main_intermediate.fct_tiktok_observations where not has_complete_amount_components and amount_excluding_reported_shipping_tax_and_fees is null").fetchone()[0] == 1
        assert connection.execute("select sum(observation_count),count(distinct currency) from main_marts.mart_tiktok_observation_quality").fetchone() == (8, 2)


def validate(real_data=False):
    results = []
    validation_root = (ROOT / ".validation").resolve()
    if not validation_root.is_relative_to(ROOT.resolve()):
        raise ValueError("Validation directory must stay inside the repository")
    validation_root.mkdir(exist_ok=True)
    # Retain isolated logs/artifacts for review, including on failed assertions.
    # Avoid recursive cleanup racing dbt's background processes on Windows.
    with nullcontext(tempfile.mkdtemp(prefix="tiktok-dbt-", dir=validation_root)) as directory:
        work = Path(directory)
        if not work.resolve().is_relative_to(validation_root):
            raise ValueError("Temporary cleanup target must stay inside validation directory")
        project = work / "dbt_transforms"
        shutil.copytree(ROOT / "dbt_transforms", project,
                        ignore=shutil.ignore_patterns("target", "logs", "dbt_packages", ".user.yml"))
        fixture = work / "fixture.csv"
        shutil.copyfile(ROOT / "tests" / "fixtures" / "tiktok_orders.csv", fixture)
        database = work / "fixture.duckdb"
        artifacts = work / "fixture-artifacts"
        build = run_dbt(project, fixture, database, artifacts)
        verify_fixture(database)
        first = snapshot(database)
        run_dbt(project, fixture, database, artifacts)
        verify_fixture(database)
        assert snapshot(database) == first, "Rerun changed stable observation IDs/statuses"
        results.append({"case": "fixture_and_identical_rerun", "status": "passed",
                        "dbt_tests": sum(r["unique_id"].startswith("test.") for r in build["results"]), "observations": 8})

        fields, rows = read_records(fixture)
        for name, column, value, failure_node in (
            ("unknown_status", "order_status", "UNRECOGNIZED_STATUS", "not_null_int_tiktok_observations_standardized_status"),
            ("nonpositive_quantity", "quantity", "0", "positive_value_stg_tiktok_orders_quantity"),
            ("invalid_money", "total_amount", "invalid-number", "stg_tiktok_orders"),
        ):
            changed = [row.copy() for row in rows]
            changed[0][column] = value
            bad_source = work / f"{name}.csv"
            write_records(bad_source, fields, changed)
            bad_build = run_dbt(project, bad_source, work / f"{name}.duckdb", work / name, expect_success=False)
            assert any(failure_node in r["unique_id"] and r["status"] in ("fail", "error")
                       for r in bad_build["results"]), f"Wrong failure reason for {name}"
            results.append({"case": name, "status": "correctly_rejected"})

        # A mapping fan-out must fail instead of doubling source observations.
        seed = project / "seeds" / "order_status_mapping.csv"
        original_seed = seed.read_bytes()
        seed.write_text(original_seed.decode("utf-8").rstrip("\r\n")
                        + "\ntiktok,COMPLETED,COMPLETED,COMPLETED,synthetic duplicate mapping\n", encoding="utf-8")
        bad_mapping = run_dbt(project, fixture, work / "mapping.duckdb", work / "mapping", expect_success=False)
        assert any("status_mapping_unique" in r["unique_id"] and r["status"] == "fail"
                   for r in bad_mapping["results"]), "Mapping duplicate was not detected"
        seed.write_bytes(original_seed)
        results.append({"case": "mapping_fanout", "status": "correctly_rejected"})

        # Same path, different content => new file fingerprint and new row identity.
        changed = [row.copy() for row in rows]
        changed[0]["buyer_message"] = "synthetic changed snapshot"
        write_records(fixture, fields, changed)
        run_dbt(project, fixture, database, artifacts)
        verify_fixture(database)
        assert {r[0] for r in snapshot(database)}.isdisjoint({r[0] for r in first})
        results.append({"case": "changed_file_same_path", "status": "passed"})

        if real_data:
            source = ROOT / "data" / "raw" / "tiktok_orders.csv"
            before_hash = hashlib.sha256(source.read_bytes()).hexdigest()
            with source.open(encoding="utf-8", newline="") as stream:
                expected_count = sum(1 for _ in csv.reader(stream)) - 1
            build = run_dbt(project, source, work / "real.duckdb", work / "real-artifacts")
            with duckdb.connect(str(work / "real.duckdb"), read_only=True) as connection:
                count, unique_keys, completed, cancelled = connection.execute("""
                    select count(*),count(distinct observation_id),sum(is_completed),sum(is_cancelled)
                    from main_intermediate.fct_tiktok_observations
                """).fetchone()
            assert count == unique_keys == expected_count
            assert hashlib.sha256(source.read_bytes()).hexdigest() == before_hash
            results.append({"case": "local_tiktok_snapshot", "status": "passed", "observations": count,
                            "completed_observations": completed, "cancelled_observations": cancelled,
                            "dbt_tests": sum(r["unique_id"].startswith("test.") for r in build["results"])})
    (validation_root / "latest_validation.json").write_text(
        json.dumps({"artifact_directory": str(work), "results": results}, indent=2) + "\n", encoding="utf-8")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--real-data", action="store_true")
    args = parser.parse_args()
    for result in validate(args.real_data):
        print(json.dumps(result))
