"""Build isolated medallion databases and publish only validated, closed files."""
from contextlib import contextmanager
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "dbt_transforms"
DBT_ENTRY = "import sys; from dbt.cli.main import dbtRunner; r=dbtRunner().invoke(sys.argv[1:]); sys.exit(0 if r.success else 1)"
MODEL_TABLES = {
    "bronze": ["bronze_tiktok_orders", "bronze_shopee_orders"],
    "silver": ["silver_tiktok_observations", "silver_shopee_observations", "silver_ecommerce_observations", "silver_order_status_mapping"],
    "gold": ["gold_observation_quality", "gold_channel_daily", "gold_status_distribution", "gold_pipeline_metadata"],
}


def warehouse_path():
    return Path(os.environ.get("DBT_DUCKDB_PATH", ROOT / "data/duckdb/ecom_medallion.duckdb")).resolve()


def source_paths():
    return {ch: Path(os.environ.get(ch.upper() + "_CSV_PATH", ROOT / f"data/raw/{ch}_orders.csv")).resolve()
            for ch in ("tiktok", "shopee")}


def fingerprint(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def record_count(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        columns = next(reader)
        count = 0
        for record in reader:
            if len(record) != len(columns):
                raise ValueError(f"CSV record width differs from header: {Path(path).name}")
            count += 1
        return count


def run_dbt(project, sources, database, artifacts, expect_success=True):
    project, database, artifacts = map(Path, (project, database, artifacts))
    artifacts.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update({ch.upper() + "_CSV_PATH": Path(path).as_posix() for ch, path in sources.items()})
    environment.update(DBT_DUCKDB_PATH=database.as_posix(), DBT_TARGET_PATH=artifacts.as_posix(),
                       DBT_LOG_PATH=(artifacts / "logs").as_posix(), DBT_SEND_ANONYMOUS_USAGE_STATS="false", DBT_USE_COLORS="false")
    process = subprocess.run(
        [sys.executable, "-c", DBT_ENTRY, "build", "--full-refresh", "--target", "ci", "--no-partial-parse",
         "--project-dir", str(project), "--profiles-dir", str(project), "--vars", json.dumps({"warehouse_path": database.as_posix()})],
        cwd=project, env=environment, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300)
    output = process.stdout + process.stderr
    (artifacts / "command_output.txt").write_text(output, encoding="utf-8")
    if (process.returncode == 0) != expect_success:
        raise RuntimeError("Unexpected dbt result:\n" + "\n".join(output.splitlines()[-30:]))
    result_path = artifacts / "run_results.json"
    if not result_path.exists():
        raise RuntimeError("dbt failed before producing run_results:\n" + "\n".join(output.splitlines()[-20:]))
    return json.loads(result_path.read_text(encoding="utf-8"))


def verify_database(database, sources):
    """Verify both channels and all physical layer names before publication."""
    summary = {}
    with duckdb.connect(str(database), read_only=True) as connection:
        for schema, names in MODEL_TABLES.items():
            actual = {r[0] for r in connection.execute("select table_name from information_schema.tables where table_schema=?", [schema]).fetchall()}
            if actual != set(names):
                raise ValueError(f"Unexpected tables in {schema}: {sorted(actual)}")
        for ch, path in sources.items():
            expected = record_count(path)
            file_hash = fingerprint(path)
            raw_count, raw_keys, minimum_hash, maximum_hash = connection.execute(
                f"select count(*),count(distinct observation_id),min(_file_sha256),max(_file_sha256) from bronze.bronze_{ch}_orders").fetchone()
            silver_count, silver_keys = connection.execute(
                f"select count(*),count(distinct observation_id) from silver.silver_{ch}_observations").fetchone()
            gold_count = connection.execute("select sum(observation_count) from gold.gold_observation_quality where source_channel=?", [ch]).fetchone()[0]
            if not (expected == raw_count == raw_keys == silver_count == silver_keys == gold_count):
                raise ValueError(f"Record preservation failed for {ch}")
            if minimum_hash != file_hash or maximum_hash != file_hash:
                raise ValueError(f"Source changed during build: {ch}")
            summary[ch] = {"observations": expected, "sha256": file_hash}
        unified_count, unified_keys = connection.execute("select count(*),count(distinct observation_id) from silver.silver_ecommerce_observations").fetchone()
        if unified_count != unified_keys or unified_count != sum(s["observations"] for s in summary.values()):
            raise ValueError("Combined observation identity/count failed")
    return summary


@contextmanager
def writer_lock(destination):
    """OS advisory lock shared by the CLI and Dagster; released even after errors."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.with_suffix(".build.lock").open("a+b") as stream:
        stream.seek(0)
        if not stream.read(1):
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RuntimeError("Another pipeline is already building this warehouse") from error
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def publish_database(candidate, destination):
    candidate, destination = Path(candidate), Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if candidate.stem != destination.stem:
        raise ValueError("Candidate must retain the destination catalog name")
    with duckdb.connect(str(candidate)) as connection:
        connection.execute("checkpoint")
    backup = None
    if destination.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup = destination.with_name(f"{destination.stem}.backup-{stamp}.duckdb")
        shutil.copy2(destination, backup)
    for attempt in range(30):
        try:
            os.replace(candidate, destination)
            return backup
        except PermissionError:
            if attempt == 29:
                raise RuntimeError("Warehouse is held open; release the old connection and retry. Previous database remains available.")
            time.sleep(0.1)


def build_and_publish(destination=None, sources=None):
    destination = Path(destination or warehouse_path()).resolve()
    sources = sources or source_paths()
    with writer_lock(destination):
        # Same volume and basename make atomic publication and stored view refs safe.
        builds = destination.parent / ".builds"
        builds.mkdir(exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix="medallion-", dir=builds))
        candidate = work / destination.name
        result = run_dbt(PROJECT, sources, candidate, work / "artifacts")
        summary = verify_database(candidate, sources)
        backup = publish_database(candidate, destination)
        # Reopen at its published filename to catch catalog-qualified view problems.
        verify_database(destination, sources)
        receipt = {"warehouse": str(destination), "backup": str(backup) if backup else None,
                   "sources": summary, "data_tests": sum(r["unique_id"].startswith("test.") for r in result["results"]),
                   "artifacts": str(work / "artifacts"), "published_at_utc": datetime.now(timezone.utc).isoformat()}
        (destination.parent / "latest_build.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        (PROJECT / "target").mkdir(exist_ok=True)
        shutil.copy2(work / "artifacts" / "manifest.json", PROJECT / "target" / "manifest.json")
        return receipt
