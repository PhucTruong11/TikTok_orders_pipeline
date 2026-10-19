"""Materialize both fixture sources through Dagster without touching the live DB."""
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def validate():
    summary = json.loads((ROOT / ".validation/latest_validation.json").read_text(encoding="utf-8"))
    work = Path(tempfile.mkdtemp(prefix="dagster-check-", dir=ROOT / ".validation"))
    values = {ch.upper() + "_CSV_PATH": path for ch, path in summary["fixture_sources"].items()}
    values["DBT_DUCKDB_PATH"] = str(work / "ecom_medallion.duckdb")
    previous = {key: os.environ.get(key) for key in values}
    manifest = ROOT / "dbt_transforms/target/manifest.json"
    prior_manifest = manifest.read_bytes() if manifest.exists() else None
    try:
        os.environ.update(values)
        manifest.parent.mkdir(exist_ok=True)
        shutil.copy2(Path(summary["artifact_directory"]) / "fixture-artifacts/manifest.json", manifest)
        from dagster import Definitions, DagsterInstance, materialize
        from pipeline_orchestration.definitions import defs, ecommerce_dbt_assets, dbt_resource
        from pipeline_orchestration.warehouse import verify_database, source_paths
        Definitions.validate_loadable(defs)
        instance_dir = work / "instance"
        instance_dir.mkdir()
        with DagsterInstance.ephemeral(tempdir=str(instance_dir)) as instance:
            result = materialize([ecommerce_dbt_assets], resources={"dbt": dbt_resource}, instance=instance)
            assert result.success
        counts = verify_database(work / "ecom_medallion.duckdb", source_paths())
        assert counts["tiktok"]["observations"] == 8
        assert counts["shopee"]["observations"] == 7
        print("DAGSTER_SMOKE_PASSED", json.dumps(counts))
    finally:
        if prior_manifest is None:
            manifest.unlink(missing_ok=True)
        else:
            manifest.write_bytes(prior_manifest)
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


if __name__ == "__main__":
    validate()
