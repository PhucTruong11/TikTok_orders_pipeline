"""Dagster orchestration for the two-source Bronze/Silver/Gold pipeline."""
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

from dagster import AssetExecutionContext, Definitions, Failure
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, build_schedule_from_dbt_selection

from pipeline_orchestration.warehouse import (
    source_paths, warehouse_path, writer_lock, verify_database, publish_database,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = REPO_ROOT / 'dbt_transforms'
environment_dbt = Path(sys.executable).parent / ('dbt.exe' if os.name=='nt' else 'dbt')
dbt_exe = str(environment_dbt) if environment_dbt.exists() else shutil.which('dbt')
if not dbt_exe:
    raise RuntimeError('dbt executable was not found in the active Python environment or PATH')
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project, name='ecommerce_dbt_assets')
def ecommerce_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # Publication requires a full candidate, never a partially selected catalog.
    if context.selected_asset_keys != ecommerce_dbt_assets.keys:
        raise Failure('Select all ecommerce assets to build and publish the complete warehouse.')
    destination=warehouse_path()
    with writer_lock(destination):
        builds=destination.parent/'.builds';builds.mkdir(exist_ok=True)
        work=Path(tempfile.mkdtemp(prefix='dagster-',dir=builds))
        candidate=work/destination.name
        yield from dbt.cli(['build','--full-refresh','--vars',json.dumps({'warehouse_path':candidate.as_posix()})],context=context).stream()
        summary=verify_database(candidate,source_paths())
        backup=publish_database(candidate,destination)
        verify_database(destination,source_paths())
        context.log.info('Published %s; records=%s; backup=%s',destination,summary,backup)


dbt_resource=DbtCliResource(project_dir=str(DBT_PROJECT_DIR),profiles_dir=str(DBT_PROJECT_DIR),dbt_executable=dbt_exe)
daily_dbt_schedule=build_schedule_from_dbt_selection(
    [ecommerce_dbt_assets],job_name='daily_ecommerce_pipeline',cron_schedule='0 7 * * *',execution_timezone='Asia/Ho_Chi_Minh')
afternoon_dbt_schedule=build_schedule_from_dbt_selection(
    [ecommerce_dbt_assets],job_name='afternoon_ecommerce_pipeline',cron_schedule='0 15 * * *',execution_timezone='Asia/Ho_Chi_Minh')
defs=Definitions(assets=[ecommerce_dbt_assets],schedules=[daily_dbt_schedule,afternoon_dbt_schedule],resources={'dbt':dbt_resource})
