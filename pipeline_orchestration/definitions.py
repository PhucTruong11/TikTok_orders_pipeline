from asyncio import coroutines
import os
import sys
import shutil
from pathlib import Path
from dagster import Definitions, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# === Đường dẫn thư mục ===
REPO_ROOT = Path(__file__).parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_transforms"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# Tự động tìm dbt executable (tương thích Windows & Linux)
dbt_exe = shutil.which("dbt") or str(
    Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
)


@dbt_assets(manifest=MANIFEST_PATH)
def tiktok_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Software-Defined Assets từ dbt project.

    Mỗi dbt model (stg_tiktok_orders, fct_orders, mart_daily_revenue,...)
    sẽ tự động trở thành 1 Asset trên Dagster UI.

    Khi bạn bấm "Materialize" trên UI, Dagster sẽ chạy `dbt build`
    (= dbt run + dbt test) theo đúng thứ tự dependency.
    """
    yield from dbt.cli(["build"], context=context).stream()


# === Dagster Resource: cấu hình kết nối dbt ===
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    dbt_executable=dbt_exe,
)
from dagster_dbt import build_schedule_from_dbt_selection

# === Lập lịch (Schedules) ===
# Chạy toàn bộ pipeline dbt vào lúc 07:00 sáng mỗi ngày
daily_dbt_schedule = build_schedule_from_dbt_selection(
    [tiktok_dbt_assets],
    job_name="daily_tiktok_pipeline",
    cron_schedule="0 7 * * *", # 07:00 hàng ngày
)

noon_dbt_schedule = build_schedule_from_dbt_selection(
    [tiktok_dbt_assets],
    job_name="noon_tiktok_pipeline",
    cron_schedule="0 13 * * *",
)

# === Khai báo Definitions (entry point của Dagster) ===
defs = Definitions(
    assets=[tiktok_dbt_assets],
    schedules=[daily_dbt_schedule, noon_dbt_schedule],
    resources={
        "dbt": dbt_resource,
    },
)
