import sys
import shutil
import os
from pathlib import Path
from prefect import flow, task
import subprocess

# === Đường dẫn thư mục ===
REPO_ROOT = Path(__file__).parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_transforms"

# Tự động tìm dbt executable (tương thích Windows & Linux)
dbt_exe = shutil.which("dbt") or str(
    Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
)

@task(name="dbt_build_task", log_prints=True)
def run_dbt_build():
    """
    Task chạy lệnh `dbt build` bằng subprocess.
    """
    print(f"Bắt đầu chạy dbt build tại: {DBT_PROJECT_DIR}")
    
    # Chạy dbt build với profile nằm cùng thư mục dự án
    result = subprocess.run(
        [dbt_exe, "build", "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True
    )
    
    # In logs ra console của Prefect
    print(result.stdout)
    
    if result.stderr:
        print(result.stderr)
        
    if result.returncode != 0:
        raise Exception("dbt build thất bại! Kiểm tra log bên trên.")
    
    print("dbt build thành công!")

@flow(name="TikTok_Ecom_Data_Pipeline_Prefect", description="Luồng xử lý dữ liệu dbt điều phối bởi Prefect")
def ecom_pipeline_flow():
    """
    Định nghĩa Flow chính của Prefect
    """
    run_dbt_build()

if __name__ == "__main__":
    # Lập lịch chạy tự động: 7h00 sáng và 13h00 trưa mỗi ngày
    # Khi chạy lệnh `python dbt_flow.py`, terminal sẽ ở trạng thái "Lắng nghe" (Listening)
    ecom_pipeline_flow.serve(
        name="dbt-build-deployment",
        cron="0 7,15 * * *",
        tags=["dbt", "ecommerce"]
    )
