# Vận hành pipeline Bronze → Silver → Gold

Tất cả lệnh chạy từ repo root bằng Python environment hiện có.

## Build và phục vụ dashboard

```powershell
.venv/Scripts/python.exe scripts/build_warehouse.py
.venv/Scripts/streamlit.exe run dashboards/streamlit_app/app.py
```

Output: `data/duckdb/ecom_medallion.duckdb`. Bronze/Silver/Gold, schemas và prefix bảng đồng bộ. CLI build/test trong candidate rồi publish; giữ nguyên CSV và file warehouse legacy. Cần cả hai CSV để build đầy đủ.

## Dagster

```powershell
.venv/Scripts/dagster.exe dev -m pipeline_orchestration.definitions
```

Chọn toàn bộ `ecommerce_dbt_assets`. Lịch 07:00 và 15:00, timezone Asia/Ho_Chi_Minh; chỉ chạy tự động khi schedule được bật. OS lock ngăn CLI/Dagster cùng publish một destination. Không dùng subset cho một candidate.

Nếu cần tạo manifest mà chưa muốn build:

```powershell
Set-Location dbt_transforms
../.venv/Scripts/dbt.exe parse --profiles-dir . --no-partial-parse
Set-Location ..
```

## Kiểm thử an toàn

```powershell
.venv/Scripts/python.exe scripts/validate_ecommerce_dbt.py
.venv/Scripts/python.exe scripts/validate_ecommerce_dbt.py --real-data
.venv/Scripts/python.exe scripts/validate_dashboard.py
.venv/Scripts/python.exe scripts/validate_dagster.py
```

Validator lưu logs/run_results trong `.validation/`. `validate_dashboard.py` dùng fixture DB từ lần validator gần nhất. Không ghi vào database đang phục vụ.

## Cấu hình

Đặt đường dẫn tuyệt đối trong `TIKTOK_CSV_PATH`, `SHOPEE_CSV_PATH`, `DBT_DUCKDB_PATH` để thay input/output. Sau thay đổi, build lại rồi bấm Làm mới dữ liệu trên Streamlit.

`data/duckdb/latest_build.json` ghi receipt CLI lần publish gần nhất. Backup database có tên `ecom_medallion.backup-<UTC timestamp>.duckdb`. Khi cần rollback, dừng các writer/readers và phục hồi backup đã kiểm tra bằng thao tác file trên cùng volume; không sửa trực tiếp raw hoặc xóa file legacy để xử lý một build fail.

Schema/metric definitions và flow: [model contract](dbt_transforms/models/README.md). Gold mô tả observations và coverage, không suy ra đơn hoặc SKU từ ID đã mask.
