# 🚀 DATA PIPELINE CHEATSHEET

Tài liệu này lưu trữ toàn bộ các câu lệnh cần thiết để khởi chạy, quản lý và cập nhật hệ thống Data Pipeline (dbt + Dagster + Evidence).

---

## 1. Môi trường Ảo (Virtual Environment)
**Luôn phải kích hoạt môi trường ảo** trước khi chạy bất kỳ lệnh nào bên dưới.
```bash
# Kích hoạt môi trường (Windows)
.venv\Scripts\activate
```

---

## 2. Làm việc với dbt (Phân tích dữ liệu)
Nếu bạn thay đổi code SQL trong thư mục `dbt_transforms/models/` hoặc đổi file `.csv` gốc, bạn có thể chạy thủ công bằng dbt để test thử:

```bash
# Di chuyển vào thư mục dbt
cd dbt_transforms

# Chạy toàn bộ tiến trình (Build = Run + Test)
dbt build

# Chỉ chạy một bảng cụ thể (ví dụ: mart_daily_revenue)
dbt build --select mart_daily_revenue

# Chỉ chạy mô hình (không test)
dbt run
```

---

## 3. Dagster (Giao diện Quản lý - Offline)
Sử dụng Dagster khi bạn muốn có một cái nhìn tổng quan về sự phụ thuộc giữa các bảng dữ liệu (Data Lineage) và xem sơ đồ luồng chạy trực tiếp trên máy của bạn.

```bash
# Đứng ở thư mục gốc của dự án, khởi chạy Server Dagster:
# (Thiết lập DAGSTER_HOME để tránh bị sinh ra các thư mục rác .tmp_dagster_home)
$env:DAGSTER_HOME="$(Get-Location)\.dagster"
dagster dev -m pipeline_orchestration.definitions
```
👉 **Truy cập:** [http://localhost:3000](http://localhost:3000)
- Mở **Overview** để xem toàn bộ Asset.
- Chọn bảng và bấm **Materialize** để chạy dbt từ giao diện web.
- Đóng Terminal (Ctrl+C) thì Dagster UI sẽ tắt và lịch sẽ ngưng chạy.
