# 🚀 DATA PIPELINE CHEATSHEET

Tài liệu này lưu trữ toàn bộ các câu lệnh cần thiết để khởi chạy, quản lý và cập nhật hệ thống Data Pipeline (dbt + Dagster + Prefect + Streamlit).

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

---

## 4. Prefect (Giao diện Quản lý - Cloud)
Sử dụng Prefect khi bạn muốn một UI hiện đại, quản lý log dễ nhìn và giám sát từ xa (trên web điện thoại/máy tính khác).

```bash
# (Chỉ làm 1 lần) Đăng nhập Terminal của bạn vào Prefect Cloud
prefect cloud login

# Khởi chạy luồng dbt và bắt đầu lắng nghe Lịch trình (Schedules)
python prefect_orchestration/dbt_flow.py
```
👉 **Truy cập:** [https://app.prefect.cloud](https://app.prefect.cloud)
- Lệnh Python ở trên sẽ bị "treo" (Listening) ở Terminal. Nó đóng vai trò làm công nhân (Worker). Khi tới đúng giờ hẹn, Cloud sẽ ra lệnh và Terminal này sẽ chạy code.
- Đóng Terminal (Ctrl+C) thì Lịch trên Cloud báo chạy sẽ bị Lỗi (Failed) vì không có Worker phản hồi.

---

## 5. Streamlit (Giao diện Dashboard Báo cáo)
Khởi chạy trang web trực quan hóa dữ liệu để hiển thị kết quả kinh doanh.

```bash
# Khởi chạy Streamlit Dashboard
streamlit run dashboards\streamlit_app\app.py
```
👉 **Truy cập:** [http://localhost:8501](http://localhost:8501)
- Streamlit sẽ tự động reload lại trang web nếu bạn có sửa đổi code trong file `app.py`.
