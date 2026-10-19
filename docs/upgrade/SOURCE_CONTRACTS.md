# Hợp đồng nguồn — baseline offline

Ngày cập nhật: 19/10/2026. Trạng thái: baseline cho thiết kế, chưa phải hợp đồng business grain đã xác thực.

Quyết định đã chốt với người dùng: chấp nhận dữ liệu đã che vì bảo mật; không đặt mục tiêu giải che hay lấy lại ID gốc. Giữ Dagster, Python/uv/`.venv`, DuckDB, dbt-duckdb và Streamlit local. Airflow/Docker không nằm trong đợt cải thiện hiện tại. Masking là đặc tính đầu vào cần thích nghi khi chọn grain và metric, không phải lỗi phải sửa ở nguồn.

## Provenance

- Theo thông tin người dùng: bộ `vietnam_ecommerce` của Xóm Dataset, truy cập database có đăng nhập qua DataGrip rồi dùng chức năng export/download CSV trực tiếp; không có script kéo dữ liệu riêng.
- Trang catalog được người dùng cung cấp: https://dataset.xomdata.com/datasets
- Nguồn vận hành được chọn: hai file CSV đang có trên máy. Không giả định vẫn kết nối được database cũ hoặc còn bản export khác.
- Ngày export, tên bảng/schema đầy đủ, SQL export, engine nguồn, điều kiện lọc và tài liệu nguồn: chưa có bằng chứng. Không lấy file modification time hoặc cột sync thay cho ngày export.
- Đã quan sát `pkId`, mã đơn, product/SKU IDs và nhiều nhãn bị che bằng `*` trong CSV. Nơi áp dụng và quy tắc masking đầy đủ, dữ liệu thực/giả lập, tính đại diện thị trường và quan hệ nhà bán giữa hai file: chưa xác minh.
- Trang công khai `e_commerce` hiện có cấu trúc khác; không dùng tài liệu của bộ đó để gán ý nghĩa cho hai CSV hiện tại.

## Tài sản nguồn

| File tính từ repo root | Dòng | Cột | Kiểu nguồn vận hành |
|---|---:|---:|---|
| `data/raw/tiktok_orders.csv` | 102.000 | 71 | Snapshot CSV tĩnh |
| `data/raw/shopee_orders.csv` | 25.546 | 84 | Snapshot CSV tĩnh |

Fingerprint SHA-256, kích thước, schema và SQL kiểm tra nằm trong [source_profile.json](source_profile.json). Kiểm tra sâu tại [CSV_AUDIT.md](CSV_AUDIT.md) xác nhận CSV strict parse đạt, không exact duplicate rows và ID masking có sẵn trong nội dung file. Raw được đọc dạng string để bảo toàn giá trị hiện có. Chưa biết bản export gốc là full snapshot, delta hay kết quả join; “snapshot tĩnh” ở đây chỉ mô tả cách dùng các file hiện có.

## Quy tắc cho implementation tiếp theo

1. Giữ bản CSV gốc bất biến; tạo bản sao lưu riêng ngoài working data trước khi thực hiện thay đổi dữ liệu. Không đưa raw vào Git.
2. Định danh file bằng content hash, không chỉ filename. Khóa dòng quan sát là `(source, file_sha256, row_number_in_file)`; khóa này phục vụ ingestion/retry/truy vết, không chứng minh danh tính order hay order line.
3. Manifest ghi dataset/source, file hash, row count, schema version, ingestion time và nguồn gốc do người dùng cung cấp. Trường export time chưa biết giữ null, không tự điền ngày hiện tại.
4. Đọc cùng file/hash lần nữa không append raw observations trùng. Giữ source channel và currency ở các tầng; schema/cast failures phải được ghi nhận.
5. Không dedup nghiệp vụ bằng masked `pkId` hoặc mã đơn hiện có; chấp nhận chúng là nhãn đã che. Giữ observations bằng technical row key, không yêu cầu khôi phục ID gốc để tiếp tục. Không giả định ghép shop/channel đã sửa được toàn bộ vấn đề.
6. Khi chưa chứng minh grain, cách ly nhóm mơ hồ và hiển thị coverage; nếu phần dữ liệu còn lại cũng không xác định được grain thì không bật KPI order/AOV như số liệu đã xác thực. Không làm sạch bằng cách tùy ý bỏ bản ghi để đạt unique test.
7. Full refresh trên snapshot là baseline cho đợt hiện tại. Incremental/replay/backfill mô phỏng được để lại cho lần mở rộng sau, không phải điều kiện hoàn tất. Nếu về sau thực hiện simulation thì phải gắn nhãn, lưu quy tắc/seed và expected output.
8. Lịch mặc định manual/file-arrival; với hai file không đổi, chạy lại hằng ngày chỉ là rebuild. UI tách business coverage và thời điểm pipeline build, không diễn giải rebuild là dữ liệu thị trường mới.
9. Giữ môi trường local hiện có. CI/test dùng fixture nhỏ có grain xác định, không có thông tin khách hàng; fixture chỉ kiểm chứng logic, không thay thế bộ CSV chính.
10. Phân tích từ snapshot ghi rõ “trong dataset”; không suy ra market share, tính đại diện hay quan hệ nhân quả khi thiếu bằng chứng.

## Những mục vẫn chưa biết

Business key ổn định hoặc stable pseudonymous key; nơi áp dụng/quy tắc masking; timezone; ý nghĩa unit/line/order của discount và tiền; tiêu chí `is_primary_row`; semantics refund/settlement; đồng nhất sản phẩm/shop giữa hai kênh. Không coi masked ID collisions là bằng chứng ID gốc tái sử dụng. Các mục này được quản lý như giới hạn dữ liệu, không biến việc truy cập lại nguồn cũ thành điều kiện để dựng môi trường và kiểm thử engineering.
