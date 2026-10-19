# Kiểm tra sâu hai CSV xuất từ DataGrip

Ngày: 19/10/2026. Phạm vi: toàn bộ hai file local; không đăng nhập database nguồn, không sửa CSV hay warehouse. Người dùng xác nhận xuất CSV trực tiếp bằng DataGrip, không dùng script riêng.

## Kết luận

**Hai file hợp lệ về cấu trúc CSV. Vấn đề lớn là dữ liệu định danh đã bị che bằng dấu `*`, nên các giá trị đang thấy không đủ làm business key.** Không có bằng chứng từ kiểm tra này rằng DataGrip đã làm lặp các dòng dữ liệu hoặc làm hỏng định dạng export. Cũng chưa thể xác nhận export đầy đủ so với database không còn truy cập được.

Cần sửa diễn giải ở lần đánh giá đầu: các distinct order IDs thấp và `pkId` trùng là những giá trị **đã che**, không phải bằng chứng trực tiếp rằng mã đơn gốc bị tái sử dụng hay quá trình xuất sai. Việc che đã tồn tại trong nội dung CSV; chưa biết được áp dụng tại nguồn, cấu hình export hay bước nào trước đó.

## 1. Phương pháp và bằng chứng

- Python `csv.reader(strict=True)` đọc UTF-8 toàn bộ file, kiểm tra header, số cột từng record, record rỗng, header lặp và newline trong ô.
- DuckDB in-memory đọc độc lập với toàn bộ cột kiểu string; số record khớp Python CSV reader.
- So sánh chính xác toàn bộ cột để tìm duplicate rows; không dùng `pkId` làm tiêu chí duplicate.
- Profile cả 155 cột: null, blank, dấu che `*`, distinct count. Kiểm tra 44 cột số và 24 cột timestamp tính riêng từng nguồn.
- Kiểm tra currency, khoảng ngày, biến động volume theo tháng, thứ tự timestamp, thiếu mốc hoàn tất, header/line indicators và một số khóa ghép ứng viên.
- SHA-256 hai file khớp với lần profile trước; nội dung nguồn không đổi giữa các lần kiểm tra này.
- Script: [audit_csv_files.py](audit_csv_files.py). Notebook có code và output: [csv_audit.ipynb](csv_audit.ipynb). SQL, kết quả aggregate, schema, hash: [csv_audit_evidence.json](csv_audit_evidence.json), gồm 27 query results.

Tất cả counts trong báo cáo là **dòng CSV hoặc nhóm chẩn đoán**, không phải số đơn nghiệp vụ đã chứng minh. Không xuất thông tin khách hàng vào báo cáo.

## 2. Tính toàn vẹn định dạng

| Kiểm tra | TikTok | Shopee |
|---|---:|---:|
| Dung lượng byte | 100.815.605 | 21.568.414 |
| Record dữ liệu | 102.000 | 25.546 |
| Cột | 71 | 84 |
| Record sai số cột | 0 | 0 |
| Tên cột trùng | 0 | 0 |
| Header lặp trong dữ liệu | 0 | 0 |
| Record rỗng | 0 | 0 |
| Bản ghi chứa newline trong ô | 0 | 86 |
| Dòng trùng hoàn toàn | 0 | 0 |
| UTF-8 strict và CSV strict parse | Đạt | Đạt |

86 record Shopee có xuống dòng trong ô nhưng quote hợp lệ. Vì vậy đếm physical lines không tương đương đếm record; các script dùng splitlines hoặc đếm newline sẽ sai. Hai file có newline kết thúc. Không thấy dấu hiệu file bị cắt giữa một record, nhưng một export bị giới hạn số dòng vẫn có thể hoàn toàn hợp lệ về cú pháp; muốn kiểm chứng completeness cần row count/query ở nguồn.

## 3. Masking và khóa: Critical cho order-level analytics

Độ tin cậy cao: kiểm tra trực tiếp text trong file và biểu thức pattern, không phải chỉ suy luận từ distinct count.

| Cột | Quan sát |
|---|---|
| `pkId`, cả hai nguồn | 100% giữ 5 ký tự hexadecimal đầu rồi 31 dấu `*` |
| TikTok `order_id` | 100% giữ 5 ký tự đầu rồi dấu `*`; tổng độ dài 18 |
| Shopee `order_sn` | 100% giữ 5 ký tự đầu rồi dấu `*`; tổng độ dài 14 |
| Product/SKU IDs | Mọi giá trị non-null trong các cột được kiểm tra đều có `*` |
| User/shop identifiers | Cũng bị che; thêm vào composite key không khôi phục định danh gốc |
| Tên sản phẩm, SKU, shop | Có dấu che ở cả hai nguồn |
| Shopee state/city/district/country | Có dấu che ở toàn bộ dòng |

Tổng cộng TikTok có 18 cột chứa dấu che; Shopee có 24. Không giả định toàn bộ dấu `*` ở mọi cột đều có cùng quy tắc, nhưng pattern ID nêu trên được xác nhận đầy đủ.

### Hệ quả cho pipeline hiện có

- `distinct(order_id)=386` và `distinct(order_sn)=35` là số **mask khác nhau**, không phải số đơn thật.
- Giữ một record cho mỗi masked `pkId` sẽ loại 4.842 dòng TikTok (4,75%) và 305 dòng Shopee (1,19%), mặc dù không có dòng nào trùng hoàn toàn. Đây là số dòng bị loại bởi quy tắc đó, chưa phải số bản ghi nghiệp vụ hợp lệ bị mất đã được xác thực.
- `fct_orders` group theo masked `order_id` có thể gộp nhiều đơn khác nhau. Khi đó AOV, tỷ lệ hủy theo đơn, giỏ hàng, tiền header và quan hệ order-line không được bảo đảm.
- `hash(channel, order_id)` vẫn gom những ID gốc đã che thành cùng mask. Hash chỉ đổi biểu diễn, không khôi phục thông tin đã mất.
- Không dùng SKU/name đã che để nối cùng sản phẩm giữa hai sàn; không dùng buyer ID đã che để tính retention/khách quay lại; mapping geography Shopee cũng cần xem là chưa xác thực.
- `raw_payload` của Shopee null ở toàn bộ 25.546 dòng, nên cột này không cung cấp ID gốc để phục hồi.

### Đã kiểm tra khóa ghép có timestamp

Thử nhóm `(user_id, shop_name, masked_order_id, created_time)` chỉ để chẩn đoán:

| Chỉ tiêu | TikTok | Shopee |
|---|---:|---:|
| Nhóm ứng viên | 84.718 | 19.661 |
| Nhóm có nhiều status | 209 | 32 |
| Nhóm có nhiều giá trị tiền header | 533 | 57 |

TikTok vẫn có 590 nhóm lặp `row_idx`. Shopee có 172 nhóm chứa nhiều hơn một dòng `is_primary_row=true`. Khác biệt có thể liên quan history/correction/collision hoặc semantics cột; chưa đủ bằng chứng phân biệt. Vì vậy **không đổi các số nhóm này thành số đơn**, và không triển khai composite key này như khóa đã được chứng minh.

TikTok có 79.053 dòng `row_idx=1`; Shopee có 19.833 dòng `is_primary_row=true`. Đây là indicators đáng nghiên cứu thêm, chưa đủ để gọi là tổng số đơn hoặc dùng chúng để xóa phần còn lại của dữ liệu.

## 4. Tiền và currency

- Toàn bộ giá trị non-null trong 44 cột số được chọn cast `decimal(24,4)` thành công. Không có quantity null/không parse được; TikTok quantity đều bằng 1, Shopee từ 1 đến 15. Đây là đặc điểm dữ liệu, chưa phải lỗi.
- TikTok chỉ có VND; Shopee có 25.542 dòng VND và 4 dòng SGD. Phải giữ currency riêng, không cộng tất cả rồi gắn nhãn VND.
- Shopee thiếu `model_discounted_price` ở 1.137 dòng và `total_order_value` ở 276 dòng. Cần quy tắc null rõ ràng, không tự quy đổi missing thành 0.
- Shopee có 877 dòng `escrow_amount<0`, tất cả mang `COMPLETED`. **Chưa kết luận sai dữ liệu**: cần semantics về settlement, phí và điều chỉnh. Không tự clamp về 0 hay đổi dấu.
- Ở Shopee, 4.497 dòng non-primary vẫn có `buyer_total_amount>0`; phí và escrow cũng xuất hiện ở nhiều dòng non-primary. Đây là bằng chứng không được cộng thẳng các trường tiền header trên mọi line. Nó chưa chứng minh chính xác công thức phân bổ/phí của nguồn.
- Chưa đối soát tổng doanh thu hoặc payout vì chưa có business grain và định nghĩa trường tiền đáng tin. Kiểm tra parse/range không tương đương kiểm chứng KPI tài chính.

## 5. Thời gian và coverage

Không có giá trị non-null không cast được trong 24 cột timestamp được kiểm tra. Không thực hiện timezone conversion vì timezone gốc chưa được xác nhận.

| Phát hiện | Ý nghĩa và xử lý |
|---|---|
| TikTok `shipped_time`, `completed_time`, `auto_cancel_time` null toàn bộ | Không tính duration từ các cột này hoặc mặc định chúng bằng mốc khác |
| Shopee `raw_payload`, `estimated_shipping_date` null toàn bộ | Không dựa vào payload phục hồi ID hay estimated date để tính SLA |
| TikTok `delivery_sla_time` có dữ liệu 5.812/102.000 dòng, khoảng 5,70% | SLA phải hiển thị coverage và chỉ đo tập đủ timestamp; không coi phần thiếu là on-time |
| 9 dòng TikTok `COMPLETED` thiếu `delivered_time` | Đếm như anomaly, không loại tùy ý hay tự điền ngày |
| 165 dòng Shopee `COMPLETED` thiếu `completed_time` | Chỉ số theo ngày hoàn tất có phần thiếu cần công khai |
| 118 dòng TikTok trả tiền trước ngày tạo đúng 1 giây | Low severity; có thể do clock/rounding/semantics; không nên đánh rớt batch vì quy tắc timestamp tuyệt đối trước khi xác minh |

Không thấy delivered trước created trong TikTok; không thấy paid/shipped/completed trước created trong Shopee ở các kiểm tra đã chạy. Các kiểm tra này không bao phủ mọi quy tắc thời gian nghiệp vụ.

Khoảng created time: TikTok 31/10/2025–09/09/2026; Shopee 13/12/2025–07/09/2026. Volume theo tháng rất khác: TikTok tập trung nhiều ở 06–07/2026, Shopee ở 01–02/2026. Chỉ riêng giao nhau min/max chưa chứng minh cùng kỳ lấy mẫu, completeness hay cùng shop/population. Không diễn giải volume tăng/giảm thành tăng trưởng thị trường khi chưa có cơ chế export và coverage nguồn.

## 6. Có thể tiếp tục làm gì

| Mục tiêu | Đánh giá |
|---|---|
| Ingestion, schema checks, raw lineage, Docker, Airflow, retry, publish | Làm được với hai snapshot hiện có |
| Chất lượng cột và phân bố dữ liệu ở mức quan sát CSV | Làm được, ghi rõ grain là observation và coverage |
| Order count/AOV/cancellation theo đơn, tổng tiền header | Chưa đủ business key đáng tin; không công bố như KPI đã kiểm chứng |
| So cùng product/customer/shop giữa hai kênh | Chưa đủ định danh để mapping đáng tin |
| Geography giữa hai kênh | Shopee bị che nhãn địa lý; không tự join theo prefix |
| Market share/insight đại diện thị trường | Không đủ bằng chứng về mẫu và nguồn gốc |
| Incremental/backfill có expected output đúng | Dùng fixture synthetic có keys ổn định; replay gắn nhãn mô phỏng |

Đề xuất tiếp theo trong phạm vi project: giữ toàn bộ raw observations với technical row key `(source, file_sha256, record_ordinal)`, không dedup bằng masked PK; tách một nhánh dữ liệu snapshot dùng cho profiling và một fixture đa kênh có grain xác định dùng chứng minh core facts, metrics, incremental/recovery. Nếu sau này có nguồn export mới, ưu tiên stable pseudonymous keys bảo toàn quan hệ một-một thay vì chỉ giữ prefix. Chưa thay đổi pipeline hoặc xóa dữ liệu trong lần audit này.

## 7. Quy tắc kiểm thử nên bổ sung

- CSV record width và strict parser; xử lý quoted multiline cells.
- Phát hiện masking ở cột bắt buộc làm business key; chặn chế độ trusted order analytics khi key chỉ là mask.
- File-hash idempotency ở raw ingestion; không đánh đồng file retry với business dedup.
- Required numeric cast, quantity hợp lệ, currency partition và null policy theo trường.
- Coverage của timestamp/SLA; không bật freshness threshold dữ liệu mới cho snapshot tĩnh.
- Regression trên fixture: hai order IDs gốc khác nhau có cùng prefix vẫn là hai đơn; đơn nhiều line không nhân đôi tiền header; full/incremental/retry parity.
- Không tự viết test `escrow_amount>=0` hoặc `paid_time>=created_time` tuyệt đối khi các ngoại lệ chưa được giải thích.

Giới hạn còn lại: không có row count/SQL export/schema constraints từ database gốc; không kiểm chứng được nguồn che ID, tính thực/giả lập hay phạm vi lấy mẫu. Không có cách chứng minh khôi phục ID gốc từ dấu `*` chỉ bằng hai CSV này.
