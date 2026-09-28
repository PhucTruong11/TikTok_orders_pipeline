{% macro clean_string(column_name) %}
    -- Xóa khoảng trắng ở đầu và cuối chuỗi
    -- Loại bỏ các khoảng trắng thừa liên tiếp (ví dụ: "Nguyễn   Văn   A" -> "Nguyễn Văn A")
    -- Ép kiểu varchar để đảm bảo tương thích
    trim(regexp_replace(cast({{ column_name }} as varchar), '\s+', ' ', 'g'))
{% endmacro %}
