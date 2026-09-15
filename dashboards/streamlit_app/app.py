import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="TikTok Shop Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Ẩn Taskbar, Menu và Footer của Streamlit */
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}

    /* Gradient Background cho Metrics */
    div[data-testid="stMetricValue"] {
        font-size: 28px !important;
        font-weight: 800 !important;
        color: #ff0050;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 14px !important;
        font-weight: 600 !important;
        color: #333333;
    }
    /* Style chung */
    .block-container {
        padding-top: 1rem !important;
    }
    h1 {
        background: -webkit-linear-gradient(45deg, #00f2fe, #4facfe, #00f2fe);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 900 !important;
    }
    
    /* Làm đẹp các Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
        font-size: 16px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: transparent !important;
        border-bottom: 3px solid #ff0050 !important;
        color: #ff0050 !important;
    }
    
    /* Khung (Card) cho các Metrics */
    div[data-testid="metric-container"] {
        background-color: #ffffff;
        border: 1px solid #e6e6e6;
        padding: 15px 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 12px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# --- DB CONNECTION ---
@st.cache_resource
def get_db_connection():
    db_path = Path(__file__).parents[2] / "data" / "duckdb" / "ecom_warehouse.duckdb"
    return duckdb.connect(str(db_path), read_only=True)

conn = get_db_connection()

# --- HEADER ---
st.title("TikTok Shop Intelligence Dashboard")
st.markdown("*Dữ liệu được xử lý qua kiến trúc Medallion (dbt + DuckDB)*")
st.divider()

# --- FETCH DATA ---
@st.cache_data(ttl=600)
def load_data(query):
    return conn.execute(query).df()

# 1. Daily Revenue Data
df_daily = load_data("SELECT * FROM main_marts.mart_daily_revenue ORDER BY order_date")
# 2. Product Performance
df_product = load_data("SELECT * FROM main_marts.mart_product_performance LIMIT 50")
# 3. Geo Analysis
df_geo = load_data("SELECT * FROM main_marts.mart_geo_analysis WHERE region_state != 'Không xác định'")
# 4. Financial
df_finance = load_data("SELECT * FROM main_marts.mart_financial_reconciliation ORDER BY report_month")


# --- TABS ---
tab1, tab2, tab3, tab4 = st.tabs([
    "Tổng quan Doanh thu", 
    "Top Sản phẩm", 
    "Vận hành & Địa lý",
    "Đối soát Tài chính"
])

# ==========================================
# TAB 1: TỔNG QUAN DOANH THU
# ==========================================
with tab1:
    st.subheader("Bức tranh Kinh doanh Tổng thể")
    
    # Tổng hợp số liệu
    total_gross = df_daily['gross_revenue'].sum()
    total_net = df_daily['net_revenue'].sum()
    total_orders = df_daily['total_orders'].sum()
    avg_aov = df_daily['avg_order_value'].mean()
    cancel_rate = (df_daily['cancelled_orders'].sum() / total_orders * 100) if total_orders > 0 else 0
    
    # Metrics Header
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Tổng Doanh thu (Gross)", f"{total_gross:,.0f} ₫")
    col2.metric("Doanh thu Thực nhận (Net)", f"{total_net:,.0f} ₫")
    col3.metric("Tổng Số Đơn", f"{total_orders:,}")
    col4.metric("Giá trị Đơn TB (AOV)", f"{avg_aov:,.0f} ₫")
    col5.metric("Tỷ lệ Hủy Đơn", f"{cancel_rate:.1f}%")
    
    st.markdown("---")
    
    # Charts
    col_chart1, col_chart2 = st.columns([2, 1])
    
    with col_chart1:
        # Biểu đồ xu hướng doanh thu
        fig_revenue = px.area(
            df_daily, 
            x='order_date', 
            y=['gross_revenue', 'net_revenue'],
            labels={'value': 'Doanh thu (VNĐ)', 'order_date': 'Ngày', 'variable': 'Chỉ số'},
            title='Xu hướng Doanh thu theo Ngày',
            color_discrete_sequence=['#00f2fe', '#ff0050']
        )
        fig_revenue.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig_revenue, use_container_width=True)
        
    with col_chart2:
        # Biểu đồ trạng thái đơn hàng
        total_completed = df_daily['completed_orders'].sum()
        total_cancelled = df_daily['cancelled_orders'].sum()
        total_processing = df_daily['processing_orders'].sum()
        
        fig_status = px.pie(
            names=['Hoàn thành', 'Đã hủy', 'Đang xử lý'],
            values=[total_completed, total_cancelled, total_processing],
            title='Tỷ trọng Trạng thái Đơn hàng',
            hole=0.4,
            color_discrete_sequence=['#00C49F', '#FF4D4F', '#FFBB28']
        )
        fig_status.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_status, use_container_width=True)

# ==========================================
# TAB 2: HIỆU SUẤT SẢN PHẨM
# ==========================================
with tab2:
    st.subheader("Phân tích Bán hàng theo Sản phẩm")
    
    col_p1, col_p2 = st.columns([1, 1])
    with col_p1:
        # Top 10 SKUs by Revenue
        top10_revenue = df_product.head(10).sort_values(by='total_revenue', ascending=True).copy()
        top10_revenue['short_name'] = top10_revenue['product_name'].apply(lambda x: x[:35] + '...' if len(str(x)) > 35 else x)
        
        fig_top_revenue = px.bar(
            top10_revenue, 
            x='total_revenue', 
            y='short_name', 
            orientation='h',
            title='Top 10 Sản phẩm mang lại Doanh thu lớn nhất',
            color='total_revenue',
            color_continuous_scale='Blues',
            hover_data={'product_name': True, 'short_name': False}
        )
        fig_top_revenue.update_layout(yaxis_title=None, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_top_revenue, use_container_width=True)
        
    with col_p2:
        # Phân tán (Scatter) - Doanh thu vs Tỷ lệ hủy
        fig_scatter = px.scatter(
            df_product.head(30), 
            x='total_revenue', 
            y='cancellation_rate_pct',
            size='total_qty_sold',
            color='cancellation_rate_pct',
            hover_name='product_name',
            title='Tương quan Doanh thu & Tỷ lệ hủy đơn (Size = Số lượng bán)',
            color_continuous_scale='Reds',
            labels={'total_revenue': 'Tổng Doanh thu', 'cancellation_rate_pct': 'Tỷ lệ Hủy (%)'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    # Data table chi tiết
    df_product_vn = df_product[['product_id', 'product_name', 'total_qty_sold', 'total_revenue', 'avg_sale_price', 'cancellation_rate_pct']].rename(columns={
        'product_id': 'Mã SP',
        'product_name': 'Tên Sản Phẩm',
        'total_qty_sold': 'Số lượng Bán',
        'total_revenue': 'Tổng Doanh thu',
        'avg_sale_price': 'Giá bán TB',
        'cancellation_rate_pct': 'Tỷ lệ Hủy (%)'
    })
    st.markdown("#### Bảng Chi Tiết Sản Phẩm")
    st.dataframe(df_product_vn, use_container_width=True, hide_index=True)

# ==========================================
# TAB 3: VẬN HÀNH & ĐỊA LÝ
# ==========================================
with tab3:
    st.subheader("Phân phối Địa lý & Vận hành")
    
    # Doanh thu theo tỉnh thành (Treemap)
    fig_geo = px.treemap(
        df_geo.head(20), 
        path=['region_state', 'city_town'], 
        values='gross_revenue',
        color='gross_revenue',
        color_continuous_scale='Mint',
        title='Top 20 Tỉnh/Thành mang lại Doanh thu cao nhất'
    )
    fig_geo.update_layout(margin=dict(t=40, b=10, l=10, r=10))
    st.plotly_chart(fig_geo, use_container_width=True)
    
    st.markdown("---")
        
    # Phân tích phương thức thanh toán
    payment_summary = df_geo.groupby('top_payment_method')['total_orders'].sum().reset_index()
    payment_summary = payment_summary.sort_values('total_orders', ascending=True)
    
    fig_payment = px.bar(
        payment_summary,
        x='total_orders',
        y='top_payment_method',
        orientation='h',
        title='Cơ cấu Phương thức Thanh toán theo Đơn hàng',
        labels={'total_orders': 'Tổng số đơn', 'top_payment_method': 'Phương thức TT'},
        color='total_orders',
        color_continuous_scale='Purples'
    )
    fig_payment.update_layout(margin=dict(t=40, b=10, l=10, r=10), yaxis_title=None)
    st.plotly_chart(fig_payment, use_container_width=True)

# ==========================================
# TAB 4: ĐỐI SOÁT TÀI CHÍNH
# ==========================================
with tab4:
    st.subheader("Biến động Dòng tiền (Gross to Net)")
    
    if not df_finance.empty:
        # Lấy tháng gần nhất
        latest_month_df = df_finance.iloc[-1]
        
        # Biểu đồ Waterfall (Thác nước) mô phỏng dòng tiền
        fig_waterfall = go.Figure(go.Waterfall(
            name="2026", orientation="v",
            measure=["absolute", "relative", "relative", "relative", "relative", "total"],
            x=["Gross GMV", "Chiết khấu Sàn", "Chiết khấu Shop", "Phí Vận chuyển", "Thuế & Phí phụ", "Net Revenue"],
            textposition="outside",
            text=[f"{latest_month_df['gross_gmv']/1e6:.1f}M", 
                  f"-{latest_month_df['platform_discount']/1e6:.1f}M", 
                  f"-{latest_month_df['seller_discount']/1e6:.1f}M", 
                  f"-{latest_month_df['actual_shipping_fee']/1e6:.1f}M", 
                  f"-{latest_month_df['total_tax']/1e6:.1f}M", 
                  f"{latest_month_df['net_revenue']/1e6:.1f}M"],
            y=[
                latest_month_df['gross_gmv'], 
                -latest_month_df['platform_discount'], 
                -latest_month_df['seller_discount'], 
                -latest_month_df['actual_shipping_fee'], 
                -latest_month_df['total_tax'], 
                latest_month_df['net_revenue']
            ],
            connector={"line":{"color":"rgb(63, 63, 63)"}},
        ))
        
        fig_waterfall.update_layout(
            title="Mô phỏng Dòng tiền từ GMV xuống Net Revenue (Tháng gần nhất)",
            showlegend=False,
            waterfallgap=0.3,
            margin=dict(t=50, b=10, l=10, r=10)
        )
        st.plotly_chart(fig_waterfall, use_container_width=True)
        
        # Việt hóa tên cột và hiển thị
        df_finance_vn = df_finance.rename(columns={
            'report_month': 'Tháng Báo Cáo',
            'order_status': 'Trạng thái Đơn',
            'total_orders': 'Tổng số đơn',
            'gross_gmv': 'Tổng GMV (Gross)',
            'platform_discount': 'Sàn Tài Trợ',
            'seller_discount': 'Shop Tài Trợ',
            'total_discount': 'Tổng Chiết Khấu',
            'actual_shipping_fee': 'Phí Vận Chuyển',
            'total_tax': 'Thuế & Phí Khác',
            'net_revenue': 'Dòng Tiền Thực Nhận (Net Cash Flow)'
        })
        st.markdown("#### Bảng Kê Dòng Tiền Chi Tiết (Cash Flow)")
        st.dataframe(df_finance_vn, use_container_width=True, hide_index=True)
    else:
        st.info("Chưa đủ dữ liệu đối soát tài chính theo tháng.")
