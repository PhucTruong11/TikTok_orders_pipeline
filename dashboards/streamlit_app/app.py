import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# Cấu hình trang
st.set_page_config(
    page_title="Bảng điều khiển Quản trị",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS phong cách Modern, Minimalist & Glassmorphism
st.markdown("""
<style>
    /* Import modern font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Clean up default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Global Background (Sleek Dark) */
    .stApp {
        background-color: #0d1117;
    }
    
    /* Modern Glassmorphism Metric Cards */
    div[data-testid="metric-container"] {
        background: rgba(22, 27, 34, 0.6);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        padding: 24px;
        border-radius: 16px;
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.2);
        transition: transform 0.2s ease, box-shadow 0.2s ease, border 0.2s ease;
    }
    
    div[data-testid="metric-container"]:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        border: 1px solid rgba(255, 255, 255, 0.15);
    }
    
    /* Metric Label (Title) */
    div[data-testid="metric-container"] > div:nth-child(1) {
        color: #8b949e !important;
        font-size: 13px !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 8px;
    }
    
    /* Metric Value */
    div[data-testid="metric-container"] > div:nth-child(2) {
        color: #ffffff !important;
        font-size: 32px !important;
        font-weight: 600 !important;
        letter-spacing: -1px;
    }
    
    /* Headers */
    h1, h2, h3, h4 {
        color: #ffffff;
        font-weight: 600;
        letter-spacing: -0.5px;
    }
    
    /* Section dividers */
    hr {
        border-color: rgba(255, 255, 255, 0.1);
        margin-top: 3rem;
        margin-bottom: 3rem;
    }
    
    /* Adjust spacing */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Khởi tạo màu sắc theme cho biểu đồ Plotly
chart_theme = {
    'bg': 'rgba(0,0,0,0)',
    'text': '#c9d1d9',
    'grid': 'rgba(255, 255, 255, 0.1)',
    'primary': '#58a6ff',
    'secondary': '#238636',
    'accent': '#a371f7'
}

@st.cache_resource
def get_duckdb_conn():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(current_dir, "../../data/duckdb/ecom_warehouse.duckdb"))
    return duckdb.connect(db_path, read_only=True)

def apply_chart_layout(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(size=18, color='#ffffff')),
        plot_bgcolor=chart_theme['bg'],
        paper_bgcolor=chart_theme['bg'],
        font=dict(color=chart_theme['text'], family="Inter"),
        xaxis=dict(gridcolor=chart_theme['grid'], zerolinecolor=chart_theme['grid']),
        yaxis=dict(gridcolor=chart_theme['grid'], zerolinecolor=chart_theme['grid']),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    return fig

try:
    conn = get_duckdb_conn()
    
    # ---------------- HEADER ----------------
    st.markdown("<h1>Bảng điều khiển Quản trị</h1>", unsafe_allow_html=True)
    st.markdown("<p style='color: #8b949e; font-size: 15px; margin-top: -15px;'>Hệ thống dữ liệu tự động qua dbt & DuckDB</p>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # ================= SECTION 1: TỔNG QUAN =================
    st.subheader("Bức tranh Toàn cảnh Doanh thu")
    
    df_revenue = conn.execute("SELECT * FROM main_marts.mart_daily_revenue ORDER BY order_date DESC").fetchdf()
    
    if not df_revenue.empty:
        col1, col2, col3, col4 = st.columns(4)
        total_net = df_revenue['net_revenue'].sum()
        total_gross = df_revenue['gross_revenue'].sum()
        total_ord = df_revenue['total_orders'].sum()
        avg_aov = total_net / total_ord if total_ord > 0 else 0
        
        col1.metric("Thực nhận (Net)", f"{total_net:,.0f} ₫")
        col2.metric("Tổng doanh thu (Gross)", f"{total_gross:,.0f} ₫")
        col3.metric("Tổng số đơn hàng", f"{total_ord:,}")
        col4.metric("Giá trị trung bình đơn (AOV)", f"{avg_aov:,.0f} ₫")
        
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        fig_rev = px.area(
            df_revenue, x='order_date', y=['net_revenue', 'gross_revenue'], 
            labels={'value': 'Doanh thu (VND)', 'variable': 'Chỉ số', 'order_date': ''},
            color_discrete_sequence=[chart_theme['primary'], chart_theme['accent']]
        )
        
        # Đổi tên chú thích (legend)
        newnames = {'net_revenue': 'Thực nhận', 'gross_revenue': 'Tổng doanh thu'}
        fig_rev.for_each_trace(lambda t: t.update(name = newnames[t.name],
                                  legendgroup = newnames[t.name],
                                  hovertemplate = t.hovertemplate.replace(t.name, newnames[t.name])
                                 ))
                                 
        fig_rev = apply_chart_layout(fig_rev, "Xu hướng Doanh thu")
        st.plotly_chart(fig_rev, use_container_width=True)

    st.divider()

    # ================= SECTION 2: TÀI CHÍNH =================
    st.subheader("Phân tích Dòng tiền")
    try:
        if not df_revenue.empty:
            gross = df_revenue['gross_revenue'].sum()
            platform_disc = -df_revenue['total_platform_discount'].sum()
            seller_disc = -df_revenue['total_seller_discount'].sum()
            # Giả định phí = gross - platform - seller - net
            net = df_revenue['net_revenue'].sum()
            fees = net - (gross + platform_disc + seller_disc)
            
            fig_waterfall = go.Figure(go.Waterfall(
                name = "Dòng tiền", orientation = "v",
                measure = ["absolute", "relative", "relative", "relative", "total"],
                x = ["Tổng Doanh thu", "Sàn trợ giá", "Shop giảm giá", "Phí VC & Thuế", "Thực nhận"],
                textposition = "outside",
                text = [f"{gross/1e6:.1f}M", f"{platform_disc/1e6:.1f}M", f"{seller_disc/1e6:.1f}M", f"{fees/1e6:.1f}M", f"{net/1e6:.1f}M"],
                y = [gross, platform_disc, seller_disc, fees, net],
                connector = {"line":{"color":"rgba(255,255,255,0.2)"}},
                increasing = {"marker":{"color": chart_theme['primary']}},
                decreasing = {"marker":{"color": chart_theme['accent']}},
                totals = {"marker":{"color": "#79c0ff"}}
            ))
            fig_waterfall = apply_chart_layout(fig_waterfall, "Cơ cấu Phân bổ Dòng tiền")
            st.plotly_chart(fig_waterfall, use_container_width=True)
    except Exception as e:
        st.error(e)

    st.divider()

    # ================= SECTION 3: SẢN PHẨM =================
    st.subheader("Hiệu suất Sản phẩm")
    try:
        df_products = conn.execute("SELECT * FROM main_marts.mart_product_performance ORDER BY total_qty_sold DESC").fetchdf()
        if not df_products.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig_qty = px.bar(
                    df_products.head(10), x='total_qty_sold', y='product_name', 
                    orientation='h',
                    color='total_qty_sold', color_continuous_scale='Purp'
                )
                fig_qty.update_layout(yaxis={'categoryorder':'total ascending', 'title': ''}, xaxis={'title': 'Số lượng bán'}, coloraxis_showscale=False)
                fig_qty = apply_chart_layout(fig_qty, "Top 10 Sản phẩm Bán chạy")
                st.plotly_chart(fig_qty, use_container_width=True)
            
            with col2:
                fig_scatter = px.scatter(
                    df_products, x='total_qty_sold', y='total_revenue', 
                    size='total_qty_sold', color='total_revenue',
                    hover_name='product_name', color_continuous_scale='Purp'
                )
                fig_scatter.update_layout(xaxis={'title': 'Số lượng bán'}, yaxis={'title': 'Doanh thu mang lại'}, coloraxis_showscale=False)
                fig_scatter = apply_chart_layout(fig_scatter, "Tương quan Số lượng & Doanh thu")
                st.plotly_chart(fig_scatter, use_container_width=True)
    except Exception as e:
        st.error(e)

    st.divider()

    # ================= SECTION 4: VẬN HÀNH =================
    st.subheader("Tổng quan Vận hành & Vị trí")
    try:
        df_geo = conn.execute("SELECT region_state, count(*) as total_orders FROM main_marts.mart_geo_analysis GROUP BY region_state ORDER BY total_orders DESC").fetchdf()
        if not df_geo.empty:
            fig_geo = px.pie(
                df_geo.head(10), values='total_orders', names='region_state', 
                hole=0.5, color_discrete_sequence=px.colors.sequential.Purp[::-1]
            )
            fig_geo.update_traces(textposition='inside', textinfo='percent+label')
            fig_geo = apply_chart_layout(fig_geo, "Phân bổ Khách hàng theo Tỉnh/Thành phố")
            st.plotly_chart(fig_geo, use_container_width=True)
    except Exception:
        pass

except Exception as e:
    st.error(f"Lỗi: {e}")
    st.info("Chưa có dữ liệu. Vui lòng chạy lệnh `dbt build` trước.")
