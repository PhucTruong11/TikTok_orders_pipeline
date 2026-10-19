"""Snapshot analytics over the published two-source Gold layer."""
from pathlib import Path
import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

ROOT=Path(__file__).resolve().parents[2]
st.set_page_config(page_title='Shopee · TikTok | Snapshot analytics',layout='wide')
st.markdown("""<style>
.block-container {max-width:1440px;padding-top:2rem;}
div[data-testid="stMetric"] {border:1px solid #64748b55;border-radius:12px;padding:16px;}
</style>""",unsafe_allow_html=True)
COLORS={'tiktok':'#38bdf8','shopee':'#fb923c'}
LABELS={'tiktok':'TikTok','shopee':'Shopee'}
STATUS_LABELS={'UNPAID':'Chưa thanh toán','AWAITING_SHIPMENT':'Chờ chuẩn bị hàng',
 'AWAITING_COLLECTION':'Chờ lấy hàng','IN_TRANSIT':'Đang vận chuyển','DELIVERED':'Đã giao',
 'COMPLETED':'Hoàn tất','CANCELLED':'Đã hủy','RETURN_REQUESTED':'Yêu cầu trả hàng','RETURNED':'Đã trả hàng'}


@st.cache_data(show_spinner=False)
def load_gold(path, signature):
    # Cache dataframes only. Close every connection so a new DB can be published.
    with duckdb.connect(path,read_only=True) as connection:
        return tuple(connection.execute('select * from gold.'+name).fetchdf() for name in (
            'gold_channel_daily','gold_observation_quality','gold_status_distribution','gold_pipeline_metadata'))


def filter_dates(frame, channels, currency, start, end):
    dates=pd.to_datetime(frame['observation_date']).dt.date
    return frame[frame['source_channel'].isin(channels) & frame['currency'].eq(currency)
                 & dates.between(start,end)].copy()


def chart(fig):
    fig.update_layout(margin=dict(l=12,r=12,t=35,b=12),legend_title_text='Nguồn dữ liệu')
    st.plotly_chart(fig,width='stretch')


def main():
    st.title('Shopee · TikTok')
    st.caption('Snapshot analytics · Bronze → Silver → Gold · Dagster / dbt / DuckDB local')
    st.info('Một bản ghi là một observation trong CSV, chưa phải một đơn hàng hoặc SKU duy nhất. '
            'Thông tin đã mask được giữ nguyên. Các so sánh chỉ mô tả dataset này.')
    database=Path(os.environ.get('DBT_DUCKDB_PATH',ROOT/'data/duckdb/ecom_medallion.duckdb')).resolve()
    if not database.exists():
        st.info('Chưa có warehouse Medallion. Từ thư mục project, chạy:')
        st.code('.venv/Scripts/python.exe scripts/build_warehouse.py',language='powershell')
        return
    try:
        stat=database.stat()
        daily,quality,statuses,metadata=load_gold(str(database),(stat.st_mtime_ns,stat.st_size))
    except duckdb.Error:
        st.warning('Chưa đọc được các bảng Gold. Hãy hoàn tất build warehouse rồi bấm Làm mới dữ liệu.')
        if st.button('Làm mới dữ liệu',key='retry_refresh'):
            load_gold.clear();st.rerun()
        return
    if daily.empty:
        st.info('Warehouse chưa có observations để hiển thị.');return
    with st.sidebar:
        st.header('Phạm vi phân tích')
        channels=st.multiselect('Nguồn dữ liệu',sorted(daily.source_channel.unique()),
                               default=sorted(daily.source_channel.unique()),format_func=lambda x:LABELS.get(x,x))
        currencies=sorted(daily.currency.unique())
        currency=st.selectbox('Tiền tệ',currencies,index=currencies.index('VND') if 'VND' in currencies else 0)
        common_period=st.checkbox('Ưu tiên kỳ giao nhau giữa các nguồn',value=True)
        if st.button('Làm mới dữ liệu'):
            load_gold.clear();st.rerun()
    if not channels:
        st.info('Chọn ít nhất một nguồn dữ liệu.');return
    available=daily[daily.source_channel.isin(channels) & daily.currency.eq(currency)]
    if available.empty:
        st.info('Không có bản ghi cho nguồn và tiền tệ đã chọn.');return
    dates=pd.to_datetime(available.observation_date).dt.date
    lower,upper=dates.min(),dates.max()
    start,end=lower,upper
    if common_period and len(channels)>1:
        coverage=available.assign(day=dates).groupby('source_channel').day.agg(['min','max'])
        if len(coverage)==len(channels) and coverage['min'].max()<=coverage['max'].min():
            start,end=coverage['min'].max(),coverage['max'].min()
        else:
            st.caption('Không có kỳ giao nhau ở mọi nguồn cho tiền tệ này; đang dùng cửa sổ có dữ liệu. '
                       'Việc giao nhau theo ngày cũng chưa chứng minh hai nguồn có cùng cơ cấu mẫu.')
    selected=st.sidebar.date_input('Khoảng ngày tạo trong dữ liệu',value=(start,end),min_value=lower,max_value=upper,
                                   key='dates_'+currency+'_'+','.join(channels)+'_'+str(common_period))
    if len(selected)!=2:
        st.info('Chọn đủ ngày bắt đầu và kết thúc.');return
    start,end=selected
    daily=filter_dates(daily,channels,currency,start,end)
    quality=filter_dates(quality,channels,currency,start,end)
    statuses=filter_dates(statuses,channels,currency,start,end)
    if daily.empty:
        st.info('Không có observations trong khoảng đã chọn.');return
    meta=metadata[metadata.source_channel.isin(channels) & metadata.currency.eq(currency)]
    if not meta.empty:
        built=pd.to_datetime(meta.last_build_at,utc=True).max().tz_convert('Asia/Bangkok')
        st.caption('Build gần nhất: '+built.strftime('%d/%m/%Y %H:%M:%S %z')+
                   ' · Đây là thời điểm build, không phải ngày export hoặc thời điểm thị trường hiện tại.')
    count=int(daily.observation_count.sum())
    priced_count=count-int(daily.missing_sale_price_count.sum())
    cards=st.columns(4)
    cards[0].metric('Bản ghi quan sát',f'{count:,}')
    cards[1].metric('Nguồn có dữ liệu',str(daily.source_channel.nunique()))
    cards[2].metric('Bản ghi có giá bán',f'{priced_count:,}')
    cards[3].metric('Coverage giá bán',f'{100*priced_count/count:.1f}%')
    overview,status_tab,quality_tab=st.tabs(['Tổng quan','Trạng thái observations','Chất lượng và coverage'])
    with overview:
        st.subheader('Số bản ghi theo ngày')
        chart(px.line(daily.sort_values('observation_date'),x='observation_date',y='observation_count',color='source_channel',
                      color_discrete_map=COLORS,labels={'observation_date':'Ngày tạo','observation_count':'Bản ghi'}))
        summary=daily.groupby('source_channel',as_index=False)[['observation_count','observed_unit_count','priced_unit_count','missing_sale_price_count','observed_sales_value_sum']].sum(min_count=1)
        summary['weighted_observed_sale_price']=summary.observed_sales_value_sum/summary.priced_unit_count.replace(0,float('nan'))
        display=summary[['source_channel','observation_count','observed_unit_count','missing_sale_price_count','weighted_observed_sale_price']].rename(columns={
            'source_channel':'Nguồn','observation_count':'Bản ghi','observed_unit_count':'Đơn vị ghi nhận',
            'missing_sale_price_count':'Bản ghi thiếu giá','weighted_observed_sale_price':'Giá bán ghi nhận có trọng số ('+currency+')'})
        st.dataframe(display,width='stretch',hide_index=True)
        st.subheader('Phân bố giá theo thời gian')
        price_rows=daily[daily.weighted_observed_sale_price.notna()]
        if price_rows.empty:
            st.info('Không có giá bán được ghi nhận trong phạm vi đã chọn.')
        else:
            chart(px.line(price_rows.sort_values('observation_date'),x='observation_date',y='weighted_observed_sale_price',color='source_channel',
                          color_discrete_map=COLORS,labels={'observation_date':'Ngày tạo','weighted_observed_sale_price':'Giá ghi nhận ('+currency+')'}))
        st.caption('Giá có trọng số được tính từ tổng giá × quantity chia tổng quantity của phần có giá; '
                   'missing không được thay bằng 0. Chưa đối sánh cùng SKU giữa hai nguồn, nên khác biệt giá có thể do cơ cấu sản phẩm. '
                   'Không diễn giải thành AOV, doanh thu đơn hàng hoặc thị phần.')
    with status_tab:
        grouped=statuses.groupby(['source_channel','standardized_status'],as_index=False).observation_count.sum()
        totals=grouped.groupby('source_channel').observation_count.transform('sum')
        grouped['observation_pct']=100*grouped.observation_count/totals
        grouped['status_label']=grouped.standardized_status.map(STATUS_LABELS).fillna(grouped.standardized_status)
        chart(px.bar(grouped,x='status_label',y='observation_pct',color='source_channel',barmode='group',color_discrete_map=COLORS,
                     labels={'status_label':'Trạng thái chuẩn hóa','observation_pct':'Tỷ trọng observations (%)'}))
        st.dataframe(grouped[['source_channel','standardized_status','observation_count','observation_pct']],width='stretch',hide_index=True)
        st.caption('TO_RETURN của Shopee là yêu cầu trả hàng, được tách khỏi đã trả hàng. Tỷ trọng trên đây đếm records, không phải tỷ lệ hủy/hoàn theo đơn.')
    with quality_tab:
        grouped=quality.groupby('source_channel',as_index=False).sum(numeric_only=True)
        for output,numerator in [('price_coverage_pct','missing_sale_price_count'),('delivery_sla_coverage_pct','delivery_sla_eligible_count'),('completed_time_coverage_pct','completed_time_populated_count')]:
            grouped[output]=100*grouped[numerator]/grouped.observation_count
        grouped['price_coverage_pct']=100-grouped.price_coverage_pct
        st.dataframe(grouped[['source_channel','observation_count','unmapped_status_count','masked_pk_count','masked_order_id_count','price_coverage_pct','delivery_sla_coverage_pct','completed_time_coverage_pct']],width='stretch',hide_index=True)
        st.caption('Masking là đặc tính được chấp nhận. Shopee không có delivered_time/delivery SLA tương đương trong CSV này; '
                   'coverage bằng 0 nghĩa là không đủ dữ liệu để đo, không phải tất cả đều trễ hoặc đúng hạn.')
        st.subheader('Phạm vi snapshot theo nguồn và tiền tệ')
        st.dataframe(meta[['source_channel','currency','first_observation_date','last_observation_date','observation_count']],width='stretch',hide_index=True)
    st.caption('Bronze: CSV nguyên bản và metadata · Silver: dữ liệu ép kiểu, mapping và observations chung · Gold: các bảng tổng hợp phục vụ giao diện.')


if __name__=='__main__':
    main()
