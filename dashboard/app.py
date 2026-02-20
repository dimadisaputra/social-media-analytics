import os
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Load variabel dari file .env di root directory
load_dotenv()

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Social Media Analytics", page_icon="🚀", layout="wide", initial_sidebar_state="expanded")

# Custom CSS untuk mempercantik UI
st.markdown("""
    <style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
    </style>
""", unsafe_allow_html=True)

# --- DATABASE CONNECTION ---
@st.cache_resource
def init_connection():
    private_key_path = os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"))
    with open(private_key_path, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="GOLD_DEV" 
    )

@st.cache_data(ttl=600)
def run_query(query):
    conn = init_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetch_pandas_all()
    # Konversi kolom tanggal ke format datetime pandas
    if 'DATE_DAY' in df.columns:
        df['DATE_DAY'] = pd.to_datetime(df['DATE_DAY'])
    return df

# --- FETCH DATA ---
with st.spinner('Memuat data dari Data Warehouse...'):
    # Kita tambahkan post_description agar bisa diintip kontennya
    query_posts = """
        SELECT 
            p.post_key,
            p.platform,
            p.post_description,
            p.view_count,
            p.like_count,
            p.comment_count,
            p.share_count,
            d.date_day,
            u.username
        FROM fct_social_posts p
        JOIN dim_date d ON p.created_date_key = d.date_day
        JOIN dim_users u ON p.author_user_key = u.user_key
    """
    df_posts = run_query(query_posts)

# --- UI MAIN APP ---
if df_posts.empty:
    st.warning("Belum ada data di layer GOLD_DEV.")
    st.stop()

# --- SIDEBAR: FILTERS ---
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/1968/1968641.png", width=50) # Dummy icon
st.sidebar.title("Filter Control 🎛️")

# 1. Date Range Filter
st.sidebar.subheader("Time Period")
min_date = df_posts['DATE_DAY'].min().date()
max_date = df_posts['DATE_DAY'].max().date()

date_preset = st.sidebar.selectbox("Quick Range", ["All Time", "Year to Date (YTD)", "Last 30 Days", "Custom Range"])

if date_preset == "All Time":
    start_date, end_date = min_date, max_date
elif date_preset == "Year to Date (YTD)":
    start_date, end_date = datetime(max_date.year, 1, 1).date(), max_date
elif date_preset == "Last 30 Days":
    start_date, end_date = max_date - timedelta(days=30), max_date
else:
    # Custom Range Picker
    date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = date_range[0], date_range[0]

# 2. Platform Filter
st.sidebar.subheader("Platform")
platforms_available = df_posts['PLATFORM'].unique().tolist()
selected_platforms = st.sidebar.multiselect("Select Platform", platforms_available, default=platforms_available)

# 3. Account / Username Filter
st.sidebar.subheader("Accounts")
accounts_available = sorted(df_posts['USERNAME'].unique().tolist())
selected_accounts = st.sidebar.multiselect("Select Accounts", accounts_available, default=accounts_available)

# --- APPLY FILTERS ---
mask = (
    (df_posts['DATE_DAY'].dt.date >= start_date) & 
    (df_posts['DATE_DAY'].dt.date <= end_date) &
    (df_posts['PLATFORM'].isin(selected_platforms)) &
    (df_posts['USERNAME'].isin(selected_accounts))
)
filtered_df = df_posts[mask]


# --- MAIN DASHBOARD AREA ---
st.title("🚀 Social Media Performance Hub")
st.markdown(f"Menganalisis performa konten dari **{start_date.strftime('%d %b %Y')}** hingga **{end_date.strftime('%d %b %Y')}**")

if filtered_df.empty:
    st.info("Tidak ada data yang cocok dengan filter yang dipilih.")
    st.stop()

# --- KPI METRICS ---
total_views = filtered_df['VIEW_COUNT'].sum()
total_likes = filtered_df['LIKE_COUNT'].sum()
total_comments = filtered_df['COMMENT_COUNT'].sum()
total_shares = filtered_df['SHARE_COUNT'].sum()

# Engagement Rate: (Likes + Comments + Shares) / Views
engagement_rate = ((total_likes + total_comments + total_shares) / total_views * 100) if total_views > 0 else 0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("👁️ Total Views", f"{total_views:,.0f}")
col2.metric("❤️ Total Likes", f"{total_likes:,.0f}")
col3.metric("💬 Total Comments", f"{total_comments:,.0f}")
col4.metric("🔁 Total Shares", f"{total_shares:,.0f}")
col5.metric("🔥 Engagement Rate", f"{engagement_rate:.2f}%")

st.markdown("---")

# --- TABS FOR MODERN UI ---
tab1, tab2, tab3 = st.tabs(["📊 Overview & Trends", "🏆 Creators Performance", "🎯 Content Analysis"])

with tab1:
    col_t1, col_t2 = st.columns([2, 1])
    
    with col_t1:
        st.subheader("📈 Traffic Trend over Time")
        # Aggregasi data per tanggal
        trend_df = filtered_df.groupby('DATE_DAY').agg({'VIEW_COUNT': 'sum', 'LIKE_COUNT': 'sum'}).reset_index()
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=trend_df['DATE_DAY'], y=trend_df['VIEW_COUNT'], name="Views", line=dict(color="#00C4B6", width=3)))
        fig_trend.add_trace(go.Scatter(x=trend_df['DATE_DAY'], y=trend_df['LIKE_COUNT'], name="Likes", line=dict(color="#FF3366", width=2)))
        fig_trend.update_layout(margin=dict(l=0, r=0, t=30, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig_trend, use_container_width=True)

    with col_t2:
        st.subheader("🌐 Platform Share")
        platform_df = filtered_df.groupby('PLATFORM')['VIEW_COUNT'].sum().reset_index()
        fig_donut = px.pie(platform_df, values='VIEW_COUNT', names='PLATFORM', hole=0.5, color_discrete_sequence=px.colors.sequential.Teal)
        fig_donut.update_layout(margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_donut, use_container_width=True)

with tab2:
    st.subheader("👤 Top Performing Accounts")
    # PERBAIKAN: Gunakan POST_KEY huruf besar
    acc_df = filtered_df.groupby('USERNAME').agg({'VIEW_COUNT': 'sum', 'LIKE_COUNT': 'sum', 'POST_KEY': 'count'}).reset_index()
    acc_df.rename(columns={'POST_KEY': 'Total Posts'}, inplace=True)
    acc_df = acc_df.sort_values(by='VIEW_COUNT', ascending=True) 
    
    fig_bar = px.bar(acc_df, x='VIEW_COUNT', y='USERNAME', orientation='h', 
                     hover_data=['LIKE_COUNT', 'Total Posts'], 
                     labels={'VIEW_COUNT': 'Total Views', 'USERNAME': 'Account'},
                     color='VIEW_COUNT', color_continuous_scale='Viridis')
    fig_bar.update_layout(margin=dict(l=0, r=0, t=30, b=0), coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)

with tab3:
    col_c1, col_c2 = st.columns(2)
    
    with col_c1:
        st.subheader("🌌 Engagement Scatter Plot")
        st.caption("Korelasi antara Views dan Likes untuk mendeteksi konten viral.")
        # PERBAIKAN: Gunakan POST_DESCRIPTION huruf besar
        fig_scatter = px.scatter(filtered_df, x="VIEW_COUNT", y="LIKE_COUNT", color="PLATFORM", 
                                 hover_data=["USERNAME", "POST_DESCRIPTION"],
                                 labels={'VIEW_COUNT': 'Total Views', 'LIKE_COUNT': 'Total Likes'},
                                 opacity=0.7)
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    with col_c2:
        st.subheader("📋 Top 10 Contents by Views")
        top_posts = filtered_df.sort_values(by="VIEW_COUNT", ascending=False).head(10)
        
        # PERBAIKAN: Gunakan POST_DESCRIPTION huruf besar
        display_df = top_posts[['USERNAME', 'POST_DESCRIPTION', 'VIEW_COUNT', 'LIKE_COUNT', 'PLATFORM']].copy()
        
        # Potong deskripsi jika terlalu panjang
        display_df['POST_DESCRIPTION'] = display_df['POST_DESCRIPTION'].apply(lambda x: str(x)[:50] + '...' if len(str(x)) > 50 else x)
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)