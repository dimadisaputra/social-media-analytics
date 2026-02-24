"""
app.py  —  Social Media Analytics Dashboard
─────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import warnings
import urllib3
import os
import sys

# Add the parent directory to sys.path to resolve 'dashboard' package imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# Set page config first
st.set_page_config(
    page_title="Social Media Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from dashboard.config import apply_theme
from dashboard.data import load_data
from dashboard.utils import calc_eng
from dashboard.components import (
    render_kpi_cards, render_trend_chart, render_donut_chart, 
    render_scatter_chart, render_sentiment_overview, 
    render_top_posts, render_wordcloud_section
)

# Apply global CSS
apply_theme()

# Page Header
st.markdown("""
<div class="page-header">
  <p class="page-title">Social Media <span>Analytics</span></p>
  <p class="page-sub">Snowflake · dbt · Cortex Sentiment</p>
</div>
""", unsafe_allow_html=True)

# Load data
with st.spinner("Loading data from Snowflake…"):
    df_posts_raw, df_comments_raw, df_sent_summary_raw = load_data()

if df_posts_raw.empty:
    st.warning("No post data found in GOLD_DEV.")
    st.stop()

# Calculate Engagement Rate
df_posts_raw["ENG_RATE"] = df_posts_raw.apply(calc_eng, axis=1)

# Filter Bar
min_date = df_posts_raw["DATE_DAY"].min().date()
max_date = df_posts_raw["DATE_DAY"].max().date()

f1, f2, f3, f4, f5 = st.columns([1.2, 1.0, 1.6, 1.5, 1.2])

with f1:
    date_preset = st.selectbox("Time Range", ["All Time", "Year to Date", "Last 30 Days", "Custom"])

if date_preset == "All Time":
    start_date, end_date = min_date, max_date
elif date_preset == "Year to Date":
    start_date, end_date = datetime(max_date.year, 1, 1).date(), max_date
elif date_preset == "Last 30 Days":
    start_date, end_date = max_date - timedelta(days=30), max_date
else:
    with f1:
        col_s, col_e = st.columns(2)
        with col_s:
            start_date = st.date_input("Start", min_date)
        with col_e:
            end_date = st.date_input("End", max_date)

with f2:
    platforms_avail = sorted(df_posts_raw["PLATFORM"].dropna().unique().tolist())
    sel_platforms = st.multiselect("Platform", platforms_avail, default=platforms_avail)

with f3:
    accounts_avail = sorted(df_posts_raw["USERNAME"].dropna().unique().tolist())
    sel_accounts = st.multiselect("Account", accounts_avail, default=accounts_avail)

with f4:
    metric_focus = st.selectbox("Trend Metric", ["Likes", "Views (TikTok)", "Engagement Rate", "Comments"])

with f5:
    posts_sort = st.selectbox("Rank Posts by", ["LIKE_COUNT", "COMMENT_COUNT", "VIEW_COUNT"], format_func=lambda x: {"LIKE_COUNT":"Likes","COMMENT_COUNT":"Comments","VIEW_COUNT":"Views"}[x])

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# Apply filters
post_mask = (
    (df_posts_raw["DATE_DAY"].dt.date >= start_date)
    & (df_posts_raw["DATE_DAY"].dt.date <= end_date)
    & (df_posts_raw["PLATFORM"].isin(sel_platforms))
    & (df_posts_raw["USERNAME"].isin(sel_accounts))
)
df = df_posts_raw[post_mask].copy()

if df.empty:
    st.info("No data matches the selected filters.")
    st.stop()

filtered_keys    = set(df["POST_KEY"].tolist())
df_comments      = df_comments_raw[df_comments_raw["POST_KEY"].isin(filtered_keys)].copy()
df_sent_sum      = df_sent_summary_raw[df_sent_summary_raw["POST_KEY"].isin(filtered_keys)].copy()

if "COMMENT_DATE" in df_sent_sum.columns:
    df_sent_sum["COMMENT_DATE"] = pd.to_datetime(df_sent_sum["COMMENT_DATE"])

# KPI Cards
render_kpi_cards(df)

# Charts Section 1
row1_l, row1_r = st.columns([2.3, 1])
with row1_l:
    render_trend_chart(df, metric_focus)
with row1_r:
    render_donut_chart(df, metric_focus)

# Sentiment Overview
render_sentiment_overview(df, df_comments, df_sent_sum)

# Scatter Chart
render_scatter_chart(df)

# Word Cloud
render_wordcloud_section(df_comments)

# Top Posts
render_top_posts(df, df_sent_sum, posts_sort)

# Footer
st.markdown("""
<div style="margin-top:48px;text-align:center;color:#D1D5DB;font-size:11px;letter-spacing:.04em;">
  Social Media Analytics · Snowflake + dbt + Cortex
</div>
""", unsafe_allow_html=True)