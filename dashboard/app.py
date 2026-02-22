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

load_dotenv()

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Pulse · Social Analytics",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

def hex_to_rgba(hex_color, alpha=0.06):
    hex_color = hex_color.lstrip("#")
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"

# ─────────────────────────────────────────────
# GLOBAL STYLES
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;1,9..40,300&display=swap');

/* ── Reset & Base ── */
*, *::before, *::after { box-sizing: border-box; }

html, body, [data-testid="stAppViewContainer"] {
    background-color: #080B12 !important;
    color: #E8EAF0 !important;
    font-family: 'DM Sans', sans-serif !important;
}

/* Remove default Streamlit padding */
.block-container { padding: 2rem 2.5rem 4rem 2.5rem !important; max-width: 1400px !important; }
[data-testid="stSidebar"] { background: #0D1117 !important; border-right: 1px solid #1E2535 !important; }
[data-testid="stSidebar"] > div { padding: 1.5rem 1.2rem !important; }

/* ── Typography ── */
h1, h2, h3, h4 { font-family: 'Syne', sans-serif !important; letter-spacing: -0.02em !important; }

/* ── Sidebar ── */
.sidebar-logo {
    font-family: 'Syne', sans-serif;
    font-size: 1.4rem;
    font-weight: 800;
    letter-spacing: -0.03em;
    color: #fff;
    margin-bottom: 2rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid #1E2535;
    display: flex;
    align-items: center;
    gap: 10px;
}
.sidebar-logo span { color: #5B7FFF; }

.sidebar-section-label {
    font-family: 'Syne', sans-serif;
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #4A5568;
    margin: 1.5rem 0 0.5rem 0;
}

/* ── Streamlit Widget Overrides ── */
[data-testid="stSelectbox"] > div, [data-testid="stMultiSelect"] > div {
    background: #0D1117 !important;
}
div[data-baseweb="select"] > div { background: #131925 !important; border: 1px solid #1E2535 !important; border-radius: 8px !important; color: #E8EAF0 !important; }
div[data-baseweb="select"] svg { color: #4A5568 !important; }
[data-testid="stDateInput"] input { background: #131925 !important; border: 1px solid #1E2535 !important; border-radius: 8px !important; color: #E8EAF0 !important; }

/* ── Page Header ── */
.page-header {
    margin-bottom: 2rem;
}
.page-header h1 {
    font-size: 2.2rem;
    font-weight: 800;
    color: #fff;
    margin: 0 0 0.3rem 0;
}
.page-header p {
    color: #5A6478;
    font-size: 0.9rem;
    font-weight: 300;
    margin: 0;
}

/* ── KPI Cards ── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin-bottom: 2rem;
}
.kpi-card {
    background: #0D1117;
    border: 1px solid #1E2535;
    border-radius: 14px;
    padding: 1.3rem 1.5rem;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s ease, transform 0.2s ease;
}
.kpi-card:hover { border-color: #2D3D6A; transform: translateY(-2px); }
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--accent, #5B7FFF);
    opacity: 0.8;
}
.kpi-card .kpi-icon {
    font-size: 1rem;
    margin-bottom: 0.8rem;
    opacity: 0.7;
}
.kpi-card .kpi-value {
    font-family: 'Syne', sans-serif;
    font-size: 1.75rem;
    font-weight: 700;
    color: #fff;
    line-height: 1;
    margin-bottom: 0.3rem;
}
.kpi-card .kpi-label {
    font-size: 0.75rem;
    color: #5A6478;
    font-weight: 400;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}
.kpi-card .kpi-sub {
    font-size: 0.75rem;
    color: #4A9D7E;
    margin-top: 0.5rem;
    font-weight: 500;
}

/* ── Section Header ── */
.section-header {
    font-family: 'Syne', sans-serif;
    font-size: 1rem;
    font-weight: 700;
    color: #fff;
    margin-bottom: 0.2rem;
}
.section-sub {
    font-size: 0.78rem;
    color: #5A6478;
    margin-bottom: 1rem;
    font-weight: 300;
}

/* ── Chart Container ── */
.chart-card {
    background: #0D1117;
    border: 1px solid #1E2535;
    border-radius: 14px;
    padding: 1.5rem;
    margin-bottom: 1rem;
}

/* ── Data Table ── */
[data-testid="stDataFrame"] {
    border: 1px solid #1E2535 !important;
    border-radius: 10px !important;
    overflow: hidden !important;
}
[data-testid="stDataFrame"] th {
    background: #131925 !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 0.7rem !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: #4A5568 !important;
}
[data-testid="stDataFrame"] td { font-size: 0.82rem !important; color: #C5C9D6 !important; }
[data-testid="stDataFrame"] tr:hover td { background: #131925 !important; }

/* ── Tabs ── */
[data-testid="stTabs"] button {
    font-family: 'Syne', sans-serif !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.04em !important;
    color: #4A5568 !important;
    border-radius: 0 !important;
    padding: 0.6rem 1.2rem !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #fff !important;
    border-bottom: 2px solid #5B7FFF !important;
    background: transparent !important;
}
[data-testid="stTabs"] > div:first-child { border-bottom: 1px solid #1E2535 !important; }

/* ── Divider ── */
hr { border-color: #1E2535 !important; margin: 1.5rem 0 !important; }

/* ── Spinner ── */
[data-testid="stSpinner"] { color: #5B7FFF !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #080B12; }
::-webkit-scrollbar-thumb { background: #1E2535; border-radius: 2px; }

/* ── Platform Badge ── */
.badge {
    display: inline-block;
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 2px 8px;
    border-radius: 4px;
    margin-right: 4px;
}
.badge-tiktok { background: #1A1A2E; color: #9B8FFF; border: 1px solid #2A2245; }
.badge-instagram { background: #1A1228; color: #FF7AB2; border: 1px solid #2D1840; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# PLOTLY THEME
# ─────────────────────────────────────────────
PLOT_BG   = "#0D1117"
PAPER_BG  = "#0D1117"
GRID_COL  = "#1A2035"
TEXT_COL  = "#8892A4"
FONT_FAM  = "DM Sans"

PALETTE = {
    "tiktok":    "#9B8FFF",
    "instagram": "#FF7AB2",
    "accent1":   "#5B7FFF",
    "accent2":   "#4A9D7E",
    "accent3":   "#F0A05A",
}

def apply_dark_theme(fig, height=340):
    fig.update_layout(
        height=height,
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family=FONT_FAM, color=TEXT_COL, size=11),
        margin=dict(l=8, r=8, t=24, b=8),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=GRID_COL,
            font=dict(size=11, color=TEXT_COL),
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        xaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=10)),
        yaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=10)),
    )
    return fig

# ─────────────────────────────────────────────
# CONNECTION & QUERY
# ─────────────────────────────────────────────
@st.cache_resource
def init_connection():
    private_key_path = os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"))
    with open(private_key_path, "rb") as key:
        p_key = serialization.load_pem_private_key(key.read(), password=None, backend=default_backend())
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="GOLD_DEV",
    )

@st.cache_data(ttl=600)
def run_query(query):
    conn = init_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetch_pandas_all()
    if "DATE_DAY" in df.columns:
        df["DATE_DAY"] = pd.to_datetime(df["DATE_DAY"])
    return df

# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
with st.spinner("Connecting to data warehouse…"):
    df_posts = run_query("""
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
        JOIN dim_date  d ON p.created_date_key = d.date_day
        JOIN dim_users u ON p.author_user_key   = u.user_key
    """)

if df_posts.empty:
    st.warning("No data found in GOLD_DEV layer.")
    st.stop()

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div class="sidebar-logo">
            <span>◈</span> Pulse
        </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section-label">Time Period</div>', unsafe_allow_html=True)
    min_date = df_posts["DATE_DAY"].min().date()
    max_date = df_posts["DATE_DAY"].max().date()

    date_preset = st.selectbox(
        "Quick Range",
        ["All Time", "Year to Date (YTD)", "Last 30 Days", "Custom Range"],
        label_visibility="collapsed",
    )
    if date_preset == "All Time":
        start_date, end_date = min_date, max_date
    elif date_preset == "Year to Date (YTD)":
        start_date, end_date = datetime(max_date.year, 1, 1).date(), max_date
    elif date_preset == "Last 30 Days":
        start_date, end_date = max_date - timedelta(days=30), max_date
    else:
        dr = st.date_input("Range", [min_date, max_date], min_value=min_date, max_value=max_date)
        start_date, end_date = (dr[0], dr[1]) if len(dr) == 2 else (dr[0], dr[0])

    st.markdown('<div class="sidebar-section-label">Platform</div>', unsafe_allow_html=True)
    platforms_available = sorted(df_posts["PLATFORM"].unique().tolist())
    selected_platforms = st.multiselect(
        "Platform", platforms_available, default=platforms_available, label_visibility="collapsed"
    )

    st.markdown('<div class="sidebar-section-label">Accounts</div>', unsafe_allow_html=True)
    accounts_available = sorted(df_posts["USERNAME"].unique().tolist())
    selected_accounts = st.multiselect(
        "Accounts", accounts_available, default=accounts_available, label_visibility="collapsed"
    )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:0.7rem;color:#2D3748;border-top:1px solid #1E2535;padding-top:1rem;">'
        f'Last refreshed · {datetime.now().strftime("%d %b %Y, %H:%M")}</div>',
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
# FILTER
# ─────────────────────────────────────────────
mask = (
    (df_posts["DATE_DAY"].dt.date >= start_date)
    & (df_posts["DATE_DAY"].dt.date <= end_date)
    & (df_posts["PLATFORM"].isin(selected_platforms))
    & (df_posts["USERNAME"].isin(selected_accounts))
)
df = df_posts[mask].copy()

if df.empty:
    st.info("No data matches the selected filters.")
    st.stop()

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown(f"""
<div class="page-header">
    <h1>Content Performance</h1>
    <p>{start_date.strftime('%d %b %Y')} — {end_date.strftime('%d %b %Y')} &nbsp;·&nbsp; {', '.join(selected_platforms)}</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# KPI CARDS
# Note: view_count and share_count are NULL for Instagram posts.
# We sum only non-null values so Instagram likes/comments still count.
# ─────────────────────────────────────────────
total_posts    = len(df)
total_likes    = int(df["LIKE_COUNT"].sum())
total_comments = int(df["COMMENT_COUNT"].sum())
total_views    = int(df["VIEW_COUNT"].sum())    # TikTok only
total_shares   = int(df["SHARE_COUNT"].sum())   # TikTok only

# Engagement Rate per platform awareness:
# For TikTok  → (likes + comments + shares) / views * 100
# For IG      → no view_count, so we skip ER for blended calc; show interactions instead
total_interactions = total_likes + total_comments + total_shares
er_tiktok_rows = df[df["PLATFORM"] == "tiktok"]
er = (
    (er_tiktok_rows["LIKE_COUNT"].sum() + er_tiktok_rows["COMMENT_COUNT"].sum() + er_tiktok_rows["SHARE_COUNT"].sum())
    / er_tiktok_rows["VIEW_COUNT"].sum() * 100
    if er_tiktok_rows["VIEW_COUNT"].sum() > 0 else 0
)

def fmt(n):
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{n:,}"

st.markdown(f"""
<div class="kpi-grid">
    <div class="kpi-card" style="--accent:#5B7FFF">
        <div class="kpi-icon">◈</div>
        <div class="kpi-value">{fmt(total_posts)}</div>
        <div class="kpi-label">Total Posts</div>
        <div class="kpi-sub">all platforms</div>
    </div>
    <div class="kpi-card" style="--accent:#FF6B8A">
        <div class="kpi-icon">♥</div>
        <div class="kpi-value">{fmt(total_likes)}</div>
        <div class="kpi-label">Total Likes</div>
        <div class="kpi-sub">TikTok + Instagram</div>
    </div>
    <div class="kpi-card" style="--accent:#4A9D7E">
        <div class="kpi-icon">◎</div>
        <div class="kpi-value">{fmt(total_views)}</div>
        <div class="kpi-label">Total Views</div>
        <div class="kpi-sub">TikTok only</div>
    </div>
    <div class="kpi-card" style="--accent:#F0A05A">
        <div class="kpi-icon">⟡</div>
        <div class="kpi-value">{er:.2f}%</div>
        <div class="kpi-label">Engagement Rate</div>
        <div class="kpi-sub">TikTok (likes+comments+shares / views)</div>
    </div>
</div>
""", unsafe_allow_html=True)

# Secondary row — comments + shares
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("💬 Total Comments", fmt(total_comments), help="TikTok + Instagram")
with c2:
    st.metric("↗ Total Shares", fmt(total_shares), help="TikTok only — Instagram does not expose share count")
with c3:
    st.metric("👤 Unique Accounts", df["USERNAME"].nunique())

st.markdown("<hr>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["  Overview  ", "  Creators  ", "  Content  "])

# ── TAB 1: OVERVIEW ──────────────────────────
with tab1:
    col_left, col_right = st.columns([3, 2], gap="medium")

    with col_left:
        st.markdown('<div class="section-header">Engagement Over Time</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Daily aggregated likes and comments across all platforms</div>', unsafe_allow_html=True)

        trend = (
            df.groupby(["DATE_DAY", "PLATFORM"])
            .agg(LIKE_COUNT=("LIKE_COUNT", "sum"), COMMENT_COUNT=("COMMENT_COUNT", "sum"))
            .reset_index()
        )
        fig_trend = go.Figure()
        for platform in trend["PLATFORM"].unique():
            sub = trend[trend["PLATFORM"] == platform]
            color = PALETTE.get(platform, PALETTE["accent1"])
            fig_trend.add_trace(go.Scatter(
                x=sub["DATE_DAY"], y=sub["LIKE_COUNT"],
                name=f"{platform} Likes",
                line=dict(color=color, width=2),
                fill="tozeroy",
                fillcolor = hex_to_rgba(color, 0.06)
            ))
            fig_trend.add_trace(go.Scatter(
                x=sub["DATE_DAY"], y=sub["COMMENT_COUNT"],
                name=f"{platform} Comments",
                line=dict(color=color, width=1.5, dash="dot"),
            ))
        apply_dark_theme(fig_trend, height=300)
        st.plotly_chart(fig_trend, use_container_width=True)

    with col_right:
        st.markdown('<div class="section-header">Platform Mix</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Share of total interactions by platform</div>', unsafe_allow_html=True)

        platform_df = (
            df.groupby("PLATFORM")
            .agg(interactions=pd.NamedAgg(column="LIKE_COUNT", aggfunc="sum"))
            .reset_index()
        )
        # Add comments too
        platform_comments = df.groupby("PLATFORM")["COMMENT_COUNT"].sum().reset_index()
        platform_df = platform_df.merge(platform_comments, on="PLATFORM")
        platform_df["total"] = platform_df["interactions"] + platform_df["COMMENT_COUNT"]

        colors = [PALETTE.get(p, PALETTE["accent1"]) for p in platform_df["PLATFORM"]]
        fig_donut = go.Figure(go.Pie(
            labels=platform_df["PLATFORM"],
            values=platform_df["total"],
            hole=0.65,
            marker=dict(colors=colors, line=dict(color=PLOT_BG, width=3)),
            textfont=dict(size=11, family=FONT_FAM),
            hovertemplate="<b>%{label}</b><br>Interactions: %{value:,.0f}<extra></extra>",
        ))
        fig_donut.add_annotation(
            text=f"<b>{fmt(int(platform_df['total'].sum()))}</b><br><span style='font-size:10px'>total</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=18, color="#fff", family="Syne"),
            align="center",
        )
        apply_dark_theme(fig_donut, height=300)
        fig_donut.update_layout(showlegend=True, legend=dict(orientation="v", x=1, y=0.5))
        st.plotly_chart(fig_donut, use_container_width=True)

    # TikTok-specific row
    tiktok_df = df[df["PLATFORM"] == "tiktok"]
    if not tiktok_df.empty:
        st.markdown('<div class="section-header" style="margin-top:1rem">TikTok — Views Trend</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Daily view count (TikTok only — Instagram does not expose view_count)</div>', unsafe_allow_html=True)
        views_trend = tiktok_df.groupby("DATE_DAY")["VIEW_COUNT"].sum().reset_index()
        fig_views = go.Figure(go.Bar(
            x=views_trend["DATE_DAY"],
            y=views_trend["VIEW_COUNT"],
            marker=dict(
                color=views_trend["VIEW_COUNT"],
                colorscale=[[0, "#1A2340"], [0.5, "#2D4A8A"], [1, PALETTE["accent1"]]],
                line=dict(width=0),
            ),
            hovertemplate="%{x|%d %b}<br><b>%{y:,.0f}</b> views<extra></extra>",
        ))
        apply_dark_theme(fig_views, height=220)
        st.plotly_chart(fig_views, use_container_width=True)

# ── TAB 2: CREATORS ──────────────────────────
with tab2:
    col_a, col_b = st.columns([3, 2], gap="medium")

    with col_a:
        st.markdown('<div class="section-header">Top Accounts by Likes</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Sum of likes per account across all platforms</div>', unsafe_allow_html=True)

        acc_df = (
            df.groupby(["USERNAME", "PLATFORM"])
            .agg(
                total_likes=("LIKE_COUNT", "sum"),
                total_comments=("COMMENT_COUNT", "sum"),
                total_posts=("POST_KEY", "count"),
            )
            .reset_index()
            .sort_values("total_likes", ascending=True)
            .tail(15)
        )
        colors = [PALETTE.get(p, PALETTE["accent1"]) for p in acc_df["PLATFORM"]]
        fig_bar = go.Figure(go.Bar(
            y=acc_df["USERNAME"],
            x=acc_df["total_likes"],
            orientation="h",
            marker=dict(color=colors, line=dict(width=0)),
            customdata=acc_df[["total_comments", "total_posts", "PLATFORM"]].values,
            hovertemplate=(
                "<b>@%{y}</b><br>"
                "Likes: <b>%{x:,.0f}</b><br>"
                "Comments: %{customdata[0]:,.0f}<br>"
                "Posts: %{customdata[1]}<br>"
                "Platform: %{customdata[2]}<extra></extra>"
            ),
        ))
        apply_dark_theme(fig_bar, height=380)
        fig_bar.update_layout(yaxis=dict(tickfont=dict(size=11)))
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_b:
        st.markdown('<div class="section-header">Likes vs Comments</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Engagement balance per account</div>', unsafe_allow_html=True)

        bubble_df = (
            df.groupby(["USERNAME", "PLATFORM"])
            .agg(total_likes=("LIKE_COUNT", "sum"), total_comments=("COMMENT_COUNT", "sum"), total_posts=("POST_KEY", "count"))
            .reset_index()
        )
        colors = [PALETTE.get(p, PALETTE["accent1"]) for p in bubble_df["PLATFORM"]]
        fig_bub = go.Figure(go.Scatter(
            x=bubble_df["total_likes"],
            y=bubble_df["total_comments"],
            mode="markers+text",
            marker=dict(
                size=bubble_df["total_posts"] * 3 + 8,
                color=colors,
                line=dict(color=PLOT_BG, width=2),
                opacity=0.85,
            ),
            text=bubble_df["USERNAME"],
            textposition="top center",
            textfont=dict(size=9, color=TEXT_COL),
            hovertemplate="<b>@%{text}</b><br>Likes: %{x:,.0f}<br>Comments: %{y:,.0f}<extra></extra>",
        ))
        apply_dark_theme(fig_bub, height=380)
        fig_bub.update_layout(xaxis_title="Total Likes", yaxis_title="Total Comments")
        st.plotly_chart(fig_bub, use_container_width=True)

# ── TAB 3: CONTENT ───────────────────────────
with tab3:
    col_p, col_q = st.columns([2, 3], gap="medium")

    with col_p:
        st.markdown('<div class="section-header">Posts per Platform</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Volume distribution over time</div>', unsafe_allow_html=True)

        volume_df = df.groupby(["DATE_DAY", "PLATFORM"])["POST_KEY"].count().reset_index()
        volume_df.columns = ["DATE_DAY", "PLATFORM", "count"]
        fig_vol = px.area(
            volume_df, x="DATE_DAY", y="count", color="PLATFORM",
            color_discrete_map=PALETTE,
        )
        fig_vol.update_traces(line=dict(width=1.5))
        apply_dark_theme(fig_vol, height=240)
        st.plotly_chart(fig_vol, use_container_width=True)

    with col_q:
        st.markdown('<div class="section-header">Top 10 Posts by Likes</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Ranked by total likes · hover for full description</div>', unsafe_allow_html=True)

        top_posts = df.sort_values("LIKE_COUNT", ascending=False).head(10).copy()
        top_posts["SHORT_DESC"] = top_posts["POST_DESCRIPTION"].apply(
            lambda x: str(x)[:60] + "…" if len(str(x)) > 60 else str(x)
        )
        top_posts["PLATFORM_BADGE"] = top_posts["PLATFORM"]

        display_df = top_posts[["PLATFORM_BADGE", "USERNAME", "SHORT_DESC", "LIKE_COUNT", "COMMENT_COUNT"]].copy()
        display_df.columns = ["Platform", "Account", "Description", "Likes", "Comments"]

        st.dataframe(
            display_df.style.format({"Likes": "{:,.0f}", "Comments": "{:,.0f}"}),
            use_container_width=True,
            hide_index=True,
            height=260,
        )

    # Scatter — only for TikTok where view_count exists
    tiktok_scatter = df[df["PLATFORM"] == "tiktok"].copy()
    if not tiktok_scatter.empty:
        st.markdown('<div class="section-header" style="margin-top:1rem">TikTok — Views × Likes Correlation</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Detect viral outliers · bubble size = comment count · Instagram excluded (no view_count)</div>', unsafe_allow_html=True)

        fig_sc = px.scatter(
            tiktok_scatter,
            x="VIEW_COUNT", y="LIKE_COUNT",
            size=tiktok_scatter["COMMENT_COUNT"].clip(lower=1),
            color="USERNAME",
            hover_data={"POST_DESCRIPTION": True, "VIEW_COUNT": True, "LIKE_COUNT": True},
            labels={"VIEW_COUNT": "Views", "LIKE_COUNT": "Likes"},
            opacity=0.8,
        )
        apply_dark_theme(fig_sc, height=360)
        fig_sc.update_traces(marker=dict(line=dict(color=PLOT_BG, width=1)))
        st.plotly_chart(fig_sc, use_container_width=True)

    ig_scatter = df[df["PLATFORM"] == "instagram"].copy()
    if not ig_scatter.empty:
        st.markdown('<div class="section-header" style="margin-top:1rem">Instagram — Likes × Comments Correlation</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Detect high-engagement posts · view_count not available for Instagram</div>', unsafe_allow_html=True)

        fig_ig = px.scatter(
            ig_scatter,
            x="LIKE_COUNT", y="COMMENT_COUNT",
            color="USERNAME",
            hover_data={"POST_DESCRIPTION": True},
            labels={"LIKE_COUNT": "Likes", "COMMENT_COUNT": "Comments"},
            opacity=0.8,
        )
        apply_dark_theme(fig_ig, height=320)
        st.plotly_chart(fig_ig, use_container_width=True)