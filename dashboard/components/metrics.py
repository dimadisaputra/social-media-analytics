import streamlit as st
from dashboard.utils import fmt

def render_kpi_cards(df):
    total_views    = df["VIEW_COUNT"].sum(min_count=1)    or 0
    total_likes    = df["LIKE_COUNT"].sum(min_count=1)    or 0
    total_comments = df["COMMENT_COUNT"].sum(min_count=1) or 0
    total_posts    = len(df)

    tt = df[df["PLATFORM"] == "tiktok"]
    ig = df[df["PLATFORM"] == "instagram"]
    tt_views = float(tt["VIEW_COUNT"].sum(min_count=1)    or 0)
    tt_eng   = float((tt["LIKE_COUNT"].sum(min_count=1)   or 0) +
                     (tt["COMMENT_COUNT"].sum(min_count=1) or 0) +
                     (tt["SHARE_COUNT"].sum(min_count=1)   or 0))
    ig_fol   = float(ig["AUTHOR_FOLLOWERS"].sum(min_count=1) or 0)
    ig_eng   = float((ig["LIKE_COUNT"].sum(min_count=1)   or 0) +
                     (ig["COMMENT_COUNT"].sum(min_count=1) or 0))
    er_parts = []
    if tt_views > 0: er_parts.append(tt_eng / tt_views * 100)
    if ig_fol   > 0: er_parts.append(ig_eng / ig_fol   * 100)
    blended_er = sum(er_parts) / len(er_parts) if er_parts else 0.0

    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-icon ic-blue">👁</div>
        <div class="kpi-body">
          <div class="kpi-label">Total Views</div>
          <div class="kpi-value">{fmt(total_views)}</div>
          <div class="kpi-sub">TikTok only</div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon ic-rose">❤️</div>
        <div class="kpi-body">
          <div class="kpi-label">Total Likes</div>
          <div class="kpi-value">{fmt(total_likes)}</div>
          <div class="kpi-sub">All platforms</div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon ic-amber">💬</div>
        <div class="kpi-body">
          <div class="kpi-label">Total Comments</div>
          <div class="kpi-value">{fmt(total_comments)}</div>
          <div class="kpi-sub">All platforms</div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon ic-emerald">⚡</div>
        <div class="kpi-body">
          <div class="kpi-label">Engagement Rate</div>
          <div class="kpi-value">{blended_er:.2f}%</div>
          <div class="kpi-sub">{total_posts} posts · blended</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)
