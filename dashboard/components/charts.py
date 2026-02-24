import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from dashboard.config import TIKTOK, IG, BLUE, RED, GREEN, AMBER, LAYOUT, L, COLOR_MAP
from dashboard.utils import fmt, hex_to_rgba

def render_trend_chart(df, metric_focus):
    metric_cfg = {
        "Likes":           ("LIKE_COUNT",    "Likes",      RED),
        "Views (TikTok)":  ("VIEW_COUNT",    "Views",      BLUE),
        "Engagement Rate": ("ENG_RATE",      "Eng. Rate",  GREEN),
        "Comments":        ("COMMENT_COUNT", "Comments",   AMBER),
    }
    m_col, m_lbl, m_clr = metric_cfg[metric_focus]

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown(f'<div class="card-title">📈 {metric_focus} Over Time</div>', unsafe_allow_html=True)

    trend = (
        df.groupby(["DATE_DAY", "PLATFORM"])
        .apply(lambda g: pd.Series({
            "METRIC":     g[m_col].sum(),
            "POST_COUNT": len(g),
            "TOP_POST": (
                g.loc[g[m_col].idxmax(), "POST_DESCRIPTION"][:60] + "…"
                if g[m_col].notna().any() else "—"
            ),
        }), include_groups=False)
        .reset_index()
    )

    fig_trend = go.Figure()
    for plat, clr in [("tiktok", TIKTOK), ("instagram", IG)]:
        sub = trend[trend["PLATFORM"] == plat]
        if sub.empty:
            continue
        fig_trend.add_trace(go.Scatter(
            x=sub["DATE_DAY"], y=sub["METRIC"],
            name=plat.capitalize(),
            mode="lines",
            line=dict(color=clr, width=2.5, shape="spline"),
            fill="tozeroy",
            fillcolor=hex_to_rgba(clr, 0.07),
            customdata=sub[["POST_COUNT", "TOP_POST"]].values,
            hovertemplate=(
                "<b>%{x|%d %b %Y}</b>  ·  " + plat.capitalize() + "<br>"
                f"{m_lbl}: <b>%{{y:,.0f}}</b>  ·  Posts: <b>%{{customdata[0]}}</b><br>"
                "Top post: %{customdata[1]}<extra></extra>"
            ),
        ))

    fig_trend.update_layout(**LAYOUT, height=285)
    st.plotly_chart(fig_trend, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

def render_donut_chart(df, metric_focus):
    metric_cfg = {
        "Likes":           ("LIKE_COUNT",    "Likes"),
        "Views (TikTok)":  ("VIEW_COUNT",    "Views"),
        "Engagement Rate": ("ENG_RATE",      "Avg Eng. Rate (%)"),
        "Comments":        ("COMMENT_COUNT", "Comments"),
    }
    m_col, m_lbl = metric_cfg[metric_focus]

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown(f'<div class="card-title">🌐 Platform Split · {m_lbl}</div>', unsafe_allow_html=True)

    if m_col == "ENG_RATE":
        plat_df = df.groupby("PLATFORM")[m_col].mean().reset_index()
        total_val = plat_df[m_col].mean()
        val_fmt = f"{total_val:.2f}%"
    else:
        plat_df = df.groupby("PLATFORM")[m_col].sum().reset_index()
        total_val = plat_df[m_col].sum()
        val_fmt = fmt(total_val)
    
    fig_donut = go.Figure(go.Pie(
        labels=plat_df["PLATFORM"].str.capitalize(),
        values=plat_df[m_col],
        hole=0.60,
        marker=dict(colors=[TIKTOK if p.lower() == "tiktok" else IG for p in plat_df["PLATFORM"]], line=dict(color="#FFFFFF", width=3)),
        textinfo="percent+label",
        textfont=dict(size=14, color="#FFFFFF", family="Inter", weight="bold"),
        hovertemplate="<b>%{label}</b><br>" + m_lbl + ": %{value:,.0f}<br>%{percent}<extra></extra>" if m_col != "ENG_RATE" else "<b>%{label}</b><br>" + m_lbl + ": %{value:.2f}%<br>%{percent}<extra></extra>",
    ))
    fig_donut.update_layout(
        **{**LAYOUT, "margin": dict(l=0, r=0, t=20, b=20)},
        height=285, showlegend=False,
        annotations=[dict(
            text=f"<b>{val_fmt}</b><br><span style='font-size:11px;color:#9CA3AF'>{m_lbl}</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color="#111827", size=17, family="Inter"),
        )],
    )
    st.plotly_chart(fig_donut, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

def render_scatter_chart(df):
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">🌌 Likes vs Engagement Rate</div>', unsafe_allow_html=True)
    st.caption("Each dot = one post · size = comment count · hover for post preview")
    
    sc_df = df.copy()
    sc_df["DESC_SHORT"] = sc_df["POST_DESCRIPTION"].apply(
        lambda x: (str(x)[:72]+"…") if pd.notna(x) and len(str(x))>72 else str(x or "—")
    )
    sc_df["CMT_SAFE"] = sc_df["COMMENT_COUNT"].fillna(1).clip(lower=1)

    fig_sc = px.scatter(
        sc_df, x="LIKE_COUNT", y="ENG_RATE",
        color="PLATFORM", size="CMT_SAFE",
        hover_name="USERNAME",
        custom_data=["DESC_SHORT", "CMT_SAFE"],
        color_discrete_map=COLOR_MAP,
        labels={"LIKE_COUNT":"Likes","ENG_RATE":"Eng. Rate (%)","PLATFORM":"Platform"},
        opacity=0.70, size_max=28,
    )
    fig_sc.update_traces(hovertemplate=(
        "<b>@%{hovertext}</b><br>"
        "❤ Likes: <b>%{x:,.0f}</b>  ·  ⚡ Eng: <b>%{y:.2f}%</b><br>"
        "💬 Comments: <b>%{customdata[1]:,.0f}</b><br>"
        "<i style='color:#9CA3AF'>%{customdata[0]}</i><extra></extra>"
    ))
    fig_sc.update_layout(**LAYOUT, height=420)

    st.plotly_chart(fig_sc, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
