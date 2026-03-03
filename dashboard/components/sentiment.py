import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.config import POS_C, NEU_C, NEG_C, GRID, LAYOUT, L
from dashboard.utils import fmt, hex_to_rgba, fetch_oembed_thumbnail

def render_sentiment_overview(df, df_comments, df_sent_sum):
    if df_sent_sum.empty:
        return

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">💬 Comment Sentiment</div>', unsafe_allow_html=True)

    total_scored = int(df_sent_sum["SCORED_COMMENTS"].sum() or 0)
    total_pos    = int(df_sent_sum["POSITIVE_COUNT"].sum()  or 0)
    total_neu    = int(df_sent_sum["NEUTRAL_COUNT"].sum()   or 0)
    total_neg    = int(df_sent_sum["NEGATIVE_COUNT"].sum()  or 0)

    pos_pct = total_pos / total_scored * 100 if total_scored else 0
    neu_pct = total_neu / total_scored * 100 if total_scored else 0
    neg_pct = total_neg / total_scored * 100 if total_scored else 0

    st.markdown(f"""
    <div class="sent-row">
      <div class="sent-pill sp-pos"><div class="sp-dot"></div>Positive — {fmt(total_pos)} ({pos_pct:.1f}%)</div>
      <div class="sent-pill sp-neu"><div class="sp-dot"></div>Neutral — {fmt(total_neu)} ({neu_pct:.1f}%)</div>
      <div class="sent-pill sp-neg"><div class="sp-dot"></div>Negative — {fmt(total_neg)} ({neg_pct:.1f}%)</div>
    </div>
    """, unsafe_allow_html=True)

    sc1, sc2, sc3 = st.columns(3)

    # ── Sentiment distribution bar ──
    with sc1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">Distribution</div>', unsafe_allow_html=True)
        fig_dist = go.Figure(go.Bar(
            x=["Positive", "Neutral", "Negative"],
            y=[total_pos, total_neu, total_neg],
            marker_color=[POS_C, NEU_C, NEG_C],
            marker_cornerradius=6, # rounded modern look
            text=[f"{fmt(v)} ({p:.0f}%)" for v, p in
                  [(total_pos,pos_pct),(total_neu,neu_pct),(total_neg,neg_pct)]],
            textposition="outside",
            hovertemplate="%{x}: <b>%{y:,.0f}</b><extra></extra>",
        ))
        fig_dist.update_layout(**L(
            height=260,
            yaxis=dict(gridcolor=GRID, zeroline=False, showline=False, showticklabels=False),
        ))
        st.plotly_chart(fig_dist, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Sentiment over time ──
    with sc2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">Trend Over Time</div>', unsafe_allow_html=True)
        sent_time = (
            df_sent_sum.groupby("COMMENT_DATE")
            .agg(POS=("POSITIVE_COUNT","sum"),
                 NEU=("NEUTRAL_COUNT","sum"),
                 NEG=("NEGATIVE_COUNT","sum"))
            .reset_index()
        )
        fig_st = go.Figure()
        for col, clr, lbl in [("POS",POS_C,"Positive"),("NEU",NEU_C,"Neutral"),("NEG",NEG_C,"Negative")]:
            fig_st.add_trace(go.Scatter(
                x=sent_time["COMMENT_DATE"], y=sent_time[col],
                name=lbl, mode="lines",
                line=dict(color=clr, width=2, shape="spline"),
                fill="tozeroy",
                fillcolor=hex_to_rgba(clr, 0.06),
                hovertemplate=f"{lbl}: <b>%{{y:,.0f}}</b><extra></extra>",
            ))
        fig_st.update_layout(**LAYOUT, height=260)
        st.plotly_chart(fig_st, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Avg sentiment score by platform ──
    with sc3:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">Avg Score by Platform</div>', unsafe_allow_html=True)
        if not df_comments.empty:
            plat_sent = (
                df_comments.groupby("PLATFORM")["SENTIMENT_SCORE"]
                .mean().reset_index()
            )
            bar_colors = [
                POS_C if float(v) > 0.2 else (NEG_C if float(v) < -0.2 else NEU_C)
                for v in plat_sent["SENTIMENT_SCORE"]
            ]
            fig_ps = go.Figure(go.Bar(
                x=plat_sent["PLATFORM"].str.capitalize(),
                y=plat_sent["SENTIMENT_SCORE"].round(3),
                marker_color=bar_colors,
                marker_cornerradius=6, # rounded modern look
                text=plat_sent["SENTIMENT_SCORE"].round(3),
                textposition="outside",
                hovertemplate="%{x}: <b>%{y:.3f}</b><extra></extra>",
            ))
            fig_ps.update_layout(**L(
                height=260,
                yaxis=dict(range=[-1,1], gridcolor=GRID, zeroline=True,
                           zerolinecolor="#E5E7EB", showline=False),
            ))
            st.plotly_chart(fig_ps, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Top Posts by Sentiment ──
    st.markdown('<div class="section-title" style="margin-top:16px;">🏆 Top Posts by Sentiment</div>', unsafe_allow_html=True)
    
    # AGGREGATE by post_key first (since df_sent_sum is at grain post_key + date)
    df_post_total = (
        df_sent_sum.groupby("POST_KEY")
        .agg(POS=("POSITIVE_COUNT","sum"),
             NEU=("NEUTRAL_COUNT","sum"),
             NEG=("NEGATIVE_COUNT","sum"),
             SCORED=("SCORED_COMMENTS","sum"))
        .reset_index()
    )

    top_pos_key = df_post_total.loc[df_post_total["POS"].idxmax(), "POST_KEY"] if not df_post_total.empty and df_post_total["POS"].max() > 0 else None
    top_neu_key = df_post_total.loc[df_post_total["NEU"].idxmax(), "POST_KEY"] if not df_post_total.empty and df_post_total["NEU"].max() > 0 else None
    top_neg_key = df_post_total.loc[df_post_total["NEG"].idxmax(), "POST_KEY"] if not df_post_total.empty and df_post_total["NEG"].max() > 0 else None

    cols = st.columns(3)
    
    cards = [
        ("Most Positive", top_pos_key, "POS", "sb-positive", "Positive"),
        ("Most Neutral", top_neu_key, "NEU", "sb-neutral", "Neutral"),
        ("Most Negative", top_neg_key, "NEG", "sb-negative", "Negative"),
    ]

    for col, (title, post_key, metric_col, badge_class, label) in zip(cols, cards):
        with col:
            if post_key:
                # Find post details from filtered main df
                matches = df[df["POST_KEY"] == post_key]
                if matches.empty:
                    st.markdown(f'<div class="card" style="height:100%;"><div class="card-title">{title}</div><div style="font-size:13px;color:#9CA3AF;margin-top:20px;">No platform data.</div></div>', unsafe_allow_html=True)
                    continue
                
                post = matches.iloc[0]
                sent_stats = df_post_total[df_post_total["POST_KEY"] == post_key].iloc[0]
                
                metric_val = int(sent_stats[metric_col] or 0)
                total_scored = int(sent_stats["SCORED"] or 1)
                metric_pct = (metric_val / total_scored) * 100 if total_scored > 0 else 0

                platform = str(post.get("PLATFORM", "")).lower()
                post_url = str(post.get("POST_URL") or "")
                desc     = str(post.get("POST_DESCRIPTION") or "—")
                cover_image_url = str(post.get("COVER_IMAGE_URL") or "")

                desc = desc[:150].replace("\n", " ").replace("\\n", " ").replace("<", "&lt;").replace(">", "&gt;")

                if platform == "instagram":
                    thumb_url = cover_image_url if cover_image_url and cover_image_url != "nan" else None
                else:
                    thumb_url = fetch_oembed_thumbnail(post_url, platform)

                icon = "🎵" if platform == "tiktok" else "📸"
                thumb_html = (
                    f'<div style="flex-shrink:0;"><img src="{thumb_url}" style="width:60px; height:60px; object-fit:cover; border-radius:6px;" loading="lazy"/></div>'
                    if thumb_url else f'<div style="width:60px; height:60px; border-radius:6px; background:#F3F4F6; display:flex; align-items:center; justify-content:center; font-size:20px; flex-shrink:0;">{icon}</div>'
                )

                link_open  = f'<a href="{post_url}" target="_blank" style="text-decoration:none; color:inherit; display:flex; gap:12px; margin-top:8px;">' if post_url else '<div style="display:flex; gap:12px; margin-top:8px;">'
                link_close = "</a>" if post_url else "</div>"

                full_card_html = (
                    f'<div class="card" style="height:100%;">'
                    f'<div class="card-title" style="margin-bottom:8px;">{title}</div>'
                    f'<div style="font-size:12px; color:#4B5563; margin-bottom:12px;">'
                    f'<span class="sb {badge_class}" style="margin-left:0; margin-right:6px;">{metric_pct:.1f}% {label} Comments</span>'
                    f'</div>'
                    f'{link_open}'
                    f'{thumb_html}'
                    f'<div style="flex:1; display:flex; flex-direction:column; justify-content:center;">'
                    f'<div style="font-size:12px; color:#4B5563; line-height:1.4; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden;">{desc}</div>'
                    f'</div>'
                    f'{link_close}'
                    f'</div>'
                )
                st.markdown(full_card_html, unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="card" style="height:100%;"><div class="card-title">{title}</div><div style="font-size:13px;color:#9CA3AF;margin-top:20px;">No {label.lower()} posts found.</div></div>', unsafe_allow_html=True)
