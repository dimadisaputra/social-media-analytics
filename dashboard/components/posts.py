import streamlit as st
import pandas as pd
from dashboard.utils import fmt, fetch_oembed_thumbnail


def render_top_posts(df, df_sent_sum, posts_sort):
    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">🎞 Top 10 Posts</div>', unsafe_allow_html=True)

    top_posts = (
        df.sort_values(by=posts_sort, ascending=False)
        .dropna(subset=[posts_sort])
        .head(10)
    )

    if not df_sent_sum.empty:
        dom = (
            df_sent_sum.groupby("POST_KEY")["DOMINANT_SENTIMENT"]
            .agg(lambda x: x.mode().iloc[0] if len(x) else "unscored")
            .reset_index()
        )
        top_posts = top_posts.merge(dom, on="POST_KEY", how="left")
    else:
        top_posts["DOMINANT_SENTIMENT"] = "unscored"

    all_html = '<div style="display:flex; flex-direction:column; gap:12px;">'

    for i, (_, post) in enumerate(top_posts.iterrows()):
        rank    = i + 1
        platform        = str(post.get("PLATFORM", "")).lower()
        post_url        = str(post.get("POST_URL") or "")
        desc            = str(post.get("POST_DESCRIPTION") or "—")
        likes           = int(post.get("LIKE_COUNT")    or 0)
        comments        = int(post.get("COMMENT_COUNT") or 0)
        views_r         = post.get("VIEW_COUNT")
        dom_s           = str(post.get("DOMINANT_SENTIMENT") or "unscored").lower()
        cover_image_url = str(post.get("COVER_IMAGE_URL") or "")

        badge_cls = "badge-tiktok"   if platform == "tiktok" else "badge-instagram"
        badge_lbl = "TikTok"         if platform == "tiktok" else "Instagram"
        icon      = "🎵"             if platform == "tiktok" else "📸"

        sent_badge = (
            f'<span class="sb sb-{dom_s}">{dom_s.capitalize()}</span>'
            if dom_s in ("positive", "neutral", "negative") else ""
        )
        views_html = (
            f'<span class="ps">👁 <strong>{fmt(views_r)}</strong></span>'
            if pd.notna(views_r) and views_r else ""
        )

        if platform == "instagram":
            thumb_url = cover_image_url if cover_image_url and cover_image_url != "nan" else None
        else:
            thumb_url = fetch_oembed_thumbnail(post_url, platform)

        thumb_html = (
            f'<div class="post-thumb-wrap"><img src="{thumb_url}" class="post-thumb" loading="lazy"/></div>'
            if thumb_url
            else f'<div class="post-thumb-ph">{icon}</div>'
        )

        link_open  = f'<a href="{post_url}" target="_blank" class="post-list-item">' if post_url else '<div class="post-list-item">'
        link_close = "</a>" if post_url else "</div>"

        desc = desc[:200].replace("\n", " ").replace("<", "&lt;").replace(">", "&gt;")

        # CRITICAL: Streamlit's markdown parser converts newlines inside HTML into `<p>` tags
        # or separate markdown blocks if left alone, which brutally breaks flexbox rendering.
        # We must construct a completely minified, single-line HTML string to bypass
        # the automatic markdown parsing injections.
        html_item = (
            f'{link_open}'
            f'<div class="post-rank">#{rank}</div>'
            f'{thumb_html}'
            f'<div class="post-meta-content">'
            f'<div style="margin-bottom: 6px;">'
            f'<span class="badge {badge_cls}">{badge_lbl}</span>{sent_badge}'
            f'</div>'
            f'<div class="post-desc">{desc}</div>'
            f'<div class="post-stats">'
            f'<span class="ps">❤ <strong>{fmt(likes)}</strong></span> '
            f'<span class="ps">💬 <strong>{fmt(comments)}</strong></span> '
            f'{views_html}'
            f'</div>'
            f'</div>'
            f'{link_close}'
        )
        
        all_html += html_item

    all_html += "</div>"
    st.markdown(all_html, unsafe_allow_html=True)