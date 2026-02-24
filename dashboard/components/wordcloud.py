import streamlit as st
import pandas as pd
import io
import plotly.graph_objects as go
from dashboard.utils import build_word_freq, render_wordcloud
from dashboard.config import BLUE, GREEN, AMBER, RED, GRID, LAYOUT, L

def render_wordcloud_section(df_comments):
    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">☁ Comment Word Cloud</div>', unsafe_allow_html=True)
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.caption("Cleaned: EN + ID stopwords, mentions, hashtags, URLs, emoji, and punctuation removed.")

    if df_comments.empty:
        st.info("No scored comments available for the selected filters.")
    else:
        wc_filter = st.radio(
            "Sentiment filter",
            ["All", "Positive", "Neutral", "Negative"],
            horizontal=True,
        )

        colormap_cfg = {
            "All":      ("Blues",  "#FFFFFF"),
            "Positive": ("Greens", "#F0FDF4"),
            "Neutral":  ("YlOrBr", "#FFFBEB"),
            "Negative": ("Reds",   "#FEF2F2"),
        }
        cmap, bg_col = colormap_cfg[wc_filter]

        if wc_filter == "All":
            source_texts = tuple(df_comments["COMMENT_TEXT"].dropna().tolist())
        else:
            label = wc_filter.lower()
            source_texts = tuple(
                df_comments[df_comments["SENTIMENT_LABEL"] == label]["COMMENT_TEXT"]
                .dropna().tolist()
            )

        if not source_texts:
            st.info(f"No {wc_filter.lower()} comments to display.")
        else:
            with st.spinner("Building word cloud…"):
                freq      = build_word_freq(source_texts)
                freq_items = tuple(sorted(freq.items(), key=lambda x: -x[1]))
                wc_img    = render_wordcloud(freq_items, cmap, bg_col)

            wc_col, bar_col = st.columns([1.6, 1])

            with wc_col:
                if wc_img:
                    buf = io.BytesIO()
                    wc_img.save(buf, format="PNG")
                    st.image(buf.getvalue(), use_container_width=True)
                else:
                    st.info("Not enough text to generate a word cloud.")

            with bar_col:
                top10 = list(freq_items[:10])
                if top10:
                    wdf = pd.DataFrame(top10, columns=["Word", "Count"])
                    fig_wf = go.Figure(go.Bar(
                        x=wdf["Count"], y=wdf["Word"],
                        orientation="h",
                        marker_color={"All":BLUE,"Positive":GREEN,"Neutral":AMBER,"Negative":RED}[wc_filter],
                        marker_cornerradius=4,
                        hovertemplate="%{y}: <b>%{x:,}</b><extra></extra>",
                    ))
                    fig_wf.update_layout(**L(
                        margin=dict(l=0, r=0, t=28, b=0),
                        height=360,
                        yaxis=dict(autorange="reversed", gridcolor=GRID, zeroline=False, showline=False),
                        xaxis=dict(gridcolor=GRID, zeroline=False, showline=False),
                        title=dict(text="Top 10 Words", font=dict(size=13, color="#111827"), pad=dict(b=16))
                    ))
                    st.plotly_chart(fig_wf, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)
