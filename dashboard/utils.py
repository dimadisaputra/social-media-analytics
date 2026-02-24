import re
import io
import requests
import numpy as np
import pandas as pd
import streamlit as st
from wordcloud import WordCloud
from PIL import Image
import nltk
from nltk.corpus import stopwords
import os

# ─── NLTK stopwords ────────────────────────────────────────────────────────
@st.cache_resource
def load_stopwords():
    nltk.download("stopwords", quiet=True)
    en = set(stopwords.words("english"))
    id_words = {
        "yang","dan","di","ke","dari","ini","itu","dengan","untuk","pada",
        "adalah","dalam","tidak","ada","juga","saya","kamu","kami","mereka",
        "sudah","akan","bisa","kita","atau","tapi","karena","jadi","kalau",
        "sama","ya","lagi","buat","udah","banget","aja","ga","gak","nggak",
        "si","nya","lah","deh","sih","dong","nih","mau","tau","kayak","terus",
        "emang","memang","iya","kan","yg","ku","mu","lo","lu","gue","gw",
        "aku","kk","bg","bang","kak","min","mba","mas","pak","bu","om",
        "tante","wkwk","wkwkwk","haha","hahaha","xixi","eh","oh","ah",
        "hehe","huhu","aww","woah","wow","yuk","ayo","hai","hi","hello",
    }
    return en | id_words

STOPWORDS_ALL = load_stopwords()

EMOJI_RE = re.compile(
    "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002702-\U000027B0\U000024C2-\U0001F251]+",
    flags=re.UNICODE,
)
URL_RE     = re.compile(r"https?://\S+|www\.\S+")
MENTION_RE = re.compile(r"@\w+")
HASHTAG_RE = re.compile(r"#\w+")
PUNCT_RE   = re.compile(r"[^\w\s]")
MULTI_SP   = re.compile(r"\s+")

def clean_text(text: str) -> str:
    t = str(text or "")
    t = URL_RE.sub(" ", t)
    t = MENTION_RE.sub(" ", t)
    t = HASHTAG_RE.sub(" ", t)
    t = EMOJI_RE.sub(" ", t)
    t = PUNCT_RE.sub(" ", t)
    t = t.lower()
    tokens = [w for w in t.split() if w not in STOPWORDS_ALL and len(w) > 2]
    return " ".join(tokens)

@st.cache_data(show_spinner=False)
def build_word_freq(texts: tuple) -> dict:
    combined = " ".join(clean_text(t) for t in texts)
    freq: dict[str, int] = {}
    for word in MULTI_SP.split(combined):
        if word:
            freq[word] = freq.get(word, 0) + 1
    return freq

@st.cache_data(show_spinner=False)
def render_wordcloud(freq_items: tuple, colormap: str, bg: str) -> 'Image.Image | None':
    freq = dict(freq_items)
    if not freq:
        return None
    # Increased resolution for clearer word cloud width 1600, height 800
    wc = WordCloud(
        width=1600, height=800,
        background_color=bg,
        colormap=colormap,
        max_words=100,
        prefer_horizontal=0.82,
        min_font_size=16,
        collocations=False,
    ).generate_from_frequencies(freq)
    return wc.to_image()

def fmt(n) -> str:
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    n = float(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{n:,.0f}"

def hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

def calc_eng(row) -> float:
    platform  = str(row.get("PLATFORM", "")).lower()
    likes     = float(row.get("LIKE_COUNT")    or 0)
    comments  = float(row.get("COMMENT_COUNT") or 0)
    if platform == "tiktok":
        views  = float(row.get("VIEW_COUNT")  or 0)
        shares = float(row.get("SHARE_COUNT") or 0)
        return (likes + comments + shares) / views * 100 if views > 0 else 0.0
    followers = float(row.get("AUTHOR_FOLLOWERS") or 0)
    return (likes + comments) / followers * 100 if followers > 0 else 0.0

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_oembed_thumbnail(post_url: str, platform: str) -> 'str | None':
    if not post_url:
        return None
    try:
        # We only use oembed for TikTok now based on user instruction
        if platform == "tiktok":
            resp = requests.get(
                "https://www.tiktok.com/oembed",
                params={"url": post_url},
                timeout=5,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("thumbnail_url") or data.get("thumbnail_url_with_play_button")
    except Exception:
        pass
    return None
