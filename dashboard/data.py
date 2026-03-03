import os
import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

@st.cache_resource
def init_connection():
    path = os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"))
    with open(path, "rb") as f:
        p_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
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
        schema=os.getenv("SNOWFLAKE_GOLD_SCHEMA"),
    )

@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    conn = init_connection()
    with conn.cursor() as cur:
        cur.execute(sql)
        df = cur.fetch_pandas_all()
    if "DATE_DAY" in df.columns:
        df["DATE_DAY"] = pd.to_datetime(df["DATE_DAY"])
    return df

@st.cache_data(ttl=600)
def load_data():
    df_posts_raw = run_query("""
        SELECT
            p.post_key,
            p.platform,
            p.post_description,
            p.post_url,
            p.cover_image_url,
            p.like_count,
            p.comment_count,
            p.view_count,
            p.share_count,
            d.date_day,
            u.username,
            u.author_followers
        FROM fct_social_posts p
        JOIN dim_date  d ON p.created_date_key = d.date_day
        JOIN dim_users u ON p.author_user_key  = u.user_key
    """)

    df_comments_raw = run_query("""
        SELECT
            c.comment_key,
            c.post_key,
            c.platform,
            c.comment_text,
            c.like_count         AS comment_like_count,
            c.reply_count,
            c.created_at,
            c.sentiment_score,
            c.sentiment_label,
            c.sentiment_strength
        FROM fct_social_comments c
        WHERE c.sentiment_label IS NOT NULL
    """)

    df_sent_summary_raw = run_query("""
        SELECT
            s.post_key,
            s.platform,
            s.comment_date,
            s.total_comments,
            s.scored_comments,
            s.positive_count,
            s.neutral_count,
            s.negative_count,
            s.positive_pct,
            s.neutral_pct,
            s.negative_pct,
            s.avg_sentiment_score,
            s.like_weighted_sentiment_score,
            s.dominant_sentiment
        FROM fct_comment_sentiment_summary s
    """)
    
    return df_posts_raw, df_comments_raw, df_sent_summary_raw
