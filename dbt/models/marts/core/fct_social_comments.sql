-- models/marts/core/fct_social_comments.sql
-- Unified fact table for social media comments across all platforms.
--
-- Sentiment scores are LEFT JOINed from int_comments_sentiment so that:
--   • Rows without a score (empty text, not yet processed) are still present
--     in the fact table with NULL sentiment columns — no data is lost.
--   • Cortex compute lives entirely in the intermediate layer; this model
--     never calls SNOWFLAKE.CORTEX.SENTIMENT() directly.

WITH tiktok AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'tiktok\'', 'comment_id']) }}    AS comment_key,
        {{ dbt_utils.generate_surrogate_key(['\'tiktok\'', 'post_id']) }}        AS post_key,          -- FK → fct_social_posts
        {{ dbt_utils.generate_surrogate_key(['\'tiktok\'', 'user_id']) }}        AS commenter_user_key, -- FK → dim_users
        'tiktok'                AS platform,

        comment_id              AS native_comment_id,
        post_id                 AS native_post_id,
        user_id                 AS commenter_id,
        author_username         AS commenter_username,

        comment_text,
        created_at,

        comment_like_count      AS like_count,
        reply_count,

        NULL::STRING            AS parent_comment_id,      -- TikTok staging does not expose threading
        NULL::BOOLEAN           AS is_pinned,              -- TikTok staging does not expose this flag
        NULL::BOOLEAN           AS is_liked_by_post_owner, -- TikTok staging does not expose this flag

        ingested_at
    FROM {{ ref('stg_tiktok_comments') }}
),

instagram AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'instagram\'', 'comment_id']) }}  AS comment_key,
        {{ dbt_utils.generate_surrogate_key(['\'instagram\'', 'post_id']) }}      AS post_key,          -- FK → fct_social_posts
        {{ dbt_utils.generate_surrogate_key(['\'instagram\'', 'user_id']) }}      AS commenter_user_key, -- FK → dim_users
        'instagram'             AS platform,

        comment_id              AS native_comment_id,
        post_id                 AS native_post_id,
        user_id                 AS commenter_id,
        author_username         AS commenter_username,

        comment_text,
        created_at,

        comment_like_count      AS like_count,
        reply_count,

        parent_comment_id,
        is_pinned,
        is_liked_by_post_owner,

        ingested_at
    FROM {{ ref('stg_instagram_comments') }}
),

all_comments AS (
    SELECT * FROM tiktok
    UNION ALL
    SELECT * FROM instagram
),

-- Pull pre-computed Cortex scores from the intermediate layer.
-- Only comment_key + the three sentiment columns are needed here.
sentiment AS (
    SELECT
        comment_key,
        sentiment_score,
        sentiment_label,
        sentiment_strength,
        scored_at           AS sentiment_scored_at
    FROM {{ ref('int_comments_sentiment') }}
)

SELECT
    -- Keys
    c.comment_key,
    c.post_key,
    c.commenter_user_key,

    -- Degenerate dimensions
    c.platform,
    c.native_comment_id,
    c.native_post_id,
    c.commenter_id,
    c.commenter_username,
    c.comment_text,
    c.created_at,

    -- Engagement metrics
    c.like_count,
    c.reply_count,

    -- Threading & flags
    c.parent_comment_id,
    c.is_pinned,
    c.is_liked_by_post_owner,

    -- Sentiment  (NULL when text was empty or incremental run hasn't scored yet)
    s.sentiment_score,
    s.sentiment_label,
    s.sentiment_strength,
    s.sentiment_scored_at,

    -- Metadata
    c.ingested_at

FROM all_comments   c
LEFT JOIN sentiment s ON c.comment_key = s.comment_key