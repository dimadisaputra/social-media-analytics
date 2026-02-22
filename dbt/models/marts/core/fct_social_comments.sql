-- models/marts/core/fct_social_comments.sql
-- Unified fact table for social media comments across all platforms

WITH tiktok AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'tiktok\'', 'comment_id']) }}    AS comment_key,
        {{ dbt_utils.generate_surrogate_key(['\'tiktok\'', 'post_id']) }}        AS post_key,  -- FK ke fct_social_posts
        {{ dbt_utils.generate_surrogate_key(['\'tiktok\'', 'user_id']) }}        AS commenter_user_key,  -- FK ke dim_users
        'tiktok'                AS platform,

        comment_id              AS native_comment_id,
        post_id                 AS native_post_id,
        user_id                 AS commenter_id,
        author_username         AS commenter_username,

        comment_text,
        created_at,

        comment_like_count      AS like_count,
        reply_count,

        NULL::STRING            AS parent_comment_id,  -- TikTok staging does not expose parent threading
        NULL::BOOLEAN           AS is_pinned,          -- TikTok staging does not expose this flag
        NULL::BOOLEAN           AS is_liked_by_post_owner,  -- TikTok staging does not expose this flag

        ingested_at
    FROM {{ ref('stg_tiktok_comments') }}
),

instagram AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'instagram\'', 'comment_id']) }}  AS comment_key,
        {{ dbt_utils.generate_surrogate_key(['\'instagram\'', 'post_id']) }}      AS post_key,  -- FK ke fct_social_posts
        {{ dbt_utils.generate_surrogate_key(['\'instagram\'', 'user_id']) }}      AS commenter_user_key,  -- FK ke dim_users
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
)

SELECT * FROM tiktok
UNION ALL
SELECT * FROM instagram