-- models/marts/core/dim_users.sql
-- Dimension table for all users across platforms (TikTok + Instagram)

WITH all_users AS (
    -- TikTok post authors (richer profile data available)
    SELECT
        user_id             AS native_user_id,
        username,
        'tiktok'            AS platform,
        author_followers,
        author_total_videos AS author_total_posts,
        NULL::BOOLEAN       AS author_is_verified,  -- Not available in stg_tiktok_posts
        ingested_at
    FROM {{ ref('stg_tiktok_posts') }}

    UNION ALL

    -- TikTok commenters (minimal profile data)
    SELECT
        user_id             AS native_user_id,
        author_username     AS username,
        'tiktok'            AS platform,
        NULL::INT           AS author_followers,
        NULL::INT           AS author_total_posts,
        NULL::BOOLEAN       AS author_is_verified,
        ingested_at
    FROM {{ ref('stg_tiktok_comments') }}

    UNION ALL

    -- Instagram post authors (richer profile data available)
    SELECT
        user_id             AS native_user_id,
        author_username     AS username,
        'instagram'         AS platform,
        author_followers,
        author_total_posts,
        author_is_verified,
        ingested_at
    FROM {{ ref('stg_instagram_posts') }}

    UNION ALL

    -- Instagram commenters (minimal profile data)
    SELECT
        user_id             AS native_user_id,
        author_username     AS username,
        'instagram'         AS platform,
        NULL::INT           AS author_followers,
        NULL::INT           AS author_total_posts,
        author_is_verified,
        ingested_at
    FROM {{ ref('stg_instagram_comments') }}
),

-- Pick the most complete record per user, prioritising post-author rows
-- which carry richer profile data (followers, total_posts, verified flag)
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY platform, native_user_id
            ORDER BY
                author_followers    IS NOT NULL DESC,
                author_total_posts  IS NOT NULL DESC,
                author_is_verified  IS NOT NULL DESC,
                ingested_at         DESC
        ) AS rn
    FROM all_users
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['platform', 'native_user_id']) }}  AS user_key,
    native_user_id,
    username,
    platform,
    author_followers,
    author_total_posts,
    author_is_verified,
    ingested_at                                                             AS last_seen_at
FROM ranked
WHERE rn = 1