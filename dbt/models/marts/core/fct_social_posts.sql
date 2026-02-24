-- models/marts/core/fct_social_posts.sql
-- Unified fact table for social media posts across all platforms

WITH tiktok_posts AS (
    SELECT
        event_id,
        post_id,
        user_id,
        username,
        platform,
        post_url,
        post_description,
        NULL::string AS cover_image_url,
        created_at,
        view_count,
        like_count,
        comment_count,
        share_count,
        favorite_count,
        ingested_at
    FROM {{ ref('stg_tiktok_posts') }}
),

instagram_posts AS (
    SELECT
        event_id,
        post_id,
        user_id,
        username,
        platform,
        post_url,
        post_description,
        cover_image_url,
        created_at,
        NULL::INT AS view_count,       -- Instagram does not expose view_count for photo/carousel posts
        like_count,
        comment_count,
        NULL::INT AS share_count,      -- Instagram does not expose share_count in this payload
        NULL::INT AS favorite_count,   -- Instagram does not expose favorite/save count in this payload
        ingested_at
    FROM {{ ref('stg_instagram_posts') }}
),

all_posts AS (
    SELECT * FROM tiktok_posts
    UNION ALL
    SELECT * FROM instagram_posts
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['p.platform', 'p.post_id']) }}    AS post_key,

    -- Foreign Keys
    {{ dbt_utils.generate_surrogate_key(['p.platform', 'p.user_id']) }}    AS author_user_key,
    p.created_at::DATE                                                      AS created_date_key,

    -- Degenerate Dimensions
    p.post_id           AS native_post_id,
    p.platform,
    p.post_url,
    p.post_description,
    p.cover_image_url,

    -- Metrics
    p.view_count,
    p.like_count,
    p.comment_count,
    p.share_count,
    p.favorite_count,

    -- Metadata
    p.event_id,
    p.ingested_at

FROM all_posts p