-- models/staging/instagram/stg_instagram_posts.sql

WITH raw_data AS (
    SELECT
        event_id,
        platform,
        username,
        entity_type,
        raw_payload,
        ingested_at
    FROM {{ source('bronze', 'raw_social_events') }}
    WHERE platform = 'instagram'
      AND entity_type = 'post'
)

SELECT
    -- Identifiers
    event_id,
    raw_payload:id::string                                      AS post_id,
    raw_payload:iphone_struct:owner:pk::string                  AS user_id,
    username,
    platform,

    -- Content Info
    CONCAT('https://www.instagram.com/p/', raw_payload:shortcode::string) AS post_url,
    raw_payload:caption::string                                 AS post_description,
    TO_TIMESTAMP_NTZ(raw_payload:date::int)                     AS created_at,
    raw_payload:__typename::string                              AS post_type,
    -- 1 = photo, 8 = carousel/sidecar, etc.
    raw_payload:iphone_struct:media_type::int                   AS media_type_code,
    raw_payload:iphone_struct:product_type::string              AS product_type,
    raw_payload:iphone_struct:carousel_media_count::int         AS carousel_slide_count,
    raw_payload:iphone_struct:is_paid_partnership::boolean      AS is_paid_partnership,

    -- Engagement Stats (Metrics)
    raw_payload:edge_media_preview_like:count::int              AS like_count,
    raw_payload:comments::int                                   AS comment_count,

    -- Author Info
    raw_payload:iphone_struct:owner:username::string            AS author_username,
    raw_payload:user:full_name::string                          AS author_full_name,
    raw_payload:user:is_verified::boolean                       AS author_is_verified,
    raw_payload:user:is_private::boolean                        AS author_is_private,
    raw_payload:user:edge_followed_by:count::int                AS author_followers,
    raw_payload:user:edge_follow:count::int                     AS author_following,
    raw_payload:user:edge_owner_to_timeline_media:count::int    AS author_total_posts,

    -- Media Info
    raw_payload:display_url::string                             AS cover_image_url,
    raw_payload:iphone_struct:original_width::int               AS image_width,
    raw_payload:iphone_struct:original_height::int              AS image_height,

    -- Metadata
    ingested_at

FROM raw_data