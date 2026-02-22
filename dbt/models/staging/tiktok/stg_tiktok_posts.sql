-- models/staging/tiktok/stg_tiktok_posts.sql

WITH raw_data AS (
    SELECT
        event_id,
        platform,
        username,
        entity_type,
        raw_payload,
        ingested_at
    FROM {{ source('bronze', 'raw_social_events') }}
    WHERE platform = 'tiktok' 
      AND entity_type = 'post'
)

SELECT
    -- Identifiers
    event_id,
    raw_payload:id::string                                      AS post_id,
    raw_payload:author:id::string                               AS user_id,
    username,
    platform,
    -- Content Info
    raw_payload:desc::string                                    AS post_description,
    TO_TIMESTAMP_NTZ(raw_payload:createTime::int)               AS created_at,
    raw_payload:textLanguage::string                            AS language,
    -- Engagement Stats (Metrics)
    raw_payload:stats:playCount::int                            AS view_count,
    raw_payload:stats:diggCount::int                            AS like_count,
    raw_payload:stats:commentCount::int                         AS comment_count,
    raw_payload:stats:shareCount::int                           AS share_count,
    raw_payload:stats:collectCount::int                         AS favorite_count,
    -- Author Stats
    raw_payload:authorStats:followerCount::int                  AS author_followers,
    raw_payload:authorStats:videoCount::int                     AS author_total_videos,
    -- Metadata
    ingested_at
FROM raw_data