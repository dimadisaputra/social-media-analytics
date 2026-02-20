-- Model: stg_tiktok_comments
-- Deskripsi: Parsing data komentar TikTok dari Bronze Layer

WITH raw_comments AS (
    SELECT
        event_id,
        platform,
        username,
        entity_type,
        raw_payload,
        ingested_at
    FROM {{ source('bronze', 'raw_social_events') }}
    WHERE platform = 'tiktok' 
      AND entity_type = 'comment'
)

SELECT
    -- Identifiers
    raw_payload:cid::string AS comment_id,
    raw_payload:aweme_id::string AS post_id, -- FK ke stg_tiktok_posts
    raw_payload:user:uid::string AS user_id,
    raw_payload:user:unique_id::string AS author_username,
    
    -- Comment Content
    raw_payload:text::string AS comment_text,
    raw_payload:comment_language::string AS language,
    TO_TIMESTAMP_NTZ(raw_payload:create_time::int) AS created_at,
    
    -- Engagement Stats
    raw_payload:digg_count::int AS comment_like_count,
    raw_payload:reply_comment_total::int AS reply_count,
    
    -- Threading Info
    raw_payload:reply_id::string AS parent_reply_id, -- Jika "0" maka ini komentar utama
    
    -- Metadata
    ingested_at
FROM raw_comments