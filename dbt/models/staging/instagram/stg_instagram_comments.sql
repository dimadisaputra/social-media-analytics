-- models/staging/instagram/stg_instagram_comments.sql

WITH raw_comments AS (
    SELECT
        event_id,
        platform,
        username,
        entity_type,
        raw_payload,
        ingested_at
    FROM {{ source('bronze', 'raw_social_events') }}
    WHERE platform = 'instagram'
      AND entity_type = 'comment'
)

SELECT
    -- Identifiers
    raw_payload:id::string                                          AS comment_id,
    raw_payload:iphone_struct:media_id::string                      AS post_id, -- FK ke stg_instagram_posts
    raw_payload:iphone_struct:user:id::string                       AS user_id,
    raw_payload:iphone_struct:user:username::string                 AS author_username,

    -- Comment Content
    raw_payload:text::string                                        AS comment_text,
    TO_TIMESTAMP_NTZ(raw_payload:created_at::int)                   AS created_at,

    -- Engagement Stats
    raw_payload:edge_liked_by:count::int                            AS comment_like_count,
    raw_payload:iphone_struct:child_comment_count::int              AS reply_count,

    -- Threading Info
    raw_payload:iphone_struct:type::int                             AS comment_type,
    -- type 0 = top-level comment, type 2 = reply
    raw_payload:iphone_struct:parent_comment_id::string             AS parent_comment_id,
    -- NULL if top-level comment

    -- Comment Flags
    raw_payload:iphone_struct:is_pinned::boolean                    AS is_pinned,
    raw_payload:iphone_struct:is_liked_by_media_owner::boolean      AS is_liked_by_post_owner,
    raw_payload:iphone_struct:is_ranked_comment::boolean            AS is_ranked_comment,
    raw_payload:iphone_struct:is_edited::boolean                    AS is_edited,

    -- Author Info
    raw_payload:iphone_struct:user:full_name::string                AS author_full_name,
    raw_payload:iphone_struct:user:is_verified::boolean             AS author_is_verified,
    raw_payload:iphone_struct:user:is_private::boolean              AS author_is_private,

    -- Metadata
    ingested_at

FROM raw_comments