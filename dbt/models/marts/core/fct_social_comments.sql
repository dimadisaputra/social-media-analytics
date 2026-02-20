-- models/marts/core/fct_social_comments.sql

WITH tiktok AS (
    SELECT
        'tiktok_' || comment_id AS global_comment_id,
        'tiktok_' || post_id AS global_post_id, -- Foreign Key ke fct_social_posts
        'tiktok' AS platform,
        
        user_id AS commenter_id,
        author_username AS commenter_username,
        
        comment_text,
        created_at,
        
        comment_like_count AS like_count,
        reply_count
    FROM {{ ref('stg_tiktok_comments') }}
)

SELECT * FROM tiktok