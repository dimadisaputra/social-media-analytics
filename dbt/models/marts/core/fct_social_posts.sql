WITH posts AS (
    SELECT * FROM {{ ref('stg_tiktok_posts') }}
),

users AS (
    SELECT * FROM {{ ref('dim_users') }}
)

SELECT
    p.event_id AS post_key,
    
    -- GENERATE FOREIGN KEY (Harus sama persis logicnya dengan dim_users)
    {{ dbt_utils.generate_surrogate_key(['p.platform', 'p.user_id']) }} AS author_user_key,
    
    p.created_at::DATE AS created_date_key,
    
    -- Degenerate Dimensions
    p.post_id AS native_post_id,
    p.platform,
    p.post_description,
    p.language,
    
    -- Metrics
    p.view_count,
    p.like_count,
    p.comment_count,
    p.share_count,
    p.favorite_count,
    
    p.ingested_at
FROM posts p