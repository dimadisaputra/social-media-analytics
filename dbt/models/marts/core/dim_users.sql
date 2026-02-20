-- models/marts/core/dim_users.sql

WITH all_users AS (
    SELECT
        user_id AS native_user_id, -- Kolom ini sekarang sudah ada di stg_tiktok_posts
        username,
        'tiktok' AS platform,
        ingested_at
    FROM {{ ref('stg_tiktok_posts') }}
    
    UNION ALL
    
    SELECT
        user_id AS native_user_id,
        author_username AS username,
        'tiktok' AS platform,
        ingested_at
    FROM {{ ref('stg_tiktok_comments') }}
),

deduplicated AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['platform', 'native_user_id']) }} as user_key,
        native_user_id,
        username,
        platform,
        MAX(ingested_at) as last_seen_at
    FROM all_users
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM deduplicated