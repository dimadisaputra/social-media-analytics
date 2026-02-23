-- models/marts/core/fct_comment_sentiment_summary.sql
--
-- PURPOSE:
--   Aggregated sentiment summary per post, ready for dashboards and BI tools.
--   Analysts should query THIS model rather than joining fct_social_comments
--   directly for sentiment reporting — it is cheaper and pre-aggregated.
--
-- GRAIN: one row per (post_key, platform, date)

{{
  config(
    materialized = 'table',
    tags         = ['sentiment', 'marts']
  )
}}

WITH comments AS (

    SELECT
        post_key,
        platform,
        created_at::DATE        AS comment_date,

        -- Scoring coverage flags
        sentiment_label,
        sentiment_strength,
        sentiment_score,

        -- Engagement
        like_count,
        reply_count
    FROM {{ ref('fct_social_comments') }}

),

agg AS (

    SELECT
        post_key,
        platform,
        comment_date,

        -- ── Volume ──────────────────────────────────────────────────────────
        COUNT(*)                                                AS total_comments,
        COUNT(sentiment_label)                                  AS scored_comments,
        -- How many comments still await scoring (incremental lag)
        COUNT(*) - COUNT(sentiment_label)                       AS pending_score_count,

        -- ── Sentiment distribution ──────────────────────────────────────────
        COUNT_IF(sentiment_label = 'positive')                  AS positive_count,
        COUNT_IF(sentiment_label = 'neutral')                   AS neutral_count,
        COUNT_IF(sentiment_label = 'negative')                  AS negative_count,

        -- ── Percentages (only over scored comments to avoid skew) ───────────
        ROUND(
            COUNT_IF(sentiment_label = 'positive') * 100.0
            / NULLIF(COUNT(sentiment_label), 0), 2)             AS positive_pct,
        ROUND(
            COUNT_IF(sentiment_label = 'neutral') * 100.0
            / NULLIF(COUNT(sentiment_label), 0), 2)             AS neutral_pct,
        ROUND(
            COUNT_IF(sentiment_label = 'negative') * 100.0
            / NULLIF(COUNT(sentiment_label), 0), 2)             AS negative_pct,

        -- ── Aggregate score ─────────────────────────────────────────────────
        -- avg_sentiment_score: quick single-number health indicator per post
        ROUND(AVG(sentiment_score), 4)                          AS avg_sentiment_score,

        -- Weighted by likes: a liked comment carries more audience weight
        ROUND(
            SUM(sentiment_score * COALESCE(like_count, 0))
            / NULLIF(SUM(COALESCE(like_count, 0)), 0), 4)       AS like_weighted_sentiment_score,

        -- ── Strength distribution ────────────────────────────────────────────
        COUNT_IF(sentiment_strength = 'strong')                 AS strong_sentiment_count,
        COUNT_IF(sentiment_strength = 'moderate')               AS moderate_sentiment_count,
        COUNT_IF(sentiment_strength = 'weak')                   AS weak_sentiment_count,

        -- ── Engagement ───────────────────────────────────────────────────────
        SUM(like_count)                                         AS total_comment_likes,
        SUM(reply_count)                                        AS total_replies

    FROM comments
    GROUP BY 1, 2, 3

),

-- Add a human-readable dominant sentiment label for quick filtering
final AS (

    SELECT
        *,
        CASE
            WHEN scored_comments = 0                            THEN 'unscored'
            WHEN positive_count >= neutral_count
             AND positive_count >= negative_count               THEN 'positive'
            WHEN negative_count > positive_count
             AND negative_count > neutral_count                 THEN 'negative'
            ELSE                                                     'neutral'
        END                                                     AS dominant_sentiment

    FROM agg

)

SELECT * FROM final
