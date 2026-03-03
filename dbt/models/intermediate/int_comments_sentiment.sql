-- models/intermediate/int_comments_sentiment.sql
--
-- PURPOSE:
--   Runs SNOWFLAKE.CORTEX.SENTIMENT() once per unique comment and caches
--   the result. This is the ONLY place in the project where Cortex is called.
--
-- DAG POSITION:
--
--   stg_tiktok_comments   ──┐
--                            ├──► int_comments_sentiment ──► fct_social_comments
--   stg_instagram_comments ─┘
--
--   IMPORTANT: this model sources from the two STAGING models directly,
--   NOT from fct_social_comments. Sourcing from fct_social_comments would
--   create a cycle (fct → int → fct) and crash the dbt DAG.
--
-- DESIGN RATIONALE:
--   1. INCREMENTAL materialization  → Cortex is billed per character processed.
--      We never re-score a comment we have already scored.
--   2. Staging as source            → comment_text is available in staging;
--      there is no need to wait for the fact table to be built first.
--   3. Surrogate key built here     → we replicate the same generate_surrogate_key
--      logic used in fct_social_comments so the key is join-compatible.
--   4. Input deduplication          → same comment may arrive in multiple raw
--      events. We score each (platform, comment_id) pair exactly once.
--   5. NULL guard                   → empty / NULL text is filtered before the
--      Cortex call; the function throws a runtime error on blank strings.
--   6. Score bucketing here         → derived label and strength columns live
--      next to the raw score, keeping the fact table clean.
--
-- INCREMENTAL STRATEGY:
--   unique_key   = comment_key
--   New rows detected via: ingested_at > MAX(ingested_at) already in this table.
--   on_schema_change = 'sync_all_columns' auto-alters the target if columns change.

{{
  config(
    materialized     = 'incremental',
    unique_key       = 'comment_key',
    on_schema_change = 'sync_all_columns',
    tags             = ['sentiment', 'cortex', 'incremental']
  )
}}

WITH tiktok_comments AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'tiktok\'', 'comment_id']) }} AS comment_key,
        'tiktok'        AS platform,
        comment_text,
        created_at,
        ingested_at
    FROM {{ ref('stg_tiktok_comments') }}

    {% if is_incremental() %}
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1970-01-01'::TIMESTAMP) FROM {{ this }} WHERE platform = 'tiktok')
    {% endif %}

),

instagram_comments AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['\'instagram\'', 'comment_id']) }} AS comment_key,
        'instagram'     AS platform,
        comment_text,
        created_at,
        ingested_at
    FROM {{ ref('stg_instagram_comments') }}

    {% if is_incremental() %}
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1970-01-01'::TIMESTAMP) FROM {{ this }} WHERE platform = 'instagram')
    {% endif %}

),

-- Combine both platforms before deduplication and scoring
all_comments AS (

    SELECT * FROM tiktok_comments
    UNION ALL
    SELECT * FROM instagram_comments

),

-- Deduplicate: the same comment_key may appear multiple times if the ingestion
-- pipeline retried or produced duplicate raw events. Score each key once only.
deduped AS (

    SELECT *
    FROM all_comments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY comment_key ORDER BY ingested_at DESC) = 1

),

-- Guard: drop NULL / empty / whitespace-only text before calling Cortex.
-- SNOWFLAKE.CORTEX.SENTIMENT('') raises a runtime error and wastes credits.
valid_comments AS (

    SELECT *
    FROM deduped
    WHERE comment_text IS NOT NULL
      AND TRIM(comment_text) != ''

),

-- Call Cortex once per valid comment.
-- We store the raw score in a CTE so derived columns reference the CTE value
-- instead of calling the function three times (avoids triple billing per row).
raw_scored AS (

    SELECT
        comment_key,
        platform,
        comment_text,
        created_at,
        ingested_at,
        -- FLOAT [-1.0, +1.0]  |  -1 = most negative, +1 = most positive
        -- Snowflake auto-detects language; no lang param needed.
        SNOWFLAKE.CORTEX.SENTIMENT(comment_text) AS sentiment_score
    FROM valid_comments

)

SELECT
    comment_key,
    platform,
    comment_text,
    created_at,
    ingested_at,

    -- ── Raw score ────────────────────────────────────────────────────────────
    sentiment_score,

    -- ── Bucketed label ───────────────────────────────────────────────────────
    -- ±0.2 dead-band for neutral follows standard NLP convention.
    CASE
        WHEN sentiment_score >  0.2  THEN 'positive'
        WHEN sentiment_score < -0.2  THEN 'negative'
        ELSE                              'neutral'
    END                             AS sentiment_label,

    -- ── Magnitude / strength bucket ──────────────────────────────────────────
    -- Absolute value captures opinion intensity regardless of direction.
    -- Useful for surfacing high-signal comments in dashboards.
    CASE
        WHEN ABS(sentiment_score) >= 0.6  THEN 'strong'
        WHEN ABS(sentiment_score) >= 0.2  THEN 'moderate'
        ELSE                                   'weak'
    END                             AS sentiment_strength,

    -- Audit: when was this row actually scored by Cortex?
    CURRENT_TIMESTAMP()             AS scored_at

FROM raw_scored