# Social Media Analytics

A full-stack data pipeline and analytics dashboard for monitoring TikTok and Instagram performance. Built on Snowflake, dbt, and Streamlit — with AI-powered sentiment analysis via Snowflake Cortex.

---

## Architecture Overview

```
[TikTok / Instagram APIs]
          │
          ▼
  [Scraper Layer]          ← Python + Playwright (TikTok), Instaloader (Instagram)
          │
          ▼
  [Bronze Layer]           ← Snowflake: RAW_SOCIAL_EVENTS (raw JSON VARIANT)
          │
          ▼
  [Silver Layer]           ← dbt staging + intermediate models
          │  └─ int_comments_sentiment  ← Snowflake Cortex sentiment scoring
          ▼
  [Gold Layer]             ← dbt mart models (facts + dimensions)
          │
          ▼
  [Streamlit Dashboard]    ← Interactive analytics UI
```

**Orchestration:** Prefect flows coordinate scraping → loading → dbt runs end-to-end.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Scraping | Python, TikTokApi (Playwright), Instaloader |
| Orchestration | Prefect 3 + prefect-dbt |
| Data Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Sentiment AI | Snowflake Cortex (`CORTEX.SENTIMENT()`) |
| Dashboard | Streamlit + Plotly |
| Configuration | Pydantic Settings + python-dotenv |

---

## Project Structure

```
├── config/                  # Environment-aware settings (dev/staging/prod)
├── ingestion/
│   ├── scraper/             # TikTok and Instagram scraper implementations
│   │   ├── tiktok.py        # Playwright-based scraper with bot detection mitigation
│   │   └── instagram.py     # Instaloader-based scraper with 2FA support
│   └── loaders/
│       └── snowflake.py     # Bronze layer loader using MERGE
├── orchestration/
│   └── flows/
│       └── social_media_ingestion.py  # Prefect flow: scrape → load → dbt
├── dbt/
│   ├── models/
│   │   ├── staging/         # stg_tiktok_posts, stg_instagram_posts, comments
│   │   ├── intermediate/    # int_comments_sentiment (Cortex scoring)
│   │   └── marts/core/      # fct_social_posts, fct_social_comments,
│   │                        # fct_comment_sentiment_summary, dim_date, dim_users
│   └── sources/bronze.yml
├── dashboard/
│   ├── app.py               # Streamlit entry point
│   ├── components/          # KPI cards, charts, sentiment, posts, word cloud
│   ├── data.py              # Snowflake queries + caching
│   ├── config.py            # Chart theme and global CSS
│   └── utils.py             # Formatters, word cloud builder, engagement calc
└── warehouse/               # Snowflake DDL setup scripts (01–05)
```

---

## Setup

### Prerequisites

- Python 3.12+
- A Snowflake account with Cortex enabled
- Snowflake key-pair authentication configured
- A TikTok `ms_token` (obtained from browser cookies)
- Instagram credentials (with optional 2FA secret)

### 1. Install dependencies

```bash
pip install -e .
```

### 2. Configure environment

```bash
cp .env.example .env
```

Fill in `.env`:

```env
APP_ENV=dev

# TikTok
TIKTOK_MS_TOKEN=<your_ms_token>

# Instagram
INSTAGRAM_USERNAME=<your_username>
INSTAGRAM_PASSWORD=<your_password>
INSTAGRAM_2FA_SECRET=<totp_secret_if_applicable>

# Snowflake
SNOWFLAKE_ACCOUNT=<account_identifier>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PRIVATE_KEY_PATH=~/.snowflake/rsa_key.p8
SNOWFLAKE_DATABASE=SOCIAL_MEDIA_DW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=<your_role>

# Proxies (optional)
WEBSHARE_API_KEY=<key_if_using_proxy>
```

### 3. Initialize Snowflake

Run the warehouse setup scripts in order:

```sql
-- In Snowflake UI or SnowSQL
01_create_database.sql
02_create_schemas.sql
03_create_warehouses.sql
04_create_bronze_table.sql
05_enable_any_region.sql   -- Required for Cortex cross-region access
```

### 4. Set up dbt

```bash
cd dbt
dbt deps
dbt run
```

---

## Running the Pipeline

### One-off run

```bash
python -m orchestration.flows.social_media_ingestion
```

### Serving as a Prefect deployment

```bash
python orchestration/flows/social_media_ingestion.py
```

This starts a Prefect server and registers the flow with default parameters (configurable via Prefect UI). The `platforms` parameter controls which accounts to scrape:

```python
{
    "tiktok": {
        "target": "some_username",
        "video_count": 10,
        "comment_count": 10,
    },
    "instagram": {
        "target": "some_username",
        "post_count": 10,
        "comment_count": 10,
    }
}
```

---

## Running the Dashboard

```bash
streamlit run dashboard/app.py
```

The dashboard connects to Snowflake's Gold layer and provides:

- **KPI Cards** — Total views, likes, comments, and blended engagement rate
- **Trend Chart** — Likes / views / engagement / comments over time by platform
- **Platform Split** — Donut chart showing metric distribution across TikTok and Instagram
- **Sentiment Overview** — Distribution, trend, and per-platform average sentiment scores powered by Cortex
- **Top Posts by Sentiment** — Most positive, neutral, and negative posts
- **Scatter Plot** — Likes vs. engagement rate (dot size = comment count)
- **Word Cloud** — Comment word frequency, filterable by sentiment label
- **Top 10 Posts** — Ranked by likes, comments, or views with thumbnails

---

## dbt Models

### Staging (Silver schema — views)
| Model | Description |
|---|---|
| `stg_tiktok_posts` | Flattened TikTok post payloads |
| `stg_tiktok_comments` | Flattened TikTok comment payloads |
| `stg_instagram_posts` | Flattened Instagram post payloads |
| `stg_instagram_comments` | Flattened Instagram comment payloads |

### Intermediate (Silver schema — incremental)
| Model | Description |
|---|---|
| `int_comments_sentiment` | Cortex sentiment scoring — runs once per unique comment, never re-scores |

### Marts (Gold schema — tables)
| Model | Description |
|---|---|
| `fct_social_posts` | Unified post fact table across all platforms |
| `fct_social_comments` | Unified comment fact table with LEFT JOIN sentiment |
| `fct_comment_sentiment_summary` | Pre-aggregated sentiment per post per day |
| `dim_date` | Date dimension (via dbt_date package) |
| `dim_users` | Deduplicated user dimension across platforms |

---

## Key Design Decisions

**Incremental Cortex scoring** — `int_comments_sentiment` uses an incremental materialization with a `unique_key` on `comment_key`. Cortex is only called for new comments, preventing repeated billing on existing data.

**Bronze MERGE pattern** — The Snowflake loader uses a staging table + `MERGE` statement to ensure idempotent loads. Duplicate `event_id`s are silently ignored.

**Bot detection mitigation (TikTok)** — The TikTok scraper implements token rotation, escalating cooldowns (60s → 300s), and a full Playwright teardown/rebuild when persistent bot detection is encountered.

**Cancellation-safe concurrency** — Prefect flow uses a custom `_gather_with_cancellation` wrapper that propagates `CancelledError` to all subtasks, ensuring Playwright processes are properly cleaned up on flow suspension or cancellation.

---

## Environment Modes

| `APP_ENV` | Log Level | Debug |
|---|---|---|
| `dev` | DEBUG | ✅ |
| `staging` | INFO | ❌ |
| `prod` | WARNING | ❌ |