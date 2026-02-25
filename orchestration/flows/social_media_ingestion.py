import asyncio
from typing import Any, Dict, List, Optional

from prefect import flow, task
from loguru import logger

from prefect_dbt.cli.commands import DbtCoreOperation

from ingestion.scraper.tiktok import TiktokScraper
from ingestion.scraper.instagram import InstagramScraper
from ingestion.loaders.snowflake import SnowflakeLoader


# ---------------------------------------------------------------------------
# Platform Registry
# ---------------------------------------------------------------------------
# To add a new platform (e.g. YouTube, X, Facebook):
#   1. Create scraper in ingestion/scraper/<platform>.py implementing BaseScraper
#   2. Add an entry here with its scraper class and event builder function
#   3. Done — no changes needed to the flow itself

PLATFORM_REGISTRY: Dict[str, Dict[str, Any]] = {
    "tiktok": {
        "scraper_class": TiktokScraper,
        "event_builder": None,  # assigned after function definitions below
    },
    "instagram": {
        "scraper_class": InstagramScraper,
        "event_builder": None,  # assigned after function definitions below
    },
}


# ---------------------------------------------------------------------------
# Scraper Tasks
# ---------------------------------------------------------------------------

@task(name="Scrape TikTok Data", retries=3, retry_delay_seconds=30)
async def scrape_tiktok_data(
    user_id: str,
    video_count: int = 10,
    comment_count: int = 10,
) -> Dict[str, Any]:
    """
    Scrape user profile, posts, and comments from TikTok.
    """
    scraper = TiktokScraper()
    await scraper.create_session()

    data: Dict[str, Any] = {"posts": [], "comments": []}

    try:
        posts = await scraper.get_posts(user_id, video_count)
        data["posts"] = posts

        for post in posts:
            post_id = post.get("id")
            if not post_id:
                logger.warning(f"[tiktok] Skipping post with missing ID for user: {user_id}")
                continue
            comments = await scraper.get_comments(post_id, comment_count)
            for comment in comments:
                comment["_related_post_id"] = post_id
            data["comments"].extend(comments)

    finally:
        await scraper.cleanup()

    logger.info(f"[tiktok] Scrape complete — user: {user_id} | posts: {len(data['posts'])} | comments: {len(data['comments'])}")
    return data


@task(name="Scrape Instagram Data", retries=3, retry_delay_seconds=60)
async def scrape_instagram_data(
    username: str,
    post_count: int = 10,
    comment_count: int = 10,
) -> Dict[str, Any]:
    """
    Scrape user profile, posts, and comments from Instagram.
    """
    scraper = InstagramScraper()
    await scraper.create_session()

    data: Dict[str, Any] = {"posts": [], "comments": []}

    try:
        posts = await scraper.get_posts(username, post_count)
        data["posts"] = posts

        for post in posts:
            shortcode = post.get("shortcode")
            if not shortcode:
                logger.warning(f"[instagram] Skipping post with missing shortcode for user: {username}")
                continue
            comments = await scraper.get_comments(shortcode, comment_count)
            for comment in comments:
                comment["_related_post_shortcode"] = shortcode
            data["comments"].extend(comments)

    finally:
        if hasattr(scraper, "cleanup") and callable(scraper.cleanup):
            await scraper.cleanup()

    logger.info(f"[instagram] Scrape complete — user: {username} | posts: {len(data['posts'])} | comments: {len(data['comments'])}")
    return data


# ---------------------------------------------------------------------------
# Snowflake Loader Task
# ---------------------------------------------------------------------------

@task(name="Load to Snowflake")
async def load_to_snowflake(platform: str, data: Dict[str, Any]) -> None:
    """
    Build events from scraped data and load them to the Snowflake bronze layer.
    """
    registry_entry = PLATFORM_REGISTRY.get(platform)
    if not registry_entry:
        logger.warning(f"Unknown platform '{platform}' — skipping Snowflake load.")
        return

    events = registry_entry["event_builder"](data)
    logger.info(f"[{platform}] Total events built: {len(events)}")

    if not events:
        logger.warning(f"[{platform}] No events to load — skipping.")
        return

    loader = SnowflakeLoader()
    try:
        loader.load_events(events)
    finally:
        loader.close()


# ---------------------------------------------------------------------------
# dbt Transformation Task
# ---------------------------------------------------------------------------

@task(name="Run dbt Models")
def run_dbt_models() -> None:
    """
    Run dbt models for the Silver (staging/intermediate) and Gold (marts) layers.
    """
    logger.info("Starting dbt run for Silver and Gold layers via prefect-dbt...")

    # ✅ Fix 1: Use prefect_dbt.core (DbtCoreOperation imported above)
    dbt_op = DbtCoreOperation(
        commands=[
            "dbt deps",
            "dbt run"
        ],
        working_dir="dbt",
        project_dir=".",
    )

    dbt_op.run()

    logger.info("dbt run completed successfully.")


# ---------------------------------------------------------------------------
# Event Builders
# ---------------------------------------------------------------------------

def _build_tiktok_events(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build normalized event dicts from raw TikTok scraped data.
    """
    events = []

    def get_username(item: dict) -> str:
        return (
            item.get("user", {}).get("uniqueId")
            or item.get("uniqueId")
            or item.get("author", {}).get("uniqueId")
            or item.get("user", {}).get("unique_id")
            or "unknown"
        )

    if user := data.get("user"):
        user_id = user.get("id") or user.get("user", {}).get("id")
        if user_id:
            events.append({
                "event_id": f"tiktok_user_{user_id}",
                "platform": "tiktok",
                "username": get_username(user),
                "entity_type": "user",
                "raw_payload": user,
            })
        else:
            logger.warning("[tiktok] User event skipped: missing user ID.")

    for post in data.get("posts", []):
        post_id = post.get("id")
        if not post_id:
            logger.warning("[tiktok] Post event skipped: missing post ID.")
            continue
        events.append({
            "event_id": f"tiktok_post_{post_id}",
            "platform": "tiktok",
            "username": get_username(post),
            "entity_type": "post",
            "raw_payload": post,
        })

    for comment in data.get("comments", []):
        comment_id = comment.get("cid")
        if not comment_id:
            logger.warning("[tiktok] Comment event skipped: missing comment ID.")
            continue
        events.append({
            "event_id": f"tiktok_comment_{comment_id}",
            "platform": "tiktok",
            "username": get_username(comment),
            "entity_type": "comment",
            "raw_payload": comment,
        })

    return events


def _build_instagram_events(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build normalized event dicts from raw Instagram scraped data.
    """
    events = []

    def get_username(item: dict) -> str:
        return (
            item.get("user", {}).get("username")
            or item.get("iphone_struct", {}).get("user", {}).get("username")
            or "unknown"
        )

    if user := data.get("user"):
        user_pk = user.get("pk")
        if user_pk:
            events.append({
                "event_id": f"ig_user_{user_pk}",
                "platform": "instagram",
                "username": user.get("username", "unknown"),
                "entity_type": "user",
                "raw_payload": user,
            })
        else:
            logger.warning("[instagram] User event skipped: missing pk.")

    for post in data.get("posts", []):
        post_id = post.get("id")
        if not post_id:
            logger.warning("[instagram] Post event skipped: missing post ID.")
            continue
        events.append({
            "event_id": f"ig_post_{post_id}",
            "platform": "instagram",
            "username": get_username(post),
            "entity_type": "post",
            "raw_payload": post,
        })

    for comment in data.get("comments", []):
        comment_id = comment.get("id")
        if not comment_id:
            logger.warning("[instagram] Comment event skipped: missing comment ID.")
            continue
        events.append({
            "event_id": f"ig_comment_{comment_id}",
            "platform": "instagram",
            "username": get_username(comment),
            "entity_type": "comment",
            "raw_payload": comment,
        })

    return events


# Wire event builders into the registry after they are defined
PLATFORM_REGISTRY["tiktok"]["event_builder"] = _build_tiktok_events
PLATFORM_REGISTRY["instagram"]["event_builder"] = _build_instagram_events


# ---------------------------------------------------------------------------
# Per-platform pipeline runner mapping
# ---------------------------------------------------------------------------

_SCRAPER_TASK_MAP: Dict[str, Dict[str, Any]] = {
    "tiktok": {
        "task": scrape_tiktok_data,
        "target_kwarg": "user_id",
        "count_kwargs": {"video_count", "comment_count"},
    },
    "instagram": {
        "task": scrape_instagram_data,
        "target_kwarg": "username",
        "count_kwargs": {"post_count", "comment_count"},
    },
}


# ---------------------------------------------------------------------------
# Main Flow
# ---------------------------------------------------------------------------

@flow(name="Social Media Ingestion")
async def social_media_ingestion(
    platforms: Dict[str, Dict[str, Any]],
) -> None:
    """
    Orchestrate scraping, loading to Bronze, and executing dbt models.

    Args:
        platforms: Dict keyed by platform name, each value is a config dict with:
            - 'target' (str, required): username or user ID to scrape
            - any platform-specific count kwargs (e.g. video_count, post_count, comment_count)
    """
    if not platforms:
        logger.warning("No platforms provided — nothing to ingest.")
        return

    unknown = set(platforms) - set(PLATFORM_REGISTRY)
    if unknown:
        logger.warning(f"Unknown platform(s) will be skipped: {unknown}")

    scrape_futures = []

    # 1. Submit all scraping tasks in parallel
    for platform, config in platforms.items():
        if platform not in PLATFORM_REGISTRY:
            continue

        target = config.get("target")
        if not target:
            continue

        scrape_kwargs = {k: v for k, v in config.items() if k != "target"}
        logger.info(f"[{platform}] Submitting parallel scrape for target: {target}")

        task_meta = _SCRAPER_TASK_MAP[platform]

        future = task_meta["task"].submit(**{task_meta["target_kwarg"]: target}, **scrape_kwargs)
        scrape_futures.append((platform, future))

    # 2. Wait for all scrapers to finish and resolve their states
    scrape_results = []
    scrape_failures = []

    for platform, future in scrape_futures:
        try:
            # .submit() returns a PrefectFuture, NOT a coroutine.
            # Call future.result() directly — do NOT use `await` here.
            data = future.result()
            scrape_results.append((platform, data))
        except Exception as e:
            logger.error(f"[{platform}] Scrape task failed: {e}")
            scrape_failures.append(platform)

    # Guard — if ALL scrapers failed, there's nothing to load or transform.
    # Abort early instead of running dbt on stale/empty data.
    if not scrape_results:
        logger.error(
            f"All scrape tasks failed ({scrape_failures}). "
            "Aborting pipeline — Snowflake load and dbt run will NOT execute."
        )
        raise RuntimeError(f"All scrapers failed: {scrape_failures}")

    if scrape_failures:
        logger.warning(
            f"Some scrapers failed ({scrape_failures}) but {len(scrape_results)} succeeded. "
            "Continuing with partial data."
        )

    # 3. Submit Snowflake loading tasks in parallel
    load_futures = []
    for platform, data in scrape_results:
        logger.info(f"[{platform}] Submitting load to Snowflake...")
        future = load_to_snowflake.submit(platform, data)
        load_futures.append((platform, future))

    # 4. Wait for Snowflake loads to finish
    load_failures = []
    for platform, future in load_futures:
        try:
            future.result()
        except Exception as e:
            logger.error(f"[{platform}] Snowflake load failed: {e}")
            load_failures.append(platform)

    # Guard — if ALL loads failed, don't run dbt.
    if len(load_failures) == len(load_futures):
        logger.error(
            "All Snowflake loads failed. "
            "Aborting pipeline — dbt run will NOT execute."
        )
        raise RuntimeError(f"All Snowflake loads failed: {load_failures}")

    logger.info("Bronze layer ingestion finished. Proceeding to dbt transformations...")

    # 5. Run dbt models
    try:
        dbt_future = run_dbt_models.submit()
        dbt_future.result()
    except Exception as e:
        logger.error(f"dbt pipeline encountered an error: {e}")
        raise

    logger.info("Social Media Analytics orchestration complete.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    social_media_ingestion.serve(
        name="social-media-ingestion-all",
        parameters={
            "platforms": {
                "tiktok": {
                    "target": "gibran_rakabuming",
                    "video_count": 5,
                    "comment_count": 5,
                },
                "instagram": {
                    "target": "gibran_rakabuming",
                    "post_count": 5,
                    "comment_count": 5,
                },
            }
        },
    )