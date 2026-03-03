import asyncio
import warnings
from pathlib import Path
from typing import Any, Dict, List, Tuple

from prefect import flow, task
from loguru import logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

# Filter out prefect_dbt warnings globally
warnings.filterwarnings("ignore", category=UserWarning, module="prefect_dbt")

from ingestion.scraper.tiktok import TiktokScraper
from ingestion.scraper.instagram import InstagramScraper
from ingestion.loaders.snowflake import SnowflakeLoader

# ---------------------------------------------------------------------------
# Cancellation-aware concurrency helper
# ---------------------------------------------------------------------------
async def _gather_with_cancellation(*coros) -> List[Any]:
    """
    Executes coroutines concurrently while enforcing strict cancellation.
    
    If Prefect cancels or suspends the flow, it injects a CancelledError.
    This wrapper catches that error and explicitly sends a .cancel() signal 
    to all underlying tasks (like Playwright subprocesses) to prevent zombie processes,
    ensuring they trigger their 'finally' blocks for proper cleanup.
    """
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        # return_exceptions=True allows the pipeline to continue if one scraper fails naturally,
        # without crashing the siblings.
        return await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.warning("Prefect UI Cancellation/Suspension detected! Forcefully stopping all tasks...")
        for t in tasks:
            if not t.done():
                t.cancel()
        
        # Await tasks again so their 'finally' cleanup blocks (e.g., closing browsers) can finish
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

# ---------------------------------------------------------------------------
# Platform Registry & Event Builders
# ---------------------------------------------------------------------------
def _build_tiktok_events(data: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    for post in data.get("posts", []):
        post_id = post.get("id")
        if post_id:
            events.append({
                "event_id": f"tiktok_post_{post_id}",
                "platform": "tiktok",
                "username": get_username(post),
                "entity_type": "post",
                "raw_payload": post,
            })

    for comment in data.get("comments", []):
        comment_id = comment.get("cid")
        if comment_id:
            events.append({
                "event_id": f"tiktok_comment_{comment_id}",
                "platform": "tiktok",
                "username": get_username(comment),
                "entity_type": "comment",
                "raw_payload": comment,
            })

    return events

def _build_instagram_events(data: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    for post in data.get("posts", []):
        post_id = post.get("id")
        if post_id:
            events.append({
                "event_id": f"ig_post_{post_id}",
                "platform": "instagram",
                "username": get_username(post),
                "entity_type": "post",
                "raw_payload": post,
            })

    for comment in data.get("comments", []):
        comment_id = comment.get("id")
        if comment_id:
            events.append({
                "event_id": f"ig_comment_{comment_id}",
                "platform": "instagram",
                "username": get_username(comment),
                "entity_type": "comment",
                "raw_payload": comment,
            })

    return events

PLATFORM_REGISTRY: Dict[str, Dict[str, Any]] = {
    "tiktok": {
        "scraper_class": TiktokScraper,
        "event_builder": _build_tiktok_events,
    },
    "instagram": {
        "scraper_class": InstagramScraper,
        "event_builder": _build_instagram_events,
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
    """Scrape user profile, posts, and comments from TikTok."""
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

    except asyncio.CancelledError:
        logger.warning("[tiktok] Task explicitly cancelled — cleaning up Playwright...")
        raise
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
    """Scrape user profile, posts, and comments from Instagram."""
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

    except asyncio.CancelledError:
        logger.warning("[instagram] Task explicitly cancelled — cleaning up...")
        raise
    finally:
        if hasattr(scraper, "cleanup") and callable(scraper.cleanup):
            await scraper.cleanup()

    logger.info(f"[instagram] Scrape complete — user: {username} | posts: {len(data['posts'])} | comments: {len(data['comments'])}")
    return data

# ---------------------------------------------------------------------------
# Snowflake & dbt Tasks
# ---------------------------------------------------------------------------
@task(name="Load to Snowflake")
async def load_to_snowflake(platform: str, data: Dict[str, Any]) -> None:
    """Build events from scraped data and load them to the Snowflake bronze layer."""
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

@task(name="Run dbt Models")
def run_dbt_models() -> None:
    """Run dbt models for the Silver and Gold layers using the modern PrefectDbtRunner."""
    settings = PrefectDbtSettings(project_dir="dbt", profiles_dir=str(Path.home() / ".dbt"))
    runner = PrefectDbtRunner(settings=settings)

    logger.info("[dbt] Step 1/2 — Installing packages (dbt deps)...")
    runner.invoke(["deps"])

    logger.info("[dbt] Step 2/2 — Running dbt models...")
    runner.invoke(["run"])

    logger.info("[dbt] All steps completed successfully.")

# ---------------------------------------------------------------------------
# Task Map
# ---------------------------------------------------------------------------
_SCRAPER_TASK_MAP: Dict[str, Dict[str, Any]] = {
    "tiktok": {
        "task": scrape_tiktok_data,
        "target_kwarg": "user_id",
    },
    "instagram": {
        "task": scrape_instagram_data,
        "target_kwarg": "username",
    },
}

# ---------------------------------------------------------------------------
# Main Flow
# ---------------------------------------------------------------------------
@flow(name="Social Media Ingestion")
async def social_media_ingestion(platforms: Dict[str, Dict[str, Any]]) -> None:
    """Orchestrate scraping, loading to Bronze, and executing dbt models."""
    if not platforms:
        logger.warning("No platforms provided — nothing to ingest.")
        return

    unknown = set(platforms) - set(PLATFORM_REGISTRY)
    if unknown:
        logger.warning(f"Unknown platform(s) will be skipped: {unknown}")

    # 1. Prepare Scraper Coroutines
    scrape_coros = []
    platform_names = []

    for platform, config in platforms.items():
        if platform not in PLATFORM_REGISTRY or not config.get("target"):
            continue

        target = config["target"]
        scrape_kwargs = {k: v for k, v in config.items() if k != "target"}
        logger.info(f"[{platform}] Preparing scrape task for target: {target}")

        task_meta = _SCRAPER_TASK_MAP[platform]
        task_func = task_meta["task"]
        
        # Instantiate the task coroutine directly
        coro = task_func(**{task_meta["target_kwarg"]: target}, **scrape_kwargs)
        scrape_coros.append(coro)
        platform_names.append(platform)

    # 2. Execute Scrapers Concurrently with strict cancellation handling
    try:
        results = await _gather_with_cancellation(*scrape_coros)
    except asyncio.CancelledError:
        logger.error("Scrape phase was forcefully halted by Prefect. Aborting pipeline.")
        raise

    scrape_results: List[Tuple[str, Dict[str, Any]]] = []
    scrape_failures: List[str] = []

    for platform, result in zip(platform_names, results):
        if isinstance(result, asyncio.CancelledError):
            logger.error(f"[{platform}] Scrape task was explicitly cancelled.")
            scrape_failures.append(platform)
        elif isinstance(result, Exception):
            logger.error(f"[{platform}] Scrape task failed: {result}")
            scrape_failures.append(platform)
        else:
            scrape_results.append((platform, result))

    if not scrape_results:
        raise RuntimeError(f"All scrape tasks failed or were cancelled: {scrape_failures}")

    if scrape_failures:
        logger.warning(f"Continuing with partial data. Failures: {scrape_failures}")

    # 3. Prepare Snowflake Loader Coroutines
    load_coros = []
    load_platforms = []
    
    for platform, data in scrape_results:
        logger.info(f"[{platform}] Preparing load to Snowflake task...")
        coro = load_to_snowflake(platform, data)
        load_coros.append(coro)
        load_platforms.append(platform)

    # 4. Execute Loaders Concurrently with strict cancellation handling
    try:
        results = await _gather_with_cancellation(*load_coros)
    except asyncio.CancelledError:
        logger.error("Load phase was forcefully halted by Prefect. Aborting pipeline.")
        raise

    load_failures: List[str] = []

    for platform, result in zip(load_platforms, results):
        if isinstance(result, asyncio.CancelledError) or isinstance(result, Exception):
            logger.error(f"[{platform}] Snowflake load failed or was cancelled: {result}")
            load_failures.append(platform)

    if len(load_failures) == len(load_coros):
        raise RuntimeError(f"All Snowflake loads failed: {load_failures}")

    logger.info("Bronze layer ingestion finished. Proceeding to dbt transformations...")

    # 5. Run dbt Models
    run_dbt_models()
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
                    "video_count": 10,
                    "comment_count": 10,
                },
                "instagram": {
                    "target": "gibran_rakabuming",
                    "post_count": 10,
                    "comment_count": 10,
                },
            }
        },
    )