import asyncio
from typing import Any, Dict, List, Optional

from prefect import flow, task
from loguru import logger

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

    Args:
        user_id: TikTok username to scrape.
        video_count: Number of videos to fetch.
        comment_count: Number of comments to fetch per video.

    Returns:
        Dict with keys 'user', 'posts', and 'comments'.
    """
    scraper = TiktokScraper()
    await scraper.create_session()

    data: Dict[str, Any] = {"user": None, "posts": [], "comments": []}

    try:
        data["user"] = await scraper.get_user(user_id)

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

    logger.info(
        f"[tiktok] Scrape complete — user: {user_id} | "
        f"posts: {len(data['posts'])} | comments: {len(data['comments'])}"
    )
    return data


@task(name="Scrape Instagram Data", retries=3, retry_delay_seconds=60)
async def scrape_instagram_data(
    username: str,
    post_count: int = 10,
    comment_count: int = 10,
) -> Dict[str, Any]:
    """
    Scrape user profile, posts, and comments from Instagram.

    Args:
        username: Instagram username to scrape.
        post_count: Number of posts to fetch.
        comment_count: Number of comments to fetch per post.

    Returns:
        Dict with keys 'user', 'posts', and 'comments'.
    """
    scraper = InstagramScraper()
    await scraper.create_session()

    data: Dict[str, Any] = {"user": None, "posts": [], "comments": []}

    try:
        data["user"] = await scraper.get_user(username)

        posts = await scraper.get_posts(username, post_count)
        data["posts"] = posts

        for post in posts:
            # get_comments expects shortcode (e.g. from instagram.com/p/<shortcode>/)
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

    logger.info(
        f"[instagram] Scrape complete — user: {username} | "
        f"posts: {len(data['posts'])} | comments: {len(data['comments'])}"
    )
    return data


# ---------------------------------------------------------------------------
# Snowflake Loader Task
# ---------------------------------------------------------------------------

@task(name="Load to Snowflake")
async def load_to_snowflake(platform: str, data: Dict[str, Any]) -> None:
    """
    Build events from scraped data and load them to the Snowflake bronze layer.

    Args:
        platform: Source platform key (must exist in PLATFORM_REGISTRY).
        data: Scraped data dict with keys 'user', 'posts', and 'comments'.
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
# Event Builders
# ---------------------------------------------------------------------------

def _build_tiktok_events(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build normalized event dicts from raw TikTok scraped data.

    Args:
        data: Dict with keys 'user', 'posts', 'comments'.

    Returns:
        List of event dicts ready for Snowflake ingestion.
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

    Args:
        data: Dict with keys 'user', 'posts', 'comments'.

    Returns:
        List of event dicts ready for Snowflake ingestion.
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
# Per-platform pipeline runner
# ---------------------------------------------------------------------------

# Maps each platform key to its scrape task and the kwarg name for the target identifier
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


async def _run_platform(
    platform: str,
    target: str,
    scrape_kwargs: Dict[str, Any],
) -> None:
    """
    Run a single platform's scrape + load pipeline with isolated error handling.

    Failures are caught and logged so sibling platforms are not affected.

    Args:
        platform: Platform key from PLATFORM_REGISTRY.
        target: Username or user ID to scrape.
        scrape_kwargs: Extra keyword args forwarded to the scrape task
                       (e.g. video_count, post_count, comment_count).
    """
    task_meta = _SCRAPER_TASK_MAP.get(platform)
    if not task_meta:
        logger.warning(f"No scraper task registered for platform '{platform}' — skipping.")
        return

    scrape_task = task_meta["task"]
    target_kwarg = task_meta["target_kwarg"]

    try:
        data = await scrape_task(**{target_kwarg: target}, **scrape_kwargs)
        await load_to_snowflake(platform, data)
    except Exception as e:
        logger.error(f"[{platform}] Ingestion failed for target '{target}': {e}")


# ---------------------------------------------------------------------------
# Main Flow
# ---------------------------------------------------------------------------

@flow(name="Social Media Ingestion")
async def social_media_ingestion(
    platforms: Dict[str, Dict[str, Any]],
) -> None:
    """
    Orchestrate scraping and loading for one or more social media platforms.

    Each platform runs concurrently and independently — a failure in one
    does not affect others.

    Args:
        platforms: Dict keyed by platform name, each value is a config dict with:
            - 'target' (str, required): username or user ID to scrape
            - any platform-specific count kwargs (e.g. video_count, post_count, comment_count)

    Example:
        Run TikTok only::

            social_media_ingestion(platforms={
                "tiktok": {"target": "someuser", "video_count": 10, "comment_count": 5},
            })

        Run Instagram only::

            social_media_ingestion(platforms={
                "instagram": {"target": "someuser", "post_count": 10, "comment_count": 5},
            })

        Run all platforms::

            social_media_ingestion(platforms={
                "tiktok":    {"target": "someuser", "video_count": 10, "comment_count": 5},
                "instagram": {"target": "someuser", "post_count": 10, "comment_count": 5},
            })
    """
    if not platforms:
        logger.warning("No platforms provided — nothing to ingest.")
        return

    unknown = set(platforms) - set(PLATFORM_REGISTRY)
    if unknown:
        logger.warning(f"Unknown platform(s) will be skipped: {unknown}")

    pipeline_tasks = []
    for platform, config in platforms.items():
        if platform not in PLATFORM_REGISTRY:
            continue

        target = config.get("target")
        if not target:
            logger.warning(f"[{platform}] Missing 'target' in config — skipping.")
            continue

        scrape_kwargs = {k: v for k, v in config.items() if k != "target"}
        logger.info(f"[{platform}] Queueing ingestion for target: {target}")

        pipeline_tasks.append(
            _run_platform(platform=platform, target=target, scrape_kwargs=scrape_kwargs)
        )

    await asyncio.gather(*pipeline_tasks)
    logger.info("All platform ingestion tasks completed.")


# ---------------------------------------------------------------------------
# Entrypoint examples
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # --- Run all platforms ---
    # social_media_ingestion.serve(
    #     name="social-media-ingestion-all",
    #     parameters={
    #         "platforms": {
    #             "tiktok": {
    #                 "target": "gibran_rakabuming",
    #                 "video_count": 10,
    #                 "comment_count": 10,
    #             },
    #             "instagram": {
    #                 "target": "gibran_rakabuming",
    #                 "post_count": 10,
    #                 "comment_count": 10,
    #             },
    #         }
    #     },
    # )

    # --- Run TikTok only ---
    # social_media_ingestion.serve(
    #     name="social-media-ingestion-tiktok",
    #     parameters={
    #         "platforms": {
    #             "tiktok": {
    #                 "target": "gibran_rakabuming",
    #                 "video_count": 10,
    #                 "comment_count": 10,
    #             },
    #         }
    #     },
    # )

    # --- Run Instagram only ---
    social_media_ingestion.serve(
        name="social-media-ingestion-instagram",
        parameters={
            "platforms": {
                "instagram": {
                    "target": "gibran_rakabuming",
                    "post_count": 10,
                    "comment_count": 10,
                },
            }
        },
    )