from prefect import flow, task
from ingestion.scraper.tiktok import TiktokScraper
from ingestion.loaders.snowflake import SnowflakeLoader
from loguru import logger


@task(name="Scrape TikTok Data", retries=3, retry_delay_seconds=30)
async def scrape_tiktok_data(user_id: str, video_count: int, comment_count: int):
    scraper = TiktokScraper()
    await scraper.create_session()
    
    data = {"user": None, "posts": [], "comments": []}
    
    try:
        # user_info = await scraper.get_user(user_id)
        # data["user"] = user_info
        
        posts = await scraper.get_posts(user_id, video_count)
        data["posts"] = posts
        
        for post in posts:
            post_id = post["id"]
            comments = await scraper.get_comments(post_id, comment_count)
            for comment in comments:
                comment["_related_post_id"] = post_id
            data["comments"].extend(comments)
            
    finally:
        await scraper.cleanup()
        
    return data

@task(name="Load to Snowflake")
def load_to_snowflake(data):
    loader = SnowflakeLoader()
    events = []
    
    def get_safe_username(item):
        return item.get("user", {}).get("uniqueId") or item.get("uniqueId") or item.get("author", {}).get("uniqueId") or item.get("user", {}).get("unique_id") or "unknown"

    if data["user"]:
        u = data["user"]
        events.append({
            "event_id": "tiktok_user_" + (u.get("id") or u.get("user", {}).get("id")),
            "platform": "tiktok",
            "username": get_safe_username(u),
            "entity_type": "user",
            "raw_payload": u
        })
        
    for p in data["posts"]:
        events.append({
            "event_id": "tiktok_post_" + p.get("id"),
            "platform": "tiktok",
            "username": get_safe_username(p),
            "entity_type": "post",
            "raw_payload": p
        })
        
    for c in data["comments"]:
        events.append({
            "event_id": "tiktok_comment_" + c.get("cid"),
            "platform": "tiktok",
            "username": get_safe_username(c),
            "entity_type": "comment",
            "raw_payload": c
        })
    
    print(f"Total events built: {len(events)}")

    if events:
        loader = SnowflakeLoader()
        loader.load_events(events)
        loader.close()

    else:
        logger.warning("No events to load")

@flow
async def social_media_ingestion(user_id: str, video_count: int = 10, comment_count: int = 10):
    scraped_data = await scrape_tiktok_data(user_id, video_count, comment_count)
    load_to_snowflake(scraped_data)

if __name__ == "__main__":
    import asyncio

    social_media_ingestion.serve(
        name="social-media-ingestion",
        parameters={
            "user_id": "gibran_rakabuming",
            "video_count": 10,
            "comment_count": 10
        }
    )
