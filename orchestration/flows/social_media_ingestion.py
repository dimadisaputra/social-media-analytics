from prefect import flow, task
from ingestion.scraper.tiktok import TiktokScraper


@task
async def get_user(user_id: str):
    scraper = TiktokScraper()
    return await scraper.get_user(user_id)

@task
async def get_post(post_id: str):
    scraper = TiktokScraper()
    return await scraper.get_post(post_id)

@task
async def get_comments(post_id: str):
    scraper = TiktokScraper()
    return await scraper.get_comments(post_id)

@flow
async def social_media_ingestion(user_id: str):
    user = await get_user(user_id)
    # post = await get_post(post_id)
    # comments = await get_comments(post_id)
    print(user)
    return user

if __name__ == "__main__":
    import asyncio
    # For async flows, we can use serve to register it with the UI
    social_media_ingestion.serve(
        name="social-media-ingestion",
        parameters={
            "user_id": "dimadisaputra",
        }
    )
