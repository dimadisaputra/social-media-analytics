# from ingestion.scraper.instagram import InstagramScraper
from ingestion.scraper.tiktok import TiktokScraper

import json
import os
import asyncio

async def test_scraper():
    scraper = TiktokScraper()
    username = "gadgetin"
    
    # Fetch user info
    user_data = await scraper.get_user(username)
    
    # Fetch posts
    posts = await scraper.get_posts(username, video_count=5)
    
    # Fetch comments separately
    all_comments = []
    for post in posts:
        post_id = post.get('id')
        comments = await scraper.get_comments(post_id)
        all_comments.extend(comments)

    # Ensure data directory exists
    os.makedirs('data/tt', exist_ok=True)
    
    # Save user data
    with open('data/tt/tiktok_user.json', 'w', encoding='utf-8') as f:
        json.dump(user_data, f, indent=4, ensure_ascii=False)
        
    # Save posts data
    with open('data/tt/tiktok_posts.json', 'w', encoding='utf-8') as f:
        json.dump(posts, f, indent=4, ensure_ascii=False)
        
    # Save comments data
    with open('data/tt/tiktok_comments.json', 'w', encoding='utf-8') as f:
        json.dump(all_comments, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(test_scraper())
