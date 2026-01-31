import json
import os
import asyncio
from ingestion.scraper.tiktok import TiktokScraper

async def main():
    print("Hello from social-media-analytics!")
    scraper = TiktokScraper()
    try:
        # username = "gibran_rakabuming"
        # print(f"Fetching user: {username}")
        # user_data = await scraper.get_user(username, user_video_count=30)
        
        # # Ensure data directory exists
        # os.makedirs("data", exist_ok=True)
        
        # # Save results to JSON
        # file_path = f"data/{username}.json"
        # with open(file_path, "w", encoding="utf-8") as f:
        #     json.dump(user_data, f, indent=4, ensure_ascii=False)
            
        # print(f"Successfully saved user data to {file_path}")

        post_id = "7600422668290051348"
        print(f"Fetching comments: {post_id}")
        comments = await scraper.get_comments(post_id=post_id, comment_count=10)
        
        # Ensure data directory exists
        os.makedirs("data", exist_ok=True)
        
        # Save results to JSON
        file_path = f"data/{post_id}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(comments, f, indent=4, ensure_ascii=False)
            
        print(f"Successfully saved comments data to {file_path}")
    finally:
        # Ensure cleanup happens even if there's an error
        await scraper.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
