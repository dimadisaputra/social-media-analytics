import os
import asyncio
import json
from config import settings
from .base import BaseScraper
from TikTokApi import TikTokApi
from proxyproviders import ProxyProvider, Webshare
from proxyproviders.algorithms import RoundRobin


class TiktokScraper(BaseScraper):
    def __init__(self):
        self.api = TikTokApi()
        self.session = False
        self.proxy_provider = Webshare(api_key=settings.webshare_api_key)

    async def create_session(self):
        await self.api.create_sessions(
            ms_tokens=[settings.tiktok_ms_token],
            num_sessions=1,
            sleep_after=30,
            headless=settings.tiktok_headless,
            browser=settings.tiktok_browser,
            # proxy_provider=self.proxy_provider,
            # proxy_algorithm=RoundRobin(),
            timeout=60000
        )
        self.session = True

    async def cleanup(self):
        await self.api.close_sessions()
        await self.api.stop_playwright()
    
    async def get_user(self, user_id: str, video_count: int = 10):
        if not self.session:
            await self.create_session()
        
        try:
            user = self.api.user(username=user_id)
            user_info = await user.info()
            
            user_videos = []
            async for video in user.videos(count=video_count):
                user_videos.append(video.as_dict)

            return {
                "user_info": user_info,
                "user_videos": user_videos
            }

        except KeyError as e:
            print(f"\nKeyError: {e}")
            print(f"This usually means the user doesn't exist or TikTok returned unexpected data.")
            print(f"Please verify the username '{user_id}' exists on TikTok.")
            raise

        except Exception as e:
            print(f"\nError fetching user data: {e}")
            raise

    async def get_post(self, post_id: str):
        pass

    async def get_comments(self, post_id: str, comment_count: int = 10):
        if not self.session:
            await self.create_session()
        
        try:
            post = self.api.video(id=post_id)

            comments = []
            async for comment in post.comments(count=comment_count):
                comments.append(comment.as_dict)

            return comments

        except Exception as e:
            print(f"\nError fetching comments: {e}")
            raise