import asyncio
import json
import os
from typing import Any, Dict, List

from config import settings
from loguru import logger
from proxyproviders import ProxyProvider, Webshare
from proxyproviders.algorithms import RoundRobin
from TikTokApi import TikTokApi

from .base import BaseScraper


class TiktokScraper(BaseScraper):
    """
    Scraper implementation for TikTok using TikTokApi with Playwright sessions.

    Manages browser sessions with proxy rotation via Webshare and RoundRobin algorithm.
    Sessions are lazily initialized on first API call.
    """

    def __init__(self):
        """Initialize TiktokScraper with API client and Webshare proxy provider."""
        self.api = TikTokApi()
        self.session = False
        self.proxy_provider = Webshare(api_key=settings.webshare_api_key)
        logger.debug("TiktokScraper initialized.")

    async def create_session(self):
        """
        Create and authenticate a TikTok browser session.

        Uses ms_token from settings for authentication, with headless browser
        and proxy rotation configured. Sleeps 30 seconds after session creation
        to avoid rate limiting.

        Raises:
            Exception: If session creation fails.
        """
        logger.info("Creating TikTok session...")
        await self.api.create_sessions(
            ms_tokens=[settings.tiktok_ms_token],
            num_sessions=1,
            sleep_after=30,
            headless=settings.tiktok_headless,
            browser=settings.tiktok_browser,
            proxy_provider=self.proxy_provider,
            proxy_algorithm=RoundRobin(),
            timeout=60000,
        )
        self.session = True
        logger.success("TikTok session created successfully.")

    async def cleanup(self):
        """
        Close all active TikTok sessions and stop Playwright.

        Should be called when the scraper is no longer needed to free resources.
        """
        logger.info("Cleaning up TikTok sessions...")
        await self.api.close_sessions()
        await self.api.stop_playwright()
        logger.debug("TikTok sessions closed and Playwright stopped.")

    async def get_user(self, user_id: str, video_count: int = 10) -> Dict[str, Any]:
        """
        Fetch profile information for a TikTok user.

        Args:
            user_id: TikTok username to look up.
            video_count: Unused parameter kept for interface consistency.

        Returns:
            Dict containing raw user profile data from TikTok API.

        Raises:
            Exception: If the user cannot be fetched or session fails.
        """
        if not self.session:
            await self.create_session()

        logger.info(f"Fetching TikTok user info for: {user_id}")
        try:
            user = self.api.user(username=user_id)
            user_info = await user.info()
            logger.success(f"Successfully fetched user info for: {user_id}")
            return user_info

        except Exception as e:
            logger.error(f"Error fetching user '{user_id}': {e}")
            raise

    async def get_posts(self, user_id: str, video_count: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch recent videos posted by a TikTok user.

        Args:
            user_id: TikTok username whose videos to retrieve.
            video_count: Maximum number of videos to fetch. Defaults to 10.

        Returns:
            List of dicts, each representing a video's raw metadata.

        Raises:
            Exception: If videos cannot be fetched or session fails.
        """
        if not self.session:
            await self.create_session()

        logger.info(f"Fetching up to {video_count} videos for TikTok user: {user_id}")
        try:
            user = self.api.user(username=user_id)
            videos = []
            async for video in user.videos(count=video_count):
                videos.append(video.as_dict)
                if len(videos) >= video_count:
                    break

            logger.success(f"Fetched {len(videos)} videos for user: {user_id}")
            return videos[:video_count]

        except Exception as e:
            logger.error(f"Error fetching posts for user '{user_id}': {e}")
            raise

    async def get_comments(self, post_id: str, comment_count: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch comments on a specific TikTok video.

        Args:
            post_id: Numeric TikTok video ID.
            comment_count: Maximum number of comments to fetch. Defaults to 10.

        Returns:
            List of dicts, each representing a comment's raw data.

        Raises:
            Exception: If comments cannot be fetched or session fails.
        """
        if not self.session:
            await self.create_session()

        logger.info(f"Fetching up to {comment_count} comments for post ID: {post_id}")
        try:
            post = self.api.video(id=post_id)
            comments = []
            async for comment in post.comments(count=comment_count):
                comments.append(comment.as_dict)
                if len(comments) >= comment_count:
                    break

            logger.success(f"Fetched {len(comments)} comments for post ID: {post_id}")
            return comments[:comment_count]

        except Exception as e:
            logger.error(f"Error fetching comments for post '{post_id}': {e}")
            raise