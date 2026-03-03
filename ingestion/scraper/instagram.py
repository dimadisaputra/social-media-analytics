import asyncio
import json
from itertools import islice
from typing import Any, Dict, List

import instaloader
import pyotp
from instaloader.exceptions import TwoFactorAuthRequiredException
from loguru import logger

from config import settings
from .base import BaseScraper


class InstagramScraper(BaseScraper):
    """
    Scraper implementation for Instagram using Instaloader.

    Handles session persistence to avoid repeated logins, with optional
    2FA support via TOTP. Sessions are lazily initialized on first API call.
    
    Uses asyncio.to_thread() to prevent Instaloader's synchronous I/O 
    from blocking the main async event loop, allowing for proper task cancellation.
    """

    def __init__(self):
        """Initialize InstagramScraper with an Instaloader instance."""
        self.loader = instaloader.Instaloader()
        self.session = False
        logger.debug("InstagramScraper initialized.")

    async def create_session(self):
        """
        Authenticate with Instagram and persist the session to disk.
        Executes synchronously in a background thread to prevent blocking.
        """
        def _sync_create_session():
            logger.info(f"Creating Instagram session for user: {settings.instagram_username}")
            try:
                self.loader.load_session_from_file(settings.instagram_username)
                logger.success("Loaded existing Instagram session from file.")
            except FileNotFoundError:
                logger.info("No saved session found. Performing fresh login...")
                try:
                    self.loader.login(settings.instagram_username, settings.instagram_password)
                except TwoFactorAuthRequiredException:
                    logger.info("2FA required. Generating TOTP code...")
                    totp = pyotp.TOTP(settings.instagram_2fa_secret)
                    self.loader.two_factor_login(totp.now())
                    logger.success("2FA login successful.")
                self.loader.save_session_to_file()
                logger.debug("Instagram session saved to file.")

        await asyncio.to_thread(_sync_create_session)
        self.session = True

    async def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Fetch profile information for an Instagram user.
        """
        if not self.session:
            await self.create_session()

        def _sync_get_user():
            logger.info(f"Fetching Instagram profile for: {user_id}")
            try:
                profile = instaloader.Profile.from_username(self.loader.context, user_id)
                logger.success(f"Successfully fetched profile for: {user_id}")
                return profile._node
            except Exception as e:
                logger.error(f"Error fetching user '{user_id}': {e}")
                raise

        return await asyncio.to_thread(_sync_get_user)

    async def get_posts(self, user_id: str, post_count: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch recent posts from an Instagram user's profile.
        """
        if not self.session:
            await self.create_session()

        def _sync_get_posts():
            logger.info(f"Fetching up to {post_count} posts for Instagram user: {user_id}")
            try:
                profile = instaloader.Profile.from_username(self.loader.context, user_id)
                user_data = profile._node
                posts = []
                for post in islice(profile.get_posts(), post_count):
                    post_data = post._node
                    post_data["user"] = user_data
                    posts.append(post_data)

                logger.success(f"Fetched {len(posts)} posts for user: {user_id}")
                return posts
            except Exception as e:
                logger.error(f"Error fetching posts for user '{user_id}': {e}")
                raise

        return await asyncio.to_thread(_sync_get_posts)

    async def get_comments(self, post_id: str, comment_count: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch comments on a specific Instagram post.
        """
        if not self.session:
            await self.create_session()

        def _sync_get_comments():
            logger.info(f"Fetching up to {comment_count} comments for post shortcode: {post_id}")
            try:
                post = instaloader.Post.from_shortcode(self.loader.context, post_id)
                comments = []
                for comment in islice(post.get_comments(), comment_count):
                    comments.append(comment._node)

                logger.success(f"Fetched {len(comments)} comments for post: {post_id}")
                return comments
            except Exception as e:
                logger.error(f"Error fetching comments for post '{post_id}': {e}")
                raise

        return await asyncio.to_thread(_sync_get_comments)