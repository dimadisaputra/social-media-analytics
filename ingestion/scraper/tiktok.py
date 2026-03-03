import asyncio
import random
from typing import Any, Dict, List, Optional

from TikTokApi import TikTokApi
from TikTokApi.exceptions import EmptyResponseException, NotFoundException
from config import settings
from loguru import logger

from .base import BaseScraper

if settings.tiktok_use_proxy:
    from proxyproviders import Webshare
    from proxyproviders.algorithms import RoundRobin


class TiktokScraper(BaseScraper):
    """
    Scraper implementation for TikTok using TikTokApi with Playwright sessions.

    Bot detection mitigation strategy:
    - Random delays between requests
    - Per-endpoint cooldown tracking
    - Full session + API object recreation on persistent bot detection
    - ms_token rotation if multiple tokens configured
    - Graceful skip on permanently blocked content
    """

    def __init__(self):
        self._init_api()
        self.session = False
        self._ms_token_index = 0
        logger.debug(f"TiktokScraper initialized. Proxy: {settings.tiktok_use_proxy}")

    def _init_api(self):
        """Create a fresh TikTokApi instance. Called on init and full reset."""
        self.api = TikTokApi()
        self.proxy_provider = (
            Webshare(api_key=settings.webshare_api_key)
            if settings.tiktok_use_proxy
            else None
        )

    def _next_ms_token(self) -> str:
        """
        Rotate to the next available ms_token.

        If multiple tokens are configured, cycles through them to reduce
        the chance of a single token being flagged.

        Returns:
            The next ms_token string to use.
        """
        tokens = settings.tiktok_ms_tokens  # List[str], falls back to [settings.tiktok_ms_token]
        token = tokens[self._ms_token_index % len(tokens)]
        self._ms_token_index += 1
        return token

    async def create_session(self):
        """
        Create a fresh TikTok browser session.

        Closes any existing session before creating a new one.
        Rotates ms_token on each call to reduce bot fingerprinting.
        """
        if self.session:
            logger.info("Closing existing session before recreating...")
            try:
                await self.api.close_sessions()
            except Exception as e:
                logger.warning(f"Error closing sessions (ignoring): {e}")
            self.session = False

        token = self._next_ms_token()
        logger.info(f"Creating TikTok session (token index {self._ms_token_index - 1})...")

        session_kwargs = dict(
            ms_tokens=[token],
            num_sessions=1,
            sleep_after=settings.tiktok_session_sleep_after,
            headless=settings.tiktok_headless,
            browser=settings.tiktok_browser,
            timeout=60000,
        )

        if self.proxy_provider:
            session_kwargs["proxy_provider"] = self.proxy_provider
            session_kwargs["proxy_algorithm"] = RoundRobin()

        await self.api.create_sessions(**session_kwargs)
        self.session = True
        logger.success("TikTok session created successfully.")

    async def _full_reset(self, label: str):
        """
        Perform a complete teardown and rebuild of the API + session.

        More aggressive than just recreating the session — destroys the entire
        TikTokApi object and Playwright instance, then rebuilds from scratch.
        Used when bot detection persists across session recreations.

        Args:
            label: Operation label for logging context.
        """
        logger.warning(f"[{label}] Performing full API reset (stop Playwright + new TikTokApi instance)...")
        try:
            await self.api.close_sessions()
            await self.api.stop_playwright()
        except Exception as e:
            logger.warning(f"[{label}] Error during full reset teardown (ignoring): {e}")

        self.session = False
        self._init_api()  # Brand new TikTokApi + proxy_provider instance
        logger.info(f"[{label}] API reset complete. Creating new session...")
        await self.create_session()

    async def cleanup(self):
        """Close all active TikTok sessions and stop Playwright."""
        logger.info("Cleaning up TikTok sessions...")
        try:
            await self.api.close_sessions()
            await self.api.stop_playwright()
        except Exception as e:
            logger.warning(f"Cleanup error (ignoring): {e}")
        self.session = False
        logger.debug("TikTok sessions closed and Playwright stopped.")

    async def _pause(self, label: str = "request"):
        """Random delay between normal requests to appear more human."""
        delay = random.uniform(settings.tiktok_min_delay, settings.tiktok_max_delay)
        logger.debug(f"Pausing {delay:.2f}s before {label}...")
        await asyncio.sleep(delay)

    async def _bot_detection_cooldown(self, attempt: int, label: str):
        """
        Handle bot detection with escalating response based on attempt number.

        Attempt 1 → short cooldown + recreate session (rotate ms_token)
        Attempt 2+ → long cooldown + full API reset (new Playwright + ms_token)

        The escalation ensures we don't waste 5 minutes on the first failure,
        but apply maximum reset when the first recovery attempt also fails.

        Args:
            attempt: Which attempt just failed (1-indexed).
            label: Operation label for logging.
        """
        if attempt == 1:
            # First bot detection: short cooldown + session recreation
            cooldown = settings.tiktok_bot_detection_cooldown_short
            logger.warning(
                f"[{label}] Bot detection on attempt {attempt}. "
                f"Short cooldown {cooldown}s then recreating session..."
            )
            await asyncio.sleep(cooldown)
            await self.create_session()
        else:
            # Persistent bot detection: long cooldown + full teardown
            cooldown = settings.tiktok_bot_detection_cooldown
            logger.warning(
                f"[{label}] Persistent bot detection on attempt {attempt}. "
                f"Long cooldown {cooldown}s then full API reset..."
            )
            await asyncio.sleep(cooldown)
            await self._full_reset(label)

    async def _with_retry(self, label: str, coro_fn):
        """
        Execute an async callable with retry logic.

        Handles two error classes differently:
        - EmptyResponseException → bot detection path (cooldown + session/API reset)
        - Other exceptions → standard exponential backoff

        NotFoundException is NOT retried — content is gone permanently.

        Args:
            label: Human-readable name for the operation.
            coro_fn: Zero-argument async callable to execute.

        Returns:
            Result of the successful coroutine, or None if content not found.

        Raises:
            EmptyResponseException: If bot detection persists after all retries.
            Exception: Last exception if all retries are exhausted.
        """
        last_exception: Optional[Exception] = None

        for attempt in range(1, settings.tiktok_max_retries + 1):
            try:
                await self._pause(label)
                return await coro_fn()

            except NotFoundException as e:
                # Content deleted/private — no point retrying
                logger.warning(f"[{label}] Content not found (skipping): {e}")
                return None

            except EmptyResponseException as e:
                last_exception = e
                logger.warning(
                    f"[{label}] Attempt {attempt}/{settings.tiktok_max_retries} "
                    f"failed: bot detection (EmptyResponseException)."
                )
                if attempt < settings.tiktok_max_retries:
                    await self._bot_detection_cooldown(attempt, label)
                else:
                    logger.error(
                        f"[{label}] Bot detection persisted after all retries. "
                        f"Check your ms_token freshness and consider enabling proxy."
                    )

            except Exception as e:
                last_exception = e
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    f"[{label}] Attempt {attempt}/{settings.tiktok_max_retries} failed: {e}. "
                    f"Retrying in {wait:.2f}s..."
                )
                if attempt < settings.tiktok_max_retries:
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"[{label}] All retries exhausted.")

        raise last_exception

    async def get_user(self, user_id: str, video_count: int = 10) -> Dict[str, Any]:
        """Fetch profile information for a TikTok user."""
        if not self.session:
            await self.create_session()
        logger.info(f"Fetching TikTok user info for: {user_id}")

        async def _fetch():
            user = self.api.user(username=user_id)
            info = await user.info()
            logger.success(f"Successfully fetched user info for: {user_id}")
            return info

        return await self._with_retry(f"get_user:{user_id}", _fetch)

    async def get_posts(self, user_id: str, video_count: int = 10) -> List[Dict[str, Any]]:
        """Fetch recent videos posted by a TikTok user."""
        if not self.session:
            await self.create_session()
        logger.info(f"Fetching up to {video_count} videos for TikTok user: {user_id}")

        async def _fetch():
            user = self.api.user(username=user_id)
            videos = []
            async for video in user.videos(count=video_count):
                videos.append(video.as_dict)
                if len(videos) >= video_count:
                    break
            logger.success(f"Fetched {len(videos)} videos for user: {user_id}")
            return videos[:video_count]

        return await self._with_retry(f"get_posts:{user_id}", _fetch)

    async def get_comments(self, post_id: str, comment_count: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch comments on a specific TikTok video.

        Returns an empty list (instead of raising) if the content is not found
        or permanently blocked, allowing the caller to continue with other posts.
        """
        if not self.session:
            await self.create_session()
        logger.info(f"Fetching up to {comment_count} comments for post ID: {post_id}")

        async def _fetch():
            post = self.api.video(id=post_id)
            comments = []
            async for comment in post.comments(count=comment_count):
                comments.append(comment.as_dict)
                if len(comments) >= comment_count:
                    break
            logger.success(f"Fetched {len(comments)} comments for post ID: {post_id}")
            return comments[:comment_count]

        result = await self._with_retry(f"get_comments:{post_id}", _fetch)
        return result if result is not None else []