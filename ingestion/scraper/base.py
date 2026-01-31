from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseScraper(ABC):

    @abstractmethod
    async def get_user(self, user_id: str) -> Dict[str, Any]:
        """Fetch user profile information."""
        pass

    @abstractmethod
    async def get_post(self, post_id: str) -> Dict[str, Any]:
        """Fetch a specific post's details."""
        pass

    @abstractmethod
    async def get_comments(self, post_id: str) -> List[Dict[str, Any]]:
        """Fetch comments for a specific post."""
        pass