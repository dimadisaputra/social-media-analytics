import json
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator, Field
from typing import List, Optional

class Settings(BaseSettings):
    app_env: str = "dev"
    tiktok_ms_token_file: Optional[str] = None
    tiktok_ms_token: Optional[str] = None
    tiktok_ms_tokens: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def populate_ms_tokens(self):
        ms_token_path = Path(self.tiktok_ms_token_file)
        if not self.tiktok_ms_tokens and ms_token_path.exists():
            with open(ms_token_path, "r") as f:
                self.tiktok_ms_tokens = json.load(f)
        
        if not self.tiktok_ms_tokens and self.tiktok_ms_token:
            self.tiktok_ms_tokens = [self.tiktok_ms_token]
        return self

    tiktok_browser: str = "chromium"
    tiktok_headless: bool = True
    tiktok_use_proxy: bool = False
    tiktok_session_sleep_after: int = 30
    tiktok_min_delay: float = 10.0
    tiktok_max_delay: float = 15.0
    tiktok_max_retries: int = 3
    tiktok_bot_detection_cooldown_short: int = 60
    tiktok_bot_detection_cooldown: int = 300

    webshare_api_key: str

    instagram_username: str = ""
    instagram_password: str = ""
    instagram_2fa_secret: str | None = None
    
    snowflake_account: str
    snowflake_user: str
    snowflake_private_key_path: str
    snowflake_database: str = "SOCIAL_MEDIA_DW"
    snowflake_bronze_schema: str = "BRONZE"
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_role: str | None = None

    debug: bool = False
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )
