from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_env: str = "dev"
    tiktok_ms_token: str
    tiktok_browser: str
    tiktok_headless: bool
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
