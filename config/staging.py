from .base import Settings

class StagingSettings(Settings):
    debug: bool = False
    log_level: str = "INFO"