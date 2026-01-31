from .base import Settings

class DevSettings(Settings):
    debug: bool = True
    log_level: str = "DEBUG"