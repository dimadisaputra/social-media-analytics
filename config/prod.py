from .base import Settings

class ProdSettings(Settings):
    debug: bool = False
    log_level: str = "WARNING"
