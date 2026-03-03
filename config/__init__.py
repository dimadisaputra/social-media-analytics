import os
import sys
from dotenv import load_dotenv
from loguru import logger
from .base import Settings
from .dev import DevSettings
from .staging import StagingSettings
from .prod import ProdSettings

load_dotenv()

env = os.getenv("APP_ENV", "dev")

if env == "staging":
    settings = StagingSettings()
elif env == "prod":
    settings = ProdSettings()
else:
    settings = DevSettings()

# Configure loguru global logger
logger.remove()
logger.add(
    sys.stderr,
    level=settings.log_level,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    diagnose=settings.debug,
    backtrace=settings.debug,
)