import os
from dotenv import load_dotenv
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