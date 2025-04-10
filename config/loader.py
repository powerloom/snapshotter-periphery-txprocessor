import json
from functools import lru_cache
from utils.models.settings_model import Settings
import os

CONFIG_DIR = os.path.dirname(__file__)
SETTINGS_FILE = os.path.join(CONFIG_DIR, 'settings.json')

@lru_cache
def get_core_config() -> Settings:
    """Load settings from the settings.json file."""
    if not os.path.exists(SETTINGS_FILE):
         raise RuntimeError(f"Settings file not found at {SETTINGS_FILE}. Ensure the entrypoint script has run.")
    try:
        with open(SETTINGS_FILE, 'r') as f:
            settings_dict = json.load(f)
        return Settings(**settings_dict)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Error decoding settings file ({SETTINGS_FILE}): {str(e)}")
    except Exception as e:
         raise RuntimeError(f"Error loading settings from {SETTINGS_FILE}: {str(e)}")
