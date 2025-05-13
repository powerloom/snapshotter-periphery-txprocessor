import os
import json
from functools import lru_cache
from pathlib import Path
from utils.models.settings_model import Settings, PreloaderConfig
from utils.models.data_models import EventFiltersConfig
from utils.logging import logger

CONFIG_DIR = os.path.dirname(__file__)
SETTINGS_FILE = os.path.join(CONFIG_DIR, 'settings.json')
PRELOADER_CONFIG_FILE = os.path.join(CONFIG_DIR, 'preloaders.json')
EVENT_FILTER_CONFIG_PATH = os.getenv('EVENT_FILTER_CONFIG_PATH', '/app/shared_config/event_filters.example.json')

_logger = logger.bind(module='ConfigLoader')

@lru_cache()
def get_core_config() -> Settings:
    """Load settings from the settings.json file."""
    _logger.info(f"📖 Loading settings from: {SETTINGS_FILE}")
    if not os.path.exists(SETTINGS_FILE):
        _logger.error(f"❌ Settings file not found at {SETTINGS_FILE}")
        raise RuntimeError(f"Settings file not found at {SETTINGS_FILE}. Ensure the entrypoint script has run.")
    try:
        with open(SETTINGS_FILE, 'r') as f:
            settings_dict = json.load(f)
        settings = Settings(**settings_dict)
        _logger.success("✅ Successfully loaded settings")
        return settings
    except json.JSONDecodeError as e:
        _logger.error(f"❌ Error decoding settings file: {e}")
        raise RuntimeError(f"Error decoding settings file ({SETTINGS_FILE}): {str(e)}")
    except Exception as e:
        _logger.error(f"❌ Error loading settings: {e}")
        raise RuntimeError(f"Error loading settings from {SETTINGS_FILE}: {str(e)}")

@lru_cache()
def get_preloader_config() -> PreloaderConfig:
    """Load preloader configuration from the preloaders.json file."""
    _logger.info(f"📖 Loading preloader config from: {PRELOADER_CONFIG_FILE}")
    if not os.path.exists(PRELOADER_CONFIG_FILE):
        _logger.error(f"❌ Preloader config file not found")
        raise RuntimeError(f"Preloader config file not found at {PRELOADER_CONFIG_FILE}.")
    try:
        with open(PRELOADER_CONFIG_FILE, 'r') as f:
            config_dict = json.load(f)
        config = PreloaderConfig(**config_dict)
        _logger.success(f"✅ Successfully loaded {len(config.preloaders)} preloader configurations")
        return config
    except json.JSONDecodeError as e:
        _logger.error(f"❌ Error decoding preloader config: {e}")
        raise RuntimeError(f"Error decoding preloader config file ({PRELOADER_CONFIG_FILE}): {str(e)}")
    except Exception as e:
        _logger.error(f"❌ Error loading preloader config: {e}")
        raise RuntimeError(f"Error loading preloader config from {PRELOADER_CONFIG_FILE}: {str(e)}")

@lru_cache()
def get_event_filter_config() -> EventFiltersConfig:
    """Load event filter configuration, expecting event_topics."""
    service_root = Path(__file__).parent.parent
    # Resolve potential relative paths from workspace root
    if not EVENT_FILTER_CONFIG_PATH.startswith('/'):
        # Assuming workspace root is one level above service root
        workspace_root = service_root.parent 
        full_config_path = (workspace_root / EVENT_FILTER_CONFIG_PATH).resolve()
    else:
        full_config_path = Path(EVENT_FILTER_CONFIG_PATH)
    
    _logger.info(f"📖 Loading event filter config from: {full_config_path}")
    if not full_config_path.exists():
        _logger.error(f"❌ Event filter config file not found at {full_config_path}")
        # Provide a helpful default path if not found
        default_path = (service_root.parent / 'config/event_filters.example.json').resolve()
        _logger.info(f"ℹ️ Default path would be: {default_path}")
        raise RuntimeError(f"Event filter config file not found at {full_config_path}. Check EVENT_FILTER_CONFIG_PATH env var.")

    try:
        with open(full_config_path, 'r') as f:
            config_dict = json.load(f)

        config = EventFiltersConfig(**config_dict)

        return config
    except json.JSONDecodeError as e:
        _logger.error(f"❌ Error decoding event filter config file '{full_config_path}': {e}")
        raise RuntimeError(f"Error decoding event filter config file '{full_config_path}': {str(e)}")
    except Exception as e: # Catch Pydantic ValidationError etc.
        _logger.error(f"❌ Error loading/validating event filter config '{full_config_path}': {type(e).__name__} - {e}")
        raise RuntimeError(f"Error loading/validating event filter config from {full_config_path}: {str(e)}")
