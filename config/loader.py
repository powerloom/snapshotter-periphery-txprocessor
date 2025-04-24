import os
import json
from functools import lru_cache
from pathlib import Path
from utils.models.settings_model import Settings, PreloaderConfig, EventFiltersConfig
from utils.logging import logger

CONFIG_DIR = os.path.dirname(__file__)
SETTINGS_FILE = os.path.join(CONFIG_DIR, 'settings.json')
PRELOADER_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'preloaders.json')
EVENT_FILTER_CONFIG_PATH = os.getenv('EVENT_FILTER_CONFIG_PATH', 'config/event_filters.example.json')

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

        workspace_root = service_root.parent
        for filter_def in config.filters:
             # Ensure address_source and config_file exist before proceeding
             if not hasattr(filter_def, 'address_source') or not hasattr(filter_def.address_source, 'config_file'):
                 _logger.warning(f"⚠️ Filter '{filter_def.filter_name}' is missing address_source configuration. Skipping address loading.")
                 continue

             projects_config_path_str = filter_def.address_source.config_file
             # Resolve project config path relative to workspace root
             if not projects_config_path_str.startswith('/'):
                 projects_config_path = (workspace_root / projects_config_path_str).resolve()
             else:
                 projects_config_path = Path(projects_config_path_str)

             if not projects_config_path.exists():
                 _logger.error(f"❌ Projects config file not found for filter '{filter_def.filter_name}': {projects_config_path}")
                 # Decide behaviour: skip filter or raise error
                 raise RuntimeError(f"Projects config file '{projects_config_path}' not found for filter '{filter_def.filter_name}'.")
             
             try:
                 with open(projects_config_path, 'r') as f:
                     projects_data = json.load(f)
             except json.JSONDecodeError as e:
                  _logger.error(f"❌ Error decoding projects config file '{projects_config_path}': {e}")
                  raise RuntimeError(f"Error decoding projects config file '{projects_config_path}': {e}")

             all_addresses = set()
             if 'config' in projects_data and isinstance(projects_data['config'], list):
                 for project_entry in projects_data['config']:
                     if 'projects' in project_entry and isinstance(project_entry['projects'], list):
                         # Ensure addresses are strings and lowercase for consistency
                         valid_addresses = {str(addr).lower() for addr in project_entry['projects'] if isinstance(addr, str)}
                         all_addresses.update(valid_addresses)
             
             filter_def.target_addresses = list(all_addresses)
             _logger.info(f"  Loaded {len(filter_def.target_addresses)} unique target addresses for filter '{filter_def.filter_name}' from {projects_config_path}")
        
        _logger.success(f"✅ Successfully loaded {len(config.filters)} event filter configurations")
        return config
    except json.JSONDecodeError as e:
        _logger.error(f"❌ Error decoding event filter config file '{full_config_path}': {e}")
        raise RuntimeError(f"Error decoding event filter config file '{full_config_path}': {str(e)}")
    except Exception as e: # Catch Pydantic ValidationError etc.
        _logger.error(f"❌ Error loading/validating event filter config '{full_config_path}': {type(e).__name__} - {e}")
        raise RuntimeError(f"Error loading/validating event filter config from {full_config_path}: {str(e)}")
