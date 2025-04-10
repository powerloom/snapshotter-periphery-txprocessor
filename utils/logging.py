import sys
import os
from loguru import logger
from config.loader import get_core_config # Adjusted import path

# Ensure logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs') # Assumes logs dir is one level up from utils
os.makedirs(LOGS_DIR, exist_ok=True)

# Load settings safely
try:
    settings = get_core_config().logs
    LOG_LEVEL = settings.level.upper()
    WRITE_TO_FILES = settings.write_to_files
    DEBUG_MODE = settings.debug_mode
except Exception as e:
    # Fallback defaults if config loading fails during setup
    print(f"Warning: Could not load logging settings, using defaults. Error: {e}", file=sys.stderr)
    LOG_LEVEL = "INFO"
    WRITE_TO_FILES = True
    DEBUG_MODE = False


# Remove default handler
logger.remove()

# Console handler
logger.add(
    sys.stderr,
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True,
)

# File handlers (optional based on config)
if WRITE_TO_FILES:
    log_file_map = {
        "DEBUG": os.path.join(LOGS_DIR, "debug.log"),
        "INFO": os.path.join(LOGS_DIR, "info.log"),
        "WARNING": os.path.join(LOGS_DIR, "warning.log"),
        "ERROR": os.path.join(LOGS_DIR, "error.log"),
        "CRITICAL": os.path.join(LOGS_DIR, "critical.log"),
    }

    # Add handlers for each level >= configured LOG_LEVEL
    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    log_level_index = levels.index(LOG_LEVEL)

    for level_name in levels[log_level_index:]:
        logger.add(
            log_file_map[level_name],
            level=level_name,
            rotation="10 MB",  # Rotate file when it reaches 10MB
            retention="7 days",  # Keep logs for 7 days
            compression="zip",  # Compress rotated files
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            filter=lambda record: record["level"].name == level_name # Only log messages of this exact level
        )

logger.info(f"Logger initialized. Level: {LOG_LEVEL}, Write to files: {WRITE_TO_FILES}")

# Add specific debug logging if needed
if DEBUG_MODE:
    logger.debug("Debug mode enabled.")
