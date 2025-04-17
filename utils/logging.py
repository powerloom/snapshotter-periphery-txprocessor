import sys
import os
from loguru import logger

# Ensure logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Remove default handler
logger.remove()

# Console handler - always add with default INFO level
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True,
)

def configure_file_logging(write_to_files: bool = True, log_level: str = "INFO", debug_mode: bool = False):
    """Configure file-based logging based on settings."""
    log_level = log_level.upper()
    
    if write_to_files:
        log_file_map = {
            "DEBUG": os.path.join(LOGS_DIR, "debug.log"),
            "INFO": os.path.join(LOGS_DIR, "info.log"),
            "WARNING": os.path.join(LOGS_DIR, "warning.log"),
            "ERROR": os.path.join(LOGS_DIR, "error.log"),
            "CRITICAL": os.path.join(LOGS_DIR, "critical.log"),
        }

        # Add handlers for each level >= configured LOG_LEVEL
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        log_level_index = levels.index(log_level)

        for level_name in levels[log_level_index:]:
            logger.add(
                log_file_map[level_name],
                level=level_name,
                rotation="10 MB",
                retention="7 days",
                compression="zip",
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
                filter=lambda record: record["level"].name == level_name
            )

    logger.info(f"Logger configured. Level: {log_level}, Write to files: {write_to_files}")

    if debug_mode:
        logger.debug("Debug mode enabled.")

# Export the configured logger and configuration function
__all__ = ["logger", "configure_file_logging"]
