from pydantic import BaseModel, Field, validator
from rpc_helper.utils.models.settings_model import RPCConfigBase
from typing import Union, List, Dict, Optional

class RedisDataRetentionConfig(BaseModel):
    """Redis data retention configuration model."""
    max_blocks: int
    ttl_seconds: int

class Redis(BaseModel):
    """Redis configuration model."""
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False
    data_retention: RedisDataRetentionConfig

class Logs(BaseModel):
    """Logging configuration model."""
    debug_mode: bool = False
    write_to_files: bool = True
    level: str = "INFO"

class TxProcessorConfig(BaseModel):
    """Transaction Processor specific configuration."""
    redis_queue_key: str = 'pending_transactions'
    redis_block_timeout: int = 0  # 0 for blocking indefinitely

class Settings(BaseModel):
    """Main settings configuration model."""
    rpc: RPCConfigBase
    redis: Redis
    logs: Logs
    processor: TxProcessorConfig
    namespace: str

class Preloader(BaseModel):
    """Preloader configuration model."""
    task_type: str
    module: str
    class_name: str

class PreloaderConfig(BaseModel):
    """Preloader configuration model."""
    preloaders: List[Preloader]
