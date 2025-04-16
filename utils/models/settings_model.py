from pydantic import BaseModel
from typing import Union, Optional

class Redis(BaseModel):
    """Redis configuration model."""
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False

class RPCConfig(BaseModel):
    """RPC configuration model."""
    url: str
    retry: int = 3
    request_time_out: int = 15
    # Add other RPC helper settings if needed

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
    rpc: RPCConfig
    redis: Redis
    logs: Logs
    processor: TxProcessorConfig
    namespace: str