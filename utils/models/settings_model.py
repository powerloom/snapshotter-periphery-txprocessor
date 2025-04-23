from pydantic import BaseModel, Field
from typing import Union, List, Dict, Optional

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

class Preloader(BaseModel):
    """Preloader configuration model."""
    task_type: str
    module: str
    class_name: str

class PreloaderConfig(BaseModel):
    """Preloader configuration model."""
    preloaders: List[Preloader]

class AddressSource(BaseModel):
    config_file: str
    # We might add a field here later to specify how to extract addresses e.g., by project_type
    
class EventFilterDefinition(BaseModel):
    filter_name: str
    abi_path: str
    event_topics: List[str] = Field(..., min_items=1)
    address_source: AddressSource # Or make this optional and allow direct address list
    redis_key_pattern: str
    target_addresses: List[str] = Field(default_factory=list, exclude=True) 

class EventFiltersConfig(BaseModel):
    filters: List[EventFilterDefinition]