from pydantic import BaseModel, Field
from typing import List, Dict, Set, Any, Optional


class AddressSource(BaseModel):
    config_file: str


class EventFilterDefinition(BaseModel):
    filter_name: str
    abi_path: str
    event_topics: List[str] = Field(..., min_items=1)
    address_source: Optional[AddressSource] = None # Make optional to potentially support direct address lists later
    target_addresses: List[str] = Field(default_factory=list)
    redis_key_pattern: str

    # Exclude target_addresses from being loaded directly from config JSON
    # It will be populated by the loader logic.
    class Config:
        fields = {'target_addresses': {'exclude': True}}


class EventFiltersConfig(BaseModel):
    filters: List[EventFilterDefinition]


class ProcessedEventDetail(BaseModel):
    name: str
    abi: Dict[str, Any] # The event ABI dictionary


class ProcessedFilterData(BaseModel):
    # Keys are standard 0x-prefixed, lowercase topic hashes
    events_by_topic: Dict[str, ProcessedEventDetail]
    redis_key_pattern: str