import json
from pathlib import Path
from typing import Dict, Any
from web3 import Web3

from .base import TxPreloaderHook
from config.loader import get_event_filter_config
from utils.logging import logger
from utils.redis.redis_conn import RedisPool


class EventFilter(TxPreloaderHook):
    """Default hook that dumps transaction receipt to Redis."""
    
    def __init__(self):
        self._logger = logger.bind(module='EventFilterHook')
        self.filters_config = get_event_filter_config()
        self.processed_filters = {}
        self._prepare_filters()

    def _prepare_filters(self):
        """Load ABIs, calculate topics, and store processed filter info."""
        workspace_root = Path(__file__).parent.parent.parent.parent # Adjust if needed
        for filter_def in self.filters_config.filters:
            try:
                abi_path = workspace_root / filter_def.abi_path
                self._logger.info(f"🔧 Processing filter '{filter_def.filter_name}': ABI at {abi_path}")
                if not abi_path.exists():
                    self._logger.error(f"  ❌ ABI file not found: {abi_path}")
                    continue
                
                with open(abi_path, 'r') as f:
                    abi = json.load(f)
                
                # Find event ABIs and calculate topics
                event_abis = {item['name']: item for item in abi if item.get('type') == 'event'}
                target_event_details = {}
                for event_name in filter_def.event_names:
                    if event_name in event_abis:
                        event_abi = event_abis[event_name]
                        # Note: web3.py's abi.find_matching_event_abis might be simpler
                        event_signature_text = Web3.abi.build_event_signature(event_abi)
                        event_topic_hash = Web3.keccak(text=event_signature_text).hex()
                        target_event_details[event_topic_hash] = {
                            'name': event_name,
                            'abi': event_abi
                        }
                        self._logger.info(f"  ✔️ Found event '{event_name}' with topic {event_topic_hash}")
                    else:
                         self._logger.warning(f"  ⚠️ Event '{event_name}' not found in ABI for filter '{filter_def.filter_name}'")

                if not target_event_details:
                     self._logger.error(f"  ❌ No valid events found for filter '{filter_def.filter_name}', skipping.")
                     continue
                     
                self.processed_filters[filter_def.filter_name] = {
                    'target_addresses_lower': {addr.lower() for addr in filter_def.target_addresses},
                    'events_by_topic': target_event_details,
                    'redis_key_pattern': filter_def.redis_key_pattern
                }
                self._logger.success(f"  👍 Filter '{filter_def.filter_name}' prepared successfully.")

            except Exception as e:
                self._logger.error(f"💥 Failed to prepare filter '{filter_def.filter_name}': {e}")

    async def process_receipt(self, tx_hash: str, receipt: Dict[str, Any], namespace: str) -> None:
        redis = await RedisPool.get_pool()
        # Convert hex block number to int
        block_number = int(receipt['blockNumber'], 16)
        
        pass
