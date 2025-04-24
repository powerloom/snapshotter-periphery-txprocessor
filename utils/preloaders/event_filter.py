import json
from pathlib import Path
from typing import Dict, Any, List
from web3 import Web3
from web3._utils import abi as abi_utils
from web3._utils.events import get_event_data
from eth_utils.abi import event_abi_to_log_topic

from .base import TxPreloaderHook
from config.loader import get_event_filter_config
from utils.logging import logger
from utils.redis.redis_conn import RedisPool


class EventFilter(TxPreloaderHook):
    """Filters transaction logs based on configured event topics and addresses."""
    
    def __init__(self):
        self._logger = logger.bind(module='EventFilterHook')
        self.filters_config = get_event_filter_config()
        self.processed_filters: Dict[str, Dict[str, Any]] = {}
        self._prepare_filters()

    def _prepare_filters(self):
        """Load ABIs, find event ABIs matching configured topics, and store processed filter info."""
        workspace_root = Path(__file__).parent.parent.parent.parent
        
        loaded_abis: Dict[str, List[Dict[str, Any]]] = {}

        for filter_def in self.filters_config.filters:
            try:
                abi_path_str = filter_def.abi_path
                if not abi_path_str.startswith('/'):
                    abi_path = (workspace_root / abi_path_str).resolve()
                else:
                    abi_path = Path(abi_path_str)

                self._logger.info(f"🔧 Processing filter '{filter_def.filter_name}': ABI at {abi_path}")
                
                if str(abi_path) not in loaded_abis:
                    if not abi_path.exists():
                        self._logger.error(f"  ❌ ABI file not found: {abi_path}")
                        raise RuntimeError(f"ABI file '{abi_path}' not found for filter '{filter_def.filter_name}'.")
                    try:
                        with open(abi_path, 'r') as f:
                            loaded_abis[str(abi_path)] = json.load(f)
                            self._logger.debug(f"  📂 Loaded ABI from {abi_path}")
                    except json.JSONDecodeError as e:
                        self._logger.error(f"  ❌ Error decoding ABI file '{abi_path}': {e}")
                        raise RuntimeError(f"Error decoding ABI file '{abi_path}': {e}")

                abi = loaded_abis[str(abi_path)]
                
                # Build a set of configured topics (standard: lowercase, 0x-prefixed)
                config_topics_set = set()
                for topic in filter_def.event_topics:
                    normalized_topic = topic.lower()
                    if not normalized_topic.startswith('0x'):
                        normalized_topic = '0x' + normalized_topic
                    config_topics_set.add(normalized_topic)
                
                self._logger.info(f"  🔍 Will look for {len(config_topics_set)} standard configured topics: {config_topics_set}")
                
                target_event_details: Dict[str, Dict[str, Any]] = {}
                all_event_abis = [item for item in abi if item.get('type') == 'event']

                # Iterate through ABIs once, calculate standard hash, and check against the config set
                for event_abi_item in all_event_abis:
                    try:
                        calculated_topic_hash_bytes = event_abi_to_log_topic(event_abi_item)
                        # Convert calculated hash to standard format (lowercase, 0x-prefix)
                        standard_calculated_hash = '0x' + calculated_topic_hash_bytes.hex().lower()

                        # Check if this standard calculated hash is one we care about
                        if standard_calculated_hash in config_topics_set:
                            event_name = event_abi_item.get('name', 'UnnamedEvent')
                            self._logger.info(f"  ✔️ Matched ABI event '{event_name}' to configured topic (hash: {standard_calculated_hash})")
                            target_event_details[standard_calculated_hash] = {
                                'name': event_name,
                                'abi': event_abi_item
                            }
                    except Exception as abi_calc_err:
                        self._logger.warning(f"  ⚠️ Error processing ABI item: {event_abi_item.get('name', '?')} - {abi_calc_err}")
                        continue

                # After checking all ABIs, verify all configured topics were found
                found_topics = set(target_event_details.keys())
                missing_topics = config_topics_set - found_topics
                for missing in missing_topics:
                    self._logger.warning(f"  ⚠️ Configured topic {missing} not found in ABI {abi_path} for filter '{filter_def.filter_name}'")
                
                if not target_event_details:
                     self._logger.error(f"  ❌ No valid event ABIs found for any configured topics in filter '{filter_def.filter_name}', skipping this filter.")
                     continue
                     
                target_addresses_lower = {addr.lower() for addr in filter_def.target_addresses}
                self._logger.info(f"  🎯 Filter will target {len(target_addresses_lower)} addresses.")

                self.processed_filters[filter_def.filter_name] = {
                    'target_addresses_lower': target_addresses_lower,
                    'events_by_topic': target_event_details, 
                    'redis_key_pattern': filter_def.redis_key_pattern
                }
                self._logger.success(f"  👍 Filter '{filter_def.filter_name}' prepared successfully.")

            except Exception as e:
                self._logger.error(f"💥 Failed to prepare filter '{filter_def.filter_name}': {type(e).__name__} - {e}")

    async def process_receipt(self, tx_hash: str, receipt: Dict[str, Any], namespace: str) -> None:
        # This method remains unchanged as it relies on the prepared filters
        # Implementation to be added later
        pass
