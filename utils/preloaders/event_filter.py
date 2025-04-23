import json
from pathlib import Path
from typing import Dict, Any, List
from web3 import Web3

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
        workspace_root = Path(__file__).parent.parent.parent.parent # Adjust if needed
        
        # Pre-load ABIs to avoid redundant file reads if multiple filters use the same ABI
        loaded_abis: Dict[str, List[Dict[str, Any]]] = {}

        for filter_def in self.filters_config.filters:
            try:
                abi_path_str = filter_def.abi_path
                # Resolve ABI path relative to workspace root
                if not abi_path_str.startswith('/'):
                    abi_path = (workspace_root / abi_path_str).resolve()
                else:
                    abi_path = Path(abi_path_str)

                self._logger.info(f"🔧 Processing filter '{filter_def.filter_name}': ABI at {abi_path}")
                
                # Load ABI if not already loaded
                if str(abi_path) not in loaded_abis:
                    if not abi_path.exists():
                        self._logger.error(f"  ❌ ABI file not found: {abi_path}")
                        # Decide behaviour: skip filter or raise error
                        raise RuntimeError(f"ABI file '{abi_path}' not found for filter '{filter_def.filter_name}'.")
                    
                    try:
                        with open(abi_path, 'r') as f:
                            loaded_abis[str(abi_path)] = json.load(f)
                            self._logger.debug(f"  📂 Loaded ABI from {abi_path}")
                    except json.JSONDecodeError as e:
                        self._logger.error(f"  ❌ Error decoding ABI file '{abi_path}': {e}")
                        raise RuntimeError(f"Error decoding ABI file '{abi_path}': {e}")

                abi = loaded_abis[str(abi_path)]
                
                # Find event ABIs in the loaded ABI by matching calculated topic hashes
                target_event_details: Dict[str, Dict[str, Any]] = {}
                all_event_abis = [item for item in abi if item.get('type') == 'event']

                for config_topic in filter_def.event_topics:
                    found_abi = None
                    config_topic_lower = config_topic.lower()
                    for event_abi_item in all_event_abis:
                        try:
                            # Calculate topic hash for this event ABI item
                            event_signature_text = Web3.abi.build_event_signature(event_abi_item)
                            calculated_topic_hash = Web3.keccak(text=event_signature_text).hex()
                            
                            if calculated_topic_hash.lower() == config_topic_lower:
                                found_abi = event_abi_item
                                event_name = event_abi_item.get('name', 'UnnamedEvent')
                                self._logger.info(f"  ✔️ Matched config topic {config_topic_lower} to event '{event_name}' in ABI")
                                target_event_details[config_topic_lower] = {
                                    'name': event_name,
                                    'abi': found_abi
                                }
                                break # Found the ABI for this topic, move to next config topic
                        except Exception as abi_calc_err:
                            # Log error if calculating signature/hash fails for an ABI item
                            self._logger.warning(f"  ⚠️ Error processing ABI item for event matching: {event_abi_item.get('name', '?')} - {abi_calc_err}")
                            continue # Skip this potentially malformed ABI item

                    if not found_abi:
                         self._logger.warning(f"  ⚠️ Configured topic {config_topic_lower} not found in ABI {abi_path} for filter '{filter_def.filter_name}'")
                
                if not target_event_details:
                     self._logger.error(f"  ❌ No valid event ABIs found for any configured topics in filter '{filter_def.filter_name}', skipping this filter.")
                     continue
                     
                # Use lowercase addresses for case-insensitive matching
                target_addresses_lower = {addr.lower() for addr in filter_def.target_addresses}
                self._logger.info(f"  🎯 Filter will target {len(target_addresses_lower)} addresses.")

                self.processed_filters[filter_def.filter_name] = {
                    'target_addresses_lower': target_addresses_lower,
                    'events_by_topic': target_event_details, # Keys are lowercase topics
                    'redis_key_pattern': filter_def.redis_key_pattern
                }
                self._logger.success(f"  👍 Filter '{filter_def.filter_name}' prepared successfully.")

            except Exception as e:
                self._logger.error(f"💥 Failed to prepare filter '{filter_def.filter_name}': {type(e).__name__} - {e}")
                # Optionally re-raise or just log and continue with other filters
                # raise # Uncomment to stop processing if one filter fails

    async def process_receipt(self, tx_hash: str, receipt: Dict[str, Any], namespace: str) -> None:
        """Process logs in a transaction receipt, decode events matching filters, and store in Redis."""
        if not receipt or 'logs' not in receipt or not receipt['logs']:
            return # No logs to process

        redis = await RedisPool.get_pool()
