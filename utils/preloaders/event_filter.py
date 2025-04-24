import json
from pathlib import Path
from typing import Dict, Any, List
from web3 import Web3
from web3._utils.events import get_event_data
from eth_utils.abi import event_abi_to_log_topic

from .base import TxPreloaderHook
from config.loader import get_event_filter_config
from utils.models.data_models import ProcessedFilterData, ProcessedEventDetail 
from utils.logging import logger
from utils.redis.redis_conn import RedisPool


class EventFilter(TxPreloaderHook):
    """Filters transaction logs based on configured event topics and addresses."""
    
    def __init__(self):
        self._logger = logger.bind(module='EventFilterHook')
        self.filters_config = get_event_filter_config()
        self.processed_filters: Dict[str, ProcessedFilterData] = {}
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
                
                config_topics_set = set()
                for topic in filter_def.event_topics:
                    normalized_topic = topic.lower()
                    if not normalized_topic.startswith('0x'):
                        normalized_topic = '0x' + normalized_topic
                    config_topics_set.add(normalized_topic)
                
                self._logger.info(f"  🔍 Will look for {len(config_topics_set)} standard configured topics: {config_topics_set}")
                
                target_event_details: Dict[str, ProcessedEventDetail] = {}
                all_event_abis = [item for item in abi if item.get('type') == 'event']

                for event_abi_item in all_event_abis:
                    try:
                        calculated_topic_hash_bytes = event_abi_to_log_topic(event_abi_item)
                        standard_calculated_hash = '0x' + calculated_topic_hash_bytes.hex().lower()

                        if standard_calculated_hash in config_topics_set:
                            event_name = event_abi_item.get('name', 'UnnamedEvent')
                            self._logger.info(f"  ✔️ Matched ABI event '{event_name}' to configured topic (hash: {standard_calculated_hash})")
                            target_event_details[standard_calculated_hash] = ProcessedEventDetail(
                                name=event_name,
                                abi=event_abi_item
                            )
                    except Exception as abi_calc_err:
                        self._logger.warning(f"  ⚠️ Error processing ABI item: {event_abi_item.get('name', '?')} - {abi_calc_err}")
                        continue

                found_topics = set(target_event_details.keys())
                missing_topics = config_topics_set - found_topics
                for missing in missing_topics:
                    self._logger.warning(f"  ⚠️ Configured topic {missing} not found in ABI {abi_path} for filter '{filter_def.filter_name}'")
                
                if not target_event_details:
                     self._logger.error(f"  ❌ No valid event ABIs found for any configured topics in filter '{filter_def.filter_name}', skipping this filter.")
                     continue
                     
                target_addresses_lower = {addr.lower() for addr in filter_def.target_addresses}
                self._logger.info(f"  🎯 Filter will target {len(target_addresses_lower)} addresses.")

                self.processed_filters[filter_def.filter_name] = ProcessedFilterData(
                    target_addresses_lower=target_addresses_lower,
                    events_by_topic=target_event_details,
                    redis_key_pattern=filter_def.redis_key_pattern
                )
                self._logger.success(f"  👍 Filter '{filter_def.filter_name}' prepared successfully.")

            except Exception as e:
                self._logger.error(f"💥 Failed to prepare filter '{filter_def.filter_name}': {type(e).__name__} - {e}")

    async def process_receipt(self, tx_hash: str, receipt: Dict[str, Any], namespace: str) -> None:
        """Process logs in a transaction receipt, decode events matching filters, and store in Redis ZSets (one per address)."""
        if not receipt or 'logs' not in receipt or not isinstance(receipt.get('logs'), list) or not receipt['logs']:
            return

        try:
            redis = await RedisPool.get_pool()
            web3_codec = Web3.codec
            block_number_hex = receipt.get('blockNumber')
            tx_index_hex = receipt.get('transactionIndex')
            if block_number_hex is None or tx_index_hex is None:
                self._logger.warning(f"Missing blockNumber or transactionIndex in receipt for tx {tx_hash}")
                return
            
            block_number = int(block_number_hex, 16)
            tx_index = int(tx_index_hex, 16)

        except Exception as init_err:
            self._logger.error(f"Error during initial setup for tx {tx_hash}: {init_err}")
            return

        # Define a multiplier for the composite score (block_number dominates log_index)
        SCORE_BLOCK_MULTIPLIER = 1_000_000

        # Group ZADD commands by key (address)
        # Structure: {redis_key: {score1: member1, score2: member2, ...}}
        commands_by_key: Dict[str, Dict[int, str]] = {}
        found_events_count = 0

        for log_entry in receipt['logs']:
            try:
                log_address = log_entry.get('address')
                log_topics = log_entry.get('topics')
                log_index_hex = log_entry.get('logIndex')
                
                if not log_address or not log_topics or log_index_hex is None:
                    self._logger.trace(f"Skipping invalid log entry in tx {tx_hash}: {log_entry}")
                    continue

                log_address_lower = log_address.lower()
                log_topic0_hex = log_topics[0].hex() if hasattr(log_topics[0], 'hex') else str(log_topics[0])
                log_topic0_standard = ('0x' + log_topic0_hex.lower().lstrip('0x'))
                log_index = int(log_index_hex, 16)

                for filter_name, processed_filter in self.processed_filters.items():
                    if log_address_lower in processed_filter.target_addresses_lower:
                        if log_topic0_standard in processed_filter.events_by_topic:
                            event_details = processed_filter.events_by_topic[log_topic0_standard]
                            event_abi = event_details.abi
                            event_name = event_details.name
                            
                            try:
                                decoded_event = get_event_data(web3_codec, event_abi, log_entry)
                                
                                # Calculate composite score: (block_number * MULTIPLIER) + log_index
                                score = (block_number * SCORE_BLOCK_MULTIPLIER) + log_index

                                # Prepare key (per address) and member for Redis ZSet
                                redis_key = processed_filter.redis_key_pattern.format(
                                    namespace=namespace, 
                                    address=log_address
                                )
                                
                                # Member is the JSON string of event details
                                event_data_to_store = {
                                    'eventName': event_name,
                                    'filterName': filter_name,
                                    'txHash': tx_hash,
                                    'blockNumber': block_number,
                                    'txIndex': tx_index,
                                    'logIndex': log_index,
                                    'address': log_address,
                                    'topics': [t.hex() if hasattr(t, 'hex') else str(t) for t in log_topics],
                                    'data': log_entry.get('data', ''),
                                    'args': dict(decoded_event['args']),
                                    '_score': score
                                }
                                member = json.dumps(event_data_to_store)
                                
                                # Group ZADD commands by key
                                if redis_key not in commands_by_key:
                                    commands_by_key[redis_key] = {}
                                
                                if score in commands_by_key[redis_key]:
                                     self._logger.warning(f"Score collision detected for key {redis_key}, score {score}. Overwriting previous member for this tx.")
                                
                                commands_by_key[redis_key][score] = member
                                found_events_count += 1
                                self._logger.debug(f"  -> Matched event '{event_name}' from filter '{filter_name}' in tx {tx_hash} (LogIndex: {log_index}). Score: {score}")

                            except Exception as decode_err:
                                self._logger.error(
                                    f"💥 Error decoding/processing event '{event_name}' (topic: {log_topic0_standard}) "
                                    f"for filter '{filter_name}' in tx {tx_hash} (LogIndex: {log_index}): {decode_err}"
                                )
            except Exception as log_proc_err:
                 self._logger.error(f"💥 Unexpected error processing log entry in tx {tx_hash}: {log_proc_err} | Log: {log_entry}")
                 continue

        if found_events_count > 0:
            try:
                pipeline = redis.pipeline(transaction=False)
                for r_key, score_member_map in commands_by_key.items():
                    pipeline.zadd(r_key, mapping=score_member_map)
                await pipeline.execute()
                self._logger.success(f"💾 Stored {found_events_count} filtered events from tx {tx_hash} into Redis ZSets (Key: {list(commands_by_key.keys())}).")
            except Exception as redis_err:
                 self._logger.error(f"❌ Failed to store filtered events from tx {tx_hash} to Redis: {redis_err}")
