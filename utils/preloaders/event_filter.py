import json
from pathlib import Path
from typing import Dict, Any, List
from web3._utils.events import get_event_data
from eth_utils.abi import event_abi_to_log_topic
from eth_abi.codec import ABICodec
from eth_abi.registry import registry as default_abi_registry
from utils.code_detectors import UniswapV3PoolDetector
from .base import TxPreloaderHook
from config.loader import get_event_filter_config
from utils.models.data_models import ProcessedFilterData, ProcessedEventDetail 
from utils.logging import logger
from utils.redis.redis_conn import RedisPool
from web3 import Web3, AsyncHTTPProvider
from web3.eth import AsyncEth
from config.loader import get_core_config


class EventFilter(TxPreloaderHook):
    """Filters transaction logs based on configured event topics and addresses."""

    def __init__(self):
        self._logger = logger.bind(module='EventFilterHook')
        self.filters_config = get_event_filter_config()
        self.processed_filters: Dict[str, ProcessedFilterData] = {}
        self._prepare_filters()
        self.settings = get_core_config()

        self.codec = ABICodec(default_abi_registry)
        self.detected_pool_addresses: Dict[str, bool] = {}

    def _get_web3_instance(self) -> Web3:
        """Get a Web3 instance using the RPC URL from settings.
        
        Returns:
            Web3: A configured Web3 instance
        """
        if hasattr(self, '_web3_instance'):
            return self._web3_instance
        
        provider = AsyncHTTPProvider(self.settings.rpc.url)
        self._web3_instance = Web3(provider)
        self._web3_instance.eth = AsyncEth(self._web3_instance)
        
        return self._web3_instance
        
    async def _get_redis_pool(self):
        """Get Redis connection pool.
        
        Returns:
            Redis: A Redis connection instance
        """
        return await RedisPool.get_pool()

    async def is_uniswap_v3_pool(self, address: str) -> bool:
        """Check if the given address is a UniswapV3Pool contract.
        
        Args:
            address: The address to check
            
        Returns:
            bool: True if the address is a UniswapV3Pool contract, False otherwise
        """
        # Check cache first
        address_lower = address.lower()
        if address_lower in self.detected_pool_addresses:
            return self.detected_pool_addresses[address_lower]

        # Retry up to 3 times before marking as non-pool
        retry_count = 3
        for attempt in range(retry_count):
            try:
                is_pool = await self._uniswap_v3_detector.is_uniswap_v3_pool(address)
                if is_pool:
                    self.detected_pool_addresses[address_lower] = True
                    return True
                # Only continue retrying on failure
            except Exception as e:
                self._logger.warning(f"Attempt {attempt + 1}/{retry_count} failed for {address}: {e}")
                if attempt == retry_count - 1:
                    # Log the final failure
                    self._logger.error(f"All {retry_count} attempts failed to detect pool status for {address}")
        
        # After all retries failed
        self.detected_pool_addresses[address_lower] = False
        return False
    
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

                self.processed_filters[filter_def.filter_name] = ProcessedFilterData(
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
            web3 = self._get_web3_instance()
            redis = await self._get_redis_pool()
            if not hasattr(self, '_uniswap_v3_detector'):
                self._uniswap_v3_detector = UniswapV3PoolDetector(web3, redis)
            
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
        # Structure: {redis_key: {member1: score1, member2: score2, ...}}
        commands_by_key: Dict[str, Dict[str, int]] = {}
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
                    if await self.is_uniswap_v3_pool(log_address_lower):
                        if log_topic0_standard in processed_filter.events_by_topic:
                            event_details = processed_filter.events_by_topic[log_topic0_standard]
                            event_abi = event_details.abi
                            event_name = event_details.name

                            try:
                                # Use the class codec
                                decoded_event = get_event_data(self.codec, event_abi, log_entry)

                                # Calculate composite score
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
                                    'topics': ['0x' + (t.hex() if hasattr(t, 'hex') else str(t)).lstrip('0x').lower() for t in log_topics],
                                    'data': log_entry.get('data', ''),
                                    'args': dict(decoded_event['args']),
                                    '_score': score  # Keep score in data for reference if needed
                                }
                                member = json.dumps(event_data_to_store)

                                # Group ZADD commands by key, using {member: score} mapping
                                if redis_key not in commands_by_key:
                                    commands_by_key[redis_key] = {}

                                commands_by_key[redis_key][member] = score
                                found_events_count += 1
                                log_msg = (f"  -> Matched event '{event_name}' from filter '{filter_name}' "
                                          f"in tx {tx_hash} (LogIndex: {log_index}). Score: {score}")
                                self._logger.debug(log_msg)

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
                for r_key, member_score_map in commands_by_key.items():
                    pipeline.zadd(r_key, mapping=member_score_map)
                await pipeline.execute()
                self._logger.success(f"💾 Stored {found_events_count} filtered events from tx {tx_hash} into Redis ZSets (Key: {list(commands_by_key.keys())}).")
            except Exception as redis_err:
                self._logger.error(f"❌ Failed to store filtered events from tx {tx_hash} to Redis: {redis_err}")
