import asyncio
import redis.exceptions
from redis import asyncio as aioredis
from utils.models.settings_model import Settings
from utils.redis_conn import RedisPool
from utils.rpc import RpcHelper
from utils.logging import logger
from utils.redis_keys import block_tx_htable_key
import json

class TxProcessor:
    _redis: aioredis.Redis

    def __init__(self, settings: Settings):
        self.settings = settings
        self.rpc_helper = RpcHelper(settings.rpc)
        self._logger = logger.bind(module='TxProcessor')
        self.queue_key = f'{settings.processor.redis_queue_key}:{settings.namespace}'
        self.block_timeout = settings.processor.redis_block_timeout

    async def _init(self):
        """Initialize Redis connection and RPC helper."""
        try:
             self._redis = await RedisPool.get_pool()
             # await self.rpc_helper.init() # Uncomment if RPC helper has async init
             self._logger.info("🚀 TxProcessor initialized successfully.")
        except Exception as e:
            self._logger.critical(f"❌ Failed to initialize TxProcessor: {e}")
            raise

    async def process_transaction(self, tx_hash: str):
        """Fetch receipt for a single transaction hash."""
        self._logger.info(f"🔍 Processing transaction hash: {tx_hash}")
        try:
            receipt = await self.rpc_helper.get_transaction_receipt(tx_hash)
            if receipt:
                self._logger.success(f"✅ Successfully fetched receipt for {tx_hash}")
                # Store the full receipt as JSON in the hashtable
                await self._redis.hset(
                    block_tx_htable_key(self.settings.namespace, receipt['blockNumber']),
                    tx_hash,
                    json.dumps(receipt)
                )
            else:
                # This could be normal if the tx hasn't been mined yet or is invalid
                self._logger.warning(f"⚠️ No receipt found for {tx_hash} (might be pending or invalid)")
        except Exception as e:
            self._logger.error(f"💥 Failed to process {tx_hash}: {str(e)}")
            # Optional: Add logic to requeue or handle failures

    async def start_consuming(self):
        """Continuously consume transaction hashes from Redis queue."""
        await self._init()
        self._logger.info(f"🔄 Starting consumer for Redis queue '{self.queue_key}' (timeout: {self.block_timeout}s)")

        while True:
            try:
                # Blocking right pop from the list
                result = await self._redis.brpop(self.queue_key, timeout=self.block_timeout)
                if result:
                    _queue_name, tx_hash_bytes = result
                    tx_hash = tx_hash_bytes # Already decoded if decode_responses=True in RedisPool
                    self._logger.debug(f"📨 Received transaction hash: {tx_hash}")
                    # Process the transaction hash asynchronously
                    # Use create_task for concurrency if receipt fetching is slow
                    asyncio.create_task(self.process_transaction(tx_hash))
                else:
                    # Timeout occurred (only if self.block_timeout > 0)
                    self._logger.trace("⏳ No new transaction hash received within timeout, looping...")
                    pass # Loop continues, will block again on brpop

            except (redis.exceptions.ConnectionError, ConnectionRefusedError, asyncio.TimeoutError) as conn_err:
                self._logger.error(f"❌ Redis connection error: {conn_err}. Retrying connection...")
                await RedisPool.close() # Ensure old pool is closed
                await asyncio.sleep(5)
                await self._init() # Re-initialize connection
            except Exception as e:
                self._logger.error(f"🔥 Unexpected error consuming from Redis: {type(e).__name__} - {e}")
                # Optional: Add a delay before retrying on other errors
                await asyncio.sleep(2)
