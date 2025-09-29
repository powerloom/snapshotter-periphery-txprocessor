import asyncio
import redis.exceptions
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from utils.models.settings_model import Settings
from utils.redis.redis_conn import RedisPool
from utils.logging import logger
import random
from config.loader import get_preloader_config, PRELOADER_CONFIG_FILE
from utils.preloaders.manager import PreloaderManager
import os
from collections import defaultdict

class TxProcessor:
    _redis: aioredis.Redis

    def __init__(self, settings: Settings):
        self.settings = settings
        self.rpc_helper = RpcHelper(settings.rpc)
        self._logger = logger.bind(module='TxProcessor')
        self.queue_key = f'{settings.processor.redis_queue_key}:{settings.namespace}'
        self.block_timeout = settings.processor.redis_block_timeout
        self.retry_counts = defaultdict(int)  # Track retry attempts per transaction
        
        # Load preloader hooks from configuration
        self._logger.info(f"🔧 Initializing TxProcessor with namespace: {settings.namespace}")
        self._logger.info(f"📋 Using Redis queue key: {self.queue_key}")
        self._logger.info(f"⏱️ Redis block timeout: {self.block_timeout}s")
        self._logger.info(f"📁 Loading preloader config from: {os.path.abspath(PRELOADER_CONFIG_FILE)}")
        preloader_config = get_preloader_config()
        self.preloader_hooks = PreloaderManager.load_hooks(preloader_config)
        self._logger.info(f"🔌 Loaded {len(self.preloader_hooks)} preloader hooks:")
        for hook in self.preloader_hooks:
            self._logger.info(f"  ├─ {hook.__class__.__name__}")

    async def _init(self):
        """Initialize Redis connection and RPC helper."""
        try:
            self._redis = await RedisPool.get_pool()
            # await self.rpc_helper.init() # Uncomment if RPC helper has async init
            
            # Initialize all preloader hooks
            for hook in self.preloader_hooks:
                try:
                    await hook.init()
                except Exception as e:
                    self._logger.error(f"🔎 No init supported in preloader hook {hook.__class__.__name__}: {e}")

            await self.rpc_helper.init()
                    
            self._logger.info("🚀 TxProcessor initialized successfully.")
        except Exception as e:
            self._logger.critical(f"❌ Failed to initialize TxProcessor: {e}")
            raise

    async def process_transaction(self, tx_hash: str):
        """Fetch receipt for a single transaction hash."""
        self._logger.info(f"🔍 Processing transaction hash: {tx_hash}")
        try:
            receipt = await self.rpc_helper.get_transaction_receipt_json(tx_hash)
            if receipt:
                self._logger.success(f"✅ Successfully fetched receipt for {tx_hash}")
                
                # 10% chance to perform the old-block check and queue clear
                if random.random() < 0.1:
                    current_block_number = await self.rpc_helper.get_current_block_number()
                    # check if transaction is more than 100 blocks old
                    receipt_block_number = int(receipt['blockNumber'], 16)  # Convert hex to int
                    if current_block_number - receipt_block_number > 100:
                        self._logger.warning(f"⚠️ Transaction {tx_hash} is more than 100 blocks old!")
                        # empty the entire queue
                        await self._redis.delete(self.queue_key)
                        self._logger.info("🔄 Cleared entire queue")
                        return
                # Run all preloader hooks
                for hook in self.preloader_hooks:
                    try:
                        await hook.process_receipt(tx_hash, receipt, self.settings.namespace)
                    except Exception as e:
                        self._logger.error(f"💥 Error in preloader hook {hook.__class__.__name__}: {e}")
            else:
                self._logger.warning(f"⚠️ No receipt found for {tx_hash} (might be pending or invalid)")
        except Exception as e:
            self._logger.error(f"💥 Failed to process {tx_hash}: {str(e)}")
            # Only retry if we haven't seen this transaction twice before
            if self.retry_counts[tx_hash] < 2:
                self.retry_counts[tx_hash] += 1
                await self._redis.lpush(self.queue_key, tx_hash)
                self._logger.info(f"🔄 Pushed {tx_hash} back to queue for retry (attempt {self.retry_counts[tx_hash]})")
            else:
                self._logger.error(f"❌ Max retries reached for {tx_hash}, giving up")

    async def start_consuming(self):
        """Continuously consume transaction hashes from Redis queue."""
        await self._init()
        self._logger.info(f"🔄 Starting consumer for Redis queue '{self.queue_key}' (timeout: {self.block_timeout}s)")

        while True:
            try:
                # Blocking right pop from the list
                result = await self._redis.brpop([self.queue_key], timeout=self.block_timeout)
                if result:
                    _queue_name, tx_hash = result
                    self._logger.info(f"📨 Consumed tx hash from queue '{self.queue_key}': {tx_hash}")
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
