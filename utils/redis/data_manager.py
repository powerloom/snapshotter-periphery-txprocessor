from typing import Optional, Dict, Any, Union, Awaitable
import redis.asyncio as aioredis
from utils.logging import logger
from utils.models.settings_model import RedisDataRetentionConfig
from utils.redis.redis_conn import RedisPool
from utils.redis.redis_keys import block_tx_htable_key

class RedisDataManager:
    """Manages Redis data retention and cleanup."""
    
    def __init__(self, retention_config: RedisDataRetentionConfig):
        self.retention_config = retention_config
        self._redis: Optional[aioredis.Redis] = None
        self._logger = logger.bind(module='RedisDataManager')
        self._last_cleanup_block = 0
        # Cleanup interval is 10% of max_blocks, but at least 10 blocks
        self._cleanup_interval = max(10, self.retention_config.max_blocks // 10)
        self._logger.info(
            f"Initialized RedisDataManager with max_blocks={retention_config.max_blocks}, "
            f"cleanup_interval={self._cleanup_interval}"
        )

    async def init(self) -> None:
        """Initialize Redis connection."""
        self._redis = await RedisPool.get_pool()

    async def set_with_ttl(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set a key with TTL."""
        if not self._redis:
            return

        ttl = ttl or self.retention_config.ttl_seconds
        await self._redis.set(key, value, ex=ttl)

    async def add_receipt(self, namespace: str, block_number: int, tx_hash: str, receipt: str) -> None:
        """Add a transaction receipt."""
        if not self._redis:
            return
        key = block_tx_htable_key(namespace, block_number)
        await self._redis.hset(key, tx_hash, receipt)
        self._logger.info("🗄️ Added receipt to redis: {}", tx_hash)

    async def get_receipt(self, namespace: str, block_number: int, tx_hash: str) -> Optional[str]:
        """Get a transaction receipt."""
        if not self._redis:
            return None
        key = block_tx_htable_key(namespace, block_number)
        result = await self._redis.hget(key, tx_hash)
        if result is None:
            return None
        return str(result)

    async def close(self) -> None:
        """Cleanup resources."""
        pass 