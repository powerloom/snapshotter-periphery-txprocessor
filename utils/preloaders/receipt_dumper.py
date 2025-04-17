import json
from typing import Dict, Any
from .base import TxPreloaderHook
from utils.redis.redis_conn import RedisPool
from utils.redis.redis_keys import block_tx_htable_key

class ReceiptDumper(TxPreloaderHook):
    """Default hook that dumps transaction receipt to Redis."""
    
    async def process_receipt(self, tx_hash: str, receipt: Dict[str, Any], namespace: str) -> None:
        redis = await RedisPool.get_pool()
        # Convert hex block number to int
        block_number = int(receipt['blockNumber'], 16)
        # Store receipt in block-tx mapping
        await redis.hset(
            block_tx_htable_key(namespace, block_number),
            tx_hash,
            json.dumps(receipt)
        )
