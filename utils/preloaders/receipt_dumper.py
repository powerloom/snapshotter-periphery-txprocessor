import json
from typing import Dict, Any
from .base import TxPreloaderHook
from utils.redis.data_manager import RedisDataManager
from config.loader import get_core_config

class ReceiptDumper(TxPreloaderHook):
    """Default hook that dumps transaction receipt to Redis."""
    
    def __init__(self):
        self.settings = get_core_config()
        self.data_manager = RedisDataManager(self.settings.redis.data_retention)
    
    async def init(self):
        """Initialize the data manager."""
        await self.data_manager.init()
    
    async def process_receipt(self, tx_hash: str, receipt: Dict[str, Any], namespace: str) -> None:
        # Convert hex block number to int
        block_number = int(receipt['blockNumber'], 16)
        # Store receipt in block-tx mapping
        await self.data_manager.add_receipt(
            namespace,
            block_number,
            tx_hash,
            json.dumps(receipt)
        )
    
    async def close(self):
        """Cleanup resources."""
        await self.data_manager.close()
