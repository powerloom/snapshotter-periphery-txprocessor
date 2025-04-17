from abc import ABC, abstractmethod
from typing import Dict, Any

class TxPreloaderHook(ABC):
    """Base class for transaction receipt preloader hooks."""
    
    @abstractmethod
    async def process_receipt(self, tx_hash: str, receipt: Dict[str, Any], namespace: str) -> None:
        """Process a transaction receipt after it's fetched.
        
        Args:
            tx_hash: The transaction hash
            receipt: The transaction receipt dictionary from the RPC
            namespace: The current service namespace
        """
        pass
