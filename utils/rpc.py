import httpx
import asyncio
from typing import Optional, Dict, Any
from utils.models.settings_model import RPCConfig
from utils.logging import logger
# from web3 import Web3 # Or your preferred library

class RpcHelper:
    def __init__(self, config: RPCConfig):
        self.config = config
        self.client = httpx.AsyncClient(timeout=config.request_time_out)
        self._logger = logger.bind(module='RpcHelper')

    async def _make_request(self, method: str, params: list) -> Optional[Dict[str, Any]]:
        """Makes a JSON-RPC request with retries."""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }
        for attempt in range(self.config.retry + 1):
            try:
                response = await self.client.post(self.config.url, json=payload)
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    self._logger.error(f"RPC Error ({method}): {data['error']}")
                    return None
                return data.get("result")
            except httpx.RequestError as e:
                self._logger.warning(f"Request failed ({method}, attempt {attempt+1}/{self.config.retry+1}): {e}")
                if attempt == self.config.retry:
                    self._logger.error(f"Max retries exceeded for {method}.")
                    return None
                await asyncio.sleep(1) # Simple backoff
            except Exception as e:
                 self._logger.error(f"Unexpected error during RPC call ({method}): {e}")
                 return None
        return None

    async def get_transaction_receipt(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """Fetches the transaction receipt for a given hash."""
        self._logger.trace(f"Fetching receipt for tx: {tx_hash}")
        # Example using generic JSON-RPC call
        result = await self._make_request("eth_getTransactionReceipt", [tx_hash])

        # # Example using Web3.py (synchronous, needs adaptation for async or use async web3)
        # try:
        #     receipt = self.web3.eth.get_transaction_receipt(tx_hash)
        #     return dict(receipt) if receipt else None
        # except Exception as e:
        #     self._logger.error(f"Failed to get receipt for {tx_hash} using Web3.py: {e}")
        #     return None

        return result

    async def close(self):
        await self.client.aclose()
