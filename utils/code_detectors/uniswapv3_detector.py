import binascii
from typing import Union, List, Dict, Any
from web3 import Web3
from eth_typing import HexStr, Address
from redis import asyncio as aioredis
import json
from utils.logging import logger

logger = logger.bind(module='UniswapV3PoolDetector')


class UniswapV3PoolDetector:
    """Detector for UniswapV3Pool contracts.

    This class provides functionality to identify UniswapV3Pool contracts by their bytecode signature.
    """

    def __init__(self, web3: Web3, redis: aioredis.Redis):
        """Initialize the UniswapV3PoolDetector.

        Args:
            web3: Web3 instance connected to a node
            redis: Redis instance for caching results
        """
        self.web3 = web3
        self.redis = redis
        self.logger = logger.bind(context="UniswapV3PoolDetector")

        # Unique function signatures present in UniswapV3Pool contracts
        # These are specific keccak256 hashes of function signatures that are characteristic of the contract
        self.FUNCTION_SIGNATURES = {
            # fee() function signature
            '0xddca3f43': 'fee()',
            # slot0() function signature
            '0x3850c7bd': 'slot0()',
            # factory() function signature
            '0xc45a0155': 'factory()',
            # token0() function signature
            '0x0dfe1681': 'token0()',
            # token1() function signature
            '0xd21220a7': 'token1()',
            # liquidity() function signature
            '0x1a686502': 'liquidity()'
        }

        # Common events in UniswapV3Pool contracts - topic0 hashes
        self.POOL_EVENT_TOPICS = {
            # Swap event
            '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67': 'Swap',
            # Mint event
            '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde': 'Mint',
            # Burn event
            '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c': 'Burn',
            # Flash event
            '0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633': 'Flash'
        }

        # Deterministic bytecode fragments that are unique to UniswapV3Pool
        # These are fragments of deployed bytecode that uniquely identify UniswapV3Pool contracts
        self.BYTECODE_SIGNATURES = [
            # This is a distinctive code pattern from UniswapV3Pool implementation
            '3d5989525d3d5989525d3d5989525d',  # Distinctive stack manipulation pattern
            '4946554e49535741505f5633',  # Hex for "IUNISWAP_V3" found in Uniswap contracts
        ]

        # Minimal ABI for function verification
        self.MINIMAL_ABI = [
            {
                "inputs": [],
                "name": "factory",
                "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "token0",
                "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "token1",
                "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "fee",
                "outputs": [{"internalType": "uint24", "name": "", "type": "uint24"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]

        # Minimal ABI for ERC20 token metadata
        self.ERC20_MINIMAL_ABI = [
            {
                "inputs": [],
                "name": "name",
                "outputs": [{"internalType": "string", "name": "", "type": "string"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "symbol",
                "outputs": [{"internalType": "string", "name": "", "type": "string"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "decimals",
                "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]

    async def is_uniswap_v3_pool(self, address: Union[str, Address]) -> bool:
        """Check if the given address is a UniswapV3Pool contract.

        Args:
            address: The address to check

        Returns:
            bool: True if the address is a UniswapV3Pool contract, False otherwise
        """
        logger.info(f"Checking if {address} is a UniswapV3Pool contract")
        try:
            # Normalize the address
            address = Web3.to_checksum_address(address)

            # Get the contract bytecode - must use await because get_code returns a coroutine
            bytecode = await self.web3.eth.get_code(address)

            if not bytecode or bytecode == '0x' or bytecode == HexStr('0x'):
                return False

            # Convert to hex string without 0x prefix for easier searching
            bytecode_hex = binascii.hexlify(bytecode).decode('ascii')

            # Method 1: Check for bytecode signatures
            for signature in self.BYTECODE_SIGNATURES:
                if signature in bytecode_hex.lower():
                    return True

            # Method 2: Check for function signatures
            # Count how many UniswapV3Pool function signatures are found in the bytecode
            signature_count = 0
            for signature in self.FUNCTION_SIGNATURES:
                if signature in bytecode_hex.lower():
                    signature_count += 1

            # If we find at least 4 of the 6 expected function signatures, it's likely a UniswapV3Pool
            if signature_count >= 4:
                return True

            # Method 3: Additional verification by calling key view functions
            try:
                pool_metadata = await self.get_pool_metadata(address)
                if not pool_metadata:
                    return False
                
                # TODO: Will be removed later
                # Considering only WETH-* pairs for now
                weth_address = self.web3.to_checksum_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
                if pool_metadata['token0']['address'] != weth_address and pool_metadata['token1']['address'] != weth_address:
                    return False

                # Verify addresses are valid
                Web3.to_checksum_address(pool_metadata['factory'])
                Web3.to_checksum_address(pool_metadata['token0']['address'])
                Web3.to_checksum_address(pool_metadata['token1']['address'])
                Web3.to_checksum_address(pool_metadata['factory'])

                # Uniswap V3 fees are usually one of these values: 500 (0.05%), 3000 (0.3%), 10000 (1%)
                # But we'll just check it's a valid uint24
                if isinstance(pool_metadata['fee'], int) and 0 <= pool_metadata['fee'] < 2**24:
                    return True

                # If we got here without exception but fee isn't valid, this might not be a real pool
                return False

            except Exception as e:
                # Function calls failed, not a UniswapV3Pool
                return False

        except Exception as e:
            # Any other exception means it's not a valid contract or has other issues
            return False

    def get_key_event_topics(self) -> List[str]:
        """Get the list of event topic signatures that are characteristic of UniswapV3Pool contracts.

        Returns:
            List[str]: List of topic0 signatures (keccak256 hashes of event definitions)
        """
        return list(self.POOL_EVENT_TOPICS.keys())

    async def get_pool_metadata(self, pool_address: Union[str, Address]) -> Dict[str, Any]:
        """Get token0 and token1 metadata from a UniswapV3Pool including name, symbol, and decimals.

        Args:
            pool_address: The address of the UniswapV3Pool contract

        Returns:
            Dict containing token0 and token1 metadata or None if not a valid pool or tokens
        """
        try:
            # Normalize the address
            pool_address = Web3.to_checksum_address(pool_address)

            # Check Redis cache first
            cache_key = f'pool_metadata:{pool_address}'
            cached_data = await self.redis.get(cache_key)

            if cached_data:
                return json.loads(cached_data)

            # Get the pool contract
            pool_contract = self.web3.eth.contract(address=pool_address, abi=self.MINIMAL_ABI)

            # Get token addresses
            token0_address = await pool_contract.functions.token0().call()
            token1_address = await pool_contract.functions.token1().call()

            token0_address = Web3.to_checksum_address(token0_address)
            token1_address = Web3.to_checksum_address(token1_address)

            # Get tokens metadata
            token0_metadata = await self._get_erc20_metadata(token0_address)
            if not token0_metadata:
                return None
            token1_metadata = await self._get_erc20_metadata(token1_address)
            if not token1_metadata:
                return None

            factory_address = await pool_contract.functions.factory().call()
            factory_address = Web3.to_checksum_address(factory_address)

            # Get pool fee
            fee = await pool_contract.functions.fee().call()

            # Build complete pool metadata
            pool_metadata = {
                'address': pool_address,
                'token0': {
                    'address': token0_address,
                    **token0_metadata
                },
                'token1': {
                    'address': token1_address,
                    **token1_metadata
                },
                'fee': fee,
                'factory': factory_address
            }

            # Cache the result in Redis with 1 hour expiry
            await self.redis.set(cache_key, json.dumps(pool_metadata), ex=3600)

            return pool_metadata

        except Exception as e:
            # self.logger.error(f"Error getting token metadata for pool {pool_address}: {str(e)}")
            return None
        
    async def _get_erc20_metadata(self, token_address: Union[str, Address]) -> Dict[str, Any]:
        """Get ERC20 token metadata (name, symbol, decimals).

        Args:
            token_address: The address of the ERC20 token

        Returns:
            Dict containing token metadata (name, symbol, decimals)
        """
        try:
            # Normalize the address
            token_address = Web3.to_checksum_address(token_address)

            # Check Redis cache first
            cache_key = f'erc20_metadata:{token_address}'
            cached_data = await self.redis.get(cache_key)

            if cached_data:
                return json.loads(cached_data)

            # Create token contract instance
            token_contract = self.web3.eth.contract(address=token_address, abi=self.ERC20_MINIMAL_ABI)

            # Call view functions to get metadata
            name = await token_contract.functions.name().call()
            symbol = await token_contract.functions.symbol().call()
            decimals = await token_contract.functions.decimals().call()

            metadata = {
                'name': name,
                'symbol': symbol,
                'decimals': decimals
            }

            # Cache the result in Redis with 1 day expiry (since token metadata rarely changes)
            await self.redis.set(cache_key, json.dumps(metadata), ex=86400)

            return metadata

        except Exception as e:
            # self.logger.error(f"Error getting metadata for token {token_address}: {str(e)}")
            # Return default values if we can't fetch the metadata
            return None
