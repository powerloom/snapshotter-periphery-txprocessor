from typing import Union, List, Dict, Any
from web3 import Web3, AsyncWeb3
from eth_typing import HexStr, Address
from redis import asyncio as aioredis
import json
from utils.logging import logger

logger = logger.bind(module='UniswapV3PoolDetector')


class UniswapV3PoolDetector:
    """Production-ready detector for UniswapV3Pool contracts.

    This class provides robust functionality to identify UniswapV3Pool contracts
    using multiple verification methods including bytecode analysis, function
    signature verification, and metadata validation.
    """

    def __init__(self, web3: Union[Web3, AsyncWeb3], redis: aioredis.Redis):
        """Initialize the UniswapV3PoolDetector.

        Args:
            web3: Web3 or AsyncWeb3 instance connected to a node
            redis: Redis instance for caching results
        """
        self.web3 = web3
        self.redis = redis
        self.logger = logger.bind(context="UniswapV3PoolDetector")

        # Function signatures that MUST be present in UniswapV3Pool contracts
        self.REQUIRED_FUNCTION_SIGNATURES = {
            '0xddca3f43': 'fee()',
            '0x3850c7bd': 'slot0()',
            '0xc45a0155': 'factory()',
            '0x0dfe1681': 'token0()',
            '0xd21220a7': 'token1()',
            '0x1a686502': 'liquidity()',
            '0x70cf754a': 'tickSpacing()',
            '0x128acb08': 'feeGrowthGlobal0X128()',
            '0xa138ed29': 'feeGrowthGlobal1X128()'
        }

        # Minimal ABI for pool verification
        self.POOL_ABI = [
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
            },
            {
                "inputs": [],
                "name": "tickSpacing",
                "outputs": [{"internalType": "int24", "name": "", "type": "int24"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "slot0",
                "outputs": [
                    {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
                    {"internalType": "int24", "name": "tick", "type": "int24"},
                    {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
                    {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
                    {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
                    {"internalType": "uint8", "name": "feeProtocol", "type": "uint8"},
                    {"internalType": "bool", "name": "unlocked", "type": "bool"}
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]

        # ERC20 ABI for token verification
        self.ERC20_ABI = [
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

        # Valid Uniswap V3 fee tiers
        self.VALID_FEE_TIERS = {100, 500, 3000, 10000}

    async def is_uniswap_v3_pool(self, address: Union[str, Address]) -> bool:
        """Check if the given address is a UniswapV3Pool contract.

        Uses multiple verification methods to ensure accuracy:
        1. Bytecode function signature analysis
        2. Contract metadata validation
        3. Fee tier validation
        4. Tick spacing validation

        Args:
            address: The address to check

        Returns:
            bool: True if the address is a UniswapV3Pool contract, False otherwise
        """
        try:
            # Normalize the address
            address = Web3.to_checksum_address(address)
            self.logger.info(f"Checking if {address} is a UniswapV3Pool contract")

            # Check cache first
            cache_key = f'uniswap_v3_pool_check:{address}'
            cached_result = await self.redis.get(cache_key)
            if cached_result is not None:
                result = json.loads(cached_result)
                self.logger.info(f"Cache hit for {address}: {result}")
                return result

            # Step 1: Check if contract exists
            if not await self._has_contract_code(address):
                await self._cache_result(cache_key, False)
                return False

            # Step 2: Check bytecode for function signatures
            if not await self._has_required_function_signatures(address):
                self.logger.info(f"Required function signatures not found for {address}")
                await self._cache_result(cache_key, False)
                return False

            # Step 3: Verify contract metadata
            pool_metadata = await self.get_pool_metadata(address)
            if not pool_metadata:
                self.logger.info(f"Could not get pool metadata for {address}")
                await self._cache_result(cache_key, False)
                return False

            # Step 4: Validate fee tier
            if not self._is_valid_fee_tier(pool_metadata['fee']):
                self.logger.info(f"Invalid fee tier {pool_metadata['fee']} for {address}")
                await self._cache_result(cache_key, False)
                return False

            # Step 5: Additional validation - check tick spacing
            if not self._is_valid_tick_spacing(pool_metadata['fee'], pool_metadata.get('tick_spacing')):
                self.logger.info(f"Invalid tick spacing for fee {pool_metadata['fee']} for {address}")
                await self._cache_result(cache_key, False)
                return False

            # TODO: Remove this WETH filter once testing is complete
            weth_address = Web3.to_checksum_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
            token0_addr = pool_metadata['token0']['address']
            token1_addr = pool_metadata['token1']['address']
            if token0_addr != weth_address and token1_addr != weth_address:
                self.logger.info(f"Neither token0 nor token1 is WETH for {address}")
                await self._cache_result(cache_key, False)
                return False

            # All checks passed
            self.logger.info(f"Successfully verified {address} as UniswapV3Pool")
            await self._cache_result(cache_key, True)
            return True

        except Exception as e:
            self.logger.error(f"Error checking if {address} is a UniswapV3Pool contract: {str(e)}")
            await self._cache_result(cache_key, False)
            return False

    async def _has_contract_code(self, address: str) -> bool:
        """Check if the address has contract bytecode."""
        try:
            bytecode = await self.web3.eth.get_code(address)
            return bytecode and bytecode != '0x' and bytecode != HexStr('0x')
        except Exception as e:
            self.logger.error(f"Error getting bytecode for {address}: {str(e)}")
            return False

    async def _has_required_function_signatures(self, address: str) -> bool:
        """Check if the contract bytecode contains required function signatures."""
        try:
            bytecode = await self.web3.eth.get_code(address)
            if not bytecode:
                return False

            # Convert to hex string without 0x prefix for searching
            bytecode_hex = bytecode.hex().lower()

            # Count matching function signatures
            signature_matches = 0
            for signature in self.REQUIRED_FUNCTION_SIGNATURES:
                # Remove 0x prefix and search in bytecode
                sig_hex = signature[2:].lower()
                if sig_hex in bytecode_hex:
                    signature_matches += 1

            # Require at least 6 out of 9 signatures to be present
            # This allows for some variations in contract implementations
            required_matches = 6
            success = signature_matches >= required_matches
            
            self.logger.info(f"Found {signature_matches} signatures in {address}")
            
            return success

        except Exception as e:
            self.logger.error(f"Error checking function signatures for {address}: {str(e)}")
            return False

    def _is_valid_fee_tier(self, fee: int) -> bool:
        """Check if the fee is a valid Uniswap V3 fee tier."""
        return isinstance(fee, int) and fee in self.VALID_FEE_TIERS

    def _is_valid_tick_spacing(self, fee: int, tick_spacing: int) -> bool:
        """Check if tick spacing matches the expected value for the fee tier."""
        if not isinstance(tick_spacing, int):
            return False
            
        # Expected tick spacing for each fee tier
        expected_tick_spacing = {
            100: 1,
            500: 10,
            3000: 60,
            10000: 200
        }
        
        return expected_tick_spacing.get(fee) == tick_spacing

    async def _cache_result(self, cache_key: str, result: bool) -> None:
        """Cache the verification result."""
        try:
            await self.redis.set(cache_key, json.dumps(result), ex=3600)  # 1 hour cache
        except Exception as e:
            self.logger.warning(f"Failed to cache result: {str(e)}")

    async def get_pool_metadata(self, pool_address: Union[str, Address]) -> Dict[str, Any]:
        """Get comprehensive metadata from a UniswapV3Pool.

        Args:
            pool_address: The address of the UniswapV3Pool contract

        Returns:
            Dict containing pool metadata or None if not a valid pool
        """
        try:
            pool_address = Web3.to_checksum_address(pool_address)

            # Check cache first
            cache_key = f'pool_metadata:{pool_address}'
            cached_data = await self.redis.get(cache_key)
            if cached_data:
                return json.loads(cached_data)

            # Create pool contract instance
            pool_contract = self.web3.eth.contract(address=pool_address, abi=self.POOL_ABI)

            # Get basic pool data
            try:
                token0_address = await pool_contract.functions.token0().call()
                token1_address = await pool_contract.functions.token1().call()
                factory_address = await pool_contract.functions.factory().call()
                fee = await pool_contract.functions.fee().call()
                tick_spacing = await pool_contract.functions.tickSpacing().call()
            except Exception as e:
                self.logger.error(f"Failed to get basic pool data for {pool_address}: {str(e)}")
                return None

            # Normalize addresses
            token0_address = Web3.to_checksum_address(token0_address)
            token1_address = Web3.to_checksum_address(token1_address)
            factory_address = Web3.to_checksum_address(factory_address)

            # Get token metadata
            token0_metadata = await self._get_erc20_metadata(token0_address)
            token1_metadata = await self._get_erc20_metadata(token1_address)

            if not token0_metadata or not token1_metadata:
                self.logger.error(f"Failed to get token metadata for pool {pool_address}")
                return None

            # Build metadata
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
                'tick_spacing': tick_spacing,
                'factory': factory_address
            }

            # Cache the result
            await self.redis.set(cache_key, json.dumps(pool_metadata), ex=3600)
            return pool_metadata

        except Exception as e:
            self.logger.error(f"Error getting pool metadata for {pool_address}: {str(e)}")
            return None

    async def _get_erc20_metadata(self, token_address: Union[str, Address]) -> Dict[str, Any]:
        """Get ERC20 token metadata with robust error handling."""
        try:
            token_address = Web3.to_checksum_address(token_address)

            # Check cache first
            cache_key = f'erc20_metadata:{token_address}'
            cached_data = await self.redis.get(cache_key)
            if cached_data:
                return json.loads(cached_data)

            # Create token contract
            token_contract = self.web3.eth.contract(address=token_address, abi=self.ERC20_ABI)

            # Get metadata with fallbacks
            try:
                name = await token_contract.functions.name().call()
            except Exception:
                name = "Unknown Token"

            try:
                symbol = await token_contract.functions.symbol().call()
            except Exception:
                symbol = "UNKNOWN"

            try:
                decimals = await token_contract.functions.decimals().call()
            except Exception:
                decimals = 18  # Default to 18 decimals

            metadata = {
                'name': name,
                'symbol': symbol,
                'decimals': decimals
            }

            # Cache for 24 hours
            await self.redis.set(cache_key, json.dumps(metadata), ex=86400)
            return metadata

        except Exception as e:
            self.logger.error(f"Error getting ERC20 metadata for {token_address}: {str(e)}")
            return None

    def get_key_event_topics(self) -> List[str]:
        """Get the list of event topic signatures characteristic of UniswapV3Pool contracts."""
        return [
            '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',  # Swap
            '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde',  # Mint
            '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',  # Burn
            '0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633',  # Flash
        ]


if __name__ == "__main__":
    import asyncio
    
    async def main():
        rpc_url = "https://rpc-eth-lb.blockvigil.com/v1/ca71e8fda51e1985672de5f5349f5363e369a7be"
        web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url))
        
        try:
            redis = await aioredis.from_url("redis://localhost:6379")
            await redis.ping()
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            raise
            
        detector = UniswapV3PoolDetector(web3, redis)
        
        # Test with a known Uniswap V3 pool
        test_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"  # USDC/WETH 0.05%
        result = await detector.is_uniswap_v3_pool(test_address)
        print(f"Is UniswapV3Pool ({test_address}): {result}")
        
        # Test metadata retrieval
        metadata = await detector.get_pool_metadata(test_address)
        print(f"Pool metadata: {metadata}")
    
    asyncio.run(main())