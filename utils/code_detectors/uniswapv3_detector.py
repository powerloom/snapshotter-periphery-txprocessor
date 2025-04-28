import binascii
from typing import Union, List
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
                contract = self.web3.eth.contract(address=address, abi=self.MINIMAL_ABI)
                
                # Try to call all view functions - will raise exception if any fails
                factory = await contract.functions.factory().call()
                token0 = await contract.functions.token0().call()
                token1 = await contract.functions.token1().call()
                fee = await contract.functions.fee().call()
                
                pool_metadata = {
                    'factory': factory,
                    'token0': token0,
                    'token1': token1,
                    'fee': fee
                }
                # Store the result in Redis
                await self.redis.set(f'uniswapv3_pool:{address}', json.dumps(pool_metadata))
                
                # Verify addresses are valid
                Web3.to_checksum_address(factory)
                Web3.to_checksum_address(token0)
                Web3.to_checksum_address(token1)
                
                # Uniswap V3 fees are usually one of these values: 500 (0.05%), 3000 (0.3%), 10000 (1%)
                # But we'll just check it's a valid uint24
                if isinstance(fee, int) and 0 <= fee < 2**24:
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