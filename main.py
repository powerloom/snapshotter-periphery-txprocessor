import asyncio
from config.loader import get_core_config
from utils.tx_processor import TxProcessor
from utils.logging import logger
from utils.redis.redis_conn import RedisPool # Import for shutdown
from utils.rpc import RpcHelper # Import for shutdown

async def main():
    processor = None # Define processor outside try block for finally clause
    try:
        settings = get_core_config()
        processor = TxProcessor(settings)
        logger.info("🚀 Starting Transaction Processor Service...")
        await processor.start_consuming()
    except Exception as e:
        logger.critical(f"🆘 Service failed to start or crashed: {e}")
        # Perform cleanup if necessary
    finally:
        logger.info("Shutting down resources...")
        if processor and processor.rpc_helper:
            await processor.rpc_helper.close() # Close HTTPX client
        await RedisPool.close() # Close Redis pool
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Service interrupted by user.")
    # `finally` block in main() handles other exceptions and cleanup
