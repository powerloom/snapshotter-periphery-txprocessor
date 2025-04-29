import asyncio
from config.loader import get_core_config
from rpc_helper.rpc import RpcHelper
from utils.tx_processor import TxProcessor
from utils.logging import logger, configure_file_logging
from utils.redis.redis_conn import RedisPool

async def main():
    processor = None
    try:
        settings = get_core_config()
        # Reconfigure logging with settings
        configure_file_logging(
            write_to_files=settings.logs.write_to_files,
        )
        logger.info("🚀 Starting Transaction Processor Service...")
        processor = TxProcessor(settings)
        await processor.start_consuming()
    except Exception as e:
        logger.critical(f"🆘 Service failed to start or crashed: {e}")
        raise
    finally:
        logger.info("Shutting down resources...")
        if processor and processor.rpc_helper:
            await processor.rpc_helper.close()
        await RedisPool.close()
        logger.info("Shutdown complete.")

if __name__ == "__main__":    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Service interrupted by user.")
    except Exception as e:
        logger.critical(f"🆘 Service crashed: {e}")
        raise