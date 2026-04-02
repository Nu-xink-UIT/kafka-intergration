import asyncio
import signal
import logging
from goldprice.core.config import settings
from goldprice.core.logger import get_logger
from goldprice.core.kafka_producer import KafkaProducerClient
from goldprice.client.goldprice_client import GoldPriceClient
from goldprice.service.polling import GoldPricePollingService


logger = get_logger(__name__)

async def main():
    logger.info(f"GoldPrice Producer Started")

    client = GoldPriceClient()
    producer = KafkaProducerClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVER)
    service = GoldPricePollingService(client=client, producer=producer)

    loop = asyncio.get_running_loop() # gracefull shutdown

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: service.stop())

    try:
        await producer.start() # connect to Kafka before polling
        await service.start()
    except Exception as e:
        logger.error(f"System Error: {e}", exc_info=True)
    finally:
        await service.stop()
        logger.info("GoldPrice Producer Stopped")


if __name__ == "__main__":
    try:
        # use asycion.run to activate main ()
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
