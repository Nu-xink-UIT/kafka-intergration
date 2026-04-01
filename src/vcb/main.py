import asyncio
import signal

from src.vcb.core.config import settings
from src.vcb.core.logger import get_logger
from src.vcb.client.vcb_client import VCBClient
from src.vcb.service.polling import VCBPollingService
from src.vcb.core.kafka_producer import KafkaProducerClient

logger = get_logger(__name__)

async def main():
    producer = KafkaProducerClient(settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    client = VCBClient(settings.API_URL)

    service = VCBPollingService(client, producer)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        logger.info("Đang dừng...")
        stop_event.set()
        service.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    task = asyncio.create_task(service.start())
    await stop_event.wait()
    
    # Bắt đầu dừng hệ thống
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Task Polling đã được hủy và dừng lại.")

    await producer.stop()
    logger.info("Producer đã đóng. Hệ thống dừng hoàn toàn.")

if __name__ == "__main__":
    asyncio.run(main())
