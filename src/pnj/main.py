import asyncio
import signal

from client.pnj_client import GoldPriceClient
from core.config import settings
from core.kafka_producer import KafkaProducerClient
from service.polling import GoldPollingService


async def main():
    producer = KafkaProducerClient(settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    client = GoldPriceClient(settings.API_URL)

    service = GoldPollingService(client, producer)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    task = asyncio.create_task(service.start())

    await stop_event.wait()

    service.stop()
    await producer.stop()
    await task


if __name__ == "__main__":
    asyncio.run(main())
