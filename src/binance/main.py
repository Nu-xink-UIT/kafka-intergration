import asyncio
import signal
from src.binance.client.binance_client import BinanceWebSocketClient
from src.binance.core.config import settings
from src.binance.core.logger import get_logger
from src.binance.service.streaming import BinanceStreamingService
from src.binance.core.kafka_producer import KafkaProducerClient

logger = get_logger(__name__)

async def main():
    producer = KafkaProducerClient(settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    client = BinanceWebSocketClient(settings.BINANCE_WS_URL)
    streaming_service = BinanceStreamingService(client, producer, settings.BINANCE_TOPIC)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def shutdown():
        logger.info("Tín hiệu dừng nhận được...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    # Chạy service
    task = asyncio.create_task(streaming_service.start())

    # Chờ cho đến khi có tín hiệu dừng HOẶC task bị lỗi
    pending = [task, stop_event.wait()]
    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

    logger.info("Đang dọn dẹp tài nguyên...")
    streaming_service.stop()

    # Đợi một chút để các tin nhắn đang gửi (in-flight) được đi hết
    await asyncio.sleep(1)
    await producer.stop()

    # Hủy các task còn sót lại nếu có
    for p in pending:
        p.cancel()

if __name__ == "__main__":
    asyncio.run(main())

