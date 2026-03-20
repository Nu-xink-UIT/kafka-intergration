import asyncio
from src.binance.core.logger import get_logger

logger = get_logger(__name__)

class BinanceStreamingService:
    def __init__(self, client, producer, topic : str):
        self.client = client
        self.producer = producer
        self.topic = topic
        self.running = False

    async def start(self):
        """Bắt đầu dịch vụ streaming, nghe dữ liệu từ Binance và gửi đến Kafka"""
        self.running = True
        logger.info("Binance Streaming Service đã bắt đầu.")

        async for data in self.client.listen():
            if not self.running:
                break
            try:
                key = data.get("s", "unknown")
                await self.producer.send(
                    topic = self.topic, key=key,
                    value=data)

            except Exception as e:
                logger.error(f"Đã xảy ra lỗi khi gửi dữ liệu từ Binance đến Kafka: {e}")

    def stop(self):
        self.running = False
        logger.info("Binance Streaming Service đã dừng.")
