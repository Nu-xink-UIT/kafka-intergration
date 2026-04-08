import asyncio
from datetime import datetime, timezone
from src.binance.core.logger import get_logger

logger = get_logger(__name__)


BINANCE_SCHEMA = {
    "type": "struct",
    "name": "BinanceRecord",
    "optional": False,
    "fields": [
        {"field": "fetched_at", "type": "string", "optional": True},
        {"field": "e", "type": "string", "optional": True},
        {"field": "E", "type": "int64", "optional": True},
        {"field": "s", "type": "string", "optional": True},
        {"field": "p", "type": "string", "optional": True},
        {"field": "P", "type": "string", "optional": True},
        {"field": "w", "type": "string", "optional": True},
        {"field": "x", "type": "string", "optional": True},
        {"field": "c", "type": "string", "optional": True},
        {"field": "Q", "type": "string", "optional": True},
        {"field": "b", "type": "string", "optional": True},
        {"field": "B", "type": "string", "optional": True},
        {"field": "a", "type": "string", "optional": True},
        {"field": "A", "type": "string", "optional": True},
        {"field": "o", "type": "string", "optional": True},
        {"field": "h", "type": "string", "optional": True},
        {"field": "l", "type": "string", "optional": True},
        {"field": "v", "type": "string", "optional": True},
        {"field": "q", "type": "string", "optional": True},
        {"field": "O", "type": "int64", "optional": True},
        {"field": "C", "type": "int64", "optional": True},
        {"field": "F", "type": "int64", "optional": True},
        {"field": "L", "type": "int64", "optional": True},
        {"field": "n", "type": "int64", "optional": True}
    ]
}

class BinanceStreamingService:
    def __init__(self, client, producer, topic: str):
        self.client = client
        self.producer = producer
        self.topic = topic
        self.running = False

    async def start(self):
        """Bắt đầu dịch vụ streaming, nghe dữ liệu từ Binance"""
        self.running = True
        logger.info("Binance Streaming Service đã bắt đầu.")

        async for data in self.client.listen():
            if not self.running:
                break
            try:
                # 1. Thêm metadata thời gian lấy dữ liệu
                data["fetched_at"] = datetime.now(timezone.utc).isoformat()

                # Lấy key là tên đồng coin
                message_key = data.get("s", "unknown")

                # 2. ĐÓNG GÓI DỮ LIỆU 
                parquet_ready_message = {
                    "schema": BINANCE_SCHEMA,
                    "payload": data
                }

                # 3. Gửi kiện hàng hoàn chỉnh vào Kafka
                await self.producer.send(
                    topic=self.topic,
                    key=message_key,
                    value=parquet_ready_message
                )

            except Exception as e:
                logger.error(f"Đã xảy ra lỗi khi gửi dữ liệu từ Binance đến Kafka: {e}")

    def stop(self):
        self.running = False
        logger.info("Binance Streaming Service đã dừng.")