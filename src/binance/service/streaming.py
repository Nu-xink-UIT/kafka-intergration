import asyncio
from datetime import datetime, timezone
from src.binance.core.logger import get_logger

from schemas.binance_schema import validate_binance_data, KAFKA_BINANCE_SCHEMA

logger = get_logger(__name__)

class BinanceStreamingService:
    def __init__(self, client, producer, topic: str):
        self.client = client
        self.producer = producer
        self.topic = topic
        self.running = False

    async def start(self):
        """Bắt đầu dịch vụ streaming, nghe dữ liệu từ Binance"""
        self.running = True
        logger.info("Binance Streaming Service đã bắt đầu (Chế độ Pydantic & Parquet).")

        async for data in self.client.listen():
            if not self.running:
                break
            try:
                # 1. Thêm metadata thời gian lấy dữ liệu
                data["fetched_at"] = datetime.now(timezone.utc).isoformat()

                # BƯỚC 2: ĐƯA DATA QUA PYDANTIC
                clean_data = validate_binance_data(data)

                # BƯỚC 3: BỎ QUA NẾU LÀ DỮ LIỆU RÁC
                if not clean_data:
                    continue

                # Lấy key là tên đồng coin từ dữ liệu đã được làm sạch
                message_key = clean_data.get("s", "unknown")

                # BƯỚC 4: ĐÓNG GÓI SCHEMA VÀ PAYLOAD
                parquet_ready_message = {
                    "schema": KAFKA_BINANCE_SCHEMA,
                    "payload": clean_data
                }

                # 5. Gửi vào Kafka
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