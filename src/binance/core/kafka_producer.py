import json
import logging
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)

class KafkaProducerClient:
    """Client Kafka Producer sử dụng aiokafka để gửi dữ liệu bất đồng bộ đến Kafka"""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def start(self):
        """Khởi tạo kết nối đến Kafka"""
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers,
        retry_backoff_ms=500,
        acks='all')


        try:
            await self.producer.start()
            logger.info(f"Đã kết nối thành công đến Kafka tại: {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Đã xảy ra lỗi khi kết nối đến Kafka: {e}")
            raise
    async def stop(self):
        """Đóng kết nối đến Kafka"""
        if self.producer:
            await self.producer.stop()
            logger.info("Đã đóng kết nối đến Kafka.")

    async def send(self, topic: str, key: str, value: dict):
        if not self.producer:
            raise Exception("Kafka Producer chưa được khởi tạo. Vui lòng gọi start() trước khi gửi dữ liệu.")
        try:
            payload = json.dumps(value).encode('utf-8')
            message_key = key.encode('utf-8') if key else None

            await self.producer.send_and_wait(topic=topic, value=payload, key=message_key)

        except KafkaError as e:
            logger.error(f"Lỗi khi gửi tin nhắn vào topic {topic}: {e}")