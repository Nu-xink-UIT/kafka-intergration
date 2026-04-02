from aiokafka import AIOKafkaProducer
import json
import logging

logger = logging.getLogger(__name__)
class KafkaProducerClient:

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            retry_backoff_ms=10
        )

    async def start(self):
        try:
            logger.info(f"Connecting with Kafka Broker: {self.bootstrap_servers}")
            await self.producer.start()
            logger.info(f"Successfully connected with Kafka Broker")
        except Exception as e:
            logger.error(f"Could not start Kafka Producer: {e}")
            raise


    async def stop(self):
        logger.info(f"Close connection with Kafka")
        await self.producer.stop()

    async def send(self, topic: str, key: str, value: dict):
        logger.debug(f"Sending data to {topic} with key: {key}")
        try:
            await self.producer.send_and_wait(
                topic,
                json.dumps(value).encode(),
                key = key.encode()
            )
            logger.info(f"Successfully send 1 message to topic: {topic}")

        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            raise