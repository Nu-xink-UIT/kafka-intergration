from aiokafka import AIOKafkaProducer
import json


class KafkaProducerClient:

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers
        )

    async def start(self):
        await self.producer.start()

    async def stop(self):
        await self.producer.stop()

    async def send(self, topic: str, key: str, value: dict):
        await self.producer.send_and_wait(
            topic,
            json.dumps(value).encode(),
            key=key.encode()
        )