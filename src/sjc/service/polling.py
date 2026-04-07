import asyncio
from sjc.core.config import settings
from sjc.core.logger import get_logger
import datetime

logger = get_logger(__name__)


class GoldPollingService:

    def __init__(self, client, producer):
        self.client = client
        self.producer = producer
        self.running = False
        self.last_seen_date = None

    async def start(self):
        logger.info("Starting Gold Polling Service at SJC")
        self.running = True

        while self.running:
            try:
                response = await self.client.fetch()
                logger.info(f"Response {response}")
                if not response.get("success"):
                    logger.warning("API returned success=False")
                    continue

                latest_date = response.get("latestDate")
                records = response.get("data", [])


                if not records:
                    logger.warning("No data returned from API")
                    continue

                tasks = []
                if self.last_seen_date == latest_date:
                    logger.info(f"304: Gold price is not modified since {latest_date}")
                else:
                    for record in records:
                        payload = {
                            "fetched_at": datetime.datetime.utcnow().isoformat(),
                            "latestDate": latest_date,
                            **record
                        }


                        tasks.append(
                            self.producer.send(
                                topic=settings.KAFKA_TOPIC,
                                key=str(record.get("Id")),
                                value=payload,
                            )
                        )

                    await asyncio.gather(*tasks)

                    self.last_seen_date = latest_date
                    logger.info(f"Sent {len(records)} records to Kafka. New update: {latest_date}")

            except Exception:
                logger.exception("Polling error")
                self.producer.flush()
            await asyncio.sleep(settings.POLL_INTERVAL)
        self.producer.flush()
        logger.info("Stopping service, flushing producer...")
        logger.info("GoldPollingService stopped")

    def stop(self):
        logger.info("Shutdown signal received")
        self.running = False
