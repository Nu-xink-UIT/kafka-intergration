import datetime
import asyncio
from goldprice.core.config import settings
from goldprice.core.logger import get_logger
from goldprice.core.schemas import GoldPriceRecord

logger = get_logger(__name__)

class GoldPricePollingService:
    def __init__(self, client, producer):
        self.client = client
        self.producer = producer
        self.gate = asyncio.Semaphore(2)
        self.running = False

    async def _fetch_rates(self, curr, topic):
        async with self.gate:
            url = f"{settings.URL_RATES_BASE}{curr}"
            data = await self.client.fetch(url)

            if not data or not data.get("items"):
                logger.warning(f"Currency code: {curr} - API returned empty list")
                return False
            try:
                items_data = data.get['items'][0]

                raw_payload = {
                    "fetched_at": datetime.datetime.utcnow().isoformat(),
                    "ts": data.get("ts"),
                    "tsj": data.get("tsj"),
                    "date": data.get("date"),
                    **items_data
                    }
                # Force data into schema for validation
                validated_payload = GoldPriceRecord(**raw_payload)
                # model_dump() transform object Pydantic into dictionary for Kafka
                final_payload = validated_payload.model_dump()
                await self.producer.send(topic=topic, key=curr, value=final_payload)
            except Exception as e:
                logger.error(f"Schema validation failed for {curr}: {e}")
            return True

    async def poll_rates(self, currencies: list, interval: int):
        while self.running:
            logger.info(f"Process {len(currencies)} currency codes: {currencies}")
            tasks = [self._fetch_rates(curr, settings.TOPIC_RATES) for curr in currencies]
            results = await asyncio.gather(*tasks)

            success_count = sum(1 for r in results if r)
            fail_count = len(currencies) - success_count

            if fail_count > 0:
                logger.error(f"{fail_count} code failed out of {len(currencies)}")
            else:
                logger.info(f"Successfully polled {len(currencies)} codes")
            await asyncio.sleep(interval)

    async def start(self):
        self.running = True
        logger.info("Starting Gold Polling Service")
        await asyncio.gather(
            self.poll_rates(settings.CURRENCIES, settings.INTERVAL_REALTIME),
        )

    def stop(self):
        logger.info("Stop Polling Service")
        self.running = False


