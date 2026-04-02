import datetime
import asyncio
from goldprice.core.config import settings
from goldprice.core.logger import get_logger

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

            payload = {
                "fetched_at": datetime.datetime.utcnow().isoformat(),
                **data
                }
            await self.producer.send(topic=topic, key=curr, value=payload)
            return True

    async def poll_rates(self, currencies: list, interval: int, group_name):
        while self.running:
            logger.info(f"{group_name}: Process {len(currencies)} currency codes")
            tasks = [self._fetch_rates(curr, settings.TOPIC_RATES) for curr in currencies]
            results = await asyncio.gather(*tasks)

            success_count = sum(1 for r in results if r)
            fail_count = len(currencies) - success_count

            if fail_count > 0:
                logger.error(f"[{group_name}]: {fail_count} code failed out of {len(currencies)}")
            else:
                logger.info(f"[{group_name}]: Successfully polled {len(currencies)} codes")
            await asyncio.sleep(interval)


    async def _fetch_performance(self, curr, metal, topic):
        async with self.gate:
            ny_ts = settings.get_ny_timestamp()
            logger.info(f"New York Timestamp: {ny_ts}")

            url = f"{settings.URL_PERFORMANCE_BASE}{metal}-price-performance-{curr}.json?v={ny_ts}"

            data = await self.client.fetch(url)

            if data and data.get("Change"):
                payload = {
                    "fetched_at": datetime.datetime.utcnow().isoformat(),
                    "type": "20y_performance",
                    "metal": metal,
                    "curr": curr,
                    **data
                }
                kafka_key = f"{metal}_{curr}"
                await self.producer.send(topic=topic, key=kafka_key, value=payload)
                return True
            return False


    async def poll_performance(self, currencies, interval):
        while self.running:
            logger.info(f"Polling Performance Data")
            tasks = []
            for curr in currencies:
                tasks.append(self._fetch_performance(curr, "gold", settings.TOPIC_PERFORMANCE))
                tasks.append(self._fetch_performance(curr, "silver", settings.TOPIC_PERFORMANCE))

            if tasks:
                results = await asyncio.gather(*tasks)
                success_count = sum(1 for r in results if r)
                logger.info(f"Finished Polling Performance: ({success_count/len(tasks)})")
            await asyncio.sleep(interval)

    async def poll_yearly(self, metal, interval):
        while self.running:
            ny_ts = settings.get_ny_timestamp()
            logger.info(f"New York Timestamp: {ny_ts}")
            logger.info(f"Start polling 8 big countries in 16 years for {metal}")
            url = f"{settings.URL_PERFORMANCE_BASE}{metal}-price_performance_x.json?v={ny_ts}"
            data = await self.client.fetch(url)

            if data and data.get("data"):
                payload = {
                    "fetched_at": datetime.datetime.utcnow().isoformat(),
                    "type": "8-big-cats",
                    "metal": metal,
                    **data
                }
                await self.producer.send(topic=settings.TOPIC_PERFORMANCE, key=metal, value=payload)
                logger.info(f"Loaded data of 8 big countries in 16 years for {metal} into topic {settings.TOPIC_PERFORMANCE}")
            else:
                logger.error(f"Failed to load data of 8 big countries in 16 years for {metal} into topic {settings.TOPIC_PERFORMANCE}")
            await asyncio.sleep(interval)



    async def start(self):
        self.running = True
        logger.info("Starting Gold Polling Service")
        all_code = settings.TIER_1_CURRENCIES + settings.TIER_2_CURRENCIES
        await asyncio.gather(
            self.poll_rates(settings.TIER_1_CURRENCIES, settings.INTERVAL_REALTIME_T1, "TIER_01"),
            self.poll_rates(settings.TIER_2_CURRENCIES, settings.INTERVAL_REALTIME_T2, "TIER_02"),
            self.poll_performance(all_code, settings.INTERVAL_PERFORMANCE),
            self.poll_yearly("gold" ,settings.INTERVAL_YEARLY),
            self.poll_yearly("silver" ,settings.INTERVAL_YEARLY),
        )


    def stop(self):
        logger.info("Stop Polling Service")
        self.running = False




