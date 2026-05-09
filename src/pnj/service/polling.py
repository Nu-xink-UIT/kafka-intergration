import asyncio
import datetime

from pnj.core.config import settings
from pnj.core.logger import get_logger
from pnj.core.schemas import PNJRecord


logger = get_logger(__name__)


class GoldPollingService:

    def __init__(self, client, producer):
        self.client = client
        self.producer = producer
        self.running = False
        self.last_update_tracker = {}
        # Use dict to store last_seen_date for each product_id

    async def start(self):
        logger.info("Starting Gold Polling Service at PNJ (Alternative source)")
        self.running = True

        while self.running:
            try:
                response = await self.client.fetch()
                logger.info(f"Response {response}")
                if not response.get("success"):
                    logger.warning("API returned success=False")
                    continue

                locations = response.get('locations', []) # locations here is a list
                if not locations:
                    logger.error('No locations data returned from API')
                    continue
                tasks = []
                current_id = 1
                for loc in locations:
                    raw_branch_name = loc.get('name')
                    branch_name = 'Hồ Chí Minh' if raw_branch_name == 'TPHCM' else raw_branch_name
                    if raw_branch_name == 'Giá vàng nữ trang':
                        branch_name = 'Hồ Chí Minh'
                    gold_types = loc.get('gold_type', [])
                    for gold in gold_types:
                        gold_name = gold.get('name')
                        updated_at = gold.get('updated_at')
                        is_sjc = (gold_name.upper() == 'SJC')
                        is_nu_trang = (raw_branch_name == 'Giá vàng nữ trang')
                        if not (is_sjc or is_nu_trang):
                            continue
                        # Check update for each produce
                        tracker_key = f'{branch_name}_{gold_name}'
                        if self.last_update_tracker.get(tracker_key) == updated_at:
                            continue
                            # Do not store if there is no change
                        type_name = 'Vàng SJC 1 lượng' if is_sjc else gold_name
                        try:
                            data = PNJRecord(
                                fetched_at=datetime.datetime.utcnow().isoformat(),
                                latestDate=updated_at,
                                Id=current_id,
                                TypeName=type_name,
                                BranchName=branch_name,
                                BuyValue=gold.get('gia_mua'),
                                SellValue=gold.get('gia_ban')
                            )
                            payload = data.model_dump()

                            tasks.append(
                                self.producer.send(
                                    topic=settings.KAFKA_TOPIC,
                                    key=current_id,
                                    value=payload,
                                )
                            )
                            self.last_update_tracker[tracker_key] = updated_at
                            current_id =+ 1
                        except Exception as e:
                            logger.error(f'Error validating record:{branch_name}_{gold_name}: {e}')
                if tasks:
                    await asyncio.gather(*tasks)
                    logger.info(f'Successfully sent {len(tasks)} new/update records to Kafka')
                else:
                    logger.info("No new updates found in this poll")
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


