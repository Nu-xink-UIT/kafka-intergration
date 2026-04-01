import asyncio
from datetime import datetime, timedelta, timezone
from src.vcb.core.config import settings
from src.vcb.core.logger import get_logger

logger = get_logger(__name__)

class VCBPollingService:
    def __init__(self, client, producer):
        self.client = client
        self.producer = producer
        self.running = False

    def get_next_run_time(self):
        """Tính toán thơi fgian chạy tiếp theo dựa trên cấu hình"""

        vietnam_tz = timezone(timedelta(hours=7))
        now = datetime.now(vietnam_tz)
        upcoming_times = []

        for hour, minunte in settings.SCHEDULED_TIMES:
            scheduled_time = now.replace(hour=hour, minute=minunte, second=0, microsecond=0)
            if scheduled_time > now:
                upcoming_times.append(scheduled_time)

        if not upcoming_times:
            first_h, first_m = settings.SCHEDULED_TIMES[0]
            next_run = (now + timedelta(days=1)).replace(hour=first_h, minute=first_m, second=0, microsecond=0)

        else:
            next_run = min(upcoming_times)

        return (next_run - now).total_seconds(), next_run

    async def start(self):
        """Bắt đầu polling"""

        logger.info("Bắt đầu VCB Polling Service (3 lần/ngày)")
        self.running = True

        while self.running:
            wait_seconds, next_run_time = self.get_next_run_time()
            logger.info(f"Tiếp theo sẽ chạy vào {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} (sau {wait_seconds:.2f} giây)")

            try:
                await asyncio.sleep(wait_seconds)
            except asyncio.CancelledError:
                logger.info("VCB Polling Service đã được dừng")
                break

            if not self.running:
                break

            try:
                response = await self.client.fetch()
                if response and "ExrateList" in response:
                    # lấy dữ liệu chính
                    data = response["ExrateList"]

                    #1. Thêm metadata
                    fetched_at = datetime.now().isoformat()
                    data["fetched_at"] = fetched_at

                    # 2. Gửi dữ liệu vào kafka
                    await self.producer.send(
                        topic=settings.KAFKA_TOPIC,
                        key=fetched_at,
                        value=data
                    )

                    logger.info(f"Đã gửi dữ liệu vào Kafka tại {fetched_at}")

                else:
                    logger.warning("Dữ liệu API trả về lỗi format hoặc rỗng")
            except Exception as e:
                logger.error(f"Lỗi khi lấy dữ liệu hoặc gửi vào Kafka: {e}")

            await asyncio.sleep(60)

        logger.info("VCB Polling Service đã dừng hoàn toàn")

    def stop(self):
        self.running = False