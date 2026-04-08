import asyncio
from datetime import datetime, timedelta, timezone
from src.vcb.core.config import settings
from src.vcb.core.logger import get_logger

logger = get_logger(__name__)

VCB_SCHEMA = {
    "type": "struct",
    "name": "VCBExchangeRateRecord",
    "optional": False,
    "fields": [
        {"field": "fetched_at", "type": "string", "optional": True},
        {"field": "DateTime", "type": "string", "optional": True},
        {"field": "Source", "type": "string", "optional": True},
        {"field": "CurrencyCode", "type": "string", "optional": True},
        {"field": "CurrencyName", "type": "string", "optional": True},
        {"field": "Buy", "type": "string", "optional": True},
        {"field": "Transfer", "type": "string", "optional": True},
        {"field": "Sell", "type": "string", "optional": True}
    ]
}

class VCBPollingService:
    def __init__(self, client, producer):
        self.client = client
        self.producer = producer
        self.running = False

    def get_next_run_time(self):
        """Tính toán thời gian chạy tiếp theo dựa trên cấu hình"""
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
        logger.info("Bắt đầu VCB Polling Service (Chế độ Schema/Parquet)")
        self.running = True

        while self.running:
            wait_seconds, next_run_time = self.get_next_run_time()
            logger.info(f"Tiếp theo sẽ chạy vào {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} (sau {wait_seconds:.2f} giây)")

            # Đợi đến giờ chạy tiếp theo
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
                    raw_data = response["ExrateList"]

                    # 1. Trích xuất Metadata chung
                    fetched_at = datetime.now(timezone.utc).isoformat()
                    date_time = raw_data.get("DateTime")
                    source = raw_data.get("Source")

                    # Lấy danh sách tỷ giá (nếu không có thì trả về list rỗng)
                    exrates = raw_data.get("Exrate", [])

                    # 2. Lặp qua từng đồng tiền để flatten và làm sạch tên cột
                    for rate in exrates:
                        flat_record = {
                            "fetched_at": fetched_at,
                            "DateTime": date_time,
                            "Source": source,
                            # Bỏ chữ '@' và dùng .strip() để xóa khoảng trắng thừa ở tên
                            "CurrencyCode": rate.get("@CurrencyCode"),
                            "CurrencyName": rate.get("@CurrencyName", "").strip(),
                            "Buy": rate.get("@Buy"),
                            "Transfer": rate.get("@Transfer"),
                            "Sell": rate.get("@Sell")
                        }

                        # 3. ĐÓNG GÓI SCHEMA VÀ PAYLOAD
                        parquet_ready_message = {
                            "schema": VCB_SCHEMA,
                            "payload": flat_record
                        }

                        # 4. Gửi kiện hàng hoàn chỉnh vào Kafka
                        await self.producer.send(
                            topic=settings.KAFKA_TOPIC,
                            key=f"{fetched_at}_{flat_record['CurrencyCode']}",
                            value=parquet_ready_message
                        )

                    logger.info(f"Đã trải phẳng và gửi {len(exrates)} bản ghi Parquet-ready vào Kafka tại {fetched_at}")

                else:
                    logger.warning("Dữ liệu API trả về lỗi format hoặc rỗng")
            except Exception as e:
                logger.error(f"Lỗi khi lấy dữ liệu hoặc gửi vào Kafka: {e}")

        logger.info("VCB Polling Service đã dừng hoàn toàn")

    def stop(self):
        self.running = False