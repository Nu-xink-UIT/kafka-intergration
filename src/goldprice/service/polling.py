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
        self.gate = asyncio.Semaphore(1)
        self.running = False
        self.last_seen_ts = {}

    async def _fetch_rates(self, curr, topic):
        """Xử lý lấy dữ liệu cho một mã tiền tệ cụ thể"""
        async with self.gate:
            # TẠO ĐỘ TRỄ 1.5 GIÂY GIỮA CÁC MÃ ĐỂ LÁCH RATE LIMIT CỦA TƯỜNG LỬA
            await asyncio.sleep(1.5)

            url = f"{settings.URL_RATES_BASE}{curr}"

            try:
                data = await self.client.fetch(url)
            except Exception as e:
                logger.error(f"Lỗi kết nối hoặc API khi lấy {curr}: {e}")
                return False

            if not data or not data.get("items"):
                logger.warning(f"Mã {curr}: API trả về danh sách trống hoặc lỗi định dạng")
                return False

            current_ts = data.get("ts")

            # Bỏ qua nếu dữ liệu chưa có cập nhật mới
            if self.last_seen_ts.get(curr) == current_ts:
                logger.info(f"304: Gold price for {curr} is not modified since {current_ts}")
                return True

            try:
                items_data = data.get('items')[0]

                raw_payload = {
                    "fetched_at": datetime.datetime.utcnow().isoformat(),
                    "ts": current_ts,
                    "tsj": data.get("tsj"),
                    "date": data.get("date"),
                    **items_data
                }

                validated_payload = GoldPriceRecord(**raw_payload)
                final_payload = validated_payload.model_dump()

                await self.producer.send(topic=topic, key=curr, value=final_payload)

                self.last_seen_ts[curr] = current_ts
                logger.info(f"Sent record for {curr} to Kafka. New update: {current_ts}")

                return True
            except Exception as e:
                logger.error(f"Lỗi xác thực Schema hoặc gửi Kafka cho {curr}: {e}")
                return False

    async def poll_rates(self, currencies: list, interval: int):
        """Vòng lặp chính thực hiện lấy dữ liệu định kỳ"""
        while self.running:
            logger.info(f"Bắt đầu chu kỳ lấy dữ liệu cho {len(currencies)} mã: {currencies}")

            tasks = [self._fetch_rates(curr, settings.TOPIC_RATES) for curr in currencies]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            success_count = sum(1 for r in results if r is True)

            if success_count == len(currencies):
                logger.info(f"Hoàn thành chu kỳ: Thành công 100% ({success_count}/{len(currencies)})")
            else:
                logger.warning(f"Hoàn thành chu kỳ: Chỉ thành công {success_count}/{len(currencies)}")

            await asyncio.sleep(interval)

    async def start(self):
        """Khởi chạy dịch vụ"""
        self.running = True
        logger.info("Gold Polling Service đã khởi động (Chế độ chống sập, chống rate-limit và chống spam 304)")
        try:
            await self.poll_rates(settings.CURRENCIES, settings.INTERVAL_REALTIME)
        except Exception as e:
            logger.critical(f"Lỗi nghiêm trọng không thể phục hồi trong Service: {e}")
        finally:
            self.running = False

    def stop(self):
        """Dừng dịch vụ"""
        logger.info("Đang phát lệnh dừng Polling Service...")
        self.running = False