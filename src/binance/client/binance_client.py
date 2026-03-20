import asyncio
import websockets
import json
import logging

logger = logging.getLogger(__name__)

class BinanceWebSocketClient:
    def __init__ (self, url: str):
        self.url = url

    async def listen(self):
        """Duy trì kết nối và yeild dữ liệu khi có tín nhắn mới"""

        while True:
            try:
                async with websockets.connect(self.url) as websocket:
                    logger.info(f"Đã kết nối thành công đến Binance WebSocket theo đường dẫn: {self.url}")
                    async for message in websocket:
                        yield json.loads(message)
            except Exception as e:
                logger.error(f"Đã xảy ra lỗi khi kết nối đến Binance WebSocket: {e}")
                logger.info("Đang cố gắng kết nối lại sau 5 giây...")
                await asyncio.sleep(5)

