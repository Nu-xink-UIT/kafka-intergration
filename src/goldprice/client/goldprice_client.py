from wsgiref import headers

import aiohttp
import ssl
import certifi
import logging

logger = logging.getLogger(__name__)
class GoldPriceClient:

    def __init__(self):
        # Thiết lập SSL để đảm bảo kết nối HTTPS an toàn
        self.ssl_context = ssl.create_default_context(
            cafile=certifi.where()
        )
    async def fetch(self, url:str) -> dict:
        logger.info(f"Begin to fetch data from: {url}")
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://goldprice.org',
                'Referer': 'https://goldprice.org/',
                'Connection': 'keep-alive',
            }
            async with aiohttp.ClientSession(headers=headers) as session:
                # Bước mồi: Truy cập trang chủ trước để nhận session/cookie
                try:
                    await session.get("https://goldprice.org", ssl=self.ssl_context, timeout=5)
                except: pass

                # Gọi API chính thức
                async with session.get(url, ssl=self.ssl_context, timeout=15) as response:
                    if response.status == 502:
                        logger.warning(f"Server Goldprice quá tải (502) tại {url}")
                    response.raise_for_status()
                    return await response.json(content_type=None)
        except aiohttp.ClientError as e:
            logger.error(f"Connection error when calling API: {e}")
            raise
        except Exception as e:
            logger.error(f"Error during API calling: {e}")
            raise
