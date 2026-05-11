import aiohttp
import ssl
import certifi
import logging
import os
from aiohttp_socks import ProxyConnector

logger = logging.getLogger(__name__)

class GoldPriceClient:
    def __init__(self):
        self.ssl_context = ssl.create_default_context(
            cafile=certifi.where()
        )

    async def fetch(self, url:str) -> dict:
        logger.info(f"Begin to fetch data from: {url} via SOCKS5")
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://goldprice.org',
                'Referer': 'https://goldprice.org/',
                'Connection': 'keep-alive',
            }

            proxy_url = os.getenv("HTTP_PROXY", "socks5://127.0.0.1:4000")
            connector = ProxyConnector.from_url(proxy_url, rdns=True)

            async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
                try:
                    await session.get("https://goldprice.org", ssl=self.ssl_context, timeout=5)
                except Exception as e:
                    logger.debug(f"Lỗi nhẹ khi mồi cookie (có thể bỏ qua): {e}")

                # Gọi API chính thức
                async with session.get(url, ssl=self.ssl_context, timeout=15) as response:
                    if response.status == 502:
                        logger.warning(f"Server Goldprice quá tải (502) tại {url}")
                    response.raise_for_status()
                    return await response.json(content_type=None)

        except aiohttp.ClientError as e:
            logger.error(f"Connection error when calling API via Proxy: {e}")
            raise
        except Exception as e:
            logger.error(f"Error during API calling via Proxy: {e}")
            raise