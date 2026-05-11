import aiohttp
import ssl
import certifi
import logging
import os
from aiohttp_socks import ProxyConnector

logger = logging.getLogger(__name__)

class GoldPriceClient:
    def __init__(self):
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def fetch(self, url:str) -> dict:
        logger.info(f"Begin to fetch data from: {url} via SOCKS5")
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Origin': 'https://goldprice.org',
                'Referer': 'https://goldprice.org/',
                'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'Connection': 'keep-alive',
            }

            proxy_url = os.getenv("HTTP_PROXY", "socks5://127.0.0.1:4000")
            connector = ProxyConnector.from_url(proxy_url, rdns=True)

            async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
                try:
                    await session.options("https://data-asg.goldprice.org", ssl=self.ssl_context, timeout=5)
                except Exception as e:
                    pass

                async with session.get(url, ssl=self.ssl_context, timeout=15) as response:
                    if response.status == 403:
                        logger.error(f"Vẫn bị chặn 403 tại {url}. Tường lửa không cho phép IP Proxy này.")
                    elif response.status == 502:
                        logger.warning(f"Server Goldprice quá tải (502) tại {url}")

                    response.raise_for_status()
                    return await response.json(content_type=None)

        except aiohttp.ClientError as e:
            logger.error(f"Lỗi kết nối HTTP: {e}")
            raise
        except Exception as e:
            logger.error(f"Lỗi hệ thống khi gọi API: {e}")
            raise