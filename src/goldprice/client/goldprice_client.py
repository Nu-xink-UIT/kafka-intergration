import ssl
import certifi
import logging
import os
from curl_cffi.requests import AsyncSession # Dùng thư viện giả lập Chrome

logger = logging.getLogger(__name__)

class GoldPriceClient:
    def __init__(self):
        pass

    async def fetch(self, url:str) -> dict:
        logger.info(f"Begin to fetch data from: {url} with impersonate=chrome124")
        try:
            # Header nhẹ nhàng hơn vì thư viện đã lo phần lõi giả lập
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://goldprice.org',
                'Referer': 'https://goldprice.org/',
            }

            proxy_url = os.getenv("HTTP_PROXY") # Lấy proxy nếu có cấu hình trong YAML
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

            # impersonate="chrome124" là tính năng "ăn tiền" giúp qua mặt Cloudflare
            async with AsyncSession(impersonate="chrome124", proxies=proxies) as session:

                try:
                    await session.options("https://data-asg.goldprice.org", timeout=5)
                except: pass

                response = await session.get(url, headers=headers, timeout=15)

                if response.status_code == 403:
                    logger.error(f"Vẫn bị chặn 403 tại {url}.")
                elif response.status_code == 502:
                    logger.warning(f"Server Goldprice quá tải (502) tại {url}")

                response.raise_for_status()
                return response.json()

        except Exception as e:
            logger.error(f"Lỗi hệ thống khi gọi API: {e}")
            raise