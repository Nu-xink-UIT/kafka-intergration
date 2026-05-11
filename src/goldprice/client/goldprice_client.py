import ssl
import certifi
import logging
import os
import asyncio
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

class GoldPriceClient:
    def __init__(self):
        pass

    async def fetch(self, url:str, max_retries: int = 3) -> dict:
        logger.info(f"Begin to fetch data from: {url} with impersonate=chrome124")

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Origin': 'https://goldprice.org',
            'Referer': 'https://goldprice.org/',
        }

        proxy_url = os.getenv("HTTP_PROXY")
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        for attempt in range(1, max_retries + 1):
            try:
                async with AsyncSession(impersonate="chrome124", proxies=proxies) as session:
                    try:
                        await session.options("https://data-asg.goldprice.org", timeout=5)
                    except: pass

                    response = await session.get(url, headers=headers, timeout=15)

                    if response.status_code == 403:
                        logger.error(f"Bị chặn 403 tại {url}. Máy chủ từ chối.")
                        response.raise_for_status()

                    elif response.status_code == 502:
                        if attempt < max_retries:
                            wait_time = 2 * attempt # Chờ 2s, 4s...
                            logger.warning(f"Server Goldprice quá tải (502) tại {url}. Đang chờ {wait_time}s để thử lại lần {attempt + 1}/{max_retries}...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Đã thử {max_retries} lần nhưng server vẫn báo 502 tại {url}.")
                            response.raise_for_status() # Bỏ cuộc sau 3 lần

                    response.raise_for_status()
                    return response.json()

            except Exception as e:
                if attempt < max_retries and ("502" in str(e) or "timeout" in str(e).lower()):
                    wait_time = 2 * attempt
                    logger.warning(f"Gặp lỗi mạng/502 tại {url}. Đang chờ {wait_time}s để thử lại lần {attempt + 1}...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Lỗi hệ thống khi gọi API tại {url}: {e}")
                    raise