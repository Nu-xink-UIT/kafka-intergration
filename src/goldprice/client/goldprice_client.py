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
        logger.info("Begin to fetch data from: {url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=self.ssl_context, timeout=15) as response:
                    response.raise_for_status()
                    data = await response.json(content_type=None)
                    logger.info("Successfully fetched data")
                    return data
        except aiohttp.ClientError as e:
            logger.error("Connection error when calling API: ", e)
            raise
        except Exception as e:
            logger.error("Error during API calling: ", e)
            raise
