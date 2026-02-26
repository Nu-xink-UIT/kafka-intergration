import aiohttp
import ssl
import certifi


class GoldPriceClient:

    def __init__(self, url: str):
        self.url = url
        self.ssl_context = ssl.create_default_context(
            cafile=certifi.where()
        )

    async def fetch(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, ssl=self.ssl_context) as response:
                response.raise_for_status()
                return await response.json(content_type=None)
