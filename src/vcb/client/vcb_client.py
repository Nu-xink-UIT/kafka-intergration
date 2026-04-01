import aiohttp
import xmltodict
import ssl
import certifi
from src.vcb.core.logger import get_logger

logger = get_logger(__name__)

class VCBClient:
    def __init__ (self, api_url: str):
        self.api_url = api_url
        self.ssl_context = ssl.create_default_context(
            cafile=certifi.where()
        )
    async def fetch(self) -> dict:
        """Lấy dữ liẹu XML từ VCB và convert sang Dict"""

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.api_url, ssl=self.ssl_context) as response:
                    response.raise_for_status()
                    xml_data = await response.text()
                    return xmltodict.parse(xml_data)
            except Exception as e:
                logger.error(f"Lỗi khi lấy dữ liệu từ VCB:{e}")
                raise