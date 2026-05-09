import aiohttp
import ssl
import certifi


class GoldPriceClient:

    def __init__(self, url: str):
        self.url = url
        self.ssl_context = ssl.create_default_context(
            cafile=certifi.where()
        )
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Origin': 'https://sjc.com.vn',
            'Referer': 'https://sjc.com.vn'
        }

    async def fetch(self) -> dict:
        async with aiohttp.ClientSession(headers=self.header) as session:
            async with session.get(self.url, ssl=self.ssl_context) as response:
                response.raise_for_status()
                return await response.json(content_type=None)


    # async def fetch(self) -> dict:
    # # Sử dụng kết nối không kiểm tra SSL gắt gao để test
    #     connector = aiohttp.TCPConnector(ssl=False)

    #     async with aiohttp.ClientSession(headers=self.header, connector=connector) as session:
    #         # Bước 1: Ghé thăm trang chủ để "nhận" Cookie
    #         async with session.get("https://sjc.com.vn") as root_res:
    #             await root_res.text()

    #         # Bước 2: Gọi API thực tế
    #         async with session.get(self.url) as response:
    #             print(f"Status: {response.status}")
    #             if response.status == 403:
    #                 # Nếu vẫn 403, thử in ra text để xem họ báo gì (có thể là trang bắt giải Captcha)
    #                 error_body = await response.text()
    #                 print(f"Error detail: {error_body[:200]}")

    #             response.raise_for_status()
    #             return await response.json(content_type=None)
