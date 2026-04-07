import os

class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sjc.raw")
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "3600"))
    API_URL = os.getenv(
        "API_URL",
        "https://sjc.com.vn/GoldPrice/Services/PriceService.ashx"
    )

settings = Settings()
