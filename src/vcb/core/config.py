import os

class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vcb_exchange_rates")
    API_URL = os.getenv("API_URL", "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx")

    SCHEDULED_TIMES = [
        (9, 0),   # 09:00 AM
        (14, 0),  # 02:00 PM
        (17, 15)  # 05:15 PM
    ]

settings = Settings()
