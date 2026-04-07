import os
from datetime import datetime
import pytz

class Settings:
    KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    # topic list
    TOPIC_RATES = 'goldprice.raw'

    # top 20 currency code influenced Vietnam
    CURRENCIES = ['VND', 'AUD', 'CAD', 'CHF', 'CNY', 'DKK', 'EUR', 'GBP', 'HKD', 'INR', 'JPY', 'KRW', 'KWD', 'MYR', 'NOK', 'RUB', 'SAR', 'SEK', 'SGD', 'THB', 'USD']
    # pollingin terval
    INTERVAL_REALTIME = 60

    # URL bases
    URL_RATES_BASE = "https://data-asg.goldprice.org/dbXRates/"

    @staticmethod
    def get_ny_timestamp():
        ny_tz = pytz.timezone('America/New_York')
        return datetime.now(ny_tz).strftime("%Y%m%d-%H%M")


settings = Settings()