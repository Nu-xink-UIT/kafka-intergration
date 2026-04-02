import os
from datetime import datetime
import pytz

class Settings:
    KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS'', ''localhost:9092')

    # topic list
    TOPIC_RATES = 'goldprice.rates.raw'
    TOPIC_PERFORMANCE = 'goldprice.performance.raw'

    # top 30 currency code influenced Vietnam
    TIER_1_CURRENCIES = ['USD', 'VND', 'CNY', 'EUR', 'GBP', 'JPY', 'KRW', 'SGD', 'HKD', 'TWD']
    TIER_2_CURRENCIES = ['THB', 'MYR', 'GBP', 'CHF', 'AUD', 'CAD', 'NZD', 'INR', 'IDR', 'PHP', 'VND', 'KHR', 'LAK', 'AED', 'SAR', 'QAR', 'KWD', 'BHD', 'OMR', 'RUB', 'TRY', 'PLN']

    # pollingin terval
    INTERVAL_REALTIME_T1 = 60
    INTERVAL_REALTIME_T2 = 60 * 5
    INTERVAL_PERFORMANCE = 3600 * 6 # 6 hours
    INTERVAL_YEARLY = 3600 * 24

    # URL bases
    URL_RATES_BASE = "https://data-asg.goldprice.org/dbXRates/"
    URL_PERFORMANCE_BASE = "https://goldprice.org/performance-json/"

    @staticmethod
    def get_ny_timestamp():
        ny_tz = pytz.timezone('America/New_York')
        return datetime.now(ny_tz).strftime("%Y%m%d-%H%M")


settings = Settings()