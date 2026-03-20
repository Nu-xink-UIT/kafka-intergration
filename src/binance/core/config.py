import os

class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    BINANCE_WS_URL= os.getenv('BINANCE_WS_URL', 'wss://stream.binance.com:9443/ws/paxgusdt@ticker')
    BINANCE_TOPIC = os.getenv('BINANCE_TOPIC', 'binance.raw')

settings = Settings()