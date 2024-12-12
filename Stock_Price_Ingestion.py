from kafka import KafkaProducer
print("KafkaProducer imported successfully!")
import yfinance as yf
import json
import time

import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

def get_stock_data(ticker):
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1d", interval="1m")
    last_quote = hist.iloc[-1]
    return {
        "timestamp": last_quote.name.strftime("%Y-%m-%d %H:%M:%S"),
        "ticker": ticker,
        "open": last_quote["Open"],
        "high": last_quote["High"],
        "low": last_quote["Low"],
        "close": last_quote["Close"],
        "volume": last_quote["Volume"]
    }

def produce_messages(producer, topic, ticker):
    while True:
        data = get_stock_data(ticker)
        producer.send(topic, value=data)
        print(f"Sent: {data}")
        time.sleep(60)  # Fetch data every minute

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    topic_name = "stock-prices"
    ticker_symbol = "AAPL"  # Replace with the desired stock ticker
    produce_messages(producer, topic_name, ticker_symbol)