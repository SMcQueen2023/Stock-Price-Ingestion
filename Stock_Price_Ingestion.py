from kafka import KafkaProducer
import yfinance as yf
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        try:
            data = get_stock_data(ticker)
            producer.send(topic, value=data)
            logging.info(f"Sent: {data}")
        except Exception as e:
            logging.error(f"Error while producing message: {e}")
        time.sleep(60)  # Fetch data every minute

if __name__ == "__main__":
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",  # Ensure this matches your Kafka broker
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        logging.info("Kafka producer initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Kafka producer: {e}")
        exit(1)

    topic_name = "stock-prices"
    ticker_symbol = "AAPL"  # Replace with the desired stock ticker

    try:
        produce_messages(producer, topic_name, ticker_symbol)
    except KeyboardInterrupt:
        logging.info("Graceful shutdown initiated.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        producer.close()
        logging.info("Kafka producer closed.")
