import json
import time
import logging
import signal
import sys
from kafka import KafkaProducer
import yfinance as yf
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global flag for graceful shutdown
terminate = False

def signal_handler(sig, frame):
    """
    Signal handler for graceful shutdown.
    """
    global terminate
    logging.info("Termination signal received. Shutting down...")
    terminate = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_stock_data(ticker):
    """
    Fetches the latest stock data for the given ticker symbol.
    """
    try:
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
    except Exception as e:
        logging.error(f"Failed to fetch stock data for {ticker}: {e}")
        return None

def produce_messages(producer, topic, ticker):
    """
    Sends stock data messages to a Kafka topic.
    """
    while not terminate:
        data = get_stock_data(ticker)
        if data:
            try:
                producer.send(topic, value=data).add_callback(
                    lambda metadata: logging.info(f"Message sent to {metadata.topic}:{metadata.partition}")
                ).add_errback(
                    lambda exc: logging.error(f"Failed to send message: {exc}")
                )
                producer.flush()
            except KafkaError as e:
                logging.error(f"Kafka error: {e}")
        time.sleep(60)

def main():
    kafka_broker = "127.0.0.1:9092"  # Ensure IPv4 is used
    kafka_topic = "stock_data"
    stock_ticker = "AAPL"  # Or load dynamically

    producer = None
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("Kafka producer initialized.")

        # Start producing messages
        produce_messages(producer, kafka_topic, stock_ticker)
    except KafkaError as e:
        logging.error(f"Error in Kafka producer: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if producer:
            logging.info("Closing Kafka producer...")
            producer.close()

if __name__ == "__main__":
    main()