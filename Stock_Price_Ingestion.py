from kafka import KafkaProducer
import yfinance as yf
import json
import time
import logging
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_stock_data(ticker):
    """
    Fetch the latest stock data for a given ticker using yfinance.
    """
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d", interval="1m")
        if hist.empty:
            logging.warning(f"No data retrieved for ticker: {ticker}")
            return None
        
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
        logging.error(f"Error fetching stock data for ticker {ticker}: {e}")
        return None

def produce_messages(producer, topic, ticker):
    """
    Continuously fetch stock data and send it to the Kafka topic.
    """
    while True:
        try:
            data = get_stock_data(ticker)
            if data:
                producer.send(topic, value=data).add_callback(on_send_success).add_errback(on_send_error)
                logging.info(f"Sent: {data}")
            else:
                logging.warning(f"No data to send for ticker: {ticker}")
        except KafkaError as ke:
            logging.error(f"Kafka error while producing message: {ke}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        time.sleep(60)  # Fetch data every minute

def on_send_success(record_metadata):
    """
    Callback on successful send to Kafka.
    """
    logging.info(f"Message sent successfully to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    """
    Callback on error during send to Kafka.
    """
    logging.error(f"Error while sending message: {excp}")

if __name__ == "__main__":
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",  # Update with your Kafka broker details
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        logging.info("Kafka producer initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Kafka producer: {e}")
        exit(1)

    # Define Kafka topic and stock ticker
    topic_name = "stock-prices"  # Ensure this topic exists in your Kafka broker
    ticker_symbol = "AAPL"  # Replace with the desired stock ticker symbol

    try:
        produce_messages(producer, topic_name, ticker_symbol)
    except KeyboardInterrupt:
        logging.info("Graceful shutdown initiated by user.")
    except Exception as e:
        logging.error(f"Unexpected error during message production: {e}")
    finally:
        try:
            producer.close()
            logging.info("Kafka producer closed successfully.")
        except Exception as e:
            logging.error(f"Error while closing Kafka producer: {e}")