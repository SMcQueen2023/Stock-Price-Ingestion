import json  # For serializing Python objects into JSON format
import time  # To implement delays (e.g., sleep)
import logging  # To log information, errors, and other messages
import signal  # To handle termination signals for graceful shutdown
import sys  # For handling system-specific parameters and exit
from kafka import KafkaProducer  # For producing messages to a Kafka topic
import yfinance as yf  # To fetch stock data from Yahoo Finance
from kafka.errors import KafkaError  # For handling Kafka errors

# Configure logging to display log messages with timestamps, log level, and message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global flag to control graceful shutdown of the producer
terminate = False

def signal_handler(sig, frame):
    """
    Handles termination signals (SIGINT/SIGTERM) for graceful shutdown of the producer.
    """
    global terminate
    logging.info("Termination signal received. Shutting down...")
    terminate = True

# Register signal handlers for SIGINT (Ctrl+C) and SIGTERM (termination signal)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_stock_data(ticker):
    """
    Fetches the latest stock data for a given ticker symbol from Yahoo Finance.
    The data includes timestamp, open, high, low, close prices, and volume for the last minute.
    """
    try:
        stock = yf.Ticker(ticker)  # Get the stock object for the given ticker
        hist = stock.history(period="1d", interval="1m")  # Fetch 1-minute interval data for 1 day
        last_quote = hist.iloc[-1]  # Get the most recent quote (last row)
        
        # Return the relevant data as a dictionary
        return {
            "timestamp": last_quote.name.strftime("%Y-%m-%d %H:%M:%S"),  # Format the timestamp
            "ticker": ticker,  # Stock ticker symbol
            "open": last_quote["Open"],  # Opening price
            "high": last_quote["High"],  # Highest price
            "low": last_quote["Low"],  # Lowest price
            "close": last_quote["Close"],  # Closing price
            "volume": last_quote["Volume"]  # Trade volume
        }
    except Exception as e:
        logging.error(f"Failed to fetch stock data for {ticker}: {e}")
        return None  # Return None if data fetching fails

def produce_messages(producer, topic, ticker):
    """
    Continuously sends stock data to a Kafka topic at regular intervals (every minute).
    Stops if the terminate flag is set.
    """
    while not terminate:  # Loop runs until termination signal is received
        data = get_stock_data(ticker)  # Get the latest stock data for the ticker
        if data:  # If data was successfully retrieved
            try:
                # Send the stock data to the specified Kafka topic
                producer.send(topic, value=data).add_callback(
                    lambda metadata: logging.info(f"Message sent to {metadata.topic}:{metadata.partition}")  # Log on success
                ).add_errback(
                    lambda exc: logging.error(f"Failed to send message: {exc}")  # Log on failure
                )
                producer.flush()  # Ensure all buffered messages are sent
            except KafkaError as e:
                logging.error(f"Kafka error: {e}")  # Log Kafka-related errors
        time.sleep(60)  # Sleep for 60 seconds (1 minute) before sending the next message

def main():
    # Kafka broker and topic configuration
    kafka_broker = "127.0.0.1:9092"  # Ensure IPv4 address is used for the Kafka broker
    kafka_topic = "stock_data"  # Kafka topic where stock data will be sent
    stock_ticker = "AAPL"  # Stock ticker to track (can be updated dynamically)

    producer = None
    try:
        # Initialize Kafka producer with necessary configurations
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,  # Kafka broker address
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the stock data as JSON
        )
        logging.info("Kafka producer initialized.")  # Log producer initialization

        # Start sending messages to Kafka
        produce_messages(producer, kafka_topic, stock_ticker)
    except KafkaError as e:
        logging.error(f"Error in Kafka producer: {e}")  # Log Kafka errors during producer setup
    except Exception as e:
        logging.error(f"Unexpected error: {e}")  # Log other unexpected errors
    finally:
        if producer:  # If producer was initialized
            logging.info("Closing Kafka producer...")  # Log closing the producer
            producer.close()  # Close the Kafka producer to release resources

# Start the main function if the script is executed directly
if __name__ == "__main__":
    main()