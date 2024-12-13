from kafka import KafkaProducer  # Import KafkaProducer to send messages to a Kafka topic
print("KafkaProducer imported successfully!")

import yfinance as yf  # Import yfinance to fetch stock market data
import json  # Import JSON to serialize data before sending it to Kafka
import time  # Import time to add delays in the message production loop

import six  # Import six for compatibility adjustments
import sys  # Import sys to handle Python version compatibility issues

# Compatibility adjustment for Python 3.12+ to resolve module import issues in Kafka library
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

# Function to fetch stock data for a given ticker symbol
def get_stock_data(ticker):
    """
    Fetches the latest stock data for the given ticker symbol.
    Args:
        ticker (str): The stock ticker symbol (e.g., 'AAPL').
    Returns:
        dict: A dictionary containing the latest stock data, including timestamp, open, high, low, close, and volume.
    """
    stock = yf.Ticker(ticker)  # Create a yfinance Ticker object for the stock
    hist = stock.history(period="1d", interval="1m")  # Get the 1-day history with 1-minute intervals
    last_quote = hist.iloc[-1]  # Extract the latest available quote
    return {
        "timestamp": last_quote.name.strftime("%Y-%m-%d %H:%M:%S"),  # Convert timestamp to a formatted string
        "ticker": ticker,  # Stock ticker symbol
        "open": last_quote["Open"],  # Opening price
        "high": last_quote["High"],  # Highest price
        "low": last_quote["Low"],  # Lowest price
        "close": last_quote["Close"],  # Closing price
        "volume": last_quote["Volume"]  # Trade volume
    }

# Function to produce messages to Kafka
def produce_messages(producer, topic, ticker):
    """
    Sends stock data messages to a Kafka topic in an infinite loop.
    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        ticker (str): The stock ticker symbol to fetch data for.
    """
    while True:
        data = get_stock_data(ticker)  # Fetch the latest stock data
        producer.send(topic, value=data)  # Send the data to the Kafka topic
        print(f"Sent: {data}")  # Log the sent data for debugging
        time.sleep(60)  # Wait for 60 seconds before fetching data again

# Main entry point of the script
if __name__ == "__main__":
    # Define Kafka connection details
    kafka_broker = "localhost:9092"  # Update with your Kafka broker address
    kafka_topic = "stock_data"  # Update with your Kafka topic name
