from kafka import KafkaConsumer  # Import KafkaConsumer to receive messages from a Kafka topic
import json  # Import JSON to deserialize messages received from Kafka
import pyodbc  # Import pyodbc to interact with SQL Server databases

# Configure SQL Server connection
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"  # Specify the ODBC driver for SQL Server
    "Server=localhost;"  # Database server address (here, localhost)
    "Database=StockPriceAnalysis;"  # The name of the database where data will be inserted
    "Trusted_Connection=yes;"  # Use Windows authentication for the connection
)

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Kafka broker configuration
kafka_broker = "localhost:9092"  # Kafka broker's address
kafka_topic = "stock-prices"  # Kafka topic to consume messages from

# Set up Kafka consumer with appropriate configurations
try:
    consumer = KafkaConsumer(
        kafka_topic,  # Kafka topic to subscribe to
        bootstrap_servers=kafka_broker,  # Kafka broker address
        auto_offset_reset="earliest",  # Start consuming from the earliest available message
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))  # Deserialize JSON messages
    )
    print(f"Connected to Kafka broker at {kafka_broker} successfully!")
except Exception as e:
    print(f"Error connecting to Kafka broker: {e}")
    exit()  # Exit if Kafka connection fails

# Loop to continuously read messages from the Kafka topic
try:
    for message in consumer:
        data = message.value  # Extract the stock price data from the Kafka message
        try:
            # SQL INSERT query to insert stock price data into the StockPrices table
            query = """
                INSERT INTO StockPrices (Timestamp, Ticker, OPN_PRC, HIGH_PRC, LOW_PRC, CLS_PRC, Volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, 
                           data["timestamp"],  # Stock data timestamp
                           data["ticker"],  # Stock ticker symbol
                           data["open"],  # Opening price
                           data["high"],  # Highest price
                           data["low"],  # Lowest price
                           data["close"],  # Closing price
                           data["volume"]  # Trade volume
            )

            # Commit the transaction to the database
            conn.commit()
            print(f"Inserted: {data}")  # Log the inserted data for tracking
        except Exception as e:
            # Handle errors during SQL execution and log the error
            print(f"Error inserting data into SQL Server: {e}")
            conn.rollback()  # Rollback transaction in case of an error
finally:
    # Close the database connection and cursor after processing all messages
    cursor.close()  # Close the cursor
    conn.close()  # Close the database connection
    print("Connection closed.")
