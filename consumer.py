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
cursor = conn.cursor()  # Create a cursor object to execute SQL commands

# Ensure Kafka broker is reachable
kafka_broker = "localhost:9092"  # Update with your Kafka broker's actual address

# Set up Kafka consumer to consume messages from the "stock-prices" topic
try:
    consumer = KafkaConsumer(
        "stock-prices",  # Kafka topic to subscribe to
        bootstrap_servers=kafka_broker,  # Address of the Kafka broker
        auto_offset_reset="earliest",  # Start consuming messages from the earliest offset
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))  # Deserialize JSON messages
    )
    print("Connected to Kafka broker successfully!")
except Exception as e:
    print(f"Error connecting to Kafka broker: {e}")
    exit()

# Loop through each message in the Kafka topic
for message in consumer:
    data = message.value  # Extract the message payload (stock data) from the Kafka message
    try:
        # Execute an SQL INSERT statement to insert the data into the StockPrices table
        cursor.execute("""
        INSERT INTO StockPrices (Timestamp, Ticker, OPN_PRC, HIGH_PRC, LOW_PRC, CLS_PRC, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, 
        # Bind values from the data dictionary to the SQL query placeholders
        data["timestamp"],  # Timestamp of the stock data
        data["ticker"],  # Stock ticker symbol
        data["open"],  # Opening price
        data["high"],  # Highest price
        data["low"],  # Lowest price
        data["close"],  # Closing price
        data["volume"]  # Trade volume
        )
        conn.commit()  # Commit the transaction to save changes in the database
        print(f"Inserted: {data}")  # Print a confirmation message with the inserted data
    except Exception as e:
        print(f"Error inserting data into SQL Server: {e}")
