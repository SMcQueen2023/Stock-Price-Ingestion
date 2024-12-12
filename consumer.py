from kafka import KafkaConsumer
import json
import pyodbc

# Configure SQL Server connection
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=StockPriceAnalysis;"  # Replace with your database name
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS StockPrices (
    ID INT IDENTITY PRIMARY KEY,
    Timestamp DATETIME,
    Ticker NVARCHAR(10),
    Open FLOAT,
    High FLOAT,
    Low FLOAT,
    Close FLOAT,
    Volume INT
)
""")
conn.commit()

# Consume messages
consumer = KafkaConsumer(
    "stock-prices",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

for message in consumer:
    data = message.value
    cursor.execute("""
    INSERT INTO StockPrices (Timestamp, Ticker, Open, High, Low, Close, Volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data["timestamp"], data["ticker"], data["open"], data["high"], data["low"], data["close"], data["volume"])
    conn.commit()
    print(f"Inserted: {data}")