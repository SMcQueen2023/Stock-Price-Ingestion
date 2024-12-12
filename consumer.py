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
    INSERT INTO StockPrices (Timestamp, Ticker, OPN_PRC, HIGH_PRC, LOW_PRC, CLS_PRC, Volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data["timestamp"], data["ticker"], data["open"], data["high"], data["low"], data["close"], data["volume"])
    conn.commit()
    print(f"Inserted: {data}")
