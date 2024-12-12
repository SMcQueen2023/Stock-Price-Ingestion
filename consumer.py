from kafka import KafkaConsumer
import json
import pyodbc

# Configure SQL Server connection
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=StockPriceAnalysis;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
IF NOT EXISTS (
    SELECT 1 
    FROM sys.objects 
    WHERE object_id = OBJECT_ID(N'StockPrices') 
    AND type = N'U'
)
BEGIN
    CREATE TABLE StockPrices (
        Id INT PRIMARY KEY IDENTITY(1,1),
        Timestamp DATETIME,
        Ticker NVARCHAR(10),
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume INT
    )
END
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
