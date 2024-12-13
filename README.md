# Stock Price Ingestion
## Overview
This is a data engineering project using:
- Kafka to stream the AAPL stock price using the Yahoo Finance API
- Microsoft SQL Server (hosted locally) to store the data from Kafka
- Microsoft Visual Studio Code to create and modify scripts
    - Python for language to start/stop Kafka and feed the data to SQL Server
- Power BI for data visualization
- GitHub for version control

## Steps

Get the path to your Kafka directory (...\kafka_2.13-3.9.0) and the location of your Python scripts.

1. Start Zookeeper from the Kafka Directory with ".\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"
2. Start Kafka Broker from the Kafka Directory with ".\bin\windows\kafka-server-start.bat .\config\server.properties"
3. Run the Python script "Stock-Price-Ingestion.py" from the GitHub Repo Directory
4. Run the Python script "consumer.py" in the GitHub Repo Directory
