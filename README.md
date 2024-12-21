# Stock Price Ingestion
## Overview
In this project, I successfully developed a real-time streaming solution for Apple Inc. (AAPL) stock prices using the Yahoo Finance API and Apache Kafka. The real-time data was streamed and ingested into Microsoft SQL Server, stored, and processed for further analysis. I then created interactive and insightful visualizations using Power BI to provide an intuitive view of stock price trends, volatility, and key metrics. To ensure efficient version control and collaboration, the entire project was managed through GitHub. Additionally, I integrated generative AI to enhance various aspects of the project, including data analysis, code development, and troubleshooting, improving efficiency and reducing development time. This project strengthened my skills in real-time data processing, database management, business intelligence, and leveraging AI for enhanced decision-making.

## Steps without Auto-Start Script

Get the path to your Kafka directory (...\kafka_2.13-3.9.0) and the location of your Python scripts.

1. Start Zookeeper from the Kafka Directory with ".\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"
2. Start Kafka Broker from the Kafka Directory with ".\bin\windows\kafka-server-start.bat .\config\server.properties"
3. Run the Python script "Stock-Price-Ingestion.py" from the GitHub Repo Directory
4. Run the Python script "consumer.py" in the GitHub Repo Directory
