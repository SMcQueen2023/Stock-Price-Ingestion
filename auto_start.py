import subprocess
import os
import time

# Directories
kafka_directory = r"C:\kafka_2.13-3.9.0"
repo_directory = r"C:\Users\scott\OneDrive\Documents\GitHub\Stock-Price-Ingestion"

# Step 1: Start Zookeeper in a new PowerShell
def start_zookeeper():
    print("Starting Zookeeper in a new PowerShell...")
    zookeeper_command = os.path.join(kafka_directory, "bin", "windows", "zookeeper-server-start.bat")
    zookeeper_config = os.path.join(kafka_directory, "config", "zookeeper.properties")
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', '{zookeeper_command} {zookeeper_config}'"], shell=True)
    time.sleep(5)  # Wait for Zookeeper to start

# Step 2: Start Kafka Server in a new PowerShell
def start_kafka_server():
    print("Starting Kafka server in a new PowerShell...")
    kafka_command = os.path.join(kafka_directory, "bin", "windows", "kafka-server-start.bat")
    kafka_config = os.path.join(kafka_directory, "config", "server.properties")
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', '{kafka_command} {kafka_config}'"], shell=True)
    time.sleep(5)  # Wait for Kafka server to start

# Step 3: Run Stock Price Ingestion Script in a new PowerShell
def run_stock_price_ingestion():
    print("Running Stock Price Ingestion script in a new PowerShell...")
    ingestion_script = os.path.join(repo_directory, "Stock_Price_Ingestion.py")
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', 'python {ingestion_script}'"], shell=True)
    time.sleep(5)  # Wait for the ingestion script to start

# Step 4: Run Kafka Consumer Script in a new PowerShell
def run_kafka_consumer():
    print("Running Kafka Consumer script in a new PowerShell...")
    consumer_script = os.path.join(repo_directory, "consumer.py")
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', 'python {consumer_script}'"], shell=True)

# Main function
def main():
    start_zookeeper()
    start_kafka_server()
    run_stock_price_ingestion()
    run_kafka_consumer()

if __name__ == "__main__":
    main()