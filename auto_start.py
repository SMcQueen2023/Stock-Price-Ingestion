import subprocess  # Module to spawn new processes and interact with them
import os  # Module to interact with the operating system
import time  # Module to add delays in execution

# Directories (prompt user for input)
print("Please provide the following directories:")
kafka_directory = input("Enter the path to the Kafka installation directory: ").strip()
repo_directory = input("Enter the path to the GitHub repository containing scripts: ").strip()

# Step 1: Start Zookeeper in a new PowerShell
def start_zookeeper():
    """
    Starts the Zookeeper service, which is a prerequisite for running Kafka, in a new PowerShell window.
    """
    print("Starting Zookeeper in a new PowerShell...")
    # Full path to the Zookeeper startup script
    zookeeper_command = os.path.join(kafka_directory, "bin", "windows", "zookeeper-server-start.bat")
    # Configuration file for Zookeeper
    zookeeper_config = os.path.join(kafka_directory, "config", "zookeeper.properties")
    # Run Zookeeper in a new PowerShell window and keep it open
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', '{zookeeper_command} {zookeeper_config}'"], shell=True)
    time.sleep(5)  # Wait for Zookeeper to start before proceeding

# Step 2: Start Kafka Server in a new PowerShell
def start_kafka_server():
    """
    Starts the Kafka server in a new PowerShell window.
    """
    print("Starting Kafka server in a new PowerShell...")
    # Full path to the Kafka server startup script
    kafka_command = os.path.join(kafka_directory, "bin", "windows", "kafka-server-start.bat")
    # Configuration file for the Kafka server
    kafka_config = os.path.join(kafka_directory, "config", "server.properties")
    # Run Kafka server in a new PowerShell window and keep it open
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', '{kafka_command} {kafka_config}'"], shell=True)
    time.sleep(5)  # Wait for the Kafka server to start before proceeding

# Step 3: Run Stock Price Ingestion Script in a new PowerShell
def run_stock_price_ingestion():
    """
    Executes the Stock Price Ingestion script in a new PowerShell window.
    """
    print("Running Stock Price Ingestion script in a new PowerShell...")
    # Path to the Stock Price Ingestion Python script
    ingestion_script = os.path.join(repo_directory, "Stock_Price_Ingestion.py")
    # Run the ingestion script in a new PowerShell window and keep it open
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', 'python {ingestion_script}'"], shell=True)
    time.sleep(5)  # Wait for the ingestion script to start before proceeding

# Step 4: Run Kafka Consumer Script in a new PowerShell
def run_kafka_consumer():
    """
    Executes the Kafka Consumer script in a new PowerShell window.
    """
    print("Running Kafka Consumer script in a new PowerShell...")
    # Path to the Kafka Consumer Python script
    consumer_script = os.path.join(repo_directory, "consumer.py")
    # Run the consumer script in a new PowerShell window and keep it open
    subprocess.Popen(["powershell", "-Command", f"Start-Process powershell -ArgumentList '-NoExit', 'python {consumer_script}'"], shell=True)

# Main function to orchestrate the startup sequence
def main():
    """
    Main function to start Zookeeper, Kafka server, Stock Price Ingestion, and Kafka Consumer sequentially.
    """
    start_zookeeper()  # Start Zookeeper service
    start_kafka_server()  # Start Kafka server
    run_stock_price_ingestion()  # Start the Stock Price Ingestion script
    run_kafka_consumer()  # Start the Kafka Consumer script

# Entry point of the script
if __name__ == "__main__":
    main()