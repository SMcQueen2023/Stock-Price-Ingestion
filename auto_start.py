import subprocess  # Module to spawn new processes and interact with them
import os  # Module to interact with the operating system
import time  # Module to add delays in execution
import logging  # Module to log script activities

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_valid_directory(prompt):
    """
    Prompt the user to input a directory path and validate it.
    """
    while True:
        path = input(prompt).strip()
        if os.path.isdir(path):
            return path
        print("Invalid directory. Please try again.")

# Directories (prompt user for input)
print("Please provide the following directories:")
kafka_directory = get_valid_directory("Enter the path to the Kafka installation directory: ")
repo_directory = get_valid_directory("Enter the path to the GitHub repository containing scripts: ")

def validate_file_exists(file_path):
    """
    Validate that a file exists at the specified path.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Required file not found: {file_path}")

# Step 1: Start Zookeeper in a new PowerShell
def start_zookeeper():
    """
    Starts the Zookeeper service, which is a prerequisite for running Kafka, in a new PowerShell window.
    """
    logging.info("Starting Zookeeper in a new PowerShell...")
    zookeeper_command = os.path.join(kafka_directory, "bin", "windows", "zookeeper-server-start.bat")
    zookeeper_config = os.path.join(kafka_directory, "config", "zookeeper.properties")

    validate_file_exists(zookeeper_command)
    validate_file_exists(zookeeper_config)

    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', '{zookeeper_command} {zookeeper_config}'"])
    logging.info("Zookeeper started.")
    time.sleep(5)

# Step 2: Start Kafka Server in a new PowerShell
def start_kafka_server():
    """
    Starts the Kafka server in a new PowerShell window.
    """
    logging.info("Starting Kafka server in a new PowerShell...")
    kafka_command = os.path.join(kafka_directory, "bin", "windows", "kafka-server-start.bat")
    kafka_config = os.path.join(kafka_directory, "config", "server.properties")

    validate_file_exists(kafka_command)
    validate_file_exists(kafka_config)

    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', '{kafka_command} {kafka_config}'"])
    logging.info("Kafka server started.")
    time.sleep(5)

# Step 3: Run Stock Price Ingestion Script in a new PowerShell
def run_stock_price_ingestion():
    """
    Executes the Stock Price Ingestion script in a new PowerShell window.
    """
    logging.info("Running Stock Price Ingestion script in a new PowerShell...")
    ingestion_script = os.path.join(repo_directory, "Stock_Price_Ingestion.py")

    validate_file_exists(ingestion_script)

    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', 'python {ingestion_script}'"])
    logging.info("Stock Price Ingestion script started.")
    time.sleep(5)

# Step 4: Run Kafka Consumer Script in a new PowerShell
def run_kafka_consumer():
    """
    Executes the Kafka Consumer script in a new PowerShell window.
    """
    logging.info("Running Kafka Consumer script in a new PowerShell...")
    consumer_script = os.path.join(repo_directory, "consumer.py")

    validate_file_exists(consumer_script)

    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', 'python {consumer_script}'"])
    logging.info("Kafka Consumer script started.")

def main():
    """
    Main function to start Zookeeper, Kafka server, Stock Price Ingestion, and Kafka Consumer sequentially.
    """
    try:
        start_zookeeper()  # Start Zookeeper service
        start_kafka_server()  # Start Kafka server
        run_stock_price_ingestion()  # Start the Stock Price Ingestion script
        run_kafka_consumer()  # Start the Kafka Consumer script
    except FileNotFoundError as e:
        logging.error(e)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
