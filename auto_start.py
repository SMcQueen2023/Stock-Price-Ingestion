import subprocess  # Module to spawn new processes and interact with them
import os  # Module to interact with the operating system
import time  # Module to add delays in execution
import logging  # Module to log script activities

# Configure logging to display log messages with timestamps, log levels, and the actual message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_valid_directory(prompt):
    """
    Prompts the user to input a directory path and validates whether the path is a valid directory.
    If the input is invalid, the user is repeatedly prompted until a valid directory is provided.
    """
    while True:
        path = input(prompt).strip()  # Ask the user for a directory path and remove any extra spaces
        if os.path.isdir(path):  # Check if the provided path is a valid directory
            return path  # Return the valid directory path
        print("Invalid directory. Please try again.")  # Inform the user of the invalid directory

# Prompt user for Kafka and GitHub repository directories
print("Please provide the following directories:")
kafka_directory = get_valid_directory("Enter the path to the Kafka installation directory: ")
repo_directory = get_valid_directory("Enter the path to the GitHub repository containing scripts: ")

def validate_file_exists(file_path):
    """
    Validates that a file exists at the specified path.
    Raises a FileNotFoundError if the file does not exist.
    """
    if not os.path.exists(file_path):  # Check if the file exists at the specified path
        raise FileNotFoundError(f"Required file not found: {file_path}")  # Raise an error if the file is missing

# Step 1: Start Zookeeper in a new PowerShell window
def start_zookeeper():
    """
    Starts the Zookeeper service in a new PowerShell window.
    Zookeeper is a prerequisite for running Kafka.
    """
    logging.info("Starting Zookeeper in a new PowerShell...")
    zookeeper_command = os.path.join(kafka_directory, "bin", "windows", "zookeeper-server-start.bat")  # Path to Zookeeper start script
    zookeeper_config = os.path.join(kafka_directory, "config", "zookeeper.properties")  # Path to Zookeeper config file

    # Validate that the Zookeeper script and config file exist
    validate_file_exists(zookeeper_command)
    validate_file_exists(zookeeper_config)

    # Start Zookeeper in a new PowerShell window using subprocess
    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', '{zookeeper_command} {zookeeper_config}'"])
    logging.info("Zookeeper started.")  # Log the success message
    time.sleep(5)  # Wait for 5 seconds to ensure Zookeeper starts before proceeding

# Step 2: Start Kafka Server in a new PowerShell window
def start_kafka_server():
    """
    Starts the Kafka server in a new PowerShell window.
    Kafka requires Zookeeper to be running, so this step starts Kafka after Zookeeper.
    """
    logging.info("Starting Kafka server in a new PowerShell...")
    kafka_command = os.path.join(kafka_directory, "bin", "windows", "kafka-server-start.bat")  # Path to Kafka start script
    kafka_config = os.path.join(kafka_directory, "config", "server.properties")  # Path to Kafka server config file

    # Validate that the Kafka script and config file exist
    validate_file_exists(kafka_command)
    validate_file_exists(kafka_config)

    # Start Kafka server in a new PowerShell window using subprocess
    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', '{kafka_command} {kafka_config}'"])
    logging.info("Kafka server started.")  # Log the success message
    time.sleep(5)  # Wait for 5 seconds to ensure Kafka starts before proceeding

# Step 3: Run Stock Price Ingestion Script in a new PowerShell window
def run_stock_price_ingestion():
    """
    Executes the Stock Price Ingestion script in a new PowerShell window.
    This script will start pulling stock data and pushing it to Kafka.
    """
    logging.info("Running Stock Price Ingestion script in a new PowerShell...")
    ingestion_script = os.path.join(repo_directory, "Stock_Price_Ingestion.py")  # Path to the stock price ingestion script

    # Validate that the ingestion script exists
    validate_file_exists(ingestion_script)

    # Run the Stock Price Ingestion script in a new PowerShell window using subprocess
    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', 'python {ingestion_script}'"])
    logging.info("Stock Price Ingestion script started.")  # Log the success message
    time.sleep(5)  # Wait for 5 seconds before starting the next step

# Step 4: Run Kafka Consumer Script in a new PowerShell window
def run_kafka_consumer():
    """
    Executes the Kafka Consumer script in a new PowerShell window.
    This script consumes the messages sent by the Stock Price Ingestion script.
    """
    logging.info("Running Kafka Consumer script in a new PowerShell...")
    consumer_script = os.path.join(repo_directory, "consumer.py")  # Path to the Kafka consumer script

    # Validate that the consumer script exists
    validate_file_exists(consumer_script)

    # Run the Kafka Consumer script in a new PowerShell window using subprocess
    subprocess.Popen(["powershell", "-Command", "Start-Process", "powershell", "-ArgumentList", 
                      f"'-NoExit', 'python {consumer_script}'"])
    logging.info("Kafka Consumer script started.")  # Log the success message

def main():
    """
    Main function to start Zookeeper, Kafka server, Stock Price Ingestion, and Kafka Consumer sequentially.
    Handles any errors that occur during the process, such as missing files.
    """
    try:
        start_zookeeper()  # Start Zookeeper service
        start_kafka_server()  # Start Kafka server
        run_stock_price_ingestion()  # Start the Stock Price Ingestion script
        run_kafka_consumer()  # Start the Kafka Consumer script
    except FileNotFoundError as e:
        logging.error(e)  # Log an error if a required file is missing
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")  # Log any other unexpected errors

# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()