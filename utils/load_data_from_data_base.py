import pandas as pd
import mysql.connector
from mysql.connector import Error
from tqdm import tqdm
import logging

def mysql_to_csv(
    db_config,
    table_name='credit_data',
    output_csv_file='./UCI_Credit_Card_from_db.csv',
    log_level=logging.INFO
):
    """
    Export data from a MySQL table to a CSV file with progress logging.
    
    Parameters:
    - db_config (dict): Dictionary with MySQL connection parameters
                       (host, database, user, password).
    - table_name (str): Name of the table to query (default: 'credit_data').
    - output_csv_file (str): Path to save the output CSV file
                            (default: './UCI_Credit_Card_from_db.csv').
    - log_level (int): Logging level (default: logging.INFO).
    
    Returns:
    - bool: True if successful, False if an error occurs.
    """
    # Configure logging
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Connect to MySQL database
        logging.info("Connecting to MySQL database...")
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            cursor = connection.cursor()
            logging.info("Successfully connected to the database")
            
            # Query all data from the specified table
            logging.info(f"Fetching data from '{table_name}' table")
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]
            logging.info(f"Retrieved columns: {columns}")
            
            # Fetch all rows with progress tracking
            logging.info("Fetching rows from database")
            rows = []
            for row in tqdm(cursor.fetchall(), desc="Processing rows"):
                rows.append(row)
            
            # Create DataFrame
            logging.info("Converting data to DataFrame")
            df = pd.DataFrame(rows, columns=columns)
            
            # Rename 'default_payment_next_month' to 'default.payment.next.month' if present
            if 'default_payment_next_month' in df.columns:
                df = df.rename(columns={'default_payment_next_month': 'default.payment.next.month'})
                logging.info("Renamed column 'default_payment_next_month' to 'default.payment.next.month'")
            
            # Save DataFrame to CSV
            logging.info(f"Saving DataFrame to CSV: {output_csv_file}")
            df.to_csv(output_csv_file, index=False)
            logging.info(f"Successfully saved {len(df)} records to {output_csv_file}")
            
            return True

    except Error as e:
        logging.error(f"Error: {e}")
        return False

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logging.info("MySQL connection closed")
