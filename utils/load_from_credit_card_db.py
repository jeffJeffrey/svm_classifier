import pandas as pd
import mysql.connector
from mysql.connector import Error
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def mysql_to_csv(
    db_config,
    output_csv_file='./UCI_Credit_Card_reconstructed.csv',
    log_level=logging.INFO
):
    """
    Reconstruct the UCI Credit Card dataset from normalized MySQL tables and save to CSV.
    
    Parameters:
    - db_config (dict): MySQL connection parameters (host, database, user, password).
    - output_csv_file (str): Path to save the output CSV file.
    - log_level (int): Logging level (default: logging.INFO).
    
    Returns:
    - bool: True if successful, False if an error occurs.
    """
    # Update logging level if specified
    logging.getLogger().setLevel(log_level)
    
    try:
        # Log connection attempt
        logging.info("Attempting to connect to MySQL database with config: %s", 
                    {k: v for k, v in db_config.items() if k != 'password'})
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            logging.info("Successfully connected to database: %s", db_config['database'])
            
            # Define the SQL query to reconstruct the dataset
            logging.info("Preparing SQL query to reconstruct dataset from normalized tables")
            query = """
            SELECT 
                u.id AS ID,
                u.limit_bal AS LIMIT_BAL,
                u.sex AS SEX,
                u.education AS EDUCATION,
                u.marriage AS MARRIAGE,
                u.age AS AGE,
                MAX(CASE WHEN ps.month_offset = 0 THEN ps.status END) AS PAY_0,
                MAX(CASE WHEN ps.month_offset = 1 THEN ps.status END) AS PAY_2,
                MAX(CASE WHEN ps.month_offset = 2 THEN ps.status END) AS PAY_3,
                MAX(CASE WHEN ps.month_offset = 3 THEN ps.status END) AS PAY_4,
                MAX(CASE WHEN ps.month_offset = 4 THEN ps.status END) AS PAY_5,
                MAX(CASE WHEN ps.month_offset = 5 THEN ps.status END) AS PAY_6,
                MAX(CASE WHEN ba.month_offset = 1 THEN ba.amount END) AS BILL_AMT1,
                MAX(CASE WHEN ba.month_offset = 2 THEN ba.amount END) AS BILL_AMT2,
                MAX(CASE WHEN ba.month_offset = 3 THEN ba.amount END) AS BILL_AMT3,
                MAX(CASE WHEN ba.month_offset = 4 THEN ba.amount END) AS BILL_AMT4,
                MAX(CASE WHEN ba.month_offset = 5 THEN ba.amount END) AS BILL_AMT5,
                MAX(CASE WHEN ba.month_offset = 6 THEN ba.amount END) AS BILL_AMT6,
                MAX(CASE WHEN pa.month_offset = 1 THEN pa.amount END) AS PAY_AMT1,
                MAX(CASE WHEN pa.month_offset = 2 THEN pa.amount END) AS PAY_AMT2,
                MAX(CASE WHEN pa.month_offset = 3 THEN pa.amount END) AS PAY_AMT3,
                MAX(CASE WHEN pa.month_offset = 4 THEN pa.amount END) AS PAY_AMT4,
                MAX(CASE WHEN pa.month_offset = 5 THEN pa.amount END) AS PAY_AMT5,
                MAX(CASE WHEN pa.month_offset = 6 THEN pa.amount END) AS PAY_AMT6,
                ds.default_payment_next_month AS default_payment_next_month
            FROM users u
            LEFT JOIN payment_status ps ON u.id = ps.user_id
            LEFT JOIN bill_amounts ba ON u.id = ba.user_id
            LEFT JOIN payment_amounts pa ON u.id = pa.user_id
            LEFT JOIN default_status ds ON u.id = ds.user_id
            GROUP BY u.id, u.limit_bal, u.sex, u.education, u.marriage, u.age, ds.default_payment_next_month
            """
            
            # Execute query and load into DataFrame
            logging.info("Executing SQL query to fetch data")
            df = pd.read_sql(query, connection)
            logging.info("Query executed successfully, retrieved %d rows", len(df))
            
            # Log DataFrame columns and validate
            logging.info("DataFrame columns: %s", list(df.columns))
            expected_columns = [
                'ID', 'LIMIT_BAL', 'SEX', 'EDUCATION', 'MARRIAGE', 'AGE',
                'PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6',
                'BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3', 'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6',
                'PAY_AMT1', 'PAY_AMT2', 'PAY_AMT3', 'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6',
                'default_payment_next_month'
            ]
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                logging.error("Missing expected columns in DataFrame: %s", missing_columns)
                return False
            logging.info("All expected columns present in DataFrame")
            
            # Rename default_payment_next_month to default.payment.next.month
            logging.info("Renaming column 'default_payment_next_month' to 'default.payment.next.month'")
            df = df.rename(columns={'default_payment_next_month': 'default.payment.next.month'})
            
            # Save DataFrame to CSV with progress tracking
            logging.info("Starting CSV write to: %s", output_csv_file)
            with tqdm(total=len(df), desc="Saving rows to CSV") as pbar:
                df.to_csv(output_csv_file, index=False)
                pbar.update(len(df))
            
            logging.info("Successfully saved %d records to %s", len(df), output_csv_file)
            return True

    except Error as e:
        logging.error("MySQL error occurred: %s", e)
        return False
    except Exception as e:
        logging.error("Unexpected error occurred: %s", e)
        return False

    finally:
        if connection.is_connected():
            logging.info("Closing MySQL connection")
            connection.close()
            logging.info("MySQL connection closed")

if __name__ == "__main__":
    # Example usage
    db_config = {
        'host': 'localhost',
        'database': 'credit_card_db',
        'user': 'root',
        'password': ''
    }
    success = mysql_to_csv(db_config)
    if success:
        print("CSV file created successfully!")
    else:
        print("Failed to create CSV file.")