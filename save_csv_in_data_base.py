import pandas as pd
import mysql.connector
from mysql.connector import Error

# Database connection parameters
db_config = {
    'host': 'localhost',
    'database': 'client_svm_data_base',
    'user': 'root',
    'password': ''
}

# CSV file path
csv_file = './UCI_Credit_Card.csv'

try:
    # Connect to MySQL database
    connection = mysql.connector.connect(**db_config)
    
    if connection.is_connected():
        cursor = connection.cursor()
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Rename the column with dots to a SQL-friendly name
        df = df.rename(columns={'default.payment.next.month': 'default_payment_next_month'})
        
        # Convert float64 columns to Python float
        float_columns = [
            'LIMIT_BAL', 'BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3', 'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6',
            'PAY_AMT1', 'PAY_AMT2', 'PAY_AMT3', 'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6'
        ]
        for col in float_columns:
            df[col] = df[col].astype(float)
        
        # Create table with all columns from the dataset
        create_table_query = """
        CREATE TABLE IF NOT EXISTS credit_data (
            ID INT PRIMARY KEY,
            LIMIT_BAL FLOAT,
            SEX INT,
            EDUCATION INT,
            MARRIAGE INT,
            AGE INT,
            PAY_0 INT,
            PAY_2 INT,
            PAY_3 INT,
            PAY_4 INT,
            PAY_5 INT,
            PAY_6 INT,
            BILL_AMT1 FLOAT,
            BILL_AMT2 FLOAT,
            BILL_AMT3 FLOAT,
            BILL_AMT4 FLOAT,
            BILL_AMT5 FLOAT,
            BILL_AMT6 FLOAT,
            PAY_AMT1 FLOAT,
            PAY_AMT2 FLOAT,
            PAY_AMT3 FLOAT,
            PAY_AMT4 FLOAT,
            PAY_AMT5 FLOAT,
            PAY_AMT6 FLOAT,
            default_payment_next_month INT
        )
        """
        cursor.execute(create_table_query)
        
        # Insert data
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO credit_data (
                ID, LIMIT_BAL, SEX, EDUCATION, MARRIAGE, AGE,
                PAY_0, PAY_2, PAY_3, PAY_4, PAY_5, PAY_6,
                BILL_AMT1, BILL_AMT2, BILL_AMT3, BILL_AMT4, BILL_AMT5, BILL_AMT6,
                PAY_AMT1, PAY_AMT2, PAY_AMT3, PAY_AMT4, PAY_AMT5, PAY_AMT6,
                default_payment_next_month
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                int(row['ID']), 
                float(row['LIMIT_BAL']), 
                int(row['SEX']), 
                int(row['EDUCATION']), 
                int(row['MARRIAGE']), 
                int(row['AGE']),
                int(row['PAY_0']), 
                int(row['PAY_2']), 
                int(row['PAY_3']), 
                int(row['PAY_4']), 
                int(row['PAY_5']), 
                int(row['PAY_6']),
                float(row['BILL_AMT1']), 
                float(row['BILL_AMT2']), 
                float(row['BILL_AMT3']), 
                float(row['BILL_AMT4']), 
                float(row['BILL_AMT5']), 
                float(row['BILL_AMT6']),
                float(row['PAY_AMT1']), 
                float(row['PAY_AMT2']), 
                float(row['PAY_AMT3']), 
                float(row['PAY_AMT4']), 
                float(row['PAY_AMT5']), 
                float(row['PAY_AMT6']),
                int(row['default_payment_next_month'])
            )
            cursor.execute(insert_query, values)
        
        # Commit the transaction
        connection.commit()
        print(f"{cursor.rowcount} records inserted successfully")

except Error as e:
    print(f"Error: {e}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed")