#%%
# Import necessary libraries
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import logging
from datetime import datetime

#%%# Define the database connection string
DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-5AKI1TP\SQLEXPRESS;"
    "Database=AdventureWorks2022;"
    "Trusted_Connection=yes;"
)

#%%
# Set up logging
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Also set up logging to print to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Log the start of the script
logging.info("Script started.")

#%%
# Define functions for executing SQL queries, uploading data, and retrieving data

def execute_sql(query):
    """
    Execute SQL query using a hardcoded connection string.

    Parameters:
        query (str): SQL query to execute.

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for executing SQL.")
        conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
        cursor = conn.cursor()
        logging.info(f"Executing query: {query}")
        cursor.execute(query)
        conn.commit()
        logging.info("SQL query executed successfully.")
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()
        conn.close()
        logging.info("Database connection closed after executing SQL.")

#%%
def upload_data(table, dataframe, upload_type):
    """
    Upload data to a specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        logging.info(f"Uploading data to table: {table}")
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        logging.info(f"Data uploaded successfully to {table}.")
    except Exception as e:
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")

#%%
def retrieve_data(query):
    """
    Retrieve data from the database using SQL query.

    Parameters:
        query (str): SQL query to retrieve data.

    Returns:
        DataFrame: Pandas DataFrame containing retrieved data.
    """
    try:
        logging.info("Attempting to connect to the database for retrieving data.")
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        logging.info(f"Retrieving data with query: {query}")
        df = pd.read_sql(query, engine)
        logging.info("Data retrieved successfully.")
    except Exception as e:
        logging.error(f"Error retrieving data: {e}")
        print(f"Error retrieving data: {e}")
        df = pd.DataFrame()  # Return empty DataFrame in case of error
    return df



#%%
#Creating new table into the Database
q = """
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CustomerFeedback]') AND type in (N'U'))
CREATE TABLE dbo.CustomerFeedback(
FeedbackID INT PRIMARY KEY ,
CustomerID INT,
FeedbackDate DATE,
Comments VARCHAR(250))
"""
try:
    df2=execute_sql(q)
    logging.info("Attempting to connect to the database to execute sql")
    conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
    cursor = conn.cursor()
    logging.info(f"Executing query: {q}")
    cursor.execute(q)
    conn.commit()
    logging.info("Table created successfully.")
except Exception as e:
    logging.error(f"error creating table: {e}")
    print(f"error creating table: {e}")
finally:
    if cursor:
        cursor.close()
    conn.close()
    logging.info("Database closed after executing query")
#%%    
#Populating the dbo.CustomerFeedback table with values
populate = """
INSERT INTO dbo.CustomerFeedback(FeedbackID,CustomerID, FeedbackDate, Comments)
VALUES
(1, 1001, '2024-05-31','Excellent Service'),
(2, 1002, '2024-05-26', 'Good job!'),
(3, 1003, '2024-05-24', 'Service could be improved'),
(4, 1004, '2024-05-20', 'Very Satisfied'),
(5, 1005, '2024-05-19', 'Prompt Response'),
(6, 1006, '2024-05-16', 'fast Delivery Service'),
(7,1007, '2024-05-10', 'Friendly Staff'),
(8, 1008, '2024-05-04', 'Delayed Response'),
(9, 1009, '2024-04-28', 'Impressed with the quality of service'),
(10, 1010, '2024-04-23', 'Recommended to a friend');
"""

try:
    df2=execute_sql(populate)
    logging.info("Attempting to connect to the database to execute sql")
    conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
    cursor = conn.cursor()
    logging.info(f"Executing query: {populate}")
    cursor.execute(populate)
    conn.commit()
    logging.info("Data inserted successfully.")
except Exception as e:
    logging.error(f"error executing query: {e}")
    print(f"error executing query: {e}")
finally:
    if cursor:
        cursor.close()
    conn.close()
    logging.info("Database closed after executing query")
#%%
#Updating a record in the CustomerFeedback table
update_record= """
UPDATE dbo.CustomerFeedback
SET CustomerID = 1001,
    FeedbackDate ='2024-06-05',
    Comments = 'Excellent Customer Service'
WHERE FeedbackID = 1;
"""
try:
    df2=execute_sql(update_record)
    logging.info("Attempting to connect to the database to execute sql")
    conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
    cursor = conn.cursor()
    logging.info(f"Executing query: {update_record}")
    cursor.execute(update_record)
    conn.commit()
    logging.info("Data updated successfully.")
except Exception as e:
    logging.error(f"error executing query: {e}")
    print(f"error executing query: {e}")
finally:
    if cursor:
        cursor.close()
    conn.close()
    logging.info("Database closed after executing query")
 #%%
 #Altering the CustomerFeedback table to add a new column
alter_table = """
ALTER TABLE dbo.CustomerFeedback
ADD Rating INT;
"""
try:
    df2=execute_sql(alter_table)
    logging.info("Attempting to connect to the database to execute sql")
    conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
    cursor = conn.cursor()
    logging.info(f"Executing query: {alter_table}")
    cursor.execute(alter_table)
    conn.commit()
    logging.info("New column inserted successfully.")
except Exception as e:
    logging.error(f"error executing query: {e}")
    print(f"error executing query: {e}")
finally:
    if cursor:
        cursor.close()
    conn.close()
    logging.info("Database closed after executing query")
#%%
#Inserting values into the new column "Rating"
update_rating =[
"UPDATE dbo.CustomerFeedback SET Rating = 5 WHERE FeedbackID = 1",
"UPDATE dbo.CustomerFeedback SET Rating = 3 WHERE FeedbackID = 2",
"UPDATE dbo.CustomerFeedback SET Rating = 2 WHERE FeedbackID = 3",
"UPDATE dbo.CustomerFeedback SET Rating = 4 WHERE FeedbackID = 4", 
"UPDATE dbo.CustomerFeedback SET Rating = 3 WHERE FeedbackID = 5",
"UPDATE dbo.CustomerFeedback SET Rating = 5 WHERE FeedbackID = 6",
"UPDATE dbo.CustomerFeedback SET Rating = 4 WHERE FeedbackID = 7",
"UPDATE dbo.CustomerFeedback SET Rating = 1 WHERE FeedbackID = 8",
"UPDATE dbo.CustomerFeedback SET Rating = 3 WHERE FeedbackID = 9",
"UPDATE dbo.CustomerFeedback SET Rating = 4 WHERE FeedbackID = 10"
];

try:
    df2=execute_sql(update_rating)
    logging.info("Attempting to connect to the database to execute sql")
    conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
    cursor = conn.cursor()
    for query in update_rating:
        logging.info(f"Executing query: {query}")
        cursor.execute(query)
    conn.commit()
    logging.info("Data updated successfully.")
except Exception as e:
    logging.error(f"error executing query: {e}")
    print(f"error executing query: {e}")
finally:
    if cursor:
        cursor.close()
    conn.close()
    logging.info("Database closed after executing query")

#%%
#Deleting a record from the dbo.CustomerFeedback table
delete_record = """
DELETE FROM dbo.CustomerFeedback WHERE FeedbackID = 2
"""

try:
    df2=execute_sql(delete_record)
    logging.info("Attempting to connect to the database to execute sql")
    conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
    cursor = conn.cursor()
    logging.info(f"Executing query: {delete_record}")
    cursor.execute(delete_record)
    conn.commit()
    logging.info("Record Deleted Successfully")
except Exception as e:
    logging.error(f"error executing query: {e}")
    print(f"error executing query: {e}")
finally:
    if cursor:
        cursor.close()
    conn.close()
    logging.info("Database closed after executing query")



#%%
data_to_upload = "C:\\Users\\HP\Downloads\\archive (7)\\7817_1.csv"
Table_name = "ProductReviews"
upload_type = "append"

try:
        logging.info("Attempting to connect to the database for uploading data.")
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        logging.info(f"Uploading data to table: {Table_name}")
        df=pd.read_csv(data_to_upload)
        logging.info(f"Dataframe Shape: {df.shape}")
        logging.info(f"First few rows of the dataframe:\n{df.head()}")
        logging.info(f"csv file{data_to_upload} read successfully.")
        logging.info(f"uploading data to Table: {Table_name}.")
        df.to_sql(name=Table_name,con=engine,index=False, if_exists=upload_type,schema="dbo",chunksize=10000)
        logging.info(f"Data uploaded successfully to {Table_name}.")
        print(f"Data uploaded successfully to {Table_name}.")
except Exception as e:
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}") 

#%%
#Retrieving Data from the Database
retrieval_query = """
SELECT *
FROM [Person].[Person]

"""
# Retrieve data from the database
try:
    data_frame = retrieve_data(retrieval_query)
    logging.info("Data retrieved successfully.")
    print("Data retrieved successfully:")
    print(data_frame.head())  # Display the first few rows of the retrieved data
except Exception as e:
    logging.error(f"Failed to retrieve data: {e}")
    print(f"Failed to retrieve data: {e}")

#%%
query = """
SELECT a.SalesOrderID, a.OrderDate, b.CustomerID, 
b.PersonID, b.AccountNumber
FROM Sales.SalesOrderHeader a
INNER JOIN Sales.Customer b
ON a.CustomerID = b.CustomerID
"""

try:
    df = retrieve_data(query)
    logging.info("Data retrieved successfully.")
    print('Data retrieved successfully.')
    print(df)
except Exception as e:
    logging.error(f"Failed to retrieve data: {e}")
    print(f"Failed to retrieve data: {e}")    

#%%
a = """
SELECT a.ProductID, b.Name AS product_name,a.SalesOrderID, a.OrderQty, a.UnitPrice, SUM(a.OrderQty * a.UnitPrice)-a.UnitPriceDiscount AS Total_Sales,
a.ModifiedDate
FROM Sales.SalesOrderDetail a
INNER JOIN Production.Product b
ON a.ProductID = b.ProductID
WHERE a.ModifiedDate BETWEEN '2013-01-01 00:00:00.000' AND '2013-12-31 00:00:00.000'
GROUP BY a.ProductID, b.Name, a.SalesOrderID, a.OrderQty, a.UnitPrice,a.UnitPriceDiscount, a.ModifiedDate
ORDER BY Total_Sales DESC
"""

try:
    df = retrieve_data(a)
    logging.info("Data retrieved successfully.")
    print('Data retrieved successfully.')
    print(df)

    #Export DataFrame to a csv file
    df.to_csv("LastYearSales.csv", index = False)
    logging.info("Data Exported LastYearSales.csv successfully")
    print("Data Exported to LastYearSales.csv successfully")
except Exception as e:
    logging.error(f"Failed to retrieve or export data: {e}")
    print(f"Failed to retrieve or export data: {e}") 













