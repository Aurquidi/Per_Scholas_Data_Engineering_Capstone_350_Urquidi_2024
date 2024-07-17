# %%
!pip install mysql-connector-python
!pip install requests
!pip install --upgrade mysql-connector-python
!pip install cryptography

# %%
import requests
import pyspark.sql.functions as funct
from pyspark.sql.types import *
import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
import cryptography
import credentials as cred



# %% [markdown]
# # **Data Extraction from the Loan Data _API_**

# %% [markdown]
# ###  Extracting the data from the API endpoint and storing it in a Spark dataframe.  The API url is provided in the Capstone Guidance Document.

# %%
import requests


url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'



response = requests.get(url)
response.headers["Content-Type"]
api_results = response.json()
print(f"Status code: {response.status_code}")
print(f"{response.headers}")

# %% [markdown]
# ### Creating a spark dataframe, loan_application_df, from the API response.

# %%
# Create Spark DataFrame
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("LoanApplication").getOrCreate()

# Create DataFrame from API results
loan_application_df = spark.createDataFrame(api_results)

# Show the first few rows of the DataFrame
loan_application_df.show(5)


# %%
#Reviewing the schema along with the Data that has been loaded
loan_application_df.printSchema()
loan_application_df.show(15)

# %% [markdown]
# ### Use the schema described above to create a Spark dataframe that will maintain the structure types, structure fields, string and long types

# %%
# Create Spark DataFrame with the specified schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Define the schema
schema = StructType([
    StructField("Application_ID", StringType(), True),
    StructField("Application_Status", StringType(), True),
    StructField("Credit_History", LongType(), True),
    StructField("Dependents", StringType(), True),
    StructField("Education", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Income", StringType(), True),
    StructField("Married", StringType(), True),
    StructField("Property_Area", StringType(), True),
    StructField("Self_Employed", StringType(), True)
])


# Create DataFrame from API results with the specified schema
loan_application_df = spark.createDataFrame(api_results, schema)

# Show the first few rows of the DataFrame
loan_application_df.show(5)
loan_application_df.describe().show()


# %% [markdown]
# # Establishing DB connection to create the CDW_SAPP_loan_application table as per the guidance

# %%
# Prepare for MySQL connection
host = cred.host
user = cred.user
password = cred.password
database = "creditcard_capstone"

# Connect to MySQL and create table
try:
    conn = msql.connect(host=host, user=user, password=password, database=database)
    if conn.is_connected():
        cursor = conn.cursor()
        
        # Create table with the specified schema
        create_table_query = """
        CREATE TABLE IF NOT EXISTS CDW_SAPP_loan_application (
            Application_ID VARCHAR(255),
            Application_Status VARCHAR(255),
            Credit_History BIGINT,
            Dependents VARCHAR(255),
            Education VARCHAR(255),
            Gender VARCHAR(255),
            Income VARCHAR(255),
            Married VARCHAR(255),
            Property_Area VARCHAR(255),
            Self_Employed VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)
        print("Table created successfully")

except Error as e:
    print(f"Error: {e}")
    print(f"Error type: {type(e).__name__}")
    print(f"Error args: {e.args}")

finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed")
        
        

# %% [markdown]
# #### This next step will write the spark dataframe, loan_application_df to the MYSQL creditcard_capstone.CDW_SAPP_loan_application

# %%
loan_application_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()


# %% [markdown]
# ### Converting quickly to pandas_df to see appearance of data and appearance of table columns

# %%
pandas_df = loan_application_df.toPandas()
pandas_df.head()

# %%
!pip list


# %% [markdown]
# 


