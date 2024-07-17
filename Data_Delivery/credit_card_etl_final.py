# %%
# from google.colab import drive
# drive.mount('/content/drive')

# %%
# !pip install pyspark

# %%
#Import all necessary libraries and packages

import pyspark
import pyspark.sql.functions as funct
from pyspark.sql import SparkSession
import pandas as pd
import credentials as cred 
spark = SparkSession.builder.appName("credit_card_clean").getOrCreate()

# %%
credit_card_filepath = r"C:\Users\chito\Developer\Capstone_350\Per_Scholas_Data_Engineering_Capstone_350_Urquidi_2024\Source_Data\cdw_sapp_credit.json"
credit_card_df = spark.read.option('multiLine', True).json(credit_card_filepath)

# %% [markdown]
# 
# 
# ---
# 
# 
# # **DATA EXPLORATION**

# %%
credit_card_df.printSchema()

# %%
credit_card_df.show(5)

# %%
credit_card_df.count()

# %%
credit_card_df.describe().show()

# %% [markdown]
# ## There are 46694 records in this file. Looking at the dataframe with describe(method) reveals that each field has the same count. Thus, there are no null or missing values to consider.There are 115 branch codes with branch numbers raning from 1 to 197 which was previously  confirmed in the other dataframes. The social security numbers are within the min and max range so they are all 16 digits long. Similarly, Social Security number s are within the min and max range are all 10 digists long, all months of the year are represented, and 28 days of the month. All transactions occurred in 2018.

# %% [markdown]
# ### An important steop is to confirm that each credit card number has a unique identifier attached to it. If not, we can conclude there are errors or some customers have multiple credit cards which can be a reasonable assumption.

# %%
credit_card_df.select(funct.countDistinct("CREDIT_CARD_NO")).show()
credit_card_df.select(funct.countDistinct("CUST_SSN")).show()
credit_card_df.select(funct.countDistinct("BRANCH_CODE")).show()
credit_card_ssn = credit_card_df.select("CREDIT_CARD_NO","CUST_SSN").groupBy("CREDIT_CARD_NO","CUST_SSN").count()
credit_card_ssn.count()

# %% [markdown]
# ### The grouping indicates that there are an equal number of credit card numbers and unique customer social security numbers.

# %% [markdown]
# 

# %% [markdown]
# ### Also, helpful to review if the transactions typify what you would expect with the use of credit cards, albeit some transactions may reflect personal preferences for customers that could be private.

# %%
credit_card_df.groupBy('TRANSACTION_TYPE').count().orderBy(funct.col('count').desc()).show()

# %% [markdown]
# ### These are all very generic categories. Expected more variety; however, these are all within expectations of transaction types. The test category seems that it reflects test transactions when many times credit cards will be tested with small dollar amounts to verify the validity of the credit card or with certain transactions that require a hold.

# %% [markdown]
# 
# 
# ---
# 
# 
# # **TRANSFORMING THE DATA**

# %%
from pyspark.sql.functions import concat, col, expr, lpad

# Pad single-digit months with a leading zero
credit_card_df = credit_card_df.withColumn("MONTH", lpad(col("MONTH"), 2, '0'))

# Pad single-digit days with a leading zero
credit_card_df = credit_card_df.withColumn("DAY", lpad(col("DAY"), 2, '0'))

# Create a new column with the concatenated date fields
credit_card_df = credit_card_df.withColumn("TIMEID",  funct.format_string("%s%s%s",
            credit_card_df['YEAR'], credit_card_df['MONTH'], credit_card_df['DAY']))
# Convert the new column to a date format
#credit_card_df = credit_card_df.withColumn("TIMEID", expr("to_date(TIMEID, 'yyyyMMdd')"))
credit_card_df.select("DAY","MONTH","YEAR","TIMEID").show(5)
# Drop the original date columns
credit_card_df = credit_card_df.drop("YEAR", "MONTH", "DAY")

# Show the first five rows of the transformed dataframe
credit_card_df.show(5)

# %% [markdown]
# ### The Day, Month, and Year have been joined into a new column called TIMEID with the appropriate format of yyMMdd as per the mapping document.

# %%
#Reording the dataframe columns as described in the mapping document
credit_card_df = credit_card_df.select('TRANSACTION_ID','CREDIT_CARD_NO','TIMEID','CUST_SSN',
                     'BRANCH_CODE','TRANSACTION_TYPE','TRANSACTION_VALUE')


# %%
credit_card_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()


# %%
pandas_df = credit_card_df.toPandas()
pandas_df.head()

# %%
#pip list


