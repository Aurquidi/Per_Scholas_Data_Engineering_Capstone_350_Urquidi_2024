# %% [markdown]
# # Creating the MySQL Database creditcard_capstone

# %%
# install PyMySQL
# install cryptography
# install mysql-connector-python
# install pyspark
# install --upgrade mysql-connector-python
# install cryptography

# %%
import pyspark.sql.functions as funct
import pymysql
from pyspark.sql.types import *
from mysql.connector import Error
import credentials as cred

# %% [markdown]
# ### Creating a mysql database called "creditcard_capstone" as prescribed in the Capstone guidance document

# %%
try:
    conn = pymysql.connect(
        host=cred.host,
        database='usersdb',
        user=cred.user,
        password=cred.password,
        port=cred.port
    )
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE creditcard_capstone")
    print('Database is created')
except pymysql.Error as e:
    print('Error while connecting to MySQL:', e)

# %% [markdown]
# ## Once the DB, creditcard_capstone has been created, the appropriate tables can be loaded directly from the spark dataframes.


# %% [markdown]
# Initiating the process for cleaning the branch dataset. The raw data is contained in a json file. The data upload occured on 6/14/24 as prescribed in the Capstone 350 Project Requirements document. The process requires Pyspark modules.

# %%
import pyspark
import pyspark.sql.functions as funct
from pyspark.sql import SparkSession
import pandas as pd
import credentials as cred 


# %%
#This creates the new Sparksession
spark = SparkSession.builder.appName("branch_clean").getOrCreate()

# %%
#Creating a shortcut to the filepath for the source file for branches
branch_filepath = r"C:\Users\chito\Developer\Capstone_350\Per_Scholas_Data_Engineering_Capstone_350_Urquidi_2024\Source_Data\cdw_sapp_branch.json"

# %%
#The raw data json file contains multiline records that output an error when the spark.read.json function is used to attempt to load the data thus use the option("multiline", True)

branch_df = spark.read.option("multiLine", True).json(branch_filepath)

# %% [markdown]
# # **Data Exploration**

# %%
#Showing the first 10 rows 
branch_df.show(10)

# %%
branch_df.printSchema()

# %%
branch_df.count()

# %%
branch_df.describe().show()

# %%
print(branch_df.distinct().count()) # 115 unique branches
branch_df.select(pyspark.sql.functions.countDistinct("BRANCH_CITY")).show() #115 unique branch cities
branch_df.select(pyspark.sql.functions.countDistinct("BRANCH_CODE")).show() #115 unique branch codes

# %%
branch_df.explain()

# %%
branch_df.groupBy('BRANCH_NAME').count().orderBy('count').show() #All 115 branches have the Example Bank name
branch_df.groupBy()

# %%
from pyspark.sql.functions import col
# Find count of NA or missing values for each column
na_counts = {col_name: branch_df.filter(col(col_name).isNull() | (col(col_name) == "")).count() for col_name in branch_df.columns}

# Print the counts of missing values per column
for column, count in na_counts.items():
    print(f"Column {column} has {count} missing values")

# %% [markdown]
# #### Preliminary analysis indicates that the data is consistent, aligns with the schema, and there are no na or null values. As per the mapping requirements for the branch dataset, since there are no NA or Null values, there is no benefit to adding the defalut value of "99999". However, some of the zip code values reveal that they are missing a digit since all US zip codes have a length of 5 digits. Let's investigate how many and which zip codes are of improper length.

# %%
branch_df.select('BRANCH_CITY', 'BRANCH_STATE','BRANCH_ZIP')\
    .where(pyspark.sql.functions.length(branch_df["BRANCH_ZIP"]) < 5).show()

# %% [markdown]
# !US_zip_code_map
# 

# %% [markdown]
# #### **The zip code map reveals that these northeastern US states have zip codes that begin with the range of 02 - 08; thus includes the zip codes presented in the states above. The entire region has zip codes with an intial "0" so we will need to ajust the Branch_ZIP column to correct and add the inital "0" value back to the zip code.**

# %% [markdown]
# <img src="US_zip_code_map.png" alt="US Zip Code Map" style="width:750px;height:600px;">
# 

# %% [markdown]
# # Transforming the Data

# %%
branch_df = branch_df.withColumn('BRANCH_ZIP',\
                    pyspark.sql.functions.when((pyspark.sql.functions.length(branch_df['BRANCH_ZIP']) == 4) &
                        branch_df['BRANCH_STATE'].isin(["NJ", "CT", "NH", "MA", "VT", "RI", "ME"]),
                    pyspark.sql.functions.format_string("0%s",branch_df['BRANCH_ZIP']))\
                    .otherwise(branch_df["BRANCH_ZIP"]))
# "0%s" adds a leading 0 to each string in the column that meets both conditions as specified with the & operator

# %%
#Verifying that all zip code values are of length = 5 as per zip code requirements
branch_df.withColumn("zip_len", pyspark.sql.functions.length(branch_df["BRANCH_ZIP"]))\
    .groupBy("zip_len").count().show()

# %%
branch_ac_df = branch_df.withColumn("first_phone", pyspark.sql.functions.substring("BRANCH_PHONE",0,3))
branch_ac_df.groupBy('first_phone').count().orderBy('count').show()
#Verifying that all phone numbers have the same prefix area code

# %% [markdown]
# ### Creating a User Defined Function(UDF) to transform phone numbers into the format (xxx)xxx-xxxx

# %%
def format_phone_number(phone_number):
    if len(phone_number) == 10:
        return f"({phone_number[:3]}){phone_number[3:6]}-{phone_number[6:10]}"
    else:
        return "Invalid phone number length"



formatted_number = format_phone_number("1234567890")
print(formatted_number)









# %%
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType



# Create a UDF from the Python function
format_phone_number_udf = udf(format_phone_number, StringType())

# Apply the UDF to the DataFrame column

branch_df = branch_df.withColumn("BRANCH_PHONE", format_phone_number_udf(branch_df["BRANCH_PHONE"]))

branch_df.show(50
               )
#UDF successfully transforms the phone numbers

# %% [markdown]
# ### After review of the table above, one issue has become obvious. Cities that have mutiple words, like El Paso or Redondo Beach, are not seperated with a white space in the BRANCH_City column. Correcting this will provide better presenation of the column data

# %%
from pyspark.sql.functions import regexp_replace

#Using regexp_replace, the text is searched through regex functionality to search for a lowercase letter followed by an uppercase letter. The two characters are then separated by a white space.
branch_df = branch_df.withColumn("BRANCH_CITY", regexp_replace(branch_df["BRANCH_CITY"], r"([a-z])([A-Z])", r"$1 $2"))

branch_df.show(50)


# %% [markdown]
# #### **As outlined in the mapping document requirements, the dataframe columns should be re-ordered to reflect the document column order, such that: 
# BRANCH_CODE|
# BRANCH_NAME|
# BRANCH_STREET|
# BRANCH_CITY|
# BRANCH_STATE|
# BRANCH_ZIP|
# BRANCH_PHONE|
# LAST_UPDATED|
# 

# %%
branch_df = branch_df.select('BRANCH_CODE','BRANCH_NAME','BRANCH_STREET','BRANCH_CITY',
                 'BRANCH_STATE','BRANCH_ZIP','BRANCH_PHONE','LAST_UPDATED')

branch_df.show(5)

# %% [markdown]
# #### The final transformation requires that the LAST_UPDATED column be in a timestamp format as per the mapping document

# %%
from pyspark.sql import functions as funct


branch_df = branch_df.withColumn('LAST_UPDATED', funct.to_timestamp('LAST_UPDATED', 'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'))
branch_df.show()


# %%
print(spark.version)



# %% [markdown]
# ### **Writing the branch_df directly to the creditcard_capstone DB is key. Below, you will notice several code blocks in which I tried roundabout methods of loading data. TRANSFORMING INTO A PANDAS DATAFRAME AND THEN READING TO JSON FILES CREATES AN INFINITE NUMBER OF ISSUES. DO NOT ATTEMPT THIS APPROACH!! 

# %%
branch_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()



# %% [markdown]
# 

# %%
branch_df.printSchema()

# %%
pandas_df = branch_df.toPandas()
pandas_df.head()

# %%
# list


# %%
# from google.colab import drive
# drive.mount('/content/drive')

# %%
# # install pyspark

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
# list


# %%
## install pyspark

# %%
#Import all necessary libraries and packages

import pyspark
import pyspark.sql.functions as funct
from pyspark.sql import SparkSession
import pandas as pd
import credentials as cred 

# %%
#Creating a shortcut to the filepath for the source file for branches
customer_filepath = r"C:\Users\chito\Developer\Per_Scholas_Data_Engineering_Capstone_350_Urquidi_2024\Data_Delivery\customer_etl_final.py"

# %%
#This creates the new Sparksession
spark = SparkSession.builder.appName("customer_clean").getOrCreate()
#spark = SparkSession.builder.master("local").appName("test").getOrCreate()


# %%
#Google Colab
#Creating a shortcut to the filepath for the source file for branches
customer_filepath = r"C:\Users\chito\Developer\Per_Scholas_Data_Engineering_Capstone_350_Urquidi_2024\Source_Data\cdw_sapp_customer.json"1

# %%
#The raw data json file contains multiline records that output an error when the spark.read.json function is used to attempt to load the data thus use the option("multiline", True)

customer_df = spark.read.option("multiLine", True).json(customer_filepath)

# %% [markdown]
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Data Exploration
# 

# %%
#Showing the first 10 rows
customer_df.show(10) #A quick review of the initial 10 rows indicates that the CUST_PHONE column is missing digits for a US based system that uses 10 digits including area code for telephone numbers.

# %%
customer_df.printSchema()

# %% [markdown]
# **As the columns will need to be reordered according to the mapping document, it makes sense to place the columns in the order presecribed in the mapping document. <span style="font-size: larger;">A special note here**
# </span>. In this analysis, the assumption is that the APT_NO column actually represensents the number portion of a street address since it's unlikely that all customers would live in an apartment. Later in the transformation section of the customer dataframe, placing the APT_NO adjacent to the STREET_NAME column will be the basis of the analysis.
# 

# %%
customer_df = customer_df.select('CREDIT_CARD_NO', 'SSN', 'CUST_EMAIL', 'CUST_PHONE','FIRST_NAME','MIDDLE_NAME', 'LAST_NAME', 'APT_NO', 'STREET_NAME','CUST_CITY', 'CUST_ZIP', 'CUST_STATE', 'CUST_COUNTRY', 'LAST_UPDATED')

customer_df.show(5)

# %%
customer_df.describe().show()

# %% [markdown]
# ### Again, as indicated above, the CUST_PHONE column shows the value to be only 7 digits rather than 10 digits thus requiring transformation. Similarly, credit card numbers are 16 digits, social security numbers are 9 digits, US states use the 2 letter postal abbreviation, and zip codes are 5 digits.

# %%
customer_df.explain()

# %%
#Count of customer records
customer_df.count()

# %%
print(customer_df.distinct().count()) # Should be 952 unique customers
customer_df.select(funct.countDistinct("CREDIT_CARD_NO")).show() #952 unique Credit Card #s for each customer
customer_df.select(funct.countDistinct("CUST_EMAIL")).show() #928 unique customer emails so the data should be examined for null or missing values
customer_df.select(funct.countDistinct("CUST_PHONE")).show() #901 unique customer phone numbers. This could be because of area code issues which will be engaged in the data transformation section
customer_df.select(funct.countDistinct("SSN")).show()

# %%
# Find count of NA or missing values for each column
from pyspark.sql.functions import col
na_counts = {col_name: customer_df.filter(col(col_name).isNull() | (col(col_name) == "")).count() for col_name in customer_df.columns}

# Print the counts of missing values per column
for column, count in na_counts.items():
    print(f"Column {column} has {count} missing values")

# %%
#Since there are no null or missing values, the CUST_EMAIL column needs to be inspected for duplicate values
# Display rows with duplicate CUST_EMAIL values
customer_df.groupBy("CUST_EMAIL").count().where("count > 1").show(24) # of duplicate emails (952-928 = 24). These will need transformation.
#The duplicate emails are exhibited below:

# %%
# check that all emails in the CUST_EMAIL column have the correct format with alphanumeric "characters@.example.com" using Regex for pyspark



# Define the regular expression pattern for valid emails
email_pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.example\.com$"

# Filter the DataFrame to select rows where the CUST_EMAIL column does not match the pattern
invalid_emails_df = customer_df.filter(~funct.col("CUST_EMAIL").rlike(email_pattern))

# Count the number of rows with invalid emails
invalid_email_count = invalid_emails_df.count()

# Print the count of invalid emails
print(f"Number of rows with invalid emails: {invalid_email_count}")

# Show the rows with invalid emails
invalid_emails_df.show(5)


# %% [markdown]
# #### **All customer emails have the correct format. The issue is with the duplicated emails where individuals share the same first name first inititial and the same last name. There are 24 duplicate emails in the dataframe. 952-24 = 928 as the count of count(DISTINCT CUST_EMAIL) showed 928. However, we will need to account for this when transforming the data. The methodology will use the customer middle name, MIDDLE_NAME, as a way to distinguish between duplicate emails since it appears that the email format reflects: first_initial_last_name@example.com. Transformation will only occur for those duplicates and not each emai in the data frame**

# %% [markdown]
# ____________________________________________________________________________________________________________________________________________
# ### **Data Transformation**

# %%
#Adding space between customer city names that have multiple words therby creating space to appropriate decipher the customer city name, e.g., "El Paso", "King of Prussia"
from pyspark.sql.functions import regexp_replace

#Using regexp_replace, the text is searched through regex functionality to search for a lowercase letter followed by an uppercase letter. The two characters are then separated by a white space.
customer_df = customer_df.withColumn("CUST_CITY", regexp_replace(customer_df["CUST_CITY"], r"([a-z])([A-Z])", r"$1 $2"))
customer_df.show(10)


# %% [markdown]
# ### Transforming the duplicate emails with the use of the customer MIDDLE_NAME field to differentiate duplicates. This seems the best method to avoid duplicate emails from what are almost certainly unique individuals not sharing a household with the other person.

# %%

from pyspark.sql.window import Window
from pyspark.sql import functions as funct  # Using your alias

w = Window.partitionBy('CUST_EMAIL').orderBy('MIDDLE_NAME')
ranked_df = customer_df.withColumn('rank', funct.rank().over(w))

# Update emails for duplicates
ranked_df = (
    ranked_df.withColumn(
        'CUST_EMAIL',  # Correct the column name to 'CUST_EMAIL'
        funct.when(
            funct.col('rank') > 1,
            funct.concat(
                funct.substring('FIRST_NAME', 1, 1),
                funct.substring('MIDDLE_NAME', 1, 1),
                funct.col('LAST_NAME'),                   # Full last name (no slicing)
                funct.lit('@example.com'),
            ),
        ).otherwise(funct.col('CUST_EMAIL')),  # Reference correct original email column
    )
    .drop('rank')
)

ranked_df.show(10)


# %%
from pyspark.sql.functions import col

# Perform a join on a unique identifier (e.g., CUST_ID)
joined_df = customer_df.alias("original").join(ranked_df.alias("updated"), on="SSN")

# Filter to show only rows where the email was changed
corrected_duplicates_df = joined_df.filter(
    col("original.CUST_EMAIL") != col("updated.CUST_EMAIL")
).select(
    col("original.SSN"),
    col("original.FIRST_NAME"),
    col("original.MIDDLE_NAME"),
    col("original.LAST_NAME"),
    col("original.CUST_EMAIL").alias("original_email"),
    col("updated.CUST_EMAIL").alias("updated_email"),
)

corrected_duplicates_df.show(24)


# %%
from pyspark.sql.functions import col, coalesce

# Join on SSN, prioritize updated emails
updated_customer_df = customer_df.alias("original").join(
    ranked_df.alias("updated"), on="SSN", how="left"
).select(
    # Include all original columns except the original CUST_EMAIL
    *[col("original." + c) for c in customer_df.columns if c != "CUST_EMAIL"],
    # Select the updated or original CUST_EMAIL column
    coalesce(col("updated.CUST_EMAIL"), col("original.CUST_EMAIL")).alias("CUST_EMAIL")
)

# Display the updated dataframe
updated_customer_df.show()


# %%
#Reordering columns again as per the mapping document
updated_customer_df = updated_customer_df.select('CREDIT_CARD_NO', 'SSN', 'CUST_EMAIL', 'CUST_PHONE','FIRST_NAME','MIDDLE_NAME', 'LAST_NAME', 'APT_NO', 'STREET_NAME','CUST_CITY', 'CUST_ZIP', 'CUST_STATE', 'CUST_COUNTRY', 'LAST_UPDATED')

updated_customer_df.show(10)

# %%
updated_customer_df.select(funct.countDistinct("CUST_EMAIL")).show()

# %% [markdown]
# ### After transformation, the updated data frame shows 952 distinct customer emails. The dataframe has been updated accordingly. It is now updated_customer_df.

# %% [markdown]
# ##### **The mapping document requires that the First and Last Names be entered in "Title" case. I will begin this transformation with the LAST_NAME column. However, I did notice that some of the last names with an "Mc" prefix do not have the first letter after the prefix beginning with a capital letter, as in "McKinney". First, I will transform these records to this format: "Mc[A-Z]"**

# %%
Last_Name_McC = updated_customer_df.filter(funct.col("LAST_NAME").startswith("Mc"))
total_count = Last_Name_McC.count()
print(total_count)
Last_Name_McC.show()


# %%


# Apply a user defined function to capitalize the second and third letters
def fix_mc_name(name):
    if name and len(name) >= 3 and name.startswith("Mc"):
        return f"Mc{name[2].upper()}{name[3:].lower()}"
    return name

fix_mc_name_udf = funct.udf(fix_mc_name)

# Update the original DataFrame
updated_customer_df = updated_customer_df.withColumn(
    "LAST_NAME", fix_mc_name_udf(funct.col("LAST_NAME"))
)

# Display the updated DataFrame to verify changes
updated_customer_df.show()


# %%
Last_Name_McC = updated_customer_df.filter(funct.col("LAST_NAME").startswith("Mc"))
total_count = Last_Name_McC.count()
print(total_count)
Last_Name_McC.show()
#The last names have been adjusted to reflect the correct format to Mc[A-Z]

# %% [markdown]
# #### Now, let us inititiate the transformation to title case for both the first and last names as per the mapping document.

# %%
first_name_filter = updated_customer_df.filter(~funct.col('FIRST_NAME').rlike('^[A-Z][a-z]+$'))
last_name_filter = updated_customer_df.filter(~funct.col('LAST_NAME').rlike('^[A-Z][a-z]+$'))

# ^[A-Z] checks to see if there is a single capital letter at the start of the string.
# [a-z]+$ checks to see if there are only lower case letters after the first letter
# and those letters also go on till the end of the string. Since this is using a ~ operation, the Mc[A-Z] format names I transformed will be identified in the filter. However, none of these have to be transformed. Likewise, no first names need to be transformed to reflect title case.
first_name_filter.select('FIRST_NAME').show()
last_name_filter.select('LAST_NAME').show(26)

# %% [markdown]
# ##### No additional transformation of first and last names is required, all First and Last Names are in title case. The middle name must now be converted to lowercase as required by the mapping document.
# 

# %%
from pyspark.sql.functions import lower

# Convert MIDDLE_NAME to lowercase
updated_customer_df = updated_customer_df.withColumn("MIDDLE_NAME", lower(funct.col("MIDDLE_NAME")))

# Display updated DataFrame
updated_customer_df.show()


# %% [markdown]
# #### Previously discussed, I will merge the APT_NO and STREET_NAME columns into a new column called FULL_STREET_ADDRESS.

# %%
from pyspark.sql.functions import concat, lit

# Concatenate APT_NO and STREET_NAME, adding a space in between
updated_customer_df = updated_customer_df.withColumn(
    "FULL_STREET_ADDRESS",
    concat(funct.col("APT_NO"), lit(" "), funct.col("STREET_NAME"))
)

updated_customer_df = updated_customer_df.drop("APT_NO", "STREET_NAME")

# Display updated DataFrame
updated_customer_df.show()


# %% [markdown]
# **This transformation requires that the LAST_UPDATED column be in a timestamp format as per the mapping document**

# %%
from pyspark.sql import functions as funct


updated_customer_df = updated_customer_df.withColumn('LAST_UPDATED', funct.to_timestamp('LAST_UPDATED', 'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'))
updated_customer_df.show()

# %% [markdown]
# ### The area code for each customer is missing. The mapping document requires that the customer phone number be entered as a 10 digiit format of (xxx)xxx-xxxx. However, all the customer phone numbers are missing an area code. The challenge then becomes finding a way to map the location of each customer's home address to a viable area code. Very few resources exist for matching area codes to zip codes or city/town jurisdictions. I found a resource; however, that had an easy crosswalk from zip code and city/town to area codes that didnn't require multiple transformation steps. This dataset is in a CSV format that I will match to the existing updated_customer df to add area code values to each customer phone number. 

# %% [markdown]
# ##### **NOTE: There are some zip codes that have more than 1 area code attached. For simplicity and ease of processing, the first area code listed will be utilized. Finding exact area codes would require signficant time resources and geocoding for exactly precise matches well beyond the intent of this ETL Capstone**

# %%
#area_code_df = spark.read.csv(r"C:\Users\chito\Developer\Capstone_350\Raw_Data\ZipData.csv", header=False, inferSchema=True)
#area_code_df = spark.read.csv("/content/ZipData.csv", header=False, inferSchema=True)#Google Colab code
area_code_df = spark.read.csv(r"C:\Users\chito\Developer\Per_Scholas_Data_Engineering_Capstone_350_Urquidi_2024\Source_Data\ZipData.csv", header=False, inferSchema=True)
area_code_df = area_code_df.toDF('CUST_ZIP', 'CUST_STATE', 'CUST_CITY', 'CUST_COUNTY', 'AREA_CODE', 'TIME_ZONE', 'DST')
area_code_df.show()
area_code_df.count() # There are 42407 records in the area code dataframe.

# %%
# Assuming 'updated_customer_df' is the left DataFrame and 'area_code_df' is the right DataFrame
joined_df = updated_customer_df.join(area_code_df,
                                  on=updated_customer_df['CUST_ZIP'] == area_code_df['CUST_ZIP'],
                                  how='left') \
                             .select(updated_customer_df["*"],
                                     area_code_df["CUST_CITY"].alias("right_CUST_CITY"),
                                     area_code_df["CUST_STATE"].alias("right_CUST_STATE"),
                                     area_code_df["CUST_COUNTY"].alias("right_CUST_COUNTY"),
                                     area_code_df["AREA_CODE"].alias("right_AREA_CODE"))

joined_df.show()
joined_df.count()
joined_df.printSchema()

# %% [markdown]
# ### Unfortunately, after the join the number of records increased from 952 to 979. I'm assuming there were duplicate values on the zip code since that is what was used to join the dataframes. I need to correct for this now.

# %%
duplicate_zip_counts = area_code_df.groupBy('CUST_ZIP').count().filter("count > 1")
duplicate_zip_counts.show()

# %%
from pyspark.sql.functions import split, col

# Split the 'AREA_CODE' column by '/' and extract the first element

joined_df = joined_df.withColumn('right_AREA_CODE', split(col('right_AREA_CODE'), '/')[0])
joined_df.show()

# %%
from pyspark.sql.functions import concat, lit

# Concatenate AREA_CODE and CUST_PHONE in the desired format 
joined_df = joined_df.withColumn('FORMATTED_PHONE',
                                 concat(lit('('),
                                        col('right_AREA_CODE'), lit(')'),
                                        col('CUST_PHONE').substr(1, 3), lit('-'),
                                        col('CUST_PHONE').substr(4, 4)))

joined_df.show()

# %%
# Replace CUST_PHONE with FORMATTED_PHONE
joined_df = joined_df.drop('CUST_PHONE').withColumnRenamed('FORMATTED_PHONE', 'CUST_PHONE')

joined_df.show()

# %%
# List of columns to drop
columns_to_drop = ["right_CUST_CITY", "right_CUST_STATE", "right_CUST_COUNTY", "right_AREA_CODE"]
# Drop the columns
joined_df = joined_df.drop(*columns_to_drop)

joined_df.show()
joined_df.describe().show()

# %%
from pyspark.sql.functions import col

updated_customer_df = updated_customer_df.select(col("*"), "CUST_PHONE")
updated_customer_df.show()

# %%

# Select all columns from joined_df, including the renamed 'CUST_PHONE'
final_df = joined_df.select(col("*"))
final_df.show()

# %%
final_df.printSchema()

# %%
final_df = final_df.select('SSN','FIRST_NAME','MIDDLE_NAME','LAST_NAME','CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED')

# %%
final_df.show()

# %%
final_df.count()
final_df.show(979)

# %%
from pyspark.sql.functions import count

grouped_df = final_df.groupBy('CREDIT_CARD_NO').agg(count('*').alias('num_records'))
grouped_df.show(979)

# %%
duplicate_credit_card_df = grouped_df.filter(col('num_records') > 1)
duplicate_credit_card_df.show(30)

# %%
duplicate_records_df = duplicate_credit_card_df.join(final_df, on='CREDIT_CARD_NO', how='inner')
duplicate_records_df.show(30)
#verifying the duplicates by Credit Card no and dropping the duplicates

# %%
final_customer_dedup_df = final_df.dropDuplicates(['CREDIT_CARD_NO'])
final_customer_dedup_df.count()
final_customer_dedup_df.describe().show()
final_customer_dedup_df.show()
#Once again, we the final deduplicated data frame shows 952 records

# %% [markdown]
# ### **Writing the final_customer_dedup_df directly to the creditcard_capstone DB is key. Below, you will notice several code blocks in which I tried roundabout methods of loading data. TRANSFORMING INTO A PANDAS DATAFRAME AND THEN READING TO JSON FILES CREATES AN INFINITE NUMBER OF ISSUES. DO NOT ATTEMPT THIS APPROACH!! 

# %%
final_customer_dedup_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER ") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()


# %% [markdown]
# #### For the sake of verification, I copied the final_customer_dedup_df to a pandas dataframe to review the set up of the columns and format of the table to ensure the mysql table matches.

# %%
pandas_df = final_customer_dedup_df.toPandas()
pandas_df.head()

# %%
# list


# %%
# install mysql-connector-python
# install requests
# install --upgrade mysql-connector-python
# install cryptography

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
# list


# %% [markdown]
# 


import time
import credentials as cred
import mysql.connector as mysql
import re
from datetime import datetime

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=cred.host,
            user=cred.user,
            password=cred.password,
            database='creditcard_capstone'
        )
        return connection
    except mysql.connector.Error as err:
        print("Error connecting to database:", err)
        return None

def startup_sequence():
    print(".")
    time.sleep(0.5)
    print("..")
    time.sleep(0.5)
    print("Starting up application..")
    time.sleep(0.8)
    print("Importing required python libraries")
    time.sleep(1)
    print("Libraries fully loaded")
    time.sleep(1)
    print("Unauthorized access not permitted")
    time.sleep(1)
    print(
        "Any unauthorized access will lead to immediate criminal prosecution "
        "and the full extent of civil penalties as permitted by law")
    time.sleep(0.8)
    print(".")
    time.sleep(0.4)
    print("..")
    time.sleep(0.4)
    print("Accessing database")
    time.sleep(1)
    print("Connecting to the database")
    time.sleep(1)

def welcome_title():
    print(''''
        $$$$$$$$\                                            $$\                 $$$$$$$\                      $$\                                                                                                                                                                                    
    $$  _____|                                           $$ |                $$  __$$\                     $$ |                                                                                                                                                                                   
    $$ |      $$\   $$\ $$$$$$\  $$$$$$\$$$$\   $$$$$$\  $$ | $$$$$$\        $$ |  $$ | $$$$$$\  $$$$$$$\  $$ |  $$\                                                                                                                                                                              
    $$$$$\    \$$\ $$  |\____$$\ $$  _$$  _$$\ $$  __$$\ $$ |$$  __$$\       $$$$$$$\ | \____$$\ $$  __$$\ $$ | $$  |                                                                                                                                                                             
    $$  __|    \$$$$  / $$$$$$$ |$$ / $$ / $$ |$$ /  $$ |$$ |$$$$$$$$ |      $$  __$$\  $$$$$$$ |$$ |  $$ |$$$$$$  /                                                                                                                                                                              
    $$ |       $$  $$< $$  __$$ |$$ | $$ | $$ |$$ |  $$ |$$ |$$   ____|      $$ |  $$ |$$  __$$ |$$ |  $$ |$$  _$$<                                                                                                                                                                               
    $$$$$$$$\ $$  /\$$\\$$$$$$$ |$$ | $$ | $$ |$$$$$$$  |$$ |\$$$$$$$\       $$$$$$$  |\$$$$$$$ |$$ |  $$ |$$ | \$$\                                                                                                                                                                              
    \________|\__/  \__|\_______|\__| \__| \__|$$  ____/ \__| \_______|      \_______/  \_______|\__|  \__|\__|  \__|                                                                                                                                                                             
                                               $$ |                                                                                                                                                                                                                                               
                                               $$ |                                                                                                                                                                                                                                               
                                               \__|                                                                                                                                                                                                                                               
     $$$$$$\                                                                                   $$$$$$\                            $$\ $$\   $$\            $$$$$$\                            $$\        $$$$$$\                      $$\ $$\                     $$\     $$\                     
    $$  __$$\                                                                                 $$  __$$\                           $$ |\__|  $$ |          $$  __$$\                           $$ |      $$  __$$\                     $$ |\__|                    $$ |    \__|                    
    $$ /  \__| $$$$$$\  $$$$$$$\   $$$$$$$\ $$\   $$\ $$$$$$\$$$$\   $$$$$$\   $$$$$$\        $$ /  \__| $$$$$$\   $$$$$$\   $$$$$$$ |$$\ $$$$$$\         $$ /  \__| $$$$$$\   $$$$$$\   $$$$$$$ |      $$ /  $$ | $$$$$$\   $$$$$$\  $$ |$$\  $$$$$$$\ $$$$$$\ $$$$$$\   $$\  $$$$$$\  $$$$$$$\  
    $$ |      $$  __$$\ $$  __$$\ $$  _____|$$ |  $$ |$$  _$$  _$$\ $$  __$$\ $$  __$$\       $$ |      $$  __$$\ $$  __$$\ $$  __$$ |$$ |\_$$  _|        $$ |       \____$$\ $$  __$$\ $$  __$$ |      $$$$$$$$ |$$  __$$\ $$  __$$\ $$ |$$ |$$  _____|\____$$\\_$$  _|  $$ |$$  __$$\ $$  __$$\ 
    $$ |      $$ /  $$ |$$ |  $$ |\$$$$$$\  $$ |  $$ |$$ / $$ / $$ |$$$$$$$$ |$$ |  \__|      $$ |      $$ |  \__|$$$$$$$$ |$$ /  $$ |$$ |  $$ |          $$ |       $$$$$$$ |$$ |  \__|$$ /  $$ |      $$  __$$ |$$ /  $$ |$$ /  $$ |$$ |$$ |$$ /      $$$$$$$ | $$ |    $$ |$$ /  $$ |$$ |  $$ |
    $$ |  $$\ $$ |  $$ |$$ |  $$ | \____$$\ $$ |  $$ |$$ | $$ | $$ |$$   ____|$$ |            $$ |  $$\ $$ |      $$   ____|$$ |  $$ |$$ |  $$ |$$\       $$ |  $$\ $$  __$$ |$$ |      $$ |  $$ |      $$ |  $$ |$$ |  $$ |$$ |  $$ |$$ |$$ |$$ |     $$  __$$ | $$ |$$\ $$ |$$ |  $$ |$$ |  $$ |
    \$$$$$$  |\$$$$$$  |$$ |  $$ |$$$$$$$  |\$$$$$$  |$$ | $$ | $$ |\$$$$$$$\ $$ |            \$$$$$$  |$$ |      \$$$$$$$\ \$$$$$$$ |$$ |  \$$$$  |      \$$$$$$  |\$$$$$$$ |$$ |      \$$$$$$$ |      $$ |  $$ |$$$$$$$  |$$$$$$$  |$$ |$$ |\$$$$$$$\\$$$$$$$ | \$$$$  |$$ |\$$$$$$  |$$ |  $$ |
     \______/  \______/ \__|  \__|\_______/  \______/ \__| \__| \__| \_______|\__|             \______/ \__|       \_______| \_______|\__|   \____/        \______/  \_______|\__|       \_______|      \__|  \__|$$  ____/ $$  ____/ \__|\__| \_______|\_______|  \____/ \__| \______/ \__|  \__|
                                                                                                                                                                                                                  $$ |      $$ |                                                                  
                                                                                                                                                                                                                  $$ |      $$ |                                                                  
                                                                                                                                                                                                                  \__|      \__|                                                                  
                  $$\                                                                                                                                                                                                                                                                             
                $$$$ |                                                                                                                                                                                                                                                                            
    $$\    $$\  \_$$ |                                                                                                                                                                                                                                                                            
    \$$\  $$  |   $$ |                                                                                                                                                                                                                                                                            
     \$$\$$  /    $$ |                                                                                                                                                                                                                                                                            
      \$$$  /     $$ |                                                                                                                                                                                                                                                                            
       \$  /$$\ $$$$$$\                                                                                                                                                                                                                                                                           
        \_/ \__|\______|                                                                                                                                                                                                                                                                          



     $$$$$$\                                          $$\           $$\        $$\            $$$$$$\   $$$$$$\   $$$$$$\  $$\   $$\                                                                                                                                                              
    $$  __$$\                                         \__|          $$ |       $$ |          $$  __$$\ $$$ __$$\ $$  __$$\ $$ |  $$ |                                                                                                                                                             
    $$ /  \__| $$$$$$\   $$$$$$\  $$\   $$\  $$$$$$\  $$\  $$$$$$\  $$$$$$$\ $$$$$$\         \__/  $$ |$$$$\ $$ |\__/  $$ |$$ |  $$ |                                                                                                                                                             
    $$ |      $$  __$$\ $$  __$$\ $$ |  $$ |$$  __$$\ $$ |$$  __$$\ $$  __$$\\_$$  _|         $$$$$$  |$$\$$\$$ | $$$$$$  |$$$$$$$$ |                                                                                                                                                             
    $$ |      $$ /  $$ |$$ /  $$ |$$ |  $$ |$$ |  \__|$$ |$$ /  $$ |$$ |  $$ | $$ |          $$  ____/ $$ \$$$$ |$$  ____/ \_____$$ |                                                                                                                                                             
    $$ |  $$\ $$ |  $$ |$$ |  $$ |$$ |  $$ |$$ |      $$ |$$ |  $$ |$$ |  $$ | $$ |$$\       $$ |      $$ |\$$$ |$$ |            $$ |                                                                                                                                                             
    \$$$$$$  |\$$$$$$  |$$$$$$$  |\$$$$$$$ |$$ |      $$ |\$$$$$$$ |$$ |  $$ | \$$$$  |      $$$$$$$$\ \$$$$$$  /$$$$$$$$\       $$ |                                                                                                                                                             
     \______/  \______/ $$  ____/  \____$$ |\__|      \__| \____$$ |\__|  \__|  \____/       \________| \______/ \________|      \__|                                                                                                                                                             
                        $$ |      $$\   $$ |              $$\   $$ |                                                                                                                                                                                                                              
                        $$ |      \$$$$$$  |              \$$$$$$  |                                                                                                                                                                                                                              
                        \__|       \______/                \______/                                                                                                                                                                                                                               

    ''')

def transaction_details_module():
    # 2.1.1 - Prompt for zip code
    while True:
        zip_code = input("Enter a 5-digit zip code: ")
        if re.match(r'^\d{5}$', zip_code):
            break
        print("Invalid zip code. Please enter exactly 5 digits.")

    # 2.1.2 - Prompt for month and year
    while True:
        date_input = input("Enter month and year (MM/YYYY): ")
        try:
            date = datetime.strptime(date_input, "%m/%Y")
            month, year = date.month, date.year
            break
        except ValueError:
            print("Invalid date format. Please use MM/YYYY.")

    # 2.1.3 & 2.1.4 - Query database and sort transactions
    transactions = get_transaction_by_zip_year_month(zip_code, year, month)

    # Display results
    if transactions:
        print("\nTransactions for ZIP: {}, Date: {}/{}".format(zip_code, month, year))
        print("{:<15} {:<20} {:<20} {:<20} {:<12} {:<15} {:<10} {:<20}".format(
            "SSN", "First Name", "Last Name", "Credit Card No", "Branch ZIP", "Date", "Day", "Total Value"))
        print("-" * 132)
        for transaction in transactions:
            print("{:<15} {:<20} {:<20} {:<20} {:<12} {:<15} {:<10} ${:<19.2f}".format(*transaction))
    else:
        print("No transactions found for the given criteria.")

def get_transaction_by_zip_year_month(zip_code, year, month):
    connection = connect_to_database()
    if connection is None:
        print("Failed to connect to the database")
        return []

    try:
        with connection.cursor() as cursor:
            query = """
            SELECT 
                CC.SSN, CC.FIRST_NAME, CC.LAST_NAME, CC.CREDIT_CARD_NO,
                BR.BRANCH_ZIP, DATE(CR.TIMEID) AS transaction_date,  
                DAYOFMONTH(CR.TIMEID) AS transaction_day,
                SUM(CR.TRANSACTION_VALUE) AS total_transaction_value
            FROM 
                CDW_SAPP_CREDIT_CARD CR
            JOIN 
                CDW_SAPP_CUSTOMER CC ON CR.CREDIT_CARD_NO = CC.CREDIT_CARD_NO
            JOIN 
                CDW_SAPP_BRANCH BR ON CR.BRANCH_CODE = BR.BRANCH_CODE
            WHERE 
                BR.BRANCH_ZIP = %s AND 
                YEAR(CR.TIMEID) = %s AND
                MONTH(CR.TIMEID) = %s
            GROUP BY 
                CC.SSN, CC.FIRST_NAME, CC.LAST_NAME, CC.CREDIT_CARD_NO, 
                BR.BRANCH_ZIP, DATE(CR.TIMEID), DAYOFMONTH(CR.TIMEID)
            ORDER BY 
                transaction_date DESC, transaction_day DESC
            """
            cursor.execute(query, (zip_code, year, month))
            return cursor.fetchall()
    except mysql.connector.Error as error:
        print(f"Failed to retrieve transactions: {error}")
        return []
    finally:
        connection.close()

def check_existing_account_details(card_number):
    connection = connect_to_database()
    if connection is None:
        print("Failed to connect to the database")
        return

    try:
        with connection.cursor() as cursor:
            query = "SELECT * FROM CDW_SAPP_CUSTOMER WHERE CREDIT_CARD_NO = %s"
            cursor.execute(query, (card_number,))
            customer = cursor.fetchone()
            if customer:
                print("Customer Details:")
                print(f"SSN: {customer[0]}")
                print(f"First Name: {customer[1]}")
                print(f"Middle Name: {customer[2]}")
                print(f"Last Name: {customer[3]}")
                print(f"Credit Card No: {customer[4]}")
                print(f"Full Street Address: {customer[5]}")
                print(f"City: {customer[6]}")
                print(f"State: {customer[7]}")
                print(f"Country: {customer[8]}")
                print(f"ZIP: {customer[9]}")
                print(f"Phone: {customer[10]}")
                print(f"Email: {customer[11]}")
            else:
                print("No customer found with that credit card number.")
    except mysql.connector.Error as error:
        print(f"Failed to retrieve customer details: {error}")
    finally:
        connection.close()

def generate_monthly_bill(card_number, month, year):
    connection = connect_to_database()
    if connection is None:
        print("Failed to connect to the database")
        return

    try:
        with connection.cursor() as cursor:
            query = """
            SELECT 
                CC.SSN, CC.FIRST_NAME, CC.LAST_NAME,
                SUM(CR.TRANSACTION_VALUE) AS total_bill
            FROM 
                CDW_SAPP_CREDIT_CARD CR
            JOIN 
                CDW_SAPP_CUSTOMER CC ON CR.CREDIT_CARD_NO = CC.CREDIT_CARD_NO
            WHERE 
                CR.CREDIT_CARD_NO = %s AND
                YEAR(CR.TIMEID) = %s AND
                MONTH(CR.TIMEID) = %s
            GROUP BY 
                CC.SSN, CC.FIRST_NAME, CC.LAST_NAME
            """
            cursor.execute(query, (card_number, year, month))
            bill_data = cursor.fetchone()

            if bill_data:
                print(f"Monthly Bill for {bill_data[1]} {bill_data[2]} (SSN: {bill_data[0]}):")
                print(f"Credit Card: {card_number}")
                print(f"Month/Year: {month}/{year}")
                print(f"Total Bill: ${bill_data[3]:.2f}")
            else:
                print("No transactions found for the given card, month, and year.")
    except mysql.connector.Error as error:
        print(f"Failed to generate monthly bill: {error}")
    finally:
        connection.close()


def display_customer_transactions(card_number, start_date, end_date):
    connection = connect_to_database()
    if connection is None:
        print("Failed to connect to the database")
        return

    try:
        with connection.cursor() as cursor:
            # First, let's check if the card number exists
            check_query = "SELECT COUNT(*) FROM CDW_SAPP_CREDIT_CARD WHERE CREDIT_CARD_NO = %s"
            cursor.execute(check_query, (card_number,))
            count = cursor.fetchone()[0]
            if count == 0:
                print(f"No transactions found for card number: {card_number}")
                return

            # Now, let's modify the main query to be more inclusive and add some debugging
            query = """
            SELECT 
                DATE(TIMEID) AS transaction_date,
                TRANSACTION_TYPE,
                TRANSACTION_VALUE,
                TIMEID
            FROM CDW_SAPP_CREDIT_CARD
            WHERE CREDIT_CARD_NO = %s 
              AND DATE(TIMEID) BETWEEN DATE(%s) AND DATE(%s)
            ORDER BY TIMEID DESC
            """
            cursor.execute(query, (card_number, start_date, end_date))
            transactions = cursor.fetchall()

            if transactions:
                print(f"Transactions for card {card_number} between {start_date} and {end_date}:")
                print("{:<15} {:<20} {:<15} {:<20}".format("Date", "Type", "Amount", "Exact DateTime"))
                print("-" * 70)
                for transaction in transactions:
                    print("{:<15} {:<20} ${:<14.2f} {:<20}".format(
                        transaction[0].strftime('%Y-%m-%d'),
                        transaction[1],
                        transaction[2],
                        transaction[3]
                    ))
            else:
                print("No transactions found for the given card number and date range.")
                # Let's print some debug info
                print(f"Debug: Card number: {card_number}")
                print(f"Debug: Start date: {start_date}")
                print(f"Debug: End date: {end_date}")

                # Let's check the date range for this card
                range_query = """
                SELECT MIN(DATE(TIMEID)), MAX(DATE(TIMEID)) 
                FROM CDW_SAPP_CREDIT_CARD 
                WHERE CREDIT_CARD_NO = %s
                """
                cursor.execute(range_query, (card_number,))
                date_range = cursor.fetchone()
                if date_range:
                    print(f"Debug: Transactions for this card exist from {date_range[0]} to {date_range[1]}")

    except mysql.connector.Error as error:
        print(f"Failed to retrieve customer transactions: {error}")
    finally:
        connection.close()
def get_account_modification_input():
    card_number = input("Enter the credit card number: ")

    print("\nWhich field would you like to update?")
    print("1. First Name")
    print("2. Last Name")
    print("3. Full Street Address")
    print("4. City")
    print("5. State")
    print("6. Country")
    print("7. ZIP")
    print("8. Phone")
    print("9. Email")

    choice = input("Enter your choice (1-9): ")

    field_mapping = {
        '1': 'FIRST_NAME',
        '2': 'LAST_NAME',
        '3': 'FULL_STREET_ADDRESS',
        '4': 'CUST_CITY',
        '5': 'CUST_STATE',
        '6': 'CUST_COUNTRY',
        '7': 'CUST_ZIP',
        '8': 'CUST_PHONE',
        '9': 'CUST_EMAIL'
    }

    if choice not in field_mapping:
        raise ValueError("Invalid choice")

    field_to_update = field_mapping[choice]
    new_value = input(f"Enter the new value for {field_to_update}: ")

    return card_number, field_to_update, new_value

import mysql.connector

def modify_account_details(connection, card_number, field_to_update, new_value):
    allowed_fields = {
        'FIRST_NAME': lambda x: len(x) <= 50 and x.replace(' ', '').isalpha(),
        'LAST_NAME': lambda x: len(x) <= 50 and x.replace(' ', '').isalpha(),
        'FULL_STREET_ADDRESS': lambda x: len(x) <= 100,
        'CUST_CITY': lambda x: len(x) <= 50 and x.replace(' ', '').isalpha(),
        'CUST_STATE': lambda x: len(x) == 2 and x.isalpha(),
        'CUST_COUNTRY': lambda x: len(x) <= 50 and x.replace(' ', '').isalpha(),
        'CUST_ZIP': lambda x: len(x) == 5 and x.isdigit(),
        'CUST_PHONE': lambda x: re.match(r'^\(\d{3}\)\d{3}-\d{4}$', x) is not None,
        'CUST_EMAIL': lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', x) is not None,
    }

    if field_to_update not in allowed_fields:
        raise ValueError(f"Invalid field: {field_to_update}")

    if not allowed_fields[field_to_update](new_value):
        raise ValueError(f"Invalid value for {field_to_update}")

    cursor = None
    try:
        cursor = connection.cursor()

        # Check if the account exists
        check_query = "SELECT COUNT(*) FROM CDW_SAPP_CUSTOMER WHERE CREDIT_CARD_NO = %s"
        cursor.execute(check_query, (card_number,))
        count = cursor.fetchone()[0]
        print(f"Debug: Count of matching accounts: {count}")

        if count == 0:
            print(f"Debug: No account found with credit card number: {card_number}")
            return

        # Check the value before update
        cursor.execute(f"SELECT {field_to_update} FROM CDW_SAPP_CUSTOMER WHERE CREDIT_CARD_NO = %s", (card_number,))
        before_value = cursor.fetchone()[0]
        print(f"Debug: Value before update: {before_value}")

        # If the account exists, proceed with the update
        query = f"UPDATE CDW_SAPP_CUSTOMER SET {field_to_update} = %s WHERE CREDIT_CARD_NO = %s AND {field_to_update} != %s"
        print(f"Debug: Executing query: {query} with values {new_value}, {card_number}, and {new_value}")
        cursor.execute(query, (new_value, card_number, new_value))
        connection.commit()

        print(f"Debug: Rows affected by update: {cursor.rowcount}")

        # Check the value after update
        cursor.execute(f"SELECT {field_to_update} FROM CDW_SAPP_CUSTOMER WHERE CREDIT_CARD_NO = %s", (card_number,))
        after_value = cursor.fetchone()[0]
        print(f"Debug: Value after update: {after_value}")

        if cursor.rowcount == 0:
            print(f"No changes were made to the account for credit card number: {card_number}")
        else:
            print(f"Account details updated successfully for card number: {card_number}")

        # Check the final state of the account
        cursor.execute("SELECT * FROM CDW_SAPP_CUSTOMER WHERE CREDIT_CARD_NO = %s", (card_number,))
        result = cursor.fetchone()
        if result:
            print(f"Debug: Final state of account: {result}")
        else:
            print("Debug: Could not retrieve final state of account")

    except mysql.connector.Error as error:
        print(f"Failed to update account details: {error}")
        connection.rollback()

    finally:
        if cursor:
            cursor.close()



    



def main():
    max_retries = 3
    for i in range(max_retries):
        connection = connect_to_database()
        if connection is not None:
            startup_sequence()
            welcome_title()
            break
        else:
            print(f"Attempt {i + 1}: Failed to connect to the database.")
            if i < max_retries - 1:
                print("Retrying...")
                time.sleep(5)
    else:
        print(f"Could not establish connection after {max_retries} attempts")
        return

    while True:
        print("\nMain Menu:")
        print("1. Get transactions by ZIP code, month and year")
        print("2. Check existing account details")
        print("3. Modify existing account details")
        print("4. Generate monthly bill")
        print("5. Display transactions between two dates")
        print("E. Exit")

        choice = input("Enter your choice: ").upper()

        if choice == '1':
            transaction_details_module()
        elif choice == '2':
            card_number = input("Enter credit card number: ")
            check_existing_account_details(card_number)
        elif choice == '3':
            connection = connect_to_database()
            if connection:
                try:
                    card_number, field_to_update, new_value = get_account_modification_input()
                    modify_account_details(connection, card_number, field_to_update, new_value)
                except ValueError as e:
                    print(f"Error: {e}")
                finally:
                    connection.close()
            else:
                print("Failed to connect to the database")
        elif choice == '4':
            card_number = input("Enter credit card number: ")
            month = int(input("Enter month (1-12): "))
            year = int(input("Enter year: "))
            generate_monthly_bill(card_number, month, year)
        elif choice == '5':
            card_number = input("Enter credit card number: ")
            start_date = input("Enter start date (YYYY-MM-DD): ")
            end_date = input("Enter end date (YYYY-MM-DD): ")
            display_customer_transactions(card_number, start_date, end_date)
        elif choice == 'E':
            print("Thank you for using the Credit Card Management System. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()