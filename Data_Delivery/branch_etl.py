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
!pip list


