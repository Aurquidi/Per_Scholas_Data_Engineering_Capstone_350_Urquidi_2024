# %%
#!pip install pyspark

# %%
#Import all necessary libraries and packages

import pyspark
import pyspark.sql.functions as funct
from pyspark.sql import SparkSession
import pandas as pd
import credentials as cred 

# %%
#Creating a shortcut to the filepath for the source file for branches
customer_filepath = r"C:\Users\chito\Developer\Per_Scholas_Data_Engineering_Capstone_350_Urquidi_2024\Source_Data\cdw_sapp_customer.json"

# %%
#This creates the new Sparksession
spark = SparkSession.builder.appName("customer_clean").getOrCreate()
#spark = SparkSession.builder.master("local").appName("test").getOrCreate()


# %%
#Google Colab
#Creating a shortcut to the filepath for the source file for branches
#customer_filepath = "/content/cdw_sapp_customer.json"

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
#pip list


