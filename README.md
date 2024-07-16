# Per_Scholas Data Engineering Capstone_350
 ##Per_Scholas Data Engineering Final Capstone 2024
 This project contains all files related to the submission of the Data Engineering Capstone 350 in July of 2024. The primary goal of this capstone project entails the extraction of credit card transactions, cutomer, branch, and loan data from various datasets. The raw data requires extensive data cleaning (as described below) at which point the data can be loaded into a database.  In addition, the project includes a detailed analysis of the data, including several data visualizations, as well as a front-end console user interface application for interaction with the data.

## Workflow Process for Capstone
The Capstone_Workflow jpg file provides a clear illustration of the process management for the Capstone. It succintly describes the process of extracting JSON raw files, converting them to Pyspark dataframes, and then loading into the MySQL Credit_Capstone_databse. It also details the extraction of loan application API data that was also tranformed into a Pyspark file that was subsequently written into the MySQL database. Please review the Figure in the Figures_Visualizations folder.

!Capstone_Workflow.jpg



 ## Primary Objectives
1. Extract data from 4 customer related datasets, clean data, and load into a database.  The datasets cover Credit Card Transactions, Bank Branch information, and Customer information.  Tools used to meet objective 1 include:
   * PySpark for extracting and cleaning
   * 4 raw data JSON files
   * MySQL database
2. Build a front-end python based Menu console application to find customer data and modify that data in the database, if necessary, based on menu commands entered by the front-end user.
   * MySQL connector library
   * credentials module import
3. Provide visualizations that perform the following tasks:
    * Calculate and plot which transaction type has the highest transaction count
    * Calculate and plot top 10 states with the highest number of customers
    * Calculate the total transaction sum for each customer based on their individual transactions. Identify the top 10 customers with the highest transaction amounts (in 
        ** python libraries
        ** MySql Database
        ** Tableau
4. Pull Loan Application data from an API (https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json) endpoint and store it in the database
   * python libraries
   * PySpark SQL
   * MySQL database
5. Provide visualizations that provide analysis using loan data loaded in step 4: 
   * Calculate and plot the percentage of applications approved for self-employed applicants. Use the appropriate chart or graph to represent this data.
   * Calculate the percentage of rejection for married male applicants. Use the ideal chart or graph to represent this data
   * Calculate and plot the top three months with the largest volume of transaction data. Use the ideal chart or graph to represent this data
   * Calculate and plot which branch processed the highest total dollar value of healthcare transactions. Use the ideal chart or graph to represent this data
        ** python libraries
        ** Tableau


## Requirements to run this repo
* The requirements.txt file contains all the python libraries needed to run this project.  
* The pyspark 3.5 library requires installation of spark on your system for the library to function. If necessary for proper functioning, the my-sql connector needs to be installed in the jars subfolder
* The credentials.py file contains the information needed to log into the MySQL db.  Credentials need to be customized as per users' specific use cases. The file should be an imported module as well.
## Notes about the files in this repo
* Mapping Document - Guidance on mapping the cleaned JSON files to the MySQL database. Initially, I converted the cleaned JSON files to panda dataframes from spark dataframes so as to make it easier to load data into the MySQL database. However, this proved to be an untenable situation as the clean JSON files were not properly formatted as arrays thus causing significant issues loading into the creditcard_capston.db. Instead. I used the "jdbc" connector to write the data to the database.
* CAP 350 - Data Engineering - Capstone Project Requirements Document.docx
* ZipData.csv - Crosswalk of zip code data to area codes
* The source data directory contains the 3 datasets I was suplied with (files preceeded by cdw).  The area_codes.json file contains the cleaned up area code data that I used to generate the area codes for the customer's phone numbers.  The full_area_code_dataset.zip is the full area code dataset in a zipped up form because it was too big to be uploaded to github in its raw state.  The source of the geojson file is (kaggle datasets download -d pompelmo/usa-states-geojson)
* The clean data directory contains the datasets after they had been transformed and cleaned in step 1.
* The SQL_scripts_Capstone_350.sql file contains the sql script to create and populate the entire MySQL database that I created and loaded during the steps in this project and also contains the SQL queries for the user terminal application.
* Credentials.py contains the login information for the MySQL database.  It isn't included in the repo because it contains sensitive information.  Users need to create their own credentials.py file with a similar format (user can identify the MySQL database if desired):
```
host_name = xxxxxx (localhost)
user_name = xxxxxx
password = xxxxxx
port = xxxxxx
```

## Data Extraction, Exploration, Cleaning, Transformation, and Loading
Each python script has signficant notes and guidance, in markdown language or comments, about these topics. Review these for more specific insights.