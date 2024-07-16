
# Connection to cdw_sapp_branch
branch_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()

# Connection to cdw_sapp_credit_card

credit_card_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()

# Connection to cdw_sapp_customer
final_customer_dedup_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER ") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()

# Establish table with connection to DB
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
        
        
# Upload cdw_sapp_loan_application
loan_application_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
  .option("user", cred.user) \
  .option("password", cred.password) \
  .save()
  
  # Highest number of transactions
  transaction_type = ("SELECT TRANSACTION_TYPE, count(TRANSACTION_TYPE) "
                    "FROM cdw_sapp_credit_card "
                    "GROUP BY TRANSACTION_TYPE "
                    "ORDER BY TRANSACTION_TYPE")
cursor.execute(transaction_type)
transactions = cursor.fetchall()  
transaction_type_df = pd.DataFrame(transactions)
transaction_type_df.head()
transaction_type_df = transaction_type_df.sort_values(1, ascending=False)
print(transaction_type_df.head())
print(transaction_type_df.to_string(index=False))

# Top 10 states with the highest number of customers.
state_counts_sql=('''SELECT CUST_STATE, COUNT(CUST_STATE) as Count 
                FROM cdw_sapp_customer
                GROUP BY CUST_STATE
                ORDER BY Count DESC''')
cursor.execute(state_counts_sql)   #cursor was assigned in the connect_sql() function
state_results = cursor.fetchall()

customer_states = []
state_counts = []

for row in state_results:
    customer_states.append(row[0])
    state_counts.append(row[1]) 
    
state_counts_sql = '''SELECT CUST_STATE, COUNT(CUST_STATE) as Count 
                FROM cdw_sapp_customer
                GROUP BY CUST_STATE
                ORDER BY Count DESC'''
cursor.execute(state_counts_sql)   #cursor was assigned in the connect_sql() function
state_results = cursor.fetchall()

# Print header
print("State | Count")
print("--------------")

# Print each row
for row in state_results:
    state = row[0]
    count = row[1]
    print(f"{state:5} | {count:5}")
    
# The top 10 customers with the highest transaction amounts (in dollar value) by SSN

import pandas as pd

cust_transactions_sql = '''
    SELECT CUST_SSN as SSN, count(TRANSACTION_VALUE) as Quantity
    FROM cdw_sapp_credit_card
    GROUP BY CUST_SSN
    ORDER BY Quantity DESC
    LIMIT 10
'''

customer_transactions_df = pd.read_sql(cust_transactions_sql, conn)
customer_transactions_df.head()

cust_highest_sql=('''SELECT CUST_SSN as SSN,sum(TRANSACTION_VALUE) as Total
                FROM cdw_sapp_credit_card
                GROUP BY CUST_SSN 
                ORDER BY Total DESC
                LIMIT 10''')

cursor.execute(cust_highest_sql)   #cursor was assigned in the connect_sql() function
customer_highest = cursor.fetchall()

cust_ssn = []
trans_total = []

for row in customer_highest:
    cust_ssn.append(row[0])
    trans_total.append(row[1]) 

print(customer_highest)
customer_highest_df = pd.read_sql(cust_highest_sql, conn)
customer_highest_df.head(11)

# Loan Application Visualizations
self_applicant_approved = ('''SELECT Application_Status, COUNT(*) AS COUNT
                           FROM cdw_sapp_loan_application
                           WHERE Self_employed = "Yes"
                           GROUP BY Application_Status''')
cursor.execute(self_applicant_approved)
self_applicant_approvals = cursor.fetchall()



self_applicant_approvals_df = pd.DataFrame(self_applicant_approvals)
self_applicant_approvals_df

# Total Acceptance Rate
all_others_status = ("SELECT Application_Status, COUNT(*) AS Count "
                    "FROM cdw_sapp_loan_application "
                    "GROUP BY Application_Status")
cursor.execute(all_others_status)                #cursor was assigned in the connect_sql() function
all_others_results = cursor.fetchall()          
all_others_status_df = pd.DataFrame(all_others_results)
all_others_status_df

# Married male applicants
married_male = ('''SELECT Application_Status, COUNT(*) AS Count
                    FROM cdw_sapp_loan_application
                    WHERE Gender = 'Male' AND Married = 'Yes'
                    GROUP BY Application_Status
                    ORDER BY Count DESC''')
cursor.execute(married_male)                #cursor was assigned in the connect_sql() function
married_male_results = cursor.fetchall()          
married_male_df = pd.DataFrame(married_male_results)
married_male_df

# Top three months of transaxtions
top_three_months = ('''SELECT SUBSTRING(TIMEID,5,2) AS mon, sum(transaction_value) as total
                FROM cdw_sapp_credit_card
                GROUP BY mon
                ORDER BY total DESC
                LIMIT 3''')

cursor.execute(top_three_months)                #cursor was assigned in the connect_sql() function
top_three_months_results = cursor.fetchall()          
top_three_months_df = pd.DataFrame(top_three_months_results)
top_three_months_df

top_three_months = ('''SELECT SUBSTRING(TIMEID,5,2) AS mon, sum(transaction_value) as total
                FROM cdw_sapp_credit_card
                GROUP BY mon
                ORDER BY total DESC
                LIMIT 3''')

cursor.execute(top_three_months)                #cursor was assigned in the connect_sql() function
top_three_months_results = cursor.fetchall() 
import pandas as pd

# ... (Your code to connect to the database and execute the query)
cursor.execute(top_three_months)
top_three_months_results = cursor.fetchall()

# Create a Pandas DataFrame from the fetched results
top_three_months_df = pd.DataFrame(top_three_months_results, columns=['Month', 'Total Value'])

# Map month numbers to month names
month_names = {
    '01': 'January',
    '02': 'February',
    '03': 'March',
    '04': 'April',
    '05': 'May',
    '06': 'June',
    '07': 'July',
    '08': 'August',
    '09': 'September',
    '10': 'October',
    '11': 'November',
    '12': 'December'
}

# Convert 'Month' column to string and replace with month names
top_three_months_df['Month'] = top_three_months_df['Month'].astype(str).replace(month_names)

print(top_three_months_df)

top_three_months_volume = ('''SELECT SUBSTRING(TIMEID,5,2) AS mon, count(transaction_value) as total
                FROM cdw_sapp_credit_card
                GROUP BY mon
                ORDER BY total DESC
                LIMIT 3''')

cursor.execute(top_three_months_volume)                #cursor was assigned in the connect_sql() function
top_three_months_volume_results = cursor.fetchall() 
import pandas as pd

top_three_months_volume_df = pd.DataFrame(top_three_months_volume_results, columns=['Month', 'Total Volume'])

# Map month numbers to month names
month_names = {
    '01': 'January',
    '02': 'February',
    '03': 'March',
    '04': 'April',
    '05': 'May',
    '06': 'June',
    '07': 'July',
    '08': 'August',
    '09': 'September',
    '10': 'October',
    '11': 'November',
    '12': 'December'
}

# Convert 'Month' column to string and replace with month names
top_three_months_volume_df['Month'] = top_three_months_volume_df['Month'].astype(str).replace(month_names)

print(top_three_months_volume_df)

# Branch top transactions for healthcare
branch_healthcare_tv = ('''SELECT BRANCH_CODE, sum(transaction_value) as total
                    FROM cdw_sapp_credit_card
                    WHERE TRANSACTION_TYPE = 'healthcare'
                    GROUP BY BRANCH_CODE
                    ORDER BY total DESC
                    LIMIT 5''')
cursor.execute(branch_healthcare_tv)
branch_healthcare_tv_results = cursor.fetchall()
branch_healthcare_tv_df = pd.DataFrame(branch_healthcare_tv_results)
branch_healthcare_tv_df = branch_healthcare_tv_df.rename(columns = {0: 'Branch Code'})
branch_healthcare_tv_df = branch_healthcare_tv_df.rename(columns={1: 'Transaction Value'})
branch_healthcare_tv_df

# Function get_transaction_by_zip_year_month(zip_code, year, month):
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
                
# Function generate_monthly_bill(card_number, month, year):
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
# Function display_customer_transactions(card_number, start_date, end_date):

SELECT 
                DATE(TIMEID) AS transaction_date,
                TRANSACTION_TYPE,
                TRANSACTION_VALUE,
                TIMEID
            FROM CDW_SAPP_CREDIT_CARD
            WHERE CREDIT_CARD_NO = %s 
              AND DATE(TIMEID) BETWEEN DATE(%s) AND DATE(%s)
            ORDER BY TIMEID DESC
            
# range query
SELECT MIN(DATE(TIMEID)), MAX(DATE(TIMEID)) 
                FROM CDW_SAPP_CREDIT_CARD 
                WHERE CREDIT_CARD_NO = %s
                
SELECT COUNT(*) FROM CDW_SAPP_CUSTOMER WHERE CREDIT_CARD_NO = %s

UPDATE CDW_SAPP_CUSTOMER SET {field_to_update} = %s WHERE CREDIT_CARD_NO = %s AND {field_to_update} != %s




                
