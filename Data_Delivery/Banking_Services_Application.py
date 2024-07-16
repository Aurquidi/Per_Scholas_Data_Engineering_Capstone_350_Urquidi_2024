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