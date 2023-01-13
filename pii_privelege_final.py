import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import csv
import sys
from datetime import datetime
import pymysql
import os
from datetime import datetime


class Sheet:
    client = None

    def __init__(self, sheet_file_name: str, credentials_path: str):
        self.sheet_file_name = sheet_file_name
        self.credentials_path = credentials_path

    def get_client(self):
        if Sheet.client is None:
            try:
                client = gspread.service_account(self.credentials_path)
                Sheet.client = client
            except Exception as e:
                print(f"Error getting client: {e}")
        return Sheet.client

    def get_workbook(self):
        try:
            return self.get_client().open(self.sheet_file_name)
        except Exception as e:
            print(f"Error getting workbook: {e}")

    def get_sheets(self):
        try:
            return self.get_workbook().worksheets()
        except Exception as e:
            print(f"Error getting sheets: {e}")

    def get_sheet(self, worksheet_name):
        try:
            return self.get_workbook().worksheet(worksheet_name)
        except Exception as e:
            print(f"Error getting sheet: {e}")


class SheetData:

    def __init__(self, worksheet: gspread.Worksheet):
        self.worksheet = worksheet
        self.df = pd.DataFrame(worksheet.get_all_records())

    def get_db_users_list(self) -> list:
        """
        Retrieves the unique list of database users from the 'pii db users' column in the worksheet.

        Returns:
        - A list of unique database users.
        :return:
        """
        try:
            # Extract the 'pii db users' column from the DataFrame
            users = self.df['pii db users'].tolist()
            # looks like this:  users = ['db1,db2', '', 'db3', 'db4,db5']

            # Initialize an empty list to store the split user lists
            user_lists = []

            # Loop through the users
            for user in users:
                # Split the user string into a list and add it to the list of user lists
                user_list = user.split(',')
                # truncate white spaces from the beginning and end of every db user name
                user_list = [user.strip() for user in user_list]
                user_lists.append(user_list)
                # 2d list

            # Flatten the list of lists into a single list
            users = [user for user_list in user_lists for user in user_list]

            # Remove any empty strings from the list
            users = [user for user in users if user]

            # Return the unique list of users
            return list(set(users))
        except Exception as e:
            print(f"Error getting database users: {e}")

    def table_dict(self) -> dict:
        """
        Reads a worksheet and returns a dictionary where the keys are the table names and the values are lists of columns belonging to that table.
        """
        try:
            # Initialize the dictionary
            table_dict = {}

            # Loop through the rows of the DataFrame
            for index, row in self.df.iterrows():
                # Get the table name and column from the row
                tablename = row['tablename']
                column = row['PII col list']

                # If the table name is not in the dictionary, add it with an empty list as the value
                if tablename not in table_dict:
                    table_dict[tablename] = []

                # Add the column to the list of columns for the table
                table_dict[tablename].append(column)

            return table_dict
        except Exception as e:
            print(f"Error getting table dictionary: {e}")

    def get_user_permissions(self) -> dict:

        """
                Returns a nested dictionary where the outer keys are the database users and the inner keys are the table names,
                and the values are lists of columns that are accessible by each database user for each table.

                Returns dic like:

                {
                    "db1": {
                        "aadhaar_manual_verification": ["response"],
                        "borrower_user_snapshot": ["phone_number", "pan_number"]
                    },
                    "db2": {
                    "aadhaar_manual_verification": ["response"],
                    "borrower_user_snapshot": ["phone_number", "pan_number"]
                    }
                }
        """
        try:
            # Initialize the dictionary
            user_permissions = {}

            # Loop through the rows of the DataFrame
            for index, row in self.df.iterrows():
                # skip the row where no pii col list column is empty
                if row['pii db users'] == "":
                    continue

                # Get the table name, column, and database users from the row
                tablename = row['tablename']
                column = row['PII col list']
                users = row['pii db users'].split(',')

                # Loop through the database users
                for user in users:
                    # Strip any leading or trailing white space from the user name
                    user = user.strip()

                    # If the user is not in the dictionary, add it with an empty dictionary as the value
                    if user not in user_permissions:
                        user_permissions[user] = {}

                    # If the table name is not in the inner dictionary, add it with an empty list as the value
                    if tablename not in user_permissions[user]:
                        user_permissions[user][tablename] = []

                    # Add the column to the list of columns for the table and user
                    user_permissions[user][tablename].append(column)

            return user_permissions
        except Exception as e:
            # Handle the exception here, for example by logging the error or printing an error message
            print(f"An error occurred: {e}")


class MySQLConnection:
    def __init__(self, host: str, user: str, password: str, db: str, port: int = 3306):
        """
        Initializes a MySQL connection with the specified parameters.

        Parameters:
        - host (str): The hostname or IP address of the MySQL server.
        - user (str): The username to use when connecting to the MySQL server.
        - password (str): The password to use when connecting to the MySQL server.
        - db (str): The name of the database to use.
        - port (int): The port number to use when connecting to the MySQL server. Defaults to 3306.
        """
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Connects to the MySQL server and initializes the cursor object.
        """
        try:
            self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password,
                                              db=self.db, port=self.port)
            self.cursor = self.connection.cursor()
            print("\n\n Connection To SQL Server Successful\n\n")
        except Exception as e:
            print(f"Error connecting to MySQL server: {e}")

    def execute_query(self, query: str) -> list:
        """
        Executes the specified query and returns the results.

        Parameters:
        - query (str): The query to execute.

        Returns:
        - A list of rows returned by the query.
        """
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            result_list = []
            for row in rows:
                result_list.append(row[0])
            return result_list
        except Exception as e:
            print(f"Error executing query: {e}")

    def close(self):
        """
        Closes the MySQL connection and cursor.
        """
        self.cursor.close()
        self.connection.close()


class File:
    def __init__(self, file_prefix: str):
        """
        Initializes a File object with the specified file prefix.

        Parameters:
        - file_prefix (str): The prefix to use for the file name.
        """
        self.file_prefix = file_prefix
        self.filename = None
        self.file = None

    def generate_filename(self):
        """
        Generates a unique file name based on the current timestamp.
        """
        try:
            timestamp = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
            self.filename = self.file_prefix    + '.sql'
        except Exception as e:
            print(f"Error generating file name: {e}")

    def create_file(self):
        """
        Creates a new file with the generated file name.
        """
        try:
            self.file = open(self.filename, 'x')
        except Exception as e:
            print(f"Error creating file: {e}")

    def write_grants(self, grants: list):
        """
        Writes the specified grants to the file.

        Parameters:
        - grants (list): The grants to write to the file.
        """
        try:
            for grant in grants:
                self.file.write(grant + '\n')
        except Exception as e:
            print(f"Error writing to file: {e}")

    def close_file(self):
        """
        Closes the file.
        """
        self.file.close()


def generate_sql_nonpii(sqlObject, csv_tables, username, tables, grants):
    dbname = sqlObject.db
    # generate sql command of all tables
    # subtract the column list is table name present in csv file : csv_tables
    for t_name in tables:
        if t_name not in list(csv_tables.keys()):
            # grant select access to whole table
            sql = "GRANT SELECT ON " + dbname + "." + t_name + " TO "  + username + ";"
            grants.append(sql)
        else:

            sqlquery = "SELECT `COLUMN_NAME`  FROM `INFORMATION_SCHEMA`.`COLUMNS`  WHERE `TABLE_SCHEMA`='" + dbname + "'  AND `TABLE_NAME`='" + t_name + "'; "
            col_list = sqlObject.execute_query(sqlquery)  # get list of columns in table : t_name
            permission = [x for x in col_list if x not in csv_tables[t_name]]  # subtract columns in csv_file
            str = ",".join(permission)  # filtered col
            sql = "GRANT SELECT (" + str + ") ON " + sqlObject.db + "." + t_name + " TO " + username + ";"
            grants.append(sql)


def generate_sql_pii(sqlObject, privilege_dict, grants):
    x = privilege_dict
    for username, tables in x.items():
        for table, columns in tables.items():
            column_str = ",".join(columns)
            sql = "GRANT SELECT (" + column_str + ") ON " + sqlObject.db + "." + table + " TO " + username + ";"
            grants.append(sql)


def sheet_helper(workbook_file_name = "pii_sheet", worksheet_file_name = "pii", credentials_path = ''):

    # create sheet object
    sheet = Sheet(workbook_file_name, credentials_path)
    worksheet = sheet.get_sheet(worksheet_file_name)
    print("\n\n Connection To Google Sheet API Successful\n\n")
    return worksheet


def sql_helper(host = '10.180.0.3' , user = 'bolt_dev', password = 'bolT123', db = 'rebase_qa23'):
    # Assume that the necessary values have been stored in variables
    port = 3306
    # Create an instance of the MySQLConnection class
    conn = MySQLConnection(host, user, password, db, port)
    return conn


def main():
    # take arguments for connection
    host = None
    dbUser = None
    dbPassword = None
    dbName = None
    user = None

    workbook_file_name = None
    worksheet_file_name = None
    credentials_path = None

    try :
        host, dbUser , dbPassword, dbName, workbook_file_name, worksheet_file_name, credentials_path =  sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]
    except:
        print(" \nplease provide all arguments in order: host, dbUser , dbPassword, dbName, workbook_file_name, worksheet_file_name, credentials_path  \n ")
        return 0

    # initialise worksheet object
    worksheet = sheet_helper(workbook_file_name, worksheet_file_name, credentials_path)
    # initialise SheetData object to retreive data from worksheet
    sheet_data = SheetData(worksheet)

    # create dic where keys are table name and values are list of pii columns
    tables_dict = sheet_data.table_dict()
    # create list of db users mentioned in sheet
    users_list = sheet_data.get_db_users_list()
    # create nested dict, having
    pii_privelege_dict = sheet_data.get_user_permissions()
    # print(tables_dict, end='\n\n')
    # print(users_list, end='\n\n')
    # print(pii_privelege_dict, end='\n\n')

    # initialise MySql Server Conection object
    conn = sql_helper( host, dbUser , dbPassword, dbName)
    # form connection to the SQL server
    conn.connect()

    sqlquery = " SHOW TABLES;"
    tables = conn.execute_query(sqlquery)
    # print("\n\n\ntables\n\n\n")
    # print(tables)

    grants = []

    for username in users_list:
        generate_sql_nonpii(conn, tables_dict, username, tables, grants)

    #create statements for pii columns priveleges to all users
    generate_sql_pii(conn, pii_privelege_dict, grants)
    # print("\n\n\n GRANTS\n\n\n")
    # print(len(grants))

    # for el in grants:
    #     print(el, end="\n")

    file = File("grants_file")
    # Generate a unique file name
    file.generate_filename()
    # Create a new file
    file.create_file()
    # Write the contents of the grants list to the file
    file.write_grants(grants)
    # Close the file
    file.close_file()

    return 0


if __name__ == "__main__":
    main()


