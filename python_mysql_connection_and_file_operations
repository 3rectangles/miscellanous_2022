"""
imports csv file whose each line contains tablename and corresponding columns.
make dict= {tablename: list of colmns of that table}

make connection to mysql db, and grant select access on every column of a TABLE1 which isnt present in dic[TABLE1]

make a file_name_timestamp.txt with timestamp for unique naming_convention, containing grant query for all tables for a particular user

"""


import csv
# import the mysql client for python

import requests
with open('./disk_image_list.csv', mode='r') as f:
    reader = csv.reader(f)
    image_list = []
    for row in reader:
        image_list.append(row)

print(len(image_list), len(image_list[0]))
#made dictionary, with key : table name. pair value being list of columns
tables = {}
for table in image_list:
    tables[table[0]]=table[1:]


#print(tables)

import pymysql
# Create a connection object
host = "10.180.0.3"
dbUser = "bolt_dev"
dbPassword = "bolT123"
dbName = "rebase_qa05"
charSet = "utf8mb4"

table_name = "user_sms"

cusrorType = pymysql.cursors.DictCursor
connectionObject = pymysql.connect(host=host, user=dbUser, password=dbPassword, db=dbName, port=3306)
# Create a cursor object
cursorObject   = connectionObject.cursor()

# SQL query string
# sqlQuery = "SELECT column_name FROM information_schema.columns WHERE table_name='user_sms';"
# Execute the sqlQuery

# keysList = list(mydict.keys())
keysList =["user_sms", "user_log", "user_rating"]
columns={}
for table_name in keysList:
    sqlQuery = "DESCRIBE " + table_name + ";"
    cursorObject.execute(sqlQuery)

    #Fetch all the rows
    rows = cursorObject.fetchall()
    #print(type(rows), rows)
    list2 = []
    for row in rows:
        (list2.append(row[0]))
    columns[table_name] = list2

print(len(columns), "\n", columns)


"""
columns = {'user_sms': ['id', 'user_id', 'user_data_md5', 'user_data_id', 'processing_version', 'sms_pattern_md5', 'duplicate_udid', 'sms_date', 'sender', 'bank_name', 'merchant_name', 'txn_medium', 'txn_purpose', 'txn_type', 'amount', 'account_number', 'balance', 'exact_pattern_match', 'red_flag', 'created_at', 'updated_at']
, 'user_log': ['id', 'user_id', 'type', 'data', 'phone_number', 'logged_on', 'created_at', 'updated_at', 'contact_back_on', 'support_message_id', 'channel', 'extra', 'category']
, 'user_rating': ['id', 'date', 'rating', 'created_at', 'updated_at', 'cumulative_rating', 'users']}

"""

# tables : a dic from csv file
# columns : a dic taken from a database, having same key values paired with list of columns of its repective table

# keysList = list(mydict.keys())
permission={}
keysList =["user_sms", "user_log", "user_rating"]
for table_name in keysList:
    permission[table_name] = []
    for x in columns[table_name]:
        if x not in tables[table_name]:
            permission[table_name].append(x)

"""
GRANT SELECT (col1), INSERT (col1, col2), UPDATE (col2)
ON mydb.mytable
TO john@localhost;
"""

""""
list1 = ["a", "b", "c"]
a=",".join(list1)
b ="GRANT SELECT (" + a + ") ON mydb.mytable TO john@localhost;"
print(b)
output: GRANT SELECT (a,b,c) ON mydb.mytable TO john@localhost;
"""
grants = []
username = "john"
for key, value_list in permission.items():
    query = ",".join(value_list)
    sql ="GRANT SELECT (" + query + ") ON mydb.mytable TO "+ username + "@localhost;"
    grants.append(sql)


# create txt file with all grants sql query




from datetime import datetime

date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
filename = "grants_"+date+".txt"
f = open(filename, "x")


# open file
with open(filename, 'w+') as f:
    # write elements of list
    for items in grants:
        f.write('%s\n' % items)

    print("File written successfully")

# close the file
f.close()


