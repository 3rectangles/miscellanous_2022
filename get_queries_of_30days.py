import psycopg2


def get_objectid():
    # Connect to the database
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="password")

    # Create a cursor to execute queries
    cur = conn.cursor()

    # Select the unique object_id values from the events table where the created_at column is from the past 30 days
    query = "SELECT DISTINCT object_id FROM events WHERE action='execute' AND created_at >= NOW() - INTERVAL '30' DAY ;"
    cur.execute(query)

    # Remove None values from the query results and store the remaining values in a list
    results = cur.fetchall()
    object_id_list = []
    for row in results:
        if row[0] is not None:
            object_id_list.append(row[0])

    # Print the list of object_id values
    print(object_id_list)

    # Close the cursor and connection
    cur.close()
    conn.close()
    return object_id_list
def get_queries(object_id):
    queries = []
    failed_queries = []
    for id in object_id:
        # Connect to the database
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="password")

        # Create a cursor to execute queries
        cur = conn.cursor()

        # Try to execute the SQL query
        try:
            # Select the query from the queries table where the id column is equal to the object_id value
            query = f"SELECT query FROM queries WHERE id = {id} ;"
            cur.execute(query)

            # Add the query results to the queries list
            results = cur.fetchall()
            for row in results:
                queries.append(row[0])

        # Handle any exceptions that may be raised when executing the SQL query
        except Exception as e:
            # Add the failed query to the failed_queries list
            failed_queries.append(query)

    # Close the cursor and connection
    cur.close()
    conn.close()

    return queries, failed_queries




def main():
    object_id = get_objectid()
    #pass the following commented list to check for wrong queries
    # object_id=  ['15588', '15597', '15641', '15622', '15594', '15633', '15627', '15631','44','qew', None]

    queries, failed_queries= get_queries(object_id)
    print("xxxxxxxxxxxxxxxxxxxxxxxxxxxx failed queries xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    for i in failed_queries:
        print(i)
    print("\n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\n")
    for i in queries:
        print(i)
        print(";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;")


if __name__ == "__main__":
    main()

