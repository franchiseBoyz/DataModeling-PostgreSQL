import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Function that creates the database and connects to the SparkifyDB

def create_database():

    # Connect to the default Database
    conn = psycopg2.connect("host=127.0.0.1 dbname=data_eng user=franchise password='420GLOCKzone.'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Connect to the Sparkify database using UTF8encoding
    cur.execute('DROP DATABASE IF EXISTS sparkify')
    cur.execute("CREATE DATABASE sparkify WITH ENCODING 'utf8' TEMPLATE template0")

    # Close connection to the Database
    conn.close()

    # Connect to the Sparkify Database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkify user=franchise password='420GLOCKzone.'")
    cur = conn.cursor()

    return cur, conn

# Dropping tables using the queries in 'drop_table_queries' list.

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Creating tables using the queries in 'create_table_queries' list.

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():

    """"
    - Drop tables if they exist and creates the sparkify database.
    - Establish connection with the Sparkify Database and gets cursor to it.
    - Drops all the tables
    - Creates all the tables
    - Closes the connection
    """

    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == '__main__':
    main()


         