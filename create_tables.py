import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Function that creates the database and connects to the SparkifyDB

def create_database():

    # Connect to the default Database
    conn = psycopg2.connect("host=127.0.0.1 dbname=data_eng user=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Connect to the Sparkify database using UTF8encoding
    cur.execute('DROP DATABASE IF EXISTS sparkify')
    cur.execute("CREATE DATABASE sparkify WITH ENCODING 'utf8' TEMPLATE template0")

    # Close connection to the Database
    conn.close()

    # Connect to the Sparkify Database
    conn = psycopg2.connect("host=127.0.0.1  dbname=data_eng user=postgres")
    cur = conn.cursor()

    return cur, conn

# Dropping tables using the queries in 'create_table_queries' list.

def drop_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



         