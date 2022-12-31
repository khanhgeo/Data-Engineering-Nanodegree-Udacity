import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData

def create_database():
    """
    - Creates and connects to the capstoneprojectdb
    - Returns the connection and cursor to capstoneprojectdb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS capstoneprojectdb")
    cur.execute("CREATE DATABASE capstoneprojectdb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=capstoneprojectdb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_ERdiagram():
    """
    Creates entity relationship diagram 
    """
    graph = create_schema_graph(metadata=MetaData('postgresql://student:student@127.0.0.1/capstoneprojectdb'))
    graph.write_png('capstoneprojectdb_erd.png')


def main():
    """
    - Drops (if exists) and Creates the capstoneproject database. 
    
    - Establishes connection with the capstoneproject database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    create_ERdiagram()

    conn.close()


if __name__ == "__main__":
    main()