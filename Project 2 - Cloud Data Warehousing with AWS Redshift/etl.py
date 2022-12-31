import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Copy data from S3 bucket into staging tables in Redshift
    """
    print("Loading data into staging tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Staging tables were loaded")


def insert_tables(cur, conn):
    """
    - Insert data from staging tables into dimension and fact tables in Redshift
    """
    print("Insert data from staging tables into dimension and fact tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Dimension and fact tables were loaded")


def main():
    """
    - Connect to Redshift
    - Copy data from S3 bucket into staging tables in Redshift
    - Insert data from staging tables into dimension and fact tables in Redshift
    - Close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()