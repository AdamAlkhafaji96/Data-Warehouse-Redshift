import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load data from S3 into staging tables within the data warehouse.
    Args:
        cur : cursor for current sql connection
        conn : a psycopg2 db connection
    '''        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print()
        print('{} ---> Copy table query successful'.format(query))
        print()


def insert_tables(cur, conn):
    ''' 
    Execute insert table SQL commands against the staging tables
    for insertion into fact and dimensional tables.
    Args:
        cur : cursor for current sql connection
        conn : a psycopg2 db connection
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print()
        print('{} ---> Insert table query Successful'.format(query))
        print()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print()
    print(" -> Loading staging tables.")
    load_staging_tables(cur, conn)
    
    print()
    print(" -> Inserting data.")
    insert_tables(cur, conn)
    
    print()
    print(" -> Closing connection.")
    conn.close()


if __name__ == "__main__":
    main()