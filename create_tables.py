import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    ''' 
    Execute drop table SQL commands for all data warehouse
    tables.
    Args:
        cur : cursor for current sql connection
        conn : a psycopg2 db connection
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print()
        print('{} ---> Drop table query successful'.format(query))
        print()


def create_tables(cur, conn):
    ''' 
    Execute create table SQL commands for all data warehouse
    tables.
    Args:
        cur : cursor for current sql connection
        conn : a psycopg2 db connection
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print()
        print('{} ---> Create table query successful'.format(query))
        print()


def main():
    '''
    Connects to configured Redshift database and runs SQL queries for drop
    table operations followed by create table operations.
    The data warehouse schema is set after sucessful execution.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print(" -> Connecting to Redshift database.")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(' -> Connection established.')

    print()
    print(" -> Dropping tables.")
    drop_tables(cur, conn)
    
    print()
    print(" -> Creating tables.")
    create_tables(cur, conn)
    
    print()
    print(" -> Closing connection.")
    print()
    conn.close()


if __name__ == "__main__":
    main()