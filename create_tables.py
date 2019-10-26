import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Iterate through and exectue DROP TABLE queries

    Parameters:
        cur: cursor object
            cursor to perform data base operation
        conn: connection object
            connection to database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('DELETE DONE.')


def create_tables(cur, conn):
    """Iterate through and exectue CREATE TABLE queries

    Parameters:
        cur: cursor object
            cursor to perform data base operation
        conn: connection object
            connection to database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print('CREATE DONE.')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
