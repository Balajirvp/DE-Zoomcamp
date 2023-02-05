import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name[0]
    table_name1 = params.table_name[1]
    url = params.url[0]
    url1 = params.url[1]
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(url, iterator = True, chunksize = 100000)
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name = table_name, con = engine, if_exists='replace')
    df.to_sql(name = table_name, con = engine, if_exists='append')

    for df in df_iter:
        t_start = time()
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name = table_name, con = engine, if_exists='append')
        
        t_end = time()
        
        print(f'Inserted a new chunk. Time taken (in s) - {round(t_end - t_start, 2)}')

    taxi_zone = pd.read_csv(url1)

    taxi_zone.to_sql(name = table_name1, con = engine, if_exists='replace')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = "Ingest CSV data to Postgres")

    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the csv

    parser.add_argument('--user', help="user name for postgres")
    parser.add_argument('--password', help="password for postgres")
    parser.add_argument('--host', help="host for postgres")
    parser.add_argument('--port', help="port for postgres")
    parser.add_argument('--db', help="database name for postgres")
    parser.add_argument('--table_name', nargs = 2, help="name of the table where we will write the results to")
    parser.add_argument('--url', nargs = 2, help="url of the CSV")

    args = parser.parse_args()

    main(args)