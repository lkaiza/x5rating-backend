import os
import time

import clickhouse_connect
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST')
CLICKHOUSE_USERNAME = os.environ.get('CLICKHOUSE_USERNAME')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')

client = None
while client is None:
    try:
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, username=CLICKHOUSE_USERNAME,
                                               password=CLICKHOUSE_PASSWORD)
    except:
        time.sleep(3)
        print("Trying to reconnect...")


def get_rows_shops(limit, offset):
    result = client.query(f'SELECT * FROM ymaps_shops LIMIT {limit} OFFSET {offset}')

    return result.result_rows
