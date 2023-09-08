import os
import time

import clickhouse_connect
from dotenv import load_dotenv, find_dotenv

from utils import is_nan, shop_data_to_map

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


def get_rows_shops(limit, offset, desc, only_x5):
    desc_query = " DESC" if desc else ""
    only_x5_query = " WHERE title IN ('Пятёрочка', 'Перекресток')" if only_x5 else ""
    result = client.query(f"SELECT * FROM ymaps_shops{only_x5_query} "
                          f"ORDER BY rating['ratingValue']{desc_query} "
                          f"LIMIT {limit} OFFSET {offset}")
    out = []
    for r in result.result_rows:
        out.append(shop_data_to_map(r))

    return out


def get_rows_shop_stats(shop_id):
    result = client.query(f"SELECT * FROM rating_stats WHERE id = {shop_id} "
                          "ORDER BY time_month DESC")

    out = []
    for r in result.result_rows:
        out.append({
            "id": r[0],
            "date": r[1].strftime('%Y-%m-%d'),
            "currRating": None if is_nan(r[2]) else r[2],
            "currRatingCount": r[3],
            "previousRating": None if is_nan(r[4]) else r[4],
            "ratingDiff": None if is_nan(r[5]) else r[5],
            "isHit": r[6],
        })
    print(out)
    return out
