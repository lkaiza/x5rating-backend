from fastapi import FastAPI

from dao import get_rows_shops, get_rows_x5_shops, get_rows_x5_shop_stats

app = FastAPI()


@app.get("/get_shops")
async def get_shops(limit: int = 50, page: int = 1):
    return get_rows_shops(limit, page * limit - limit)


@app.get("/get_x5_shops")
async def get_x5_shops(limit: int = 50, page: int = 1):
    return get_rows_x5_shops(limit, page * limit - limit)


@app.get("/get_x5_shop_stats")
async def get_x5_shop_stats(shop_id: int):
    return get_rows_x5_shop_stats(shop_id)
