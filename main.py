from fastapi import FastAPI

from dao import get_rows_shops, get_rows_shop_stats

app = FastAPI()


@app.get("/get_shops")
async def get_shops(limit: int = 50, page: int = 1, desc: bool = False, only_x5: bool = False):
    return get_rows_shops(limit, page * limit - limit, desc, only_x5)


@app.get("/get_shop_stats")
async def get_x5_shop_stats(shop_id: int):
    return get_rows_shop_stats(shop_id)
