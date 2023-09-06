from fastapi import FastAPI

from dao import get_rows_shops

app = FastAPI()


@app.get("/get_10_first_shops")
async def get_10_first_shops():
    return get_rows_shops(10, 0)
