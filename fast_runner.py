import uvicorn

if __name__ == "__main__":
    uvicorn.run('main:app', host="127.0.0.1", port=8001, workers=5)
# https://docs.sqlalchemy.org/en/20/_modules/examples/sharding/asyncio.html
# https://github.com/seapagan/fastapi_async_sqlalchemy2_example/blob/main/db.py
# https://habr.com/ru/companies/amvera/articles/845104/
