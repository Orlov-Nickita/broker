import asyncio
import aiohttp


async def make_api_request(session, semaphore):
    url = "http://127.0.0.1:8001/task"
    async with semaphore:
        async with session.get(url) as response:
        # async with session.post(url, params={'task_data': '123333'}) as response:
            if response.status == 200:
                data = await response.json()
                print(f"Запрос к {url} успешен. Полученные данные: {data}")
            else:
                print(response.text)
                print(f"Ошибка запроса к {url}: {response.status}")


async def main():
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(2)
        tasks = []
        for _ in range(50):
            task = asyncio.ensure_future(make_api_request(session, semaphore))
            tasks.append(task)
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
