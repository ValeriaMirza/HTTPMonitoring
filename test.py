import asyncio
import aiohttp
import time
import random

URL = "http://localhost:8000/api/login/"
NUM_REQUESTS = 200
CONCURRENCY = 20

payload = {
    "username": "m",
    "password": "1"
}


async def send_request(session, i):
    try:

        await asyncio.sleep(random.uniform(0.2, 1.5))

        async with session.post(URL, json=payload) as response:
            text = await response.text()
            print(f"Request {i} - Status: {response.status}, Response: {text[:50]}")
    except Exception as e:
        print(f"Request {i} - Failed: {str(e)}")


async def main():
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [send_request(session, i) for i in range(NUM_REQUESTS)]

        start = time.time()
        await asyncio.gather(*tasks)
        end = time.time()

        print(f"\nAll done in {end - start:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
