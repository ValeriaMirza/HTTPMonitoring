import asyncio
import aiohttp
import time
import random

LOGIN_URL = "http://127.0.0.1:8000/api/login/"
PRODUCT_URL = "http://127.0.0.1:8000/api/products/"

NUM_REQUESTS = 200
CONCURRENCY = 20

LOGIN_PAYLOAD = {
    "username": "m",
    "password": "1"
}

PRODUCT_PAYLOAD = {
    "name": "Test Product 1",
    "sku": "TP001ds",
    "price": "99.99",
    "description": "This is a test proddfduct."
}
def generate_product_payload(i):
    return {
        "name": f"Test Product {i}",
        "sku": f"m{i:05d}",  # ex: TP00001, TP00002, etc.
        "price": f"{random.uniform(10.0, 100.0):.2f}",
        "description": f"This is test product {i}."
    }


async def get_access_token(session):
    async with session.post(LOGIN_URL, json=LOGIN_PAYLOAD) as response:
        data = await response.json()
        if response.status == 200 and "access" in data:
            print("Access token obtained.")
            return data["access"]
        else:
            raise Exception(f"Login failed: {data}")


async def send_request(session, i, token, payload):
    try:
        await asyncio.sleep(random.uniform(0.2, 1.5))
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        async with session.post(PRODUCT_URL, json=payload, headers=headers) as response:
            text = await response.text()
            print(f"Request {i} - Status: {response.status}, Response: {text[:100]}")
    except Exception as e:
        print(f"Request {i} - Failed: {str(e)}")


async def main():
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        token = await get_access_token(session)

        tasks = [
    send_request(session, i, token, generate_product_payload(i))
    for i in range(NUM_REQUESTS)
]


        start = time.time()
        await asyncio.gather(*tasks)
        end = time.time()

        print(f"\nAll done in {end - start:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
