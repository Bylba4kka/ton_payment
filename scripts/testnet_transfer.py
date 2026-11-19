"""
Load test: send N transfers of 0.01 TON each.

Usage: API_TOKEN=... python scripts/testnet_transfer.py <destination>
"""

import argparse
import asyncio
import os
import time
from collections import Counter

import aiohttp

URL = "http://127.0.0.1:8080/api/v1/transfer"
TOTAL = 10
START_ID = 50


async def send_one(
    session: aiohttp.ClientSession,
    destination: str,
    i: int,
) -> tuple[int, int | str, str]:
    external_id = str(START_ID + i)
    payload = {
        "external_id": external_id,
        "currency": "TON",
        "amount": 0.01,
        "destination": destination,
        "comment": f"test my app {external_id}",
    }
    try:
        async with session.post(URL, json=payload) as resp:
            return i, resp.status, await resp.text()
    except aiohttp.ClientError as exc:
        return i, type(exc).__name__, str(exc)


async def main(token: str, destination: str) -> None:
    headers = {"Authorization": f"Bearer {token}", "accept": "*/*"}
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
    ) as session:
        started = time.perf_counter()
        results = await asyncio.gather(
            *(send_one(session, destination, i) for i in range(TOTAL)),
        )
        elapsed = time.perf_counter() - started

    for i, status, body in results:
        print(f"#{START_ID + i}: {status} {body[:200]}")  # noqa: T201

    print(f"\n{elapsed:.2f}s")  # noqa: T201
    print(dict(Counter(status for _, status, _ in results)))  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", help="TON address to send to")
    args = parser.parse_args()
    asyncio.run(main(os.getenv("API_TOKEN"), args.destination))
