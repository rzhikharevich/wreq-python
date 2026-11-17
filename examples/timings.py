"""
Request timings example.

Demonstrates how to read the timings of a request from its response.
"""

import asyncio

from wreq import Client


async def get(client: Client, url: str):
    async with client.get(url) as response:
        await response.bytes()
        timings = response.timings
        print("  connection wait: ", timings.connection_wait)
        print("  connection setup:", timings.connection_setup)
        print("  time to headers: ", timings.time_to_headers)


async def main():
    async with Client() as client:
        print("New connection:")
        await get(client, "https://httpbin.io/get")
        # The connection is ready, so the request does not wait for it.
        print("Reused connection:")
        await get(client, "https://httpbin.io/get")


if __name__ == "__main__":
    asyncio.run(main())
