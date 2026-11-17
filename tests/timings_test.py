import asyncio
import datetime
import itertools
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import wreq
from wreq import Proxy, blocking

ZERO = datetime.timedelta()


@pytest.fixture
def http_server():
    class Handler(BaseHTTPRequestHandler):
        # Keep connections open so that a second request can reuse one.
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class SocksProxy:
    """A SOCKS4 proxy that answers the HTTP requests itself."""

    # No server listens at this address. The proxy answers the requests and never connects to it.
    SERVER = "http://192.0.2.1"

    def __init__(self):
        # Connection `n` waits for `handshakes[n]` before it completes the SOCKS handshake.
        self.handshakes = defaultdict(asyncio.Event)
        self.connections = itertools.count(1)
        self.events = asyncio.Queue()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.serve, "127.0.0.1", 0)
        self.url = f"socks4://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        return self

    async def __aexit__(self, *_):
        self.server.close()

    async def serve(self, reader, writer):
        connection = next(self.connections)
        try:
            await reader.readexactly(8)
            await reader.readuntil(b"\0")
            self.events.put_nowait(("connection", connection))
            await self.handshakes[connection].wait()
            writer.write(b"\0\x5a" + bytes(6))
            while await reader.readuntil(b"\r\n\r\n"):
                self.events.put_nowait(("request", connection))
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        except (asyncio.IncompleteReadError, OSError):
            pass
        finally:
            writer.close()

    async def event(self):
        return await asyncio.wait_for(self.events.get(), 5)


async def get(client, url):
    async with client.get(url) as response:
        await response.bytes()
        return response.timings


def assert_waited_for_own_connection(timings):
    """Assert that the request started its own connection and waited for the whole setup."""
    assert ZERO < timings.connection_setup
    assert timings.connection_setup <= timings.connection_wait
    assert timings.connection_wait <= timings.time_to_headers


@pytest.mark.asyncio
async def test_timings_of_new_and_reused_connection(http_server):
    async with wreq.Client(no_proxy=True) as client:
        new = await get(client, http_server)
        reused = await get(client, http_server)

    assert_waited_for_own_connection(new)
    # The connection was ready before the second request started.
    assert reused.connection_wait == ZERO
    assert reused.connection_setup == new.connection_setup
    assert ZERO < reused.time_to_headers


@pytest.mark.asyncio
async def test_timings_of_connection_that_another_request_opened():
    async with SocksProxy() as proxy:
        async with wreq.Client(proxies=[Proxy.all(proxy.url)]) as client:
            # Each request starts a connection, and the proxy holds it in the SOCKS handshake.
            requests = []
            for connection in (1, 2):
                requests.append(asyncio.create_task(get(client, proxy.SERVER)))
                assert await proxy.event() == ("connection", connection)

            # Only connection 1 completes its handshake, so it serves both requests in turn.
            proxy.handshakes[1].set()
            first, second = await asyncio.gather(*requests)
            assert [await proxy.event(), await proxy.event()] == [("request", 1)] * 2

    assert_waited_for_own_connection(first)
    # Connection 1 started before the second request did.
    assert second.connection_setup == first.connection_setup
    assert ZERO < second.connection_wait < second.connection_setup
    assert second.connection_wait <= second.time_to_headers


@pytest.mark.asyncio
async def test_timings_measure_the_connection_setup():
    async with SocksProxy() as proxy:
        async with wreq.Client(proxies=[Proxy.all(proxy.url)]) as client:
            request = asyncio.create_task(get(client, proxy.SERVER))
            assert await proxy.event() == ("connection", 1)
            await asyncio.sleep(0.15)
            proxy.handshakes[1].set()
            timings = await request

    setup = datetime.timedelta(milliseconds=100)
    assert setup <= timings.connection_setup < 50 * setup
    assert_waited_for_own_connection(timings)


def test_timings_blocking_client(http_server):
    with blocking.Client(no_proxy=True) as client:
        with client.get(http_server) as response:
            timings = response.timings

    assert_waited_for_own_connection(timings)


@pytest.mark.asyncio
async def test_timings_module_function(http_server):
    async with wreq.get(http_server) as response:
        timings = response.timings

    assert_waited_for_own_connection(timings)
