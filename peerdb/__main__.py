import argparse
import logging
import asyncio
import socket
import sys

import uvloop

from peerdb import server, httpd

log = logging.getLogger("peerdb")


def exc_handler(loop, context):
    log.error("unhandled exception: %s", context)


def resolve_host():
    hostname = socket.gethostname()
    addr = socket.gethostbyname(hostname)
    return hostname, addr


async def main(args):

    client_addr, client_port = "0.0.0.0", 8888
    peer_addr, peer_port = "0.0.0.0", 8889
    http_addr, http_port = "0.0.0.0", 8080

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exc_handler)
    hostname, addr = await loop.run_in_executor(None, resolve_host)

    log.info("booting on %s(%s)", hostname, addr)

    coros = [
        server.client_main(client_addr, client_port),
        server.peer_main(peer_addr, peer_port),
        httpd.main(http_addr, http_port),
    ]
    if args.peer:
        connect_addr, connect_port = args.peer.split(":")
        coro = server.connect_to_peer(connect_addr, connect_port, hostname, addr, peer_port)
        coros.append(coro)

    await asyncio.gather(*coros)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s.%(msecs)03d] %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="peerdb")
    parser.add_argument("--verbose", help="verbose mode", action="store_true")
    parser.add_argument("--peer", help="peer to connect to")
    args = parser.parse_args(sys.argv[1:])

    uvloop.install()

    try:
        asyncio.run(main(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("shutting down...")
