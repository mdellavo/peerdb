import logging
import asyncio

import uvloop

from . import server, httpd

log = logging.getLogger("peerdb")


async def main():
    client_addr, client_port = "0.0.0.0", 8888
    peer_addr, peer_port = "0.0.0.0", 8889
    http_addr, http_port = "0.0.0.0", 8080

    await asyncio.gather(
        server.client_main(client_addr, client_port),
        server.peer_main(peer_addr, peer_port),
        httpd.main(http_addr, http_port),
    )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s.%(msecs)03d] %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    uvloop.install()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("shutting down...")
