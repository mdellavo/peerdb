import json
import asyncio
import logging

from aiohttp import web


from . import server

log = logging.getLogger("peerdb")

def _project_peer(peer: server.Peer):
    return {
        "address": peer.address,
    }


def _project_client(client: server.Client):
    return {
        "address": client.address,
    }


async def handler(request):
    return web.Response(text=json.dumps({
        "clients": [_project_client(client) for client in server.CLIENTS],
        "peers": [_project_peer(peer) for peer in server.PEERS],
    }))


async def main(addr, port):
    server = web.Server(handler)
    runner = web.ServerRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, addr, port)
    await site.start()
    log.info("servering http api on http://%s:%s", addr, port)
    while True:
        await asyncio.sleep(100 * 3600)
