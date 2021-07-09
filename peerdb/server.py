import asyncio
import logging
import dataclasses
from typing import Tuple

from .db import DatabaseManager
from .net import Connection, PeerMessageTypes, ClientMessageTypes, ClientResponseTypes

log = logging.getLogger("peerdb")

PEERS = []
CLIENTS = []


@dataclasses.dataclass
class Peer:
    address: str


@dataclasses.dataclass
class Client:
    address: str


class ClientError(Exception):
    pass


class PeerError(Exception):
    pass


class ClientSession:

    def __init__(self, con, dbm):
        self.con = con
        self.dbm = dbm
        self.cursors = []

    async def close(self):
        log.info("closing client session")
        for cursor in self.cursors:
            await cursor.close()

    async def handle_goodbye(self, msg):
        await self.con.send((ClientResponseTypes.OK,))
        await self.con.close()

    async def handle_cursor(self, msg):
        cursor = await self.dbm.cursor()
        self.cursors.append(cursor)
        cursor_id = len(self.cursors) - 1
        await self.con.send((ClientResponseTypes.OK, cursor_id))

    async def handle_close(self, msg):
        cursor_id = msg[1]
        cursor = self.cursors[cursor_id]
        await cursor.close()
        await self.con.send((ClientResponseTypes.OK,))

    async def handle_execute(self, msg):
        cursor_id, sql, params = msg[1:]
        cursor = self.cursors[cursor_id]
        await cursor.execute(sql, params)
        await self.con.send((ClientResponseTypes.OK,))

    async def handle_fetchmany(self, msg):
        cursor_id, size = msg[1:]
        cursor = self.cursors[cursor_id]
        result = await cursor.fetchmany(size)
        await self.con.send((ClientResponseTypes.OK, result))

    HANDLERS = {
        ClientMessageTypes.GOODBYE: handle_goodbye,
        ClientMessageTypes.CURSOR: handle_cursor,
        ClientMessageTypes.CLOSE: handle_close,
        ClientMessageTypes.EXECUTE: handle_execute,
        ClientMessageTypes.FETCHMANY: handle_fetchmany,
    }

    async def dispatch(self, msg):
        log.debug("dispatch client msg: %s", msg)

        if not isinstance(msg, (list, tuple)):
            await self.con.send((ClientResponseTypes.ERR, "invalid client message"))
            raise ClientError()

        if msg[0] not in self.HANDLERS:
            await self.con.send((ClientResponseTypes.ERR, "unknown message type"))
            raise ClientError()

        handler = self.HANDLERS[msg[0]]
        await handler(self, msg)


async def handle_peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

    con = Connection(reader, writer)

    peer = Peer(con.peer)
    PEERS.append(peer)

    log.info("peer connected from %s:%s", *peer.address)

    try:
        msg = await con.recv()

        valid_hello = (
            isinstance(msg, (list, tuple)) and
            len(msg) > 1 and
            msg[0] == PeerMessageTypes.HELLO
        )
        if not valid_hello:
            await con.send((-1, "invalid hello"))
            raise ClientError()

        while not con.closed:
            await asyncio.sleep(1)
            await con.send((PeerMessageTypes.HEARTBEAT,))
    except PeerError as e:
        log.error("peer error: %s", e)

    await con.close()

    log.info("peer disconnected from %s:%s", *peer.address)
    PEERS.remove(peer)


# FIXME refactor handle_*
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    con = Connection(reader, writer)

    client = Client(con.peer)
    CLIENTS.append(client)

    log.info("client connected from %s:%s", *client.address)

    try:
        msg = await con.recv()

        valid_hello = (
            isinstance(msg, (list, tuple)) and
            len(msg) > 1 and
            msg[0] == ClientMessageTypes.HELLO
        )
        if not valid_hello:
            await con.send((ClientResponseTypes.ERR, "invalid hello"))
            raise ClientError()

        db_name = msg[1]
        log.info("client opening db %s", db_name)
        dbm = DatabaseManager(db_name)
        session = ClientSession(con, dbm)

        msg = (ClientResponseTypes.OK,)
        await con.send(msg)

        while not con.closed:
            msg = await con.recv()
            if not msg:
               break
            await session.dispatch(msg)

        await session.close()
        await dbm.shutdown()

    except ClientError as e:
        log.error("client error: %s", e)

    await con.close()

    log.info("client disconnected from %s:%s", *client.address)
    CLIENTS.remove(client)


async def start_server(name, callback, addr, port):
    server = await asyncio.start_server(callback, addr, port)
    addr = server.sockets[0].getsockname()
    log.info("serving %s on %s:%s", name, *addr)
    return server


async def client_server(addr, port):
    return await start_server("clients", handle_client, addr, port)


async def client_main(addr, port):
    server = await client_server(addr, port)
    async with server:
        await server.serve_forever()


async def peer_server(addr, port):
    return await start_server("peers", handle_peer, addr, port)


async def peer_main(addr, port):
    server = await peer_server(addr, port)
    async with server:
        await server.serve_forever()


async def connect_to_peer(addr, port):
    reader, writer = await asyncio.open_connection(addr, port)
    con = Connection(reader, writer)

    await con.send((PeerMessageTypes.HELLO, 0))

    while not con.closed:
        msg = await con.recv()
        if not msg:
            break
        print(msg)
