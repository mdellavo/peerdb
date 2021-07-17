import asyncio
import logging
import dataclasses
import time
from typing import Tuple

from peerdb.db import DatabaseManager, is_mutation_query
from peerdb.net import Connection
from peerdb.proto import PeerMessageTypes, ClientMessageTypes, ResponseTypes
from peerdb.raft import LogManager, LogWatcher

log = logging.getLogger("peerdb")

PEERS = []
CLIENTS = []

@dataclasses.dataclass
class Peer:
    address: Tuple[str, int]
    hostname: str
    advertized: Tuple[str, int]


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
        await self.con.send((ResponseTypes.OK,))
        await self.con.close()

    async def handle_ping(self, msg):
        ts = msg[1]
        await self.con.send((ResponseTypes.OK, ts))

    async def handle_cursor(self, msg):
        cursor = await self.dbm.cursor()
        self.cursors.append(cursor)
        cursor_id = len(self.cursors) - 1
        await self.con.send((ResponseTypes.OK, cursor_id))

    async def handle_close(self, msg):
        cursor_id = msg[1]
        cursor = self.cursors[cursor_id]
        await cursor.close()
        await self.con.send((ResponseTypes.OK,))

    async def handle_execute(self, msg):
        cursor_id, sql, params = msg[1:]
        cursor = self.cursors[cursor_id]
        await cursor.execute(sql, params)
        await self.con.send((ResponseTypes.OK,))

    async def handle_executemany(self, msg):
        cursor_id, sql, params = msg[1:]
        cursor = self.cursors[cursor_id]
        await cursor.executemany(sql, params)
        await self.con.send((ResponseTypes.OK,))

    async def handle_fetchmany(self, msg):
        cursor_id, size = msg[1:]
        cursor = self.cursors[cursor_id]
        result = await cursor.fetchmany(size)
        await self.con.send((ResponseTypes.OK, result))

    HANDLERS = {
        ClientMessageTypes.GOODBYE: handle_goodbye,
        ClientMessageTypes.PING: handle_ping,
        ClientMessageTypes.CURSOR: handle_cursor,
        ClientMessageTypes.CLOSE: handle_close,
        ClientMessageTypes.EXECUTE: handle_execute,
        ClientMessageTypes.EXECUTEMANY: handle_executemany,
        ClientMessageTypes.FETCHMANY: handle_fetchmany,
    }

    async def dispatch(self, msg):
        # log.debug("dispatch client msg: %s", msg)

        if not isinstance(msg, (list, tuple)):
            await self.con.send((ResponseTypes.ERR, "invalid client message"))
            raise ClientError()

        if msg[0] not in self.HANDLERS:
            await self.con.send((ResponseTypes.ERR, "unknown message type"))
            raise ClientError()

        handler = self.HANDLERS[msg[0]]
        await handler(self, msg)


async def handle_peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

    con = Connection(reader, writer)

    peer = None
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

        hostname, advertized = msg[1:]
        peer = Peer(con.peer, hostname, advertized)
        PEERS.append(peer)

        log.info("peer connected %s", peer)

        while not con.closed:
            await asyncio.sleep(1)
            await con.send((PeerMessageTypes.HEARTBEAT, time.time()))
            resp = await con.recv()
            if resp[0] != ResponseTypes.OK:
                raise ClientError("peer did not ack heartbeat")

            t2 = time.time()
            t1 = resp[1]
            delta = (t2 - t1) * 1000
            log.info("peer %s latency: %.04fms", peer.address[0], delta)

    except PeerError as e:
        log.error("peer error: %s", e)

    await con.close()

    if peer:
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
            await con.send((ResponseTypes.ERR, "invalid hello"))
            raise ClientError()

        db_name = msg[1]
        log.info("client opening db %s", db_name)
        dbm = DatabaseManager(db_name)
        session = ClientSession(con, dbm)

        msg = (ResponseTypes.OK,)
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

    CLIENTS.remove(client)
    await con.close()

    log.info("client disconnected from %s:%s", *client.address)


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


async def connect_to_peer(connect_addr, connect_port, hostname, addr, peer_port):

    log.info("connecting to peer %s:%s", connect_addr, connect_port)

    reader, writer = await asyncio.open_connection(connect_addr, connect_port)
    con = Connection(reader, writer)

    await con.send((PeerMessageTypes.HELLO, hostname, (addr, peer_port)))

    while not con.closed:
        msg = await con.recv()
        if not msg:
            break
        log.debug("got message from peer: %s", msg)
        resp = (ResponseTypes.OK, msg[1])
        await con.send(resp)
