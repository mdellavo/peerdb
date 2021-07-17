import asyncio
import logging
import urllib.parse
import time

from peerdb.net import Connection
from peerdb.proto import ClientMessageTypes, ResponseTypes

log = logging.getLogger("peerdb")


class ClientError(Exception):
    pass


class Cursor:

    BATCH_SIZE = 1000

    def __init__(self, connection, cursor_id):
        self.connection = connection
        self.cursor_id = cursor_id

    async def close(self):
        msg = (ClientMessageTypes.CLOSE, self.cursor_id)
        await self.connection.con.send(msg)
        resp = await self.connection.con.recv()
        if resp[0] != ResponseTypes.OK:
            raise ClientError(resp[1])

    async def execute(self, sql, parameters=None):
        msg = (ClientMessageTypes.EXECUTE, self.cursor_id, sql, parameters)
        await self.connection.con.send(msg)
        resp = await self.connection.con.recv()
        if resp[0] != ResponseTypes.OK:
            raise ClientError(resp[1])

    async def executemany(self, sql, parameters):
        msg = (ClientMessageTypes.EXECUTEMANY, self.cursor_id, sql, list(parameters))
        await self.connection.con.send(msg)
        resp = await self.connection.con.recv()
        if resp[0] != ResponseTypes.OK:
            raise ClientError(resp[1])

    async def fetchmany(self, size=BATCH_SIZE):
        if self.cursor_id is None:
            raise ClientError("need to execute first")

        msg = (ClientMessageTypes.FETCHMANY, self.cursor_id, size)
        await self.connection.con.send(msg)
        resp = await self.connection.con.recv()
        if resp[0] != ResponseTypes.OK:
            raise ClientError(resp[1])
        return resp[1]


class ClientConnection:
    def __init__(self, dsn: urllib.parse.ParseResult, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.dsn = dsn
        self.con = Connection(reader, writer)

    async def close(self):
        msg = (ClientMessageTypes.GOODBYE,)
        await self.con.send(msg)
        resp = await self.con.recv()
        await self.con.close()

    async def open_db(self, name):
        msg = (ClientMessageTypes.HELLO, name)
        await self.con.send(msg)
        resp = await self.con.recv()
        if resp[0] != ResponseTypes.OK:
            raise ClientError(resp[1])

    async def ping(self):
        msg = (ClientMessageTypes.PING, time.time())
        await self.con.send(msg)
        resp = await self.con.recv()
        if resp[0] != ResponseTypes.OK:
            raise ClientError(resp[1])
        ts = resp[1]
        delta = time.time() - ts
        log.info("ping took %.04fms", delta * 1000)

    async def cursor(self, factory=Cursor):
        msg = (ClientMessageTypes.CURSOR,)
        await self.con.send(msg)
        resp = await self.con.recv()
        if resp[0] != ResponseTypes.OK:
            raise ClientError(resp[1])
        cursor_id = resp[1]
        return factory(self, cursor_id)


async def connect(dsn):
    parsed_dsn = urllib.parse.urlparse(dsn)
    if parsed_dsn.scheme != "peerdb":
        raise ValueError("Unsupported DSN: " + dsn)

    log.info("connecting to %s...", parsed_dsn.netloc)

    parts = parsed_dsn.netloc.split(":", 1)
    if len(parts) != 2:
        raise ValueError("Missing host:port")

    host, port = parts

    reader, writer = await asyncio.open_connection(host, port)
    con = ClientConnection(parsed_dsn, reader, writer)

    db_name = parsed_dsn.path[1:] if parsed_dsn.path else ":memory:"

    log.info("opening db %s", db_name)
    await con.open_db(db_name)

    return con
