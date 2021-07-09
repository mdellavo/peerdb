import asyncio
from enum import IntEnum, auto

import msgpack

BUF_LEN = 4096

class PeerMessageTypes(IntEnum):
    HELLO = auto()
    HEARTBEAT = auto()
    APPEND_LOG = auto()


class ClientMessageTypes(IntEnum):
    HELLO = auto()
    GOODBYE = auto()
    CURSOR = auto()
    CLOSE = auto()
    EXECUTE = auto()
    FETCHMANY = auto()


class ClientResponseTypes(IntEnum):
    OK = 1
    ERR = -1


class Connection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.unpacker = msgpack.Unpacker()
        self.closed = False

    @property
    def peer(self):
        return self.writer.get_extra_info("peername")

    async def close(self):
        if not self.closed:
            self.reader.feed_eof()
            self.writer.write_eof()
            self.writer.close()
            self.closed = True
            await self.writer.wait_closed()

    async def recv(self):

        msg = None
        while not self.closed:
            data = await self.reader.read(BUF_LEN)
            if not data:
                break
            self.unpacker.feed(data)
            try:
                msg = next(self.unpacker)
                break
            except StopIteration:
                continue

        return msg

    async def send(self, obj):
        payload = msgpack.packb(obj)
        rv = self.writer.write(payload)
        await self.writer.drain()
        return rv
