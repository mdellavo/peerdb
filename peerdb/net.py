import asyncio
import logging

import msgpack

from peerdb.utils import Timer

log = logging.getLogger(__name__)

BUF_LEN = 4096


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
        with Timer() as t:
            rv = self.writer.write(payload)
            await self.writer.drain()
        log.debug("sent %d bytes in %.04fms", len(payload), t.elapsed)
        return rv
