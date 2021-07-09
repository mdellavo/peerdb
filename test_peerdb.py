import os

import pytest

from peerdb import server, client
from peerdb.db import DatabaseManager

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def dbm():
    name = "test.db"

    dbm = DatabaseManager(name)
    yield dbm
    await dbm.shutdown()

    os.remove(name)


@pytest.fixture
async def client_server():
    srvr = await server.client_server("127.0.0.1", 9876)
    yield srvr
    srvr.close()
    await srvr.wait_closed()


async def test_client_server(client_server):
    con = await client.connect("peerdb://localhost:9876")

    cur = await con.cursor()
    await cur.execute("CREATE TABLE foo(id INTEGER PRIMARY KEY, name VARCHAR)")
    await cur.execute("INSERT INTO foo VALUES (1, \"foo\")")
    await cur.execute("SELECT * FROM foo")

    result = await cur.fetchmany()
    assert result == [[1, "foo"]]

    await cur.close()

    await con.close()


async def test_database_manager(dbm):

    cursor = await dbm.cursor()
    await cursor.execute("CREATE TABLE foo(id INTEGER PRIMARY KEY, name VARCHAR)")

    values = [(1, "foo"), (2, "bar"), (3, "baz")]
    for i, name in values:
        await cursor.execute("INSERT INTO foo(id, name) VALUES (?, ?)", (i, name))

    await cursor.execute("SELECT * FROM foo WHERE id > ?", (0,))
    assert await cursor.fetchall() == values
