import asyncio
import queue
import time
import functools
import logging
import threading
import sqlite3

import sqlparse

log = logging.getLogger(__name__)


def is_mutation_query(sql):
    statements = sqlparse.parse(sql)
    if len(statements) > 1:
        raise ValueError("multiple statements")

    statement = statements[0]
    if not isinstance(statement, sqlparse.sql.Statement):
        raise ValueError("not a statement")

    return statement.get_type() in ("CREATE", "INSERT", "UPDATE", "REPLACE", "DELETE")


class DatabaseWorker(threading.Thread):

    def __init__(self, queue, *args, **kwargs):
        super(DatabaseWorker, self).__init__(*args, **kwargs)
        self.queue = queue
        self.cursors = {}

    def run(self):
        log.info("%s starting up", self.name)

        while True:
            item = self.queue.get()
            if not item:
                break

            fn, future = item

            t1 = time.time()
            try:
                result = fn()
                future.get_loop().call_soon_threadsafe(future.set_result, result)
            except Exception as e:
                log.exception("error running %s", fn)
                future.set_exception(e)
            t2 = time.time()
            log.debug("ran %s in %.04fms", fn, (t2-t1) * 1000)

        log.info("%s shutdown", self.name)


class Cursor:
    BATCH_SIZE = 1000

    def __init__(self, dbm, cursor, arraysize=BATCH_SIZE):
        self.dbm = dbm
        self.cursor = cursor
        self.cursor.arraysize = arraysize

    async def execute(self, sql, params=None):
        if not params:
            params = tuple()
        return await self.dbm.dispatch(self.cursor.execute, sql, params)

    async def fetchall(self):
        return await self.dbm.dispatch(self.cursor.fetchall)

    async def fetchone(self):
        return await self.dbm.dispatch(self.cursor.fetchone)

    async def fetchmany(self, arraysize=None):
        return await self.dbm.dispatch(self.cursor.fetchmany, arraysize)

    async def close(self):
        return await self.dbm.dispatch(self.cursor.close)


class DatabaseManager:
    def __init__(self, name):
        self.name = name
        self.queue = queue.Queue()
        self._con = None
        self.worker_thread = DatabaseWorker(
            self.queue,
            name=f"db-worker-thread({name})",
        )
        self.worker_thread.start()

    async def get_con(self):
        if not self._con:
            self._con = await self.dispatch(sqlite3.connect, self.name)
        return self._con

    async def dispatch(self, func, *args, **kwargs):
        partial_func = functools.partial(func, *args, **kwargs)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.queue.put_nowait((partial_func, future))
        return await future

    async def cursor(self):
        con = await self.get_con()
        cursor = await self.dispatch(con.cursor)
        return Cursor(self, cursor)

    async def shutdown(self):
        if self._con:
            await self.dispatch(self._con.close)
            self._con = None
        self.queue.put_nowait(None)
        self.worker_thread.join()
