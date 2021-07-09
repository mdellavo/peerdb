import asyncio
import logging

from peerdb import server, client

log = logging.getLogger("peerdb-test")


async def main():
    con = await client.connect("peerdb://localhost:8888")
    cur = con.cursor()
    cur.execute("SELECT * FROM foos")
    await con.close()


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s.%(msecs)03d] %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    asyncio.run(main())
