import utils
import asyncpg


class Database:
    def __init__(self):
        self._pool = None
        self._conn = None

    async def connect(self, callback):
        self._pool = await asyncpg.create_pool(**utils.conn_kwargs)

        # initiate logging connection for discord callback
        self._conn = await self._pool.acquire()
        await self._conn.add_listener('log', callback)

    async def disconnect(self):
        await self._pool.close()

    async def fetch(self, query, *args, key=None, with_world=False):
        async with self._pool.acquire() as conn:
            response = await conn.fetch(query, *args)
            batch = [dict(row) for row in response]

            if with_world is False:
                [row.pop('world') for row in batch]

            if key is not None:
                return {row.pop(key): row for row in batch}
            else:
                return batch

    async def fetchone(self, query, *args, with_world=False):
        async with self._pool.acquire() as conn:
            response = await conn.fetchrow(query, *args)

            if response is not None:
                result = dict(response)

                if with_world is False:
                    result.pop('world')

                return result
