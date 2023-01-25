import utils
import asyncpg


class Database:
    def __init__(self):
        self._pool = None
        self._conn = None
        self.worlds = []
        self.languages = []

    async def connect(self):
        self._pool = await asyncpg.create_pool(**utils.conn_kwargs)
        await self.update_worlds()

        # initiate logging connection for discord callback
        self._conn = await self._pool.acquire()
        await self._conn.add_listener('log', self.callback)

    async def disconnect(self):
        await self._pool.close()

    def verify_world(self, world):
        if world not in self.worlds:
            raise utils.error.InvalidWorld()

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

    def create_query(self, table_types, query, world_id, *extra_args):
        if world_id not in self.worlds:
            raise utils.error.InvalidWorld()

        if isinstance(table_types, str):
            table_types = [table_types]

        table_names = [e + "_" + world_id for e in table_types]
        return str(query.format(*table_names, *extra_args))

    async def callback(self, *args):
        payload = args[-1]

        if payload == "200":
            await self.update_worlds()
        else:
            print(args)

    async def update_worlds(self):
        response = await self.fetch('SELECT world FROM world', with_world=True)

        self.worlds = [e['world'] for e in response]
        self.languages = [w[:2] for w in self.worlds]
