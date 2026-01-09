from utils import config
from collections import Counter
import xmltodict
import traceback
import datetime
import requests
import psycopg2
import time
import json
import io
import re


# TODO: rewrite this mess properly when you got time

class Cardinal:
    def __init__(self):
        self.worlds = []
        self.max_archived_days = 30
        self.do_daily = False
        self.session = requests.Session()
        self.cursor = None
        self.conn, self.res = self.connect()

        self.languages = {
            'de': "die-staemme.de",
            'ch': "staemme.ch",
            'en': "tribalwars.net",
            'nl': "tribalwars.nl",
            'pl': "plemiona.pl",
            'br': "tribalwars.com.br",
            'pt': "tribalwars.com.pt",
            'cs': "divokekmeny.cz",
            'ro': "triburile.ro",
            'ru': "voynaplemyon.com",
            'gr': "fyletikesmaxes.gr",
            'sk': "divoke-kmene.sk",
            'it': "tribals.it",
            'tr': "klanlar.org",
            'fr': "guerretribale.fr",
            'es': "guerrastribales.es",
            'ae': "tribalwars.ae",
            'uk': "tribalwars.co.uk",
            'us': "tribalwars.us"
        }

        self.types = ("player", "tribe", "village", "world")

        self.base = "https://{}.{}/map"
        self.player_url = (
            f"{self.base}/player.txt",
            f"{self.base}/kill_att.txt",
            f"{self.base}/kill_def.txt",
            f"{self.base}/kill_sup.txt",
            f"{self.base}/kill_all.txt"
        )

        self.tribe_url = (
            f"{self.base}/ally.txt",
            f"{self.base}/kill_att_tribe.txt",
            f"{self.base}/kill_def_tribe.txt",
            f"{self.base}/kill_all_tribe.txt"
        )

        self.village_url = (
            f"{self.base}/village.txt",
        )

        self.player_create = (
            "world VARCHAR(6)",
            "id BIGINT",
            "name VARCHAR(288)",
            "tribe_id INT",
            "villages INT",
            "points BIGINT",
            "rank INT",
            "att_bash BIGINT",
            "att_rank INT",
            "def_bash BIGINT",
            "def_rank INT",
            "sup_bash BIGINT",
            "sup_rank INT",
            "all_bash BIGINT",
            "all_rank INT",
            "PRIMARY KEY (world, id)"
        )

        self.tribe_create = (
            "world VARCHAR(6)",
            "id INT",
            "name VARCHAR(384)",
            "tag VARCHAR(72)",
            "member SMALLINT",
            "villages INT",
            "points BIGINT",
            "all_points BIGINT",
            "rank INT",
            "att_bash BIGINT",
            "att_rank INT",
            "def_bash BIGINT",
            "def_rank INT",
            "all_bash BIGINT",
            "all_rank INT",
            "sup_bash BIGINT",
            "sup_rank INT",
            "PRIMARY KEY (world, id)"
        )

        self.village_create = (
            "world VARCHAR(6)",
            "id INT",
            "name VARCHAR(384)",
            "x SMALLINT",
            "y SMALLINT",
            "player_id BIGINT",
            "points INT",
            "rank SMALLINT",
            "PRIMARY KEY (world, id)"
        )

        self.world_create = (
            "world VARCHAR(6) PRIMARY KEY",
            "speed FLOAT(1)",
            "unit_speed FLOAT(1)",
            "moral SMALLINT",
            "config JSON"
        )

        self.empty = ["0", "0", "0", "0", "0", "0", "0", "0"]

        # Own Calculation since Inno doesn't support it
        self.tribe_support = {}
        self.tribe_support_rank = {}

    @staticmethod
    def connect():
        kwargs = config.conn_kwargs.copy()
        kwargs['dbname'] = kwargs.pop('database')

        conn_info = [f"{k}='{v}'" for k, v in kwargs.items()]

        for _ in range(2):
            yield psycopg2.connect(" ".join(conn_info))

    def run(self):
        self.setup_tables()

        try:
            self.engine()
        except Exception as e:
            print(f"EXCEPTION OCCURRED {e}")
            traceback.print_exc()
            self.send_code("404")

        print("Attempting to reconnect")

        try:
            self.session = requests.Session()
            self.conn, self.res = self.connect()
            self.run()
        except Exception as e:
            print(f"RECONNECT FAILED {e}")
            traceback.print_exc()
            self.send_code("404")

    def engine(self):
        restart = 0

        while True:
            if restart == 0:
                seconds = self.get_seconds_till_hour()
                time.sleep(seconds)

            response = self.update()

            if not response:
                self.send_code("400")

                if restart == 5:
                    print("RESTARTED 5 TIMES AND FAILED")
                    self.send_code("404")
                    exit()
                else:
                    restart += 1
                    sec = 10 + 3 ** (restart - 1)
                    time.sleep(sec)
            else:
                # reset restarts after every successful run
                restart = 0

    def update(self):
        start = datetime.datetime.now()

        # archive every day at 12 pm
        if start.hour == 0:
            self.do_daily = True

        try:
            self.worlds = self.update_worlds(start)
        except Exception as error:
            print(f"World Update Error: {error}")

            # if no initial world load worked
            if not self.worlds:
                return False

        try:
            self.update_data()
            self.send_code("200")
        except Exception as e:
            print(f"EXCEPTION OCCURRED {e}")
            traceback.print_exc()
            return False

        if self.do_daily:
            self.archive()
            self.do_daily = False

        end = datetime.datetime.now()
        current = datetime.datetime.strftime(end, "%H:%M")
        print(f"{current} | Updated {len(self.worlds)} worlds in {end - start}")
        return True

    def update_data(self):
        self.cursor = self.conn.cursor()

        for table in self.types[:-1]:
            cache = self.create_temp(table)
            # ignoring the last element (primary key definition)
            values = [col.split()[0] for col in cache[:-1]]

            for world in self.worlds:
                if table == "tribe":
                    tribe_support_world = self.tribe_support.get(world, {})

                    ranked_support = list(tribe_support_world.items())
                    ranked_support.sort(key=lambda l: l[1], reverse=True)

                    for index, (tribe_id, _) in enumerate(ranked_support, start=1):
                        self.tribe_support_rank[tribe_id] = str(index)

                data = self.data_packer(table, world)
                file = io.StringIO("\n".join(data))
                self.cursor.copy_from(file, "cache", columns=values, sep=',')
                table_name = f"{table}_{world}"

                query = f'LOCK TABLE {table_name};' \
                        f'TRUNCATE TABLE {table_name};' \
                        f'INSERT INTO {table_name} SELECT * FROM "cache";' \
                        f'TRUNCATE TABLE "cache";'

                self.cursor.execute(query)
                # per world instead of all at once
                self.tribe_support_rank.clear()
                self.conn.commit()

        self.tribe_support.clear()
        self.cursor.close()

    def data_packer(self, table, world):
        # 2 less because of the first iteration
        pointer = 6 if table == "tribe" else 4
        urls = getattr(self, f"{table}_url")
        data_pack = {}

        for index, base in enumerate(urls):
            domain = self.languages[world[:2]]
            url = base.format(world, domain)

            cache = self.secure_get(url)
            if cache is None:
                return self.fetch_old_data(table, world)

            lines = cache.text.split('\n')

            if not lines:
                continue

            elif lines[0].startswith("<!DOCTYPE html>"):
                return self.fetch_old_data(table, world)

            for line in lines:

                if not line:
                    continue

                # first url everything besides bash data
                if index == 0:
                    entry = line.split(',')

                    if table == 'player':
                        entry += self.empty
                    elif table == 'tribe':
                        # 6 inno entries + custom sup bash
                        entry += self.empty[:8]

                        local_tribe_support = self.tribe_support.get(world, {})
                        support_points = local_tribe_support.get(entry[0], 0)
                        entry[-2] = str(support_points)

                        local_tribe_support_rank = self.tribe_support_rank.get(entry[0], "0")
                        entry[-1] = local_tribe_support_rank

                    data_pack[entry[0]] = entry

                # bash data
                else:
                    try:
                        rank, id_, bash_points = line.split(',')
                        data_pack[id_][pointer] = bash_points
                        data_pack[id_][pointer + 1] = rank

                        if index == 3 and table == 'player':
                            tribe_support_world = self.tribe_support.get(world)

                            if tribe_support_world is None:
                                tribe_support_world = self.tribe_support[world] = Counter()

                            tribe_id = data_pack[id_][2]
                            tribe_support_world[tribe_id] += int(data_pack[id_][pointer])

                    except KeyError:
                        continue

            pointer += 2

        # not that readable but way faster
        return [f'{",".join([world, *listed])}' for listed in data_pack.values()]

    def archive(self):
        cur = self.conn.cursor()

        cur.execute(
            'SELECT table_name FROM information_schema.tables '
            'WHERE table_schema=\'public\' '
            'AND table_type=\'BASE TABLE\' '
            'AND table_name ~ \'[a-z]+_\d{1,2}\''  # noqa
        )

        all_tables = [obj[0] for obj in cur.fetchall()]

        for table in self.types[:-1]:
            tables = [t for t in all_tables if table in t]
            sort = sorted(tables, key=lambda t: int(t[len(table) + 1:]), reverse=True)

            for archive_table in sort:
                num = int(archive_table[len(table) + 1:])

                if num + 1 > self.max_archived_days:
                    query = f'DROP TABLE {archive_table};'
                    cur.execute(query)
                    continue

                new_name = f"{table}_{num + 1}"
                base = 'LOCK TABLE {0};' \
                       'ALTER TABLE {0} RENAME TO {1};'
                query = base.format(archive_table, new_name)
                cur.execute(query)

            base = 'CREATE TABLE {} TABLESPACE archive AS TABLE {};'
            query = base.format(f"{table}_1", table)
            cur.execute(query)
            self.conn.commit()

    def fetch_old_data(self, table, world):
        query = f'SELECT * FROM {table} WHERE world = \'{world}\';'
        self.cursor.execute(query)

        old_data = []
        for row in self.cursor.fetchall():
            str_row = ",".join([str(e) for e in row])
            old_data.append(str_row)

        return old_data

    # creates base tables if needed
    def setup_tables(self):
        cur = self.conn.cursor()
        base = 'CREATE TABLE IF NOT EXISTS "{}" ({})'

        for table in self.types:
            empty_query = base + ' PARTITION BY LIST (world)' if table != "world" else base
            values = getattr(self, f"{table}_create")
            query = empty_query.format(table, ",".join(values))
            cur.execute(query)

        self.conn.commit()
        cur.close()

    # creates a cache table with base table columns
    def create_temp(self, table):
        base = 'DROP TABLE IF EXISTS "cache";' \
               'CREATE TABLE "cache" ({});'
        values = getattr(self, f"{table}_create")
        query = base.format(",".join(values))
        self.cursor.execute(query)
        self.conn.commit()
        return values

    # refresh valid worlds
    def update_worlds(self, date):
        cur = self.conn.cursor()
        cur.execute('SELECT world FROM world')
        old_worlds = [row[0] for row in cur.fetchall()]

        worlds = []
        for world_key, lang in self.languages.items():
            base = "https://{}/backend/get_servers.php"
            content = self.session.get(base.format(lang))
            matches = re.findall(r'([a-z]{2}([a-z])?\d+)', content.text)

            if not matches:
                continue

            elif content.text.startswith("<!DOCTYPE html>"):
                return None

            current_worlds = {k: v for k, v in matches}

            for world, world_type in current_worlds.items():
                # ignoring speed servers
                if world_type == "s":
                    continue
                else:
                    worlds.append(world)

                    # creating partitions of worlds
                    queries = []
                    for table in self.types[:-1]:
                        query = f'CREATE TABLE IF NOT EXISTS {table}_{world} ' \
                                f'PARTITION OF {table} FOR VALUES IN (\'{world}\');'
                        queries.append(query)

                    cur.execute("".join(queries))

                # config loads only for new worlds and at 12AM
                if world in old_worlds and date.hour != 0:
                    continue

                base = "https://{}.{}/interface.php?func=get_config"
                cache = self.secure_get(base.format(world, lang))

                if cache is None:
                    continue

                parsed_xml = xmltodict.parse(cache.text, dict_constructor=dict)
                world_config = parsed_xml['config']
                imp = world_config.pop('speed'), world_config.pop('unit_speed'), world_config.pop('moral')
                batch = [world, *[float(n) for n in imp], json.dumps(world_config)]

                query = 'INSERT INTO world (world, speed, unit_speed, moral, config) ' \
                        'VALUES (%s, %s, %s, %s, %s) ON CONFLICT (world) DO UPDATE SET ' \
                        'speed = EXCLUDED.speed, unit_speed = EXCLUDED.unit_speed, ' \
                        'moral = EXCLUDED.moral, config = EXCLUDED.config'

                cur.execute(query, batch)

        dead_worlds = tuple(set(old_worlds) - set(worlds))
        if dead_worlds:
            for dead_world in dead_worlds:
                self.cleanup_dead_world(cur, dead_world)

            # query = 'DELETE FROM world WHERE world IN %s;'
            # cur.execute(query, (dead_worlds,))

        self.conn.commit()
        return worlds

    def cleanup_dead_world(self, cursor, dead_world):
        for table in self.types[:-1]:
            query = f'''DROP TABLE IF EXISTS {table}_{dead_world};
            DELETE FROM world WHERE world = \'{dead_world}\';'''
            cursor.execute(query)

    def send_code(self, code):
        try:
            query = f"NOTIFY log, '{code}'"
            cur = self.res.cursor()
            cur.execute(query)
            self.res.commit()
        except Exception as e:
            print(f"EXCEPTION OCCURRED NOTIFYING {e}")
            traceback.print_exc()

    def secure_get(self, url):
        for _ in range(3):
            try:
                return self.session.get(url)
            except ConnectionError:
                self.session = requests.Session()
                time.sleep(.15)

        return None

    @staticmethod
    def get_seconds_till_hour():
        now = datetime.datetime.now()
        clean = now + datetime.timedelta(hours=1)
        goal_time = clean.replace(minute=0, second=0, microsecond=0)
        start_time = now.replace(microsecond=0)
        goal = (goal_time - start_time).seconds
        return goal

    def manual_run(self, archive=False, send_code=False):
        start = datetime.datetime.now()

        try:
            self.worlds = self.update_worlds(start)
        except Exception as error:
            print(f"World Update Error: {error}")

            # if no initial world load worked
            if not self.worlds:
                return

        try:
            self.update_data()
            if send_code:
                self.send_code("200")

        except Exception as e:
            print(f"EXCEPTION OCCURED {e}")
            traceback.print_exc()
            return

        if archive:
            self.archive()

        end = datetime.datetime.now()
        current = datetime.datetime.strftime(end, "%H:%M")
        print(f"{current} | Updated {len(self.worlds)} worlds in {end - start}")

    def manual_cleanup(self):
        start = datetime.datetime.now()

        try:
            self.worlds = self.update_worlds(start)
        except Exception as error:
            print(f"World Update Error: {error}")

        # if no initial world load worked or no worlds
        if not self.worlds:
            return

        query = '''SELECT table_name FROM information_schema.tables
        WHERE table_schema=\'public\'
        AND table_type=\'BASE TABLE\'
        AND table_name ~ \'[a-z]+_[a-z]{2}\\d{1,2}\''''

        cur = self.conn.cursor()
        cur.execute(query)
        all_world_tables = [obj[0] for obj in cur.fetchall()]

        for table in all_world_tables:
            world = table.split("_")[-1]

            if world not in self.worlds:
                cur.execute(f'DROP TABLE {table};')
                print("DROPPED TABLE", table)

        self.conn.commit()
        cur.close()


cardinal = Cardinal()
cardinal.run()
