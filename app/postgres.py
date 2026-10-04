"""PostgreSQL storage for Vercel; local development keeps SQLite."""
from contextlib import contextmanager
from pathlib import Path
import psycopg
from psycopg.rows import dict_row

class Row(dict):
    def __getitem__(self,key):
        if isinstance(key,int): return tuple(self.values())[key]
        return super().__getitem__(key)

class Cursor:
    def __init__(self,cursor): self.cursor=cursor
    @property
    def rowcount(self): return self.cursor.rowcount
    def fetchone(self):
        row=self.cursor.fetchone()
        return Row(row) if row is not None else None
    def fetchall(self): return [Row(row) for row in self.cursor.fetchall()]
    def __iter__(self): return iter(self.fetchall())

class Connection:
    def __init__(self,connection): self.connection=connection
    def execute(self,query,params=()):
        if query=='BEGIN IMMEDIATE':
            # Same critical sections as SQLite, coordinated across function instances.
            return Cursor(self.connection.execute('SELECT pg_advisory_xact_lock(73412026)'))
        if params:
            query=query.replace('%','%%').replace('?','%s')
            return Cursor(self.connection.execute(query,params))
        return Cursor(self.connection.execute(query))

@contextmanager
def connect(url):
    with psycopg.connect(url,row_factory=dict_row,connect_timeout=15,prepare_threshold=None) as conn:
        yield Connection(conn)

def initialize(url):
    with psycopg.connect(url,connect_timeout=15,prepare_threshold=None) as conn:
        conn.execute('SELECT pg_advisory_xact_lock(73412027)')
        conn.execute(Path(__file__).with_name('schema.sql').read_text())
