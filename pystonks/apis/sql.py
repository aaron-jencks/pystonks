import pathlib
import sqlite3
from typing import List, Optional, Tuple

from pystonks.utils.structures.caching import CacheAPI, ReadOnlyCacheAPI

SQL_DATE_FMT = '%Y-%m-%d'
SQL_TIME_FMT = '%H:%M:%S.%f%:z'
SQL_DT_FMT = SQL_DATE_FMT + 'T' + SQL_TIME_FMT


class SqliteController:
    _instances = {}

    @staticmethod
    def reset_instances():
        SqliteController._instances = {}

    def __new__(cls, *args, **kwargs):
        if (len(args) == 0 or args[0] is None) and ('loc' not in kwargs or kwargs['loc'] is None):
            if len(cls._instances) != 1:
                raise Exception('cannot instantiate sql singleton without exactly one instance')
            name = [k for k in cls._instances][0]
            return cls._instances[name]

        fname = str(args[0] if len(args) > 0 else kwargs['loc'])
        if fname not in cls._instances:
            cls._instances[fname] = super(SqliteController, cls).__new__(cls)
        return cls._instances[fname]

    def __init__(self, loc: Optional[pathlib.Path] = None):
        if loc is not None:
            self.loc = loc
        self.conn: Optional[sqlite3.Connection] = None
        self.can_commit = True

    def connect(self):
        if self.conn is not None:
            self.close()
        self.conn = sqlite3.connect(self.loc)

    def commit(self):
        if self.conn is not None:
            self.conn.commit()

    def close(self):
        if self.conn is None:
            return
        self.conn.close()
        self.conn = None

    def start_query(self, query: str, params: Optional[tuple]) -> sqlite3.Cursor:
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur

    def finish_query(self, commit: bool, cur: sqlite3.Cursor):
        if commit and self.can_commit:
            self.conn.commit()
        cur.close()

    def nr_query(self, query: str, params: Optional[tuple] = None, commit: bool = True):
        cur = self.start_query(query, params)
        self.finish_query(commit, cur)

    def query(self, query: str, params: Optional[tuple] = None, commit: bool = True) -> list:
        cur = self.start_query(query, params)
        rows = cur.fetchall()
        self.finish_query(commit, cur)
        return rows

    def nr_query_many(self, query: str, params: List[tuple] = None, commit: bool = True):
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        cur.executemany(query, params)
        if commit and self.can_commit:
            self.conn.commit()
        cur.close()


class ReadOnlySqliteController:

    def __init__(self, loc: Optional[pathlib.Path] = None):
        self.loc = loc

    def query(self, query: str, params: Optional[tuple] = None) -> list:
        with sqlite3.connect(self.loc) as conn:
            cur = conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            rows = cur.fetchall()
            cur.close()
        return rows


class SqliteAPI(CacheAPI):
    def __init__(self, loc: Optional[pathlib.Path] = None):
        self.conn = SqliteController(loc)

    def reset_connection(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def disable_commiting(self):
        self.conn.can_commit = False

    def enable_commiting(self):
        self.conn.can_commit = True

    def create_table(self, name: str, definition: str):
        self.conn.nr_query(f'create table if not exists {name}({definition})')

    def delete_table(self, name: str):
        self.conn.nr_query(f'drop table if exists {name}')

    def insert_row(self, name: str, parameters: tuple, columns: str = '',
                         collision_resolution: str = 'ignore'):
        base = 'insert {} into {} {} values ({})'
        cr = f'or {collision_resolution}' if len(collision_resolution) > 0 else ''
        if len(columns) > 0:
            cq = columns if columns[0] == '(' else f'({columns})'
        else:
            cq = ''
        self.conn.nr_query(base.format(cr, name, cq, ','.join(['?']*len(parameters))), parameters)

    def insert_rows(self, name: str, parameters: List[tuple], columns: str = '',
                          collision_resolution: str = 'ignore'):
        if len(parameters) == 0:
            raise Exception('insert_rows called with empty parameters list')

        base = 'insert {} into {} {} values ({})'
        cr = f'or {collision_resolution}' if len(collision_resolution) > 0 else ''
        if len(columns) > 0:
            cq = columns if columns[0] == '(' else f'({columns})'
        else:
            cq = ''

        self.conn.nr_query_many(base.format(cr, name, cq, ','.join(['?'] * len(parameters[0]))), parameters)

    def select(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
                     params: Optional[tuple] = None) -> List[tuple]:
        query = 'select {} from {} {} {}'.format(
            columns, name,
            'where {}'.format(condition) if len(condition) > 0 else '',
            extras
        )
        return self.conn.query(query, params, False)

    def custom_query(self, query: str, params: Optional[tuple] = None, commit: bool = False) -> List[tuple]:
        return self.conn.query(query, params, commit)

    def custom_nr_query(self, query: str, params: Optional[tuple] = None, commit: bool = False):
        self.conn.nr_query(query, params, commit)


class ReadOnlySqliteAPI(ReadOnlyCacheAPI):
    def __init__(self, loc: pathlib.Path):
        self.conn = ReadOnlySqliteController(loc)

    def select(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
               params: Optional[tuple] = None) -> List[tuple]:
        query = 'select {} from {} {} {}'.format(
            columns, name,
            'where {}'.format(condition) if len(condition) > 0 else '',
            extras
        )
        return self.conn.query(query, params)

    def custom_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        return self.conn.query(query, params)
