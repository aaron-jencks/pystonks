import os
import pathlib
import random as rng
import tempfile
import unittest

from pystonks.apis.sql import SqliteAPI, SqliteController


def table_exists(instance: SqliteAPI, name: str) -> bool:
    resp = instance.select('sqlite_master', 'name', 'type="table" and name=?', params=(name,))
    return len(resp) > 0


def table_row_count(instance: SqliteAPI, name: str) -> int:
    resp = instance.select(name, 'count(*)')
    return len(resp)


def generate_random_name(n: int = 10, chars: str = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') -> str:
    return ''.join(rng.sample(chars, n))


class SqlTestCase(unittest.TestCase):
    def setUp(self):
        fd, self.fname = tempfile.mkstemp('.db')
        os.close(fd)
        self.sql_instance = SqliteAPI(pathlib.Path(self.fname))

    def tearDown(self):
        self.sql_instance.reset_connection()
        os.remove(self.fname)
        SqliteController.reset_instances()

    def test_instance_dup_call(self):
        try:
            inst = SqliteAPI()
            self.assertIsNotNone(inst.conn.loc, 'singleton location should not be none')
        except:
            self.fail('multiple calls to get instance shouldn\'t require arguments')

    def test_table_creation_deletion(self):
        tname = generate_random_name()
        self.sql_instance.create_table(tname, 'symbol text, date text, timestamp text, action text, '
                                              'primary key (symbol, date, timestamp)')
        self.assertTrue(table_exists(self.sql_instance, tname), 'table should exist after creating')
        self.sql_instance.delete_table(tname)
        self.assertFalse(table_exists(self.sql_instance, tname), 'table should not exist after deleting')

    def test_table_insertion(self):
        tname = generate_random_name()
        self.sql_instance.create_table(tname, 'symbol text, date text, timestamp text, action text, '
                                              'primary key (symbol, date, timestamp)')
        self.assertTrue(table_exists(self.sql_instance, tname), 'table should exist after creating')
        self.sql_instance.insert_row(tname, (
            generate_random_name(),
            generate_random_name(),
            generate_random_name(),
            generate_random_name()
        ))
        self.assertEqual(table_row_count(self.sql_instance, tname), 1, 'row should be inserted')


if __name__ == '__main__':
    unittest.main()
