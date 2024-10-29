import pathlib
from abc import ABC, abstractmethod
from typing import Optional, List


class ReadOnlyCacheAPI(ABC):
    """
    Represents an API for caching,
    but without access to creating tables,
    or writing results
    """

    @abstractmethod
    def select(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
               params: Optional[tuple] = None) -> List[tuple]:
        """
        wraps a select query in sql
        :param name: The name of the table to query
        :param columns: The columns to return
        :param condition: The where clause of the query
        :param extras: Any additional data needed for the query that isn't in the condition
        :param params: Any parameters to be supplied to the query
        :return: Returns a list of tuples that contain the columns supplied, in the same order
        """
        pass

    def exists(self, name: str, columns: str = '*', condition: str = '', params: Optional[tuple] = None) -> bool:
        """
        Performs a select, but just checks that data is returned
        :param name: The name of the table to query
        :param columns: The columns to return
        :param condition: The where clause of the query
        :param params: Any parameters to be supplied to the query
        :return: Returns if a select statement returns at least 1 element
        """
        return len(self.select(name, columns, condition, params=params)) > 0

    @abstractmethod
    def custom_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Runs a custom query that doesn't fit any of the templates above
        :param query: The sql query to execute
        :param params: Parameters that need to be fed to the query
        :return: Returns a list of tuples corresponding to the results of the query supplied.
        """
        pass


class CacheAPI(ReadOnlyCacheAPI, ABC):
    """
    Represents an API for reading and writing to a sql-like interface.
    """

    @abstractmethod
    def reset_connection(self):
        """
        Resets the connection to the underlying cache, essentially disconnecting and (maybe) reconnecting
        :return:
        """
        pass

    @abstractmethod
    def create_table(self, name: str, definition: str):
        """
        Creates a new table with the given name and definition
        :param name: The name of the table to create
        :param definition: The definition of the table to create
        :return:
        """
        pass

    @abstractmethod
    def delete_table(self, name: str):
        """
        Deletes the given table
        :param name: The name of the table to delete
        :return:
        """
        pass

    @abstractmethod
    def insert_row(self, name: str, parameters: tuple, columns: str = '', collision_resolution: str = 'ignore'):
        """
        Inserts a new row into the database
        :param name: Name of the table to insert into
        :param parameters: The data being inserted
        :param columns: The columns that correspond to the parameters tuple
        :param collision_resolution: How to resolve collisions (duplicate primary keys)
        :return:
        """
        pass

    @abstractmethod
    def insert_rows(self, name: str, parameters: List[tuple], columns: str = '',
                    collision_resolution: str = 'ignore'):
        """
        Inserts a new row into the database
        :param name: Name of the table to insert into
        :param parameters: The list of row data being inserted
        :param columns: The columns that correspond to the tuples in the parameters
        :param collision_resolution: How to resolve collisions (duplicate primary keys)
        :return:
        """
        pass

    @abstractmethod
    def custom_nr_query(self, query: str, params: Optional[tuple] = None, commit: bool = False):
        """
        Runs a custom query in the database
        :param query: The query to execute
        :param params: Parameters to be fed into the query
        :param commit: Whether to write the changes to the database
        :return:
        """
        pass


class CachedClass(ABC):
    """
    Represents a cache for APIs to reduce burden on various REST APIs
    """

    def __init__(self, api: CacheAPI):
        """
        Creates a new cached class, also sets up the tables
        :param api: The CacheAPI to use for caching
        """
        self.db = api
        self.setup_tables()

    @abstractmethod
    def setup_tables(self):
        """
        Used to create any required tables for the caching methods in the class
        :return:
        """
        pass

    def cache_check(self, name: str, columns: str = '*', condition: str = '',
                    params: Optional[tuple] = None) -> bool:
        """
        Determines if an entry exists in the cache
        :param name: The name of the table to check
        :param columns: The columns to check
        :param condition: The where clause of the query
        :param params: Any parameters to be fed to the query
        :return: If there exists at least one entry in the underlying database that matches the query
        """
        return self.db.exists(name, columns, condition, params)

    def cache_lookup(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
                     params: Optional[tuple] = None) -> List[tuple]:
        """
        Fetches entries from the cache
        :param name: The name of the table to query
        :param columns: Columns to retrieve
        :param condition: The where clause of the query
        :param extras: Any additional query details
        :param params: Any parameters to be fed to the query
        :return: A list of tuples corresponding to the results of the query containing the provided columns
        """
        return self.db.select(name, columns, condition, extras, params)

    def cache_save(self, name: str, params: tuple, columns: str = '', force: bool = True):
        """
        Saves a single entry into the cache
        :param name: The table to save to
        :param params: The entry to save
        :param columns: The columns in the entry
        :param force: Specified to ignore collisions, or duplicate entries
        :return:
        """
        self.db.insert_row(name, params, columns, 'replace' if force else 'ignore')

    def cache_save_many(self, name: str, params: List[tuple], columns: str = '', force: bool = True):
        """
        Saves multiple entries into the cache
        :param name: The table to save to
        :param params: The entries to save
        :param columns: The columns in each entry
        :param force: Specified to ignore collisions, or duplicate entries
        :return:
        """
        if len(params) > 0:
            self.db.insert_rows(name, params, columns, 'replace' if force else 'ignore')
        else:
            print('cache save called with no rows')


class ReadOnlyCachedClass(ABC):
    """
    Represents a readonly version of the CachedClass.
    It contains the same methods, but without the ability to setup tables and save entries.
    """
    def __init__(self, api: ReadOnlyCacheAPI):
        """
        Creates a new readonly cached class
        :param api: The CacheAPI to use for caching
        """
        self.db = api

    def cache_check(self, name: str, columns: str = '*', condition: str = '',
                    params: Optional[tuple] = None) -> bool:
        """
        Determines if an entry exists in the cache
        :param name: The name of the table to check
        :param columns: The columns to check
        :param condition: The where clause of the query
        :param params: Any parameters to be fed to the query
        :return: If there exists at least one entry in the underlying database that matches the query
        """
        return self.db.exists(name, columns, condition, params)

    def cache_lookup(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
                     params: Optional[tuple] = None) -> List[tuple]:
        """
        Fetches entries from the cache
        :param name: The name of the table to query
        :param columns: Columns to retrieve
        :param condition: The where clause of the query
        :param extras: Any additional query details
        :param params: Any parameters to be fed to the query
        :return: A list of tuples corresponding to the results of the query containing the provided columns
        """
        return self.db.select(name, columns, condition, extras, params)
