import pathlib
from abc import ABC, abstractmethod
from typing import Optional, List


class ReadOnlyCacheAPI(ABC):
    @abstractmethod
    def select(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
               params: Optional[tuple] = None) -> List[tuple]:
        pass

    def exists(self, name: str, columns: str = '*', condition: str = '', params: Optional[tuple] = None) -> bool:
        return len(self.select(name, columns, condition, params=params)) > 0

    @abstractmethod
    def custom_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        pass


class CacheAPI(ReadOnlyCacheAPI, ABC):
    @abstractmethod
    def reset_connection(self):
        pass

    @abstractmethod
    def create_table(self, name: str, definition: str):
        pass

    @abstractmethod
    def delete_table(self, name: str):
        pass

    @abstractmethod
    def insert_row(self, name: str, parameters: tuple, columns: str = '', collision_resolution: str = 'ignore'):
        pass

    @abstractmethod
    def insert_rows(self, name: str, parameters: List[tuple], columns: str = '',
                    collision_resolution: str = 'ignore'):
        pass

    @abstractmethod
    def custom_query(self, query: str, params: Optional[tuple] = None, commit: bool = False) -> List[tuple]:
        pass

    @abstractmethod
    def custom_nr_query(self, query: str, params: Optional[tuple] = None, commit: bool = False):
        pass


class CachedClass(ABC):
    def __init__(self, api: CacheAPI):
        self.db = api
        self.setup_tables()

    @abstractmethod
    def setup_tables(self):
        pass

    def cache_check(self, name: str, columns: str = '*', condition: str = '',
                    params: Optional[tuple] = None) -> bool:
        return self.db.exists(name, columns, condition, params)

    def cache_lookup(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
                     params: Optional[tuple] = None) -> List[tuple]:
        return self.db.select(name, columns, condition, extras, params)

    def cache_save(self, name: str, params: tuple, columns: str = '', force: bool = True):
        self.db.insert_row(name, params, columns, 'replace' if force else 'ignore')

    def cache_save_many(self, name: str, params: List[tuple], columns: str = '', force: bool = True):
        if len(params) > 0:
            self.db.insert_rows(name, params, columns, 'replace' if force else 'ignore')
        else:
            print('cache save called with no rows')


class ReadOnlyCachedClass(ABC):
    def __init__(self, api: ReadOnlyCacheAPI):
        self.db = api

    def cache_check(self, name: str, columns: str = '*', condition: str = '',
                    params: Optional[tuple] = None) -> bool:
        return self.db.exists(name, columns, condition, params)

    def cache_lookup(self, name: str, columns: str = '*', condition: str = '', extras: str = '',
                     params: Optional[tuple] = None) -> List[tuple]:
        return self.db.select(name, columns, condition, extras, params)
