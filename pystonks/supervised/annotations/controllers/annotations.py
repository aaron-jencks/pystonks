import datetime as dt
from typing import Optional, List

from pystonks.apis.sql import SQL_DATE_FMT
from pystonks.supervised.annotations.models import Annotation, TradeActions
from pystonks.utils.structures.caching import CachedClass, CacheAPI


class AnnotationAPI(CachedClass):
    def __init__(self, cache: CacheAPI):
        super().__init__(cache)

    def setup_tables(self):
        self.db.create_table('annotations_finished', 'symbol text, date text, primary key (symbol, date)')
        self.db.create_table(
            'annotations',
            'symbol text, timestamp text, action text, primary key (symbol, timestamp)'
        )
        self.db.reset_connection()

    def count(self, symbol: Optional[str] = '', timestamp: Optional[dt.datetime] = None) -> int:
        if symbol is None or symbol == '':
            if timestamp is None:
                order = 'order by date(timestamp) asc, symbol asc, timestamp asc'
                params = None
            else:
                order = 'date(timestamp) = ? order by symbol asc, timestamp asc'
                params = (timestamp.strftime(SQL_DATE_FMT),)
        elif timestamp is None:
            order = 'symbol = ? order by date(timestamp) asc, timestamp asc'
            params = (symbol,)
        else:
            order = 'symbol = ? and date(timestamp) = ? order by timestamp asc'
            params = (symbol, timestamp.strftime(SQL_DATE_FMT))

        return self.cache_lookup(
            'annotations',
            columns='count(*)',
            extras=order,
            params=params
        )[0][0]

    def create(self, anno: Annotation):
        self.cache_save(
            'annotations',
            params=(
                anno.symbol,
                anno.timestamp.isoformat(),
                anno.action.name
            )
        )

    def retrieve(self, symbol: str, timestamp: dt.datetime) -> Optional[Annotation]:
        if self.cache_check('annotations', condition='symbol = ? and timestamp = ?', params=(
            symbol, timestamp.isoformat()
        )):
            rows = self.cache_lookup('annotations', condition='symbol = ? and timestamp = ?', params=(
                symbol, timestamp.isoformat()
            ))[0]
            annot = Annotation(
                rows[0],
                dt.datetime.fromisoformat(rows[1]),
                TradeActions[rows[2]]
            )
        else:
            annot = None
        return annot

    def retrieve_all(self, symbol: Optional[str] = '', timestamp: Optional[dt.datetime] = None) -> List[Annotation]:
        if symbol is None or symbol == '':
            if timestamp is None:
                rows = self.cache_lookup(
                    'annotations',
                    extras='order by date(timestamp) asc, symbol asc, timestamp asc'
                )
            else:
                rows = self.cache_lookup(
                    'annotations',
                    condition='date(timestamp) = ? order by symbol asc, timestamp asc',
                    params=(timestamp.strftime(SQL_DATE_FMT),)
                )
        elif timestamp is None:
            rows = self.cache_lookup(
                'annotations',
                condition='symbol = ? order by date(timestamp) asc, timestamp asc',
                params=(symbol,)
            )
        else:
            rows = self.cache_lookup(
                'annotations',
                condition='symbol = ? and date(timestamp) = ? order by timestamp asc',
                params=(symbol, timestamp.strftime(SQL_DATE_FMT))
            )
        result = [
            Annotation(
                r[0],
                dt.datetime.fromisoformat(r[1]),
                TradeActions[r[2]]
            )
            for r in rows
        ]
        return result

    def update(self, new_anno: Annotation):
        self.db.custom_nr_query(
            'update or replace annotations set action = ? where symbol = ? and timestamp = ?',
            params=(
                new_anno.action.name,
                new_anno.symbol,
                new_anno.timestamp.isoformat()
            ),
            commit=True
        )
        self.db.reset_connection()

    def delete(self, symbol: str, timestamp: dt.datetime):
        self.db.custom_nr_query(
            'delete from annotations where symbol = ? and timestamp = ?',
            (
                symbol,
                timestamp.isoformat()
            ),
            commit=True
        )
        self.db.reset_connection()

    def delete_all(self, symbol: str, date: dt.datetime):
        self.db.custom_nr_query(
            'delete from annotations where symbol = ? and date(timestamp) = ?',
            (
                symbol,
                date.strftime(SQL_DATE_FMT)
            ),
            commit=True
        )
        self.db.reset_connection()

    def finish(self, symbol: str, timestamp: dt.datetime):
        self.cache_save('annotations_finished', params=(symbol, timestamp.strftime(SQL_DATE_FMT)))
        self.db.reset_connection()

    def is_finished(self, symbol: str, timestamp: dt.datetime) -> bool:
        return self.cache_check(
            'annotations_finished',
            condition='symbol = ? and date = ?',
            params=(symbol, timestamp.strftime(SQL_DATE_FMT))
        )

    def finished_count(self) -> int:
        rows = self.db.select('annotations_finished', 'count(*)')
        return rows[0][0]
