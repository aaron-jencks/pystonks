import datetime as dt
from pathlib import Path
from typing import List, Optional

from pystonks.apis.alpolyhoo import AlFinnPolyHooStaticFilterAPI, AlPolyHooStaticFilterAPI
from pystonks.apis.sql import SqliteAPI
from pystonks.market.filter import StaticTickerFilter
from pystonks.supervised.annotations.controllers.annotations import AnnotationAPI
from pystonks.supervised.annotations.models import Annotation


class AnnotatorCluster(AlPolyHooStaticFilterAPI):
    def __init__(self, db: Path, alpaca_key: str, alpaca_secret: str,
                 polygon_key: str, static_filters: List[StaticTickerFilter], paper: bool = True):
        self.cache = SqliteAPI(db)
        self.annotations = AnnotationAPI(self.cache)
        super().__init__(alpaca_key, alpaca_secret, polygon_key, paper, static_filters, self.cache)

    def count(self, symbol: Optional[str] = '', timestamp: Optional[dt.datetime] = None) -> int:
        return self.annotations.count(symbol, timestamp)

    def create_annotation(self, anno: Annotation):
        self.annotations.create(anno)

    def retrieve_annotation(self, symbol: str, timestamp: dt.datetime) -> Optional[Annotation]:
        return self.annotations.retrieve(symbol, timestamp)

    def retrieve_all_annotations(self, symbol: Optional[str] = '', timestamp: Optional[dt.datetime] = None) -> List[Annotation]:
        return self.annotations.retrieve_all(symbol, timestamp)

    def update_annotation(self, new_anno: Annotation):
        self.annotations.update(new_anno)

    def delete_annotation(self, symbol: str, timestamp: dt.datetime):
        self.annotations.delete(symbol, timestamp)

    def delete_all_annotations(self, symbol: str, date: dt.datetime):
        self.annotations.delete_all(symbol, date)

    def finish_annotations(self, symbol: str, timestamp: dt.datetime):
        self.annotations.finish(symbol, timestamp)

    def are_annotations_finished(self, symbol: str, timestamp: dt.datetime) -> bool:
        return self.annotations.is_finished(symbol, timestamp)

    def finished_annotations_count(self) -> int:
        return self.annotations.finished_count()


class FinnAnnotatorCluster(AlFinnPolyHooStaticFilterAPI):
    def __init__(self, db: Path, alpaca_key: str, alpaca_secret: str,
                 polygon_key: str, finnhub_key: str, static_filters: List[StaticTickerFilter], paper: bool = True):
        self.cache = SqliteAPI(db)
        self.annotations = AnnotationAPI(self.cache)
        super().__init__(alpaca_key, alpaca_secret, polygon_key, finnhub_key, paper, static_filters, self.cache)

    def count(self, symbol: Optional[str] = '', timestamp: Optional[dt.datetime] = None) -> int:
        return self.annotations.count(symbol, timestamp)

    def create_annotation(self, anno: Annotation):
        self.annotations.create(anno)

    def retrieve_annotation(self, symbol: str, timestamp: dt.datetime) -> Optional[Annotation]:
        return self.annotations.retrieve(symbol, timestamp)

    def retrieve_all_annotations(self, symbol: Optional[str] = '', timestamp: Optional[dt.datetime] = None) -> List[Annotation]:
        return self.annotations.retrieve_all(symbol, timestamp)

    def update_annotation(self, new_anno: Annotation):
        self.annotations.update(new_anno)

    def delete_annotation(self, symbol: str, timestamp: dt.datetime):
        self.annotations.delete(symbol, timestamp)

    def delete_all_annotations(self, symbol: str, date: dt.datetime):
        self.annotations.delete_all(symbol, date)

    def finish_annotations(self, symbol: str, timestamp: dt.datetime):
        self.annotations.finish(symbol, timestamp)

    def are_annotations_finished(self, symbol: str, timestamp: dt.datetime) -> bool:
        return self.annotations.is_finished(symbol, timestamp)

    def finished_annotations_count(self) -> int:
        return self.annotations.finished_count()
