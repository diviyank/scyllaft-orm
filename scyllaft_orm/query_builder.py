"""Define the from pydantic import validate_call
query builder."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type

from loguru import logger
from pydantic import validate_call
from scyllaft import Scylla

from .column import AggregateExpr, Column, ColumnExpr
from .table import Table


class Query(ABC):
    """Generic query expression."""

    @abstractmethod
    def execute(self, scylla_instance: Scylla) -> Any:
        raise NotImplementedError

    @abstractmethod
    def build_query(self) -> str:
        raise NotImplementedError

    def __str__(self):
        return self.build_query()


class Select(Query):
    """Select query from scylla."""

    @validate_call
    def __init__(self, *columns: Column | AggregateExpr) -> None:
        assert len(columns) > 0, "Select expression cannot be empty!"

        self._table: Type[Table] = columns[0].table
        self._where: List[ColumnExpr] = []
        self._allow_filtering: bool = False
        self._limit: int = 0
        self._distinct: bool = False
        assert all(
            [col.table == self._table for col in columns]
        ), "Columns do not originate from the same table"

    def allow_filtering(self) -> Select:
        logger.warning(
            "Allow filtering usually leads to degraded performance. Consider reviewing your query."
        )
        self._allow_filtering = True
        return self

    @validate_call
    def where(self, *predicates: ColumnExpr) -> Select:
        assert len(predicates) > 0, "where condition cannot be empty!"
        assert all(
            [predicate.table == self._table for predicate in predicates]
        ), "Columns do not originate from the same table"
        self._where.extend(predicates)
        return self

    @validate_call
    def group_by(self, *columns: Column) -> Select:
        assert len(columns) > 0, "group_by condition cannot be empty!"
        assert all(
            [col.table == self._table for col in columns]
        ), "Columns do not originate from the same table"
        self._group_by = columns
        return self

    @validate_call
    def limit(self, _limit: int) -> Select:
        assert _limit > 0, "Limit cannot be null nor negative"
        self._limit = _limit
        return self

    def distinct(self) -> Select:
        self._distinct = True
        return self


class Update(Query):
    """Update query."""

    @validate_call
    def __init__(self, table: Type[Table]):
        self._table = table
        self._where: List[ColumnExpr] = []
        self._set_values: Dict[Column, Any] = {}

    @validate_call
    def set(self, column: Column, value: Any) -> Update:
        self._set_values[column] = value
        return self

    @validate_call
    def where(self, *predicates: ColumnExpr) -> Update:
        assert len(predicates) > 0, "where condition cannot be empty!"
        assert all(
            [predicate.table == self._table for predicate in predicates]
        ), "Columns do not originate from the same table"
        self._where.extend(predicates)
        return self


class Delete(Query):
    """Update query."""

    @validate_call
    def __init__(self, table: Type[Table]):
        self._table = table
        self._where: List[ColumnExpr] = []

    @validate_call
    def where(self, *predicates: ColumnExpr):
        self._where.append(predicates)


class Insert(Query):
    """Insert query.

    Suboptimal way to insert, as the "best way" is to batch insert using directly `scyllaft`.
    """

    @validate_call
    def __init__(self, table: Type[Table]):
        self._table = table
        self._values: List[Table] = []

    @validate_call
    def values(self, *values: Table) -> Insert:
        self._values.extend(values)
        return self
