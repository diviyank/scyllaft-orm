"""Query builder for scylla."""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Type

from loguru import logger
from pydantic import validate_call
from scyllaft import Scylla

from .column import AggregateExpr, Column, ColumnExpr
from .table import Table


class Query(ABC):
    """Generic query expression."""

    async def execute(self, scylla_instance: Scylla) -> Any:
        query, parameters = self.build_query()
        result = await scylla_instance.execute(query, parameters)
        return result

    @abstractmethod
    def build_query(self) -> Tuple[str, List[Any]]:
        """Builds the query into a str and its parameters as a list."""
        raise NotImplementedError

    def __str__(self):
        """For printing purposes."""
        return self.build_query()[0]


class Select(Query):
    """Select query from scylla."""

    @validate_call
    def __init__(self, *columns: Column | AggregateExpr | Type[Table]) -> None:
        assert len(columns) > 0, "Select expression cannot be empty!"

        if inspect.isclass(columns[0]) and issubclass(columns[0], Table):
            self._table = columns[0].__tablename__
            self._keyspace = columns[0].__keyspace__
            self._select = "*"
            self._columns: List[Column | AggregateExpr] = []
        else:
            self._table: str = columns[0]._table  # type:ignore
            assert all(
                [
                    isinstance(col, (Column, AggregateExpr))
                    and (col._table == self._table)
                    and (col._keyspace == self._keyspace)
                    for col in columns
                ]
            ), "Columns do not originate from the same table or is invalid"
            self._select = ", ".join(
                [
                    (
                        f"{col._name}{f' AS {col.rename}' if col.rename is not None else ''}"
                        if isinstance(col, Column)
                        else f"{col.operator}({col._name}){f' AS {col.rename}' if col.rename is not None else ''}"  # type:ignore
                    )
                    for col in columns
                ]
            )
            self._columns = columns  # type:ignore

        self._where: List[ColumnExpr] = []
        self._allow_filtering: bool = False
        self._limit: int = 0
        self._distinct: bool = False
        self._group_by: Optional[str] = None

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
            [
                (predicate._table == self._table)
                and (predicate._keyspace == self._keyspace)
                for predicate in predicates
            ]
        ), "Columns do not originate from the same table"
        self._where.extend(predicates)
        return self

    @validate_call
    def group_by(self, *columns: Column) -> Select:
        assert len(columns) > 0, "group_by condition cannot be empty!"
        assert all(
            [
                (col._table == self._table) and (col._keyspace == self._keyspace)
                for col in columns
            ]
        ), "Columns do not originate from the same table"
        self._group_by = ", ".join([str(column._name) for column in columns])
        return self

    @validate_call
    def limit(self, _limit: int) -> Select:
        assert _limit > 0, "Limit cannot be null nor negative"
        self._limit = _limit
        return self

    def distinct(self) -> Select:
        self._distinct = True
        return self

    def build_query(self) -> Tuple[str, List[Any]]:
        query = f"SELECT {'DISTINCT' if self.distinct else ''} {self._select} FROM {self._keyspace}.{self._table}"
        parameters = []
        if self._where:
            predicates = []
            for predicate in self._where:
                predicates.append(f"{predicate._name} {predicate.operator} ?")
                parameters.append(predicate.value)
            query = f"{query} WHERE {' AND '.join(predicates)}"
        if self._group_by:
            query = f"{query} GROUP BY {self._group_by}"
        if self._limit:
            query = f"{query} LIMIT {self._limit}"
        if self._allow_filtering:
            query = f"{query} ALLOW FILTERING"
        return query, parameters


class Update(Query):
    """Update query."""

    @validate_call
    def __init__(self, table: Type[Table]):
        self._table = table
        self._keyspace = table.__keyspace__
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
            [
                (predicate._table == self._table)
                and (predicate._keyspace == self._keyspace)
                for predicate in predicates
            ]
        ), "Columns do not originate from the same table"
        self._where.extend(predicates)
        return self

    def build_query(self) -> Tuple[str, List[Any]]:
        if not self._set_values:
            raise ValueError("No SET in update query!")

        set_values = ", ".join(
            [f"{k._name} = {v}" for k, v in self._set_values.items()]
        )
        query = f"UPDATE {self._keyspace}.{self._table.__tablename__} SET {set_values}"

        parameters = []
        if self._where:
            predicates = []
            for predicate in self._where:
                predicates.append(f"{predicate._name} {predicate.operator} ?")
                parameters.append(predicate.value)
            query = f"{query} WHERE {' AND '.join(predicates)}"
        return query, parameters


class Delete(Query):
    """Update query."""

    @validate_call
    def __init__(self, table: Type[Table]):
        self._table = table
        self._keyspace = table.__keyspace__
        self._where: List[ColumnExpr] = []
        self._if_exists: bool = False

    def if_exists(self) -> Delete:
        self._if_exists = True
        return self

    @validate_call
    def where(self, *predicates: ColumnExpr) -> Delete:
        assert len(predicates) > 0, "where condition cannot be empty!"
        assert all(
            [
                (predicate._table == self._table)
                and (predicate._keyspace == self._keyspace)
                for predicate in predicates
            ]
        ), "Columns do not originate from the same table"
        self._where.extend(predicates)
        return self

    def build_query(self) -> Tuple[str, List[Any]]:
        query = f"DELETE FROM {self._keyspace}.{self._table.__tablename__}"
        parameters = []
        if self._where:
            predicates = []
            for predicate in self._where:
                predicates.append(f"{predicate._name} {predicate.operator} ?")
                parameters.append(predicate.value)
            query = f"{query} WHERE {' AND '.join(predicates)}"
        if self._if_exists:
            query = f"{query} IF EXISTS"
        return query, parameters


class Insert(Query):
    """Insert query.

    Suboptimal way to insert, as the "best way" is to batch insert using directly `scyllaft`.
    """

    @validate_call
    def __init__(self, table: Type[Table]):
        self._table = table
        self._keyspace = table.__keyspace__
        self._values: List[Table] = []

    @validate_call
    def values(self, *values: Table) -> Insert:
        self._values.extend(values)
        return self
