"""Query builder for scylla."""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import Any, Dict, ForwardRef, List, Optional, Tuple, Type

from loguru import logger
from pydantic import ConfigDict, validate_call
from scyllaft import Scylla

from .column import AggregateExpr, Column, ColumnExpr
from .table import Table

Select = ForwardRef("Select")  # type:ignore
Insert = ForwardRef("Insert")  # type:ignore
Update = ForwardRef("Update")  # type:ignore
Delete = ForwardRef("Delete")  # type:ignore


class Query(ABC):
    """Generic query expression."""

    async def execute(self, scylla_instance: Scylla) -> Any:
        """Executes the query using a Scylla instance.

        Parameters
        ----------
        scylla_instance : Scylla
            The Scylla instance used to execute the query.

        Returns
        -------
        Any
            The result of the executed query.
        """
        query, parameters = self.build_query()
        result = await scylla_instance.execute(query, parameters)
        return result

    @abstractmethod
    def build_query(self) -> Tuple[str, List[Any]]:
        """Builds the query into a string and its parameters as a list.

        Returns
        -------
        Tuple[str, List[Any]]
            The query string and the corresponding parameters.
        """
        raise NotImplementedError

    def __str__(self):
        """Returns the query string for printing purposes.

        Returns
        -------
        str
            The query string.
        """
        return self.build_query()[0]


class Select(Query):
    """Select query from Scylla."""

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def __init__(self, *columns: Column | AggregateExpr | Type[Table]) -> None:
        """Initializes the Select query.

        Parameters
        ----------
        *columns : Column or AggregateExpr or Type[Table]
            The columns to be selected in the query.
        """
        assert len(columns) > 0, "Select expression cannot be empty!"

        if inspect.isclass(columns[0]) and issubclass(columns[0], Table):
            self._table = columns[0].__tablename__
            self._keyspace = columns[0].__keyspace__
            self._select = "*"
            self._columns: List[Column | AggregateExpr] = []
        else:
            self._table: str = columns[0]._table  # type:ignore
            self._keyspace: str = columns[0]._keyspace  # type:ignore
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
        """Enables the 'ALLOW FILTERING' option in the query.

        Returns
        -------
        Select
            The updated Select query with 'ALLOW FILTERING' enabled.
        """
        logger.warning(
            "Allow filtering usually leads to degraded performance. Consider reviewing your query."
        )
        self._allow_filtering = True
        return self

    @validate_call
    def where(self, *predicates: ColumnExpr) -> Select:
        """Adds WHERE conditions to the query.

        Parameters
        ----------
        *predicates : ColumnExpr
            The column expressions to be used in the WHERE clause.

        Returns
        -------
        Select
            The updated Select query with WHERE conditions.
        """
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
        """Adds GROUP BY conditions to the query.

        Parameters
        ----------
        *columns : Column
            The columns to group by in the query.

        Returns
        -------
        Select
            The updated Select query with GROUP BY conditions.
        """
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
        """Sets a limit on the number of rows returned by the query.

        Parameters
        ----------
        _limit : int
            The maximum number of rows to return.

        Returns
        -------
        Select
            The updated Select query with a limit.
        """
        assert _limit > 0, "Limit cannot be null nor negative"
        self._limit = _limit
        return self

    def distinct(self) -> Select:
        """Enables the DISTINCT option in the query.

        Returns
        -------
        Select
            The updated Select query with DISTINCT enabled.
        """
        self._distinct = True
        return self

    def build_query(self) -> Tuple[str, List[Any]]:
        """Builds the SELECT query into a string and its parameters.

        Returns
        -------
        Tuple[str, List[Any]]
            The query string and the corresponding parameters.
        """
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

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def __init__(self, table: Type[Table]):
        """Initializes the Update query.

        Parameters
        ----------
        table : Type[Table]
            The table to be updated.
        """
        self._table = table
        self._keyspace = table.__keyspace__
        self._where: List[ColumnExpr] = []
        self._set_values: Dict[Column, Any] = {}

    @validate_call
    def set(self, column: Column, value: Any) -> Update:
        """Sets the values to be updated in the query.

        Parameters
        ----------
        column : Column
            The column to update.
        value : Any
            The new value for the column.

        Returns
        -------
        Update
            The updated Update query with the new SET values.
        """
        self._set_values[column] = value
        return self

    @validate_call
    def where(self, *predicates: ColumnExpr) -> Update:
        """Adds WHERE conditions to the update query.

        Parameters
        ----------
        *predicates : ColumnExpr
            The column expressions to be used in the WHERE clause.

        Returns
        -------
        Update
            The updated Update query with WHERE conditions.
        """
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
        """Builds the UPDATE query into a string and its parameters.

        Returns
        -------
        Tuple[str, List[Any]]
            The query string and the corresponding parameters.
        """
        if not self._set_values:
            raise ValueError("No SET in update query!")

        set_keys, parameters = [], []

        for key, value in self._set_values.items():

            set_keys.append(f"{key._name} = ?")
            parameters.append(value)

        set_values = ", ".join(set_keys)
        query = f"UPDATE {self._keyspace}.{self._table.__tablename__} SET {set_values}"

        if self._where:
            predicates = []
            for predicate in self._where:
                predicates.append(f"{predicate._name} {predicate.operator} ?")
                parameters.append(predicate.value)
            query = f"{query} WHERE {' AND '.join(predicates)}"
        return query, parameters


class Delete(Query):
    """Delete query."""

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def __init__(self, table: Type[Table]):
        """Initializes the Delete query.

        Parameters
        ----------
        table : Type[Table]
            The table from which records are to be deleted.
        """
        self._table = table
        self._keyspace = table.__keyspace__
        self._where: List[ColumnExpr] = []
        self._if_exists: bool = False

    def if_exists(self) -> Delete:
        """Adds the IF EXISTS clause to the query.

        Returns
        -------
        Delete
            The updated Delete query with IF EXISTS enabled.
        """
        self._if_exists = True
        return self

    @validate_call
    def where(self, *predicates: ColumnExpr) -> Delete:
        """Adds WHERE conditions to the delete query.

        Parameters
        ----------
        *predicates : ColumnExpr
            The column expressions to be used in the WHERE clause.

        Returns
        -------
        Delete
            The updated Delete query with WHERE conditions.
        """
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
        """Builds the DELETE query into a string and its parameters.

        Returns
        -------
        Tuple[str, List[Any]]
            The query string and the corresponding parameters.
        """
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

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def __init__(self, table: Type[Table]):
        """Initializes the Insert query.

        Parameters
        ----------
        table : Type[Table]
            The table into which records are to be inserted.
        """
        self._table = table
        self._keyspace = table.__keyspace__
        self._values: List[dict] = []

    @validate_call()
    def values(self, *values: dict) -> Insert:
        """Specifies the values to be inserted.

        Parameters
        ----------
        *values : Table
            The values to be inserted into the table.

        Returns
        -------
        Insert
            The updated Insert query with values.
        """
        self._values.extend(values)
        return self

    def build_query(self) -> List[Tuple[str, List[Any]]]:  # type:ignore
        """Builds the DELETE query into a string and its parameters.

        Returns
        -------
        Tuple[str, List[Any]]
            The query string and the corresponding parameters.
        """
        query = f"INSERT INTO {self._keyspace}.{self._table.__tablename__}"
        queries = []
        for stmt in self._values:
            c, v = zip(*stmt.items())
            queries.append(
                (f"{query} ({', '.join(c)}) VALUES ({', '.join(['?' for i in c])})", v)
            )
        if not queries:
            raise ValueError("No data to insert!")
        return queries

    async def execute(self, scylla_instance: Scylla) -> Any:
        """Executes the query using a Scylla instance.

        Parameters
        ----------
        scylla_instance : Scylla
            The Scylla instance used to execute the query.

        Returns
        -------
        Any
            The result of the executed query.
        """
        queries = self.build_query()
        result = []
        for query, parameters in list(queries):
            result.append(await scylla_instance.execute(query, parameters))
        return result
