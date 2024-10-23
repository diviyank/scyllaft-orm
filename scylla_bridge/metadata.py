"""General tooling to create table and views."""

import inspect
import sys
from typing import List, Type

from scyllaft import Scylla

from .table import Table


class MetaData:
    """MetaData class to create all views and tables."""

    async def create_all(self, scylla: Scylla) -> None:
        """Check if all tables/view if they exist and create them otherwise."""
        all_tables = inspect.getmembers(
            sys.modules["__main__"],
            lambda x: inspect.isclass(x) and issubclass(x, Table) and x != Table,
        )
        all_stmt = []
        for _, table in all_tables:
            all_stmt.extend(self._create_table(table))
        for stmt in all_stmt:
            await scylla.execute(stmt)

    def _create_table(self, table: Type[Table]) -> List[str]:
        """Create a table following its definition."""
        assert table.__index__ is not None, "Table has no index defined!"
        assert table.fields is not None, "Table has no fields defined!"
        columns = ", ".join(
            [f"{name} {col.type.value}" for name, col in table.fields.items()]
        )
        index = ", ".join(table.__index__)
        stmt = [
            f"CREATE TABLE IF NOT EXISTS {table.__keyspace__}.{table.__tablename__} ({columns}, PRIMARY KEY ({index}))"
        ]

        if table.__materialized_views__ is not None:
            for key, index_view in table.__materialized_views__.items():
                stmt.append(
                    self._create_view(
                        f"{table.__keyspace__}.{table.__tablename__}",
                        f"{table.__keyspace__}.{table.__tablename__}_{key}",
                        index_view,
                    )
                )

        return stmt

    def _create_view(self, base_table: str, name: str, index: List[str]) -> str:
        """Create a CQL Materialized view."""
        index_ = ", ".join(index)
        where_clause = " AND ".join([f"{k} IS NOT NULL" for k in index])
        return f"CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS SELECT * FROM {base_table} WHERE {where_clause} PRIMARY KEY ({index_})"
