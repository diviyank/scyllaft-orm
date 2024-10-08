"""Define the query builder."""
from typing import Any

from scyllaft import Scylla

from .tables import ScyllaTable


class Query:
    """Generic query expression."""

    def __init__(self, table: ScyllaTable) -> None:
        self._table = table
        self._args = []

    def execute(self, scylla_instance: Scylla) -> Any:
        pass

    def build_query(self) -> str:
        return ""

    def __str__(self):
        return self.build_query()


class Select(Query):
    """Select query from scylla."""


class Update(Query):
    """Update query."""


class Delete(Query):
    """Update query."""


class Insert(Query):
    """Insert query.

    Suboptimal way to insert, as the "best way" is to batch insert using directly `scyllaft`.
    """
