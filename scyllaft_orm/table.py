from __future__ import annotations

from copy import deepcopy
from enum import Enum
from typing import Dict, List

from pydantic import BaseModel

from .column import Column
from .types import SCYLLA_TO_REDIS_MAP


class Table(BaseModel):
    """
    TODO: take inspiration from SQLAlchemy

    """

    _keyspace: str
    _table_name: str
    _materialized_views: Dict[str | Enum, List[str]] = {}

    def to_redis_schema(self) -> Dict[str, List[str]]:
        mappings = {
            field.name: SCYLLA_TO_REDIS_MAP[field.type]
            for field in self.model_fields
            if isinstance(field, Column)
        }
        values = set(mappings.values())
        schema = {
            v: [key for key, value in mappings if mappings[key] == v] for v in values
        }

        return schema

    def get_view(self, param: str) -> ScyllaTable:

        if param not in self._materialized_views.keys():
            raise ValueError(
                f"Table {self._table_name} does not have a view associated with {param}."
            )
        view = deepcopy(self)
        view._table_name = f"{self._table_name}_{param}"
        return view
