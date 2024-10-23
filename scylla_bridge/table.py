"""Module defining the Table class."""

from __future__ import annotations

from copy import deepcopy
from enum import Enum
from typing import Dict, List, Optional, Type

from pydantic.dataclasses import dataclass

from .column import Column
from .metaclass import ScyllaMetaClass
from .types import SCYLLA_TO_REDIS_MAP


@dataclass
class Table(metaclass=ScyllaMetaClass):
    """Base table class for Scylla.

    Define attributes in a SQLAlchemy manner
    """

    __keyspace__: str = ""
    __tablename__: str = ""
    __index__: Optional[List[str]] = None
    __base_level__: str = ""
    __materialized_views__: Optional[Dict[str | Enum, List[str]]] = None
    fields: Optional[Dict[str, Column]] = None

    @classmethod
    def to_redis_schema(cls: Type[Table]) -> Dict[str, List[str]]:
        """Map the class to a redis-schema."""
        mappings = {
            field_name: SCYLLA_TO_REDIS_MAP[col.type]
            for field_name, col in cls.fields.items()
        }
        values = set(mappings.values())
        schema = {
            v: [key for key, value in mappings.items() if value == v] for v in values
        }

        return schema

    @classmethod
    def get_view(cls: Type[Table], param: str) -> Type[Table]:
        """Get access to a particular view like a table.

        Parameters
        ----------
        param: str
            Key of the view.
        """
        if param == cls.__base_level__:
            return cls
        if param not in cls.__materialized_views__.keys():
            raise ValueError(
                f"Table {cls.__tablename__} does not have a view associated with {param}."
            )
        view = deepcopy(cls)
        view.__tablename__ = f"{cls.__tablename__}_{param}"
        view.__index__ = cls.__materialized_views__[param]
        return view
