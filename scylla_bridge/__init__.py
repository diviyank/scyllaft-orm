"""Top level init Module."""

import os

from .column import Column
from .metaclass import ApplicationEnvironment
from .metadata import MetaData
from .query import Delete, Insert, Query, Select, Update
from .table import Table
from .types import ScyllaType

# NOTE: This might be heavy on startup time
app_env = os.environ.get("APPLICATION_ENV")
if app_env:
    ApplicationEnvironment().set_environment(app_env)


__all__ = [
    "ApplicationEnvironment",
    "MetaData",
    "Table",
    "Column",
    "Query",
    "Select",
    "Update",
    "Insert",
    "Delete",
    "ScyllaType",
]

__version__ = "1.1.0"
