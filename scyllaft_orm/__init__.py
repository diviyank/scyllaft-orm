import os

from .metadata import ApplicationEnvironment, MetaData
from .query_builder import Delete, Insert, Query, Select, Update
from .table import ScyllaTable

# NOTE: This might be heavy on startup time
app_env = os.environ.get("APPLICATION_ENV")
if app_env:
    ApplicationEnvironment().set_environment(app_env)


__all__ = [
    "ApplicationEnvironment",
    "MetaData",
    "ScyllaTable",
    "Query",
    "Select",
    "Update",
    "Insert",
    "Delete",
]
