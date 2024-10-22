"""Define specific types and mappings to Scylla."""

from enum import StrEnum
from typing import Dict


class ScyllaType(StrEnum):
    """Enum defining all the ScyllaDB Types."""

    BIGINT = "BIGINT"
    BLOB = "BLOB"
    BOOLEAN = "BOOLEAN"
    COUNTER = "COUNTER"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    DOUBLE = "DOUBLE"
    DURATION = "DURATION"
    FLOAT = "FLOAT"
    INET = "INET"
    INT = "INT"
    SMALLINT = "SMALLINT"
    TEXT = "TEXT"
    TIME = "TIME"
    TIMEUUID = "TIMEUUID"
    TINYINT = "TINYINT"
    UUID = "UUID"
    VARCHAR = "VARCHAR"
    VARINT = "VARINT"
    TIMESTAMP = "TIMESTAMP"


SCYLLA_TO_REDIS_MAP: Dict[ScyllaType, str] = {
    ScyllaType.BIGINT: "numeric",
    ScyllaType.BLOB: "text",
    ScyllaType.BOOLEAN: "numeric",
    ScyllaType.COUNTER: "numeric",
    ScyllaType.DATE: "text",
    ScyllaType.DECIMAL: "numeric",
    ScyllaType.DOUBLE: "numeric",
    ScyllaType.DURATION: "text",
    ScyllaType.FLOAT: "numeric",
    ScyllaType.INET: "text",
    ScyllaType.INT: "numeric",
    ScyllaType.SMALLINT: "numeric",
    ScyllaType.TEXT: "text",
    ScyllaType.TIME: "text",
    ScyllaType.TIMEUUID: "text",
    ScyllaType.TINYINT: "numeric",
    ScyllaType.UUID: "text",
    ScyllaType.VARCHAR: "text",
    ScyllaType.VARINT: "numeric",
    ScyllaType.TIMESTAMP: "text",
}
