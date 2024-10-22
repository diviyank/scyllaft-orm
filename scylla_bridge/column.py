"""Columns and ColumnsExpr."""

from __future__ import annotations

from typing import Any, ForwardRef, Iterable, Optional

from pydantic import validate_call
from pydantic.dataclasses import dataclass

from .types import ScyllaType

AggregateExpr = ForwardRef("AggregateExpr")  # type:ignore
Column = ForwardRef("Column")  # type:ignore


@dataclass
class ColumnExpr:
    """Represents a column expression in a ScyllaDB query.

    Parameters
    ----------
    _table : str
        The name of the table.
    _name : str
        The name of the column.
    _keyspace : str
        The name of the keyspace.
    operator : str
        The comparison operator used in the expression (e.g., '=', '<', '>', etc.).
    value : Any
        The value against which the column is being compared.
    """

    _table: str
    _name: str
    _keyspace: str
    operator: str
    value: Any

    def __and__(self, _):
        """Raise an error for Expr combination."""
        raise NotImplementedError(
            "ScyllaFT-ORM does not accept (yet) '&' operations, give them as args without operator"
        )


@dataclass
class AggregateExpr:
    """Represents an aggregate column expression in a ScyllaDB query.

    Parameters
    ----------
    _table : str
        The name of the table.
    _name : str
        The name of the column.
    _keyspace : str
        The name of the keyspace.
    operator : str
        The aggregate function applied to the column (e.g., 'SUM', 'AVG').
    rename : Optional[str], optional
        An alias for the column, by default None.
    """

    _table: str
    _name: str
    _keyspace: str
    operator: str
    rename: Optional[str] = None

    @validate_call
    def label(self, name: str) -> AggregateExpr:
        """Assigns an alias to the aggregate expression.

        Parameters
        ----------
        name : str
            The new alias for the column.

        Returns
        -------
        AggregateExpr
            The updated aggregate expression with the new alias.
        """
        self.rename = name
        return self


@dataclass
class Column:
    """Represents a column in a ScyllaDB query.

    Parameters
    ----------
    _expr : Optional[ColumnExpr], optional
        The column expression associated with the column, by default None.
    _table : Optional[str], optional
        The name of the table, by default None.
    _keyspace : Optional[str], optional
        The name of the keyspace, by default None.
    _name : Optional[str], optional
        The name of the column, by default None.
    rename : Optional[str], optional
        An alias for the column, by default None.
    """

    type: ScyllaType
    _expr: Optional[ColumnExpr] = None
    _table: Optional[str] = None
    _keyspace: Optional[str] = None
    _name: Optional[str] = None
    rename: Optional[str] = None

    def set_attributes(self, settings: dict) -> Column:
        """Sets multiple attributes of the column.

        Parameters
        ----------
        settings : dict
            A dictionary where the keys are attribute names and the values are the values to set.

        Returns
        -------
        Column
            The updated column with new attributes.
        """
        for key, value in settings.items():
            setattr(self, key, value)
        return self

    def _validate_value(self, value: object) -> None:
        """Validates the provided value for the column.

        Parameters
        ----------
        value : object
            The value to validate.
        """

    def __eq__(self, value: object) -> ColumnExpr:  # type:ignore
        """Creates a column expression for equality comparison.

        Parameters
        ----------
        value : object
            The value to compare against the column.

        Returns
        -------
        ColumnExpr
            The column expression representing the equality comparison.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return ColumnExpr(self._table, self._name, self._keyspace, "=", value)

    def __ne__(self, value: object, /) -> ColumnExpr:  # type:ignore
        """Raises an exception as inequality comparison is not supported in ScyllaDB.

        Parameters
        ----------
        value : object
            The value to compare against the column.

        Raises
        ------
        NotImplementedError
            ScyllaDB does not support inequality comparison.
        """
        raise NotImplementedError("ScyllaDB does not support inequality")

    def __lt__(self, value: object) -> ColumnExpr:
        """Creates a column expression for less-than comparison.

        Parameters
        ----------
        value : object
            The value to compare against the column.

        Returns
        -------
        ColumnExpr
            The column expression representing the less-than comparison.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return ColumnExpr(self._table, self._name, self._keyspace, "<", value)

    def __le__(self, value: object) -> ColumnExpr:
        """Creates a column expression for less-than-or-equal-to comparison.

        Parameters
        ----------
        value : object
            The value to compare against the column.

        Returns
        -------
        ColumnExpr
            The column expression representing the less-than-or-equal-to comparison.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return ColumnExpr(self._table, self._name, self._keyspace, "<=", value)

    def __gt__(self, value: object) -> ColumnExpr:
        """Creates a column expression for greater-than comparison.

        Parameters
        ----------
        value : object
            The value to compare against the column.

        Returns
        -------
        ColumnExpr
            The column expression representing the greater-than comparison.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return ColumnExpr(self._table, self._name, self._keyspace, ">", value)

    def __ge__(self, value: object) -> ColumnExpr:
        """Creates a column expression for greater-than-or-equal comparison.

        Parameters
        ----------
        value : object
            The value to compare against the column.

        Returns
        -------
        ColumnExpr
            The column expression representing the greater-than comparison.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return ColumnExpr(self._table, self._name, self._keyspace, ">=", value)

    @validate_call
    def in_(self, value: Iterable) -> ColumnExpr:
        """Creates a column expression for an "IN" comparison.

        Parameters
        ----------
        value : Iterable
            An iterable containing the values to compare against the column.

        Returns
        -------
        ColumnExpr
            The column expression representing the "IN" comparison.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return ColumnExpr(self._table, self._name, self._keyspace, " IN ", value)

    @validate_call
    def label(self, name: str) -> Column:
        """Assigns an alias to the column.

        Parameters
        ----------
        name : str
            The new alias for the column.

        Returns
        -------
        Column
            The updated column with the new alias.
        """
        self.rename = name
        return self

    def sum(self) -> AggregateExpr:
        """Creates an aggregate expression for the SUM function.

        Returns
        -------
        AggregateExpr
            The aggregate expression representing the SUM function.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return AggregateExpr(
            self._table, self._name, self._keyspace, "SUM", self.rename
        )

    def mean(self) -> AggregateExpr:
        """Creates an aggregate expression for the AVG (mean) function.

        Returns
        -------
        AggregateExpr
            The aggregate expression representing the AVG function.
        """
        assert (
            self._name is not None
            and self._table is not None
            and self._keyspace is not None
        ), "Column name is not set!"
        return AggregateExpr(
            self._table, self._name, self._keyspace, "AVG", self.rename
        )
