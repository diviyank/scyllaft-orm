"""Metaclass for tables."""

from __future__ import annotations

from loguru import logger

from .column import Column


class SingletonMeta(type):
    """The Singleton class can be implemented in different ways in Python.

    Some possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        """Check the existence of a class.

        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class ApplicationEnvironment(metaclass=SingletonMeta):
    """Class defining the ENV of the application."""

    modified: bool = False
    env: str = "dev"

    def set_environment(self, env: str) -> None:
        """Set the application env."""
        self.modified = True
        self.env = env

    def get_environment(self) -> str:
        """Get the application env."""
        if not self.modified:
            logger.warning(
                "Using the default environment : {env}, as the application env has not been set",
                env=self.env,
            )

        return self.env


app_env = ApplicationEnvironment()


class ScyllaMetaClass(type):
    """Meta-class that allows the linking of columns and tables."""

    def __init__(cls, name, bases, clsdict):
        """Init the meta-class."""
        if len(cls.mro()) > 2:
            clsdict = trigger_linking_routine(clsdict)
            super().__init__(name, bases, clsdict)
            setattr(cls, "fields", clsdict["fields"])
            setattr(cls, "__keyspace__", clsdict["__keyspace__"])
        else:
            super().__init__(name, bases, clsdict)


def trigger_linking_routine(clsdict: dict) -> dict:
    """Link all the columns to the Table."""
    fields = {}
    for key, value in clsdict.items():
        if isinstance(value, Column):
            value = value.set_attributes(
                {
                    "_name": key,
                    "_table": clsdict["__tablename__"],
                    "_keyspace": f"{clsdict['__keyspace__']}_{app_env.get_environment()}",
                }
            )
            clsdict[key] = value
            fields[key] = value
    clsdict["__keyspace__"] = f"{clsdict['__keyspace__']}_{app_env.get_environment()}"
    clsdict["fields"] = fields

    return clsdict
