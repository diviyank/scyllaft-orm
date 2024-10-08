"""General tooling to create table and views."""

from loguru import logger
from scyllaft import Scylla


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class ApplicationEnvironment(metaclass=SingletonMeta):

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


class MetaData:
    """MetaData class to create all views and tables."""

    def create_all(self, scylla: Scylla) -> None:
        """Check if all tables/view if they exist and create them otherwise."""

    def _create_table(self, scylla: Scylla) -> None:
        """Create a table following its definition."""

    def _create_view(self, scylla: Scylla) -> None:
        """Create a CQL Materialized view."""
