"""intake-erddap package."""
import intake  # noqa: F401

from .erddap import GridDAPReader, TableDAPReader
from .erddap_cat import ERDDAPCatalogReader
from .version import __version__


__all__ = [
    "ERDDAPCatalogReader",
    "TableDAPReader",
    "GridDAPReader",
    "__version__",
]
