"""intake-erddap package."""
import intake

from .erddap import GridDAPSource, TableDAPSource
from .erddap_cat import ERDDAPCatalogReader
from .version import __version__


__all__ = [
    "ERDDAPCatalogReader",
    "TableDAPSource",
    "GridDAPSource",
    "__version__",
]
