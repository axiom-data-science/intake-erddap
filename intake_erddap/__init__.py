from pkg_resources import DistributionNotFound, get_distribution

from .erddap import ERDDAPSource, ERDDAPSourceAutoPartition, ERDDAPSourceManualPartition
from .erddap_cat import ERDDAPCatalog
from .version import __version__


__all__ = [
    "ERDDAPSource",
    "ERDDAPSourceAutoPartition",
    "ERDDAPSourceManualPartition",
    "ERDDAPCatalog",
    "__version__",
]
