
import intake
from .erddap import (ERDDAPSource, ERDDAPSourceAutoPartition,
                         ERDDAPSourceManualPartition)
from .erddap_cat import ERDDAPCatalog


from pkg_resources import DistributionNotFound, get_distribution

try:
    __version__ = get_distribution("model_catalogs").version
except DistributionNotFound:
    # package is not installed
    __version__ = "unknown"
