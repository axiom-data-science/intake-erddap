
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import intake
from .erddap import (ERDDAPSource, ERDDAPSourceAutoPartition,
                         ERDDAPSourceManualPartition)
from .erddap_cat import ERDDAPCatalog
