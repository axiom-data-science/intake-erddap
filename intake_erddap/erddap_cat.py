
from . import __version__
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry
from erddapy import ERDDAP


class ERDDAPCatalog(Catalog):
    """
    Makes data sources out of all datasets the given ERDDAP service

    This uses erddapy to infer the datasets on the target server.
    Of these, those which have at least one primary key column will become
    ``ERDDAPSourceAutoPartition`` entries in this catalog.
    """
    name = 'erddap_cat'
    version = __version__

    def __init__(self, server, kwargs_search=None, **kwargs):
        self.server = server
        self.kwargs_search = kwargs_search
        super(ERDDAPCatalog, self).__init__(**kwargs)

    def _load(self):

        from intake_erddap import ERDDAPSource, ERDDAPSourceAutoPartition

        e = ERDDAP(self.server)
        e.protocol = 'tabledap'
        e.dataset_id = 'allDatasets'

        if self.kwargs_search is not None:
            search_url = e.get_search_url(
                response="csv",
                **self.kwargs_search,
                # variableName=variable,
                items_per_page=100000,
            )
            import pandas as pd
            df = pd.read_csv(search_url)
            dataidkey = 'Dataset ID'

        else:
            df = e.to_pandas()
            dataidkey = 'datasetID'

        self._entries = {}

        for index, row in df.iterrows():
            dataset_id = row[dataidkey]
            if dataset_id == 'allDatasets':
                continue

            description = 'ERDDAP dataset_id %s from %s' % (dataset_id, self.server)
            args = {'server': self.server,
                    'dataset_id': dataset_id,
                    'protocol': 'tabledap', 
                    }

            if False: # if we can use AutoPartition
                entry = LocalCatalogEntry(dataset_id, description, 'erddap_auto', True,
                                        args, {}, {}, {}, "", getenv=False,
                                        getshell=False)
                entry._metadata = {'info_url': e.get_info_url(response="csv", dataset_id=dataset_id)}
                entry._plugin = [ERDDAPSourceAutoPartition]
            else: # if we can't use AutoPartition
                entry = LocalCatalogEntry(dataset_id, description, 'erddap', True,
                                      args, {}, {}, {}, "", getenv=False,
                                      getshell=False)
                entry._metadata = {'info_url': e.get_info_url(response="csv", dataset_id=dataset_id)}
                entry._plugin = [ERDDAPSource]
            
            self._entries[dataset_id] = entry
