"""Catalog implementation for intake-erddap."""
from typing import Optional, Type

import pandas as pd

from erddapy import ERDDAP
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry

from .erddap import ERDDAPSource
from .version import __version__


class ERDDAPCatalog(Catalog):
    """
    Makes data sources out of all datasets the given ERDDAP service

    This uses erddapy to infer the datasets on the target server.
    Of these, those which have at least one primary key column will become
    ``ERDDAPSourceAutoPartition`` entries in this catalog.
    """

    name = "erddap_cat"
    version = __version__

    def __init__(
        self,
        server: str,
        kwargs_search: Optional[dict] = None,
        erddap_client: Optional[Type[ERDDAP]] = None,
        **kwargs
    ):
        """ERDDAPCatalog initialization

        Arguments
        ---------

            server
        """
        self._erddap_client = erddap_client or ERDDAP
        self.server = server
        self.kwargs_search = kwargs_search
        super(ERDDAPCatalog, self).__init__(**kwargs)

    def _load_df(self):
        e = self.get_client()
        if self.kwargs_search is not None:
            search_url = e.get_search_url(
                response="csv",
                **self.kwargs_search,
                # variableName=variable,
                items_per_page=100000,
            )
            df = pd.read_csv(search_url)
            df.rename(columns={"Dataset ID": "datasetID"}, inplace=True)
            return df

        return e.to_pandas()

    def get_client(self) -> ERDDAP:
        """Return an initialized ERDDAP Client."""
        e = self._erddap_client(self.server)
        e.protocol = "tabledap"
        e.dataset_id = "allDatasets"
        return e

    def _load(self):
        dataidkey = "datasetID"
        e = self.get_client()
        df = self._load_df()

        self._entries = {}

        for index, row in df.iterrows():
            dataset_id = row[dataidkey]
            if dataset_id == "allDatasets":
                continue

            description = "ERDDAP dataset_id %s from %s" % (dataset_id, self.server)
            args = {
                "server": self.server,
                "dataset_id": dataset_id,
                "protocol": "tabledap",
            }

            entry = LocalCatalogEntry(
                dataset_id,
                description,
                "erddap",
                True,
                args,
                {},
                {},
                {},
                "",
                getenv=False,
                getshell=False,
            )
            entry._metadata = {
                "info_url": e.get_info_url(response="csv", dataset_id=dataset_id)
            }
            entry._plugin = [ERDDAPSource]

            self._entries[dataset_id] = entry
