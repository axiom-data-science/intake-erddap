"""Catalog implementation for intake-erddap."""

from typing import Dict, List, Optional, Type, Union

import pandas as pd

from erddapy import ERDDAP
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry

from .erddap import ERDDAPSource
from .utils import match_key_to_category
from .version import __version__


class ERDDAPCatalog(Catalog):
    """
    Makes data sources out of all datasets the given ERDDAP service

    This uses erddapy to infer the datasets on the target server.
    Of these, those which have at least one primary key column will become
    ``ERDDAPSourceAutoPartition`` entries in this catalog.

    Attributes
    ----------
    search_url : str
        If a search is performed on the ERDDAP server, the search url is saved as an attribute.
    """

    name = "erddap_cat"
    version = __version__

    def __init__(
        self,
        server: str,
        kwargs_search: Optional[Dict[str, Union[str, int, float]]] = None,
        category_search: Optional[List[str]] = None,
        erddap_client: Optional[Type[ERDDAP]] = None,
        **kwargs,
    ):
        """ERDDAPCatalog initialization

        Parameters
        ----------
        server : str
            ERDDAP server address, for example: "http://erddap.sensors.ioos.us/erddap"
        kwargs_search : dict, optional
            Keyword arguments to input to search on the server before making the catalog. Options are:
            * to search by bounding box: include all of min_lon, max_lon, min_lat, max_lat: (int, float)
              Longitudes must be between -180 to +180.
            * to search within a datetime range: include both of min_time, max_time: interpretable
              datetime string, e.g., "2021-1-1"
        category_search : list, optional
            Use this to narrow search by ERDDAP category. The syntax is `[category, key]`, e.g.
            ["standard_name": "temp"]. `category` is the ERDDAP category for filtering results. Good
            choices for selecting variables are "standard_name" and "variableName". `key` is the
            custom_criteria key to narrow the search by, which will be matched to the category results
            using the custom_criteria that must be set up or input by the user, with `cf-pandas`.
            Currently only a single key can be matched at a time.
        """
        self._erddap_client = erddap_client or ERDDAP
        self.server = server
        self.search_url = None

        if kwargs_search is not None:
            checks = [
                ["min_lon", "max_lon", "min_lat", "max_lat"],
                ["min_time", "max_time"],
            ]
            for check in checks:
                if any(key in kwargs_search for key in check) and not all(
                    key in kwargs_search for key in check
                ):
                    raise KeyError(
                        f"If any of {check} are input, they all must be input."
                    )
        else:
            kwargs_search = {}
        self.kwargs_search = kwargs_search

        if category_search is not None:
            category, key = category_search
            # Currently just take first match, but there could be more than one.
            self.kwargs_search[category] = match_key_to_category(
                self.server, key, category
            )[0]

        super(ERDDAPCatalog, self).__init__(**kwargs)

    def _load_df(self):
        e = self.get_client()
        if self.kwargs_search is not None:
            search_url = e.get_search_url(
                response="csv",
                **self.kwargs_search,
                items_per_page=100000,
            )
            self.search_url = search_url
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
