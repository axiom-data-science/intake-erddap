"""Catalog implementation for intake-erddap."""

from logging import getLogger
from typing import Dict, List, Mapping, MutableMapping, Optional, Tuple, Type, Union
from urllib.error import HTTPError

import pandas as pd

from erddapy import ERDDAP
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import utils
from .erddap import GridDAPSource, TableDAPSource
from .utils import match_key_to_category
from .version import __version__


log = getLogger("intake-erddap")


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
        kwargs_search: MutableMapping[str, Union[str, int, float]] = None,
        category_search: Optional[Tuple[str, str]] = None,
        erddap_client: Optional[Type[ERDDAP]] = None,
        use_source_constraints: bool = True,
        protocol: str = "tabledap",
        metadata: dict = None,
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
            * to search using a textual keyword: include `search_for` as either
              a string or a list of strings. Multiple values will be searched
              individually and combined in the final catalog results.
        category_search : list, tuple, optional
            Use this to narrow search by ERDDAP category. The syntax is `(category, key)`, e.g.
            ("standard_name", "temp"). `category` is the ERDDAP category for filtering results. Good
            choices for selecting variables are "standard_name" and "variableName". `key` is the
            custom_criteria key to narrow the search by, which will be matched to the category results
            using the custom_criteria that must be set up or input by the user, with `cf-pandas`.
            Currently only a single key can be matched at a time.
        use_source_constraints : bool, default True
            Any relevant search parameter defined in kwargs_search will be
            passed to the source objects as constraints.
        protocol : str, default "tabledap"
            One of the two supported ERDDAP Data Access Protocols: "griddap", or
            "tabledap". "tabledap" will present tabular datasets using pandas,
            meanwhile "griddap" will use xarray.

        """
        self._erddap_client = erddap_client or ERDDAP
        self._entries: Dict[str, LocalCatalogEntry] = {}
        self._use_source_constraints = use_source_constraints
        self._protocol = protocol
        self._dataset_metadata: Optional[Mapping[str, dict]] = None
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
                    raise ValueError(
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

    def _load_df(self) -> pd.DataFrame:
        frames = []
        for url in self.get_search_urls():
            try:
                df = pd.read_csv(url)
            except HTTPError as e:
                if e.code == 404:
                    log.warning(f"search {url} returned HTTP 404")
                    continue
                raise
            df.rename(columns={"Dataset ID": "datasetID"}, inplace=True)
            frames.append(df)
        result = pd.concat(frames)
        result = result.drop_duplicates("datasetID")
        return result

    def _load_metadata(self) -> Mapping[str, dict]:
        """Returns all of the dataset metadata available from allDatasets API."""
        if self._dataset_metadata is None:
            self._dataset_metadata = utils.get_erddap_metadata(
                self.server, self.kwargs_search
            )
        return self._dataset_metadata

    def get_search_urls(self) -> List[str]:
        """Return the search URLs used in generating the catalog."""
        e = self.get_client()
        urls = []

        # cases:
        # - ks.standard_name is a list
        # - variableName is a list
        # - both are lists
        # Generalize approach: if either are defined, set to list and iterate

        if not any(
            [
                i in self.kwargs_search
                for i in ("standard_name", "variableName", "search_for")
            ]
        ):
            search_url = e.get_search_url(
                response="csv",
                **self.kwargs_search,
                items_per_page=100000,
            )
            return [search_url]

        if "standard_name" in self.kwargs_search:
            urls.extend(
                self._get_standard_name_search_urls(
                    utils.as_a_list(self.kwargs_search["standard_name"])
                )
            )
        if "variableName" in self.kwargs_search:
            urls.extend(
                self._get_variable_name_search_urls(
                    utils.as_a_list(self.kwargs_search["variableName"])
                )
            )
        if "search_for" in self.kwargs_search:
            urls.extend(
                self._get_search_for_search_urls(
                    utils.as_a_list(self.kwargs_search["search_for"])
                )
            )

        return urls

    def _get_standard_name_search_urls(self, standard_names: List[str]) -> List[str]:
        """Return the search urls for each standard_name."""
        e = self.get_client()
        urls = []
        # mypy is annoying sometimes.
        assert isinstance(self.kwargs_search, dict)

        for standard_name in standard_names:
            params = self.kwargs_search.copy()
            params.pop("variableName", None)
            params.pop("search_for", None)
            params["standard_name"] = standard_name

            search_url = e.get_search_url(
                response="csv",
                **params,
                items_per_page=100000,
            )
            urls.append(search_url)
        return urls

    def _get_variable_name_search_urls(self, variable_names: List[str]) -> List[str]:
        """Return the search urls for each variable name."""
        e = self.get_client()
        urls = []
        # mypy is annoying sometimes.
        assert isinstance(self.kwargs_search, dict)

        for variable_name in variable_names:
            params = self.kwargs_search.copy()
            params.pop("standard_name", None)
            params.pop("search_for", None)
            params["variableName"] = variable_name

            search_url = e.get_search_url(
                response="csv",
                **params,
                items_per_page=100000,
            )
            urls.append(search_url)
        return urls

    def _get_search_for_search_urls(self, search_for: List[str]) -> List[str]:
        """Return the search urls for each search query."""
        e = self.get_client()
        urls = []
        # mypy is annoying sometimes.
        assert isinstance(self.kwargs_search, dict)

        for query in search_for:
            params = self.kwargs_search.copy()
            params.pop("standard_name", None)
            params.pop("variableName", None)
            params.pop("search_for", None)

            search_url = e.get_search_url(
                response="csv",
                search_for=query,
                **params,
                items_per_page=100000,
            )
            urls.append(search_url)
        return urls

    def get_client(self) -> ERDDAP:
        """Return an initialized ERDDAP Client."""
        e = self._erddap_client(self.server)
        e.protocol = self._protocol
        e.dataset_id = "allDatasets"
        return e

    def _load(self):
        dataidkey = "datasetID"
        e = self.get_client()
        df = self._load_df()
        all_metadata = self._load_metadata()

        self._entries = {}

        for index, row in df.iterrows():
            dataset_id = row[dataidkey]
            if dataset_id == "allDatasets":
                continue

            description = "ERDDAP dataset_id %s from %s" % (dataset_id, self.server)
            args = {
                "server": self.server,
                "dataset_id": dataset_id,
                "protocol": self._protocol,
                "constraints": {},
            }
            if self._protocol == "tabledap":
                args["constraints"].update(self._get_tabledap_constraints())

            metadata = all_metadata.get(dataset_id, {})

            entry = LocalCatalogEntry(
                name=dataset_id,
                description=description,
                driver=self._protocol,
                args=args,
                metadata=metadata,
                getenv=False,
                getshell=False,
            )
            if self._protocol == "tabledap":
                entry._metadata["info_url"] = e.get_info_url(
                    response="csv", dataset_id=dataset_id
                )
                entry._plugin = [TableDAPSource]
            elif self._protocol == "griddap":
                entry._plugin = [GridDAPSource]
            else:
                raise ValueError(f"Unsupported protocol: {self._protocol}")

            self._entries[dataset_id] = entry

    def _get_tabledap_constraints(self) -> Dict[str, Union[str, int, float]]:
        """Return the constraints dictionary for a tabledap source."""
        result = {}
        if self._use_source_constraints and "min_time" in self.kwargs_search:
            result["time>="] = self.kwargs_search["min_time"]
        if self._use_source_constraints and "max_time" in self.kwargs_search:
            result["time<="] = self.kwargs_search["max_time"]
        return result
