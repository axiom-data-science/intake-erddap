"""Catalog implementation for intake-erddap."""

from copy import deepcopy
from datetime import datetime
from logging import getLogger
from typing import (
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
from urllib.error import HTTPError

import pandas as pd
import requests

from erddapy import ERDDAP
from intake.readers.entry import Catalog, DataDescription
from intake.readers.readers import BaseReader

from intake_erddap.cache import CacheStore

from . import utils
from .utils import match_key_to_category


log = getLogger("intake-erddap")


class ERDDAPCatalogReader(BaseReader):
    """
    Makes data sources out of all datasets the given ERDDAP service


    Parameters
    ----------
    server : str
        URL to the ERDDAP service. Example: ``"https://coastwatch.pfeg.noaa.gov/erddap"``

        Note
        ----
        Do not include a trailing slash.
    bbox : tuple of 4 floats, optional
        For explicit geographic search queries, pass a tuple of four floats
        in the `bbox` argument. The bounding box parameters are `(min_lon,
        min_lat, max_lon, max_lat)`.
    standard_names : list of str, optional
        For explicit search queries for datasets containing a given
        standard_name use this argument. Example: `["air_temperature",
        "air_pressure"]`.
    variable_names: list of str, optional
        For explicit search queries for datasets containing a variable with
        a given name. This can be useful when the client knows of a
        particular variable name or a convention applied where there is no
        CF standard name.
    start_time : datetime, optional
        For explicit search queries for datasets that contain data after
        `start_time`.
    end_time : datetime, optional
        For explicit search queries for datasets that contain data before
        `end_time`.
    search_for : list of str, optional
        For explicit search queries for datasets that any contain of the terms
        specified in this keyword argument.
    kwargs_search : dict, optional
        Keyword arguments to input to search on the server before making the catalog. Options are:

        - to search by bounding box: include all of min_lon, max_lon, min_lat, max_lat: (int, float)
            Longitudes must be between -180 to +180.
        - to search within a datetime range: include both of min_time, max_time: interpretable
            datetime string, e.g., "2021-1-1"
        - to search using a textual keyword: include `search_for` as either
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
    chunks : dict, optional
        For griddap protocol, pass a dictionary of chunk sizes for the xarray.
    xarray_kwargs : dict, optional
        For griddap protocol, pass a dictionary of kwargs to pass to the
        xarray.open_dataset method.
    metadata : dict, optional
        Extra metadata for the intake catalog.
    variables : list of str, optional
        List of variables to limit the dataset to, if available. If you're not
        sure what variables are available, check info_url for the station, or
        look up the dataset on the ERDDAP server.
    query_type : str, default "union"
        Specifies how the catalog should apply the query parameters. Choices are
        ``"union"`` or ``"intersection"``. If the ``query_type`` is set to
        ``"intersection"``, then the set of results will be the intersection of
        each individual query made to ERDDAP. This is equivalent to a logical
        AND of the results. If the value is ``"union"`` then the results will be
        the union of each resulting dataset. This is equivalent to a logical OR.
    open_kwargs : dict, optional
        Keyword arguments to pass to the `open` method of the ERDDAP Reader,
        e.g. pandas read_csv. Response is an optional keyword argument that will
        be used by ERDDAPY to determine the response format. Default is "csvp" and
        for TableDAP Readers, "csv" and "csv0" are reasonable choices too.
    mask_failed_qartod : bool, False
        WARNING ALPHA FEATURE. If True and `*_qc_agg` columns associated with
        data columns are available, data values associated with QARTOD flags
        other than 1 and 2 will be nan'ed out. Has not been thoroughly tested.
    dropna : bool, False.
        WARNING ALPHA FEATURE. If True, rows with data columns of nans will be
        dropped from data frame. Has not been thoroughly tested.
    cache_kwargs : dict, optional
        WARNING ALPHA FEATURE. If you want to have the data you access stored
        locally in a cache, use this keyword to input a dictionary of keywords.
        The cache is set up using ``fsspec``'s simple cache. Example configuration
        is ``cache_kwargs=dict(cache_storage="/tmp/fnames/", same_names=True)``.

    Attributes
    ----------
    search_url : str
        If a search is performed on the ERDDAP server, the search url is saved as an attribute.
    server : str
        The Base URL of the ERDDAP instance.
    """

    name = "erddap_cat"
    output_instance = "intake.readers.entry:Catalog"

    def __init__(
        self,
        server: str,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        standard_names: Optional[List[str]] = None,
        variable_names: Optional[List[str]] = None,
        start_time: Optional[Union[datetime, str]] = None,
        end_time: Optional[Union[datetime, str]] = None,
        search_for: Optional[List[str]] = None,
        kwargs_search: MutableMapping[
            str, Union[str, int, float, Sequence[str]]
        ] = None,
        category_search: Optional[Tuple[str, str]] = None,
        erddap_client: Optional[Type[ERDDAP]] = None,
        use_source_constraints: bool = True,
        protocol: str = "tabledap",
        chunks: Optional[dict] = None,
        xarray_kwargs: Optional[dict] = None,
        metadata: dict = None,
        variables: list = None,
        query_type: str = "union",
        cache_period: Optional[Union[int, float]] = 500,
        open_kwargs: dict = None,
        mask_failed_qartod: bool = False,
        dropna: bool = False,
        cache_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        if server.endswith("/"):
            server = server[:-1]
        self._erddap_client = erddap_client or ERDDAP
        self._entries: Dict[str, Catalog] = {}
        self._use_source_constraints = use_source_constraints
        self._protocol = protocol
        self._chunks = chunks
        self._xarray_kwargs = xarray_kwargs
        self._dataset_metadata: Optional[Mapping[str, dict]] = None
        self._query_type = query_type
        self.server = server
        self.search_url = None
        self.cache_store = CacheStore(cache_period=cache_period)
        self.open_kwargs = open_kwargs or {}
        self._mask_failed_qartod = mask_failed_qartod
        self._dropna = dropna
        self._cache_kwargs = cache_kwargs
        if variables is not None:
            variables = ["time", "latitude", "longitude", "z"] + variables
        self.variables = variables

        chunks = chunks or {}
        xarray_kwargs = xarray_kwargs or {}

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
        # Use deepcopy so we don't mangle objects passed in from clients
        self.kwargs_search = deepcopy(kwargs_search)

        if bbox is not None:
            if not isinstance(bbox, tuple):
                raise TypeError(
                    f"Expecting a tuple of four floats for argument bbox: {type(bbox)}"
                )
            if len(bbox) != 4:
                raise ValueError("bbox argument requires a tuple of four floats")
            self.kwargs_search["min_lon"] = bbox[0]
            self.kwargs_search["min_lat"] = bbox[1]
            self.kwargs_search["max_lon"] = bbox[2]
            self.kwargs_search["max_lat"] = bbox[3]

        if standard_names is not None:
            if not isinstance(standard_names, (list, tuple)):
                raise TypeError(
                    f"Expecting list of strings for standard_names argument: {repr(standard_names)}"
                )
            self.kwargs_search["standard_name"] = standard_names

        if variable_names is not None:
            if not isinstance(variable_names, (list, tuple)):
                raise TypeError(
                    f"Expecting list of strings for variable_names argument: {repr(variable_names)}"
                )
            self.kwargs_search["variableName"] = variable_names

        if start_time is not None:
            if not isinstance(start_time, (str, datetime)):
                raise TypeError(
                    f"Expecting a datetime for start_time argument: {repr(start_time)}"
                )
            if isinstance(start_time, str):
                start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
            self.kwargs_search["min_time"] = f"{start_time:%Y-%m-%dT%H:%M:%SZ}"

        if end_time is not None:
            if not isinstance(end_time, (str, datetime)):
                raise TypeError(
                    f"Expecting a datetime for end_time argument: {repr(end_time)}"
                )
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
            self.kwargs_search["max_time"] = f"{end_time:%Y-%m-%dT%H:%M:%SZ}"

        if search_for is not None:
            if not isinstance(search_for, (list, tuple)):
                raise TypeError(
                    f"Expecting list of strings for search_for argument: {repr(search_for)}"
                )
            self.kwargs_search["search_for"] = search_for

        if category_search is not None:
            category, key = category_search
            # Currently just take first match, but there could be more than one.
            self.kwargs_search[category] = match_key_to_category(
                self.server, key, category, cache_store=self.cache_store
            )

        metadata = metadata or {}
        metadata["kwargs_search"] = self.kwargs_search

        # Clear the cache of old stale data on initialization
        self.cache_store.clear_cache(cache_period)

        super(ERDDAPCatalogReader, self).__init__(metadata=metadata, **kwargs)

    def _load_df(self) -> pd.DataFrame:
        frames = []
        for url in self.get_search_urls():
            try:
                df = self.cache_store.read_csv(url)
            except HTTPError as e:
                if e.code == 404:
                    log.warning(f"search {url} returned HTTP 404")
                    df = pd.DataFrame({"datasetID": []})
                else:
                    raise
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    log.warning(f"search {url} returned HTTP 404")
                    df = pd.DataFrame({"datasetID": []})
                else:
                    raise
            df.rename(columns={"Dataset ID": "datasetID"}, inplace=True)
            frames.append(df)
        if self._query_type == "union":
            result = pd.concat(frames)
            result = result.drop_duplicates("datasetID")
            return result
        elif self._query_type == "intersection":
            result = None
            for frame in frames:
                if result is None:
                    result = frame
                else:
                    result = result.merge(frame, how="inner", on="datasetID")
            return result
        else:
            raise ValueError(f"_query_type is unexpected value: {self._query_type}")

    def _load_metadata(self) -> Mapping[str, dict]:
        """Returns all of the dataset metadata available from allDatasets API."""
        if self._dataset_metadata is None:
            self._dataset_metadata = utils.get_erddap_metadata(
                self.server, self.kwargs_search, cache_store=self.cache_store
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
            # query is passed directly in method invocation below
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

    def read(self):
        dataidkey = "datasetID"
        e = self.get_client()
        df = self._load_df()
        all_metadata = self._load_metadata()

        self._entries = {}

        # Remove datasets that are redundant
        if len(df) > 0:
            df = df[
                (~df["datasetID"].str.startswith("ism-"))
                * (df["datasetID"] != "allDatasets")
            ]

        entries, aliases = {}, {}
        for index, row in df.iterrows():
            dataset_id = row[dataidkey]
            metadata = all_metadata.get(dataset_id, {})

            args = {
                "server": self.server,
                "dataset_id": dataset_id,
                "variables": self.variables,
                "protocol": self._protocol,
                "constraints": {},
                "open_kwargs": self.open_kwargs,
            }
            if self._protocol == "tabledap":
                args.update(
                    {
                        "mask_failed_qartod": self._mask_failed_qartod,
                        "dropna": self._dropna,
                        "cache_kwargs": self._cache_kwargs,
                    }
                )
                args["constraints"].update(self._get_tabledap_constraints())
                datatype = "intake_erddap.erddap:TableDAPReader"
            elif self._protocol == "griddap":
                args.update(
                    {
                        "chunks": self._chunks,
                        "xarray_kwargs": self._xarray_kwargs,
                    }
                )
                # no equivalent for griddap, though maybe it works the same?
                args["constraints"].update(self._get_tabledap_constraints())
                datatype = "intake_erddap.erddap:GridDAPReader"
            else:
                raise ValueError(f"Unsupported protocol: {self._protocol}")

            metadata["info_url"] = e.get_info_url(response="csv", dataset_id=dataset_id)
            entries[dataset_id] = DataDescription(
                datatype,
                kwargs={"dataset_id": dataset_id, **args},
                metadata=metadata,
            )
            aliases[dataset_id] = dataset_id

        cat = Catalog(
            data=entries,
            aliases=aliases,
        )
        return cat

    def _get_tabledap_constraints(self) -> Dict[str, Union[str, int, float]]:
        """Return the constraints dictionary for a tabledap Reader."""
        result = {}
        if self._use_source_constraints and "min_time" in self.kwargs_search:
            min_time = self.kwargs_search["min_time"]
            if isinstance(min_time, (str, int, float)):
                result["time>="] = min_time
        if self._use_source_constraints and "max_time" in self.kwargs_search:
            max_time = self.kwargs_search["max_time"]
            if isinstance(max_time, (str, int, float)):
                result["time<="] = max_time
        return result
