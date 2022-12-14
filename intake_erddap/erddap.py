"""Source implementations for intake-erddap."""
import typing

from logging import getLogger
from typing import List, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
import requests
import xarray as xr

from erddapy import ERDDAP
from intake.source import base

from .version import __version__


log = getLogger("intake-erddap")


if typing.TYPE_CHECKING:  # pragma: no cover
    # numpy typing is only available after version 1.21
    from numpy.typing import ArrayLike


class ERDDAPSource(base.DataSource):
    """
    ERDDAP Source (Base Class). This class represents the abstract base class
    for an intake data source object for ERDDAP. Clients should use either
    ``TableDAPSource`` or ``GridDAPSource``.

    Parameters
    ----------

    dataset_id : str
        The unique datasetID value returned from ERDDAP.
    protocol : str
        Either `'griddap'` or `'tabledap'`.
    variables : list of str
    constraints : dict
        The query constraints to apply to TableDAP requests.
    metadata : dict
    erddap_client : class, optional
        The client object to use for connections to ERDDAP. Must conform to
        the `erddapy.ERDDAP` interface.
    http_client : class, optional
        The client object to use for HTTP requests. Must conform to the
        `requests` interface.

    Note
    ----
    Caches entire dataframe in memory.
    """

    name = "erddap"
    version = __version__
    container = "dataframe"
    partition_access = True

    def __init__(
        self,
        dataset_id: str,
        protocol: str,
        variables: List[str] = None,
        constraints: dict = None,
        metadata: dict = None,
        erddap_client: Optional[Type[ERDDAP]] = None,
        http_client: Optional[Type] = None,
    ):
        variables = variables or []
        constraints = constraints or {}
        metadata = metadata or {}

        self._init_args = {
            "dataset_id": dataset_id,
            "protocol": protocol,
            "variables": variables,
            "constraints": constraints,
            "metadata": metadata,
        }

        self._dataset_id = dataset_id
        self._protocol = protocol
        self._variables = variables
        self._constraints = constraints
        self._erddap_client = erddap_client or ERDDAP
        self._http = http_client or requests

        super(ERDDAPSource, self).__init__(metadata=metadata)

    def get_client(self) -> ERDDAP:
        """Return an initialized ERDDAP Client."""
        e = self._erddap_client(server=self._server)
        e.protocol = self._protocol
        e.dataset_id = self._dataset_id
        e.variables = self._variables
        e.constraints = self._constraints
        return e


class TableDAPSource(ERDDAPSource):
    """Creates a Data Source for an ERDDAP TableDAP Dataset.

    Parameters
    ----------
    server : str
        URL to the ERDDAP service. Example: ``"https://coastwatch.pfeg.noaa.gov/erddap"``

        Note
        ----
        Do not include a trailing slash.
    dataset_id : str
        The dataset identifier from ERDDAP.
    variables : list of str, optional
        A list of variables to retrieve from the dataset.
    constraints : dict, optional
        A mapping of conditions and constraints. Example:
        ``{"time>=": "2022-01-02T12:00:00Z", "lon>": -140, "lon<": 0}``
    metadata : dict, optional
        Additional metadata to include with the source passed from the catalog.
    erddap_client : type, optional
        A class that implements an interface like erdappy's ERDDAP class. The
        source will rely on this client to interface with ERDDAP for most
        requests.
    http_client : module or object, optional
        An object or module that implements an HTTP Client similar to request's
        interface. The source will use this object to make HTTP requests to
        ERDDAP in some cases.

    Examples
    --------
    Sources are normally returned from a catalog object, but a source can be instantiated directly:

    >>> source = TableDAPSource("https://erddap.senors.axds.co/erddap",
    ... "gov_usgs_waterdata_441759103261203")

    Getting a pandas DataFrame from the source:

    >>> ds = source.read()

    Once the dataset object has been instantiated, the dataset's full metadata
    is available in the source.

    >>> source.metadata
    {'info_url': 'https://erddap.sensors.axds.co/erddap/info/gov_usgs_waterdata_404513098181201...',
    'catalog_dir': '',
    'variables': {'time': {'_CoordinateAxisType': 'Time',
    'actual_range': [1430828100.0, 1668079800.0],
    'axis': 'T',
    'ioos_category': 'Time',
    'long_name': 'Time',
    'standard_name': 'time',
    'time_origin': '01-JAN-1970 00:00:00',
    'units': 'seconds since 1970-01-01T00:00:00Z'},
        ...
    """

    name = "tabledap"
    version = __version__
    container = "dataframe"
    partition_access = True

    def __init__(self, server: str, *args, **kwargs):
        self._server = server
        self._dataframe: Optional[pd.DataFrame] = None
        self._dataset_metadata: Optional[dict] = None
        kwargs.pop("protocol", None)
        # https://github.com/python/mypy/issues/6799
        super().__init__(*args, protocol="tabledap", **kwargs)  # type: ignore

    def _get_schema(self) -> base.Schema:
        if self._dataframe is None:
            # TODO: could do partial read with chunksize to get likely schema from
            # first few records, rather than loading the whole thing
            self._load()
            self._dataset_metadata = self._get_dataset_metadata()
        # make type checker happy
        assert self._dataframe is not None
        return base.Schema(
            datashape=None,
            dtype=self._dataframe.dtypes,
            shape=self._dataframe.shape,
            npartitions=1,
            extra_metadata=self._dataset_metadata,
        )

    def _get_partition(self) -> pd.DataFrame:
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe

    def read(self) -> pd.DataFrame:
        """Return the dataframe from ERDDAP"""
        return self._get_partition()

    def _close(self):
        self._dataframe = None

    def _load(self):
        e = self.get_client()
        self._dataframe: pd.DataFrame = e.to_pandas()

    def _get_dataset_metadata(self) -> dict:
        """Fetch and return the metadata document for the dataset."""
        url = f"{self._server}/info/{self._dataset_id}/index.json"
        resp = self._http.get(url)
        resp.raise_for_status()
        metadata: dict = {"variables": {}}
        for rowtype, varname, attrname, dtype, value in resp.json()["table"]["rows"]:
            if rowtype != "attribute":
                continue
            try:
                value = self._parse_metadata_value(value=value, dtype=dtype)
            except ValueError:
                log.warning(f"could not convert {dtype} {varname}:{attrname} = {value}")
                continue

            if varname == "NC_GLOBAL":
                metadata[attrname] = value
            else:
                if varname not in metadata["variables"]:
                    metadata["variables"][varname] = {}
                metadata["variables"][varname][attrname] = value
        return metadata

    def _parse_metadata_value(
        self, value: str, dtype: str
    ) -> Union[int, float, str, List[int], List[float]]:
        """Return the value from ERDDAPs metadata table parsed into a Python type."""
        newvalue: Union[int, float, str, List[int], List[float]] = value
        if dtype in ("int", "double", "float") and "," in value:
            tmp = [i.strip() for i in value.split(",")]
            if dtype == "int":
                newvalue = [int(i) for i in tmp]
            if dtype in ("float", "double"):
                newvalue = [float(i) for i in tmp]
        elif dtype == "int":
            newvalue = int(value)
        elif dtype in ("float", "double"):
            newvalue = float(value)
        return newvalue


class GridDAPSource(ERDDAPSource):
    """Creates a Data Source for an ERDDAP GridDAP Dataset.

    Parameters
    ----------
    server : str
        URL to the ERDDAP service. Example: ``"https://coastwatch.pfeg.noaa.gov/erddap"``

        Note
        ----
        Do not include a trailing slash.

    dataset_id : str
        The dataset identifier from ERDDAP.
    constraints : dict, optional
        A mapping of conditions and constraints.
    chunks : None or int or dict or str, optional
        If chunks is provided, it is used to load the new dataset into dask
        arrays. chunks=-1 loads the dataset with dask using a single chunk for
        all arrays. chunks={} loads the dataset with dask using engine preferred
        chunks if exposed by the backend, otherwise with a single chunk for all
        arrays. chunks='auto' will use dask auto chunking taking into account
        the engine preferred chunks. See dask chunking for more details.
    xarray_kwargs : dict, optional
        Arguments to be passed to the xarray open_dataset function.

    Examples
    --------
    Sources are normally returned from a catalog object, but a source can be instantiated directly:

    >>> source = GridDAPSource("https://coastwatch.pfeg.noaa.gov/erddap", "charmForecast1day",
    ... chunks={"time": 1})

    Getting an xarray dataset from the source object:

    >>> ds = source.to_dask()

    Once the dataset object has been instantiated, the dataset's full metadata
    is available in the source.

    >>> source.metadata
    {'catalog_dir': '',
    'dims': {'time': 1182, 'latitude': 391, 'longitude': 351},
    'data_vars': {'pseudo_nitzschia': ['time', 'latitude', 'longitude'],
    'particulate_domoic': ['time', 'latitude', 'longitude'],
    'cellular_domoic': ['time', 'latitude', 'longitude'],
    'chla_filled': ['time', 'latitude', 'longitude'],
    'r555_filled': ['time', 'latitude', 'longitude'],
    'r488_filled': ['time', 'latitude', 'longitude']},
    'coords': ('time', 'latitude', 'longitude'),
    'acknowledgement':
        ...

    Warning
    -------
    The ``read()`` method will raise a ``NotImplemented`` exception because the
    standard intake interface has the result read entirely into memory. For
    gridded datasets this should not be allowed, reading the entire dataset into
    memory can overwhelm the server, get the client blacklisted, and potentially
    crash the client by exhausting available system memory. If a client truly
    wants to load the entire dataset into memory, the client can invoke the
    method ``ds.load()`` on the Dataset object.
    """

    name = "griddap"
    version = __version__
    container = "xarray"
    partition_access = True

    def __init__(
        self,
        server: str,
        dataset_id: str,
        constraints: dict = None,
        chunks: Union[None, int, dict, str] = None,
        xarray_kwargs: dict = None,
        **kwargs,
    ):
        self._server = server
        self._ds: Optional[xr.Dataset] = None
        self._chunks = chunks
        self._constraints = constraints or {}
        self._xarray_kwargs = xarray_kwargs or {}
        # Initialized by the private getter _get_schema
        self._schema: Optional[base.Schema] = None
        self.urlpath = f"{server}/griddap/{dataset_id}"
        # https://github.com/python/mypy/issues/6799
        kwargs.pop("protocol", None)
        super().__init__(dataset_id=dataset_id, protocol="griddap", **kwargs)  # type: ignore

    def _get_schema(self) -> base.Schema:
        self.urlpath = self._get_cache(self.urlpath)[0]

        if self._ds is None:
            # Sets self._ds
            self._open_dataset()
            # Make mypy happy
            assert self._ds is not None
            metadata = {
                "dims": dict(self._ds.dims),
                "data_vars": {
                    k: list(self._ds[k].coords) for k in self._ds.data_vars.keys()
                },
                "coords": tuple(self._ds.coords.keys()),
            }
            metadata.update(self._ds.attrs)
            metadata["variables"] = {}
            for varname in self._ds.variables:
                metadata["variables"][varname] = self._ds[varname].attrs
            self._schema = base.Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata,
            )

        return self._schema

    def _open_dataset(self):
        self._ds = xr.open_dataset(
            self.urlpath, chunks=self._chunks, **self._xarray_kwargs
        )
        # _NCProperties is an internal property which xarray does not yet deal
        # with specially, so we remove it here to prevent it from causing
        # problems for clients.
        if "_NCProperties" in self._ds.attrs:
            del self._ds.attrs["_NCProperties"]

    def read(self):
        raise NotImplementedError(
            "GridDAPSource.read is not implemented because ds.load() for grids from ERDDAP are "
            "strongly discouraged. Use to_dask() instead."
        )

    def read_chunked(self) -> xr.Dataset:
        """Return an xarray dataset (optionally chunked)."""
        self._load_metadata()
        return self._ds

    def read_partition(self, i: Tuple[str, ...]) -> "ArrayLike":
        """Fetch one chunk of the array for a variable."""
        self._load_metadata()
        if not isinstance(i, (tuple, list)):
            raise TypeError("For Xarray sources, must specify partition as tuple")
        if isinstance(i, list):
            i = tuple(i)
        # Make mypy happy
        assert self._ds is not None
        arr = self._ds[i[0]].data
        idx = i[1:]
        if isinstance(arr, np.ndarray):
            return arr
        # dask array
        return arr.blocks[idx].compute()

    def to_dask(self) -> xr.Dataset:
        """Return an xarray dataset (optionally chunked)."""
        return self.read_chunked()

    def close(self):
        """Close open descriptors."""
        self._ds = None
        self._schema = None
