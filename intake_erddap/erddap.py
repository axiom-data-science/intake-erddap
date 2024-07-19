"""Reader implementations for intake-erddap."""

from logging import getLogger
from typing import List, Union

import cf_pandas  # noqa: F401
import fsspec
import pandas as pd
import requests
import xarray as xr

from erddapy import ERDDAP
import intake
from intake.readers.readers import BaseReader


log = getLogger("intake-erddap")


class ERDDAPReader(BaseReader):
    """
    ERDDAP Reader (Base Class). This class represents the abstract base class
    for an intake data reader object for ERDDAP. Clients should use either
    ``TableDAPReader`` or ``GridDAPReader``.

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
    open_kwargs : dict, optional
        Keyword arguments to pass on to the open function like `e.to_pandas`
        for a DataFrame. For example, {"parse_dates": True}

    Note
    ----
    Caches entire dataframe in memory.
    """

    output_instance = "xarray:Dataset"

    def get_client(
        self, server, protocol, dataset_id, variables, constraints, client=ERDDAP, **_
    ) -> ERDDAP:
        """Return an initialized ERDDAP Client."""
        e = client(server=server)
        e.protocol = protocol
        e.dataset_id = dataset_id
        e.variables = variables
        e.constraints = constraints
        return e


class TableDAPReader(ERDDAPReader):
    """Creates a Data Reader for an ERDDAP TableDAP Dataset.

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
        Additional metadata to include with the reader passed from the catalog.
    erddap_client : type, optional
        A class that implements an interface like erdappy's ERDDAP class. The
        reader will rely on this client to interface with ERDDAP for most
        requests.
    http_client : module or object, optional
        An object or module that implements an HTTP Client similar to request's
        interface. The reader will use this object to make HTTP requests to
        ERDDAP in some cases.
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

    Examples
    --------
    Readers are normally returned from a catalog object, but a Reader can be instantiated directly:

    >>> reader = TableDAPReader("https://erddap.senors.axds.co/erddap",
    ... "gov_usgs_waterdata_441759103261203")

    Getting a pandas DataFrame from the reader:

    >>> ds = reader.read()

    Once the dataset object has been instantiated, the dataset's full metadata
    is available in the reader.

    >>> reader.metadata
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

    output_instance = "pandas:DataFrame"

    def _read(
        self,
        server,
        dataset_id,
        variables=None,
        mask_failed_qartod=False,
        dropna=False,
        cache_kwargs=None,
        open_kwargs=None,
        constraints=None,
        **kw,
    ):
        open_kwargs = open_kwargs or {}
        variables = variables or []
        kw.pop("protocol", None)
        protocol = kw.pop("protocol", "tabledap")

        # check for variables in user-input list that are not available for the dataset
        meta2 = self._get_dataset_metadata(server, dataset_id)
        variables_diff = set(variables) - set(meta2["variables"].keys())
        if len(variables_diff) > 0:
            variables = [var for var in variables if var not in variables_diff]

        e = self.get_client(
            server,
            protocol,
            dataset_id,
            variables=variables,
            constraints=constraints or {},
            **kw,
        )
        if cache_kwargs is not None:
            if "response" in open_kwargs:
                response = open_kwargs["response"]
                open_kwargs.pop("response")
                url = e.get_download_url(response=response)
            else:
                url = e.get_download_url(
                    response="csvp"
                )  # should this be the default or csv?

            try:
                with fsspec.open(f"simplecache://::{url}", **(cache_kwargs or {})) as f:
                    dataframe: pd.DataFrame = pd.read_csv(f, **open_kwargs)
            except OSError as e:  # might get file name too long
                print(e)
                print(
                    "If your filenames are too long, input only a few variables"
                    "to return or input into cache kwargs `same_names=False`"
                )
        else:
            dataframe: pd.DataFrame = e.to_pandas(
                requests_kwargs={"timeout": 60}, **open_kwargs
            )
        if mask_failed_qartod:
            dataframe = self.run_mask_failed_qartod(dataframe)
        if dropna:
            dataframe = self.run_dropna(dataframe)
        return dataframe

    @staticmethod
    def data_cols(df):
        """Columns that are not axes, coordinates, nor qc_agg columns."""

        # find data columns which are what we'll use in the final step to drop nan's
        # don't include dimension/coordinates-type columns (dimcols) nor qc_agg columns (qccols)
        dimcols = df.cf.axes_cols + df.cf.coordinates_cols
        qccols = list(df.columns[df.columns.str.contains("_qc_agg")])
        datacols = [col for col in df.columns if col not in dimcols + qccols]
        return datacols

    def run_mask_failed_qartod(self, df):
        """Nan data values for which corresponding qc_agg columns is not equal to 1 or 2.

        To get this to work you may need to specify the "qc_agg" columns to come along specifically
        in the variables input.
        """

        # if a data column has an associated qc column, use it to weed out bad data by
        # setting it to nan.
        for datacol in self.data_cols(df):
            qccol = f"{datacol}_qc_agg"
            if qccol in df.columns:
                df.loc[~df[qccol].isin([1, 2]), datacol] = pd.NA
                df.drop(columns=[qccol], inplace=True)
        return df

    def run_dropna(self, df):
        """Drop nan rows based on the data columns."""
        return df.dropna(subset=self.data_cols(df))

    def _get_dataset_metadata(self, server, dataset_id) -> dict:
        """Fetch and return the metadata document for the dataset."""
        url = f"{server}/info/{dataset_id}/index.json"
        resp = requests.get(url)
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


class GridDAPReader(ERDDAPReader):
    """Creates a Data Reader for an ERDDAP GridDAP Dataset.

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
    Readers are normally returned from a catalog object, but a reader can be instantiated directly:

    >>> reader = GridDAPReader("https://coastwatch.pfeg.noaa.gov/erddap", "charmForecast1day",
    ... chunks={"time": 1})

    Getting an xarray dataset from the reader object:

    >>> ds = reader.read()

    Once the dataset object has been instantiated, the dataset's full metadata
    is available in the reader.

    >>> reader.metadata
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

    """

    # def __init__(
    #     self,
    #     server: str,
    #     dataset_id: str,
    #     constraints: dict = None,
    #     chunks: Union[None, int, dict, str] = None,
    #     xarray_kwargs: dict = None,
    #     **kwargs,
    # ):
    #     self._server = server
    #     self._chunks = chunks
    #     self._constraints = constraints or {}
    #     self._xarray_kwargs = xarray_kwargs or {}
    #     # Initialized by the private getter _get_schema
    #     self.urlpath = f"{server}/griddap/{dataset_id}"
    #     # https://github.com/python/mypy/issues/6799
    #     kwargs.pop("protocol", None)
    #     super().__init__(dataset_id=dataset_id, protocol="griddap", **kwargs)  # type: ignore

    def _read(
        self,
        server: str,
        dataset_id: str,
        constraints: dict = None,
        chunks: Union[None, int, dict, str] = None,
        xarray_kwargs: dict = None,
        **kw,
    ):
        constraints = constraints or {}
        chunks = chunks or {}
        xarray_kwargs = xarray_kwargs or {}
        urlpath = f"{server}/griddap/{dataset_id}"

        ds = xr.open_dataset(urlpath, chunks=chunks, **xarray_kwargs)
        # _NCProperties is an internal property which xarray does not yet deal
        # with specially, so we remove it here to prevent it from causing
        # problems for clients.
        if "_NCProperties" in ds.attrs:
            del ds.attrs["_NCProperties"]
        return ds
