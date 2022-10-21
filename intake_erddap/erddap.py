"""Source implementations for intake-erddap."""
from typing import List, Optional, Type

import pandas as pd

from erddapy import ERDDAP
from intake.source import base

from .version import __version__


class ERDDAPSource(base.DataSource):
    """
    One-shot ERDDAP to dataframe reader (no partitioning)
    (TableDAP only)

    Caches entire dataframe in memory.

    Parameters
    ----------
    server: str
        URI to ERDDAP server
    dataset_id: str

    variables: list

    constraints: dict

    """

    name = "erddap"
    version = __version__
    container = "dataframe"
    partition_access = True

    def __init__(
        self,
        server: str,
        dataset_id: str,
        protocol: str = "tabledap",
        variables: List[str] = None,
        constraints: dict = None,
        metadata: dict = None,
        erddap_client: Optional[Type[ERDDAP]] = None,
    ):
        variables = variables or []
        constraints = constraints or {}
        metadata = metadata or {}

        self._init_args = {
            "server": server,
            "dataset_id": dataset_id,
            "protocol": protocol,
            "variables": variables,
            "constraints": constraints,
            "metadata": metadata,
        }

        self._server = server
        self._dataset_id = dataset_id
        self._protocol = protocol
        self._variables = variables
        self._constraints = constraints
        self._dataframe: Optional[pd.DataFrame] = None
        self._erddap_client = erddap_client or ERDDAP

        super(ERDDAPSource, self).__init__(metadata=metadata)

    def get_client(self) -> ERDDAP:
        """Return an initialized ERDDAP Client."""
        e = self._erddap_client(server=self._server)
        e.protocol = self._protocol
        e.dataset_id = self._dataset_id
        e.variables = self._variables
        e.constraints = self._constraints
        return e

    def _load(self):
        e = self.get_client()
        self._dataframe: pd.DataFrame = e.to_pandas()

    def _get_schema(self) -> base.Schema:
        if self._dataframe is None:
            # TODO: could do partial read with chunksize to get likely schema from
            # first few records, rather than loading the whole thing
            self._load()
        # make type checker happy
        assert self._dataframe is not None
        return base.Schema(
            datashape=None,
            dtype=self._dataframe.dtypes,
            shape=self._dataframe.shape,
            npartitions=1,
            extra_metadata={},
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
