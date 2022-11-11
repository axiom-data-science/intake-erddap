#!/usr/bin/env pytest
# -*- coding: utf-8 -*-
"""Unit tests for the ERDDAP Source object."""
import json

from pathlib import Path
from unittest import mock

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from intake_erddap.erddap import GridDAPSource, TableDAPSource


def _grid(grid_data) -> xr.Dataset:

    time = xr.DataArray(
        data=np.array(["2022-01-01T00:00:00"], dtype="<M8[s]"),
        dims=("time",),
        attrs={"standard_name": "time", "units": "seconds since 1970-01-01"},
    )
    lon = xr.DataArray(
        data=np.arange(-180, 180, dtype=np.float32),
        dims=("lon",),
        attrs={"standard_name": "longitude", "units": "degrees_east"},
    )

    lat = xr.DataArray(
        data=np.arange(-90, 90, dtype=np.float32),
        dims=("lat",),
        attrs={"standard_name": "latitude", "units": "degrees_north"},
    )

    temp = xr.DataArray(
        data=grid_data,
        dims=("time", "lat", "lon"),
        attrs={"standard_name": "air_temperature", "units": "deg_C"},
    )

    ds = xr.Dataset({"time": time, "temp": temp, "lon": lon, "lat": lat})
    ds.attrs["_NCProperties"] = "blah"
    return ds


@pytest.fixture
def fake_grid() -> xr.Dataset:
    """Return a fake grid for testing purposes."""
    grid_data = np.ones((1, 180, 360)) * 15
    return _grid(grid_data)


@pytest.fixture
def fake_dask_grid() -> xr.Dataset:
    """Return a fake grid for testing purposes."""
    grid_data = da.ones((1, 180, 360)) * 15
    return _grid(grid_data)


@mock.patch("intake_erddap.erddap.TableDAPSource._get_dataset_metadata")
@mock.patch("erddapy.ERDDAP.to_pandas")
def test_erddap_source_read(mock_to_pandas, mock_get_dataset_metadata):
    """Tests that the source will read from ERDDAP into a pd.DataFrame."""
    df = pd.DataFrame()
    df["time (UTC)"] = ["2022-10-21T00:00:00Z", "2022-10-21T00:00:00Z"]
    df["sea_water_temperature (deg_C)"] = [13.4, 13.4]
    mock_to_pandas.return_value = df
    mock_get_dataset_metadata.return_value = {}

    source = TableDAPSource(
        server="http://erddap.invalid/erddap", dataset_id="abc123", protocol="tabledap"
    )
    df = source.read()
    assert df is not None
    assert mock_to_pandas.called
    assert len(df) == 2

    source.close()
    assert source._dataframe is None


@mock.patch("requests.get")
def test_tabledap_source_get_dataset_metadata(mock_get):
    test_data = Path(__file__).parent / "test_data/tabledap_metadata.json"
    bad = {
        "table": {
            "rows": [
                ["attribute", "NC_GLOBAL", "blah", "int", ","],
            ]
        }
    }

    resp = mock.MagicMock()
    resp.json.side_effect = [json.loads(test_data.read_text()), bad]
    mock_get.return_value = resp
    source = TableDAPSource(server="http://erddap.invalid", dataset_id="abc123")
    metadata = source._get_dataset_metadata()
    assert metadata["cdm_data_type"] == "TimeSeries"
    assert metadata["variables"]["z"]["actual_range"] == [0.0, 0.0]
    assert metadata["variables"]["depth_to_water_level"]["status_flags"] == [
        1,
        2,
        3,
        4,
        9,
    ]

    metadata = source._get_dataset_metadata()
    assert len(metadata) == 1
    assert len(metadata["variables"]) == 0


@mock.patch("xarray.open_dataset")
def test_griddap_source_no_chunks(mock_open_dataset, fake_grid):
    server = "https://erddap.invalid"
    dataset_id = "abc123"
    mock_open_dataset.return_value = fake_grid
    source = GridDAPSource(server=server, dataset_id=dataset_id)
    ds = source.to_dask()
    assert ds is fake_grid
    assert "_NCProperties" not in ds.attrs

    with pytest.raises(NotImplementedError):
        source.read()

    arr = source.read_partition(("temp", None))
    assert isinstance(arr, np.ndarray)

    arr = source.read_partition(["temp", None])
    assert isinstance(arr, np.ndarray)

    with pytest.raises(TypeError):
        source.read_partition("temp")

    source.close()
    assert source._ds is None
    assert source._schema is None


@mock.patch("xarray.open_dataset")
def test_griddap_source_with_dask(mock_open_dataset, fake_dask_grid):
    server = "https://erddap.invalid"
    dataset_id = "abc123"
    mock_open_dataset.return_value = fake_dask_grid
    source = GridDAPSource(server=server, dataset_id=dataset_id)
    arr = source.read_partition(("temp", 0))
    assert isinstance(arr, np.ndarray)
