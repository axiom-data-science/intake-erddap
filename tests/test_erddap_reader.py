#!/usr/bin/env pytest
# -*- coding: utf-8 -*-
"""Unit tests for the ERDDAP Reader object."""
import json

from pathlib import Path
from unittest import mock

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from intake_erddap.erddap import GridDAPReader, TableDAPReader


def _grid(grid_data) -> xr.Dataset:

    time = xr.DataArray(
        data=np.array(["2022-01-01T00:00:00"]),
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


@mock.patch("intake_erddap.erddap.TableDAPReader._get_dataset_metadata")
@mock.patch("erddapy.ERDDAP.to_pandas")
def test_erddap_reader_read(mock_to_pandas, mock_get_dataset_metadata):
    """Tests that the reader will read from ERDDAP into a pd.DataFrame."""
    df = pd.DataFrame()
    df["time (UTC)"] = ["2022-10-21T00:00:00Z", "2022-10-21T00:00:00Z"]
    df["sea_water_temperature (deg_C)"] = [13.4, 13.4]
    mock_to_pandas.return_value = df
    mock_get_dataset_metadata.return_value = {"variables": {}}

    reader = TableDAPReader(
        server="http://erddap.invalid/erddap", dataset_id="abc123", protocol="tabledap"
    )
    df = reader.read()

    assert df is not None
    assert mock_to_pandas.called
    assert len(df) == 2

    reader.close()


@mock.patch("intake_erddap.erddap.TableDAPReader._get_dataset_metadata")
@mock.patch("erddapy.ERDDAP.to_pandas")
def test_erddap_reader_read_processing(mock_to_pandas, mock_get_dataset_metadata):
    """Tests that the reader will read from ERDDAP into a pd.DataFrame with processing flag."""
    df = pd.DataFrame()
    df["time"] = [
        "2022-10-21T01:00:00Z",
        "2022-10-21T02:00:00Z",
        "2022-10-21T03:00:00Z",
    ]
    df["sea_water_temperature"] = [13.4, 13.4, np.nan]
    df["sea_water_temperature_qc_agg"] = [1, 4, 2]
    mock_to_pandas.return_value = df
    mock_get_dataset_metadata.return_value = {"variables": {}}

    reader = TableDAPReader(
        server="http://erddap.invalid/erddap",
        dataset_id="abc123",
        protocol="tabledap",
        mask_failed_qartod=True,
        dropna=True,
    )
    df = reader.read()
    assert df is not None
    assert mock_to_pandas.called
    # mask_failed_qartod flag removes 2nd data point and dropna removes 3rd data point
    assert len(df) == 1


@mock.patch("requests.get")
def test_tabledap_reader_get_dataset_metadata(mock_get):
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
    server = "http://erddap.invalid"
    dataset_id = "abc123"
    reader = TableDAPReader(server, dataset_id)
    metadata = reader._get_dataset_metadata(server, dataset_id)
    assert metadata["cdm_data_type"] == "TimeSeries"
    assert metadata["variables"]["z"]["actual_range"] == [0.0, 0.0]
    assert metadata["variables"]["depth_to_water_level"]["status_flags"] == [
        1,
        2,
        3,
        4,
        9,
    ]

    metadata = reader._get_dataset_metadata(server, dataset_id)
    assert len(metadata) == 1
    assert len(metadata["variables"]) == 0


@mock.patch("xarray.open_dataset")
def test_griddap_reader_no_chunks(mock_open_dataset, fake_grid):
    server = "https://erddap.invalid"
    dataset_id = "abc123"
    mock_open_dataset.return_value = fake_grid
    reader = GridDAPReader(server=server, dataset_id=dataset_id)
    ds = reader.read()
    assert ds is fake_grid
    assert "_NCProperties" not in ds.attrs
    assert "temp" in ds.variables


@mock.patch("xarray.open_dataset")
def test_griddap_reader_with_dask(mock_open_dataset, fake_dask_grid):
    server = "https://erddap.invalid"
    dataset_id = "abc123"
    mock_open_dataset.return_value = fake_dask_grid
    reader = GridDAPReader(server=server, dataset_id=dataset_id)
    arr = reader.read()
    assert isinstance(arr, xr.Dataset)
