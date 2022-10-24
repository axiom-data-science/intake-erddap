#!/usr/bin/env pytest
"""Unit tests."""
from unittest import mock

import intake
import pandas as pd
import pytest

from erddapy import ERDDAP

from intake_erddap.erddap_cat import ERDDAPCatalog


def test_nothing():
    """This test exists to ensure that at least one test works."""
    pass


@mock.patch("erddapy.ERDDAP.to_pandas")
def test_erddap_catalog(mock_to_pandas):
    """Test basic catalog API."""
    results = pd.DataFrame()
    results["datasetID"] = ["abc123"]
    mock_to_pandas.return_value = results
    server = "http://erddap.invalid/erddap"
    cat = ERDDAPCatalog(server=server)
    assert list(cat) == ["abc123"]


@mock.patch("pandas.read_csv")
def test_erddap_catalog_searching(mock_read_csv):
    """Test catalog with search parameters."""
    results = pd.DataFrame()
    results["datasetID"] = ["abc123"]
    mock_read_csv.return_value = results
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }
    server = "http://erddap.invalid/erddap"
    cat = ERDDAPCatalog(server=server, kwargs_search=kw)
    assert list(cat) == ["abc123"]


@pytest.mark.integration
def test_ioos_erddap_catalog_and_source():
    """Integration test against IOOS Sensors ERDDAP."""
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }
    server = "https://erddap.sensors.ioos.us/erddap"
    cat_sensors = intake.open_erddap_cat(server, kwargs_search=kw)
    df = cat_sensors[list(cat_sensors)[0]].read()
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_catalog_uses_di_client():
    """Tests that the catalog uses the dependency injection provided client."""
    mock_erddap_client = mock.create_autospec(ERDDAP)
    server = "http://erddap.invalid/erddap"
    cat = ERDDAPCatalog(server=server, erddap_client=mock_erddap_client)
    client = cat.get_client()
    assert isinstance(client, mock.NonCallableMagicMock)


@mock.patch("erddapy.ERDDAP.to_pandas")
def test_catalog_skips_all_datasets_row(mock_to_pandas):
    """Tests that the catalog results ignore allDatasets special dataset."""
    df = pd.DataFrame()
    df["datasetID"] = ["allDatasets", "abc123"]
    mock_to_pandas.return_value = df
    server = "http://erddap.invalid/erddap"
    cat = ERDDAPCatalog(server=server)
    assert list(cat) == ["abc123"]
