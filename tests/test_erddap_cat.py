#!/usr/bin/env pytest
"""Unit tests."""
from unittest import mock

import intake
import pandas as pd
import pytest

from intake_erddap.erddap_cat import ERDDAPCatalog


def test_nothing():
    """This test exists to ensure that at least one test works."""
    pass


@mock.patch("erddapy.ERDDAP.to_pandas")
def test_erddap_catalog(mock_to_pandas):
    results = pd.DataFrame()
    results["datasetID"] = ["abc123"]
    mock_to_pandas.return_value = results
    server = "http://erddap.invalid/erddap"
    cat = ERDDAPCatalog(server=server)
    assert list(cat) == ["abc123"]


@mock.patch("pandas.read_csv")
def test_erddap_catalog_searching(mock_read_csv):
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
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }
    server = "http://erddap.sensors.ioos.us/erddap"
    cat_sensors = intake.open_erddap_cat(server, kwargs_search=kw)
    df = cat_sensors[list(cat_sensors)[0]].read()
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
