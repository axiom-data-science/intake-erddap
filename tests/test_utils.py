#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for generic and utility functions."""
from unittest import mock
from urllib.parse import parse_qsl, urlparse

import pandas as pd

from intake_erddap import utils


def test_get_project_version():
    version = utils.get_project_version()
    assert version is not None


@mock.patch("pandas.read_csv")
def test_category_and_key(mock_read_csv):
    df_mock = pd.DataFrame()
    df_mock["Category"] = ["wind_speed", "WIND_SPEED_GUST"]
    df_mock["URL"] = ["URL1", "URL2"]
    mock_read_csv.return_value = df_mock
    server = "http://erddap.invalid/erddap"
    df = utils.return_category_options(server, "standard_name")
    assert set(df["Category"]) - set(df_mock["Category"]) == set()

    # Also test match_key_to_category with this setup
    criteria = {
        "wind_s": {
            "standard_name": "wind_speed$",
        },
    }
    match_to_key = utils.match_key_to_category(
        server, "wind_s", "standard_name", criteria
    )
    assert match_to_key == ["wind_speed"]


@mock.patch("requests.get")
def test_get_erddap_metadata(requests_mock):
    resp = mock.MagicMock()
    resp.json.return_value = {
        "table": {
            "columnNames": [
                "datasetID",
                "some_string",
                "some_int",
                "some_float",
                "some_double",
            ],
            "columnTypes": [
                "String",
                "String",
                "int",
                "float",
                "double",
            ],
            "rows": [
                ["abc123", "value", "1", "2.0", "3.0"],
            ],
        }
    }
    requests_mock.return_value = resp

    server = "https://erddap.invalid/erddap"
    data = utils.get_erddap_metadata(server=server, constraints={})
    assert len(data) == 1
    assert data["abc123"]
    assert data["abc123"]["some_string"] == "value"
    assert data["abc123"]["some_int"] == 1
    assert data["abc123"]["some_float"] == 2.0
    assert data["abc123"]["some_double"] == 3.0
    assert requests_mock.call_args.args == (
        "https://erddap.invalid/erddap/tabledap/allDatasets.json?datasetID%2Cinstitution%2Ctitle"
        "%2Csummary%2CminLongitude%2CmaxLongitude%2CminLatitude%2CmaxLatitude%2CminTime%2CmaxTime"
        "%2Cgriddap%2Ctabledap",
    )

    constraints = {
        "min_lon": -72.8,
        "min_lat": 40.40,
        "max_lon": -69.6,
        "max_lat": 42.02,
        "min_time": "2022-11-01T00:00:00Z",
        "max_time": "2022-11-02T00:00:00Z",
    }
    utils.get_erddap_metadata(server=server, constraints=constraints)
    parts = urlparse(requests_mock.call_args.args[0])
    valid_query = dict(parse_qsl(parts.query))
    assert valid_query == {
        "minTime<": "2022-11-02T00:00:00Z",
        "maxTime>": "2022-11-01T00:00:00Z",
        "minLongitude<": "-69.6",
        "maxLongitude>": "-72.8",
        "minLatitude<": "42.02",
        "maxLatitude>": "40.4",
    }
