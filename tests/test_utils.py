#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for generic and utility functions."""
from unittest import mock
from intake_erddap import utils
import pandas as pd


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
    assert set(df['Category']) - set(df_mock["Category"]) == set()

    # Also test match_key_to_category with this setup
    criteria = {
        "wind_s": {
            "standard_name": "wind_speed$",
        },
    }
    match_to_key = utils.match_key_to_category(server, "wind_s", "standard_name", criteria)
    assert match_to_key == ["wind_speed"]
