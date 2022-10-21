#!/usr/bin/env pytest
# -*- coding: utf-8 -*-
"""Unit tests for the ERDDAP Source object."""


from unittest import mock

import pandas as pd

from intake_erddap.erddap import ERDDAPSource


@mock.patch("erddapy.ERDDAP.to_pandas")
def test_erddap_source_read(mock_to_pandas):
    """Tests that the source will read from ERDDAP into a pd.DataFrame."""
    df = pd.DataFrame()
    df["time (UTC)"] = ["2022-10-21T00:00:00Z", "2022-10-21T00:00:00Z"]
    df["sea_water_temperature (deg_C)"] = [13.4, 13.4]
    mock_to_pandas.return_value = df
    source = ERDDAPSource(
        server="http://erddap.invalid/erddap", dataset_id="abc123", protocol="tabledap"
    )
    df = source.read()
    assert df is not None
    assert mock_to_pandas.called
    assert len(df) == 2
