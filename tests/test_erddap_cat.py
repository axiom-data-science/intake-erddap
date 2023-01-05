#!/usr/bin/env pytest
"""Unit tests."""
import os

from datetime import datetime
from tempfile import mkstemp
from unittest import mock
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlparse

import cf_pandas
import intake
import numpy as np
import pandas as pd
import pytest
import requests

from erddapy import ERDDAP

from intake_erddap.erddap import GridDAPSource, TableDAPSource
from intake_erddap.erddap_cat import ERDDAPCatalog


SERVER_URL = "http://erddap.invalid/erddap"


@pytest.fixture
def single_dataset_catalog() -> pd.DataFrame:
    """Fixture returns a dataframe with a single dataset ID."""
    df = pd.DataFrame()
    df["datasetID"] = ["abc123"]
    return df


def test_nothing():
    """This test exists to ensure that at least one test works."""
    pass


@pytest.fixture
def temporary_catalog():
    fd, path = mkstemp(suffix=".yml")
    os.close(fd)
    try:
        yield path
    finally:
        if os.path.exists(path):
            os.unlink(path)


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_erddap_catalog(mock_read_csv, load_metadata_mock):
    """Test basic catalog API."""
    load_metadata_mock.return_value = {}
    results = pd.DataFrame()
    results["datasetID"] = ["abc123"]
    mock_read_csv.return_value = results
    cat = ERDDAPCatalog(server=SERVER_URL)
    assert list(cat) == ["abc123"]


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_erddap_catalog_searching(mock_read_csv, load_metadata_mock):
    """Test catalog with search parameters."""
    load_metadata_mock.return_value = {}
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
    cat = ERDDAPCatalog(server=SERVER_URL, kwargs_search=kw)
    assert list(cat) == ["abc123"]


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_erddap_catalog_searching_variable(mock_read_csv, load_metadata_mock):
    load_metadata_mock.return_value = {}
    df1 = pd.DataFrame()
    df1["Category"] = ["sea_water_temperature"]
    df1["URL"] = ["http://blah.invalid"]
    df2 = pd.DataFrame()
    df2["Dataset ID"] = ["testID"]
    # pd.read_csv is called twice, so two return results
    mock_read_csv.side_effect = [df1, df2]
    criteria = {
        "temp": {
            "standard_name": "sea_water_temperature$",
        },
    }
    cf_pandas.set_options(custom_criteria=criteria)
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }
    cat = ERDDAPCatalog(
        server=SERVER_URL, kwargs_search=kw, category_search=("standard_name", "temp")
    )
    assert "standard_name" in cat.kwargs_search
    assert cat.kwargs_search["standard_name"] == ["sea_water_temperature"]


@pytest.mark.integration
def test_ioos_erddap_catalog_and_source():
    """Integration test against IOOS Sensors ERDDAP."""
    bbox = (-73.32, 39.92, -69.17, 42.27)
    kw = {
        "min_lon": bbox[0],
        "max_lon": bbox[2],
        "min_lat": bbox[1],
        "max_lat": bbox[3],
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }
    cat_sensors = intake.open_erddap_cat(
        server="https://erddap.sensors.ioos.us/erddap", kwargs_search=kw
    )
    source = cat_sensors["gov_noaa_water_wstr1"]
    df = source.read()
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    for dataset_id in cat_sensors:
        assert cat_sensors[dataset_id].metadata["institution"] is not None


@pytest.mark.integration
def test_ioos_default_init():
    """Test that the default catalog initializes."""
    cat_sensors = intake.open_erddap_cat(
        server="https://erddap.sensors.ioos.us/erddap",
    )
    assert len(cat_sensors) > 0


@pytest.mark.integration
def test_erddap_global_conneection():
    ERDDAPCatalog(
        "https://erddap.sensors.axds.co/erddap",
        kwargs_search={"standard_name": "sea_water_temperature"},
    )


def test_invalid_kwarg_search():
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }

    with pytest.raises(ValueError):
        intake.open_erddap_cat(server=SERVER_URL, kwargs_search=kw)

    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "max_time": "2021-4-2",
    }

    with pytest.raises(ValueError):
        intake.open_erddap_cat(server=SERVER_URL, kwargs_search=kw)


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_uses_di_client(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    """Tests that the catalog uses the dependency injection provided client."""
    mock_read_csv.return_value = single_dataset_catalog
    mock_erddap_client = mock.create_autospec(ERDDAP)
    cat = ERDDAPCatalog(server=SERVER_URL, erddap_client=mock_erddap_client)
    client = cat.get_client()
    assert isinstance(client, mock.NonCallableMagicMock)


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_skips_all_datasets_row(mock_read_csv, load_metadata_mock):
    load_metadata_mock.return_value = {}
    """Tests that the catalog results ignore allDatasets special dataset."""
    df = pd.DataFrame()
    df["datasetID"] = ["allDatasets", "abc123"]
    mock_read_csv.return_value = df
    cat = ERDDAPCatalog(server=SERVER_URL)
    assert list(cat) == ["abc123"]


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_params_search(mock_read_csv, load_metadata_mock):
    load_metadata_mock.return_value = {}
    df = pd.DataFrame()
    df["datasetID"] = ["allDatasets", "abc123"]
    mock_read_csv.return_value = df
    erddap_url = "https://erddap.invalid/erddap"
    search = {
        "min_lon": -100,
        "max_lon": -54,
        "min_lat": 19,
        "max_lat": 55,
        "min_time": "2022-01-01",
        "max_time": "2022-11-07",
        "standard_name": "sea_water_temperature",
    }
    cat = ERDDAPCatalog(server=erddap_url, kwargs_search=search)
    search_urls = cat.get_search_urls()
    assert search_urls
    parts = urlparse(search_urls[0])
    assert parts.scheme == "https"
    assert parts.hostname == "erddap.invalid"
    query = dict(parse_qsl(parts.query))
    assert query["minLon"] == "-100"
    assert int(float(query["minTime"])) == 1640995200
    assert query["standard_name"] == "sea_water_temperature"


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_constraints_present_in_source(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    search = {
        "min_time": "2022-01-01",
        "max_time": "2022-11-07",
    }
    cat = ERDDAPCatalog(server=SERVER_URL, kwargs_search=search)
    source = next(cat.values())
    assert source._constraints["time>="] == "2022-01-01"
    assert source._constraints["time<="] == "2022-11-07"

    cat = ERDDAPCatalog(
        server=SERVER_URL, kwargs_search=search, use_source_constraints=False
    )
    source = next(cat.values())
    assert len(source._constraints) == 0


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_with_griddap(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    search = {
        "min_time": "2022-01-01",
        "max_time": "2022-11-07",
    }
    cat = ERDDAPCatalog(server=SERVER_URL, kwargs_search=search, protocol="griddap")
    source = next(cat.values())
    assert isinstance(source, GridDAPSource)


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_with_unsupported_protocol(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    search = {
        "min_time": "2022-01-01",
        "max_time": "2022-11-07",
    }
    mock_read_csv.return_value = single_dataset_catalog
    with pytest.raises(ValueError):
        ERDDAPCatalog(server=SERVER_URL, kwargs_search=search, protocol="fakedap")


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_get_search_urls_by_category(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    kwargs_search = {
        "standard_name": ["air_pressure", "air_temperature"],
        "variableName": ["temp", "airTemp"],
        "search_for": ["kintsugi", "Asano"],
    }
    catalog = ERDDAPCatalog(server=SERVER_URL, kwargs_search=kwargs_search)
    search_urls = catalog.get_search_urls()
    assert len(search_urls) == 6


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_bbox(mock_read_csv, load_metadata_mock, single_dataset_catalog):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    catalog = ERDDAPCatalog(server=SERVER_URL, bbox=(-120.0, 30.0, -100.0, 48.0))
    assert catalog.kwargs_search["min_lon"] == -120.0
    assert catalog.kwargs_search["max_lon"] == -100.0
    assert catalog.kwargs_search["min_lat"] == 30.0
    assert catalog.kwargs_search["max_lat"] == 48.0

    with pytest.raises(TypeError):
        ERDDAPCatalog(server=SERVER_URL, bbox=[0, 0, 1, 1])
    with pytest.raises(ValueError):
        ERDDAPCatalog(server=SERVER_URL, bbox=(0, 0))


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_standard_names_arg(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    catalog = ERDDAPCatalog(
        server=SERVER_URL, standard_names=["air_temperature", "air_pressure"]
    )
    assert catalog.kwargs_search["standard_name"] == ["air_temperature", "air_pressure"]

    with pytest.raises(TypeError):
        ERDDAPCatalog(server=SERVER_URL, standard_names="air_temperature")


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_variable_names_arg(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    catalog = ERDDAPCatalog(server=SERVER_URL, variable_names=["airTemp", "Pair"])
    assert catalog.kwargs_search["variableName"] == ["airTemp", "Pair"]

    with pytest.raises(TypeError):
        ERDDAPCatalog(server=SERVER_URL, variable_names="air_temperature")


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_times_arg(mock_read_csv, load_metadata_mock, single_dataset_catalog):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    catalog = ERDDAPCatalog(
        server=SERVER_URL,
        start_time=datetime(2022, 1, 1),
        end_time=datetime(2022, 12, 1),
    )
    assert catalog.kwargs_search["min_time"] == "2022-01-01T00:00:00Z"
    assert catalog.kwargs_search["max_time"] == "2022-12-01T00:00:00Z"
    with pytest.raises(ValueError):
        ERDDAPCatalog(server=SERVER_URL, start_time="2022-1-1")
    with pytest.raises(ValueError):
        ERDDAPCatalog(server=SERVER_URL, end_time="2022-1-1")
    with pytest.raises(TypeError):
        ERDDAPCatalog(server=SERVER_URL, start_time=np.datetime64("2022-01-01"))
    with pytest.raises(TypeError):
        ERDDAPCatalog(server=SERVER_URL, end_time=np.datetime64("2022-01-01"))


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_search_for_arg(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    catalog = ERDDAPCatalog(server=SERVER_URL, search_for=["ioos", "aoos"])
    assert catalog.kwargs_search["search_for"] == ["ioos", "aoos"]

    with pytest.raises(TypeError):
        ERDDAPCatalog(server=SERVER_URL, search_for="aoos")


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_query_search_for(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    kwargs_search = {
        "search_for": ["air_pressure", "air_temperature"],
    }
    catalog = ERDDAPCatalog(server=SERVER_URL, kwargs_search=kwargs_search)
    search_urls = catalog.get_search_urls()
    url = search_urls[0]
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query))
    assert query["searchFor"] == "air_pressure"

    url = search_urls[1]
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query))
    assert query["searchFor"] == "air_temperature"


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_search_returns_404(mock_read_csv, load_metadata_mock):
    load_metadata_mock.return_value = {}
    mock_read_csv.side_effect = HTTPError(
        code=404, msg="Blah", url=SERVER_URL, hdrs={}, fp=None
    )
    cat = ERDDAPCatalog(server=SERVER_URL)
    assert len(cat) == 0
    mock_read_csv.side_effect = HTTPError(
        code=500, msg="Blah", url=SERVER_URL, hdrs={}, fp=None
    )
    with pytest.raises(HTTPError):
        ERDDAPCatalog(server=SERVER_URL)


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_saving_catalog(
    mock_read_csv, load_metadata_mock, single_dataset_catalog, temporary_catalog
):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    cat = ERDDAPCatalog(server=SERVER_URL)
    cat.save(temporary_catalog)

    cat = intake.open_catalog(temporary_catalog)
    source = next(cat.values())
    assert isinstance(source, TableDAPSource)
    assert source._protocol == "tabledap"
    assert source._server == SERVER_URL
    assert source._dataset_id == "abc123"

    cat = ERDDAPCatalog(server=SERVER_URL, protocol="griddap")
    cat.save(temporary_catalog)

    cat = intake.open_catalog(temporary_catalog)
    source = next(cat.values())
    assert isinstance(source, GridDAPSource)
    assert source._protocol == "griddap"
    assert source._server == SERVER_URL
    assert source._dataset_id == "abc123"


@mock.patch("intake_erddap.utils.get_erddap_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_loading_metadata(
    mock_read_csv, mock_get_erddap_metadata, single_dataset_catalog
):
    mock_read_csv.return_value = single_dataset_catalog
    mock_get_erddap_metadata.return_value = {
        "abc123": {"datasetID": "abc123", "institution": "FOMO"}
    }

    cat = ERDDAPCatalog(server=SERVER_URL)
    assert cat["abc123"].metadata["institution"] == "FOMO"


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_trailing_slash(mock_read_csv, load_metadata_mock, single_dataset_catalog):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    catalog = ERDDAPCatalog(server="http://blah.invalid/erddap/")
    assert catalog.server == "http://blah.invalid/erddap"


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_catalog_query_type_intersection(mock_read_csv, load_metadata_mock):
    data = [
        {
            "datasetID": "ab001",
            "title": "Example dataset",
        },
        {
            "datasetID": "ab002",
            "title": "Example dataset",
        },
        {
            "datasetID": "ab003",
            "title": "Example dataset",
        },
        {
            "datasetID": "ab004",
            "title": "Example dataset",
        },
        {
            "datasetID": "ab005",
            "title": "Example dataset",
        },
        {
            "datasetID": "ab006",
            "title": "Example dataset",
        },
        {
            "datasetID": "ab007",
            "title": "Example dataset",
        },
        {
            "datasetID": "ab008",
            "title": "Example dataset",
        },
    ]

    big_df = pd.DataFrame(data)
    sub_df1 = big_df
    sub_df2 = big_df[:4]
    sub_df3 = big_df[2:7]

    # mock 3 calls
    mock_read_csv.side_effect = [sub_df1, sub_df2, sub_df3]
    catalog = ERDDAPCatalog(
        server=SERVER_URL,
        standard_names=["air_pressure", "air_temperature"],
        variable_names=["sigma"],
        query_type="intersection",
    )
    search_urls = catalog.get_search_urls()
    assert len(search_urls) == 3


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_query_type_invalid(mock_read_csv, load_metadata_mock, single_dataset_catalog):
    load_metadata_mock.return_value = {}
    mock_read_csv.return_value = single_dataset_catalog
    with pytest.raises(ValueError):
        ERDDAPCatalog(server="http://blah.invalid/erddap/", query_type="blah")


@pytest.mark.integration
def test_empty_search_results():
    cat = intake.open_erddap_cat(
        server="https://erddap.sensors.ioos.us/erddap",
        standard_names=["sea_surface_temperature"],
        kwargs_search={
            "min_lon": -156.48529052734375,
            "max_lon": -148.9251251220703,
            "min_lat": 56.70049285888672,
            "max_lat": 61.524776458740234,
            "min_time": "2022-04-30T00:00:00.000000000",
            "max_time": "2022-12-15T23:00:00.000000000",
        },
    )
    assert len(cat) == 0


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_empty_catalog(mock_read_csv, load_metadata_mock, single_dataset_catalog):
    load_metadata_mock.return_value = {}
    resp = mock.Mock()
    resp.status_code = 404
    mock_read_csv.side_effect = requests.exceptions.HTTPError(response=resp)

    cat = ERDDAPCatalog(
        server="http://blah.invalid/erddap", standard_names=["air_temperature"]
    )
    assert len(cat) == 0
    mock_read_csv.assert_called()

    resp = mock.Mock()
    resp.status_code = 500
    mock_read_csv.side_effect = requests.exceptions.HTTPError(response=resp)
    with pytest.raises(requests.exceptions.HTTPError):
        ERDDAPCatalog(
            server="http://blah.invalid/erddap", standard_names=["air_temperature"]
        )


@mock.patch("intake_erddap.erddap_cat.ERDDAPCatalog._load_metadata")
@mock.patch("intake_erddap.cache.CacheStore.read_csv")
def test_empty_catalog_with_intersection(
    mock_read_csv, load_metadata_mock, single_dataset_catalog
):
    load_metadata_mock.return_value = {}
    resp = mock.Mock()
    resp.status_code = 404
    mock_read_csv.side_effect = requests.exceptions.HTTPError(response=resp)

    cat = ERDDAPCatalog(
        server="http://blah.invalid/erddap",
        standard_names=["air_temperature"],
        query_type="intersection",
    )
    assert len(cat) == 0
    mock_read_csv.assert_called()
