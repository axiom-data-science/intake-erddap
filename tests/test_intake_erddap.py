import intake
import pandas as pd
import pytest

from intake_erddap import (
    ERDDAPSource,
    ERDDAPSourceAutoPartition,
    ERDDAPSourceManualPartition,
)

from .utils import df, df2


def intake_init():
    # pytest imports this package last, so plugin is not auto-added
    intake.registry["erddap"] = ERDDAPSource
    intake.registry["erddap_auto"] = ERDDAPSourceAutoPartition
    intake.registry["erddap_manual"] = ERDDAPSourceManualPartition


@pytest.mark.skip(reason="Legacy tests")
def test_simple():
    server = "https://cioosatlantic.ca/erddap"
    dataset_id = "SMA_bay_of_exploits"

    d2 = ERDDAPSource(server, dataset_id).read()

    print(len(d2))
    assert len(d2) > 0


@pytest.mark.skip(reason="Legacy tests")
def test_auto():
    server = "https://cioosatlantic.ca/erddap"
    dataset_id = "SMA_bay_of_exploits"

    assert False

    table, table_nopk, uri = temp_db
    s = ERDDAPSourceAutoPartition(uri, table, index="p", sql_kwargs=dict(npartitions=2))
    assert s.discover()["npartitions"] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)


@pytest.mark.skip(reason="Legacy tests")
def test_manual():
    assert False

    table, table_nopk, uri = temp_db
    s = ERDDAPSourceManualPartition(
        uri,
        "SELECT * FROM " + table,
        where_values=["WHERE p < 20", "WHERE p >= 20"],
        sql_kwargs=dict(index_col="p"),
    )
    assert s.discover()["npartitions"] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)
