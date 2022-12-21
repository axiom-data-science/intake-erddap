#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Utility functions."""

from logging import getLogger
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import quote_plus, urlencode

import cf_pandas as cfp
import numpy as np
import pandas as pd

from pandas import DataFrame

from intake_erddap.cache import CacheStore


log = getLogger("intake-erddap")


def get_project_version() -> str:
    """Return the project version.

    This function resolves circular import problems with version.
    """
    from intake_erddap import __version__

    return __version__


def return_category_options(
    server: str,
    category: str = "standard_name",
    cache_store: Optional[CacheStore] = None,
) -> DataFrame:
    """Find category options for ERDDAP server.

    Parameters
    ----------
    server : str
        ERDDAP server address, for example: "https://erddap.sensors.ioos.us/erddap"
    category : str, optional
        ERDDAP category for filtering results. Default is "standard_name" but another good option is
        "variableName".
    cache_store : CacheStore
        The cache store to use for caching responses. If one is provided it will
        be used instead of making the requests directly.

    Returns
    -------
    DataFrame
        Column "Category" contains all options for selected category on server. Column "URL" contains
        the link for search results for searching for a given category value.
    """

    url = f"{server}/categorize/{category}/index.csv?page=1&itemsPerPage=100000"
    if cache_store is not None:
        return cache_store.read_csv(url)
    return pd.read_csv(url)


def match_key_to_category(
    server: str,
    key: str,
    category: str = "standard_name",
    criteria: Optional[dict] = None,
    cache_store: Optional[CacheStore] = None,
) -> list:
    """Find category values for server and return match to key.

    Parameters
    ----------
    server : str
        ERDDAP server address, for example: "http://erddap.sensors.ioos.us/erddap"
    key : str
        The custom_criteria key to narrow the search, which will be matched to the category results
        using the custom_criteria that must be set up ahead of time with `cf-pandas`.
    category : str, optional
        ERDDAP category for filtering results. Default is "standard_name" but another good option
        is "variableName".
    criteria : dict, optional
        Criteria to use to map from variable to attributes describing the variable. If user has
        defined custom_criteria, this will be used by default.
    cache_store : CacheStore
        The cache store to use for caching responses. If one is provided it will
        be used instead of making the requests directly.

    Returns
    -------
    list
        Values from category results that match key, according to the custom criteria.
    """

    df = return_category_options(server, category, cache_store=cache_store)
    matching_category_value = cfp.match_criteria_key(
        df["Category"].values, key, criteria=criteria
    )

    return matching_category_value


def as_a_list(value: Any) -> list:
    """Wrap value in a list if it's not already."""
    if not isinstance(value, list):
        return [value]
    return value


def get_erddap_metadata(
    server: str,
    constraints: Mapping[str, Any],
    http_client: Any = None,
    cache_store: Optional[CacheStore] = None,
) -> Mapping[str, dict]:
    """Return a map for all the dataset metadata."""
    if http_client is None:
        import requests

        http_client = requests
    constraints_query = map_constraints_to_tabledap(constraints)
    fields = [
        "datasetID",
        "institution",
        "title",
        "summary",
        "minLongitude",
        "maxLongitude",
        "minLatitude",
        "maxLatitude",
        "minTime",
        "maxTime",
        "griddap",
        "tabledap",
    ]
    url = f"{server}/tabledap/allDatasets.json?" + quote_plus(",".join(fields))
    if constraints_query:
        url += "&" + urlencode(constraints_query)
    if cache_store:  # pragma: no cover
        return parse_erddap_tabledap_response(cache_store.read_json(url))
    resp = http_client.get(url)
    resp.raise_for_status()
    return parse_erddap_tabledap_response(resp.json())


def parse_erddap_tabledap_response(data: dict) -> Mapping[str, dict]:
    """Convert table format into key value mapping."""
    results = {}
    column_names = data["table"]["columnNames"]
    dtypes = data["table"]["columnTypes"]
    for row in data["table"]["rows"]:
        try:
            entry = parse_row(column_names, dtypes, row)
        except TypeError:
            log.warning("Encountered TypeError while parsing row from ERDDAP.")
            log.debug(f"{row}")
        except ValueError:
            log.warning("Encountered ValueError while parsing row from ERDDAP.")
            log.debug(f"{row}")
        if entry is not None:
            results[entry["datasetID"]] = entry
    return results


def parse_row(
    column_names: List[str], dtypes: List[str], row: List[Any]
) -> Optional[Dict[str, Any]]:
    """Parse a row of the ERDDAP Table JSON response."""
    entry: Dict[str, Any] = {}
    for i, key in enumerate(column_names):
        dtype = dtypes[i]
        if dtype in ("double", "float"):
            if row[i] is None:
                value = np.nan
            else:
                value = float(row[i])
        elif dtype in ("long", "int"):
            if row[i] is None:
                log.warning(
                    f"ERDDAP Returned an invalid null value for an integer. Skipping dataset {row[0]}"
                )
                return None
            value = int(row[i])
        else:
            value = row[i]
        entry[key] = value
    return entry


def map_constraints_to_tabledap(constraints: Mapping[str, Any]) -> dict:
    """Transform the constraints dict that this package accepts to an ERDDAP query dict."""
    constraints_query = {}
    if "max_time" in constraints:
        constraints_query["minTime<"] = constraints["max_time"]
    if "min_time" in constraints:
        constraints_query["maxTime>"] = constraints["min_time"]
    if "min_lon" in constraints:
        constraints_query["maxLongitude>"] = constraints["min_lon"]
    if "max_lon" in constraints:
        constraints_query["minLongitude<"] = constraints["max_lon"]
    if "min_lat" in constraints:
        constraints_query["maxLatitude>"] = constraints["min_lat"]
    if "max_lat" in constraints:
        constraints_query["minLatitude<"] = constraints["max_lat"]
    return constraints_query
