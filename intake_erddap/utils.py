#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Utility functions."""

from typing import Optional

import cf_pandas as cfp
import pandas as pd

from pandas import DataFrame


def get_project_version() -> str:
    """Return the project version.

    This function resolves circular import problems with version.
    """
    from intake_erddap import __version__

    return __version__


def return_category_options(
    server: str,
    category: Optional[str] = "standard_name",
) -> DataFrame:
    """Find category options for ERDDAP server.

    Parameters
    ----------
    server : str
        ERDDAP server address, for example: "https://erddap.sensors.ioos.us/erddap"
    category : str, optional
        ERDDAP category for filtering results. Default is "standard_name" but another good option is
        "variableName".

    Returns
    -------
    DataFrame
        Column "Category" contains all options for selected category on server. Column "URL" contains
        the link for search results for searching for a given category value.
    """

    df = pd.read_csv(
        f"{server}/categorize/{category}/index.csv?page=1&itemsPerPage=100000"
    )

    return df


def match_key_to_category(
    server: str,
    key: str,
    category: str = "standard_name",
    criteria: Optional[dict] = None,
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

    Returns
    -------
    list
        Values from category results that match key, according to the custom criteria.
    """

    df = return_category_options(server, category)
    matching_category_value = cfp.match_criteria_key(
        df["Category"].values, key, criteria=criteria
    )

    return matching_category_value
