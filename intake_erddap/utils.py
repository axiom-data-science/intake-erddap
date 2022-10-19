#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Utility functions."""


def get_project_version() -> str:
    """Return the project version.

    This function resolves circular import problems with version.
    """
    from intake_erddap import __version__

    return __version__
