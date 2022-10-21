#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for generic and utility functions."""
from intake_erddap import utils


def test_get_project_version():
    version = utils.get_project_version()
    assert version is not None
