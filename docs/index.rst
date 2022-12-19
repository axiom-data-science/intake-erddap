.. gcm-filters documentation master file, created by
   sphinx-quickstart on Tue Jan 12 09:24:23 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to intake-erddap's documentation!
=========================================

.. toctree::
   :maxdepth: 2

   examples
   API <api>
   GitHub repository <https://github.com/axiom-data-science/intake-axds>

Intake ERDDAP
=============

Intake is a lightweight set of tools for loading and sharing data in data
science projects. Intake ERDDAP provides a set of integrations for ERDDAP.

- Quickly identify all datasets from an ERDDAP service in a geographic region,
  or containing certain variables.
- Produce a pandas DataFrame for a given dataset or query.
- Get an xarray Dataset for the Gridded datasets.


.. image:: https://img.shields.io/github/actions/workflow/status/axiom-data-science/intake-erddap/test.yaml?branch=main&logo=github&style=for-the-badge
    :alt: Build Status

.. image:: https://img.shields.io/codecov/c/github/axiom-data-science/intake-erddap.svg?style=for-the-badge
    :alt: Code Coverage

.. image:: https://img.shields.io/badge/License-BSD--2%20Clause-blue.svg?style=for-the-badge
    :alt: License:BSD

.. image:: https://img.shields.io/github/actions/workflow/status/axiom-data-science/intake-erddap/linting.yaml?branch=main&label=Code%20Style&style=for-the-badge
    :alt: Code Style Status

The project is available on `Github <https://github.com/axiom-data-science/intake-erddap/>`_.


TODO: Summary

The Key features are:

 - Pandas DataFrames for any TableDAP dataset.
 - xarray Datasets for any GridDAP datasets.
 - Query by any or all:
    - bounding box
    - time
    - CF ``standard_name``
    - variable name
    - Plaintext Search term
 - Save catalogs locally for future use.


Requirements
------------

- Python >= 3.8

Installation
------------

In the very near future, we will be offering the project on conda. Currently the
project is available on PyPI, so it can be installed using ``pip``::

    pip install intake-erddap


Examples
--------

To create an intake catalog for all of the ERDDAP's TableDAP offerings use::

    import intake
    catalog = intake.open_erddap_cat(
        server="https://erddap.sensors.ioos.us/erddap"
    )


The catalog objects behave like a dictionary with the keys representing the
dataset's unique identifier within ERDDAP, and the values being the
``TableDAPSource`` objects. To access a source object::

    source = catalog["datasetid"]

From the source object, a pandas DataFrame can be retrieved::

    df = source.read()

Scenarios
---------

Consider a case where you need to find all wind data near Florida.::

    import intake
    from datetime import datetime
    bbox = (-87.84, 24.05, -77.11, 31.27)
    catalog = intake.open_erddap_cat(
        server="https://erddap.sensors.ioos.us/erddap",
        bbox=bbox,
        start_time=datetime(2022, 1, 1),
        end_time=datetime(2023, 1, 1),
        standard_names=["wind_speed", "wind_from_direction"],
    )

    df = next(catalog.values()).read()


.. raw:: html

    <table class="docutils align-default">
    <thead>
        <tr style="text-align: right;">
        <th></th>
        <th>time (UTC)</th>
        <th>wind_speed (m.s-1)</th>
        <th>wind_from_direction (degrees)</th>
        </tr>
    </thead>
    <tbody>
        <tr>
        <th>0</th>
        <td>2022-12-14T19:40:00Z</td>
        <td>7.0</td>
        <td>140.0</td>
        </tr>
        <tr>
        <th>1</th>
        <td>2022-12-14T19:20:00Z</td>
        <td>7.0</td>
        <td>120.0</td>
        </tr>
        <tr>
        <th>2</th>
        <td>2022-12-14T19:10:00Z</td>
        <td>NaN</td>
        <td>NaN</td>
        </tr>
        <tr>
        <th>3</th>
        <td>2022-12-14T19:00:00Z</td>
        <td>9.0</td>
        <td>130.0</td>
        </tr>
        <tr>
        <th>4</th>
        <td>2022-12-14T18:50:00Z</td>
        <td>9.0</td>
        <td>130.0</td>
        </tr>
        <tr>
        <th>...</th>
        <td>...</td>
        <td>...</td>
        <td>...</td>
        </tr>
        <tr>
        <th>48296</th>
        <td>2022-01-01T00:40:00Z</td>
        <td>4.0</td>
        <td>120.0</td>
        </tr>
        <tr>
        <th>48297</th>
        <td>2022-01-01T00:30:00Z</td>
        <td>3.0</td>
        <td>130.0</td>
        </tr>
        <tr>
        <th>48298</th>
        <td>2022-01-01T00:20:00Z</td>
        <td>4.0</td>
        <td>120.0</td>
        </tr>
        <tr>
        <th>48299</th>
        <td>2022-01-01T00:10:00Z</td>
        <td>4.0</td>
        <td>130.0</td>
        </tr>
        <tr>
        <th>48300</th>
        <td>2022-01-01T00:00:00Z</td>
        <td>4.0</td>
        <td>130.0</td>
        </tr>
    </tbody>
    </table>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _reStructuredText: https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html
