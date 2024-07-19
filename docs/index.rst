.. gcm-filters documentation master file, created by
   sphinx-quickstart on Tue Jan 12 09:24:23 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to intake-erddap's documentation!
=========================================


Intake ERDDAP
=============

Intake is a lightweight set of tools for loading and sharing data in data
science projects. Intake ERDDAP provides a set of integrations for ERDDAP.

- Quickly identify all datasets from an ERDDAP service in a geographic region,
  or containing certain variables.
- Produce a pandas DataFrame for a given dataset or query.
- Get an xarray Dataset for the Gridded datasets.

The key features are:

- Pandas DataFrames for any TableDAP dataset.
- xarray Datasets for any GridDAP datasets.
- Query by any or all:
   - bounding box
   - time
   - CF ``standard_name``
   - variable name
   - Plaintext Search term
- Save catalogs locally for future use.

Installation
------------

The project is available on PyPI, so it can be installed using ``pip``::

    pip install intake-erddap


.. toctree::
   :maxdepth: 3
   :hidden:

   user_guide
   API <api>
   whats_new
   GitHub repository <https://github.com/axiom-data-science/intake-axds>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _reStructuredText: https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html
