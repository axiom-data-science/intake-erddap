
``intake-erddap`` Python API
=============================

.. toctree::
   :maxdepth: 2
   :caption: Documentation


``intake-erddap`` catalog
-------------------------


.. autoclass:: intake_erddap.erddap_cat.ERDDAPCatalog
   :members: get_client, get_search_urls

``intake-erddap`` source
------------------------


.. autoclass:: intake_erddap.erddap.ERDDAPReader
   :members: get_client

.. autoclass:: intake_erddap.erddap.TableDAPReader
   :members: read, read_partition, read_chunked

.. autoclass:: intake_erddap.erddap.GridDAPReader
   :members: read_partition, read_chunked, to_dask, close
