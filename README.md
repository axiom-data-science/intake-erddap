_forked from [https://github.com/jmunroe/intake-erddap](https://github.com/jmunroe/intake-erddap)_.

Intake-ERDDAP
=============

Copyright 2022 Axiom Data Science

See LICENSE

Copyright 2022 James Munroe

For changes prior to 2022-10-19, all contributions are Copyright James Munroe, see PREV-LICENSE.

[![Build Status](https://img.shields.io/github/actions/workflow/status/axiom-data-science/intake-erddap/test.yaml?branch=main&logo=github&style=for-the-badge)](https://github.com/axiom-data-science/intake-erddap/actions/workflows/test.yaml)
[![Code Coverage](https://img.shields.io/codecov/c/github/axiom-data-science/intake-erddap.svg?style=for-the-badge)](https://codecov.io/gh/axiom-data-science/intake-erddap)
[![License:BSD](https://img.shields.io/badge/License-BSD--2%20Clause-blue.svg?style=for-the-badge)](https://opensource.org/licenses/BSD-2-Clause)
[![Code Style Status](https://img.shields.io/github/actions/workflow/status/axiom-data-science/intake-erddap/linting.yaml?branch=main&label=Code%20Style&style=for-the-badge)](https://github.com/axiom-data-science/intake-erddap/actions/workflows/linting.yaml)
[![Python Package Index](https://img.shields.io/pypi/v/intake-erddap.svg?style=for-the-badge)](https://pypi.org/project/intake-erddap)

[![Read The Docs](https://readthedocs.org/projects/intake-erddap/badge/?version=latest&style=for-the-badge)](https://intake-erddap.readthedocs.io)


[Check out our Read The Docs page for additional documentation](https://intake-erddap.readthedocs.io)



Intake is a lightweight set of tools for loading and sharing data in data science projects. Intake ERDDAP provides a set of integrations for ERDDAP.

- Quickly identify all datasets from an ERDDAP service in a geographic region, or containing certain variables.
- Produce a pandas DataFrame for a given dataset or query.
- Get an xarray Dataset for the Gridded datasets.

The key features are:

 - Pandas DataFrames for any TableDAP dataset.
 - xarray Datasets for any GridDAP datasets.
 - Query by any or all:
    - bounding box
    - time
    - CF `standard_name`
    - variable name
    - Plaintext Search term
 - Save catalogs locally for future use.


## User Installation

In the very near future, we will be offering the project on conda. Currently the
project is available on PyPI, so it can be installed using `pip`

      pip install intake-erddap

## Developer Installation

### Prerequisites

The following are prerequisites for a developer environment for this project:

- [conda](https://docs.conda.io/en/latest/miniconda.html)
- (optional but highly recommended) [mamba](https://mamba.readthedocs.io/en/latest/). Hint: `conda install -c conda-forge mamba`

Note: if `mamba` isn't installed, replace all instances of `mamba` in the following instructions with `conda`.

1. Create the project environment with:
   ```
   mamba env update -f environment.yml
   ```

2. Install the development environment dependencies:
   ```
   mamba env update -f dev-environment.yml
   ```

3. Activate the new virtual environment:
   ```
   conda activate intake-erddap
   ```

4. Install the project to the virtual environment:
   ```
   pip install -e .
   ```

Note that you need to install with `pip install .` once to get the `entry_points` correct too.

## Examples

To create an `intake` catalog for all of the ERDDAP's TableDAP offerings use:

```python
import intake_erddap
catalog = intake_erddap.ERDDAPCatalogReader(
    server="https://erddap.sensors.ioos.us/erddap"
).read()
```


The catalog objects behave like a dictionary with the keys representing the dataset's unique identifier within ERDDAP, and the values being the `TableDAPReader` objects. To access a Reader object (for a single dataset, in this case for dataset_id "aoos_204"):

```python
dataset = catalog["aoos_204"]
```

From the reader object, a pandas DataFrame can be retrieved:

```python
df = dataset.read()
```

Find other dataset_ids available with

```python
list(catalog)
```

Consider a case where you need to find all wind data near Florida:

```python
import intake_erddap
from datetime import datetime
bbox = (-87.84, 24.05, -77.11, 31.27)
catalog = intake_erddap.ERDDAPCatalogReader(
   server="https://erddap.sensors.ioos.us/erddap",
   bbox=bbox,
   intersection="union",
   start_time=datetime(2022, 1, 1),
   end_time=datetime(2023, 1, 1),
   standard_names=["wind_speed", "wind_from_direction"],
   variables=["wind_speed", "wind_from_direction"],
).read()

dataset_id = list(catalog)[0]
print(dataset_id)
df = catalog[dataset_id].read()
```

Using the `standard_names` input with `intersection="union"` searches for datasets that have both "wind_speed" and "wind_from_direction". Using the `variables` input subsequently narrows the dataset to only those columns, plus "time", "latitude", "longitude", and "z".

```python
                 time (UTC)  latitude (degrees_north)  ...  wind_speed (m.s-1)  wind_from_direction (degrees)
0      2022-01-01T00:00:00Z                    28.508  ...                 3.6                          126.0
1      2022-01-01T00:10:00Z                    28.508  ...                 3.8                          126.0
2      2022-01-01T00:20:00Z                    28.508  ...                 3.6                          124.0
3      2022-01-01T00:30:00Z                    28.508  ...                 3.4                          125.0
4      2022-01-01T00:40:00Z                    28.508  ...                 3.5                          124.0
...                     ...                       ...  ...                 ...                            ...
52524  2022-12-31T23:20:00Z                    28.508  ...                 5.9                          176.0
52525  2022-12-31T23:30:00Z                    28.508  ...                 6.8                          177.0
52526  2022-12-31T23:40:00Z                    28.508  ...                 7.2                          175.0
52527  2022-12-31T23:50:00Z                    28.508  ...                 7.4                          169.0
52528  2023-01-01T00:00:00Z                    28.508  ...                 8.1                          171.0

[52529 rows x 6 columns]
```
