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



Intake is a lightweight set of tools for loading and sharing data in data
science projects. Intake ERDDAP provides a set of integrations for ERDDAP.

- Quickly identify all datasets from an ERDDAP service in a geographic region,
  or containing certain variables.
- Produce a pandas DataFrame for a given dataset or query.
- Get an xarray Dataset for the Gridded datasets.

The Key features are:

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
- (optional but highly recommended) [mamba](https://mamba.readthedocs.io/en/latest/) Hint: `conda install -c conda-forge mamba`

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


## Examples

To create an intake catalog for all of the ERDDAP's TableDAP offerings use:

```python
import intake
catalog = intake.open_erddap_cat(
    server="https://erddap.sensors.ioos.us/erddap"
)
```


The catalog objects behave like a dictionary with the keys representing the
dataset's unique identifier within ERDDAP, and the values being the
`TableDAPSource` objects. To access a source object:

```python
source = catalog["datasetid"]
```

From the source object, a pandas DataFrame can be retrieved:

```python
df = source.read()
```

Consider a case where you need to find all wind data near Florida:

```python
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
```


<table class="align-default">
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
