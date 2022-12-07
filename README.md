_forked from [https://github.com/jmunroe/intake-erddap](https://github.com/jmunroe/intake-erddap)_.

Intake-ERDDAP
=============

[![Build Status](https://img.shields.io/github/workflow/status/axiom-data-science/intake-erddap/Tests?logo=github&style=for-the-badge)](https://github.com/axiom-data-science/intake-erddap/actions)
[![Code Coverage](https://img.shields.io/codecov/c/github/axiom-data-science/intake-erddap.svg?style=for-the-badge)](https://codecov.io/gh/axiom-data-science/intake-erddap)
[![License:BSD](https://img.shields.io/badge/License-BSD--2%20Clause-blue.svg?style=for-the-badge)](https://opensource.org/licenses/BSD-2-Clause)
[![Code Style Status](https://img.shields.io/github/workflow/status/axiom-data-science/intake-erddap/linting%20with%20pre-commit?label=Code%20Style&style=for-the-badge)](https://github.com/axiom-data-science/intake-erddap/actions)
[![Python Package Index](https://img.shields.io/pypi/v/intake-erddap.svg?style=for-the-badge)](https://pypi.org/project/intake-erddap)



ERDDAP Plugin for Intake

Copyright 2022 Axiom Data Science

See LICENSE

Copyright 2022 James Munroe

For changes prior to 2022-10-19, all contributions are Copyright James Munroe, see PREV-LICENSE.


## User Installation

_TODO_

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
