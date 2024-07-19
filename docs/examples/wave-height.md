---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.16.3
kernelspec:
  display_name: Python
  language: python
  name: python3
---

Example: Investigating Significant Wave Height - Southern California
====================================================================

```{code-cell} ipython3
:tags: [hide-cell]

import intake_erddap
# import intake
import numpy as np

import cartopy.crs as ccrs

import pandas as pd

from matplotlib import pyplot as plt
from shapely import geometry

from datetime import datetime

def figure(*args, figsize=(18, 8), facecolor='white', **kwargs):
    return plt.subplots(*args, figsize=figsize, facecolor=facecolor, **kwargs)
```

Here's an example of finding _all_ stations that have significant wave height from the main IOOS ERDDAP server.

```{code-cell} ipython3
server = 'https://erddap.sensors.ioos.us/erddap'
cat = intake_erddap.ERDDAPCatalogReader(
    server=server,
    standard_names=["sea_surface_wind_wave_significant_height"]
).read()
```

```{code-cell} ipython3
df = pd.DataFrame([cat[i].metadata for i in list(cat)])
sub_df = df[['datasetID', 'minTime', 'maxTime', 'title']][:5]
sub_df.style.set_table_attributes('class="dataframe docutils"').hide(axis="index")
```

We can plot the locations of these stations on the globe.

```{code-cell} ipython3
fig, ax = figure(subplot_kw=dict(projection=ccrs.PlateCarree()))
ax.coastlines()
ax.gridlines(draw_labels=True)
ax.scatter(df['minLongitude'], df['minLatitude'])
```

Since our region of interest is off the coast of Southern California, we'll specify a bounding box and highlight the stations that intersect our region.

```{code-cell} ipython3
# Southern California Region
bbox = (-122.42, 32.04, -115.40, 35.28)
box = geometry.box(*bbox)

fig, ax = figure(subplot_kw=dict(projection=ccrs.PlateCarree()))
ax.coastlines()
ax.gridlines(draw_labels=True)
ax.scatter(df['minLongitude'], df['minLatitude'])
ax.add_geometries([box], facecolor='red', alpha=0.4, crs=ccrs.PlateCarree())
ax.set_extent([-130., -60., 20., 45.], crs=ccrs.PlateCarree())
```

We can pass this bounding box directly to the ERDDAP Catalog constructor, as well as limit our query only to stations that contain data after 2014 and through 2017. We also will limit the data returned to the variable (through the `variables` keyword) we are searching for plus basic variables (time, longitude, latitude, and depth):

```{code-cell} ipython3
cat = intake_erddap.ERDDAPCatalogReader(
    server=server,
    bbox=bbox,
    start_time=datetime(2014, 1, 1),
    end_time=datetime(2018,1,1),
    standard_names=["sea_surface_wave_significant_height"],
    variables=["sea_surface_wave_significant_height"],
    dropna=True,
).read()

len(cat)
```

```{code-cell} ipython3
df = pd.DataFrame([cat[i].metadata for i in list(cat)])
sub_df = df[['datasetID', 'minTime', 'maxTime', 'title']]
sub_df.style.set_table_attributes('class="dataframe docutils"').hide(axis="index")
```

```{code-cell} ipython3
# Southern California Region
bbox = (-122.42, 32.04, -115.40, 35.28)
box = geometry.box(*bbox)

fig, ax = figure(subplot_kw=dict(projection=ccrs.PlateCarree()))
ax.coastlines()
ax.gridlines(draw_labels=True)
ax.scatter(df['minLongitude'], df['minLatitude'])
ax.set_title("Station Locations")
```

We can now interrogate each of those stations and get a timeseries for the significant wave height data. We'll use the first four that contain wave height data.


```{code-cell} ipython3
# Just get 4 that aren't empty
stations = {}
for dataset_id in list(cat):
    df = cat[dataset_id].read()
    if len(df) > 0:
        stations[dataset_id] = df
        if len(stations) == 4:
            break
```

```{code-cell} ipython3

fig, axs = figure(nrows=len(stations), figsize=(15,10), sharex=True, sharey=True)

for i, (dataset_id, df) in enumerate(stations.items()):
    ax = axs[i]
    df.plot(ax=ax, x='time (UTC)', y='sea_surface_wave_significant_height (m)', fontsize=14, rot=30,
            title=f'{dataset_id} Significant Wave Height (m)', legend=False, xlabel="")
    ax.grid()

fig.tight_layout(pad=1)
```
