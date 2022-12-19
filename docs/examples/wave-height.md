---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python
  language: python
  name: python3
---

Example: Investigating Significant Wave Height - Southern California
====================================================================

```{code-cell} ipython3
---
tags: [hide-cell]
---
import intake_erddap
import intake
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
cat = intake.open_erddap_cat(
    server=server,
    standard_names=["sea_surface_wind_wave_significant_height"]
)
```

```{code-cell} ipython3
df = pd.DataFrame([i.metadata for i in cat.values()])
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

We can pass this bounding box directly to the ERDDAP Catalog constructor, as well as limit our query only to stations that contain data after 2014:

```{code-cell} ipython3
cat = intake.open_erddap_cat(
    server=server,
    bbox=bbox,
    start_time=datetime(2014, 1, 1),
    standard_names=["sea_surface_wind_wave_significant_height"]
)

len(cat)
```

```{code-cell} ipython3
df = pd.DataFrame([i.metadata for i in cat.values()])
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

We can now interrogate each of those stations and get a timeseries for the significant wave height data.

```{code-cell} ipython3
# Just get 4
stations = list(cat)[:4]

fig, axs = figure(nrows=len(stations), figsize=(18,18))

for i, dataset_id in enumerate(stations):
    ax = axs[i]
    source = cat[dataset_id]
    df = source.read()
    t = df['time (UTC)'].astype('M8[s]')
    sig_wave_height = df['sea_surface_wave_significant_height (m)']
    ax.plot(t, sig_wave_height)
    ax.set_title(f'{dataset_id} Significant Wave Height (m)')
    ax.set_xlim(np.datetime64('2014-01-01'), np.datetime64('2022-12-01'))
    ax.grid()
fig.tight_layout(pad=1)
```
