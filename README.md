Intake-stac
===========

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pangeo-data/intake-stac/master?filepath=examples?urlpath=lab)
[![Build Status](https://travis-ci.org/pangeo-data/intake-stac.svg?branch=master)](https://travis-ci.org/pangeo-data/intake-stac)
[![Documentation Status](https://readthedocs.org/projects/intake-stac/badge/?version=latest)](https://intake-stac.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/pangeo-data/intake-stac/branch/master/graph/badge.svg)](https://codecov.io/gh/pangeo-data/intake-stac)

This is an [intake](https://intake.readthedocs.io/en/latest)
data source for [STAC](https://stacspec.org/) catalogs.

The SpatioTemporal Asset Catalog (STAC) specification provides a common language to describe a range of geospatial information, so it can more easily be indexed and discovered. A 'spatiotemporal asset' is any file that represents information about the earth captured in a certain space and time.

Two examples of these catalogs are:

- https://planet.stac.cloud/?t=catalogs
- https://landsat-stac.s3.amazonaws.com/catalog.json

Radient Earth keeps track of a more complete listing of STAC implementations [here](https://github.com/radiantearth/stac-spec/blob/master/implementations.md).

This project provides an opinionated way for users to load datasets from these catalogs into the scientific Python ecosystem.
Currently it uses the intake-xarray pluging and support datatypes including GeoTIFF, netCDF, GRIB, and OpenDAP. Future formats could include plain shapefile data and more.

## Requirements
```
intake >= 0.5.1
intake-xarray >= 0.3.0
sat-stac >= 0.1.3
```

## Installation

`intake-stac` will eventually be published on PyPI. For now, you can point to xarray
You can install it by running the following in your terminal:
```bash
pip install git+https://github.com/pangeo-data/intake-stac
```

You can test the functionality by opening the example notebooks in the `examples/` directory:

## Usage

The package can be imported using
```python
from intake_stac import StacCatalog, StacCollection, StacItem
```

### Loading a catalog

You can load data from a STAC catalog by providing the URL to valid STAC catalog entry:
```python
catalog = StacCatalog('https://storage.googleapis.com/pdd-stac/disasters/catalog.json', 'planet-disaster-data')
list(catalog)
```

You can also point to STAC Collections or Items. Each constructor returns a Intake Catalog with the top level corresponding to the STAC object used for initialization.

```python
stac_cat = StacCatalog('https://landsat-stac.s3.amazonaws.com/catalog.json', 'landsat-stac')
collection_cat = StacCollection('https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json', 'landsat-8')
items_cat = StacItem('https://landsat-stac.s3.amazonaws.com/landsat-8-l1/111/111/2018-11-30/LC81111112018334LGN00.json', 'LC81111112018334LGN00')
```

Intake-Stac uses [sat-stac](https://github.com/sat-utils/sat-stac) to parse STAC objects. You can also pass `satstac` objects (e.g. `satstac.Collection`) directly to the Intake-Stac constructors: 

```python
import satstac
col = satstac.Collection.open('https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json')
collection_cat = StacCollection(col, 'landsat-8')
```

### Using the catalog

Once you have a catalog, you can display its entries by iterating through its contents:

```python
for entry_id, entry in catalog.items():
    display(entry)
```

If the catalog has too many entries to comfortably print all at once,
you can narrow it by searching for a term (e.g. 'thumbnail'):
```python
for entry_id, entry in catalog.search('thumbnail').items():
  display(entry)
```

### Loading a dataset
Once you have identified a dataset, you can load it into a `xarray.DataArray` using `to_dask()`:

```python
da = entry.to_dask()
```

## Roadmap

This project is in its early days. We started with [this](https://hackmd.io/cyJZkjV5TCWTJg1mUAoEVA) design document and have been working from there.
