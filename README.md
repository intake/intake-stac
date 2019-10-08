# Intake-stac

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pangeo-data/intake-stac/master?filepath=examples?urlpath=lab)
[![Build Status](https://travis-ci.org/pangeo-data/intake-stac.svg?branch=master)](https://travis-ci.org/pangeo-data/intake-stac)
[![PyPI version](https://badge.fury.io/py/intake-stac.svg)](https://badge.fury.io/py/intake-stac)
[![Documentation Status](https://readthedocs.org/projects/intake-stac/badge/?version=latest)](https://intake-stac.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/pangeo-data/intake-stac/branch/master/graph/badge.svg)](https://codecov.io/gh/pangeo-data/intake-stac)

This is an [intake](https://intake.readthedocs.io/en/latest) data source for [SpatioTemporal Asset Catalogs (STAC)](https://stacspec.org/). The STAC specification provides a common metadata specification, API, and catalog format to describe geospatial assets, so they can more easily indexed and discovered. A 'spatiotemporal asset' is any file that represents information about the earth captured in a certain space and time.

Two examples of STAC catalogs are:

- https://planet.stac.cloud/?t=catalogs
- https://landsat-stac.s3.amazonaws.com/catalog.json

[Radiant Earth](https://radiant.earth) keeps track of a more complete listing of STAC implementations [here](https://github.com/radiantearth/stac-spec/blob/master/implementations.md).

This project provides an opinionated way for users to load datasets from these catalogs into the scientific Python ecosystem. Currently it uses the intake-xarray plugin and supports several file formats including GeoTIFF, netCDF, GRIB, and OpenDAP.


## Installation

intake-stac has a few [requirements](requirements.txt), such as the Intake library. Intake-stac can be installed from Pip or the source repository. 

```bash
$ pip install intake-stac
```

From source repository:

```bash
$ git clone https://github.com/pangeo-data/intake-stac.git
$ cd intake-stac
$ pip install .
```

or

```
$ pip install git+https://github.com/pangeo-data/intake-stac
```

The [examples/](examples/) directory contains some example Jupyter Notebooks that can be used to test the functionality.

#### Versions
To install a specific versions of intake-stac, specify the version in the install command

```bash
pip install intake-stac==0.1.0
```

The table below shows the corresponding versions between intake-stac and STAC:

| sat-stac | STAC  |
| -------- | ----  |
| 0.1.x    | 0.6.x |


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

## Development

The `master` branch contains the last versioned release, and the `development` branch contains the latest version of the code. New Pull Requests should be made to the `development` branch. For additional [contributing guidelines](docs/contributing.rst) see the documentation.

### Running the tests

To run the tests some additional packages need to be installed from the `requirements-dev.txt` file.

```
$ pip install -r requirements-dev.txt
$ pytest -v -s --cov intake-stac --cov-report term-missing
```


## About
[intake-stac](https://github.com/pangeo-data/intake-stac) was created as part of the [Pangeo](http://pangeo.io) initiative.  See the initial [design document](https://hackmd.io/cyJZkjV5TCWTJg1mUAoEVA).
