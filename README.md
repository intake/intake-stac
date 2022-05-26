# Intake-STAC

![CI](https://github.com/intake/intake-stac/workflows/CI/badge.svg)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/intake/intake-stac/binder?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fintake%252Fintake-stac%26urlpath%3Dlab%252Ftree%252Fintake-stac%252Fexamples%26branch%3Dmain)
[![PyPI version](https://badge.fury.io/py/intake-stac.svg)](https://badge.fury.io/py/intake-stac)
[![Documentation Status](https://readthedocs.org/projects/intake-stac/badge/?version=latest)](https://intake-stac.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/intake/intake-stac/branch/main/graph/badge.svg?token=8VQEcrFJz9)](https://codecov.io/gh/intake/intake-stac)

This is an [Intake](https://intake.readthedocs.io/en/latest) data source for [SpatioTemporal Asset Catalogs (STAC)](https://stacspec.org/). The STAC specification provides a common metadata specification, API, and catalog format to describe geospatial assets, so they can more easily indexed and discovered. A 'spatiotemporal asset' is any file that represents information about the earth captured in a certain space and time.

Intake-STAC provides an opinionated way for users to load Assets from STAC catalogs into the scientific Python ecosystem. It uses the [intake-xarray](https://github.com/intake/intake-xarray) plugin and supports several file formats including GeoTIFF, netCDF, GRIB, and OpenDAP.

## Installation

Intake-STAC has a few [requirements](requirements.txt), such as [Intake](https://intake.readthedocs.io), [intake-xarray](https://intake-xarray.readthedocs.io/) and [pystac](https://github.com/stac-utils/pystac). Intake-stac can be installed in any of the following ways:

We recommend installing the latest release with `conda`:

```bash
$ conda install -c conda-forge intake-stac
```

Or the latest development version with `pip`:

```bash
$ pip install git+https://github.com/intake/intake-stac
```

## Quickstart

```python
import intake

catalog_url = 'https://www.planet.com/data/stac/catalog.json'
cat = intake.open_stac_catalog(catalog_url)

collection = cat['planet-disaster-data']
subset = collection['hurricane-harvey']['hurricane-harvey-0831']
item = subset['Houston-East-20170831-103f-100d-0f4f-RGB']

da = item['thumbnail'].to_dask()
da
```

The [examples/](examples/) directory contains several Jupyter Notebooks illustrating common workflows.

[STAC Index](https://stacindex.org/catalogs) is a convenient website for finding datasets with STACs

#### Versions

To install a specific version of intake-stac, specify the version in the install command

```bash
pip install intake-stac==0.4.0
```

The table below shows the corresponding versions between intake-stac and STAC:

| intake-stac | STAC        |
| ----------- | ----------- |
| 0.2.x       | 0.6.x       |
| 0.3.x       | 1.0.0-betaX |
| 0.4.x       | 1.0.0       |

## About

[intake-stac](https://github.com/intake/intake-stac) was created as part of the [Pangeo](http://pangeo.io) initiative under support from the NASA-ACCESS program. See the initial [design document](https://hackmd.io/cyJZkjV5TCWTJg1mUAoEVA).
