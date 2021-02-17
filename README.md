# Intake-STAC

![CI](https://github.com/pangeo-data/intake-stac/workflows/CI/badge.svg)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pangeo-data/intake-stac/binder?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fpangeo-data%252Fintake-stac%26urlpath%3Dlab%252Ftree%252Fintake-stac%252Fexamples%26branch%3Dmain)
[![PyPI version](https://badge.fury.io/py/intake-stac.svg)](https://badge.fury.io/py/intake-stac)
[![Documentation Status](https://readthedocs.org/projects/intake-stac/badge/?version=latest)](https://intake-stac.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/pangeo-data/intake-stac/branch/main/graph/badge.svg)](https://codecov.io/gh/pangeo-data/intake-stac)

This is an [Intake](https://intake.readthedocs.io/en/latest) data source for [SpatioTemporal Asset Catalogs (STAC)](https://stacspec.org/). The STAC specification provides a common metadata specification, API, and catalog format to describe geospatial assets, so they can more easily indexed and discovered. A 'spatiotemporal asset' is any file that represents information about the earth captured in a certain space and time.

Intake-STAC provides an opinionated way for users to load Assets from STAC catalogs into the scientific Python ecosystem. It uses the [intake-xarray](https://github.com/intake/intake-xarray) plugin and supports several file formats including GeoTIFF, netCDF, GRIB, and OpenDAP.

## Installation

Intake-STAC has a few [requirements](requirements.txt), such as [Intake](https://intake.readthedocs.io), [intake-xarray](https://intake-xarray.readthedocs.io/) and [sat-stac](https://github.com/sat-utils/sat-stac). Intake-stac can be installed in any of the following ways:

We recommend installing the latest release with `conda`:

```bash
$ conda install -c conda-forge intake-stac
```

Or the latest development version with `pip`:

```bash
$ pip install git+https://github.com/intake/intake-stac
```

## Examples

```python
from intake import open_stac_catalog
catalog_url = 'https://raw.githubusercontent.com/cholmes/sample-stac/master/stac/catalog.json'
cat = open_stac_catalog(catalog_url)
cat['Houston-East-20170831-103f-100d-0f4f-RGB'].metadata
da = cat['Houston-East-20170831-103f-100d-0f4f-RGB']['thumbnail'].to_dask()
da
```

The [examples/](examples/) directory contains some example Jupyter Notebooks that can be used to test the functionality.

#### Versions

To install a specific versions of intake-stac, specify the version in the install command

```bash
pip install intake-stac==0.3.0
```

The table below shows the corresponding versions between intake-stac and STAC:

| intake-stac | STAC  |
| ----------- | ----- |
| 0.2.x       | 0.6.x |
| 0.3.x       | 1.0.x |

## About

[intake-stac](https://github.com/pangeo-data/intake-stac) was created as part of the [Pangeo](http://pangeo.io) initiative under support from the NASA-ACCESS program. See the initial [design document](https://hackmd.io/cyJZkjV5TCWTJg1mUAoEVA).
