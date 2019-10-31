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

This project provides an opinionated way for users to load datasets from these catalogs into the scientific Python ecosystem. It uses the intake-xarray plugin and supports several file formats including GeoTIFF, netCDF, GRIB, and OpenDAP.

## Installation

intake-stac has a few [requirements](requirements.txt), such as [Intake](https://intake.readthedocs.io), [intake-xarray](https://intake-xarray.readthedocs.io/) and [sat-stac](https://github.com/sat-utils/sat-stac). Intake-stac can be installed in any of the following ways:

Using conda:

```bash
$ conda install -c conda-forge intake-stac
```

Using Pip:

```bash
$ pip install intake-stac
```

Or from the source repository:

```bash
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
| 0.[1,2].x| 0.6.x |

### Running the tests

To run the tests some additional packages need to be installed from the `requirements-dev.txt` file.

```
$ pip install -r requirements-dev.txt
$ pytest -v -s --cov intake-stac --cov-report term-missing
```

## About
[intake-stac](https://github.com/pangeo-data/intake-stac) was created as part of the [Pangeo](http://pangeo.io) initiative under support from the NASA-ACCESS program.  See the initial [design document](https://hackmd.io/cyJZkjV5TCWTJg1mUAoEVA).
