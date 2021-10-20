========
Tutorial
========

.. ipython:: python
   :suppress:

    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("ignore")
        import pandas
        import xarray

Intake-stac simply provides a thin interface that combines `sat-stac` and
Intake. It's basic usage is shown below:

To begin, import intake:

.. ipython:: python

    import intake

Loading a catalog
-----------------

You can load data from a STAC Catalog by providing the URL to valid STAC
Catalog:

.. ipython:: python

    url = 'https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/catalog.json'
    catalog = intake.open_stac_catalog(url)
    list(catalog)

You can also point to STAC Collections or Items. Each constructor returns a
Intake Catalog with the top level corresponding to the STAC object used for
initialization:

.. ipython:: python
    :verbatim:

    root_url = 'https://raw.githubusercontent.com/sat-utils/sat-stac/master/test/catalog'
    stac_cat = intake.open_stac_catalog(
        f'{root_url}/catalog.json',
    )
    collection_cat = intake.open_stac_collection(
        f'{root_url}/eo/landsat-8-l1/catalog.json',
    )
    items_cat = intake.open_stac_item(
        f'{root_url}/eo/landsat-8-l1/item.json'
    )

Intake-Stac uses `pystac <https://github.com/stac-utils/pystac>`_ to parse
STAC objects. You can also pass ``pystac`` objects (e.g.
``pystac.Catalog``) directly to the Intake-stac constructors:

.. ipython:: python
    :verbatim:

    import pystac

    pystac_cat = pystac.read_file(f'{root_url}/catalog.json')
    cat = intake.open_stac_catalog(pystac_cat)

Using the catalog
-----------------

Once you have a catalog, you can display its entries by iterating through its
contents:

.. ipython:: python

    print(list(catalog))
    cat = catalog['extensions-collection']

    print(list(cat))
    subcat = cat['proj-example']

    items = list(subcat)
    print(items)



When you locate an item of interest, you have access to metadata and methods to load assets into Python objects

.. ipython:: python

    item = subcat['B1']
    print(type(item))
    print(item.metadata)


Loading a dataset
-----------------

Once you have identified a dataset, you can load it into a ``xarray.DataArray``
using Intake's `to_dask()` method. This reads only metadata, and streams values over the network when required by computations or visualizations:

.. ipython:: python

    da = item.to_dask()
    display(da)


Working with `sat-search`
-------------------------

Intake-stac integrates with `sat-search` to faciliate dynamic search and
discovery of assets through a STAC-API. To begin, construct a search query
using `sat-search`:

.. ipython:: python

    import satsearch
    print(satsearch.__version__)

    bbox = [35.48, -3.24, 35.58, -3.14]
    dates = '2020-07-01/2020-08-15'
    URL='https://earth-search.aws.element84.com/v0'
    results = satsearch.Search.search(url=URL,
                                      collections=['sentinel-s2-l2a-cogs'],
                                      datetime=dates,
                                      bbox=bbox,
                                      sort=['-properties.datetime'])

    # 18 items found
    items = results.items()
    print(len(items))
    items.save('single-file-stac.json')

In the code section above, `items` is a `satstac.ItemsCollection` object.
Intake-stac can turn this object into an Intake catalog:

.. ipython:: python

    catalog = intake.open_stac_item_collection('single-file-stac.json')
    list(catalog)

Using xarray-assets
-------------------

Intake-stac uses the `xarray-assets`_ STAC extension to automatically use the appropriate keywords to load a STAC asset into a data container.

Intake-stac will automatically use the keywords from the `xarray-assets`_ STAC extension, if present, when loading data into a container.
For example, the STAC collection at <https://planetarycomputer.microsoft.com/api/stac/v1/collections/daymet-annual-hi> defines an
asset ``zarr-https`` with the metadata ``"xarray:open_kwargs": {"consolidated": true}"`` to indicate that this dataset should be
opened with the ``consolidated=True`` keyword argument. This will be used automatically by ``.to_dask()``


.. code-block:: python

   >>> collection = intake.open_stac_collection(
   ...     "https://planetarycomputer.microsoft.com/api/stac/v1/collections/daymet-annual-hi"
   ... )

   >>> source = collection.get_asset("zarr-https")
   >>> source.to_dask()
   <xarray.Dataset>
   Dimensions:                  (nv: 2, time: 41, x: 284, y: 584)
   Coordinates:
       lat                      (y, x) float32 dask.array<chunksize=(584, 284), meta=np.ndarray>
       lon                      (y, x) float32 dask.array<chunksize=(584, 284), meta=np.ndarray>
     * time                     (time) datetime64[ns] 1980-07-01T12:00:00 ... 20...
     * x                        (x) float32 -5.802e+06 -5.801e+06 ... -5.519e+06
     * y                        (y) float32 -3.9e+04 -4e+04 ... -6.21e+05 -6.22e+05
   Dimensions without coordinates: nv
   Data variables:
       lambert_conformal_conic  int16 ...
       prcp                     (time, y, x) float32 dask.array<chunksize=(1, 584, 284), meta=np.ndarray>
       swe                      (time, y, x) float32 dask.array<chunksize=(1, 584, 284), meta=np.ndarray>
       time_bnds                (time, nv) datetime64[ns] dask.array<chunksize=(1, 2), meta=np.ndarray>
       tmax                     (time, y, x) float32 dask.array<chunksize=(1, 584, 284), meta=np.ndarray>
       tmin                     (time, y, x) float32 dask.array<chunksize=(1, 584, 284), meta=np.ndarray>
       vp                       (time, y, x) float32 dask.array<chunksize=(1, 584, 284), meta=np.ndarray>
   Attributes:
       Conventions:       CF-1.6
       Version_data:      Daymet Data Version 4.0
       Version_software:  Daymet Software Version 4.0
       citation:          Please see http://daymet.ornl.gov/ for current Daymet ...
       references:        Please see http://daymet.ornl.gov/ for current informa...
       source:            Daymet Software Version 4.0
       start_year:        1980

.. _xarray-assets: https://github.com/stac-extensions/xarray-assets
