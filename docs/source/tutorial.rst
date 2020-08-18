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

    catalog = intake.open_stac_catalog(
        'https://storage.googleapis.com/pdd-stac/disasters/catalog.json',
        name='planet-disaster-data'
    )
    list(catalog)

You can also point to STAC Collections or Items. Each constructor returns a
Intake Catalog with the top level corresponding to the STAC object used for
initialization:

.. ipython:: python
    :verbatim:

    stac_cat = intake.open_stac_catalog(
        'https://landsat-stac.s3.amazonaws.com/catalog.json',
        name='landsat-stac'
    )
    collection_cat = intake.open_stac_collection(
        'https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json',
        name='landsat-8'
    )
    items_cat = intake.open_stac_item(
        'https://landsat-stac.s3.amazonaws.com/landsat-8-l1/111/111/2018-11-30/LC81111112018334LGN00.json',
        name='LC81111112018334LGN00'
    )

Intake-Stac uses `sat-stac <https://github.com/sat-utils/sat-stac>`_ to parse
STAC objects. You can also pass ``satstac`` objects (e.g.
``satstac.Collection``) directly to the Intake-stac constructors:

.. ipython:: python
    :verbatim:

    import satstac

    col = satstac.Collection.open(
        'https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json'
    )
    collection_cat = open_stac_collection(col, name='landsat-8')

Using the catalog
-----------------

Once you have a catalog, you can display its entries by iterating through its
contents:

.. ipython:: python

    for id, entry in catalog.items():
        display(entry)

If the catalog has too many entries to comfortably print all at once,
you can narrow it by searching for a term (e.g. 'thumbnail'):

.. ipython:: python

    for id, entry in catalog.search('thumbnail').items():
        display(entry)

Loading a dataset
-----------------

Once you have identified a dataset, you can load it into a ``xarray.DataArray``
using Intake's `to_dask()` method:

.. ipython:: python

    da = entry.to_dask()
    display(da)

Working with `sat-search`
-------------------------

Intake-stac integrates with `sat-search` to faciliate dynamic search and
discovery of assets through a STAC-API. To begin, construct a search query
using `sat-search`:

.. ipython:: python

    import satsearch

    results = satsearch.Search.search(
        collection='landsat-8-l1',
        bbox=[43.16, -11.32, 43.54, -11.96],
        sort=['<datetime'], #earliest scene first
        property=["landsat:tier=T1"])
    items = results.items()
    display(items)

In the code section above, `items` is a `satstac.ItemsCollection` object.
Intake-stac can turn this object into an Intake catalog:

.. ipython:: python

    catalog = intake.open_stac_item_collection(items)
    list(catalog)
