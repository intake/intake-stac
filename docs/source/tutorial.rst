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

    url = 'https://raw.githubusercontent.com/cholmes/sample-stac/master/stac/catalog.json'
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

Intake-Stac uses `sat-stac <https://github.com/sat-utils/sat-stac>`_ to parse
STAC objects. You can also pass ``satstac`` objects (e.g.
``satstac.Collection``) directly to the Intake-stac constructors:

.. ipython:: python
    :verbatim:

    import satstac

    col = satstac.Collection.open(f'{root_url}/catalog.json')
    collection_cat = intake.open_stac_collection(col)

Using the catalog
-----------------

Once you have a catalog, you can display its entries by iterating through its
contents:

.. ipython:: python

    print(list(catalog))
    cat = catalog['hurricane-harvey']

    print(list(cat))
    subcat = cat['hurricane-harvey-0831']

    items = list(subcat)
    print(items)



When you locate an item of interest, you have access to metadata and methods to load assets into Python objects

.. ipython:: python

    item = subcat['Houston-East-20170831-103f-100d-0f4f-RGB']
    print(type(item))
    print(item.metadata)

    assets = list(item)
    print(assets)

    asset = item['thumbnail']
    print(type(asset))
    print(asset.urlpath)


If the catalog has too many entries to comfortably print all at once,
you can narrow it by searching for a term (e.g. 'thumbnail'):

.. ipython:: python

    for id, entry in subcat.search('thumbnail').items():
        print(id)

    asset = subcat['Houston-East-20170831-103f-100d-0f4f-RGB.thumbnail']
    print(asset.urlpath)


Loading a dataset
-----------------

Once you have identified a dataset, you can load it into a ``xarray.DataArray``
using Intake's `to_dask()` method. This reads only metadata, and streams values over the network when required by computations or visualizations:

.. ipython:: python

    da = asset.to_dask()
    display(da)


Working with `sat-search`
-------------------------

Intake-stac integrates with `sat-search` to faciliate dynamic search and
discovery of assets through a STAC-API. To begin, construct a search query
using `sat-search`:

.. ipython:: python

    import satsearch
    print(satsearch.__version__)
    URL='https://earth-search.aws.element84.com/v0'
    results = satsearch.Search.search(
        url=URL,
        collections=['landsat-8-l1-c1'],
        bbox=[43.16, -11.32, 43.54, -11.96]
    )
    items = results.items()
    items

In the code section above, `items` is a `satstac.ItemsCollection` object.
Intake-stac can turn this object into an Intake catalog:

.. ipython:: python

    catalog = intake.open_stac_item_collection(items)
    list(catalog)
