import intake
import pytest
import satstac
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from intake_stac import StacCatalog, StacCollection, StacItem
from intake_stac.catalog import StacEntry


@pytest.fixture(scope="module")
def stac_cat_url():
    return 'https://raw.githubusercontent.com/sat-utils/sat-stac/master/test/catalog/catalog.json'  # noqa: F501


@pytest.fixture(scope="module")
def stac_cat_obj(stac_cat_url):
    return satstac.Catalog.open(stac_cat_url)


@pytest.fixture(scope="module")
def stac_collection_obj():
    return satstac.Collection.open('https://raw.githubusercontent.com/sat-utils/sat-stac/master/test/catalog/eo/sentinel-2-l1c/catalog.json')  # noqa: F501


@pytest.fixture(scope="module")
def stac_item_obj():
    return satstac.Item.open('https://raw.githubusercontent.com/sat-utils/sat-stac/master/test/catalog/eo/landsat-8-l1/item.json')  # noqa: F501


@pytest.fixture(scope="module")
def cat(stac_cat_url):
    return StacCatalog.from_url(stac_cat_url)


def test_init_catalog_from_url(stac_cat_url):
    cat = StacCatalog(stac_cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.name == 'stac'
    assert cat.discover()['container'] == 'catalog'

    cat = StacCatalog.from_url(stac_cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.name == 'stac'
    assert cat.discover()['container'] == 'catalog'

    # test kwargs are passed through
    cat = StacCatalog.from_url(stac_cat_url, name='intake-stac-test')
    assert 'intake-stac-test' == cat.name


def test_init_catalog_from_satstac_obj(stac_cat_obj):
    cat = StacCatalog(stac_cat_obj)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.discover()['container'] == 'catalog'
    assert cat.name == 'stac'
    assert cat.name == stac_cat_obj.id

    # test kwargs are passed through
    cat = StacCatalog(stac_cat_obj, name='intake-stac-test')
    assert 'intake-stac-test' == cat.name


def test_init_catalog_with_wrong_type_raises(stac_cat_obj):
    with pytest.raises(ValueError):
        StacCollection(stac_cat_obj)


def test_init_catalog_with_bad_url_raises():
    with pytest.raises(satstac.STACError):
        StacCatalog('foo.bar')


@pytest.mark.xfail(reason='TODO: work on nested serialization')
def test_serialize(cat):
    cat_str = cat.serialize()
    assert isinstance(cat_str, str)


def test_cat_entries(cat):
    assert list(cat)
    assert all([isinstance(v, (LocalCatalogEntry, Catalog))
                for _, v in cat.items()])


def test_cat_name_from_satstac_catalog_id(cat):
    assert cat.name == 'stac'


def test_cat_from_collection(stac_collection_obj):
    cat = StacCollection(stac_collection_obj)
    assert 'L1C_T53MNQ_A017245_20181011T011722' in cat
    assert 'B05' in cat.L1C_T53MNQ_A017245_20181011T011722


def test_cat_from_item(stac_item_obj):
    cat = StacItem(stac_item_obj)
    # weird, why is this different than the name above?
    assert 'B5' in cat


def test_cat_item_stacking(stac_item_obj):
    items = StacItem(stac_item_obj)
    list_of_bands = ['B1', 'B2']
    new_entry = items.stack_bands(list_of_bands)
    new_da = new_entry.to_dask()
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    assert (new_da.band == list_of_bands).all()


def test_cat_item_stacking_dims_of_different_type_raises_error(stac_item_obj):
    items = StacItem(stac_item_obj)
    list_of_bands = ['B1', 'ANG']
    with pytest.raises(ValueError, match=(' bands have different types: '
                                          'image/x.geotiff, text/plain')):
        new_entry = items.stack_bands(list_of_bands)


def test_cat_item_stacking_dims_of_different_size_regrids(stac_item_obj):
    items = StacItem(stac_item_obj)
    list_of_bands = ['B1', 'B10']
    new_entry = items.stack_bands(list_of_bands)
    new_da = new_entry.to_dask()
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    assert (new_da.band == list_of_bands).all()


def test_stac_entry_constructor():
    key = 'B1'
    item = {'href': 'https://landsat-pds.s3.amazonaws.com/c1/L8/120/046/LC08_L1GT_120046_20181012_20181012_01_RT/LC08_L1GT_120046_20181012_20181012_01_RT_B1.TIF',  # noqa: F501
            'type': 'image/x.geotiff',
            'eo:bands': [0],
            'title': 'Band 1 (coastal)'}

    entry = StacEntry(key, item)

    d = entry.describe()

    assert d['name'] == key
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['rasterio']
    assert d['args']['urlpath'] == item['href']
    assert d['description'] == item['title']
    assert d['metadata'] == item


# TODO - Add tests for:
# StacEntry._get_driver()
# StacEntryy._get_args()
# All catalogs ._get_metadata()
