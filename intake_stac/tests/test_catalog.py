import os
import sys

import intake
import pytest
import satstac
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from intake_stac import StacCatalog, StacCollection, StacItem, StacItemCollection
from intake_stac.catalog import StacEntry


@pytest.fixture(scope='module')
def stac_cat_url():
    return 'https://raw.githubusercontent.com/sat-utils/sat-stac/master/test/catalog/catalog.json'  # noqa: E501


@pytest.fixture(scope='module')
def stac_cat_obj(stac_cat_url):
    return satstac.Catalog.open(stac_cat_url)


@pytest.fixture(scope='module')
def stac_collection_obj():
    return satstac.Collection.open(
        'https://raw.githubusercontent.com/sat-utils/sat-stac/master/test/catalog/eo/sentinel-2-l1c/catalog.json'  # noqa: E501
    )


@pytest.fixture(scope='module')
def stac_item_collection_obj():
    # TODO: change load -> open,
    # see https://github.com/sat-utils/sat-stac/issues/52
    # use github/sat-utils/sat-stac/master/test/items.json
    return satstac.ItemCollection.load(
        os.path.join(os.path.dirname(__file__), 'items.json')  # noqa: E501
    )


@pytest.fixture(scope='module')
def stac_item_obj():
    return satstac.Item.open(
        'https://raw.githubusercontent.com/sat-utils/sat-stac/master/test/catalog/eo/landsat-8-l1/item.json'  # noqa: E501
    )


@pytest.fixture(scope='module')
def cat(stac_cat_url):
    return StacCatalog.from_url(stac_cat_url)


def test_init_catalog_from_url(stac_cat_url):
    cat = StacCatalog(stac_cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.name == 'stac-catalog'
    assert cat.discover()['container'] == 'catalog'
    assert int(cat.metadata['stac_version'][0]) >= 1

    cat = StacCatalog.from_url(stac_cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.name == 'stac-catalog'
    assert cat.discover()['container'] == 'catalog'
    assert int(cat.metadata['stac_version'][0]) >= 1

    # test kwargs are passed through
    cat = StacCatalog.from_url(stac_cat_url, name='intake-stac-test')
    assert 'intake-stac-test' == cat.name


def test_init_catalog_from_satstac_obj(stac_cat_obj):
    cat = StacCatalog(stac_cat_obj)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.discover()['container'] == 'catalog'
    assert cat.name == 'stac-catalog'
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


def test_serialize(cat):
    cat_str = cat.serialize()
    assert isinstance(cat_str, str)


def test_cat_entries(cat):
    assert list(cat)
    assert all([isinstance(v, (LocalCatalogEntry, Catalog)) for _, v in cat.items()])


def test_cat_name_from_satstac_catalog_id(cat):
    assert cat.name == 'stac-catalog'


def test_cat_from_collection(stac_collection_obj):
    cat = StacCollection(stac_collection_obj)
    assert 'S2B_25WFU_20200610_0_L1C' in cat
    assert 'B05' in cat.S2B_25WFU_20200610_0_L1C


def test_cat_from_item_collection(stac_item_collection_obj):
    cat = StacItemCollection(stac_item_collection_obj)
    assert 'LC81920292019008LGN00' in cat
    assert 'B5' in cat.LC81920292019008LGN00


def test_cat_from_item(stac_item_obj):
    cat = StacItem(stac_item_obj)
    # weird, why is this different than the name above?
    assert 'B5' in cat


def test_cat_item_stacking(stac_item_obj):
    item = StacItem(stac_item_obj)
    list_of_bands = ['B1', 'B2']
    new_entry = item.stack_bands(list_of_bands)
    assert isinstance(new_entry, StacEntry)
    assert new_entry._description == 'B1, B2'
    assert new_entry.name == 'B1_B2'
    new_da = new_entry().to_dask()
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    assert (new_da.band == list_of_bands).all()


def test_cat_item_stacking_using_common_name(stac_item_obj):
    item = StacItem(stac_item_obj)
    list_of_bands = ['coastal', 'blue']
    new_entry = item.stack_bands(list_of_bands)
    assert isinstance(new_entry, StacEntry)
    assert new_entry._description == 'B1, B2'
    assert new_entry.name == 'coastal_blue'
    new_da = new_entry().to_dask()
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    assert (new_da.band == ['B1', 'B2']).all()


def test_cat_item_stacking_dims_of_different_type_raises_error(stac_item_obj):
    item = StacItem(stac_item_obj)
    list_of_bands = ['B1', 'ANG']
    with pytest.raises(ValueError, match=('ANG not found in list of eo:bands in collection')):
        item.stack_bands(list_of_bands)


def test_cat_item_stacking_dims_with_nonexistent_band_raises_error(stac_item_obj,):  # noqa: E501
    item = StacItem(stac_item_obj)
    list_of_bands = ['B1', 'foo']
    with pytest.raises(ValueError, match="'B8', 'B9', 'blue', 'cirrus'"):
        item.stack_bands(list_of_bands)


def test_cat_item_stacking_dims_of_different_size_regrids(stac_item_obj):
    item = StacItem(stac_item_obj)
    list_of_bands = ['B1', 'B8']
    B1_da = item.B1.to_dask()
    assert B1_da.shape == (1, 7791, 7651)
    B8_da = item.B8.to_dask()
    assert B8_da.shape == (1, 15581, 15301)
    new_entry = item.stack_bands(list_of_bands)
    new_da = new_entry().to_dask()
    assert new_da.shape == (2, 15581, 15301)
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    assert (new_da.band == list_of_bands).all()


def test_stac_entry_constructor():
    key = 'B1'
    item = {
        'href': 'https://landsat-pds.s3.amazonaws.com/c1/L8/120/046/LC08_L1GT_120046_20181012_20181012_01_RT/LC08_L1GT_120046_20181012_20181012_01_RT_B1.TIF',  # noqa: E501
        'type': 'image/x.geotiff',
        'eo:bands': [0],
        'title': 'Band 1 (coastal)',
    }

    entry = StacEntry(key, item)

    d = entry.describe()

    assert d['name'] == key
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['rasterio']
    assert d['args']['urlpath'] == item['href']
    assert d['description'] == item['title']
    assert d['metadata'] == item


def test_missing_type():
    key = 'B1'
    item = {
        'href': 'https://landsat-pds.s3.amazonaws.com/c1/L8/120/046/LC08_L1GT_120046_20181012_20181012_01_RT/LC08_L1GT_120046_20181012_20181012_01_RT_B1.TIF',  # noqa: E501
        'type': '',
    }

    entry = StacEntry(key, item)

    d = entry.describe()
    print(d)
    assert d['name'] == key
    assert d['metadata']['type'] == 'application/rasterio'  # default_type
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['rasterio']


def test_unknown_type():
    key = 'B1'
    item = {
        'href': 'https://landsat-pds.s3.amazonaws.com/c1/L8/120/046/LC08_L1GT_120046_20181012_20181012_01_RT/LC08_L1GT_120046_20181012_20181012_01_RT_B1.TIF',  # noqa: E501
        'type': 'unrecognized',
    }

    entry = StacEntry(key, item)

    d = entry.describe()
    print(d)
    assert d['name'] == key
    assert d['metadata']['type'] == 'unrecognized'
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['rasterio']


def test_cat_to_geopandas(stac_item_collection_obj):
    geopandas = pytest.importorskip('geopandas')

    cat = StacItemCollection(stac_item_collection_obj)
    df = cat.to_geopandas()
    assert isinstance(df, geopandas.GeoDataFrame)
    assert len(df) == len(cat._stac_obj)
    assert isinstance(df.geometry, geopandas.GeoSeries)
    assert isinstance(df.geometry.values, geopandas.array.GeometryArray)
    assert isinstance(df.geometry.dtype, geopandas.array.GeometryDtype)
    assert df.crs == {'init': 'epsg:4326'}


@pytest.mark.parametrize('crs', ['IGNF:ETRS89UTM28', 'epsg:26909'])
def test_cat_to_geopandas_crs(crs, stac_item_collection_obj):
    geopandas = pytest.importorskip('geopandas')

    cat = StacItemCollection(stac_item_collection_obj)
    df = cat.to_geopandas(crs=crs)
    assert isinstance(df, geopandas.GeoDataFrame)
    assert len(df) == len(cat._stac_obj)
    assert df.crs == crs


def test_cat_to_missing_geopandas(stac_item_collection_obj, monkeypatch):
    from unittest import mock

    with pytest.raises(ImportError):
        with mock.patch.dict(sys.modules, {'geopandas': None}):
            cat = StacItemCollection(stac_item_collection_obj)
            _ = cat.to_geopandas()
