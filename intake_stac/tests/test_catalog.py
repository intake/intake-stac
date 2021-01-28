import sys

import intake
import pystac
import pytest
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from intake_stac import StacCatalog, StacCollection, StacItem, StacItemCollection
from intake_stac.catalog import StacEntry

# NOTE: reconfig tests to not require network?
# sat-stac examples URLs
sat_stac_repo = 'https://raw.githubusercontent.com/sat-utils/sat-stac/master'
cat_url = f'{sat_stac_repo}/test/catalog/catalog.json'
col_url = f'{sat_stac_repo}/test/catalog/eo/sentinel-2-l1c/catalog.json'
item_url = f'{sat_stac_repo}/test/catalog/eo/landsat-8-l1/item.json'

# pystac examples
pystac_repo = 'https://raw.githubusercontent.com/stac-utils/pystac/develop/tests/data-files'
# or /1.0.0-beta.2/catalog-spec/examples
# cat_url = f'{pystac_repo}/catalogs/planet-example-1.0.0-beta.2/collection.json'
# col_url = f'{pystac_repo}/data-files/item/sample-item.json'
# item_url = f'{pystac_repo}/data-files/item/sample-item.json'
itemcol_url = (
    f'{pystac_repo}/examples/1.0.0-beta.2/extensions/single-file-stac/examples/example-search.json'
)


@pytest.fixture(scope='module')
def pystac_cat():
    return pystac.Catalog.from_file(cat_url)


@pytest.fixture(scope='module')
def pystac_col():
    return pystac.Collection.from_file(col_url)


@pytest.fixture(scope='module')
def pystac_item():
    return pystac.Item.from_file(item_url)


@pytest.fixture(scope='module')
def pystac_itemcol():
    return pystac.read_file(itemcol_url)


@pytest.fixture(scope='module')
def intake_stac_cat():
    return StacCatalog.from_url(cat_url)


def test_init_catalog_from_url():
    cat = StacCatalog(cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.name == 'stac-catalog'
    assert cat.discover()['container'] == 'catalog'
    assert int(cat.metadata['stac_version'][0]) >= 1

    cat = StacCatalog.from_url(cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.name == 'stac-catalog'
    assert cat.discover()['container'] == 'catalog'
    assert int(cat.metadata['stac_version'][0]) >= 1

    # test kwargs are passed through
    cat = StacCatalog.from_url(cat_url, name='intake-stac-test')
    assert 'intake-stac-test' == cat.name


def test_init_catalog_from_pystac_obj(pystac_cat):
    cat = StacCatalog(pystac_cat)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.discover()['container'] == 'catalog'
    assert cat.name == 'stac-catalog'
    assert cat.name == pystac_cat.id

    # test kwargs are passed through
    cat = StacCatalog(pystac_cat, name='intake-stac-test')
    assert 'intake-stac-test' == cat.name


def test_init_catalog_with_wrong_type_raises(pystac_cat):
    with pytest.raises(ValueError):
        StacCollection(pystac_cat)


def test_init_catalog_with_bad_url_raises():
    # json.decoder.JSONDecodeError or FileNotFoundError
    with pytest.raises(Exception):
        StacCatalog('https://raw.githubusercontent.com/')


def test_serialize(intake_stac_cat):
    cat_str = intake_stac_cat.serialize()
    assert isinstance(cat_str, str)


def test_cat_entries(intake_stac_cat):
    assert list(intake_stac_cat)
    assert all([isinstance(v, (LocalCatalogEntry, Catalog)) for _, v in intake_stac_cat.items()])


def test_cat_name_from_pystac_catalog_id(intake_stac_cat):
    assert intake_stac_cat.name == 'stac-catalog'


@pytest.mark.skip(reason='no need for separate Collection type?')
def test_cat_from_collection(pystac_col):
    cat = StacCollection(pystac_col)
    assert 'S2B_25WFU_20200610_0_L1C' in cat
    assert 'B05' in cat.S2B_25WFU_20200610_0_L1C


# @pytest.mark.skip(reason="revist this after figuring out items")
def test_cat_from_item_collection(pystac_itemcol):
    print(pystac_itemcol.ext)
    cat = StacItemCollection(pystac_itemcol)
    assert 'LC81920292019008LGN00' in cat
    assert 'B5' in cat.LC81920292019008LGN00


def test_cat_from_item(pystac_item):
    cat = StacItem(pystac_item)
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
