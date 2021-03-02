import sys

import intake
import pystac
import pytest
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from intake_stac import StacCatalog, StacCollection, StacItem, StacItemCollection
from intake_stac.catalog import CombinedAssets, StacAsset

# sat-stac examples
# -----
sat_stac_repo = 'https://raw.githubusercontent.com/sat-utils/sat-stac/master'
cat_url = f'{sat_stac_repo}/test/catalog/catalog.json'
col_url = f'{sat_stac_repo}/test/catalog/eo/sentinel-2-l1c/catalog.json'
item_url = f'{sat_stac_repo}/test/catalog/eo/landsat-8-l1/item.json'

# pystac examples
# -----
pystac_repo = 'https://raw.githubusercontent.com/stac-utils/pystac/develop/tests/data-files'
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


def test_cat_from_collection(pystac_col):
    cat = StacCollection(pystac_col)
    subcat_name = 'sentinel-2a-catalog'
    item_name = 'S2B_25WFU_20200610_0_L1C'
    assert cat.name == pystac_col.id
    assert subcat_name in cat
    assert item_name in cat[subcat_name]
    assert 'B04' in cat[subcat_name][item_name]


def test_cat_from_item_collection(pystac_itemcol):
    cat = StacItemCollection(pystac_itemcol)
    assert 'LC80340332018034LGN00' in cat
    assert 'B5' in cat.LC80340332018034LGN00


def test_cat_from_item(pystac_item):
    cat = StacItem(pystac_item)
    assert 'B5' in cat


def test_cat_item_stacking(pystac_item):
    item = StacItem(pystac_item)
    list_of_bands = ['B1', 'B2']
    new_entry = item.stack_bands(list_of_bands)
    assert isinstance(new_entry, CombinedAssets)
    assert new_entry._description == 'B1, B2'
    assert new_entry.name == 'B1_B2'
    new_da = new_entry().to_dask()
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    # relies on path_as_pattern in intake_xarray
    # assert (new_da.band == list_of_bands).all()


def test_cat_item_stacking_using_common_name(pystac_item):
    item = StacItem(pystac_item)
    list_of_bands = ['coastal', 'blue']
    new_entry = item.stack_bands(list_of_bands)
    assert isinstance(new_entry, CombinedAssets)
    assert new_entry._description == 'B1, B2'
    assert new_entry.name == 'coastal_blue'
    new_da = new_entry().to_dask()
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    # relies on path_as_pattern in intake_xarray
    # assert (new_da.band == ['B1', 'B2']).all()


def test_cat_item_stacking_dims_of_different_type_raises_error(pystac_item):
    item = StacItem(pystac_item)
    list_of_bands = ['B1', 'ANG']
    with pytest.raises(ValueError, match=('ANG not found in list of eo:bands in collection')):
        item.stack_bands(list_of_bands)


def test_cat_item_stacking_dims_with_nonexistent_band_raises_error(pystac_item,):  # noqa: E501
    item = StacItem(pystac_item)
    list_of_bands = ['B1', 'foo']
    with pytest.raises(ValueError, match="'B8', 'B9', 'blue', 'cirrus'"):
        item.stack_bands(list_of_bands)


def test_cat_item_stacking_dims_of_different_size_regrids(pystac_item):
    item = StacItem(pystac_item)
    list_of_bands = ['B1', 'B8']
    B1_da = item.B1.to_dask()
    assert B1_da.shape == (1, 7791, 7651)
    B8_da = item.B8.to_dask()
    assert B8_da.shape == (1, 15581, 15301)
    new_entry = item.stack_bands(list_of_bands)
    new_da = new_entry().to_dask()
    assert new_da.shape == (2, 15581, 15301)
    assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']
    # relies on path_as_pattern in intake_xarray
    # assert (new_da.band == list_of_bands).all()


def test_asset_describe(pystac_item):
    item = StacItem(pystac_item)
    key = 'B1'
    asset = item[key]  # gets cataog_dir
    d = asset.describe()

    assert d['name'] == key
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['rasterio']
    assert d['args']['urlpath'] == asset.urlpath
    assert d['description'] == asset.description
    # NOTE: note sure why asset.metadata has 'catalog_dir' key
    # assert d['metadata'] == asset.metadata


def test_asset_missing_type(pystac_item):
    key = 'B1'
    asset = pystac_item.assets.get('B1')
    asset.media_type = ''
    with pytest.warns(Warning, match="STAC Asset"):
        entry = StacAsset(key, asset)
    d = entry.describe()

    assert d['name'] == key
    assert d['metadata']['type'] == 'application/rasterio'  # default_type
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['rasterio']


def test_asset_unknown_type(pystac_item):
    key = 'B1'
    asset = pystac_item.assets.get('B1')
    asset.media_type = 'unrecognized'
    entry = StacAsset(key, asset)
    d = entry.describe()

    assert d['name'] == key
    assert d['metadata']['type'] == 'unrecognized'
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['rasterio']


def test_cat_to_geopandas(pystac_itemcol):
    nfeatures = len(pystac_itemcol.ext['single-file-stac'].features)
    geopandas = pytest.importorskip('geopandas')

    cat = StacItemCollection(pystac_itemcol)
    df = cat.to_geopandas()
    assert isinstance(df, geopandas.GeoDataFrame)
    assert len(df) == nfeatures
    assert isinstance(df.geometry, geopandas.GeoSeries)
    assert isinstance(df.geometry.values, geopandas.array.GeometryArray)
    assert isinstance(df.geometry.dtype, geopandas.array.GeometryDtype)
    epsg = df.crs.to_epsg()
    assert epsg == 4326


@pytest.mark.parametrize('crs', ['IGNF:ETRS89UTM28', 'epsg:26909'])
def test_cat_to_geopandas_crs(crs, pystac_itemcol):
    nfeatures = len(pystac_itemcol.ext['single-file-stac'].features)
    geopandas = pytest.importorskip('geopandas')

    cat = StacItemCollection(pystac_itemcol)
    df = cat.to_geopandas(crs=crs)
    assert isinstance(df, geopandas.GeoDataFrame)
    assert len(df) == nfeatures
    assert df.crs == crs


def test_cat_to_missing_geopandas(pystac_itemcol, monkeypatch):
    from unittest import mock

    with pytest.raises(ImportError):
        with mock.patch.dict(sys.modules, {'geopandas': None}):
            cat = StacItemCollection(pystac_itemcol)
            _ = cat.to_geopandas()
