import datetime
import os.path
import sys
from pathlib import Path

import intake
import pystac
import pytest
import yaml
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from intake_stac import StacCatalog, StacCollection, StacItem, StacItemCollection
from intake_stac.catalog import CombinedAssets, StacAsset, drivers

here = Path(__file__).parent


cat_url = str(here / 'data/1.0.0/catalog/catalog.json')
col_url = str(here / 'data/1.0.0/collection/collection.json')
item_url = str(here / 'data/1.0.0/collection/simple-item.json')
itemcol_url = str(here / 'data/1.0.0/itemcollection/example-search.json')


@pytest.fixture(scope='module')
def pystac_cat():
    return pystac.Catalog.from_file(cat_url)


@pytest.fixture(scope='module')
def pystac_col():
    col = pystac.Collection.from_file(col_url)
    return col


@pytest.fixture(scope='module')
def pystac_item():
    return pystac.Item.from_file(item_url)


@pytest.fixture(scope='module')
def pystac_itemcol():
    # return pystac.read_file(itemcol_url)
    # ItemCollection is not a valid pystac STACObject, so can't use read_file.
    return pystac.ItemCollection.from_file(itemcol_url)


@pytest.fixture(scope='module')
def intake_stac_cat():
    return StacCatalog.from_url(cat_url)


class TestCatalog:
    def test_init_catalog_from_url(self):
        cat = StacCatalog(cat_url)
        assert isinstance(cat, intake.catalog.Catalog)
        assert cat.name == 'test'
        assert cat.discover()['container'] == 'catalog'
        assert int(cat.metadata['stac_version'][0]) >= 1

        cat = StacCatalog.from_url(cat_url)
        assert isinstance(cat, intake.catalog.Catalog)
        assert cat.name == 'test'
        assert cat.discover()['container'] == 'catalog'
        assert int(cat.metadata['stac_version'][0]) >= 1

        # test kwargs are passed through
        cat = StacCatalog.from_url(cat_url, name='intake-stac-test')
        assert 'intake-stac-test' == cat.name

    def test_init_catalog_from_pystac_obj(self, pystac_cat):
        cat = StacCatalog(pystac_cat)
        assert isinstance(cat, intake.catalog.Catalog)
        assert cat.discover()['container'] == 'catalog'
        assert cat.name == 'test'
        assert cat.name == pystac_cat.id

        # test kwargs are passed through
        cat = StacCatalog(pystac_cat, name='intake-stac-test')
        assert 'intake-stac-test' == cat.name

    def test_init_catalog_with_wrong_type_raises(self, pystac_cat):
        with pytest.raises(ValueError):
            StacCollection(pystac_cat)

    def test_init_catalog_with_bad_url_raises(self):
        # json.decoder.JSONDecodeError or FileNotFoundError
        with pytest.raises(Exception):
            StacCatalog('https://raw.githubusercontent.com/')

    def test_serialize(self, intake_stac_cat):
        cat_str = intake_stac_cat.serialize()
        assert isinstance(cat_str, str)

    def test_cat_entries(self, intake_stac_cat):
        assert list(intake_stac_cat)
        assert all(
            [isinstance(v, (LocalCatalogEntry, Catalog)) for _, v in intake_stac_cat.items()]
        )

    def test_cat_name_from_pystac_catalog_id(self, intake_stac_cat):
        assert intake_stac_cat.name == 'test'


class TestCollection:
    def test_cat_from_collection(self, pystac_col):
        cat = StacCollection(pystac_col)
        subcat_name = 'S2B_MSIL2A_20171227T160459_N0212_R054_T17QLA_20201014T165101'
        assert cat.name == pystac_col.id
        assert subcat_name in cat
        # This is taking way too long
        # item_name = 'S2B_25WFU_20200610_0_L1C'
        # assert item_name in cat[subcat_name]
        # assert 'B04' in cat[subcat_name][item_name]


class TestItemCollection:
    def test_cat_from_item_collection(self, pystac_itemcol):
        cat = StacItemCollection(pystac_itemcol)
        assert 'LC80340332018034LGN00' in cat
        assert 'B5' in cat.LC80340332018034LGN00

    @pytest.mark.parametrize('crs', ['IGNF:ETRS89UTM28', 'epsg:26909'])
    def test_cat_to_geopandas_crs(self, crs, pystac_itemcol):
        nfeatures = len(pystac_itemcol.items)
        geopandas = pytest.importorskip('geopandas')

        cat = StacItemCollection(pystac_itemcol)
        df = cat.to_geopandas(crs=crs)
        assert isinstance(df, geopandas.GeoDataFrame)
        assert len(df) == nfeatures
        assert df.crs == crs

    def test_cat_to_missing_geopandas(self, pystac_itemcol, monkeypatch):
        from unittest import mock

        with pytest.raises(ImportError):
            with mock.patch.dict(sys.modules, {'geopandas': None}):
                cat = StacItemCollection(pystac_itemcol)
                _ = cat.to_geopandas()

    def test_load_satsearch_results(self, pystac_itemcol):
        test_file = os.path.join(here, 'data/1.0.0beta2/earthsearch/single-file-stac.json')
        catalog = intake.open_stac_item_collection(test_file)
        assert isinstance(catalog, StacItemCollection)
        assert len(catalog) == 18


class TestItem:
    def test_cat_from_item(self, pystac_item):
        cat = StacItem(pystac_item)
        assert 'B02' in cat

    def test_cat_item_stacking(self, pystac_item):
        item = StacItem(pystac_item)
        list_of_bands = ['B02', 'B03']
        new_entry = item.stack_bands(list_of_bands)
        assert isinstance(new_entry, CombinedAssets)
        assert new_entry._description == 'B02, B03'
        assert new_entry.name == 'B02_B03'

    def test_cat_item_stacking_common_name(self, pystac_item):
        item = StacItem(pystac_item)
        list_of_bands = ['blue', 'green']
        new_entry = item.stack_bands(list_of_bands)
        assert isinstance(new_entry, CombinedAssets)
        assert new_entry._description == 'B02, B03'
        assert new_entry.name == 'blue_green'

    def test_cat_item_stacking_path_as_pattern(self, pystac_item):
        item = StacItem(pystac_item)
        list_of_bands = ['B02', 'B03']
        new_entry = item.stack_bands(list_of_bands, path_as_pattern='{}{band:2}.TIF')
        assert isinstance(new_entry, CombinedAssets)

    def test_cat_item_stacking_dims_of_different_type_raises_error(self, pystac_item):
        item = StacItem(pystac_item)
        list_of_bands = ['B02', 'ANG']
        with pytest.raises(ValueError, match=('ANG not found in list of eo:bands in collection')):
            item.stack_bands(list_of_bands)

    def test_cat_item_stacking_dims_with_nonexistent_band_raises_error(
        self, pystac_item,
    ):  # noqa: E501
        item = StacItem(pystac_item)
        list_of_bands = ['B01', 'foo']
        with pytest.raises(ValueError, match="'B02', 'B03', 'blue', 'green'"):
            item.stack_bands(list_of_bands)

    # def test_cat_item_stacking_dims_of_different_size_regrids(self, pystac_item):
    #     item = StacItem(pystac_item)
    #     list_of_bands = ['B1', 'B8']
    #     B1_da = item.B1.to_dask()
    #     assert B1_da.shape == (1, 8391, 8311)
    #     B8_da = item.B8.to_dask()
    #     assert B8_da.shape == (1, 16781, 16621)
    #     new_entry = item.stack_bands(list_of_bands)
    #     new_da = new_entry().to_dask()
    #     assert new_da.shape == (2, 16781, 16621)
    #     assert sorted([dim for dim in new_da.dims]) == ['band', 'x', 'y']

    def test_asset_describe(self, pystac_item):
        item = StacItem(pystac_item)
        key = 'B02'
        asset = item[key]
        d = asset.describe()

        assert d['name'] == key
        assert d['container'] == 'xarray'
        assert d['plugin'] == ['rasterio']
        assert d['args']['urlpath'] == asset.urlpath
        assert d['description'] == asset.description
        # NOTE: note sure why asset.metadata has 'catalog_dir' key ?
        # assert d['metadata'] == asset.metadata

    def test_asset_missing_type(self, pystac_item):
        key = 'B02'
        asset = pystac_item.assets.get('B02')
        asset.media_type = ''
        with pytest.warns(Warning, match='STAC Asset'):
            entry = StacAsset(key, asset)
        d = entry.describe()

        assert d['name'] == key
        assert d['metadata']['type'] == 'application/rasterio'  # default_type
        assert d['container'] == 'xarray'
        assert d['plugin'] == ['rasterio']

    def test_asset_unknown_type(self, pystac_item):
        key = 'B02'
        asset = pystac_item.assets.get('B02')
        asset.media_type = 'unrecognized'
        entry = StacAsset(key, asset)
        d = entry.describe()

        assert d['name'] == key
        assert d['metadata']['type'] == 'unrecognized'
        assert d['container'] == 'xarray'
        assert d['plugin'] == ['rasterio']

    def test_cat_item_yaml(self, pystac_item):
        cat_str = StacItem(pystac_item).yaml()
        d = yaml.load(cat_str)

        for key in ['bbox','date','datetime','geometry','version']:
            assert key in d['metadata']
        for key in ['B02','B03']:
            assert key in d['sources']

    def test_cat_item_yaml_roundtrip(self, pystac_item, tmp_path):
        cat1 = StacItem(pystac_item)
        cat_str = cat1.yaml()

        temp_file = tmp_path/'temp.yaml'
        with open(temp_file, 'w') as f:
            f.write(cat_str)

        cat2 = intake.open_catalog(temp_file)

        for key in ['B02','B03']:
            assert key in cat2
        
        assert cat1.walk() == cat2.walk(), print(cat1.walk()['B02'].describe()['direct_access'], cat2.walk()['B02'].describe()['direct_access'])


class TestDrivers:
    def test_drivers_include_all_pystac_media_types(self):
        for media_type in pystac.MediaType:
            assert media_type in drivers

    def test_drivers_can_open_all_earthsearch_sentinel_s2_l2a_cogs_assets(self):
        test_file = os.path.join(here, 'data/1.0.0beta2/earthsearch/single-file-stac.json')
        catalog = intake.open_stac_item_collection(test_file)
        _, item = next(catalog.items())
        for _, asset in item.items():
            assert asset.metadata['type'] in drivers


def test_cat_to_geopandas(pystac_itemcol):
    nfeatures = len(pystac_itemcol)
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


def test_collection_of_collection():
    space = pystac.SpatialExtent([[0, 1, 2, 3]])
    time = pystac.TemporalExtent([datetime.datetime(2000, 1, 1), datetime.datetime(2000, 1, 1)])
    child = pystac.Collection('child', 'child-description', extent=pystac.Extent(space, time))
    parent = pystac.Collection('parent', 'parent-description', extent=pystac.Extent(space, time),)
    parent.add_child(child)

    result = StacCollection(parent)
    result._load()
