import pytest

import intake
from intake_stac import STACCatalog
from intake_stac.catalog import STACEntry

@pytest.fixture(scope="module")
def cat():
    cat = STACCatalog(
        'https://storage.googleapis.com/pdd-stac/disasters/catalog.json',
        'planet-disaster-data'
    )
    return cat

def test_init_catalog(cat):
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.discover()['container'] == 'catalog'


def test_serialize(cat):
    cat_str = cat.serialize()
    assert isinstance(cat_str, str)


def test_cat_entries(cat):
    assert list(cat)
    assert all([isinstance(v, STACEntry) for _, v in cat.items()])
