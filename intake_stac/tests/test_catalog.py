import pytest

import intake
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from intake_stac import StacCatalog
from intake_stac.catalog import StacEntry

@pytest.fixture(scope="module")
def cat():
    cat = StacCatalog(
        'https://storage.googleapis.com/pdd-stac/disasters/catalog.json',
        name='planet-disaster-data'
    )
    return cat

def test_init_catalog(cat):
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.discover()['container'] == 'catalog'


@pytest.mark.xfail(reason='TODO: work on nested serialization')
def test_serialize(cat):
    cat_str = cat.serialize()
    assert isinstance(cat_str, str)


def test_cat_entries(cat):
    assert list(cat)
    assert all([isinstance(v, (LocalCatalogEntry, Catalog)) for _, v in cat.items()])
