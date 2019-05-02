import yaml

import satstac

from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry


class STACCatalog(Catalog):
    """
    A Catalog that references a STAC catalog at some URL
    and constructs an intake catalog from it, with opinionated
    choices about the drivers that will be used to load the datasets.
    In general, the drivers are:
        netcdf
        rasterio
        xarray_image
        textfiles
    """

    name: str
    url: str

    def __init__(self, url, name, metadata=None, **kwargs):
        """
        Initialize the catalog.
        
        Parameters
        ----------
        url: str
            A URL pointing to a STAC catalog
        name : str
            A name for the catalog
        metadata : dict
            Additional information about the catalog
        kwargs : dict, optional
            Passed to intake.Catolog.__init__
        """
        self.url = url
        self.name = name
        super().__init__(name=name, metadata=metadata, **kwargs)

    def _load(self):
        """
        Load the STAC catalog from the remote data source.
        """
        catalog = satstac.Catalog.open(self.url)

        self.metadata.update(get_catalog_metadata(catalog))

        if list(catalog.collections()):
            for collection in catalog.collections():
                self._entries[collection.id] = unpack_collection(collection)
        else:
            self._entries.update(unpack_items(catalog.items()))

    def serialize(self):
        """
        Serialize the catalog to yaml.
        Returns
        -------
        A string with the yaml-formatted catalog.
        """
        output = {"metadata": self.metadata, "sources": {}}
        for key, entry in self.items():
            output["sources"][key] = yaml.safe_load(entry.yaml())["sources"]
        return yaml.dump(output)


class STACEntry(LocalCatalogEntry):
    """
    A class representing a STAC catalog entry
    """

    def __init__(self, key, item):
        """
        Construct an Intake catalog entry from a STAC catalog entry.
        """
        driver = get_driver(item)
        super().__init__(key, key,
                         driver,
                         direct_access=True,
                         args=get_args(item, driver),
                         metadata=None)

    def _ipython_display_(self):
        # TODO: see https://github.com/CityOfLosAngeles/intake-dcat/blob/master/intake_dcat/catalog.py#L83
        pass


def get_driver(entry):
    drivers = {
        'application/netcdf': 'netcdf',
        'image/vnd.stac.geotiff': 'rasterio',
        'image/vnd.stac.geotiff; cloud-optimized=true': 'rasterio',
        'image/png': "xarray_image",
        'image/jpg': "xarray_image",
        'image/jpeg': "xarray_image",
        'text/xml': 'textfiles',
    }
    return drivers.get(entry['type'], entry['type'])


def get_args(entry, driver):
    if driver in ['netcdf', 'rasterio', 'xarray_image']:
        return {'urlpath': entry.get('href'), 'chunks': {}}
    else:
        return {'urlpath': entry.get('href')}


def get_catalog_metadata(cat):
    return {'description': cat.description, 'stac_version': cat.stac_version}


def get_collection_metadata(col):
    metadata = col.properties.copy()
    for attr in ['title', 'version', 'keywords', 'license', 'providers', 'extent']:
        metadata[attr] = getattr(col, attr, None)
    return metadata


def get_item_metadata(item):
    metadata = item.properties.copy()
    for attr in ['bbox', 'geometry', 'datetime', 'date']:
        metadata[attr] = getattr(item, attr, None)
    return metadata


def unpack_collection(collection):
    entries = {}
    entries[collection.id] = Catalog(
        name=collection.id,
        metadata=get_collection_metadata(collection))
    
    entries[collection.id]._entries = unpack_items(collection.items())

    return entries


def unpack_items(items):
    entries = {}
    for item in items:
        entries[item.id] = Catalog(
            name=item.id,
            metadata=get_item_metadata(item))

        for key, value in item.assets.items():
            entries[item.id]._entries[key] = STACEntry(key, value)

    return entries