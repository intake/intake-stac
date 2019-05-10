import warnings

import satstac
import yaml
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import __version__

NULL_TYPE = 'null'


class AbstractStacCatalog(Catalog):

    version = __version__
    partition_access = False

    def __init__(self, stac_obj, **kwargs):
        """
        Initialize the catalog.

        Parameters
        ----------
        stac_obj: stastac.Thing
            A satstac.Thing pointing to a STAC object
        kwargs : dict, optional
            Passed to intake.Catolog.__init__
        """
        if isinstance(stac_obj, self._stac_cls):
            self._stac_obj = stac_obj
        elif isinstance(stac_obj, str):
            self._stac_obj = self._stac_cls.open(stac_obj)
        else:
            raise ValueError(
                'Expected %s instance, got: %s' % (type(self._stac_cls),
                                                   type(stac_obj)))

        metadata = self._get_metadata(**kwargs.pop('metadata', {}))

        name = kwargs.pop('name', self._stac_obj.id)

        super().__init__(name=name, metadata=metadata, **kwargs)

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Initialize the catalog from a STAC url.

        Parameters
        ----------
        url: str
            A URL pointing to a STAC json object
        kwargs : dict, optional
            Passed to intake.Catolog.__init__
        """
        stac_obj = cls._stac_cls.open(url)
        return cls(stac_obj, **kwargs)

    def _get_metadata(self, **kwargs):
        return kwargs

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


class StacCatalog(AbstractStacCatalog):
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

    name = 'stac-catalog'
    _stac_cls = satstac.Catalog

    def _load(self):
        """
        Load the STAC Catalog.
        """
        # should we also iterate over .catalogs() here?
        collections = list(self._stac_obj.collections())
        if collections:
            for collection in collections:
                self._entries[collection.id] = LocalCatalogEntry(
                    name=collection.id,
                    description=collection.title,
                    driver=StacCollection,
                    catalog=self,
                    args={'stac_obj': collection})
        else:
            # in the future this may go away
            for item in self._stac_obj.items():
                self._entries[item.id] = LocalCatalogEntry(
                    name=item.id,
                    description='',
                    driver=StacItem,
                    catalog=self,
                    args={'stac_obj': item})

    def _get_metadata(self, **kwargs):
        return dict(description=self._stac_obj.description,
                    stac_version=self._stac_obj.stac_version,
                    **kwargs)


class StacCollection(AbstractStacCatalog):

    name = 'stac-collection'
    _stac_cls = satstac.Collection

    def _load(self):
        """
        Load the STAC Collection.
        """
        for item in self._stac_obj.items():
            self._entries[item.id] = LocalCatalogEntry(
                name=item.id,
                description='',
                driver=StacItem,
                catalog=self,
                args={'stac_obj': item})

    def _get_metadata(self, **kwargs):
        metadata = self._stac_obj.properties.copy()
        for attr in ['title', 'version', 'keywords', 'license',
                     'providers', 'extent']:
            metadata[attr] = getattr(self._stac_obj, attr, None)
        metadata.update(kwargs)
        return metadata


class StacItem(AbstractStacCatalog):

    name = 'stac-item'
    _stac_cls = satstac.Item

    def _load(self):
        """
        Load the STAC Item.
        """
        for key, value in self._stac_obj.assets.items():
            self._entries[key] = StacEntry(key, value)

    def _get_metadata(self, **kwargs):
        metadata = self._stac_obj.properties.copy()
        for attr in ['bbox', 'geometry', 'datetime', 'date']:
            metadata[attr] = getattr(self._stac_obj, attr, None)
        metadata.update(kwargs)
        return metadata


class StacEntry(LocalCatalogEntry):
    """
    A class representing a STAC catalog entry
    """

    def __init__(self, key, item):
        """
        Construct an Intake catalog entry from a STAC catalog entry.
        """
        driver = self._get_driver(item)
        super().__init__(name=key,
                         description=item.get('title', key),
                         driver=driver,
                         direct_access=True,
                         args=self._get_args(item, driver),
                         metadata=item)

    def _get_driver(self, entry):
        drivers = {
            'application/netcdf': 'netcdf',
            'image/vnd.stac.geotiff': 'rasterio',
            'image/vnd.stac.geotiff; cloud-optimized=true': 'rasterio',
            'image/x.geotiff': 'rasterio',
            'image/png': "xarray_image",
            'image/jpg': "xarray_image",
            'image/jpeg': "xarray_image",
            'text/xml': 'textfiles',
            'text/plain': 'textfiles',
            'text/html': 'textfiles'
        }
        entry_type = entry.get('type', NULL_TYPE)

        if entry_type is NULL_TYPE:
            warnings.warn(f'TODO: handle case with entry without type field. '
                          ' This entry was: {entry}')

        return drivers.get(entry_type, entry_type)

    def _get_args(self, entry, driver):
        if driver in ['netcdf', 'rasterio', 'xarray_image']:
            return {'urlpath': entry.get('href'), 'chunks': {}}
        else:
            return {'urlpath': entry.get('href')}
