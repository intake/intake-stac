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
        self._entries = {}

        for item in catalog.items():
            for key, value in item.assets.items():
                self._entries[item.id + key] = STACEntry(value)


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
        print(output)
        return yaml.dump(output)


class STACEntry(LocalCatalogEntry):
    """
    A class representign a STAC catalog entry
    """

    def __init__(self, stac_entry):
        """
        Construct an Intake catalog entry from a STAC catalog entry.
        """
        name = stac_entry.get('title', '')
        description = stac_entry.get('description', '')
        driver = get_driver(stac_entry)
        args = get_args(stac_entry, driver)
        metadata = {"stac": stac_entry}
        super().__init__(name, description, driver, True, args=args, metadata=metadata)


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
