import warnings

import satstac
import yaml
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import __version__

NULL_TYPE = "null"


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
                "Expected %s instance, got: %s"
                % (self._stac_cls, type(stac_obj))
            )

        metadata = self._get_metadata(**kwargs.pop("metadata", {}))

        try:
            name = kwargs.pop("name", self._stac_obj.id)
        except AttributeError:
            name = str(type(self._stac_obj))

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
    Intake Catalog represeting a STAC Catalog

    A Catalog that references a STAC catalog at some URL
    and constructs an intake catalog from it, with opinionated
    choices about the drivers that will be used to load the datasets.
    In general, the drivers are:

        - netcdf
        - rasterio
        - xarray_image
        - textfiles
    """

    name = "stac_catalog"
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
                    args={"stac_obj": collection},
                )
        else:
            # in the future this may go away
            for item in self._stac_obj.items():
                self._entries[item.id] = LocalCatalogEntry(
                    name=item.id,
                    description="",
                    driver=StacItem,
                    catalog=self,
                    args={"stac_obj": item},
                )

    def _get_metadata(self, **kwargs):
        return dict(
            description=self._stac_obj.description,
            stac_version=self._stac_obj.stac_version,
            **kwargs,
        )


class StacItemCollection(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC ItemCollection
    """

    name = "stac_item_collection"
    _stac_cls = satstac.ItemCollection

    def _load(self):
        """
        Load the STAC Item Collection.
        """
        for item in self._stac_obj:
            self._entries[item.id] = LocalCatalogEntry(
                name=item.id,
                description="",
                driver=StacItem,
                catalog=self,
                args={"stac_obj": item},
            )

    def _get_metadata(self, **kwargs):
        return kwargs


class StacCollection(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC Collection
    """

    name = "stac_collection"
    _stac_cls = satstac.Collection

    def _load(self):
        """
        Load the STAC Collection.
        """
        for item in self._stac_obj.items():
            self._entries[item.id] = LocalCatalogEntry(
                name=item.id,
                description="",
                driver=StacItem,
                catalog=self,
                args={"stac_obj": item},
            )

    def _get_metadata(self, **kwargs):
        metadata = self._stac_obj.properties.copy()
        for attr in [
            "title",
            "version",
            "keywords",
            "license",
            "providers",
            "extent",
        ]:
            metadata[attr] = getattr(self._stac_obj, attr, None)
        metadata.update(kwargs)
        return metadata


class StacItem(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC Item
    """

    name = "stac_item"
    _stac_cls = satstac.Item

    def _load(self):
        """
        Load the STAC Item.
        """
        for key, value in self._stac_obj.assets.items():
            self._entries[key] = StacEntry(key, value)

    def _get_metadata(self, **kwargs):
        metadata = self._stac_obj.properties.copy()
        for attr in ["bbox", "geometry", "datetime", "date"]:
            metadata[attr] = getattr(self._stac_obj, attr, None)
        metadata.update(kwargs)
        return metadata

    def stack_bands(self, bands, regrid=False):
        """
        Stack the listed bands over the ``band`` dimension.

        Parameters
        ----------
        bands : list of strings representing the different bands
        (e.g. ['B1', B2']).

        Returns
        -------
        Catalog entry containing listed bands with ``band`` as a dimension
        and coordinate.

        """
        item = {"concat_dim": "band", "urlpath": [], "type": "image/x.geotiff"}
        titles = []
        assets = self._stac_obj.assets

        try:
            band_info = self._stac_obj.collection().properties.get("eo:bands")
        except AttributeError:
            # TODO: figure out why satstac objects don't always have a
            #  collection. This workaround covers the case where
            # `.collection()` returns None
            band_info = self._stac_obj.properties.get("eo:bands")

        for band in bands:
            # band can be band id, name or common_name
            if band in assets:
                info = next(
                    (
                        b
                        for b in band_info
                        if b.get("id", b.get("name")) == band
                    ),
                    None,
                )
            else:
                info = next(
                    (b for b in band_info if b["common_name"] == band), None
                )
                if info is not None:
                    band = info.get("id", info.get("name"))

            if band not in assets or (regrid is False and info is None):
                valid_band_names = []
                for b in band_info:
                    valid_band_names.append(b.get("id", b.get("name")))
                    valid_band_names.append(b.get("common_name"))
                raise ValueError(
                    f"{band} not found in list of eo:bands in collection."
                    f"Valid values: {sorted(list(set(valid_band_names)))}"
                )

            value = assets.get(band)
            band_type = value.get("type")
            if band_type != item["type"]:
                raise ValueError(
                    f"Stacking failed: {band} has type {band_type} and "
                    f'bands must have type {item["type"]}'
                )

            href = value.get("href")
            pattern = href.replace(band, "{band}")
            if "path_as_pattern" not in item:
                item["path_as_pattern"] = pattern
            elif item["path_as_pattern"] != pattern:
                raise ValueError(
                    f"Stacking failed: {href} does not contain "
                    "band info in a fixed section of the url"
                )

            if regrid is False:
                gsd = info.get("gsd")
                if "gsd" not in item:
                    item["gsd"] = gsd
                elif item["gsd"] != gsd:
                    raise ValueError(
                        f"Stacking failed: {band} has different ground "
                        f"sampling distance ({gsd}) than other bands "
                        f'({item["gsd"]})'
                    )

            titles.append(value.get("title"))
            item["urlpath"].append(href)

        item["title"] = ", ".join(titles)
        return StacEntry("_".join(bands), item, stacked=True)


class StacEntry(LocalCatalogEntry):
    """
    A class representing a STAC catalog Entry
    """

    def __init__(self, key, item, stacked=False):
        """
        Construct an Intake catalog Entry from a STAC catalog Entry.
        """
        driver = self._get_driver(item)
        super().__init__(
            name=key,
            description=item.get("title", key),
            driver=driver,
            direct_access=True,
            args=self._get_args(item, driver, stacked=stacked),
            metadata=item,
        )

    def _get_driver(self, entry):
        drivers = {
            "application/netcdf": "netcdf",
            "image/vnd.stac.geotiff": "rasterio",
            "image/vnd.stac.geotiff; cloud-optimized=true": "rasterio",
            "image/x.geotiff": "rasterio",
            "image/png": "xarray_image",
            "image/jpg": "xarray_image",
            "image/jpeg": "xarray_image",
            "text/xml": "textfiles",
            "text/plain": "textfiles",
            "text/html": "textfiles",
        }
        entry_type = entry.get("type", NULL_TYPE)

        if entry_type is NULL_TYPE:
            warnings.warn(
                f"TODO: handle case with entry without type field. "
                " This entry was: {entry}"
            )

        return drivers.get(entry_type, entry_type)

    def _get_args(self, entry, driver, stacked=False):
        args = entry if stacked else {"urlpath": entry.get("href")}
        if driver in ["netcdf", "rasterio", "xarray_image"]:
            args.update(chunks={})

        return args
