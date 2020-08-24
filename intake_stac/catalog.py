import os.path
import warnings

import satstac
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from pkg_resources import get_distribution

__version__ = get_distribution('intake_stac').version

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
            Passed to intake.Catalog.__init__
        """
        if isinstance(stac_obj, self._stac_cls):
            self._stac_obj = stac_obj
        elif isinstance(stac_obj, str):
            self._stac_obj = self._stac_cls.open(stac_obj)
        else:
            raise ValueError('Expected %s instance, got: %s' % (self._stac_cls, type(stac_obj)))

        metadata = self._get_metadata(**kwargs.pop('metadata', {}))

        try:
            name = kwargs.pop('name', self._stac_obj.id)
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
        return kwargs  # pragma: no cover

    def serialize(self):
        """
        Serialize the catalog to yaml.

        Returns
        -------
        A string with the yaml-formatted catalog (just top-level).
        """
        return self.yaml()


class StacCatalog(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC Catalog
    https://github.com/radiantearth/stac-spec/blob/master/catalog-spec/catalog-spec.md

    A Catalog that references a STAC catalog at some URL
    and constructs an intake catalog from it, with opinionated
    choices about the drivers that will be used to load the datasets.
    In general, the drivers are:

        - netcdf
        - rasterio
        - xarray_image
        - textfiles
    """

    name = 'stac_catalog'
    _stac_cls = satstac.Catalog

    def _load(self):
        """
        Load the STAC Catalog.
        """
        subcatalog = None
        # load first sublevel catalog(s)
        for subcatalog in self._stac_obj.children():
            self._entries[subcatalog.id] = LocalCatalogEntry(
                name=subcatalog.id,
                description=subcatalog.description,
                driver=StacCatalog,
                catalog=self,
                args={'stac_obj': subcatalog.filename},
            )

        if subcatalog is None:
            # load items under last catalog
            for item in self._stac_obj.items():
                self._entries[item.id] = LocalCatalogEntry(
                    name=item.id,
                    description='',
                    driver=StacItem,
                    catalog=self,
                    args={'stac_obj': item},
                )

    def _get_metadata(self, **kwargs):
        """
        Keep copy of all STAC JSON except for links
        """
        metadata = self._stac_obj._data.copy()
        del metadata['links']
        return metadata


class StacItemCollection(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC ItemCollection
    """

    name = 'stac_item_collection'
    _stac_cls = satstac.ItemCollection

    def _load(self):
        """
        Load the STAC Item Collection.
        """
        for item in self._stac_obj:
            self._entries[item.id] = LocalCatalogEntry(
                name=item.id,
                description='',
                driver=StacItem,
                catalog=self,
                args={'stac_obj': item},
            )

    def _get_metadata(self, **kwargs):
        return kwargs

    def to_geopandas(self, crs=None):
        """
        Load the STAC Item Collection into a geopandas GeoDataFrame

        Parameters
        ----------
        crs : str or dict (optional)
              Coordinate reference system to set on the resulting frame.

        Returns
        -------
        GeoDataFrame

        """
        try:
            import geopandas as gpd
        except ImportError:
            raise ImportError(
                'Using to_geopandas requires the `geopandas` package.'
                'You can install it via Pip or Conda.'
            )

        if crs is None:
            crs = {'init': 'epsg:4326'}
        gf = gpd.GeoDataFrame.from_features(self._stac_obj.geojson(), crs=crs)
        return gf


class StacCollection(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC Collection
    https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md
    """

    name = 'stac_collection'
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
                args={'stac_obj': item.filename},
            )

    def _get_metadata(self, **kwargs):
        metadata = {}
        for attr in [
            'title',
            'version',
            'keywords',
            'license',
            'providers',
            'extent',
        ]:
            metadata[attr] = getattr(self._stac_obj, attr, None)
        metadata.update(kwargs)
        return metadata


class StacItem(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC Item
    https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md
    """

    name = 'stac_item'
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

    def _get_band_info(self):
        """
        helper function for stack_bands
        """
        # Try to get band-info at Collection then Item level
        band_info = []
        try:
            collection = self._stac_obj.collection()
            if 'item-assets' in collection._data.get('stac_extensions'):
                for val in collection._data['item_assets'].values():
                    if 'eo:bands' in val:
                        band_info.append(val.get('eo:bands')[0])
            else:
                band_info = collection.summaries['eo:bands']

        except KeyError:
            for val in self._stac_obj.assets.values():
                if 'eo:bands' in val:
                    band_info.append(val.get('eo:bands')[0])
        finally:
            if not band_info:
                raise AttributeError(
                    'Unable to parse "eo:bands" information from STAC Collection or Item Assets'
                )
        return band_info

    def stack_bands(self, bands):
        """
        Stack the listed bands over the ``band`` dimension.

        This method only works for STAC Items using the 'eo' Extension
        https://github.com/radiantearth/stac-spec/tree/master/extensions/eo

        NOTE: This method is not aware of geotransform information. It *assumes*
        bands for a given STAC Item have the same coordinate reference system (CRS).
        This is usually the case for a given multi-band satellite acquisition.
        Coordinate alignment is performed automatically upon calling the
        `to_dask()` method to load into an Xarray DataArray if bands have diffent
        ground sample distance (gsd) or array shapes.

        Parameters
        ----------
        bands : list of strings representing the different bands
        (e.g. ['B4', B5'], ['red', 'nir']).

        Returns
        -------
        StacEntry with mapping of Asset names to Xarray bands

        Example
        -------
        stack = item.stack_bands(['nir','red'])
        da = stack(chunks=dict(band=1, x=2048, y=2048)).to_dask()
        """

        if 'eo' not in self._stac_obj._data['stac_extensions']:
            raise AttributeError('STAC Item must implement "eo" extension to use this method')

        band_info = self._get_band_info()
        item = {'concat_dim': 'band', 'urlpath': []}
        titles = []
        types = []
        assets = self._stac_obj.assets
        for band in bands:
            # band can be band id, name or common_name
            if band in assets:
                info = next((b for b in band_info if b.get('id', b.get('name')) == band), None,)
            else:
                info = next((b for b in band_info if b.get('common_name') == band), None)
                if info is not None:
                    band = info.get('id', info.get('name'))

            if band not in assets or info is None:
                valid_band_names = []
                for b in band_info:
                    valid_band_names.append(b.get('id', b.get('name')))
                    valid_band_names.append(b.get('common_name'))
                raise ValueError(
                    f'{band} not found in list of eo:bands in collection.'
                    f'Valid values: {sorted(list(set(valid_band_names)))}'
                )

            value = assets.get(band)
            band_type = value.get('type')
            types.append(band_type)

            href = value.get('href')
            pattern = href.replace(band, '{band}')
            if 'path_as_pattern' not in item:
                item['path_as_pattern'] = pattern
            elif item['path_as_pattern'] != pattern:
                raise ValueError(
                    f'Stacking failed: {href} does not contain '
                    'band info in a fixed section of the url'
                )

            titles.append(band)
            item['urlpath'].append(href)

        unique_types = set(types)
        if len(unique_types) != 1:
            raise ValueError(
                f'Stacking failed: bands must have type, multiple found: {unique_types}'
            )
        else:
            item['type'] = types[0]

        item['title'] = ', '.join(titles)
        return StacEntry('_'.join(bands), item, stacked=True)


class StacEntry(LocalCatalogEntry):
    """
    A class representing a STAC catalog Entry
    """

    def __init__(self, key, item, stacked=False):
        """
        Construct an Intake catalog 'Source' from a STAC Item Asset.
        """
        driver = self._get_driver(item)

        default_plot = self._get_plot(item)
        if default_plot:
            item['plots'] = default_plot

        super().__init__(
            name=key,
            description=item.get('title', key),
            driver=driver,
            direct_access=True,
            args=self._get_args(item, driver, stacked=stacked),
            metadata=item,
        )

    def _get_plot(self, item):
        """
        Default hvplot plot based on Asset mimetype
        """
        # NOTE: consider geojson, parquet, hdf defaults in future
        default_plot = None
        type = item.get('type', None)  # also some assets do not have 'type'
        if type:
            if type in ['image/jpeg', 'image/jpg', 'image/png']:
                default_plot = dict(
                    thumbnail=dict(
                        kind='rgb',
                        x='x',
                        y='y',
                        bands='channel',
                        data_aspect=1,
                        flip_yaxis=True,
                        xaxis=False,
                        yaxis=False,
                    )
                )

            elif 'tiff' in type:
                default_plot = dict(
                    geotiff=dict(
                        kind='image',
                        x='x',
                        y='y',
                        frame_width=500,
                        data_aspect=1,
                        rasterize=True,
                        dynamic=True,
                        cmap='viridis',
                    )
                )

        return default_plot

    def _get_driver(self, entry):
        drivers = {
            'application/netcdf': 'netcdf',
            'application/x-netcdf': 'netcdf',
            'application/parquet': 'parquet',
            'application/x-parquet': 'parquet',
            'application/x-hdf5': 'netcdf',
            # 'application/x-hdf': '',
            'image/vnd.stac.geotiff': 'rasterio',
            'image/vnd.stac.geotiff; cloud-optimized=true': 'rasterio',
            'image/x.geotiff': 'rasterio',
            'image/tiff; application=geotiff': 'rasterio',
            'image/tiff; application=geotiff; profile=cloud-optimized': 'rasterio',  # noqa: E501
            'image/png': 'xarray_image',
            'image/jpg': 'xarray_image',
            'image/jpeg': 'xarray_image',
            'text/xml': 'textfiles',
            'text/plain': 'textfiles',
            'text/html': 'textfiles',
            'application/json': 'textfiles',
            # 'application/geopackage+sqlite3': 'geopandas',
            'application/geo+json': 'geopandas',
        }

        entry_type = entry.get('type', NULL_TYPE)

        if entry_type is NULL_TYPE:
            # Fallback to common file suffix mappings
            suffix = os.path.splitext(entry['href'])[-1]
            if suffix in ['.nc', '.h5', '.hdf']:
                entry_type = 'application/netcdf'
            else:
                warnings.warn(
                    f'TODO: handle case with entry without type field. This entry was: {entry}'
                )

        return drivers.get(entry_type, entry_type)

    def _get_args(self, entry, driver, stacked=False):
        args = entry if stacked else {'urlpath': entry.get('href')}
        if driver in ['netcdf', 'rasterio', 'xarray_image']:
            args.update(chunks={})

        return args
