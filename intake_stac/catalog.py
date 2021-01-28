import os.path
import warnings

import pystac
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from pkg_resources import get_distribution

__version__ = get_distribution('intake_stac').version

# STAC catalog asset 'type' determines intake driver:
# https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#media-types
default_type = 'application/rasterio'
default_driver = 'rasterio'

drivers = {
    'application/netcdf': 'netcdf',
    'application/x-netcdf': 'netcdf',
    'application/parquet': 'parquet',
    'application/x-parquet': 'parquet',
    'application/x-hdf': 'netcdf',
    'application/x-hdf5': 'netcdf',
    'application/rasterio': 'rasterio',
    'image/vnd.stac.geotiff': 'rasterio',
    'image/vnd.stac.geotiff; cloud-optimized=true': 'rasterio',
    'image/x.geotiff': 'rasterio',
    'image/tiff; application=geotiff': 'rasterio',
    'image/tiff; application=geotiff; profile=cloud-optimized': 'rasterio',  # noqa: E501
    'image/jp2': 'rasterio',
    'image/png': 'xarray_image',
    'image/jpg': 'xarray_image',
    'image/jpeg': 'xarray_image',
    'text/xml': 'textfiles',
    'text/plain': 'textfiles',
    'text/html': 'textfiles',
    'application/json': 'textfiles',
    'application/geo+json': 'geopandas',
    'application/geopackage+sqlite3': 'geopandas',
}


class AbstractStacCatalog(Catalog):

    version = __version__
    partition_access = False

    def __init__(self, stac_obj, **kwargs):
        """
        Initialize the catalog.

        Parameters
        ----------
        stac_obj: stastac.Thing
            A pystac.Thing pointing to a STAC object
        kwargs : dict, optional
            Passed to intake.Catalog.__init__
        """
        if isinstance(stac_obj, self._stac_cls):
            self._stac_obj = stac_obj
        elif isinstance(stac_obj, str):
            self._stac_obj = self._stac_cls.from_file(stac_obj)
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
        stac_obj = cls._stac_cls.from_file(url)
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
    _stac_cls = pystac.Catalog

    def _load(self):
        """
        Load the STAC Catalog.
        """
        subcatalog = None
        # load first sublevel catalog(s)
        for subcatalog in self._stac_obj.get_children():
            self._entries[subcatalog.id] = LocalCatalogEntry(
                name=subcatalog.id,
                description=subcatalog.description,
                driver=StacCatalog,
                catalog=self,
                args={'stac_obj': subcatalog.get_self_href()},
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
        metadata = self._stac_obj.to_dict()
        del metadata['links']
        return metadata


class StacCollection(AbstractStacCatalog):
    """
    Dummy class
    """

    name = 'catalog'


class StacItemCollection(AbstractStacCatalog):
    """
    Item Collection returned from stac-api
    https://github.com/radiantearth/stac-spec/blob/master/extensions/single-file-stac/README.md
    """

    name = 'single-file-stac'
    _stac_cls = pystac.Catalog

    def _load(self):
        """
        Load the STAC Item Collection.
        """
        print(dir(self))
        if not self.ext.implements('single-file-stac'):
            raise AttributeError(" StacItemCollection requires 'single-file-stac' extension")
        for item in self.ext['single-file-stac'].features:
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


class StacItem(AbstractStacCatalog):
    """
    Intake Catalog represeting a STAC Item
    https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md
    """

    name = 'stac_item'
    _stac_cls = pystac.Item

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

    def __init__(self, key, asset, stacked=False):
        """
        Construct an Intake catalog 'Source' from a STAC Item Asset.
        """
        driver = self._get_driver(asset)

        # skip for now
        # default_plot = self._get_plot(asset)
        # if default_plot:
        #    self['plots'] = default_plot

        super().__init__(
            name=key,
            description=asset.title,
            driver=driver,
            direct_access=True,
            args=self._get_args(asset, driver, stacked=stacked),
            metadata=asset,
        )

    def _get_plot(self, asset):
        """
        Default hvplot plot based on Asset mimetype
        """
        # NOTE: consider geojson, parquet, hdf defaults in future
        default_plot = None
        type = asset.media_type
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

    def _get_driver(self, asset):

        entry_type = asset.media_type

        if entry_type in ['', 'null', None]:

            suffix = os.path.splitext(asset.media_type)[-1]
            if suffix in ['.nc', '.h5', '.hdf']:
                asset.media_type = 'application/netcdf'
                warnings.warn(
                    f'STAC Asset "type" missing, assigning {entry_type} based on href suffix {suffix}:\n{asset.media_type}'  # noqa: E501
                )
            else:
                asset.media_type = default_type
                warnings.warn(
                    f'STAC Asset "type" missing, assuming default type={default_type}:\n{asset}'
                )
            entry_type = asset.media_type
            print(entry_type)

        # if mimetype not registered try rasterio driver
        driver = drivers.get(entry_type, default_driver)

        return driver

    def _get_args(self, asset, driver, stacked=False):
        args = asset if stacked else {'urlpath': asset.href}
        if driver in ['netcdf', 'rasterio', 'xarray_image']:
            args.update(chunks={})

        return args
