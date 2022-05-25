import os.path
import warnings

import pystac
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.source import DataSource
from pkg_resources import get_distribution
from pystac.extensions.eo import EOExtension

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
    'image/tiff': 'rasterio',
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
    'application/vnd+zarr': 'zarr',
    'application/xml': 'textfiles',
}


class AbstractStacCatalog(Catalog):

    version = __version__
    partition_access = False

    def __init__(self, stac_obj, **kwargs):
        """
        Initialize the catalog.

        Parameters
        ----------
        stac_obj: stastac.STACObject
            A pystac.STACObject pointing to a STAC object
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
            # Not currently tested.
            # ItemCollection does not require an id
            # Unclear what the state of ItemCollection is.
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
    Maps Intake Catalog to a STAC Catalog
    https://pystac.readthedocs.io/en/latest/api.html?#catalog-spec
    """

    name = 'stac_catalog'
    _stac_cls = pystac.Catalog

    def _load(self):
        """
        Load the STAC Catalog.
        """
        for subcatalog in self._stac_obj.get_children():
            if isinstance(subcatalog, pystac.Collection):
                # Collection subclasses Catalog, so check it first
                driver = StacCollection
            else:
                driver = StacCatalog

            self._entries[subcatalog.id] = LocalCatalogEntry(
                name=subcatalog.id,
                description=subcatalog.description,
                driver=driver,  # recursive
                catalog=self,
                args={'stac_obj': subcatalog.get_self_href()},
            )

        for item in self._stac_obj.get_items():
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
        # NOTE: why not links?
        metadata = self._stac_obj.to_dict()
        del metadata['links']
        return metadata


class StacCollection(StacCatalog):
    """
    Maps Intake Catalog to a STAC Collection
    https://pystac.readthedocs.io/en/latest/api.html#collection-spec

    Collections have a number of properties that Catalogs do not, most notably
    the spatial and temporal extents. This is currently a placeholder for
    future Collection-specific attributes and methods.
    """

    name = 'stac_catalog'
    _stac_cls = pystac.Collection

    def get_asset(
        self,
        key,
        storage_options=None,
        merge_asset_storage_options=True,
        merge_asset_open_kwargs=True,
        **kwargs,
    ):
        r"""
        Get a datasource for a collection-level asset.

        Parameters
        ----------
        key : str, optional
            The asset key to use if multiple Zarr assets are provided.
        storage_options : dict, optional
            Additional arguments for the backend fsspec filesystem.
        merge_asset_storage_option : bool, default True
            Whether to merge the storage options provided by the asset under the
            ``xarray:storage_options`` key with `storage_options`.
        merge_asset_open_kwargs : bool, default True
            Whether to merge the keywords provided by the asset under the
            ``xarray:open_kwargs`` key with ``**kwargs``.
        **kwargs
            Additional keyword options are provided to the loader, for example ``consolidated=True``
            to pass to :meth:`xarray.open_zarr`.

        Notes
        -----
        The Media Type of the asset will be used to determine how to load the data.

        Returns
        -------
        DataSource
            The dataset described by the asset loaded into a dask-backed object.
        """
        try:
            asset = self._stac_obj.assets[key]
        except KeyError:
            raise KeyError(
                f'No asset named {key}. Should be one of {list(self._stac_obj.assets)}'
            ) from None

        storage_options = storage_options or {}
        if merge_asset_storage_options:
            asset_storage_options = asset.extra_fields.get('xarray:storage_options', {})
            storage_options.update(asset_storage_options)

        if merge_asset_open_kwargs:
            asset_open_kwargs = asset.extra_fields.get('xarray:open_kwargs', {})
            kwargs.update(asset_open_kwargs)

        return StacAsset(key, asset)(storage_options=storage_options, **kwargs)


class StacItemCollection(AbstractStacCatalog):
    """
    Maps ItemCollection returned from a STAC API to Intake (Sub)Catalog
    https://github.com/radiantearth/stac-api-spec/tree/master/fragments/itemcollection

    Note search results often use the single file stac extension:
    https://pystac.readthedocs.io/en/latest/api.html?#single-file-stac-extension
    """

    name = 'stac_itemcollection'
    _stac_cls = pystac.ItemCollection

    def _load(self):
        """
        Load the STAC Item Collection.
        """
        # if not self._stac_obj.ext.implements('single-file-stac'):
        #     raise ValueError("StacItemCollection requires 'single-file-stac' extension")
        for item in self._stac_obj.items:
            self._entries[item.id] = LocalCatalogEntry(
                name=item.id,
                description='',
                driver=StacItem,
                catalog=self,
                args={'stac_obj': item},
            )

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
            crs = 'epsg:4326'
        gf = gpd.GeoDataFrame.from_features(self._stac_obj.to_dict(), crs=crs)
        return gf


class StacItem(AbstractStacCatalog):
    """
    Maps STAC Item to Intake (Sub)Catalog
    https://pystac.readthedocs.io/en/latest/api.html#item-spec
    """

    name = 'stac_item'
    _stac_cls = pystac.Item

    def __getitem__(self, key):
        result = super().__getitem__(key)
        # TODO: handle non-string assets?
        asset = self._entries[key]
        storage_options = asset._stac_obj.extra_fields.get('xarray:storage_options', {})
        open_kwargs = asset._stac_obj.extra_fields.get('xarray:open_kwargs', {})

        if isinstance(result, DataSource):
            kwargs = result._captured_init_kwargs
            kwargs = {**kwargs, **dict(storage_options=storage_options), **open_kwargs}
            result = result(*result._captured_init_args, **kwargs)

        return result

    def _load(self):
        """
        Load the STAC Item.
        """
        for key, value in self._stac_obj.assets.items():
            self._entries[key] = StacAsset(key, value)

    def _get_metadata(self, **kwargs):
        metadata = self._stac_obj.properties.copy()
        for attr in ['bbox', 'geometry', 'datetime', 'date']:
            metadata[attr] = getattr(self._stac_obj, attr, None)
        metadata.update(kwargs)
        return metadata

    def _get_band_info(self):
        """
        Return list of band info dictionaries (name, common_name, etc.)...
        """
        band_info = []
        for band in EOExtension.ext(self._stac_obj).bands:
            band_info.append(band.to_dict())
        return band_info

    def stack_bands(self, bands, path_as_pattern=None, concat_dim='band'):
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
        StacAsset with mapping of Asset names to Xarray bands

        Examples
        --------
        stack = item.stack_bands(['nir','red'])
        da = stack(chunks=dict(band=1, x=2048, y=2048)).to_dask()

        stack = item.stack_bands(['B4','B5'], path_as_pattern='{band}.TIF')
        da = stack(chunks=dict(band=1, x=2048, y=2048)).to_dask()
        """
        if not EOExtension.has_extension(self._stac_obj):
            raise ValueError('STAC Item must implement "eo" extension to use this method')

        band_info = self._get_band_info()
        configDict = {}
        metadatas = {}
        titles = []
        hrefs = []
        types = []
        assets = self._stac_obj.assets
        for band in bands:
            # band can be band id, name or common_name
            if band in assets:
                info = next(
                    (b for b in band_info if b.get('id', b.get('name')) == band),
                    None,
                )
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
            asset = assets.get(band)
            metadatas[band] = asset.to_dict()
            titles.append(band)
            types.append(asset.media_type)
            hrefs.append(asset.href)

        unique_types = set(types)
        if len(unique_types) != 1:
            raise ValueError(
                f'Stacking failed: bands must have type, multiple found: {unique_types}'
            )

        configDict['name'] = '_'.join(bands)
        configDict['description'] = ', '.join(titles)
        configDict['args'] = dict(
            chunks={}, concat_dim=concat_dim, path_as_pattern=path_as_pattern, urlpath=hrefs
        )
        configDict['metadata'] = metadatas

        return CombinedAssets(configDict)

    def _yaml(self):
        data = {'metadata': {}, 'sources': {}}
        data['metadata'].update(self.metadata)
        for key, source in self.items():
            data['sources'][key] = source._yaml()['sources']['stac_asset']
            data['sources'][key]['direct_access'] = 'allow'
            data['sources'][key]['metadata'].pop('catalog_dir', None)
        return data


class StacAsset(LocalCatalogEntry):
    """
    Maps 1 STAC Item Asset to 1 Intake Catalog Entry
    https://pystac.readthedocs.io/en/latest/api.html#asset
    """

    name = 'stac_asset'
    _stac_cls = pystac.item.Asset

    def __init__(self, key, asset):
        """
        Construct an Intake catalog 'Source' from a STAC Item Asset.
        asset = pystac.item.Asset
        """
        self._stac_obj = asset
        driver = self._get_driver(asset)

        super().__init__(
            name=key,
            description=asset.title,
            driver=driver,
            direct_access='allow',
            args=self._get_args(asset, driver),
            metadata=self._get_metadata(asset),
        )

    def _get_metadata(self, asset):
        """
        Copy STAC Asset Metadata and setup default plot
        """
        metadata = asset.to_dict()
        default_plot = self._get_plot(asset)
        if default_plot:
            metadata['plots'] = default_plot

        return metadata

    def _get_plot(self, asset):
        """
        Default hvplot plot based on Asset mimetype
        """
        # NOTE: consider geojson, parquet, hdf defaults in future...
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
        """
        Assign intake driver for data I/O
        """
        entry_type = asset.media_type

        if entry_type in ['', 'null', None]:

            suffix = '.tif'
            if asset.media_type:
                suffix = os.path.splitext(asset.media_type)[-1]
            if suffix in ['.nc', '.h5', '.hdf']:
                asset.media_type = 'application/netcdf'
                warnings.warn(
                    f'STAC Asset "type" missing, assigning {entry_type} based on href suffix {suffix}:\n{asset.media_type}'  # noqa: E501
                )
            else:
                asset.media_type = default_type
                warnings.warn(
                    f'STAC Asset "type" missing, assuming default type={default_type}:\n{asset}'  # noqa: E501
                )
            entry_type = asset.media_type

        # if mimetype not registered try rasterio driver
        driver = drivers.get(entry_type, default_driver)

        return driver

    def _get_args(self, asset, driver):
        """
        Optional keyword arguments to pass to intake driver
        """
        args = {'urlpath': asset.href}
        if driver in ['netcdf', 'rasterio', 'xarray_image']:
            # NOTE: force using dask?
            args.update(chunks={})

        return args


class CombinedAssets(LocalCatalogEntry):
    """
    Maps multiple STAC Item Assets to 1 Intake Catalog Entry
    """

    def __init__(self, configDict):
        """
        configDict = intake Entry dictionary from stack_bands() method
        """
        super().__init__(
            name=configDict['name'],
            description=configDict['description'],
            driver='rasterio',  # stack_bands only relevant to rasterio driver?
            direct_access=True,
            args=configDict['args'],
            metadata=configDict['metadata'],
        )
