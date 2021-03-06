from intake.source.base import DataSource, PatternMixin, Schema
from intake.source.utils import reverse_formats
from pkg_resources import get_distribution

__version__ = get_distribution('intake_stac').version


class RioxarraySource(DataSource, PatternMixin):
    """Open a xarray dataset via Rioxarray.
    This creates an xarray.DataArray https://github.com/corteva/rioxarray

    Parameters
    ----------
    urlpath: str or iterable, location of data
        May be a local path, or remote path if including a protocol specifier
        such as ``'s3://'``. May include glob wildcards or format pattern strings.
        Must be a format supported by rasterIO (normally GeoTiff).
        Some examples:
            - ``{{ CATALOG_DIR }}data/RGB.tif``
            - ``s3://data/*.tif``
            - ``s3://data/landsat8_band{band}.tif``
            - ``s3://data/{location}/landsat8_band{band}.tif``
            - ``{{ CATALOG_DIR }}data/landsat8_{start_date:%Y%m%d}_band{band}.tif``
    chunks: None or int or dict, optional
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays. default `None` loads numpy arrays.
    path_as_pattern: bool or str, optional
        Whether to treat the path as a pattern (ie. ``data_{field}.tif``)
        and create new coodinates in the output corresponding to pattern
        fields. If str, is treated as pattern to match on. Default is True.
    """

    name = 'rioxarray'
    version = __version__
    container = 'xarray'
    partition_access = True

    def __init__(
        self,
        urlpath,
        chunks=None,
        concat_dim='concat_dim',
        override_coords=None,
        xarray_kwargs=None,
        metadata=None,
        path_as_pattern=True,
        storage_options=None,
        **kwargs,
    ):
        self.path_as_pattern = path_as_pattern
        self.urlpath = urlpath
        self.chunks = chunks
        self.dim = concat_dim
        # self.storage_options = storage_options or {} #only relevant to fsspec?
        self.override_coords = override_coords
        self._kwargs = xarray_kwargs or {}
        self._ds = None
        # if isinstance(self.urlpath, list):
        #    self._can_be_local = fsspec.utils.can_be_local(self.urlpath[0])
        # else:
        #    self._can_be_local = fsspec.utils.can_be_local(self.urlpath)

        # Why is this necessary?
        super(RioxarraySource, self).__init__(metadata=metadata)

    def _open_files(self, files):
        """
        basically open_mfrasterio()
        """
        import rioxarray
        import xarray as xr

        # not metadata-aware, so this assigns band=1 regardless of true band#
        das = [rioxarray.open_rasterio(f, chunks=self.chunks, **self._kwargs) for f in files]
        out = xr.concat(das, dim=self.dim)

        # by default map band names to coordinates instead of band=1,1,1
        coords = {}
        # NOTE very robust, and requires potentially really long names
        # coords = {self.dim: self.name.split('_')}
        # band = 1,2,3 instead of band=1,1,1
        # coords = {self.dim: range(1, len(out.coords[self.dim])+1)}
        # NOTE that we have all the STAC metadata at our disposal here:
        # coords = dict(time = ('time', [self.metadata[f]['datetime'] for f in files]))

        if self.pattern:
            pattern_matches = reverse_formats(self.pattern, files)
            coords = {self.dim: pattern_matches[self.dim]}

        if self.override_coords:
            coords = {self.dim: self.override_coords}

        return out.assign_coords(**coords).chunk(self.chunks)

    def _open_dataset(self):
        import rioxarray

        # if self._can_be_local:
        #    files = fsspec.open_local(self.urlpath, **self.storage_options)
        # else:
        # pass URLs to delegate remote opening to rasterio library
        #    files = self.urlpath
        # files = fsspec.open(self.urlpath, **self.storage_options).open()
        files = self.urlpath
        if isinstance(files, list):
            self._ds = self._open_files(files)
        else:
            self._ds = rioxarray.open_rasterio(files, chunks=self.chunks, **self._kwargs)

    # NOTE:  don't know what's going on here
    # seems overly complicated...
    # https://github.com/intake/intake-xarray/issues/20#issuecomment-432782846
    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
        # from .xarray_container import serialize_zarr_ds
        import msgpack
        import xarray as xr

        self.urlpath, *_ = self._get_cache(self.urlpath)

        if self._ds is None:
            self._open_dataset()

            ds2 = xr.Dataset({'raster': self._ds})
            metadata = {
                'dims': dict(ds2.dims),
                'data_vars': {k: list(ds2[k].coords) for k in ds2.data_vars.keys()},
                'coords': tuple(ds2.coords.keys()),
                'array': 'raster',
            }
            # if getattr(self, 'on_server', False):
            #    metadata['internal'] = serialize_zarr_ds(ds2)
            for k, v in self._ds.attrs.items():
                try:
                    msgpack.packb(v)
                    metadata[k] = v
                except TypeError:
                    pass

            if hasattr(self._ds.data, 'npartitions'):
                npart = self._ds.data.npartitions
            else:
                npart = None

            self._schema = Schema(
                datashape=None,
                dtype=str(self._ds.dtype),
                shape=self._ds.shape,
                npartitions=npart,
                extra_metadata=metadata,
            )

        return self._schema

    def read(self):
        """Return a version of the xarray with all the data in memory"""
        self._load_metadata()
        return self._ds.load()

    def read_chunked(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def read_partition(self, i):
        """Fetch one chunk of data at tuple index i
        """
        import numpy as np

        self._load_metadata()
        if not isinstance(i, (tuple, list)):
            raise TypeError('For Xarray sources, must specify partition as ' 'tuple')
        if isinstance(i, list):
            i = tuple(i)
        if hasattr(self._ds, 'variables') or i[0] in self._ds.coords:
            arr = self._ds[i[0]].data
            i = i[1:]
        else:
            arr = self._ds.data
        if isinstance(arr, np.ndarray):
            return arr
        # dask array
        return arr.blocks[i].compute()

    def to_dask(self):
        """Return xarray object where variables are dask arrays"""
        return self.read_chunked()

    def close(self):
        """Delete open file from memory"""
        self._ds = None
        self._schema = None
