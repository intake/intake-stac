import intake  # noqa: F401

from ._version import get_versions
from .catalog import StacCatalog, StacCollection, StacItem, StacItemCollection

__version__ = get_versions()['version']
del get_versions


__all__ = ['StacCatalog', 'StacCollection', 'StacItem', 'StacItemCollection']
