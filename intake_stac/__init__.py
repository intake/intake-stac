from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .catalog import StacCatalog, StacCollection, StacItem

__all__ = ["StacCatalog", "StacCollection", "StacItem"]
