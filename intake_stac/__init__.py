from ._version import get_versions
from .catalog import StacCatalog, StacCollection, StacItem

__version__ = get_versions()['version']
del get_versions
