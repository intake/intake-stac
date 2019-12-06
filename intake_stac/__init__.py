from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

import intake  # noqa: F401
from .catalog import StacCatalog, StacCollection, StacItem, StacItemCollection

__all__ = ["StacCatalog", "StacCollection", "StacItem", "StacItemCollection"]
