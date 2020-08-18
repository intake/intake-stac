import intake  # noqa: F401
from pkg_resources import DistributionNotFound, get_distribution

from .catalog import StacCatalog, StacCollection, StacItem, StacItemCollection  # noqa: F401

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # noqa: F401; pragma: no cover
    # package is not installed
    __version__ = '999'
