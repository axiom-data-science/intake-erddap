"""Project version module."""
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("intake-erddap")
except PackageNotFoundError:
    # package is not installed
    __version__ = "unknown"