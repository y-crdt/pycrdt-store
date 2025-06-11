import importlib.metadata

from .base import BaseYStore as BaseYStore
from .base import YDocNotFound as YDocNotFound
from .file import FileYStore as FileYStore
from .file import TempFileYStore as TempFileYStore
from .sqlite import SQLiteYStore as SQLiteYStore

try:
    __version__ = importlib.metadata.version("pycrdt.store")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
