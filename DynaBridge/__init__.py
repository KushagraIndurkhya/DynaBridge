# my_dynamodb/__init__.py
from . import table
from . import schema
from . import attribute
from . import models
from . import exceptions
PACKAGE_VERSION = "0.0.2"


def get_package_version():
    return PACKAGE_VERSION
__version__ = get_package_version()
__all__ = ['table', 'schema', 'attribute', 'models', 'exceptions']

