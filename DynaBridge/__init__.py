# my_dynamodb/__init__.py
from . import table
from . import schema
from . import attribute
from . import models
from . import exceptions
PACKAGE_VERSION = "1.0"


def get_package_version():
    return PACKAGE_VERSION
