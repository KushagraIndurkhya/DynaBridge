from DynaBridge.exceptions import DynamoTypeError, DynamoValueError
import datetime
import uuid


class DynamoAttribute:
    types = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "list": list,
        "dict": dict,
        "set": set,
        "tuple": tuple,
        "datetime": str,
        "uuid": str

    }
    defaults = {
        "str": '',
        "int": 0,
        "float": 0.0,
        "bool": False,
        "list": [],
        "dict": {},
        "set": set(),
        "tuple": tuple(),
        "datetime": None,
        "uuid": None
    }
    dynamic_defaults = {
        "datetime": lambda: datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "uuid": lambda: str(uuid.uuid4()),
        "uuid1": lambda: str(uuid.uuid1()),
        "uuid4": lambda: str(uuid.uuid4()),
    }

    def _initialize_defaults(self):
        """
        The function `_initialize_defaults` returns the default value for a given attribute type if it is
        not already set.
        :return: The method is returning the default value for the attribute. If the `__attribute_default`
        is `None`, it checks if the `__attribute_type` is in the `DynamoAttribute.dynamic_defaults`
        dictionary. If it is, it calls the corresponding function to get the dynamic default value. If it is
        not in the dictionary, it returns the default value from the `DynamoAttribute.defaults`
        """
        if self.__attribute_default is None:
            if self.__attribute_type in DynamoAttribute.dynamic_defaults:
                return DynamoAttribute.dynamic_defaults[self.__attribute_type]()
            else:
                return DynamoAttribute.defaults[self.__attribute_type]
        else:
            return self.__attribute_default

    def _verify_type(self):
        """
        The function verifies if the attribute type is supported and raises an error if it is not.
        """
        if self.__attribute_type not in DynamoAttribute.defaults:
            raise DynamoTypeError(
                f"Attribute type {self.__attribute_type} is not supported. Supported types are {', '.join(DynamoAttribute.defaults.keys())}")

    def _verify_defaults(self):
        """
        The function `_verify_defaults` checks if the default value of an attribute is of the correct
        type.
        """
        if self.__attribute_default is None:
            self.__attribute_default = self._initialize_defaults()
        else:
            if not isinstance(self.__attribute_default, DynamoAttribute.types[self.__attribute_type]):
                raise DynamoTypeError(
                    f"Default value {self.__attribute_default} is not of type {self.__attribute_type}")

    def __init__(self, attribute_name, data_type, attribute_default=None, attribute_required=False, attribute_immutable=False, attribute_validator=None):
        """
        The function is an initializer for a class that sets various attributes and verifies their types and
        defaults.

        :param attribute_name: The attribute_name parameter is a string that represents the name of the
        attribute. It is used to identify and access the attribute within the class
        :param data_type: The `data_type` parameter represents the expected data type of the attribute. It
        can be any valid Python data type such as `int`, `str`, `float`, `list`, `dict`, etc. This parameter
        is used to ensure that the attribute value is of the correct type
        :param attribute_default: The attribute_default parameter is used to specify a default value for the
        attribute if no value is provided during initialization
        :param attribute_required: The attribute_required parameter is a boolean flag that indicates whether
        the attribute is required or not. If set to True, it means that the attribute must have a value
        assigned to it. If set to False, the attribute can be left empty or assigned a value later, defaults
        to False (optional)
        :param attribute_immutable: The attribute_immutable parameter is a boolean flag that indicates
        whether the attribute should be immutable or not. If attribute_immutable is set to True, it means
        that the attribute cannot be modified once it is set. If it is set to False, the attribute can be
        modified after it is set, defaults to False (optional)
        :param attribute_validator: The attribute_validator parameter is a function that can be used to
        validate the value of the attribute. It takes the value as input and returns True if the value is
        valid, and False otherwise. This can be used to enforce any additional constraints or rules on the
        attribute value
        """
        self.__attribute_name = attribute_name
        self.__attribute_type = data_type
        self._verify_type()
        self.__attribute_default = attribute_default
        self._verify_defaults()
        self.__attribute_required = attribute_required
        self.__attribute_immutable = attribute_immutable
        self.__attribute_validator = attribute_validator
        self.__value = None

    def get_attribute_name(self):
        """
        The function returns the value of the attribute name.
        :return: The method is returning the value of the attribute named "__attribute_name".
        """
        return self.__attribute_name

    def get_attribute_type(self):
        """
        The function returns the attribute type of an object.
        :return: The method is returning the value of the private attribute "__attribute_type".
        """
        return self.__attribute_type

    def get_attribute_default(self):
        """
        The function returns the value of the attribute named "__attribute_default".
        :return: The method is returning the value of the attribute named "__attribute_default".
        """
        return self.__attribute_default

    def is_attribute_required(self):
        """
        The function returns the value of the attribute "__attribute_required".
        :return: The value of the attribute `__attribute_required` is being returned.
        """
        return self.__attribute_required

    def is_attribute_immutable(self):
        """
        The function returns the value of the attribute_immutable attribute.
        :return: The method is returning the value of the attribute `__attribute_immutable`.
        """
        return self.__attribute_immutable

    def get_attribute_validator(self):
        """
        The function returns the attribute validator of an object.
        :return: The method `get_attribute_validator` is returning the value of the
        `__attribute_validator` attribute.
        """
        return self.__attribute_validator

    def get_attribute_value(self, value):
        """
        The function `get_attribute_value` returns the given value if it is not None, otherwise it
        initializes and returns the default value.

        :param value: The `value` parameter is the value that is passed to the `get_attribute_value` method
        :return: the value if it is not None or False. If the value is None or False, it will call the
        `_initialize_defaults()` method and return its result.
        """
        return value or self._initialize_defaults()

    def set_attribute_value(self, value):
        """
        The function `set_attribute_value` sets the value of an attribute, but raises errors if the
        attribute is immutable, the value is not of the correct type, or fails validation.

        :param value: The `value` parameter is the value that you want to set for the attribute
        """
        if self.__attribute_immutable:
            raise DynamoValueError(
                f"Attribute '{self.__attribute_name}' is immutable and cannot be modified.")
        if value is not None:
            if not isinstance(value, DynamoAttribute.types[self.__attribute_type]):
                raise DynamoTypeError(
                    f"Value '{value}' is not of type '{self.__attribute_type}'.")
            if self.__attribute_validator is not None and not self.__attribute_validator(value):
                raise DynamoValueError(f"Value '{value}' failed validation.")
        self.__value = value

    def get_attribute_json(self):
        """
        The function returns a JSON object containing the attribute name and value.
        :return: The method `get_attribute_json` is returning a dictionary with the attribute name as
        the key and the attribute value as the value.
        """
        return {self.__attribute_name: self.__value}

    def get_attribute_info(self):
        """
        The function `get_attribute_info` returns a dictionary containing information about an attribute.
        :return: A dictionary containing information about an attribute.
        """
        return {
            "name": self.__attribute_name,
            "type": self.__attribute_type,
            "default": self.__attribute_default,
            "required": self.__attribute_required,
            "immutable": self.__attribute_immutable,
            "validator": self.__attribute_validator
        }
