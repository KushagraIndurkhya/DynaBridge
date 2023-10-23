from DynaBridge.attribute import DynamoAttribute
from DynaBridge.exceptions import DynamoAttributeError, DynamoTypeError, DynamoValueError


# The DynamoSchema class is used to store and manage attributes for a DynamoDB schema.
class DynamoSchema:
    def __init__(self):
        """
        The above function is a constructor that initializes two empty lists, `attributes` and
        `attribute_map`.
        """
        self.attributes = []
        self.attribute_map = {}

    def add_attributes(self, attributes):
        """
        The `add_attributes` function adds attributes to an object, checking that each attribute is of type
        DynamoAttribute.
        
        :param attributes: The `attributes` parameter is a list of `DynamoAttribute` objects that are being
        added to the current object
        :return: The method `add_attributes` is returning `self`.
        """
        try:
            for attribute in attributes:
                if not isinstance(attribute, DynamoAttribute):
                    raise DynamoTypeError(
                        f"{attribute} must be of type DynamoAttribute.")
                self.attributes.append(attribute)
                self.attribute_map[attribute.get_attribute_name()] = attribute
            return self
        except Exception as e:
            raise DynamoAttributeError(f"Error adding attributes: {e}")

    def validate(self, item):
        """
        The `validate` function checks if the given `item` has all the required attributes, if the attribute
        types match the expected types, and if the attribute values pass the validation function. If any
        errors are found, it raises a `DynamoValueError` with the corresponding error messages.
        
        :param item: The `item` parameter is a dictionary that represents an item to be validated. Each key
        in the dictionary represents an attribute name, and the corresponding value represents the attribute
        value
        :return: a boolean value of True.
        """
        missing_attributes = []
        type_mismatch_attributes = []
        validation_failed_attributes = []

        for attribute in self.attributes:
            attribute_name = attribute.get_attribute_name()

            if attribute_name not in item:
                missing_attributes.append(attribute_name)
            else:
                item_value = item[attribute_name]
                expected_type = attribute.get_attribute_type()

                if not isinstance(item_value, DynamoAttribute.types[expected_type]):
                    type_mismatch_attributes.append(attribute_name)

                validator = attribute.get_attribute_validator()
                if validator is not None and not validator(item_value):
                    validation_failed_attributes.append(attribute_name)

        error_messages = []

        if missing_attributes:
            error_messages.append(
                f"Missing required attributes: {', '.join(missing_attributes)}")
        if type_mismatch_attributes:
            error_messages.append(
                f"Type mismatch for attributes: {', '.join(type_mismatch_attributes)}")
        if validation_failed_attributes:
            error_messages.append(
                f"Validation failed for attributes: {', '.join(validation_failed_attributes)}")

        if error_messages:
            raise DynamoValueError('\n'.join(error_messages))

        return True

    def fill_defaults(self, item):
        """
        The `fill_defaults` function fills in missing attributes in an item dictionary with their default
        values.
        
        :param item: The `item` parameter is a dictionary that represents an item or object
        """
        for attribute in self.attributes:
            attribute_name = attribute.get_attribute_name()
            if attribute_name not in item:
                item[attribute_name] = attribute.get_attribute_default()
        return item

    def get_schema(self):
        """
        The function `get_schema` returns a list of attribute information for each attribute in the object's
        attributes list.
        :return: A list of attribute information for each attribute in the self.attributes list.
        """
        return [attribute.get_attribute_info() for attribute in self.attributes]

    def get_schema_json(self):
        """
        The function `get_schema_json` returns a JSON representation of the attributes in an object.
        :return: a JSON object that contains the attribute names as keys and the attribute JSON
        representations as values.
        """
        return {attribute.get_attribute_name(): attribute.get_attribute_json() for attribute in self.attributes}

    def get_attribute_map(self):
        """
        The function returns the attribute map of an object.
        :return: The `get_attribute_map` method is returning the `attribute_map` attribute.
        """
        return self.attribute_map
