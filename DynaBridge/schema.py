from DynaBridge.attribute import DynamoAttribute
from DynaBridge.exceptions import DynamoAttributeError, DynamoTypeError, DynamoValueError


class DynamoSchema:
    def __init__(self):
        self.attributes = []
        self.attribute_map = {}

    def add_attributes(self, attributes):
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
        print(item)
        for attribute in self.attributes:
            attribute_name = attribute.get_attribute_name()
            if attribute_name not in item:
                item[attribute_name] = attribute.get_attribute_default()
        return item

    def get_schema(self):
        return [attribute.get_attribute_info() for attribute in self.attributes]

    def get_schema_json(self):
        return {attribute.get_attribute_name(): attribute.get_attribute_json() for attribute in self.attributes}

    def get_attribute_map(self):
        return self.attribute_map
