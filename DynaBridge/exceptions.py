class DynamoAttributeError(Exception):
    pass


class DynamoTypeError(DynamoAttributeError):
    pass


class DynamoValueError(DynamoAttributeError):
    pass
