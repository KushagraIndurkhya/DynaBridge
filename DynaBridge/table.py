
from DynaBridge.exceptions import DynamoTypeError
from DynaBridge.schema import DynamoSchema
from DynaBridge.utills import TimeUtills

import botocore
import logging


class DynamoTable:
    def _handle_error(self, operation, error):
        """
        The function `_handle_error` logs an error message and raises the error.
        
        :param operation: The `operation` parameter represents the name or description of the operation that
        encountered an error. It is used to provide context about the specific operation that failed
        :param error: The "error" parameter is the error object that was raised during the operation. It
        could be an exception object or any other type of error object
        """
        self.__logger.error("Error during %s: %s", operation, error)
        raise error

    def _handle_response(self, response):
        """
        The function `_handle_response` checks if the HTTP status code of the response is 200 and returns
        the attributes if it is, otherwise it returns None.
        
        :param response: The `response` parameter is a dictionary object that represents the response
        received from an API call. It contains information about the response, such as the HTTP status code
        and the attributes returned by the API
        :return: The method `_handle_response` returns the value of `response.get('Attributes')` if the HTTP
        status code in `response['ResponseMetadata']` is 200. Otherwise, it returns `None`.
        """
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return response.get('Attributes')
        return None   
    @staticmethod
    def ignore_key(key):
        """
        The function `ignore_key` returns True if the given key starts with '_DynamoTable_',
        '_DynamoModel_', or is equal to '_dynamo_table', otherwise it returns False.
        
        :param key: The `key` parameter is a string that represents a key or attribute name
        :return: True if the key starts with '_DynamoTable_', '_DynamoModel_', or if the key is
        '_dynamo_table'. Otherwise, it returns False.
        """
        return key.startswith('_DynamoTable_') or key.startswith('_DynamoModel_') or key in ['_dynamo_table']
    @staticmethod
    def _create_table(db_resource, table_name, primary_key, sort_key=None,ReadCapacityUnits=5,WriteCapacityUnits=5):
        """
        The `_create_table` function creates a table in DynamoDB with the specified table name, primary key,
        sort key (optional), and provisioned throughput.
        
        :param db_resource: The `db_resource` parameter is the resource object for connecting to the
        DynamoDB database. It is used to create a connection to the DynamoDB service and perform operations
        on the database
        :param table_name: The name of the table you want to create in the DynamoDB database
        :param primary_key: The primary key is the main attribute used to uniquely identify each item in the
        table. It is required for every table in DynamoDB and must be unique for each item
        :param sort_key: The `sort_key` parameter is an optional parameter that specifies the attribute to
        be used as the sort key for the table. If provided, the table will have a composite primary key
        consisting of the primary key and the sort key
        :param ReadCapacityUnits: ReadCapacityUnits is the number of read capacity units to provision for
        the table. This determines the maximum number of reads per second that the table can handle,
        defaults to 5 (optional)
        :param WriteCapacityUnits: The WriteCapacityUnits parameter specifies the number of write capacity
        units to provision for the table. Write capacity units represent the number of writes per second
        that the table can handle, defaults to 5 (optional)
        :return: the created table object.
        """
        dynamodb = db_resource
        key_schema = [
            {'AttributeName': primary_key, 'KeyType': 'HASH'}
        ]
        attribute_definitions = [
            {'AttributeName': primary_key, 'AttributeType': 'S'}
        ]

        if sort_key:
            key_schema.append({'AttributeName': sort_key, 'KeyType': 'RANGE'})
            attribute_definitions.append(
                {'AttributeName': sort_key, 'AttributeType': 'S'})

        try:
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                ProvisionedThroughput={
                    'ReadCapacityUnits': ReadCapacityUnits,
                    'WriteCapacityUnits': WriteCapacityUnits
                }
            )
            return table
        except Exception as e:
            raise e

    @classmethod
    def create_table_if_not_exists(cls, db_resource, table_name, primary_key, sort_key=None):
        """
        The function checks if a table exists in a DynamoDB database and creates it if it doesn't.
        
        :param cls: The parameter `cls` is a reference to the class itself. It is commonly used in class
        methods to access class-level variables or methods. In this case, it is used to call the
        `_create_table` method of the class
        :param db_resource: db_resource is the resource object for connecting to the DynamoDB database. It
        is used to create a connection to the database and perform operations on it
        :param table_name: The name of the table you want to create or check if it exists in the DynamoDB
        database
        :param primary_key: The primary key is a unique identifier for each item in the table. It is used to
        uniquely identify and retrieve items from the table
        :param sort_key: The `sort_key` parameter is an optional parameter that specifies the sort key for
        the table. In Amazon DynamoDB, a table can have a primary key that consists of one or two
        attributes. The primary key uniquely identifies each item in the table
        :return: The method is returning the DynamoDB table object if it already exists and is active. If
        the table does not exist, it calls the `_create_table` method to create the table and returns the
        created table object.
        """
        try:
            dynamodb = db_resource
            table = dynamodb.Table(table_name)
            if table.table_status == 'ACTIVE':
                return table
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table does not exist, create it
                return cls._create_table(db_resource, table_name, primary_key, sort_key)
            else:
                raise e

    def __init__(self, table_name, db_resource, primary_key, sort_key=None, version_attribute='lockVersion',track_last_update_time=True, is_transactional=False, is_encrypted=False):
        """
        The above function is a constructor for a class that initializes various attributes and creates a
        table if it does not already exist in a database resource.
        
        :param table_name: The name of the table in the database where the data will be stored
        :param db_resource: The `db_resource` parameter is the resource or connection object that is used to
        interact with the database. It could be an instance of a database client or a connection object that
        provides methods for executing queries and managing transactions
        :param primary_key: The primary_key parameter is used to specify the primary key attribute of the
        table. The primary key uniquely identifies each item in the table and is used to efficiently
        retrieve and update items
        :param sort_key: The `sort_key` parameter is an optional attribute that can be used to define a
        secondary sort order for items in the table. It is used in conjunction with the primary key to
        uniquely identify items in the table. If specified, the sort key must be unique for each item in the
        table
        :param version_attribute: The `version_attribute` parameter is used to specify the name of the
        attribute that will be used to track the version or lock of an item in the table. This attribute is
        typically used to prevent concurrent modifications to the same item, defaults to lockVersion
        (optional)
        :param track_last_update_time: The parameter "track_last_update_time" is a boolean flag that
        determines whether the table should track the last update time of each item. If set to True, the
        table will have a "last_update" attribute that stores the timestamp of the last update. If set to
        False, the table will not, defaults to True (optional)
        :param is_transactional: The `is_transactional` parameter is a boolean flag that indicates whether
        the table should support transactions. If set to `True`, the table will be created as a
        transactional table, allowing atomic operations and ensuring data consistency. If set to `False`,
        the table will be created as a non-, defaults to False (optional)
        :param is_encrypted: The `is_encrypted` parameter is a boolean flag that indicates whether the data
        stored in the table should be encrypted or not. If `is_encrypted` is set to `True`, the data will be
        encrypted, otherwise it will be stored as plain text, defaults to False (optional)
        """
        self.__table_name = table_name
        self.__primary_key = primary_key
        self.__sort_key = sort_key
        self.__db_resource = self.create_table_if_not_exists(
            db_resource, table_name, primary_key, sort_key)
        self.__version_attribute = version_attribute
        self.__is_transactional = is_transactional
        self.__is_encrypted = is_encrypted
        self.__schema = None
        self.__last_update_attribute = "last_update" if track_last_update_time else None
        self.__logger = logging.getLogger(__name__)
    def get_version_attribute(self):
        """
        The function `get_version_attribute` returns the value of the `__version_attribute` attribute.
        :return: The method is returning the value of the `__version_attribute` attribute.
        """
        return self.__version_attribute
    def register_schema(self, schema):
        """
        The function `register_schema` checks if the input `schema` is of type `DynamoSchema` and assigns it
        to the private attribute `__schema`.
        
        :param schema: The `schema` parameter is an instance of the `DynamoSchema` class
        """
        if not isinstance(schema, DynamoSchema):
            raise DynamoTypeError(f"{schema} must be of type DynamoSchema.")
        self.__schema = schema
    def _get_key(self, obj):
        """
        The function `_get_key` retrieves the primary and sort keys from a given object's dictionary
        representation.
        
        :param obj: The `obj` parameter is an object that has a method `get_self_json()`. This method
        returns a dictionary representation of the object
        :return: a dictionary containing the primary key and sort key of the given object.
        """
        object_dict = obj.get_self_json()
        key = {}
        if self.__primary_key in object_dict:
            key[self.__primary_key] = object_dict[self.__primary_key]
        else:
            raise Exception('Primary key is required')
        if self.__sort_key in object_dict:
            key[self.__sort_key] = object_dict[self.__sort_key]
        return key

    def _generate_key(self, primary_key, sort_key=None):
        """
        The function `_generate_key` generates a dictionary key with a primary key and an optional sort key.
        
        :param primary_key: The primary key is a unique identifier for an item in a database table. It is
        used to uniquely identify each item and is typically a non-null value
        :param sort_key: The `sort_key` parameter is an optional parameter that represents the sort key for
        generating the key. If a `sort_key` value is provided, it will be added to the generated key
        dictionary. If no `sort_key` value is provided (i.e., `sort_key` is `None
        :return: a dictionary object called "key".
        """
        key = {}
        key[self.__primary_key] = primary_key
        if sort_key is not None:
            key[self.__sort_key] = sort_key
        return key

    def save(self, item):
        """
        The `save` function saves an item to a database table, handling transactional updates, versioning,
        last update timestamp, schema validation, and error handling.
        
        :param item: The `item` parameter is a dictionary that represents the data to be saved in the
        database. It contains key-value pairs where the keys represent the attribute names and the values
        represent the attribute values of the item
        :return: The item is being returned.
        """
        try:
            if self.__is_transactional and self.__version_attribute not in item:
                item[self.__version_attribute] = 0
            elif self.__is_transactional and self.__version_attribute in item:
                item[self.__version_attribute] = item[self.__version_attribute] + 1
            if self.__last_update_attribute is not None:
                item[self.__last_update_attribute] = TimeUtills.get_current_utc_datetime()
            if self.__schema is not None:
                self.__schema.fill_defaults(item)
                self.__schema.validate(item)

            response = self.__db_resource.put_item(
                TableName=self.__table_name,
                Item=item
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return item
            else:
                self._handle_error('create', response)
        except Exception as e:
            self._handle_error('create', e)
    def update_version(self,key,new_version):
        """
        The `update_version` function updates the version attribute of an item in a database table.
        
        :param key: The `key` parameter is the primary key of the item you want to update in the database.
        It is used to identify the item you want to update
        :param new_version: The new version is the updated value that you want to set for the version
        attribute in the database item
        :return: The method `update_version` is returning the result of the `_handle_response` method.
        """
        try:
            response = self.__db_resource.update_item(
                TableName=self.__table_name,
                Key=key,
                UpdateExpression="set #version = :version",
                ExpressionAttributeNames={
                    "#version": self.__version_attribute
                },
                ExpressionAttributeValues={
                    ":version": new_version
                },
                ReturnValues="UPDATED_NEW"
            )
            return self._handle_response(response)
        except Exception as e:
            self._handle_error('update_version', e)

    def get(self, key):
        """
        The function `get` retrieves an item from a database table using a given key and handles any errors
        that occur.
        
        :param key: The `key` parameter is the key used to retrieve an item from the database. It is
        typically a dictionary that specifies the primary key values for the item you want to retrieve
        :return: The code is returning the item with the specified key from the database.
        """
        try:
            response = self.__db_resource.get_item(
                TableName=self.__table_name,
                Key=key
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Item' in response:
                return response['Item']
            else:
                return None
        except Exception as e:
            self._handle_error('get', e)
            return None

    def getAll(self):
        """
        The function `getAll` retrieves all items from a database table and returns them as a list.
        :return: the 'Items' from the response if the HTTP status code is 200.
        """
        try:
            response = self.__db_resource.scan(
                TableName=self.__table_name
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return response['Items']
            else:
                return None
        except Exception as e:
            self._handle_error('getAll', e)
            return None
    def batch_write(self, items):
        """
        The `batch_write` function writes a batch of items to a database using a batch writer, and returns a
        list of successfully written items and a list of items that encountered errors.
        
        :param items: The `items` parameter is a list of objects that need to be written to a database. Each
        object in the list should have a method called `get_self_json()` that returns a JSON representation
        of the object
        :return: The `batch_write` method returns two lists: `success` and `errors`. The `success` list
        contains items that were successfully written to the database, while the `errors` list contains
        items that encountered an error during the writing process.
        """
        try:
            success = []
            errors = []
            with self.__db_resource.batch_writer() as batch:
                for item in items:
                    item_json = item.get_self_json()
                    resp = batch.put_item(Item=item_json)
                    if resp and resp['ResponseMetadata']['HTTPStatusCode'] == 200:
                        success.append(item)
                    else:
                        errors.append(item)
            return success, errors
        except Exception as e:
            self._handle_error('batch_write', e)

    def scan(self, filter_expression=None, projection_expression=None):
        """
        The `scan` function retrieves items from a DynamoDB table based on a filter expression and returns
        the items.
        
        :param filter_expression: The filter_expression parameter is used to specify the conditions that
        must be met for an item to be included in the scan operation's results. It is an optional parameter
        and if not provided, all items in the table will be scanned
        :param projection_expression: The `projection_expression` parameter is used to specify the
        attributes that should be included in the scan result. It allows you to retrieve only specific
        attributes from the items in the table, rather than retrieving all attributes
        :return: the items that match the filter expression from the specified table in the database.
        """
        try:
            if filter_expression is None:
                filter_expression = {}
            if projection_expression is None:
                projection_expression = []
            response = self.__db_resource.scan(
                TableName=self.__table_name,
                FilterExpression=filter_expression
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return response['Items']
            else:
                self._handle_error('scan', response)
        except Exception as e:
            self._handle_error('scan', e)
    def delete(self, key):
        """
        The function deletes an item from a database table using a given key.
        
        :param key: The `key` parameter is the primary key of the item you want to delete from the database
        table. It is used to identify the item that needs to be deleted
        :return: The method is returning the result of the `_handle_response` method.
        """
        try:
            response = self.__db_resource.delete_item(
                TableName=self.__table_name,
                Key=key
            )
            return self._handle_response(response)
        except Exception as e:
            self._handle_error('delete', e)
    


    
