
from DynaBridge.exceptions import DynamoTypeError
from DynaBridge.schema import DynamoSchema
from DynaBridge.utills import TimeUtills
import botocore
import logging


class DynamoTable:
    def _handle_error(self, operation, error):
        self.__logger.error("Error during %s: %s", operation, error)
        raise error

    def _handle_response(self, response):
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return response.get('Attributes')
        return None
    
    @staticmethod
    def ignore_key(key):
        # The line `return key.startswith('_DynamoTable_') or key.startswith('_DynamoModel_') or key
        # in ['_dynamo_table']` is a static method `ignore_key` that checks if a given key should be
        # ignored or not. It returns `True` if the key starts with '_DynamoTable_', starts with
        # '_DynamoModel_', or is equal to '_dynamo_table'. This method is used to filter out certain
        # keys when working with DynamoDB tables.
        return key.startswith('_DynamoTable_') or key.startswith('_DynamoModel_') or key in ['_dynamo_table']
    @staticmethod
    def _create_table(db_resource, table_name, primary_key, sort_key=None):
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
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            return table
        except Exception as e:
            raise e

    @classmethod
    def create_table_if_not_exists(cls, db_resource, table_name, primary_key, sort_key=None):
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

    def __init__(self, table_name, db_resource, primary_key, sort_key=None, version_attribute='lockVersion', is_transactional=False, is_encrypted=False):
        self.__table_name = table_name
        self.__primary_key = primary_key
        self.__sort_key = sort_key
        self.__db_resource = self.create_table_if_not_exists(
            db_resource, table_name, primary_key, sort_key)
        self.__version_attribute = version_attribute
        self.__is_transactional = is_transactional
        self.__is_encrypted = is_encrypted
        self.__schema = None
        self.__last_update_attribute = "last_update"
        self.__logger = logging.getLogger(__name__)

    def register_schema(self, schema):
        if not isinstance(schema, DynamoSchema):
            raise DynamoTypeError(f"{schema} must be of type DynamoSchema.")
        self.__schema = schema



    def _get_key(self, obj):
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
        key = {}
        key[self.__primary_key] = primary_key
        if sort_key is not None:
            key[self.__sort_key] = sort_key
        return key

    def _transactional_save(self, key, json_item):
        try:
            key = self._get_key(json_item)
            json_item[self.__version_attribute] = json_item[self.__version_attribute] + 1
            response = self.__db_resource.update_item(
                TableName=self.__table_name,
                Key=key,
                UpdateExpression="set #version = :version",
                ExpressionAttributeNames={
                    "#version": self.__version_attribute
                },
                ExpressionAttributeValues={
                    ":version": json_item[self.__version_attribute]
                },
                ReturnValues="UPDATED_NEW"
            )
            return self._handle_response(response)
        except Exception as e:
            self._handle_error('replace', e)

    def save(self, item):
        try:
            if self.__is_transactional and self.__version_attribute not in item:
                item[self.__version_attribute] = 0
            else:
                item[self.__version_attribute] = item[self.__version_attribute] + 1
            if self.__last_update_attribute not in item:
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

    def get(self, key):
        try:
            response = self.__db_resource.get_item(
                TableName=self.__table_name,
                Key=key
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return response['Item']
            else:
                self._handle_error('get', response)
        except Exception as e:
            self._handle_error('get', e)

    def getAll(self):
        try:
            response = self.__db_resource.scan(
                TableName=self.__table_name
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return response['Items']
            else:
                self._handle_error('getAll', response)
        except Exception as e:
            self._handle_error('getAll', e)
    def batch_write(self, items):
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
        try:
            response = self.__db_resource.delete_item(
                TableName=self.__table_name,
                Key=key
            )
            return self._handle_response(response)
        except Exception as e:
            self._handle_error('delete', e)


    
