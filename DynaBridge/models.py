from boto3.dynamodb.conditions import Attr
from DynaBridge.table import DynamoTable
from DynaBridge.exceptions import *


class DynamoModel:
    _dynamo_table = None

    @classmethod
    def set_dynamo_table(cls, dynamo_table):
        cls._dynamo_table = dynamo_table

    @staticmethod
    def ignore_key(key):
        return key.startswith('_DynamoTable_') or key.startswith('_DynamoModel_') or key in ['_dynamo_table']

    def get_self_json(self):
        return {key: value for key, value in self.__dict__.items() if not self.ignore_key(key)}

    def fetch_key(self):
        table_info = self._dynamo_table.__dict__
        primary_key = table_info['_DynamoTable__primary_key']
        sort_key = table_info['_DynamoTable__sort_key']
        key = {
            primary_key: self.__dict__[primary_key]
        } if sort_key is None else {
            primary_key: self.__dict__[primary_key],
            sort_key: self.__dict__[sort_key]
        }
        return key

    def __init__(self):
        if self._dynamo_table is None:
            raise ValueError(
                "DynamoTable instance is not set. Call set_dynamo_table with a DynamoTable instance.")

    def save(self):
        return self._dynamo_table.save(self.get_self_json())

    def get(self):
        return self._dynamo_table.get(self.fetch_key())

    @classmethod
    def get_by_primary_key(cls, key):
        return cls._dynamo_table.get(key)

    @classmethod
    def get_all(cls):
        return cls._dynamo_table.getAll()

    @classmethod
    def delete_by_primary_key(cls, key):
        return cls._dynamo_table.delete(key)

    @classmethod
    def get_all_by_attribute(cls, attribute, value):
        return cls._dynamo_table.scan(filter_expression=Attr(attribute).eq(value))
