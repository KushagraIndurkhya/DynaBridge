from boto3.dynamodb.conditions import Attr
from DynaBridge.exceptions import *
import time
import functools
class DynamoModel:
    _dynamo_table = None
    @classmethod
    def from_dict(cls, data):
        instance = cls()
        for key, value in data.items():
            setattr(instance, key, value)
        return instance
    @classmethod
    def set_dynamo_table(cls, dynamo_table):
        cls._dynamo_table = dynamo_table
    @staticmethod
    def ignore_key(key):
        return key.startswith('_DynamoTable_') or key.startswith('_DynamoModel_') or key in ['_dynamo_table']
    def get_self_json(self):
        return {key: value for key, value in self.__dict__.items() if not self.ignore_key(key)}
    def json(self):
        return self.get_self_json()
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
    def create(self):
        created_obj=self._dynamo_table.save(self.get_self_json())
        return self.from_dict(created_obj) if created_obj else None
    def save(self):
        saved_object=self._dynamo_table.save(self.get_self_json())
        return self.from_dict(saved_object) if saved_object else None 
    def get(self):
        data = self._dynamo_table.get(self.fetch_key())
        return self.from_dict(data) if data else None
    def delete(self):
        return self._dynamo_table.delete(self.fetch_key())
    def save_if(self, condition):
        if condition(self):
            return self.save()
        return None
    @classmethod
    def get_by_primary_key(cls, key):
        response = cls._dynamo_table.get(key)
        return cls.from_dict(response) if response else None
    @classmethod
    def delete_by_primary_key(cls, key):
        return cls._dynamo_table.delete(key)
    @classmethod
    def get_all_by_attribute(cls, attribute, value):
        return list(map(cls.from_dict, cls._dynamo_table.scan(
            Attr(attribute).eq(value))))
    @classmethod
    def exists(cls, key):
        return True if cls._dynamo_table.get(key) is not None else False
    @classmethod
    def get_all(cls):
        return list(map(cls.from_dict, cls._dynamo_table.getAll())) 
    @classmethod
    def transactional_lock(cls,func,max_retries=100,base_sleep = 0.1 ,max_sleep = 2.0 ,logger=None,custom_retrier=None):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self._dynamo_table:
                raise ValueError("DynamoTable instance is not set. Call set_dynamo_table with a DynamoTable instance.")
            key = self.fetch_key()
            original_version = self._dynamo_table.get(key).get(self._dynamo_table.get_version_attribute(), 0)
            retries = 0
            while retries < max_retries:
                try:
                    updated_version = self._dynamo_table.get(key).get(
                        self._dynamo_table.get_version_attribute(), 0)
                    if updated_version != original_version:
                        if logger:
                            logger.warning("Version mismatch, retrying.")
                        sleep_time = min(base_sleep * 2**retries, max_sleep)
                        time.sleep(sleep_time)
                        retries += 1
                        continue
                    return func(self, *args, **kwargs)
                except Exception as e:
                    if logger:
                        logger.error(f"Transaction failed: {e}")
                    retries += 1
            raise Exception("Max retry attempts exceeded")
        return wrapper
