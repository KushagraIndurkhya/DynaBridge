from DynaBridge.table import DynamoTable
from DynaBridge.models import DynamoModel
from DynaBridge.exceptions import *
from DynaBridge.attribute import *
from DynaBridge.schema import *
from DynaBridge.utills import *
import boto3


if __name__ == "__main__":
    class User(DynamoModel):
        def __init__(self, name, country, email):
            super().__init__()
            # self.UserId=user_id
            self.Name = name
            self.Country = country
            self.Email = email
            # self.Status=status
    UserTable = DynamoTable(table_name='User', db_resource=boto3.resource(
        'dynamodb'), primary_key='UserId')
    UserSchema = DynamoSchema().add_attributes([
        DynamoAttribute(attribute_name='UserId',
                        data_type='uuid', attribute_required=True),
        DynamoAttribute(attribute_name='Name', data_type='str',
                        attribute_required=True),
        DynamoAttribute(attribute_name='Country', data_type='str',
                        attribute_required=True, attribute_default='India'),
        DynamoAttribute(attribute_name='Email', data_type='str', attribute_required=False,
                        attribute_validator=lambda x: '@' in x, attribute_default="admin+missing@salt.pe"),
        DynamoAttribute(attribute_name='Status',
                        data_type='str', attribute_required=True)
    ])
    UserTable.register_schema(UserSchema)
    User.set_dynamo_table(UserTable)
    user = User(name='Raj', country='India', email='admin@admin.in').save()
    print(User.get_by_primary_key(key={'UserId': "1"}))
    print(User.get_all_by_attribute(attribute='Country', value='India'))
