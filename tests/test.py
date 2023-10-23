from DynaBridge.table import DynamoTable
from DynaBridge.models import DynamoModel
from DynaBridge.exceptions import *
from DynaBridge.attribute import *
from DynaBridge.schema import *
import boto3

if __name__ == "__main__":
    # Create a DynamoTable
    UserTable = DynamoTable(table_name='User', db_resource=boto3.resource(
        'dynamodb'), primary_key='UserId')

    # Define a DynamoSchema with attributes
    UserSchema = DynamoSchema().add_attributes([
        DynamoAttribute(attribute_name='UserId',
                        data_type='uuid', attribute_required=True),
        DynamoAttribute(attribute_name='Name', data_type='str',
                        attribute_required=True),
        DynamoAttribute(attribute_name='Country', data_type='str',
                        attribute_required=True, attribute_default='India'),
        DynamoAttribute(attribute_name='Email', data_type='str', attribute_required=False,
                        attribute_validator=lambda x: '@' in x, attribute_default="admin+missing@test.com"),
        DynamoAttribute(attribute_name='Status',
                        data_type='str', attribute_required=True)
    ])

    # Register the schema with the table
    UserTable.register_schema(UserSchema)

    # Define a User model class
    class User(DynamoModel):
        def __init__(self, **kwargs):
            super().__init__()
            if "UserId" in kwargs:
                self.UserId = kwargs['UserId']
            if "Name" in kwargs:
                self.Name = kwargs['Name']
            if "Country" in kwargs:
                self.Country = kwargs['Country']
            if "Email" in kwargs:
                self.Email = kwargs['Email']
            if "Status" in kwargs:
                self.Status = kwargs['Status']

        def update_status(self, new_status):
            self.Status = new_status
            self.save()
    User.set_dynamo_table(UserTable)
    # Create and save a user
    user = User(Name='John', Country='India',
                Email='john@example.com', Status='Active').save()
    print(f"User created: {user.get_self_json()}")

    # Retrieve the user by primary key
    retrieved_user = User.get_by_primary_key(key={'UserId': user.UserId})
    print(f"Retrieved user: {retrieved_user.get_self_json()}")

    # Retrieve all users with a specific attribute value
    users_in_india = User.get_all_by_attribute(
        attribute='Country', value='India')
    print("Users in India:")
    for user in users_in_india:
        print(user.get_self_json())

    # Update the user
    user.Name = 'Updated John'
    user.save()
    print(f"User updated: {user.get_self_json()}")

    user.update_status(new_status='Inactive')
    # Additional feature: Conditional updates
    user.Status = 'Suspended'
    user.save_if(lambda obj: obj.Status == 'Active')
    print(f"User updated with condition: {user.get_self_json()}")

    # Delete the user
    user.delete()
    print("User deleted")

    # Check if the user exists
    if User.exists(key={'UserId': user.UserId}):
        print("User exists")
    else:
        print("User does not exist")
