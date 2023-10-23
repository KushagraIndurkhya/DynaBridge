import threading
import concurrent.futures
from DynaBridge.table import DynamoTable
from DynaBridge.models import DynamoModel
from DynaBridge.exceptions import *
from DynaBridge.attribute import *
from DynaBridge.schema import *
from DynaBridge.utills import *
import boto3


class CounterTest(DynamoModel):
    def __init__(self, **kwargs):
        super().__init__()
        if "CounterId" in kwargs:
            self.CounterId = kwargs['CounterId']
        if "CounterName" in kwargs:
            self.CounterName = kwargs['CounterName']
        if "Counter" in kwargs:
            self.Counter = kwargs.get('Counter')

    def _increment_counter(self, counter_inc=1):
        self.Counter = str(int(self.Counter) + counter_inc)

    @DynamoModel.transactional_lock
    def increment_counter(self, counter_inc=1):
        try:
            key = self.CounterId
            counter = CounterTest.get_by_primary_key(
                key={'CounterId': self.CounterId})
            counter._increment_counter(counter_inc)
            counter.save()
            return self
        except Exception as e:
            raise e


CounterTestTable = DynamoTable(table_name='CounterTest', db_resource=boto3.resource(
    'dynamodb'), primary_key='CounterId', is_transactional=True)
CounterTestSchema = DynamoSchema().add_attributes([
    DynamoAttribute(attribute_name='CounterId',
                    data_type='uuid', attribute_required=True),
    DynamoAttribute(attribute_name='Counter', data_type='str',
                    attribute_required=True, attribute_validator=lambda x: int(x) >= 0, attribute_default="0")])
CounterTestTable.register_schema(CounterTestSchema)
CounterTest.set_dynamo_table(CounterTestTable)
new_counter_key = CounterTest().create().fetch_key()
counter_sample = CounterTest().get_by_primary_key(new_counter_key)
counter_sample.increment_counter()

num_threads = 100
num_repeats = 1


# def simulate_concurrent_updates():
#     for _ in range(num_repeats):  # Simulate 100 updates
#         counter_sample.increment_counter()

def simulate_concurrent_updates(counter_sample, num_repeats):
    for _ in range(num_repeats):
        print("Incrementing counter")
        counter_sample.increment_counter()
        

def main():
    num_threads = 10
    num_repeats = 1  # Simulate 100 updates per thread

    new_counter_key = CounterTest().create().fetch_key()
    counter_sample = CounterTest().get_by_primary_key(new_counter_key)

    expected_count = num_threads * num_repeats

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(simulate_concurrent_updates, counter_sample, num_repeats) for _ in range(num_threads)]

        # Wait for all threads to complete
        concurrent.futures.wait(futures)

    updated_counter_obj = CounterTest().get_by_primary_key(new_counter_key).get_self_json()
    actual_count = int(updated_counter_obj['Counter'])
    success_percentage = (actual_count / expected_count) * 100

    print("Benchmarking Results:")
    print(f"Expected Count: {expected_count}")
    print(f"Actual Count: {actual_count}")
    print(f"Success Percentage: {success_percentage:.2f}%")

if __name__ == "__main__":
    main()