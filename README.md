# faust-avro-serializer

This repo contains an improved version of the avro serializer from
https://github.com/marcosschroh/python-schema-registry-client/. It expects the schema
to be stored in the record itself in order to mimic the behavior of Confluent's Avro SerDe.
It uses Faust's [metadata](https://faust.readthedocs.io/en/latest/userguide/models.html#polymorphic-fields) capability inside the ``Record`` class to read the Avro schema 
dynamically.
### Example

```python
from faust import Record, Schema, Stream
from faust_avro_serializer import FaustAvroSerializer
from schema_registry.client import SchemaRegistryClient
import faust

app = faust.App('myapp', broker='kafka://localhost')
my_topic_name = "my-dummy-topic"

class MyRecordExample(Record):
    _schema = {
     "type": "record",
     "namespace": "com.example",
     "name": "MyRecordExample",
     "fields": [
       { "name": "foo", "type": "string" },
       { "name": "bar", "type": "string" }
     ]
} 
    foo: str
    bar: str

client = SchemaRegistryClient("http://my-schema-registry:8081")
serializer = FaustAvroSerializer(client, my_topic_name, False)

schema_with_avro = Schema(key_serializer=str, value_serializer=serializer)

dummy_topic = app.topic(my_topic_name, schema=schema_with_avro)

@app.agents(dummy_topic)
async def my_agent(myrecord: Stream[MyRecordExample]):
    async for record in myrecord:
        print(record.to_representation())

``` 

When the serializer calls the ``_dumps`` method, it searches for the ``__faust`` field inside the
record. 
If the serializer finds the field, it is resolving the class and reads the ``_schema`` field 
containing the Avro schema.

