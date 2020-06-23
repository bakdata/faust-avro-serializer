# faust-avro-serializer

This repo contains an improved version of the avro serializer from https://github.com/marcosschroh/python-schema-registry-client/. The idea behind this improvement was to avoid the need to give an Avro schema at the moment the serializer is created. This version makes a better integration with the faust library and it uses its [metadata](https://faust.readthedocs.io/en/latest/userguide/models.html#polymorphic-fields) capability inside the ``Record`` class to read the Avro schema dynamically.

### Example

```python
from faust_avro_serializer import FaustAvroSerializer
from schema_registry.client import SchemaRegistryClient

class MyRecordExample:
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
serializer = FaustAvroSerializer(client,"my-subject")
``` 

Now when the serializer calls its ``_dumps`` method, it will search for the ``__faust`` field inside the object it gets. If the serializer finds the field, it is going to search for the class and reads the ``_schema`` field containing the Avro schema.
 