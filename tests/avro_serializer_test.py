from unittest.mock import MagicMock

import pytest
from faust import Record
from schema_registry.client import SchemaRegistryClient
from schema_registry.client.schema import AvroSchema

from faust_avro_serializer import FaustAvroSerializer, MissingSchemaException


class UserModel(Record):
    _schema = {
        "type": "record",
        "namespace": "com.example",
        "name": "FullName",
        "fields": [
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"}
        ]
    }
    first_name: str
    last_name: str


class BadSchemaModel(Record):
    _schema = {
        "type": "record",
        "namespace": "com.example",
        "name": "FullName",
        "fields": [
            {"name": "first_name"},
            {"name": "last_name", "type": "string"}
        ]
    }
    first_name: str
    last_name: str


class DummyModel(Record):
    foo: str
    bar: str


@pytest.fixture()
def client():
    client = SchemaRegistryClient("http://my-dummy-schema.com")
    client.register = MagicMock(name="register")
    client.get_by_id = MagicMock(name="get_by_id")
    client.register.return_value = 1
    client.get_by_id.return_value = AvroSchema(UserModel._schema)
    return client


def test_dumps_load_message(client):
    topic = "dummy-topic"
    faust_serializer = FaustAvroSerializer(client, topic)
    dummy_user = UserModel(first_name="foo", last_name="bar")
    dict_repr = dummy_user.to_representation()

    message_encoded = faust_serializer.dumps(dict_repr)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer.loads(message_encoded)

    assert message_decoded["first_name"] == dummy_user.first_name
    assert message_decoded["last_name"] == dummy_user.last_name


def test_missing_schema_test(client):
    topic = "dummy-topic"
    faust_serializer = FaustAvroSerializer(client, topic)
    dummy_model = DummyModel(foo="foo", bar="bar")
    with pytest.raises(MissingSchemaException) as e:
        faust_serializer.dumps(dummy_model.to_representation())
    assert str(
        e.value) == "Record does not have schema defined in '_schema' field", "_schema class variable should have schema"


def test_missing_metadata(client):
    topic = "dummy-topic"
    faust_serializer = FaustAvroSerializer(client, topic)
    dummy_model = DummyModel(foo="foo", bar="bar")
    with pytest.raises(ValueError) as e:
        dummy_dict = dummy_model.to_representation()
        del dummy_dict["__faust"]
        faust_serializer.dumps(dummy_dict)
    assert str(
        e.value) == "Record does not have namespace metadata", "Namespace for serialization should be there"


def test_class_registry_not_exist(client):
    topic = "dummy-topic"
    faust_serializer = FaustAvroSerializer(client, topic)
    dummy_dict = {
        "first_name": "foo",
        "last_name": "bar",
        "__faust": {
            "ns": "DummyNoExistClass"
        }
    }
    with pytest.raises(ValueError) as e:
        faust_serializer.dumps(dummy_dict)
    assert str(
        e.value) == "The class you are trying to deserialize doesn't exist inside the faust registry", "Faust registry should contain class"


def test_valid_schema(client):
    topic = "dummy-topic"
    faust_serializer = FaustAvroSerializer(client, topic)
    bad_schema_model = BadSchemaModel(first_name="foo", last_name="bar")
    with pytest.raises(Exception):
        faust_serializer.dumps(bad_schema_model.to_representation())
