import typing
from collections.abc import Mapping, Sequence

import faust
from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers import MessageSerializer
from faust.models.base import registry
from schema_registry.client.schema import AvroSchema


class MissingSchemaException(Exception):
    pass


class FaustAvroSerializer(MessageSerializer, faust.Codec):
    _mapping_key = {
        True: "key",
        False: "value"
    }

    def __init__(self, client: SchemaRegistryClient, subject: str, is_key=False, **kwargs):
        self.schema_registry_client = client
        self.schema_subject = subject
        self.is_key = is_key

        MessageSerializer.__init__(self, client)
        faust.Codec.__init__(self, client=client, subject=subject, is_key=is_key, **kwargs)

    def _loads(self, s: bytes) -> typing.Optional[typing.Dict]:
        # method available on MessageSerializer
        return self.decode_message(s)

    @staticmethod
    def _clean_item(item: typing.Any) -> typing.Any:
        if isinstance(item, faust.Record):
            return FaustAvroSerializer._clean_item(item.to_representation())
        elif isinstance(item, str):
            # str is also a sequence, need to make sure we don't iterate over it.
            return item
        elif isinstance(item, Mapping):
            return type(item)({key: FaustAvroSerializer._clean_item(value) for key, value in item.items()})  # type: ignore
        elif isinstance(item, Sequence):
            return type(item)(FaustAvroSerializer._clean_item(value) for value in item)  # type: ignore
        return item

    @staticmethod
    def clean_payload(payload: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        """
        Try to clean payload retrieved by faust.Record.to_representation.
        All values inside payload should be native types and not faust.Record
        On Faust versions <=1.9.0 Record.to_representation always returns a dict with native types
        as a value which are compatible with fastavro.
        On Faust 1.10.0 <= versions Record.to_representation always returns a dict but values
        can also be faust.Record, so fastavro is incapable to serialize them
        Args:
            payload (dict): Payload to clean
        Returns:
            dict that represents the clean payload
        """
        return FaustAvroSerializer._clean_item(payload)

    def _dumps(self, obj: typing.Dict[str, typing.Any]) -> bytes:
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.

        The schema is registered with the subject of 'topic-value'
        """
        if "__faust" not in obj or "ns" not in obj["__faust"]:
            raise ValueError("Record does not have namespace metadata")

        model_class_name = obj["__faust"]["ns"]

        if model_class_name not in registry:
            raise ValueError("The class you are trying to deserialize doesn't exist inside the faust registry")

        class_reference = registry[model_class_name]

        if not hasattr(class_reference, "_schema"):
            raise MissingSchemaException("Record does not have schema defined in '_schema' field")

        schema_def = getattr(class_reference, "_schema")

        avro_schema = AvroSchema(schema_def)

        obj = self.clean_payload(obj)
        # method available on MessageSerializer
        return self.encode_record_with_schema(
            f"{self.schema_subject}-{self._mapping_key[self.is_key]}", avro_schema, obj
        )
