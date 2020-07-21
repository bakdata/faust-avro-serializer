import typing

from faust import Record

from faust_avro_serializer import FaustAvroSerializer


class DummyRecord(Record):
    item: typing.Any


def test_simple_record():
    result = {"__faust": {"ns": "tests.clean_payload_test.DummyRecord"}, "item": "test"}

    dummy = DummyRecord("test")
    assert result == FaustAvroSerializer.clean_payload(dummy)


def test_nested_record():
    result = {
        "__faust": {"ns": "tests.clean_payload_test.DummyRecord"},
        "item": {"__faust": {"ns": "tests.clean_payload_test.DummyRecord"}, "item": "test"},
    }

    dummy = DummyRecord(DummyRecord("test"))
    assert result == FaustAvroSerializer.clean_payload(dummy)


def test_list_of_records():
    result = {
        "__faust": {"ns": "tests.clean_payload_test.DummyRecord"},
        "item": [
            {"__faust": {"ns": "tests.clean_payload_test.DummyRecord"}, "item": "test"},
            {"__faust": {"ns": "tests.clean_payload_test.DummyRecord"}, "item": "test"},
        ],
    }

    dummy = DummyRecord([DummyRecord("test"), DummyRecord("test")])
    assert result == FaustAvroSerializer.clean_payload(dummy)


def test_map_of_records():
    result = {
        "__faust": {"ns": "tests.clean_payload_test.DummyRecord"},
        "item": {
            "key1": {
                "__faust": {"ns": "tests.clean_payload_test.DummyRecord"},
                "item": "test",
            },
            "key2": {
                "__faust": {"ns": "tests.clean_payload_test.DummyRecord"},
                "item": "test",
            },
        },
    }

    dummy = DummyRecord({"key1": DummyRecord("test"), "key2": DummyRecord("test")})
    assert result == FaustAvroSerializer.clean_payload(dummy)
