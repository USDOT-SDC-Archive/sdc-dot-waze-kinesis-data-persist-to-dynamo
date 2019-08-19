import base64
import json
import boto3
import os
from unittest import mock
from lambdas import waze_persist_to_dynamo_handler_main


def test_lambda_handler():

    # assign __persist_to_dynamodb to a temp variable
    func1 = waze_persist_to_dynamo_handler_main.__persist_to_dynamodb
    try:
        event = {"Records": "records"}
        waze_persist_to_dynamo_handler_main.__persist_to_dynamodb = mock.MagicMock()
        waze_persist_to_dynamo_handler_main.lambda_handler(event, context=None)
        waze_persist_to_dynamo_handler_main.__persist_to_dynamodb.assert_called_with("records")

    finally:
        # reassign __persist_to_dynamodb
        waze_persist_to_dynamo_handler_main.__persist_to_dynamodb = func1


def test_decode_data():

    record = {
        "kinesis": {
            "data": json.dumps("decoded_string"),
            "sequenceNumber": 12345
        },
        "eventID": "ABC"
    }

    class Mockb64decode:

        def __init__(self, encoded_json):
            self.encoded_string = encoded_json

        def decode(self):
            return self.encoded_string

    base64.b64decode = Mockb64decode

    decoded_string = waze_persist_to_dynamo_handler_main.__decode_data(record)

    assert decoded_string == "decoded_string"


def test_persist_to_dynamodb_dataset_waze():

    func1 = waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table
    func2 = waze_persist_to_dynamo_handler_main.__get_latest_batch

    try:
        record1 = {
            "state": "Colorado",
            "month": "August",
            "num-records": 3,
            "is-historical": True,
            "table-name": "Table123",
            "year": 2019,
            "bucket-name": "bucket123",
            "s3-key": "waze/123"
        }

        records = [record1]

        def mock_decode_data(record):
            return record

        def mock_get_latest_batch():
            return "12345"

        waze_persist_to_dynamo_handler_main.__decode_data = mock_decode_data
        waze_persist_to_dynamo_handler_main.__get_latest_batch = mock_get_latest_batch
        waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table = mock.MagicMock()
        waze_persist_to_dynamo_handler_main.__persist_to_dynamodb(records)

        waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table.assert_called_with(
            record1['s3-key'], record1['table-name'], record1['state'], record1['num-records'], record1['bucket-name'],
            mock_get_latest_batch(), record1['is-historical'], record1['month'], record1['year']
        )

    finally:
        waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table = func1
        waze_persist_to_dynamo_handler_main.__get_latest_batch = func2


def test_persist_to_dynamodb_dataset_not_waze():

    func1 = waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table
    func2 = waze_persist_to_dynamo_handler_main.__get_latest_batch

    try:
        record1 = {
            "state": "Colorado",
            "month": "August",
            "num-records": 3,
            "is-historical": True,
            "table-name": "Table123",
            "year": 2019,
            "bucket-name": "bucket123",
            "s3-key": "not_waze/123"
        }

        records = [record1]

        def mock_decode_data(record):
            return record

        def mock_get_latest_batch():
            return "12345"

        waze_persist_to_dynamo_handler_main.__decode_data = mock_decode_data
        waze_persist_to_dynamo_handler_main.__get_latest_batch = mock_get_latest_batch
        waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table = mock.MagicMock()
        waze_persist_to_dynamo_handler_main.__persist_to_dynamodb(records)

        waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table.assert_not_called()

    finally:
        waze_persist_to_dynamo_handler_main.Utils.persist_record_to_dynamodb_table = func1
        waze_persist_to_dynamo_handler_main.__get_latest_batch = func2


def test_get_latest_batch():

    os.environ["LATEST_BATCH_ID"] = "latest_batch_id"
    response = {"Parameter": {"Value": "batch_id"}}

    class MockSsm:
        def get_parameter(self, *args, **kwargs):
            return response

    def mock_client(*args, **kwargs):
        return MockSsm()

    boto3.client = mock_client

    batch_id = waze_persist_to_dynamo_handler_main.__get_latest_batch()

    assert batch_id == response['Parameter']['Value']

