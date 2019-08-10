import os
import json
import traceback
import base64
import boto3
from common.logger_utility import *
from common.utils import *


def lambda_handler(event, context):
    LoggerUtility.setLevel()
    LoggerUtility.logInfo("Event data from previous function is - {}".format(event))
    try:
        # Fetch records from event and decode records
        __persist_to_dynamodb(event["Records"])
        
    except Exception as data_handling_error:
        LoggerUtility.logError("FATAL: Failed to process record- " + str(data_handling_error))
        LoggerUtility.logError("FATAL: Stacktrace- " + str(traceback.format_exc()))


def __decode_data(record):
    LoggerUtility.logInfo("KINESIS SEQUENCE NUMBER: " + str(record["kinesis"]["sequenceNumber"]) +
                          " || KINESIS EVENT ID: " + record["eventID"])
    encoded_string = record["kinesis"]["data"]
    return json.loads(base64.b64decode(encoded_string).decode())


def __persist_to_dynamodb(records):
    LoggerUtility.logInfo("Records count {} ".format(len(records)))
    """Decode the base64 data."""
    for index, record in enumerate(records):
        decoded_json = __decode_data(record)
        state = decoded_json.get("state")
        month = decoded_json.get("month")
        num_records = decoded_json.get("num-records")
        is_historical = decoded_json.get("is-historical")
        table_name = decoded_json.get("table-name")
        year = decoded_json.get("year")
        bucket = decoded_json.get("bucket-name")
        s3_key = decoded_json.get("s3-key")
        batch_id = __get_latest_batch()
        LoggerUtility.logInfo("s3_key:"+s3_key)
        data_set = s3_key.split("/")[0]
        LoggerUtility.logInfo("data_set:"+data_set)
        LoggerUtility.logInfo("BATCH_ID:"+batch_id)
        if data_set == "waze":
            Utils.persist_record_to_dynamodb_table(s3_key, table_name, state, num_records, bucket, batch_id,
                                                   is_historical, month, year)
        else:
            LoggerUtility.logInfo("Skipped the record because the data set is:"+data_set)
    LoggerUtility.logInfo("Data persisted to dynamodb successfully from Kinesis!")


def __get_latest_batch():
    try:
        ssm = boto3.client('ssm')
        latest_batch_id = os.environ["LATEST_BATCH_ID"]
        response = ssm.get_parameter(Name=latest_batch_id, WithDecryption=False)
        LoggerUtility.logInfo("Response from parameter store - {}".format(response))
        current_batch_id = response["Parameter"]["Value"]
    except Exception as ex:
        LoggerUtility.logError("Unable to get latest batch with reason - {}".format(ex))
        raise ex
    return current_batch_id
