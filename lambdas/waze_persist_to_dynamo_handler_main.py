import base64
import json
import traceback

from common.utils import *


def lambda_handler(event):
    LoggerUtility.set_level()
    LoggerUtility.log_info("Event data from previous function is - {}".format(event))
    try:
        # Fetch records from event and decode records
        __persist_to_dynamodb(event["Records"])

    except Exception as data_handling_error:
        LoggerUtility.log_error("FATAL: Failed to process record- " + str(data_handling_error))
        LoggerUtility.log_error("FATAL: Stacktrace- " + str(traceback.format_exc()))


def __decode_data(record):
    LoggerUtility.log_info("KINESIS SEQUENCE NUMBER: " + str(record["kinesis"]["sequenceNumber"]) +
                           " || KINESIS EVENT ID: " + record["eventID"])
    encoded_string = record["kinesis"]["data"]
    return json.loads(base64.b64decode(encoded_string).decode())


def __persist_to_dynamodb(records):
    LoggerUtility.log_info("Records count {} ".format(len(records)))
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
        LoggerUtility.log_info("s3_key:" + s3_key)
        data_set = s3_key.split("/")[0]
        LoggerUtility.log_info("data_set:" + data_set)
        LoggerUtility.log_info("BATCH_ID:" + batch_id)
        if data_set == "waze":
            Utils.persist_record_to_dynamodb_table(s3_key, table_name, state, num_records, bucket, batch_id,
                                                   is_historical, month, year)
        else:
            LoggerUtility.log_info("Skipped the record because the data set is:" + data_set)
    LoggerUtility.log_info("Data persisted to dynamodb successfully from Kinesis!")


def __get_latest_batch():
    try:
        ssm = boto3.client('ssm')
        latest_batch_id = os.environ["LATEST_BATCH_ID"]
        response = ssm.get_parameter(Name=latest_batch_id, WithDecryption=False)
        LoggerUtility.log_info("Response from parameter store - {}".format(response))
        current_batch_id = response["Parameter"]["Value"]
    except Exception as ex:
        LoggerUtility.log_error("Unable to get latest batch with reason - {}".format(ex))
        raise ex
    return current_batch_id
