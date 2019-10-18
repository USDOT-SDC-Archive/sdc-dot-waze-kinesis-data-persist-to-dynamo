import os
import uuid
import boto3
from common.logger_utility import LoggerUtility


class Utils:
    pass

    @staticmethod
    def persist_record_to_dynamodb_table(s3_key, table_name, state, num_records, bucket, batch_id, is_historical, month,
                                         year):
        try:
            dynamodb_curated_records_table_name = os.environ['DDB_CURATED_RECORDS_TABLE_ARN'].split('/')[1]
            persist_records = bool(int(os.environ['PERSIST_RECORDS']))
            s3_key = "s3://" + bucket + "/" + s3_key

            if persist_records:
                dynamodb = boto3.resource('dynamodb')
                curated_record_table = dynamodb.Table(dynamodb_curated_records_table_name)
                response = curated_record_table.put_item(
                    Item={
                        'CurationRecordId': str(uuid.uuid4()),
                        'BatchId': batch_id,
                        'DataTableName': table_name,
                        'S3Key': s3_key,
                        'State': state,
                        'TotalNumCuratedRecords': num_records,
                        'IsHistorical': is_historical,
                        'Year': year,
                        'Month': month
                    }
                )
                LoggerUtility.log_info("Successfully persisted record to dynamo db table - {}".format(response))
            else:
                LoggerUtility.log_info("Persist records flag is disabled, so not persisting "
                                       "any records to dynamodb table")

        except Exception as e:
            LoggerUtility.log_error("Failed to persist record to dynamo db table for key - {}".format(s3_key))
            raise e

    @staticmethod
    def get_parsed_string(data):
        try:
            if data != "" and data != "null" and data != "NULL" and data is not None:
                data = data.replace(r"'", r"\'")
                data = data.replace("\n", "")
                data = data.replace("\r", "")

        except Exception as e:
            raise e
        return data
