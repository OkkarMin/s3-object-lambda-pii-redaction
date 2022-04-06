import json
import boto3
from pprint import pprint


comprehend = boto3.client("comprehend")

with open("output.json", "r") as json_file:
    record_list = json.load(json_file)


def _detect_and_mask_pii_data(record):
    # detect using comprehend
    response = comprehend.detect_pii_entities(
        Text=json.dumps(record),
        LanguageCode="en",
    )

    # get data PII removed
    record_pii_removed = _remove_all_except_name_department_email(
        record,
        response["Entities"],
    )

    return record_pii_removed


def _remove_all_except_name_department_email(record, entities):
    record_copy = record.copy()
    record_text = json.dumps(record)
    values_to_remove = []

    for entity in entities:
        if entity["Type"] not in ["NAME", "DEPARTMENT", "EMAIL"]:
            start_index = entity["BeginOffset"]
            end_index = entity["EndOffset"]

            value_to_remove = record_text[start_index:end_index]
            values_to_remove.append(value_to_remove)

    for value_to_remove in values_to_remove:
        key = _get_key_given_value(record, value_to_remove)
        try:
            record_copy.pop(key)
        except KeyError:
            pass

    return record_copy


def _get_key_given_value(record, value_to_check):
    for key, val in record.items():
        if value_to_check in val:
            return key


for record in record_list:
    pprint(_detect_and_mask_pii_data(record))

s3 = boto3.client("s3")

original_data = s3.get_object(Bucket="original-bucket", Key="original.json")
original_json = original_data["Body"].read().decode("utf-8")


redacted_data = s3.get_object(
    Bucket="arn:aws:s3-object-lambda:REGION:ACCOUNT:accesspoint/OLAPNAME",
    Key="redacted.json",
)
redacted_csv = redacted_data["Body"].read().decode("utf-8")
