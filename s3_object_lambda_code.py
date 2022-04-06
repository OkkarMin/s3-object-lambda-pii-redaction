import csv
import boto3
import urllib3
import json

http = urllib3.PoolManager()


def lambda_handler(event, context):
    print(event)

    # 1 & 2 Extract relevant metadata including S3URL out of input event
    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    s3_url = object_get_context["inputS3Url"]

    # 3 - Download S3 File
    response = http.request("GET", s3_url)

    original_object = response.data.decode("utf-8")
    as_dict = json.loads(original_object)
    result_list = []

    # 4 - Transform object (the show starts here)
    for record in as_dict:
        if record["department"] == "Computers":
            ## removing PII data from record
            # record.pop("age")
            # record.pop("address")
            # record.pop("username")
            # record.pop("phone")
            # result_list.append(record)

            ## removing PII data from record using Amazon Comprehend
            result_list.append(_detect_and_remove_pii_data(record))

    keys = result_list[0].keys()
    with open("/tmp/result.csv", mode="w", newline="") as result_file:
        dict_writer = csv.DictWriter(result_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)

    # 5 - Write object back to S3 Object Lambda
    s3 = boto3.client("s3")
    s3.write_get_object_response(
        Body=open("/tmp/result.csv", "rb"),
        RequestRoute=request_route,
        RequestToken=request_token,
    )

    return {"status_code": 200}


def _detect_and_remove_pii_data(record):
    # detect using comprehend
    comprehend = boto3.client("comprehend")
    response = comprehend.detect_pii_entities(
        Text=json.dumps(record),
        LanguageCode="en",
    )

    # remove PII data from record
    record_pii_removed = _remove_all_except_name_department_email(
        record, response["Entities"]
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
