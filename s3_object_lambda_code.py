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
    as_list = json.loads(original_object)
    result_list = []

    # 4 - Transform object (the show starts here)
    for record in as_list:
        if record["department"] == "Computers":
            # record["password"] = _mask(record["password"])
            result_list.append(_detect_and_mask_pii_data(record))
            # result_list.append(record)

    # 5 - Write object back to S3 Object Lambda
    s3 = boto3.client("s3")
    s3.write_get_object_response(
        Body=json.dumps(result_list),
        RequestRoute=request_route,
        RequestToken=request_token,
    )

    return {"status_code": 200}


def _detect_and_mask_pii_data(record):
    # detect using comprehend
    comprehend = boto3.client("comprehend")
    response = comprehend.detect_pii_entities(
        Text=json.dumps(record),
        LanguageCode="en",
    )

    # mask record using masking function
    masked_password_record = _mask_password_in_record(record, response["Entities"])
    masked_age_record = _mask_age_in_record(
        masked_password_record, response["Entities"]
    )

    return masked_age_record


def _mask_password_in_record(record, entities):
    for entity in entities:
        if entity["Type"] == "PASSWORD" and entity["Score"] >= 0.8:
            start_index = entity["BeginOffset"]
            end_index = entity["EndOffset"]

            record_text = json.dumps(record)
            text_to_mask = record_text[start_index:end_index]

            masked_record_text = record_text.replace(
                text_to_mask, "*" * len(text_to_mask)
            )

    return json.loads(masked_record_text)


def _mask_age_in_record(record, entities):
    for entity in entities:
        if entity["Type"] == "AGE" and entity["Score"] >= 0.8:
            start_index = entity["BeginOffset"]
            end_index = entity["EndOffset"]

            record_text = json.dumps(record)
            text_to_mask = record_text[start_index:end_index]
            masked_record_text = record_text.replace(
                text_to_mask, "*" * len(text_to_mask)
            )

    return json.loads(masked_record_text)
