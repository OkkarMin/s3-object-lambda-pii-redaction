from ast import JoinedStr
from cgitb import text
import json
import re
import boto3
from pprint import pprint

comprehend = boto3.client("comprehend")

record = {
    "name": "Dr. Christopher O'Reilly",
    "age": "58",
    "address": "33975 Christiansen Roads",
    "department": "Computers",
    "salary": "178114",
    "username": "Eulah.Swift72",
    "password": "rNQvzNEuc0gkUCI",
}


def _detect_and_mask_pii_data(record):
    # detect using comprehend
    response = comprehend.detect_pii_entities(
        Text=json.dumps(record),
        LanguageCode="en",
    )

    # mask using masking function
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


pprint(_detect_and_mask_pii_data(record))
