import boto3
import os
import json
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.resource('dynamodb').Table(os.environ['DDB_TABLE'])

def replace_decimals(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    elif isinstance(obj, dict):
        return {k: replace_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_decimals(x) for x in obj]
    return obj

def lambda_handler(event, context):
    try:
        file_id = event.get('pathParameters', {}).get('id')
        if file_id is None:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "File ID is required in the path"})
            }

        resp = dynamo.get_item(Key={'file_id': file_id})

        if 'Item' in resp:
            clean_item = replace_decimals(resp['Item'])
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(clean_item)
            }
        else:
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "File not found"})
            }
    except Exception as e:
        logger.error(f"Error retrieving file metadata: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal server error"})
        }
