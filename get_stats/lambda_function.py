import boto3
import os
import json
import logging
from decimal import Decimal

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} not set")
    return value

# Read environment variables
try:
    DDB_TABLE = get_env_var('DDB_TABLE')
    DDB_COUNTERS_TABLE = get_env_var('DDB_COUNTERS_TABLE', 'counters')
except RuntimeError as e:
    logger.error(str(e))
    raise

# Initialize DynamoDB resources
dynamodb = boto3.resource('dynamodb')
dynamo_metadata = dynamodb.Table(DDB_TABLE)
dynamo_counters = dynamodb.Table(DDB_COUNTERS_TABLE)

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

def get_unique_file_count() -> int:
    items = dynamo_metadata.scan(
        ProjectionExpression='#h',
        ExpressionAttributeNames={'#h': 'hash'}
    )['Items']
    return len({item['hash'] for item in items})

def get_deduplication_stats():
    resp = dynamo_counters.get_item(Key={'counter_id': 'deduplication_stats'})
    counters = resp.get('Item', {})
    counters = convert_decimals(counters)  # convert Decimals to int/float
    return (
        counters.get('duplicates_avoided', 0),
        counters.get('total_s3_size_saved', 0)
    )

def lambda_handler(event, context):
    try:
        total_files = get_unique_file_count()
        duplicates_avoided, total_s3_size_saved = get_deduplication_stats()

        response_body = json.dumps({
            "total_files": total_files,
            "duplicates_avoided": duplicates_avoided,
            "total_s3_size_saved": total_s3_size_saved
        })

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": response_body
        }

    except Exception as e:
        logger.error(f"Failed to get admin stats: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal server error"})
        }
