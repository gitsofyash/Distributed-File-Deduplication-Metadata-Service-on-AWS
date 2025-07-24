import boto3
import os
import json
import logging
from decimal import Decimal
from urllib.parse import quote_plus, unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} not set")
    return value

try:
    DDB_TABLE = get_env_var('DDB_TABLE')
except RuntimeError as e:
    logger.error(str(e))
    raise

dynamo = boto3.resource('dynamodb').Table(DDB_TABLE)

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    else:
        return obj

def lambda_handler(event, context):
    try:
        params = event.get('queryStringParameters', {}) or {}
        limit = int(params.get('limit', 10))
        last_evaluated_key = (
            json.loads(unquote_plus(params['last_evaluated_key']))
            if 'last_evaluated_key' in params else None
        )

        # Field mapping
        alias_map = {
            '#h': 'hash',
            '#fi': 'file_id',
            '#n': 'name',
            '#s': 'size',
            '#t': 'type',
            '#ca': 'created_at',
            '#sk': 's3_key',
            '#du': 'download_url'
        }
        projection_expr = ','.join(alias_map.keys())

        scan_kwargs = {
            'Limit': limit,
            'ProjectionExpression': projection_expr,
            'ExpressionAttributeNames': alias_map
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = dynamo.scan(**scan_kwargs)
        items = response.get('Items', [])

        # No alias key exists in result. So use original field names
        renamed_items = []
        for item in items:
            new_item = {}
            for attr in alias_map.values():
                if attr in item:
                    new_item[attr] = item[attr]
            renamed_items.append(new_item)

        result = {
            'hashes': convert_decimals(renamed_items)
        }

        if 'LastEvaluatedKey' in response:
            result['next_page'] = f"?limit={limit}&last_evaluated_key={quote_plus(json.dumps(response['LastEvaluatedKey']))}"

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(result, indent=2)
        }

    except Exception as e:
        logger.error(f"Failed to list file hashes: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }
