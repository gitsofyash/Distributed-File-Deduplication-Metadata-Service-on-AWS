import boto3
import hashlib
import uuid
import base64
import os
import json
import logging
import decimal
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} not set")
    return value

# Read environment variables
try:
    DDB_TABLE = get_env_var('DDB_TABLE')
    S3_BUCKET = get_env_var('S3_BUCKET')
    DDB_COUNTERS_TABLE = get_env_var('DDB_COUNTERS_TABLE')  # with default if not set
except RuntimeError as e:
    logger.error(str(e))
    raise

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
dynamo = dynamodb.Table(DDB_TABLE)
dynamo_counters = dynamodb.Table(DDB_COUNTERS_TABLE)  # NEW

def lambda_handler(event, context):
    try:
        # Validate input event structure
        if not isinstance(event, dict):
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Invalid event structure'})
            }
        
        # Validate body and headers
        if 'body' not in event:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'No file content provided'})
            }
        if 'headers' not in event or not isinstance(event['headers'], dict):
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Missing or invalid headers'})
            }

        # Decode file content (handles base64 encoded or raw)
        body = event['body']
        is_base64_encoded = event.get('isBase64Encoded', False)
        try:
            if is_base64_encoded:
                file_bytes = base64.b64decode(body)
            else:
                file_bytes = body.encode('utf-8')
        except Exception as e:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Failed to decode file content', 'details': str(e)})
            }

        # Get filename, mime type, and file size
        filename = event['headers'].get('filename') or f"{uuid.uuid4()}.bin"
        content_type = event['headers'].get('content-type', 'application/octet-stream')
        file_size = len(file_bytes)

        # Calculate SHA256 hash
        file_hash = hashlib.sha256(file_bytes).hexdigest()
        logger.info(f"File hash: {file_hash}, filename: {filename}, size: {file_size} bytes")

        # Check for duplicate file (prefer direct get_item, fallback to scan)
        existing_item = None
        try:
            existing_item = dynamo.get_item(Key={'hash': file_hash}).get('Item')
        except Exception:
            logger.warning("Could not get_item by hash (maybe hash is not partition key)? Falling back to scan")
            existing_items = dynamo.scan(
                FilterExpression='#h = :val',
                ExpressionAttributeNames={'#h': 'hash'},
                ExpressionAttributeValues={':val': file_hash}
            )['Items']
            existing_item = existing_items[0] if existing_items else None

        # If duplicate detected, increment counters and return conflict
        if existing_item:
            logger.info("Duplicate file found")
            # Increment counters (create if not exists, atomic update)
            try:
                dynamo_counters.update_item(
                    Key={'counter_id': 'deduplication_stats'},
                    UpdateExpression='SET duplicates_avoided = if_not_exists(duplicates_avoided, :zero) + :inc, total_s3_size_saved = if_not_exists(total_s3_size_saved, :zero) + :file_size',
                    ExpressionAttributeValues={
                        ':inc': 1,
                        ':zero': 0,
                        ':file_size': file_size  # size of the duplicate upload
                    }
                )
            except Exception as e:
                logger.error(f"Failed to update deduplication counters: {str(e)}")
            
            # Return 409 Conflict with existing item metadata
            return {
                'statusCode': 409,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps(existing_item, cls=DecimalEncoder)
            }

        # Upload unique file to S3
        s3_key = f"files/{file_hash}/{filename}"
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=file_bytes,
                ContentType=content_type,
                ServerSideEncryption='aws:kms'
            )
            logger.info(f"Uploaded {filename} to S3: {s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': f'Failed to upload to S3: {str(e)}'})
            }

        # Save new file metadata to DynamoDB
        file_id = str(uuid.uuid4())
        created_at = datetime.utcnow().isoformat()
        s3_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}"
        metadata = {
            'file_id': file_id,
            'name': filename,
            'size': file_size,
            'type': content_type,
            'hash': file_hash,
            'created_at': created_at,
            's3_key': s3_key,
            'download_url': s3_url
        }
        try:
            dynamo.put_item(Item=metadata)
            logger.info("Saved metadata to DynamoDB")
        except Exception as e:
            logger.error(f"Failed to save metadata: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': f'Failed to save metadata: {str(e)}'})
            }

        # Success: return new file metadata
        return {
            'statusCode': 201,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(metadata)
        }

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error', 'details': str(e)})
        }
