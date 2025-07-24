import boto3
import os
import json
from decimal import Decimal

dynamo = boto3.resource('dynamodb').Table(os.environ['DDB_TABLE'])
s3 = boto3.client('s3')

def lambda_handler(event, context):
    file_id = event.get('pathParameters', {}).get('id')
    if not file_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "File ID is required"})
        }

    try:
        # Get metadata from DynamoDB
        resp = dynamo.get_item(Key={"file_id": file_id})
        if not resp or 'Item' not in resp:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "File not found"})
            }
        
        # Extract S3 key (adjust according to your DynamoDB schema)
        s3_key = resp['Item']['s3_key']
        
        # Generate a pre-signed URL for direct download (expires in 1 hour)
        bucket = os.environ['S3_BUCKET']
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=3600
        )
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "download_url": url,
                "filename": resp['Item'].get("filename", "file"),
                "size": int(resp['Item'].get("size", 0))
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
