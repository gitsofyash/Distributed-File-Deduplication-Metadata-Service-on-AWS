# Distributed File Deduplication & Metadata Service

A serverless backend for uploading files, detecting duplicates, storing unique files, and retrieving metadata and downloads using AWS Lambda, API Gateway, S3, and DynamoDB.

## Table of Contents

- [Architecture](#architecture)
- [Setup](#setup)
- [API Endpoints](#api-endpoints)
- [Admin APIs](#admin-apis)
- [Testing](#testing)
- [Future Enhancements](#future-enhancements)

---

## Architecture

**Overview:**  
This system focuses on deduplication: Only unique files are stored in S3; duplicate uploads are recognized by SHA-256 hash and only their metadata is tracked.

**Component Diagram**:
![Architecture Diagram](/images/aws-deduplication-architecture.jpg)


**How it Works:**
- **Client** calls `/upload` via API Gateway → **Upload Lambda**.
- **Upload Lambda** computes file hash, checks **DynamoDB** for existing hash.
  - **If new**: File is stored in **S3** and metadata in **DynamoDB**.
  - **If duplicate**: Only metadata is returned (409 Conflict), counter updated.
- **Get Metadata/Download** calls fetch file info from **DynamoDB**.
- **Download** provides **S3 pre-signed URL** for direct download.
- **Admin endpoints** aggregate system stats and list all hashes.

---

## Setup

### AWS Resources to Create

- **S3 Bucket**: Stores unique files.
- **DynamoDB Table (`file-metadata`)**: Stores file metadata with `hash` as primary key.
- **DynamoDB Table (`counters`)**: Stores global deduplication counters (`duplicates_avoided`, `total_s3_size_saved`).
  - Initialize the counters table with:
    ```
    { "counter_id": "deduplication_stats", "duplicates_avoided": 0, "total_s3_size_saved": 0 }
    ```

### Lambda Functions

Create a Lambda for each API endpoint.  
**Required Environment Variables** (set for each Lambda):

| Variable           |  Value                |
|--------------------|-----------------------------|
| `DDB_TABLE`        | FileMetadata              |
| `DDB_COUNTERS_TABLE`| counters                    |
| `S3_BUCKET`        | dfd-file-service        |

### API Gateway

- **Create an HTTP API** in AWS API Gateway.
- **Add routes**:
  - `POST /upload`
  - `GET /file/{id}`
  - `GET /download/{id}`
  - `GET /admin/stats`
  - `GET /admin/hashes`
- **Integrate each route** with the correct Lambda.
- **Enable CORS** for all routes.
- **Deploy** your API.

### IAM Roles

- **Lambda execution roles** must have access to DynamoDB (`dynamodb:GetItem`, `PutItem`, `UpdateItem`, `Scan`) and S3 (`s3:PutObject`, `s3:GetObject`).
- **No public access** is required for S3.

---

## API Endpoints

| Endpoint           | Method | Description                                    |
|--------------------|--------|------------------------------------------------|
| `/upload`          | POST   | Upload a file. If duplicate, returns 409.      |
| `/file/{id}`       | GET    | Get metadata for a file.                       |
| `/download/{id}`   | GET    | Download a file (S3 pre-signed URL).           |
| `/admin/stats`     | GET    | Get system stats (files, duplicates, savings).  |
| `/admin/hashes`    | GET    | List all file hashes and metadata (paginated).  |

---

## Admin APIs

- **`GET /admin/stats`**: Returns `total_files`, `duplicates_avoided`, `total_s3_size_saved`.
- **`GET /admin/hashes`**: Returns array of file hashes and metadata (paginated via `limit` and `last_evaluated_key`).

---

## Testing

- **Postman/curl**: Test each endpoint directly.
- **CloudWatch Logs**: Monitor Lambda execution for errors.

---


## Future Enhancements

- **Async processing** with SQS or Step Functions for background tasks.
- **File encryption** using AWS KMS.
- **Authentication** with Cognito/OAuth.
- **Rate limiting** on API Gateway.
- **Caching** for frequent hash lookups.

---

**This README provides a comprehensive, production-ready architectural overview and setup guide for your distributed file deduplication service on AWS, focusing on Lambda, API Gateway, S3, and DynamoDB.**
```

