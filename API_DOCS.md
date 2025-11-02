# AWS HealthImaging API Documentation

RESTful API for managing DICOM imports and viewing WSI (Whole Slide Imaging) images stored in AWS HealthImaging.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your AWS credentials and settings

# Start API server
python api.py
```

The API will be available at `http://localhost:8080`

## Environment Variables

Required:
- `AWS_REGION` - AWS region (default: us-east-1)
- `AWS_HEALTHIMAGING_DATASTORE_ID` - Default datastore ID
- `AWS_HEALTHIMAGING_ROLE_ARN` - IAM role ARN for HealthImaging
- `AWS_S3_OUTPUT_BUCKET` - S3 bucket for import job outputs/logs

Optional:
- `PORT` - API server port (default: 8080)
- `AWS_S3_SOURCE_BUCKET` - Default source bucket for imports

## API Endpoints

### 1. Import DICOM from S3

Import DICOM files from an S3 bucket into AWS HealthImaging.

**Endpoint:** `POST /api/import`

**Request Body:**
```json
{
  "source_bucket": "my-dicom-bucket",
  "source_prefix": "dicom-case13/",
  "output_bucket": "optional-output-bucket",
  "datastore_id": "optional-datastore-id",
  "region": "us-east-1"
}
```

**Parameters:**
- `source_bucket` (required) - S3 bucket containing DICOM files
- `source_prefix` (optional) - Folder path within source bucket
- `output_bucket` (optional) - S3 bucket for import job logs (defaults to env var)
- `datastore_id` (optional) - HealthImaging datastore ID (defaults to env var)
- `region` (optional) - AWS region (defaults to env var)

**Response:**
```json
{
  "job_id": "abc123...",
  "status": "SUBMITTED",
  "datastore_id": "12e041ae...",
  "input_uri": "s3://my-dicom-bucket/dicom-case13/",
  "output_uri": "s3://output-bucket/healthimaging-output/...",
  "studies": [
    {
      "study_uid": "1.2.276.0.7230010...",
      "series": [
        {
          "series_uid": "1.2.276.0.7230010...",
          "series_number": 0,
          "file_count": 500,
          "files": ["s3://..."]
        }
      ]
    }
  ]
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/api/import \
  -H "Content-Type: application/json" \
  -d '{
    "source_bucket": "source-healthimaging-test",
    "source_prefix": "dicom-case13/"
  }'
```

---

### 2. Get Import Job Status

Check the status of an import job.

**Endpoint:** `GET /api/import/{job_id}`

**Query Parameters:**
- `datastore_id` (optional) - Datastore ID

**Response:**
```json
{
  "job_id": "abc123...",
  "status": "COMPLETED",
  "submitted_at": "2025-10-29T22:36:27Z",
  "ended_at": "2025-10-29T22:37:15Z",
  "message": "",
  "input_uri": "s3://...",
  "output_uri": "s3://..."
}
```

**Status values:**
- `SUBMITTED` - Job queued
- `IN_PROGRESS` - Processing
- `COMPLETED` - Successfully imported
- `FAILED` - Import failed

**Example:**
```bash
curl "http://localhost:8080/api/import/abc123...?datastore_id=12e041ae..."
```

---

### 3. List Studies

List all studies in a datastore.

**Endpoint:** `GET /api/studies`

**Query Parameters:**
- `datastore_id` (optional) - Datastore ID
- `region` (optional) - AWS region

**Response:**
```json
{
  "datastore_id": "12e041ae...",
  "study_count": 1,
  "studies": [
    {
      "study_uid": "1.2.276.0.7230010...",
      "patient_id": "4f3a7f54...",
      "study_date": "20191229",
      "series_count": 7,
      "series": [
        {
          "image_set_id": "64971beea7bfd588...",
          "series_number": 0,
          "series_uid": "1.2.276.0.7230010..."
        }
      ]
    }
  ]
}
```

**Example:**
```bash
curl "http://localhost:8080/api/studies?datastore_id=12e041ae..."
```

---

### 4. View WSI by StudyUID

Display an interactive viewer for a WSI study.

**Endpoint:** `GET /api/viewer/{study_uid}`

**Query Parameters:**
- `datastore_id` (optional) - Datastore ID

**Response:** HTML page with OpenSeadragon viewer

**Example:**
```bash
# Open in browser
open "http://localhost:8080/api/viewer/1.2.276.0.7230010...?datastore_id=12e041ae..."
```

---

### 5. Get DZI Descriptor

Get Deep Zoom Image (DZI) descriptor for a study.

**Endpoint:** `GET /api/studies/{study_uid}/dzi`

**Query Parameters:**
- `datastore_id` (optional) - Datastore ID

**Response:**
```json
{
  "Image": {
    "xmlns": "http://schemas.microsoft.com/deepzoom/2008",
    "Format": "jpg",
    "Overlap": 0,
    "TileSize": 500,
    "Size": {
      "Width": 136111,
      "Height": 57850
    }
  }
}
```

---

### 6. Get Tile

Get a specific tile image for a study.

**Endpoint:** `GET /api/studies/{study_uid}/tiles/{level}/{col}_{row}.jpg`

**Path Parameters:**
- `study_uid` - Study Instance UID
- `level` - Pyramid level (0 = lowest resolution)
- `col` - Column index
- `row` - Row index

**Query Parameters:**
- `datastore_id` (optional) - Datastore ID

**Response:** JPEG image

**Example:**
```bash
curl "http://localhost:8080/api/studies/1.2.276.../tiles/0/0_0.jpg" -o tile.jpg
```

---

### 7. Delete Study

Delete all image sets for a study.

**Endpoint:** `DELETE /api/studies/{study_uid}`

**Query Parameters:**
- `datastore_id` (optional) - Datastore ID

**Response:**
```json
{
  "study_uid": "1.2.276.0.7230010...",
  "deleted_image_sets": 7,
  "total_series": 7
}
```

**Example:**
```bash
curl -X DELETE "http://localhost:8080/api/studies/1.2.276.0.7230010...?datastore_id=12e041ae..."
```

---

### 8. List Datastores

List all AWS HealthImaging datastores in a region.

**Endpoint:** `GET /api/datastores`

**Query Parameters:**
- `region` (optional) - AWS region

**Response:**
```json
{
  "region": "us-east-1",
  "datastore_count": 2,
  "datastores": [
    {
      "datastore_id": "12e041ae...",
      "datastore_name": "single-file-datastore",
      "status": "ACTIVE",
      "created_at": "2025-10-29T14:00:00Z"
    }
  ]
}
```

**Example:**
```bash
curl "http://localhost:8080/api/datastores?region=us-east-1"
```

---

### 9. Health Check

Check if API is running.

**Endpoint:** `GET /health`

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-10-29T22:00:00Z"
}
```

---

### 10. API Documentation

Get list of available endpoints.

**Endpoint:** `GET /`

**Response:**
```json
{
  "name": "AWS HealthImaging API",
  "version": "1.0",
  "endpoints": {
    "POST /api/import": "Import DICOM files from S3",
    "GET /api/import/<job_id>": "Get import job status",
    ...
  }
}
```

---

## Workflow Example

Complete workflow for importing and viewing DICOM images:

```bash
# 1. List available datastores
curl "http://localhost:8080/api/datastores"

# 2. Import DICOM files from S3
curl -X POST http://localhost:8080/api/import \
  -H "Content-Type: application/json" \
  -d '{
    "source_bucket": "my-bucket",
    "source_prefix": "dicom-case13/"
  }'
# Response: {"job_id": "abc123..."}

# 3. Check import status (repeat until COMPLETED)
curl "http://localhost:8080/api/import/abc123..."

# 4. List studies in datastore
curl "http://localhost:8080/api/studies"
# Response: {"studies": [{"study_uid": "1.2.276..."}]}

# 5. View WSI in browser
open "http://localhost:8080/api/viewer/1.2.276..."

# 6. (Optional) Delete study when done
curl -X DELETE "http://localhost:8080/api/studies/1.2.276..."
```

---

## Error Responses

All endpoints return standard error responses:

```json
{
  "error": "Description of error"
}
```

**HTTP Status Codes:**
- `200` - Success
- `201` - Created
- `400` - Bad Request (missing parameters)
- `404` - Not Found
- `500` - Internal Server Error

---

## Notes

- **Datastore Capacity:** Each datastore can store up to 50 studies efficiently
- **Import Time:** Import jobs typically take 15-30 minutes depending on file size
- **Series Handling:** AWS HealthImaging creates separate image sets per DICOM Series
- **Tile Caching:** Consider implementing tile caching for better performance in production

---

## Development

Run in development mode:
```bash
python api.py
```

Run tests:
```bash
pytest tests/
```

## Production Deployment

For production use:

1. Use a production WSGI server (gunicorn, uwsgi)
2. Enable HTTPS
3. Implement authentication/authorization
4. Add rate limiting
5. Set up monitoring and logging
6. Use a reverse proxy (nginx, ALB)

Example with gunicorn:
```bash
gunicorn -w 4 -b 0.0.0.0:8080 api:app
```
