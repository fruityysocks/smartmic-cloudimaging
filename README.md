# AWS HealthImaging API

RESTful API for managing DICOM imports and viewing WSI (Whole Slide Imaging) images stored in AWS HealthImaging.

## Features

- ✅ Import DICOM files from S3 buckets
- ✅ Multi-datastore support (up to 50 studies per datastore)
- ✅ Track import job status
- ✅ List and manage studies
- ✅ Interactive OpenSeadragon WSI viewer
- ✅ Delete studies and image sets
- ✅ RESTful API with CORS support
- ✅ HTJ2K decoding (AWS HealthImaging format)

## Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

### 2. Configure Environment

Create `.env` file (or copy from `.env.example`):

```env
# AWS Configuration
AWS_REGION=us-east-1

# AWS HealthImaging
AWS_HEALTHIMAGING_DATASTORE_ID=your-datastore-id
AWS_HEALTHIMAGING_ROLE_ARN=arn:aws:iam::ACCOUNT_ID:role/HealthImagingServiceRole

# S3 Buckets
AWS_S3_SOURCE_BUCKET=your-source-bucket
AWS_S3_OUTPUT_BUCKET=your-output-bucket

# Optional
PORT=8080
```

### 3. Setup IAM Permissions

Use the generalized setup script:

```bash
# Setup for current user
python setup_iam.py --current-user

# Or setup for specific user
python setup_iam.py --user myuser --datastore-id 12e041ae...

# Or setup for IAM roles
python setup_iam.py --roles HealthImagingServiceRole --datastore-id 12e041ae...
```

**IAM Setup Options:**
- `--current-user` - Use current IAM user from credentials
- `--user <name>` - Specific IAM user
- `--roles <role1,role2>` - Comma-separated list of IAM roles
- `--datastore-id <id>` - Specific datastore (or use env var)
- `--all-datastores` - Grant access to all datastores
- `--bucket-pattern <pattern>` - S3 bucket pattern (default: *-healthimaging-*)

**Permissions Granted:**
- Manage DICOM import jobs
- Search and access image sets
- List and describe datastores
- Delete image sets
- S3 read/write access for HealthImaging buckets

### 4. Start the API

```bash
python api.py
```

The API will be available at `http://localhost:8080`

## API Endpoints

### Import DICOM from S3

```bash
curl -X POST http://localhost:8080/api/import \
  -H "Content-Type: application/json" \
  -d '{
    "source_bucket": "my-bucket",
    "source_prefix": "dicom-case13/"
  }'
```

### Check Import Status

```bash
curl "http://localhost:8080/api/import/{job_id}"
```

### List Studies

```bash
curl "http://localhost:8080/api/studies"
```

### View WSI

```bash
# Open in browser
open "http://localhost:8080/api/viewer/{study_uid}"
```

### Delete Study

```bash
curl -X DELETE "http://localhost:8080/api/studies/{study_uid}"
```

### List Datastores

```bash
curl "http://localhost:8080/api/datastores"
```

**Complete API documentation:** See [API_DOCS.md](API_DOCS.md)

## Architecture

```
DICOM files in S3
    ↓
AWS HealthImaging Import
    ↓
Image sets in datastore (HTJ2K compressed)
    ↓
API decodes and serves tiles
    ↓
OpenSeadragon viewer
```

## S3 Buckets

You need **2 S3 buckets**:

1. **Source bucket** - Where your DICOM files are stored
2. **Output bucket** - Where AWS HealthImaging writes import job logs

AWS HealthImaging imports directly from the source bucket - no intermediate reorganization needed!

## Environment Variables

### Required

- `AWS_REGION` - AWS region (default: us-east-1)
- `AWS_HEALTHIMAGING_DATASTORE_ID` - Default datastore ID
- `AWS_HEALTHIMAGING_ROLE_ARN` - IAM role ARN for HealthImaging
- `AWS_S3_OUTPUT_BUCKET` - S3 bucket for import job outputs

### Optional

- `AWS_S3_SOURCE_BUCKET` - Default source bucket for imports
- `PORT` - API server port (default: 8080)

## Workflow Example

```bash
# 1. Import DICOM files
curl -X POST http://localhost:8080/api/import \
  -d '{"source_bucket": "my-bucket", "source_prefix": "case-13/"}'
# Response: {"job_id": "abc123..."}

# 2. Check import status (repeat until COMPLETED)
curl "http://localhost:8080/api/import/abc123..."

# 3. List studies
curl "http://localhost:8080/api/studies"
# Response: {"studies": [{"study_uid": "1.2.276..."}]}

# 4. View in browser
open "http://localhost:8080/api/viewer/1.2.276..."

# 5. (Optional) Delete when done
curl -X DELETE "http://localhost:8080/api/studies/1.2.276..."
```

## Multi-Datastore Support

The API supports multiple datastores (up to 50 studies each). Pass `datastore_id` in requests:

```bash
curl -X POST http://localhost:8080/api/import \
  -d '{
    "source_bucket": "bucket1",
    "source_prefix": "case-1/",
    "datastore_id": "datastore-1"
  }'
```

Or set different default in `.env` and restart the API.

## Troubleshooting

### Import Job Stuck

```bash
# Check job status
curl "http://localhost:8080/api/import/{job_id}"
```

Import jobs typically take 15-30 minutes. Status will be:
- `SUBMITTED` - Job queued
- `IN_PROGRESS` - Processing
- `COMPLETED` - Success
- `FAILED` - Error (check logs in S3 output bucket)

### Red/White Tiles

If you see red or white tiles in the viewer:
1. Check that image sets imported successfully
2. Verify IAM permissions include `GetImageFrame`
3. Check server logs for decoding errors

### Permission Denied

Run IAM setup:
```bash
python setup_iam.py --current-user
```

Or ask your AWS administrator to grant HealthImaging permissions.

## Development

### Project Structure

```
smartmic-cloudimaging/
├── api.py                  # Main API server
├── wsi_app.py             # Old viewer (for reference)
├── setup_iam.py           # IAM permissions setup
├── src/                   # Core pipeline code
│   └── pipeline/
├── requirements.txt       # Python dependencies
├── .env                   # Configuration (create from .env.example)
├── API_DOCS.md           # Complete API documentation
└── README.md             # This file
```

### Run Tests

```bash
pytest tests/
```

### Production Deployment

For production:

1. Use a production WSGI server:
   ```bash
   gunicorn -w 4 -b 0.0.0.0:8080 api:app
   ```

2. Enable HTTPS (use nginx, ALB, or CloudFront)
3. Implement authentication/authorization
4. Add rate limiting
5. Set up monitoring (CloudWatch, DataDog, etc.)
6. Use environment-specific configs

## License

MIT License

## Support

For issues and questions:
- GitHub Issues: [Repository URL]
- AWS HealthImaging Documentation: https://docs.aws.amazon.com/healthimaging/
