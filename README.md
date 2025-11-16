# AWS HealthImaging API

## Project Setup

### Dependencies

**1. Create a virtual environment:**

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

**2. Install required packages:**

```bash
pip install -r requirements.txt
```

**Required packages:**
- `flask` - Web framework
- `flask-cors` - Cross-origin resource sharing
- `boto3` - AWS SDK
- `python-dotenv` - Environment variable management
- `pydicom` - DICOM file handling
- `pillow` - Image processing
- `numpy` - Numerical operations
- `openjpeg` - HTJ2K/JPEG2000 decoding

### IAM Configuration

You need AWS credentials with HealthImaging permissions. There are two setup options:

**Option 1: Automated Setup (Recommended)**

Use the provided IAM setup script:

```bash
# Setup for current IAM user
python scripts/setup_iam.py --current-user

# Setup for specific user
python scripts/setup_iam.py --user myusername --datastore-id 12e041ae...

# Setup for IAM roles
python scripts/setup_iam.py --roles HealthImagingServiceRole --datastore-id 12e041ae...

# Grant access to all datastores
python scripts/setup_iam.py --current-user --all-datastores
```

**Option 2: Manual Setup**

Create IAM policies manually using the templates in `config/`:
- `config/app-permissions.json` - Application permissions
- `config/healthimaging-permissions.json` - HealthImaging-specific permissions
- `config/app-trust-policy.json` - Trust policy for roles
- `config/healthimaging-trust-policy.json` - HealthImaging trust policy

**Required IAM Permissions:**
- `medical-imaging:StartDICOMImportJob`
- `medical-imaging:GetDICOMImportJob`
- `medical-imaging:SearchImageSets`
- `medical-imaging:GetImageSet`
- `medical-imaging:GetImageSetMetadata`
- `medical-imaging:GetImageFrame`
- `medical-imaging:DeleteImageSet`
- `medical-imaging:ListDatastores`
- `medical-imaging:GetDatastore`
- `s3:GetObject` (for source bucket)
- `s3:PutObject` (for output bucket)

### Environment Variables

**1. Copy the example configuration:**

```bash
cp .env.example .env
```

**2. Edit `.env` with your values:**

## Running the Application

### Start the API Server

```bash
python run_api.py
```

The API will be available at `http://localhost:9090`

### Run CLI Scripts

**Import DICOM files:**
```bash
python scripts/import_dicom.py
```

**Generate thumbnails:**
```bash
# For specific datastore
python scripts/generate_thumbnails.py --datastore-id <id> --output thumbnails/

# For all datastores
python scripts/generate_thumbnails.py --all --output thumbnails/

# Custom thumbnail size
python scripts/generate_thumbnails.py --datastore-id <id> --size 512
```

**Setup IAM:**
```bash
python scripts/setup_iam.py --current-user
```

**Create HealthImaging role:**
```bash
python scripts/create_healthimaging_role.py
```

## API Endpoints

1. #### `GET /` -  API documentation and endpoint list.
2. #### `POST /api/import` - Import DICOM files from S3 bucket.
3. #### `GET /api/import/<job_id>?datastore_id=<id>` - Get import job status.
4. #### `GET /api/datastores` - List all available datastores.
5. #### `DELETE /api/datastores/<datastore_id>` - Delete all image sets in a datastore.
6. #### `GET /api/thumbnail/<datastore_id>` - Generate and return thumbnail for datastore. 
7. #### `GET /viewer` - Datastore selection page. 
8. #### `GET /viewer?datastore_id=<id>` - Open viewer for specific datastore. 

## Debug Endpoints 
1. #### `GET /viewer/<datastore_id>` - Get tile source descriptor (JSON for OpenSeadragon).
2. #### `GET /viewer/<datastore_id>/info` - Get WSI metadata and pyramid information.
3. #### `GET /viewer/<datastore_id>/tile_manifest` - Get tile availability manifest (shows which tiles exist at each level).
4. #### `GET /viewer/<datastore_id>/tiles/<level>/<col>_<row>.jpg` - Fetch individual tile image.

