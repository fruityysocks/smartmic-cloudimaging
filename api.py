#!/usr/bin/env python3
"""
AWS HealthImaging API Server
Provides RESTful API endpoints for managing DICOM imports and viewing WSI images.
"""

import os
import sys
import json
import gzip
import logging
from datetime import datetime
from collections import defaultdict
from flask import Flask, request, jsonify, send_file, render_template_string
from flask_cors import CORS
from io import BytesIO
from PIL import Image
import numpy as np
import boto3

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Import HTJ2K decoder
import openjpeg

def jpeg_decode(data):
    """Decode HTJ2K/JPEG2000 data using openjpeg."""
    from io import BytesIO
    return openjpeg.decode(BytesIO(data))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Global storage for datastore metadata and study caches
datastores = {}
study_metadata_cache = {}  # {study_uid: wsi_metadata}
pipeline_cache = {}  # {datastore_id: DICOMWSIPipeline}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_client(region='us-east-1'):
    """Get AWS HealthImaging client."""
    return boto3.client('medical-imaging', region_name=region)

def get_s3_client(region='us-east-1'):
    """Get S3 client."""
    return boto3.client('s3', region_name=region)

def analyze_dicom_files(bucket, prefix, region='us-east-1'):
    """
    Analyze DICOM files in S3 bucket and return unique Study/Series combinations.

    Returns:
        dict: {
            'studies': [
                {
                    'study_uid': '...',
                    'series': [
                        {'series_uid': '...', 'series_number': 0, 'files': ['s3_key1', ...]},
                        ...
                    ]
                },
                ...
            ]
        }
    """
    import pydicom
    from pydicom.errors import InvalidDicomError

    s3_client = get_s3_client(region)

    # Download and analyze DICOM files
    study_series_map = defaultdict(lambda: defaultdict(list))

    logger.info(f"Analyzing DICOM files in s3://{bucket}/{prefix}")

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    file_count = 0
    for page in pages:
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']

            # Only process .dcm files
            if not key.lower().endswith('.dcm'):
                continue

            try:
                # Download file to memory
                response = s3_client.get_object(Bucket=bucket, Key=key)
                file_bytes = response['Body'].read()

                # Read DICOM metadata (without pixel data)
                ds = pydicom.dcmread(BytesIO(file_bytes), stop_before_pixels=True)

                study_uid = str(ds.StudyInstanceUID)
                series_uid = str(ds.SeriesInstanceUID)
                series_number = int(ds.get('SeriesNumber', 0))

                study_series_map[study_uid][series_uid].append({
                    's3_key': key,
                    'series_number': series_number
                })

                file_count += 1

            except InvalidDicomError:
                logger.warning(f"Skipping non-DICOM file: {key}")
            except Exception as e:
                logger.error(f"Error reading {key}: {e}")

    # Convert to output format
    studies = []
    for study_uid, series_dict in study_series_map.items():
        series_list = []
        for series_uid, files in series_dict.items():
            series_list.append({
                'series_uid': series_uid,
                'series_number': files[0]['series_number'],  # All files in series have same number
                'file_count': len(files),
                'files': [f['s3_key'] for f in files]
            })

        studies.append({
            'study_uid': study_uid,
            'series': sorted(series_list, key=lambda x: x['series_number'])
        })

    logger.info(f"Found {file_count} DICOM files, {len(studies)} studies")

    return {'studies': studies}

def get_datastore_studies(datastore_id, region='us-east-1'):
    """Get all studies in a datastore."""
    client = get_client(region)

    response = client.search_image_sets(
        datastoreId=datastore_id,
        searchCriteria={}
    )

    image_sets = response.get('imageSetsMetadataSummaries', [])

    # Group by study
    study_map = defaultdict(lambda: {'series': [], 'patient_id': None, 'study_date': None})

    for img_set in image_sets:
        image_set_id = img_set['imageSetId']

        # Get metadata
        meta_response = client.get_image_set_metadata(
            datastoreId=datastore_id,
            imageSetId=image_set_id
        )

        blob = meta_response['imageSetMetadataBlob'].read()
        if meta_response.get('contentEncoding') == 'gzip':
            blob = gzip.decompress(blob)

        metadata = json.loads(blob)

        # Extract study info
        study_dict = metadata.get('Study', {})
        series_dict = study_dict.get('Series', {})
        if series_dict:
            first_series_uid = list(series_dict.keys())[0]
            # Extract study UID by removing last segment (series number) from series UID
            study_uid = first_series_uid.rsplit('.', 1)[0] if '.' in first_series_uid else first_series_uid
        else:
            study_uid = 'unknown'

        # Get first series to extract study-level info
        series_dict = study_dict.get('Series', {})
        if series_dict:
            first_series = next(iter(series_dict.values()))
            series_dicom = first_series.get('DICOM', {})
            series_number = series_dicom.get('SeriesNumber', 0)

            # Get study-level DICOM tags
            study_map[study_uid]['patient_id'] = study_dict.get('DICOM', {}).get('PatientID', 'N/A')
            study_map[study_uid]['study_date'] = study_dict.get('DICOM', {}).get('StudyDate', 'N/A')

            study_map[study_uid]['series'].append({
                'image_set_id': image_set_id,
                'series_number': series_number,
                'series_uid': list(series_dict.keys())[0] if series_dict else 'unknown'
            })

    # Convert to list
    studies = []
    for study_uid, info in study_map.items():
        studies.append({
            'study_uid': study_uid,
            'patient_id': info['patient_id'],
            'study_date': info['study_date'],
            'series_count': len(info['series']),
            'series': sorted(info['series'], key=lambda x: x['series_number'])
        })

    return studies

def decode_frame(frame_id, image_set_id, client, datastore_id):
    """Decode HTJ2K frame to RGB numpy array using pylibjpeg-openjpeg."""
    try:
        response = client.get_image_frame(
            datastoreId=datastore_id,
            imageSetId=image_set_id,
            imageFrameInformation={"imageFrameId": frame_id}
        )
        data = response["imageFrameBlob"].read()

        img_array = jpeg_decode(data)

        if len(img_array.shape) == 2:
            img_array = np.stack([img_array] * 3, axis=-1)

        if img_array.dtype != np.uint8:
            img_array = img_array.astype(np.uint8)

        return img_array

    except Exception as e:
        logger.error(f"Decode exception: {e}", exc_info=True)
        return None

def build_wsi_metadata_for_datastore(datastore_id, region='us-east-1'):
    """
    Build WSI metadata for entire datastore (like wsi_app.py does).
    This loads ALL image sets and builds the pyramid.
    """
    client = get_client(region)

    # Get all image sets
    response = client.search_image_sets(
        datastoreId=datastore_id,
        searchCriteria={}
    )
    all_image_sets = response.get('imageSetsMetadataSummaries', [])

    logger.info(f"Found {len(all_image_sets)} image sets - building pyramid")

    # Group frames by pyramid level across all image sets
    pyramid_levels = defaultdict(list)
    image_set_map = {}

    for img_set in all_image_sets:
        image_set_id = img_set['imageSetId']

        # Get metadata
        meta_response = client.get_image_set_metadata(
            datastoreId=datastore_id,
            imageSetId=image_set_id
        )

        blob = meta_response['imageSetMetadataBlob'].read()
        if meta_response.get('contentEncoding') == 'gzip':
            blob = gzip.decompress(blob)

        metadata = json.loads(blob)

        series = next(iter(metadata["Study"]["Series"].values()))
        instances = series["Instances"]

        series_dicom = series.get("DICOM", {})
        series_number = series_dicom.get("SeriesNumber", 0)
        level = int(series_number)

        for instance_uid, instance_data in instances.items():
            frames = instance_data.get("ImageFrames", [])
            dicom_attrs = instance_data.get("DICOM", {})

            rows = dicom_attrs.get("Rows", 500)
            cols = dicom_attrs.get("Columns", 500)
            total_cols = dicom_attrs.get("TotalPixelMatrixColumns", 0)
            total_rows = dicom_attrs.get("TotalPixelMatrixRows", 0)

            per_frame_groups = dicom_attrs.get("PerFrameFunctionalGroupsSequence", [])

            for idx, frame in enumerate(frames):
                col_pos = row_pos = 0

                if idx < len(per_frame_groups):
                    plane_seq = per_frame_groups[idx].get("PlanePositionSlideSequence", [])
                    if plane_seq:
                        col_pos = plane_seq[0].get("ColumnPositionInTotalImagePixelMatrix", 0)
                        row_pos = plane_seq[0].get("RowPositionInTotalImagePixelMatrix", 0)

                frame_info = {
                    "frame_id": frame["ID"],
                    "image_set_id": image_set_id,
                    "rows": rows,
                    "cols": cols,
                    "total_rows": total_rows,
                    "total_cols": total_cols,
                    "col_position": col_pos - 1 if col_pos > 0 else 0,
                    "row_position": row_pos - 1 if row_pos > 0 else 0
                }

                pyramid_levels[level].append(frame_info)
                image_set_map[frame["ID"]] = image_set_id

    # Build tile index for each level
    tile_index = {}
    level_bounds = {}

    for level, frames in pyramid_levels.items():
        tile_index[level] = {}
        min_col = min_row = float('inf')
        max_col = max_row = 0

        for frame in frames:
            if frame['rows'] > 0 and frame['cols'] > 0:
                tile_x = frame['col_position'] // frame['cols']
                tile_y = frame['row_position'] // frame['rows']
                tile_index[level][(tile_x, tile_y)] = frame

                min_col = min(min_col, tile_x)
                min_row = min(min_row, tile_y)
                max_col = max(max_col, tile_x)
                max_row = max(max_row, tile_y)

        level_bounds[level] = (min_col, min_row, max_col, max_row)
        logger.info(f"Level {level}: {len(frames)} tiles, bounds col[{min_col}-{max_col}] row[{min_row}-{max_row}]")

    sorted_levels = sorted(pyramid_levels.keys())
    best_quality_level = sorted_levels[0] if sorted_levels else 0

    if best_quality_level in pyramid_levels and pyramid_levels[best_quality_level]:
        base_level = best_quality_level
        base_frames = pyramid_levels[base_level]
        base_frame = base_frames[0]
        tile_size = base_frame['rows']

        # Use TotalPixelMatrix if available
        full_width = base_frame['total_cols']
        full_height = base_frame['total_rows']

        if full_width == 0 or full_height == 0:
            logger.info(f"TotalPixelMatrix not set, calculating from tile positions...")

            # Find actual pixel coverage from tiles
            min_col_pos = min_row_pos = float('inf')
            max_col_pos = max_row_pos = 0

            for frame in base_frames:
                col_pos = frame['col_position']
                row_pos = frame['row_position']
                tile_width = frame['cols']
                tile_height = frame['rows']

                min_col_pos = min(min_col_pos, col_pos)
                min_row_pos = min(min_row_pos, row_pos)
                max_col_pos = max(max_col_pos, col_pos + tile_width)
                max_row_pos = max(max_row_pos, row_pos + tile_height)

            full_width = max_col_pos - min_col_pos
            full_height = max_row_pos - min_row_pos

            logger.info(f"Calculated from tiles: {full_width}x{full_height}")
        else:
            logger.info(f"Using TotalPixelMatrix: {full_width}x{full_height}")
    else:
        tile_size = 500
        full_width = full_height = 10000

    wsi_metadata = {
        "pyramid_levels": pyramid_levels,
        "tile_index": tile_index,
        "level_bounds": level_bounds,
        "sorted_levels": sorted_levels,
        "full_width": full_width,
        "full_height": full_height,
        "tile_size": tile_size,
        "num_levels": len(sorted_levels),
        "image_set_map": image_set_map
    }

    logger.info(f"WSI pyramid initialized: {full_width}x{full_height}px, {len(sorted_levels)} levels")

    return wsi_metadata

def load_study_metadata(study_uid, datastore_id, region='us-east-1'):
    """
    Load WSI metadata for a specific study.

    For now, since we have one study per datastore, just load the entire datastore
    using the same logic as wsi_app.py (which works correctly).

    Returns:
        dict: wsi_metadata with pyramid structure, tile index, etc.
    """
    # Check cache - use datastore_id as key since we're loading full datastore
    cache_key = f"{datastore_id}"
    if cache_key in study_metadata_cache:
        logger.info(f"Using cached metadata for datastore {datastore_id}")
        return study_metadata_cache[cache_key]

    # Just use the working build function from wsi_app.py logic
    wsi_metadata = build_wsi_metadata_for_datastore(datastore_id, region)

    if not wsi_metadata:
        return None

    # Cache it
    study_metadata_cache[cache_key] = wsi_metadata

    return wsi_metadata

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/')
def index():
    """API documentation."""
    return jsonify({
        'name': 'AWS HealthImaging API',
        'version': '1.0',
        'endpoints': {
            'POST /api/import': 'Import DICOM files from S3',
            'GET /api/import/<job_id>': 'Get import job status',
            'GET /api/studies': 'List studies in datastore',
            'GET /api/viewer/<study_uid>': 'View WSI by StudyUID',
            'GET /api/studies/<study_uid>/dzi': 'Get DZI descriptor',
            'GET /api/studies/<study_uid>/tiles/<level>/<col>_<row>.jpg': 'Get tile',
            'DELETE /api/studies/<study_uid>': 'Delete study',
            'GET /api/datastores': 'List datastores'
        }
    })

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})

# ============================================================================
# ENDPOINT 1: Import DICOM from S3
# ============================================================================

@app.route('/api/import', methods=['POST'])
def import_dicom():
    """
    Import DICOM files from S3 bucket.

    POST /api/import
    Body: {
        "source_bucket": "bucket-name",
        "source_prefix": "folder/",
        "output_bucket": "optional-output-bucket",
        "datastore_id": "optional-datastore-id",
        "region": "optional-region"
    }
    """
    data = request.json

    source_bucket = data.get('source_bucket')
    source_prefix = data.get('source_prefix', '')
    datastore_id = data.get('datastore_id')
    output_bucket = data.get('output_bucket', os.getenv('AWS_S3_OUTPUT_BUCKET'))
    role_arn = os.getenv('AWS_HEALTHIMAGING_ROLE_ARN')
    region = data.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    if not source_bucket:
        return jsonify({'error': 'source_bucket is required'}), 400

    if not datastore_id:
        datastore_id = os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID')

    if not all([datastore_id, output_bucket, role_arn]):
        return jsonify({'error': 'Missing configuration: datastore_id, output_bucket, or role_arn'}), 400

    try:
        # Step 1: Analyze DICOM files
        logger.info(f"Analyzing DICOM files in s3://{source_bucket}/{source_prefix}")
        analysis = analyze_dicom_files(source_bucket, source_prefix, region)

        # Step 2: Start import job
        client = get_client(region)

        input_s3_uri = f's3://{source_bucket}/{source_prefix}'
        output_s3_uri = f's3://{output_bucket}/healthimaging-output/{source_prefix}'

        response = client.start_dicom_import_job(
            datastoreId=datastore_id,
            dataAccessRoleArn=role_arn,
            inputS3Uri=input_s3_uri,
            outputS3Uri=output_s3_uri,
            jobName=f"import-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        )

        return jsonify({
            'job_id': response['jobId'],
            'status': response['jobStatus'],
            'datastore_id': datastore_id,
            'input_uri': input_s3_uri,
            'output_uri': output_s3_uri,
            'studies': analysis['studies']
        }), 201

    except Exception as e:
        logger.error(f"Import failed: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# ENDPOINT 2: Get Import Job Status
# ============================================================================

@app.route('/api/import/<job_id>', methods=['GET'])
def get_import_status(job_id):
    """
    Get status of import job.

    GET /api/import/{job_id}?datastore_id=xxx
    """
    datastore_id = request.args.get('datastore_id', os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID'))
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    if not datastore_id:
        return jsonify({'error': 'datastore_id is required'}), 400

    try:
        client = get_client(region)

        response = client.get_dicom_import_job(
            datastoreId=datastore_id,
            jobId=job_id
        )

        job_props = response['jobProperties']

        return jsonify({
            'job_id': job_id,
            'status': job_props['jobStatus'],
            'submitted_at': job_props.get('submittedAt', '').isoformat() if job_props.get('submittedAt') else None,
            'ended_at': job_props.get('endedAt', '').isoformat() if job_props.get('endedAt') else None,
            'message': job_props.get('message', ''),
            'input_uri': job_props.get('inputS3Uri', ''),
            'output_uri': job_props.get('outputS3Uri', '')
        })

    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# ENDPOINT 3: List Studies
# ============================================================================

@app.route('/api/studies', methods=['GET'])
def list_studies():
    """
    List all studies in datastore.

    GET /api/studies?datastore_id=xxx
    """
    datastore_id = request.args.get('datastore_id', os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID'))
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    if not datastore_id:
        return jsonify({'error': 'datastore_id is required'}), 400

    try:
        studies = get_datastore_studies(datastore_id, region)

        return jsonify({
            'datastore_id': datastore_id,
            'study_count': len(studies),
            'studies': studies
        })

    except Exception as e:
        logger.error(f"Failed to list studies: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# ENDPOINT 4: View WSI by StudyUID
# ============================================================================

@app.route('/api/viewer/<study_uid>', methods=['GET'])
def view_study(study_uid):
    """
    View WSI by StudyUID.

    GET /api/viewer/{study_uid}?datastore_id=xxx
    """
    datastore_id = request.args.get('datastore_id', os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID'))

    # Load DZI metadata dynamically
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    try:
        wsi_metadata = load_study_metadata(study_uid, datastore_id, region)
        if not wsi_metadata:
            return jsonify({'error': 'Study not found or failed to load metadata'}), 404

        dzi_info = {
            'width': wsi_metadata['full_width'],
            'height': wsi_metadata['full_height'],
            'tile_size': wsi_metadata['tile_size']
        }
    except Exception as e:
        logger.error(f"Failed to load metadata for viewer: {e}")
        return jsonify({'error': str(e)}), 500

    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>WSI Viewer - Study {{ study_uid }}</title>
        <script src="https://cdn.jsdelivr.net/npm/openseadragon@3.1/build/openseadragon/openseadragon.min.js"></script>
        <style>
            body { margin: 0; padding: 20px; font-family: Arial, sans-serif; }
            h1 { margin-bottom: 10px; }
            #viewer { width: 100%; height: 800px; border: 1px solid #ccc; }
            .info { margin-bottom: 10px; color: #666; }
        </style>
    </head>
    <body>
        <h1>WSI Viewer - Study {{ study_uid }}</h1>
        <div class="info">
            Datastore: {{ datastore_id }}<br>
            Image Size: {{ dzi_info.width }} x {{ dzi_info.height }}<br>
            Tile Size: {{ dzi_info.tile_size }}
        </div>
        <div id="viewer"></div>
        <script>
            var viewer = OpenSeadragon({
                id: "viewer",
                prefixUrl: "https://cdn.jsdelivr.net/npm/openseadragon@3.1/build/openseadragon/images/",
                tileSources: {
                    Image: {
                        xmlns: "http://schemas.microsoft.com/deepzoom/2008",
                        Url: "/api/studies/{{ study_uid }}/tiles/",
                        Format: "jpg",
                        Overlap: 0,
                        TileSize: {{ dzi_info.tile_size }},
                        Size: {
                            Width: {{ dzi_info.width }},
                            Height: {{ dzi_info.height }}
                        }
                    }
                },
                showNavigator: true,
                navigatorPosition: "TOP_RIGHT"
            });
        </script>
    </body>
    </html>
    """, study_uid=study_uid, datastore_id=datastore_id, dzi_info=dzi_info)

# ============================================================================
# ENDPOINT 5: DZI and Tile Service
# ============================================================================

@app.route('/api/studies/<study_uid>/dzi', methods=['GET'])
def get_dzi(study_uid):
    """
    Get DZI descriptor for a study.

    GET /api/studies/{study_uid}/dzi?datastore_id=xxx
    """
    try:
        datastore_id = request.args.get('datastore_id', os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID'))
        region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

        # Load study metadata
        wsi_metadata = load_study_metadata(study_uid, datastore_id, region)
        if not wsi_metadata:
            return jsonify({'error': 'Study not found or failed to load metadata'}), 404

        return jsonify({
            'Image': {
                'xmlns': 'http://schemas.microsoft.com/deepzoom/2008',
                'Format': 'jpg',
                'Overlap': 0,
                'TileSize': wsi_metadata['tile_size'],
                'Size': {
                    'Width': wsi_metadata['full_width'],
                    'Height': wsi_metadata['full_height']
                }
            }
        })
    except Exception as e:
        logger.error(f"Failed to get DZI for study {study_uid}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/studies/<study_uid>/tiles/<int:level>/<int:col>_<int:row>.jpg', methods=['GET'])
def get_tile(study_uid, level, col, row):
    """
    Get tile for a study.

    GET /api/studies/{study_uid}/tiles/{level}/{col}_{row}.jpg?datastore_id=xxx
    """
    try:
        datastore_id = request.args.get('datastore_id', os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID'))
        region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

        # Load study metadata
        wsi_metadata = load_study_metadata(study_uid, datastore_id, region)
        if not wsi_metadata:
            logger.error(f"Study {study_uid} not found")
            blank = Image.new('RGB', (500, 500), color=(255, 255, 255))
            buf = BytesIO()
            blank.save(buf, format='JPEG', quality=85)
            buf.seek(0)
            return send_file(buf, mimetype='image/jpeg')

        # Map OSD level to our pyramid level (reverse order)
        available_levels = wsi_metadata["sorted_levels"]
        num_available = len(available_levels)

        if level >= num_available:
            our_level = available_levels[0]
        else:
            our_level = available_levels[-(level + 1)]

        # Apply offset based on level bounds
        if our_level in wsi_metadata["level_bounds"]:
            min_col, min_row, max_col, max_row = wsi_metadata["level_bounds"][our_level]
            actual_col = col + min_col
            actual_row = row + min_row
        else:
            actual_col = col
            actual_row = row

        tile_key = (actual_col, actual_row)

        # Debug for one specific tile
        if level == 12 and col == 0 and row == 0:
            logger.info(f"[TILE DEBUG] level={level}, col={col}, row={row}")
            logger.info(f"[TILE DEBUG] our_level={our_level}, tile_key={tile_key}")
            if our_level in wsi_metadata["tile_index"]:
                sample = list(wsi_metadata["tile_index"][our_level].keys())[:5]
                logger.info(f"[TILE DEBUG] Sample tiles in level {our_level}: {sample}")
                logger.info(f"[TILE DEBUG] Tile {tile_key} exists: {tile_key in wsi_metadata['tile_index'][our_level]}")

        # Check if tile exists
        if our_level not in wsi_metadata["tile_index"] or tile_key not in wsi_metadata["tile_index"][our_level]:
            # Return blank tile for missing tiles
            blank = Image.new('RGB', (wsi_metadata["tile_size"], wsi_metadata["tile_size"]), color=(240, 240, 240))
            buf = BytesIO()
            blank.save(buf, format="JPEG", quality=85)
            buf.seek(0)
            return send_file(buf, mimetype="image/jpeg")

        frame_info = wsi_metadata["tile_index"][our_level][tile_key]
        frame_id = frame_info["frame_id"]
        image_set_id = frame_info["image_set_id"]

        # Get or create client for this datastore
        if datastore_id not in pipeline_cache:
            pipeline_cache[datastore_id] = boto3.client('medical-imaging', region_name=region)
        client = pipeline_cache[datastore_id]

        # Decode the frame
        img_array = decode_frame(frame_id, image_set_id, client, datastore_id)

        if img_array is None:
            logger.error(f"Failed to decode frame {frame_id}")
            blank = Image.new('RGB', (wsi_metadata["tile_size"], wsi_metadata["tile_size"]), color=(200, 200, 200))
            buf = BytesIO()
            blank.save(buf, format="JPEG", quality=95)
            buf.seek(0)
            return send_file(buf, mimetype="image/jpeg")

        if img_array.shape[0] == 0 or img_array.shape[1] == 0:
            logger.error(f"Empty frame array for frame {frame_id}")
            blank = Image.new('RGB', (wsi_metadata["tile_size"], wsi_metadata["tile_size"]), color=(200, 200, 200))
            buf = BytesIO()
            blank.save(buf, format="JPEG", quality=95)
            buf.seek(0)
            return send_file(buf, mimetype="image/jpeg")

        try:
            img = Image.fromarray(img_array, mode='RGB')
        except Exception as e:
            logger.error(f"Failed to create PIL Image: {e}")
            blank = Image.new('RGB', (wsi_metadata["tile_size"], wsi_metadata["tile_size"]), color=(200, 200, 200))
            buf = BytesIO()
            blank.save(buf, format="JPEG", quality=95)
            buf.seek(0)
            return send_file(buf, mimetype="image/jpeg")

        buf = BytesIO()
        img.save(buf, format="JPEG", quality=95, subsampling=0)
        buf.seek(0)
        return send_file(buf, mimetype="image/jpeg")

    except Exception as e:
        logger.error(f"Failed to serve tile for study {study_uid}, level {level}, col {col}, row {row}: {e}")
        blank = Image.new('RGB', (500, 500), color=(255, 255, 255))
        buf = BytesIO()
        blank.save(buf, format='JPEG', quality=85)
        buf.seek(0)
        return send_file(buf, mimetype='image/jpeg')

# ============================================================================
# ENDPOINT 6: Delete Study
# ============================================================================

@app.route('/api/studies/<study_uid>', methods=['DELETE'])
def delete_study(study_uid):
    """
    Delete all image sets for a study.

    DELETE /api/studies/{study_uid}?datastore_id=xxx
    """
    datastore_id = request.args.get('datastore_id', os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID'))
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    if not datastore_id:
        return jsonify({'error': 'datastore_id is required'}), 400

    try:
        # Get all image sets for this study
        studies = get_datastore_studies(datastore_id, region)
        target_study = next((s for s in studies if s['study_uid'] == study_uid), None)

        if not target_study:
            return jsonify({'error': 'Study not found'}), 404

        client = get_client(region)
        deleted_count = 0

        for series in target_study['series']:
            image_set_id = series['image_set_id']

            try:
                client.delete_image_set(
                    datastoreId=datastore_id,
                    imageSetId=image_set_id
                )
                deleted_count += 1
                logger.info(f"Deleted image set {image_set_id}")

            except Exception as e:
                logger.error(f"Failed to delete image set {image_set_id}: {e}")

        return jsonify({
            'study_uid': study_uid,
            'deleted_image_sets': deleted_count,
            'total_series': len(target_study['series'])
        })

    except Exception as e:
        logger.error(f"Failed to delete study: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# ENDPOINT 7: List Datastores
# ============================================================================

@app.route('/api/datastores', methods=['GET'])
def list_datastores():
    """
    List all HealthImaging datastores.

    GET /api/datastores?region=us-east-1
    """
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    try:
        client = get_client(region)

        response = client.list_datastores()

        datastores = []
        for ds in response.get('datastoreSummaries', []):
            datastores.append({
                'datastore_id': ds['datastoreId'],
                'datastore_name': ds['datastoreName'],
                'status': ds['datastoreStatus'],
                'created_at': ds.get('createdAt', '').isoformat() if ds.get('createdAt') else None
            })

        return jsonify({
            'region': region,
            'datastore_count': len(datastores),
            'datastores': datastores
        })

    except Exception as e:
        logger.error(f"Failed to list datastores: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Main
# ============================================================================

if __name__ == '__main__':
    port = int(os.getenv('PORT2', 9090))
    logger.info(f"Starting AWS HealthImaging API on port {port}")
    logger.info(f"API documentation available at http://localhost:{port}/")
    app.run(host='0.0.0.0', port=port, debug=True)
