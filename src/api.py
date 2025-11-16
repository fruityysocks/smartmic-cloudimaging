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
import openjpeg

from .viewer import (
    init_datastore_pipeline,
    get_wsi_metadata,
    decode_tile,
    get_tile_manifest
)


# Load environment variables
from dotenv import load_dotenv
load_dotenv()


def jpeg_decode(data):
    """Decode HTJ2K/JPEG2000 data using openjpeg."""
    from io import BytesIO
    return openjpeg.decode(BytesIO(data))

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates')
CORS(app)

# Global storage (no longer needed, viewer module has its own cache)
datastores = {}
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

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/')
def index():
    """API documentation."""
    return jsonify({
        'name': 'AWS HealthImaging API',
        'endpoints': {
            'POST /api/import': 'Import DICOM files from S3',
            'GET /api/import/<job_id>': 'Get import job status',
            'GET /api/datastores': 'List all datastores',
            'DELETE /api/datastores/<datastore_id>': 'Delete all image sets in datastore',
            'GET /api/thumbnail/<datastore_id>': 'Generate thumbnail for datastore',
            'GET /viewer': 'Datastore selection page',
            'GET /viewer?datastore_id=<id>': 'View WSI for datastore',
            'GET /viewer/<datastore_id>': 'Get tile source descriptor (JSON)',
            'GET /viewer/<datastore_id>/info': 'Get metadata info',
            'GET /viewer/<datastore_id>/tile_manifest': 'Get tile availability manifest',
            'GET /viewer/<datastore_id>/tiles/<level>/<col>_<row>.jpg': 'Get individual tile'
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
# ENDPOINT 6: Delete Datastore
# ============================================================================

@app.route('/api/datastores/<datastore_id>', methods=['DELETE'])
def delete_datastore(datastore_id):
    """
    Delete all image sets in a datastore.

    DELETE /api/datastores/{datastore_id}?region=us-east-1
    """
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    try:
        client = get_client(region)

        # Get all image sets in the datastore
        response = client.search_image_sets(
            datastoreId=datastore_id,
            searchCriteria={}
        )
        image_sets = response.get('imageSetsMetadataSummaries', [])

        if not image_sets:
            return jsonify({
                'datastore_id': datastore_id,
                'deleted_image_sets': 0,
                'message': 'No image sets found in datastore'
            })

        deleted_count = 0
        failed_count = 0

        for img_set in image_sets:
            image_set_id = img_set['imageSetId']

            try:
                client.delete_image_set(
                    datastoreId=datastore_id,
                    imageSetId=image_set_id
                )
                deleted_count += 1
                logger.info(f"Deleted image set {image_set_id}")

            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to delete image set {image_set_id}: {e}")

        return jsonify({
            'datastore_id': datastore_id,
            'deleted_image_sets': deleted_count,
            'failed_deletions': failed_count,
            'total_image_sets': len(image_sets)
        })

    except Exception as e:
        logger.error(f"Failed to delete datastore contents: {e}")
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
# ENDPOINT 8: Generate Thumbnail
# ============================================================================

@app.route('/api/thumbnail/<datastore_id>', methods=['GET'])
def generate_thumbnail(datastore_id):
    """
    Generate thumbnail for a datastore.

    GET /api/thumbnail/<datastore_id>?size=512&max_tiles=100&region=us-east-1

    Query Parameters:
        size (int): Maximum thumbnail dimension (default: 512)
        max_tiles (int): Maximum tiles to process (default: None = all)
        region (str): AWS region (default: us-east-1)

    Returns:
        JPEG image
    """
    size = int(request.args.get('size', 512))
    max_tiles = request.args.get('max_tiles', None)
    if max_tiles:
        max_tiles = int(max_tiles)
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    try:
        logger.info(f"Generating thumbnail for datastore {datastore_id}")

        client = get_client(region)

        # Get all image sets
        response = client.search_image_sets(
            datastoreId=datastore_id,
            searchCriteria={}
        )
        image_sets = response.get('imageSetsMetadataSummaries', [])

        if not image_sets:
            return jsonify({'error': 'No image sets found in datastore'}), 404

        # Collect tiles from all image sets at lowest resolution
        all_tiles = {}
        tile_size = None
        total_cols = None
        total_rows = None

        # Try series 6 to 0 (lowest to highest resolution)
        for series_num in range(6, -1, -1):
            all_tiles.clear()

            for img_set in image_sets:
                image_set_id = img_set['imageSetId']

                # Get metadata
                metadata_response = client.get_image_set_metadata(
                    datastoreId=datastore_id,
                    imageSetId=image_set_id
                )

                blob = metadata_response['imageSetMetadataBlob'].read()
                if metadata_response.get('contentEncoding') == 'gzip':
                    blob = gzip.decompress(blob)
                metadata = json.loads(blob)

                study = metadata.get('Study', {})
                all_series = study.get('Series', {})

                for series_uid, series_data in all_series.items():
                    series_dicom = series_data.get('DICOM', {})
                    series_num_str = series_dicom.get('SeriesNumber', '0').strip()

                    try:
                        current_series = int(series_num_str)
                    except:
                        current_series = 0

                    if current_series != series_num:
                        continue

                    instances = series_data.get('Instances', {})
                    for instance_uid, instance_data in instances.items():
                        frames = instance_data.get('ImageFrames', [])
                        dicom_attrs = instance_data.get('DICOM', {})

                        if tile_size is None:
                            tile_size = dicom_attrs.get('Rows', 256)
                            total_cols = dicom_attrs.get('TotalPixelMatrixColumns', 0)
                            total_rows = dicom_attrs.get('TotalPixelMatrixRows', 0)

                        per_frame_groups = dicom_attrs.get('PerFrameFunctionalGroupsSequence', [])

                        for idx, frame in enumerate(frames):
                            if idx >= len(per_frame_groups):
                                continue

                            plane_seq = per_frame_groups[idx].get('PlanePositionSlideSequence', [])
                            if not plane_seq:
                                continue

                            col_pos = plane_seq[0].get('ColumnPositionInTotalImagePixelMatrix', 1) - 1
                            row_pos = plane_seq[0].get('RowPositionInTotalImagePixelMatrix', 1) - 1

                            tile_x = col_pos // tile_size
                            tile_y = row_pos // tile_size

                            all_tiles[(tile_x, tile_y)] = (image_set_id, frame['ID'])

            if len(all_tiles) >= 10:
                logger.info(f"Using series {series_num} with {len(all_tiles)} tiles")
                break
        else:
            return jsonify({'error': 'Insufficient tiles found'}), 404

        # Limit tiles if specified
        if max_tiles and len(all_tiles) > max_tiles:
            tiles_to_process = dict(list(all_tiles.items())[:max_tiles])
        else:
            tiles_to_process = all_tiles

        # Create output image at full dimensions
        output_image = Image.new('RGB', (total_cols, total_rows), color=(240, 240, 240))

        # Decode and place tiles
        decoded_count = 0
        for (tile_x, tile_y), (image_set_id, frame_id) in tiles_to_process.items():
            try:
                frame_response = client.get_image_frame(
                    datastoreId=datastore_id,
                    imageSetId=image_set_id,
                    imageFrameInformation={'imageFrameId': frame_id}
                )

                data = frame_response['imageFrameBlob'].read()
                img_array = jpeg_decode(data)

                # Convert grayscale to RGB
                if len(img_array.shape) == 2:
                    img_array = np.stack([img_array] * 3, axis=-1)

                if img_array.dtype != np.uint8:
                    img_array = img_array.astype(np.uint8)

                tile_img = Image.fromarray(img_array, mode='RGB')

                # Place at absolute position
                x_offset = tile_x * tile_size
                y_offset = tile_y * tile_size

                if x_offset < total_cols and y_offset < total_rows:
                    output_image.paste(tile_img, (x_offset, y_offset))
                    decoded_count += 1

            except Exception as e:
                logger.warning(f"Failed to decode tile ({tile_x}, {tile_y}): {e}")
                continue

        logger.info(f"Decoded {decoded_count} tiles")

        # Resize to thumbnail size
        output_image.thumbnail((size, size), Image.Resampling.LANCZOS)

        # Convert to JPEG bytes
        img_io = BytesIO()
        output_image.save(img_io, 'JPEG', quality=85)
        img_io.seek(0)

        return send_file(img_io, mimetype='image/jpeg')

    except Exception as e:
        logger.error(f"Failed to generate thumbnail: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# ENDPOINT: Datastore Viewer - OSD Tile Source
# ============================================================================

@app.route('/viewer', methods=['GET'])
def viewer_index():
    """
    Viewer index page - lists all datastores or shows viewer for specific datastore.

    GET /viewer - Show datastore selection page
    GET /viewer?datastore_id=xxx - Show viewer for specific datastore
    GET /viewer?datastore_id=xxx&region=us-east-1 - Specify region
    """
    datastore_id = request.args.get('datastore_id')
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))

    if datastore_id:
        # Show viewer for specific datastore
        from flask import render_template
        base_url = request.host_url.rstrip('/')
        return render_template('viewer_datastore.html',
                             datastore_id=datastore_id,
                             base_url=base_url)
    else:
        # Show datastore selection page
        from flask import render_template
        client = get_client(region)

        try:
            response = client.list_datastores()
            datastores = response.get('datastoreSummaries', [])
        except Exception as e:
            logger.error(f"Failed to list datastores: {e}")
            datastores = []

        return render_template('viewer_select.html',
                             datastores=datastores,
                             region=region)

@app.route('/viewer/<datastore_id>', methods=['GET'])
def viewer_dzi_descriptor(datastore_id):
    """
    Get DZI/tile source descriptor for OpenSeadragon viewer.

    GET /viewer/<datastore_id>?region=us-east-1

    Returns a custom tile source that can be fed to OpenSeadragon.
    """
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))
    client = get_client(region)

    # Initialize pipeline to get metadata
    wsi_metadata = init_datastore_pipeline(datastore_id, client, region)
    if not wsi_metadata:
        return jsonify({'error': 'Failed to initialize viewer metadata'}), 500

    base_url = request.host_url.rstrip('/')

    # Return OpenSeadragon-compatible tile source JSON
    return jsonify({
        'type': 'custom',
        'width': wsi_metadata['full_width'],
        'height': wsi_metadata['full_height'],
        'tileSize': wsi_metadata['tile_size'],
        'tileOverlap': 0,
        'minLevel': 0,
        'maxLevel': wsi_metadata['num_levels'] - 1,
        'num_levels': wsi_metadata['num_levels'],
        'actual_levels': wsi_metadata['sorted_levels'],
        'tile_url_template': f"{base_url}/viewer/{datastore_id}/tiles/{{level}}/{{x}}_{{y}}.jpg",
        'info_url': f"{base_url}/viewer/{datastore_id}/info",
        'manifest_url': f"{base_url}/viewer/{datastore_id}/tile_manifest"
    })

@app.route('/viewer/<datastore_id>/info', methods=['GET'])
def viewer_info(datastore_id):
    """Get detailed metadata info for datastore viewer."""
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))
    client = get_client(region)

    wsi_metadata = get_wsi_metadata(datastore_id)
    if not wsi_metadata:
        wsi_metadata = init_datastore_pipeline(datastore_id, client, region)
        if not wsi_metadata:
            return jsonify({'error': 'WSI metadata not initialized'}), 500

    try:
        return jsonify({
            'full_width': wsi_metadata['full_width'],
            'full_height': wsi_metadata['full_height'],
            'tile_size': wsi_metadata['tile_size'],
            'num_levels': wsi_metadata['num_levels'],
            'actual_levels': wsi_metadata['sorted_levels'],
            'level_bounds': {
                str(level): {
                    'min_col': bounds[0],
                    'min_row': bounds[1],
                    'max_col': bounds[2],
                    'max_row': bounds[3]
                }
                for level, bounds in wsi_metadata['level_bounds'].items()
            },
            'pyramid_structure': {
                str(level): {
                    'num_tiles': len(frames),
                    'tile_dimensions': f"{frames[0]['cols']}x{frames[0]['rows']}" if frames else 'N/A',
                    'total_dimensions': f"{frames[0]['total_cols']}x{frames[0]['total_rows']}" if frames else 'N/A'
                }
                for level, frames in wsi_metadata['pyramid_levels'].items()
            }
        })
    except Exception as e:
        logger.error(f"Error in /viewer/{datastore_id}/info endpoint: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/viewer/<datastore_id>/tile_manifest', methods=['GET'])
def viewer_tile_manifest_route(datastore_id):
    """Return manifest of all available tiles per level for datastore viewer."""
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))
    client = get_client(region)

    wsi_metadata = get_wsi_metadata(datastore_id)
    if not wsi_metadata:
        wsi_metadata = init_datastore_pipeline(datastore_id, client, region)
        if not wsi_metadata:
            return jsonify({'error': 'WSI metadata not initialized'}), 500

    manifest = get_tile_manifest(wsi_metadata)
    return jsonify(manifest)

@app.route('/viewer/<datastore_id>/tiles/<int:level>/<int:col>_<int:row>.jpg', methods=['GET'])
def viewer_get_tile(datastore_id, level, col, row):
    """Serve tile in Deep Zoom format for datastore viewer."""
    region = request.args.get('region', os.getenv('AWS_REGION', 'us-east-1'))
    client = get_client(region)

    wsi_metadata = get_wsi_metadata(datastore_id)
    if not wsi_metadata:
        wsi_metadata = init_datastore_pipeline(datastore_id, client, region)
        if not wsi_metadata:
            blank = Image.new('RGB', (500, 500), color=(240, 240, 240))
            buf = BytesIO()
            blank.save(buf, format='JPEG', quality=85)
            buf.seek(0)
            return send_file(buf, mimetype='image/jpeg')

    # Decode and return tile
    img_buf = decode_tile(datastore_id, client, wsi_metadata, level, col, row)
    return send_file(img_buf, mimetype='image/jpeg')

# ============================================================================
# Main
# ============================================================================

if __name__ == '__main__':
    port = int(os.getenv('PORT2', 9090))
    logger.info(f"Starting AWS HealthImaging API on port {port}")
    logger.info(f"API documentation available at http://localhost:{port}/")
    app.run(host='0.0.0.0', port=port, debug=True)
