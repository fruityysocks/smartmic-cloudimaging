"""
Datastore Viewer Module

Provides WSI pyramid initialization and tile serving for AWS HealthImaging datastores.
This module handles the OpenSeadragon tile source logic for viewing whole slide images.
"""

import gzip
import json
import logging
from collections import defaultdict
from io import BytesIO
import numpy as np
from PIL import Image
import openjpeg

logger = logging.getLogger(__name__)

# Global cache for datastore WSI metadata
datastore_wsi_cache = {}  # {datastore_id: wsi_metadata}


def jpeg_decode(data):
    """Decode HTJ2K/JPEG2000 data using openjpeg."""
    return openjpeg.decode(BytesIO(data))


def init_datastore_pipeline(datastore_id, client, region='us-east-1'):
    """
    Initialize DICOM pipeline and build WSI pyramid from ALL image sets in datastore.

    Args:
        datastore_id: AWS HealthImaging datastore ID
        client: boto3 medical-imaging client
        region: AWS region

    Returns:
        dict: WSI metadata containing pyramid structure, tile index, dimensions, etc.
    """
    if datastore_id in datastore_wsi_cache:
        logger.info(f"Using cached metadata for datastore {datastore_id}")
        return datastore_wsi_cache[datastore_id]

    logger.info(f"Initializing pipeline for datastore {datastore_id}")

    # Get all image sets
    try:
        response = client.search_image_sets(
            datastoreId=datastore_id,
            searchCriteria={}
        )
        image_sets = response.get('imageSetsMetadataSummaries', [])
    except Exception as e:
        logger.error(f"Failed to get image sets: {e}")
        return None

    if not image_sets:
        logger.error("No image sets found")
        return None

    logger.info(f"Found {len(image_sets)} image sets - treating as pyramid levels")

    # Group frames by pyramid level across all image sets
    pyramid_levels = defaultdict(list)
    image_set_map = {}

    for img_set in image_sets:
        image_set_id = img_set['imageSetId']

        # Get metadata
        try:
            metadata_response = client.get_image_set_metadata(
                datastoreId=datastore_id,
                imageSetId=image_set_id
            )
            blob = metadata_response['imageSetMetadataBlob'].read()
            if metadata_response.get('contentEncoding') == 'gzip':
                blob = gzip.decompress(blob)
            metadata = json.loads(blob)
        except Exception as e:
            logger.error(f"Failed to get metadata for {image_set_id}: {e}")
            continue

        series = next(iter(metadata['Study']['Series'].values()))
        instances = series['Instances']

        series_dicom = series.get('DICOM', {})
        series_number = series_dicom.get('SeriesNumber', 0)
        level = int(series_number)

        for instance_uid, instance_data in instances.items():
            frames = instance_data.get('ImageFrames', [])
            dicom_attrs = instance_data.get('DICOM', {})

            rows = dicom_attrs.get('Rows', 500)
            cols = dicom_attrs.get('Columns', 500)
            total_cols = dicom_attrs.get('TotalPixelMatrixColumns', 0)
            total_rows = dicom_attrs.get('TotalPixelMatrixRows', 0)

            per_frame_groups = dicom_attrs.get('PerFrameFunctionalGroupsSequence', [])

            for idx, frame in enumerate(frames):
                col_pos = row_pos = 0

                if idx < len(per_frame_groups):
                    plane_seq = per_frame_groups[idx].get('PlanePositionSlideSequence', [])
                    if plane_seq:
                        col_pos = plane_seq[0].get('ColumnPositionInTotalImagePixelMatrix', 0)
                        row_pos = plane_seq[0].get('RowPositionInTotalImagePixelMatrix', 0)

                frame_info = {
                    'frame_id': frame['ID'],
                    'image_set_id': image_set_id,
                    'rows': rows,
                    'cols': cols,
                    'total_rows': total_rows,
                    'total_cols': total_cols,
                    'col_position': col_pos - 1 if col_pos > 0 else 0,
                    'row_position': row_pos - 1 if row_pos > 0 else 0
                }

                pyramid_levels[level].append(frame_info)
                image_set_map[frame['ID']] = image_set_id

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
            # Calculate from tile positions
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
    else:
        tile_size = 500
        full_width = full_height = 10000

    wsi_metadata = {
        'datastore_id': datastore_id,
        'region': region,
        'pyramid_levels': pyramid_levels,
        'tile_index': tile_index,
        'level_bounds': level_bounds,
        'sorted_levels': sorted_levels,
        'full_width': full_width,
        'full_height': full_height,
        'tile_size': tile_size,
        'num_levels': len(sorted_levels),
        'image_set_map': image_set_map
    }

    # Cache it
    datastore_wsi_cache[datastore_id] = wsi_metadata

    logger.info(f"WSI pyramid initialized: {full_width}x{full_height}px")
    logger.info(f"Pyramid has {len(sorted_levels)} levels: {sorted_levels}")

    return wsi_metadata


def get_wsi_metadata(datastore_id):
    """Get cached WSI metadata for a datastore."""
    return datastore_wsi_cache.get(datastore_id)


def decode_tile(datastore_id, client, wsi_metadata, level, col, row):
    """
    Decode and return a tile as JPEG image.

    Args:
        datastore_id: AWS HealthImaging datastore ID
        client: boto3 medical-imaging client
        wsi_metadata: WSI metadata dict
        level: OSD pyramid level
        col: Tile column
        row: Tile row

    Returns:
        BytesIO: JPEG image buffer
    """
    available_levels = wsi_metadata['sorted_levels']
    num_available = len(available_levels)

    if level >= num_available:
        our_level = available_levels[0]
    else:
        our_level = available_levels[-(level + 1)]

    logger.info(f"[{datastore_id}] OSD Level {level} → Series {our_level} | Tile ({col}, {row})")

    # Apply offset based on level bounds
    if our_level in wsi_metadata['level_bounds']:
        min_col, min_row, max_col, max_row = wsi_metadata['level_bounds'][our_level]
        actual_col = col + min_col
        actual_row = row + min_row
    else:
        actual_col = col
        actual_row = row

    tile_key = (actual_col, actual_row)

    # Check if tile exists
    if our_level not in wsi_metadata['tile_index'] or tile_key not in wsi_metadata['tile_index'][our_level]:
        # Return blank tile
        blank = Image.new('RGB', (wsi_metadata['tile_size'], wsi_metadata['tile_size']), color=(240, 240, 240))
        buf = BytesIO()
        blank.save(buf, format='JPEG', quality=85)
        buf.seek(0)
        return buf

    frame_info = wsi_metadata['tile_index'][our_level][tile_key]
    frame_id = frame_info['frame_id']
    image_set_id = frame_info['image_set_id']

    # Decode frame
    try:
        response = client.get_image_frame(
            datastoreId=datastore_id,
            imageSetId=image_set_id,
            imageFrameInformation={'imageFrameId': frame_id}
        )
        data = response['imageFrameBlob'].read()

        img_array = jpeg_decode(data)

        if len(img_array.shape) == 2:
            img_array = np.stack([img_array] * 3, axis=-1)

        if img_array.dtype != np.uint8:
            img_array = img_array.astype(np.uint8)

        img = Image.fromarray(img_array, mode='RGB')
        buf = BytesIO()
        img.save(buf, format='JPEG', quality=95, subsampling=0)
        buf.seek(0)
        return buf

    except Exception as e:
        logger.error(f"Decode exception: {e}", exc_info=True)
        blank = Image.new('RGB', (wsi_metadata['tile_size'], wsi_metadata['tile_size']), color=(200, 200, 200))
        buf = BytesIO()
        blank.save(buf, format='JPEG', quality=95)
        buf.seek(0)
        return buf


def get_tile_manifest(wsi_metadata):
    """
    Generate tile manifest showing all available tiles per level.

    Args:
        wsi_metadata: WSI metadata dict

    Returns:
        dict: Manifest with tiles and bounds per level
    """
    manifest = {}

    for level in wsi_metadata['sorted_levels']:
        if level in wsi_metadata['tile_index']:
            tiles = list(wsi_metadata['tile_index'][level].keys())
            min_col, min_row, max_col, max_row = wsi_metadata['level_bounds'][level]

            level_key = int(level) if isinstance(level, str) else level

            manifest[str(level_key)] = {
                'tiles': [[int(x), int(y)] for x, y in tiles],
                'bounds': {
                    'min_col': int(min_col),
                    'min_row': int(min_row),
                    'max_col': int(max_col),
                    'max_row': int(max_row)
                },
                'num_tiles': len(tiles)
            }

    return manifest
