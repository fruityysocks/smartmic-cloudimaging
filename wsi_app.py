import sys
import os
import gzip
import json
import logging
from io import BytesIO
from collections import defaultdict
from flask import Flask, send_file, jsonify, render_template, request
from src.pipeline.pipeline import DICOMWSIPipeline
from dotenv import load_dotenv
load_dotenv()
import numpy as np
from PIL import Image
import openjpeg

def jpeg_decode(data):
    """Decode HTJ2K/JPEG2000 data using openjpeg."""
    from io import BytesIO
    return openjpeg.decode(BytesIO(data))

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- App Setup ---
app = Flask(__name__, template_folder='templates')
pipeline = None
wsi_metadata = {}

# --- Initialization ---
def init_pipeline():
    """Initialize DICOM pipeline and build WSI pyramid from ALL image sets."""
    global pipeline, wsi_metadata
    
    datastore_id = os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID')
    region = os.getenv('AWS_REGION', 'us-east-1')
    
    if not datastore_id:
        logger.error("AWS_HEALTHIMAGING_DATASTORE_ID environment variable not set!")
        return
    
    pipeline = DICOMWSIPipeline(
        datastore_id=datastore_id,
        region=region
    )
    
    image_sets = pipeline.list_imported_images()
    if not image_sets:
        logger.error("No image sets found. Please import images first.")
        return
    
    logger.info(f"Found {len(image_sets)} image sets - treating as pyramid levels")
    
    # Store all image set IDs for frame retrieval
    image_set_map = {}
    
    # Group frames by pyramid level across all image sets
    pyramid_levels = defaultdict(list)
    
    for img_set in image_sets:
        image_set_id = img_set["imageSetId"]
        
        # Get metadata
        metadata_response = pipeline.accessor.client.get_image_set_metadata(
            datastoreId=pipeline.accessor.datastore_id,
            imageSetId=image_set_id
        )
        blob = metadata_response["imageSetMetadataBlob"].read()
        if metadata_response.get("contentEncoding") == "gzip":
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
            
            logger.info(f"Calculated from tiles:")
            logger.info(f"  Pixel coverage: col[{min_col_pos}-{max_col_pos}], row[{min_row_pos}-{max_row_pos}]")
            logger.info(f"  Dimensions: {full_width}x{full_height}")
        else:
            logger.info(f"Using TotalPixelMatrix: {full_width}x{full_height}")
        
        logger.info(f"Using level {base_level} as base")
        logger.info(f"Tile size: {tile_size}px")
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
    
    logger.info(f"WSI pyramid initialized: {full_width}x{full_height}px")
    logger.info(f"Pyramid has {len(sorted_levels)} real levels: {sorted_levels}")

def decode_frame(frame_id, image_set_id):
    """Decode HTJ2K frame to RGB numpy array using pylibjpeg-openjpeg."""
    try:
        response = pipeline.accessor.client.get_image_frame(
            datastoreId=pipeline.accessor.datastore_id,
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

# --- Routes ---
@app.route("/")
def viewer():
    return render_template("viewer.html")

@app.route("/health")
def health():
    """Health check endpoint."""
    status = {
        "status": "ok" if wsi_metadata else "error",
        "pipeline_initialized": pipeline is not None,
        "metadata_initialized": bool(wsi_metadata),
    }
    
    if wsi_metadata:
        status["image_info"] = {
            "num_levels": wsi_metadata.get("num_levels", 0),
            "dimensions": f"{wsi_metadata.get('full_width', 0)}x{wsi_metadata.get('full_height', 0)}",
            "tile_size": wsi_metadata.get("tile_size", 0)
        }
    
    return jsonify(status)

@app.route("/dzi_files/<int:level>/<int:col>_<int:row>.jpg")
def get_dzi_tile(level, col, row):
    """Serve tile in Deep Zoom format."""
    available_levels = wsi_metadata["sorted_levels"]
    num_available = len(available_levels)

    if level >= num_available:
        our_level = available_levels[0]
    else:
        our_level = available_levels[-(level + 1)]

    # Log tile request mapping
    logger.info(f"OSD Level {level} → Series {our_level} | Tile ({col}, {row})")
    
    # Apply offset based on level bounds
    if our_level in wsi_metadata["level_bounds"]:
        min_col, min_row, max_col, max_row = wsi_metadata["level_bounds"][our_level]
        actual_col = col + min_col
        actual_row = row + min_row
    else:
        actual_col = col
        actual_row = row
    
    tile_key = (actual_col, actual_row)
    
    # Check if tile exists
    if our_level not in wsi_metadata["tile_index"] or tile_key not in wsi_metadata["tile_index"][our_level]:
        # Return transparent/blank tile
        blank = Image.new('RGB', (wsi_metadata["tile_size"], wsi_metadata["tile_size"]), color=(240, 240, 240))
        buf = BytesIO()
        blank.save(buf, format="JPEG", quality=85)
        buf.seek(0)
        return send_file(buf, mimetype="image/jpeg")
    
    frame_info = wsi_metadata["tile_index"][our_level][tile_key]
    frame_id = frame_info["frame_id"]
    image_set_id = frame_info["image_set_id"]
    
    img_array = decode_frame(frame_id, image_set_id)
    
    if img_array is None:
        blank = Image.new('RGB', (wsi_metadata["tile_size"], wsi_metadata["tile_size"]), color=(200, 200, 200))
        buf = BytesIO()
        blank.save(buf, format="JPEG", quality=95)
        buf.seek(0)
        return send_file(buf, mimetype="image/jpeg")
    
    if img_array.shape[0] == 0 or img_array.shape[1] == 0:
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

@app.route("/info")
def info():
    if not wsi_metadata:
        return jsonify({"error": "WSI metadata not initialized"}), 500
    
    try:
        return jsonify({
            "full_width": wsi_metadata["full_width"],
            "full_height": wsi_metadata["full_height"],
            "tile_size": wsi_metadata["tile_size"],
            "num_levels": wsi_metadata["num_levels"],
            "actual_levels": wsi_metadata["sorted_levels"],
            "level_bounds": {
                str(level): {
                    "min_col": bounds[0],
                    "min_row": bounds[1],
                    "max_col": bounds[2],
                    "max_row": bounds[3]
                }
                for level, bounds in wsi_metadata["level_bounds"].items()
            },
            "pyramid_structure": {
                str(level): {
                    "num_tiles": len(frames),
                    "tile_dimensions": f"{frames[0]['cols']}x{frames[0]['rows']}" if frames else "N/A",
                    "total_dimensions": f"{frames[0]['total_cols']}x{frames[0]['total_rows']}" if frames else "N/A"
                }
                for level, frames in wsi_metadata["pyramid_levels"].items()
            }
        })
    except Exception as e:
        logger.error(f"Error in /info endpoint: {e}")
        return jsonify({"error": str(e)}), 500
    
@app.route("/tile_manifest")
def tile_manifest():
    """Return manifest of all available tiles per level."""
    if not wsi_metadata:
        return jsonify({"error": "WSI metadata not initialized"}), 500
    
    manifest = {}
    
    for level in wsi_metadata["sorted_levels"]:
        if level in wsi_metadata["tile_index"]:
            tiles = list(wsi_metadata["tile_index"][level].keys())
            min_col, min_row, max_col, max_row = wsi_metadata["level_bounds"][level]
            
            # Convert level to int to avoid string issues
            level_key = int(level) if isinstance(level, str) else level
            
            manifest[str(level_key)] = {  # Use str() to ensure consistent key format
                "tiles": [[int(x), int(y)] for x, y in tiles],
                "bounds": {
                    "min_col": int(min_col),
                    "min_row": int(min_row),
                    "max_col": int(max_col),
                    "max_row": int(max_row)
                },
                "num_tiles": len(tiles)
            }
    
    return jsonify(manifest)

@app.route("/debug/test_tile")
def test_tile():
    """Test if we can load any tile at all."""
    if not wsi_metadata:
        return jsonify({"error": "Metadata not initialized"}), 500
    
    # Try to find the first available tile
    for level in wsi_metadata["sorted_levels"]:
        # Clean level (remove spaces, convert to int)
        clean_level = int(level) if isinstance(level, str) else level
        
        if level in wsi_metadata["tile_index"]:
            tiles = wsi_metadata["tile_index"][level]
            if tiles:
                # Get first tile
                tile_key = list(tiles.keys())[0]
                frame_info = tiles[tile_key]
                
                # Calculate relative coordinates
                if level in wsi_metadata["level_bounds"]:
                    min_col, min_row, _, _ = wsi_metadata["level_bounds"][level]
                    rel_x = tile_key[0] - min_col
                    rel_y = tile_key[1] - min_row
                else:
                    rel_x = tile_key[0]
                    rel_y = tile_key[1]
                
                return jsonify({
                    "level": clean_level,
                    "tile_coords": tile_key,
                    "relative_coords": [rel_x, rel_y],
                    "frame_info": {
                        "frame_id": frame_info["frame_id"],
                        "image_set_id": frame_info["image_set_id"],
                        "position": [frame_info["col_position"], frame_info["row_position"]],
                        "size": [frame_info["cols"], frame_info["rows"]]
                    },
                    "test_url": f"/dzi_files/{clean_level}/{rel_x}_{rel_y}.jpg"
                })
    
    return jsonify({"error": "No tiles found"}), 404



# --- Main ---
if __name__ == "__main__":
    print("Initializing WSI pipeline with multi-level pyramid support...")
    init_pipeline()
    if wsi_metadata:
        print(f"✓ WSI loaded: {wsi_metadata['full_width']}x{wsi_metadata['full_height']}px")
        print(f"✓ Pyramid: {wsi_metadata['num_levels']} levels: {wsi_metadata['sorted_levels']}")
        print(f"✓ Tile size: {wsi_metadata['tile_size']}px")
    
    port = int(os.getenv('PORT1', 8080))
    print(f"Starting server → http://localhost:{port}")
    app.run(debug=True, port=port, host="127.0.0.1")