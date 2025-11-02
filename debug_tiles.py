#!/usr/bin/env python3
"""Debug script to compare tile indices between wsi_app and api"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Load environment
from dotenv import load_dotenv
load_dotenv()

print("=== Comparing wsi_app.py vs api.py tile building ===\n")

# Test wsi_app tile building
print("1. Testing wsi_app.py tile building...")
from src.pipeline.pipeline import DICOMWSIPipeline
pipeline = DICOMWSIPipeline()

# Build metadata like wsi_app does
from collections import defaultdict
pyramid_levels = defaultdict(list)
image_set_map = {}

for img_set in pipeline.accessor.search_image_sets():
    image_set_id = img_set['imageSetId']
    metadata = pipeline.accessor.get_metadata(image_set_id)

    for series_uid, series_data in metadata.get('Study', {}).get('Series', {}).items():
        series_dicom = series_data.get('DICOM', {})
        level = int(series_dicom.get('SeriesNumber', 0))

        instances = series_data.get('Instances', {})
        per_frame_groups = []
        for inst_uid, inst_data in instances.items():
            per_frame_groups = inst_data.get('DICOM', {}).get('PerFrameFunctionalGroupsSequence', [])
            break

        image_frames = list(instances.values())[0].get('ImageFrames', []) if instances else []

        for idx, frame in enumerate(image_frames):
            col_pos = row_pos = 0
            if idx < len(per_frame_groups):
                plane_seq = per_frame_groups[idx].get("PlanePositionSlideSequence", [])
                if plane_seq:
                    col_pos = plane_seq[0].get("ColumnPositionInTotalImagePixelMatrix", 0)
                    row_pos = plane_seq[0].get("RowPositionInTotalImagePixelMatrix", 0)

            rows = frame.get("MaxPixelValue", 500)
            cols = frame.get("MinPixelValue", 500)

            frame_info = {
                "frame_id": frame["ID"],
                "col_position": col_pos - 1 if col_pos > 0 else 0,
                "row_position": row_pos - 1 if row_pos > 0 else 0,
                "rows": rows,
                "cols": cols
            }
            pyramid_levels[level].append(frame_info)

# Build tile index
tile_index = {}
for level, frames in pyramid_levels.items():
    tile_index[level] = {}
    for frame in frames:
        if frame['rows'] > 0 and frame['cols'] > 0:
            tile_x = frame['col_position'] // frame['cols']
            tile_y = frame['row_position'] // frame['rows']
            tile_index[level][(tile_x, tile_y)] = frame

print(f"Level 0 has {len(tile_index.get(0, {}))} tiles")
if 0 in tile_index:
    tiles_sorted = sorted(tile_index[0].keys())
    print(f"First 10 tiles: {tiles_sorted[:10]}")
    print(f"Last 10 tiles: {tiles_sorted[-10:]}")

    # Check specific tile (0, 0)
    if (0, 0) in tile_index[0]:
        print(f"Tile (0,0) exists!")
    if (245, 0) in tile_index[0]:
        print(f"Tile (245,0) exists!")
    if (245, 80) in tile_index[0]:
        print(f"Tile (245,80) exists!")
