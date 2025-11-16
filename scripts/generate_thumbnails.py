#!/usr/bin/env python3
"""
Generate thumbnails for DICOM WSI images stored in AWS HealthImaging.

This script creates thumbnail images by collecting tiles from all image sets
in a datastore and stitching them together at the lowest resolution level.

Usage:
    # Generate thumbnail for a specific datastore
    python generate_thumbnails.py --datastore-id <id> --output thumbnails/

    # Generate thumbnails for all datastores
    python generate_thumbnails.py --all --output thumbnails/

    # Specify thumbnail size
    python generate_thumbnails.py --datastore-id <id> --size 512 --output thumbnails/
"""

import boto3
import os
import json
import gzip
import argparse
import sys
from pathlib import Path
from PIL import Image
import numpy as np
from io import BytesIO
from dotenv import load_dotenv
from collections import defaultdict

# Import our existing decoder
try:
    import openjpeg
except ImportError:
    print("Error: openjpeg not installed. Install with: pip install pylibjpeg-openjpeg")
    sys.exit(1)


def jpeg_decode(data):
    """Decode HTJ2K/JPEG2000 data using openjpeg."""
    return openjpeg.decode(BytesIO(data))


class ThumbnailGenerator:
    """Generate thumbnails for DICOM WSI in AWS HealthImaging."""

    def __init__(self, region='us-east-1'):
        """Initialize the thumbnail generator.

        Args:
            region: AWS region for HealthImaging
        """
        self.region = region
        self.client = boto3.client('medical-imaging', region_name=region)

    def list_datastores(self):
        """List all available datastores.

        Returns:
            List of datastore summaries
        """
        try:
            response = self.client.list_datastores()
            return response.get('datastoreSummaries', [])
        except Exception as e:
            print(f"Error listing datastores: {e}")
            return []

    def get_image_sets(self, datastore_id):
        """Get all image sets in a datastore.

        Args:
            datastore_id: AWS HealthImaging datastore ID

        Returns:
            List of image set IDs
        """
        try:
            response = self.client.search_image_sets(
                datastoreId=datastore_id,
                searchCriteria={}
            )
            return response.get('imageSetsMetadataSummaries', [])
        except Exception as e:
            print(f"Error getting image sets: {e}")
            return []

    def get_metadata(self, datastore_id, image_set_id):
        """Get metadata for an image set.

        Args:
            datastore_id: Datastore ID
            image_set_id: Image set ID

        Returns:
            Parsed metadata dictionary
        """
        try:
            response = self.client.get_image_set_metadata(
                datastoreId=datastore_id,
                imageSetId=image_set_id
            )

            blob = response['imageSetMetadataBlob'].read()
            if response.get('contentEncoding') == 'gzip':
                blob = gzip.decompress(blob)

            return json.loads(blob)
        except Exception as e:
            print(f"Error getting metadata: {e}")
            return None

    def decode_frame(self, datastore_id, image_set_id, frame_id):
        """Decode a single frame from AWS HealthImaging.

        Args:
            datastore_id: Datastore ID
            image_set_id: Image set ID
            frame_id: Frame ID to decode

        Returns:
            Numpy array of decoded image (RGB)
        """
        try:
            response = self.client.get_image_frame(
                datastoreId=datastore_id,
                imageSetId=image_set_id,
                imageFrameInformation={'imageFrameId': frame_id}
            )

            data = response['imageFrameBlob'].read()
            img_array = jpeg_decode(data)

            # Convert grayscale to RGB if needed
            if len(img_array.shape) == 2:
                img_array = np.stack([img_array] * 3, axis=-1)

            # Ensure uint8
            if img_array.dtype != np.uint8:
                img_array = img_array.astype(np.uint8)

            return img_array

        except Exception as e:
            print(f"Error decoding frame {frame_id}: {e}")
            return None

    def collect_all_tiles_at_level(self, datastore_id, target_series_number):
        """Collect all tiles from all image sets at a specific pyramid level.

        Args:
            datastore_id: Datastore ID
            target_series_number: Series number to use (pyramid level)

        Returns:
            Dictionary with tile information and metadata
        """
        image_sets = self.get_image_sets(datastore_id)

        if not image_sets:
            print("  ✗ No image sets found")
            return None

        print(f"  Found {len(image_sets)} image sets in datastore")

        all_tiles = {}  # {(tile_x, tile_y): (image_set_id, frame_id)}
        tile_size = None
        total_cols = None
        total_rows = None

        for img_set in image_sets:
            image_set_id = img_set['imageSetId']

            # Get metadata
            metadata = self.get_metadata(datastore_id, image_set_id)
            if not metadata:
                continue

            study = metadata.get('Study', {})
            all_series = study.get('Series', {})

            # Find the target series in this image set
            for series_uid, series_data in all_series.items():
                series_dicom = series_data.get('DICOM', {})
                series_num_str = series_dicom.get('SeriesNumber', '0').strip()

                try:
                    series_num = int(series_num_str)
                except:
                    series_num = 0

                if series_num != target_series_number:
                    continue

                # Found the right series - process its tiles
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

                        # Convert to tile coordinates
                        tile_x = col_pos // tile_size
                        tile_y = row_pos // tile_size

                        all_tiles[(tile_x, tile_y)] = (image_set_id, frame['ID'])

        if not all_tiles:
            print(f"  ✗ No tiles found at series {target_series_number}")
            return None

        print(f"  Collected {len(all_tiles)} tiles from series {target_series_number}")

        return {
            'tiles': all_tiles,
            'tile_size': tile_size,
            'total_cols': total_cols,
            'total_rows': total_rows
        }

    def generate_thumbnail(self, datastore_id, output_path, thumbnail_size=512, max_tiles=None):
        """Generate thumbnail for a datastore.

        Args:
            datastore_id: AWS HealthImaging datastore ID
            output_path: Path to save thumbnail image
            thumbnail_size: Maximum dimension of thumbnail (will maintain aspect ratio)
            max_tiles: Maximum tiles to process (None = process all tiles)

        Returns:
            True if successful, False otherwise
        """
        print(f"\nGenerating thumbnail for datastore: {datastore_id}")

        # Try from lowest to highest resolution until we find enough tiles
        # Series 6 = lowest resolution, Series 0 = highest resolution
        for series_num in range(6, -1, -1):
            print(f"\n  Trying series {series_num}...")

            tile_data = self.collect_all_tiles_at_level(datastore_id, series_num)

            if not tile_data or len(tile_data['tiles']) == 0:
                continue

            # If we have enough tiles, use this level
            if len(tile_data['tiles']) >= 10:  # Require at least 10 tiles
                break
        else:
            print("  ✗ Could not find sufficient tiles at any resolution level")
            return False

        tiles = tile_data['tiles']
        tile_size = tile_data['tile_size']
        total_cols = tile_data['total_cols']
        total_rows = tile_data['total_rows']

        print(f"  Using series {series_num} with {len(tiles)} tiles")
        print(f"  Tile size: {tile_size}x{tile_size}")
        print(f"  Full image dimensions: {total_cols}x{total_rows}")

        # Calculate expected grid size based on full image dimensions
        expected_cols = (total_cols + tile_size - 1) // tile_size
        expected_rows = (total_rows + tile_size - 1) // tile_size
        expected_tiles = expected_cols * expected_rows

        print(f"  Expected grid: {expected_cols}x{expected_rows} tiles ({expected_tiles} total)")
        print(f"  Available tiles: {len(tiles)} ({len(tiles)/expected_tiles*100:.1f}% coverage)")

        # Limit number of tiles if specified
        if max_tiles and len(tiles) > max_tiles:
            print(f"  Limiting to {max_tiles} tiles")
            tiles_to_process = dict(list(tiles.items())[:max_tiles])
        else:
            tiles_to_process = tiles

        # Find actual grid bounds from available tiles
        min_col = min(x for x, y in tiles_to_process.keys())
        min_row = min(y for x, y in tiles_to_process.keys())
        max_col = max(x for x, y in tiles_to_process.keys())
        max_row = max(y for x, y in tiles_to_process.keys())

        grid_width = max_col - min_col + 1
        grid_height = max_row - min_row + 1

        print(f"  Processing {len(tiles_to_process)} tiles in {grid_width}x{grid_height} grid")

        # Create output image sized to full dimensions (not just available tiles)
        # This ensures proper aspect ratio
        output_width = total_cols
        output_height = total_rows
        output_image = Image.new('RGB', (output_width, output_height), color=(240, 240, 240))

        # Decode and place each tile at its absolute position
        decoded_count = 0
        for (tile_x, tile_y), (image_set_id, frame_id) in tiles_to_process.items():
            img_array = self.decode_frame(datastore_id, image_set_id, frame_id)

            if img_array is not None:
                tile_img = Image.fromarray(img_array, mode='RGB')

                # Calculate absolute position based on tile coordinates
                x_offset = tile_x * tile_size
                y_offset = tile_y * tile_size

                # Ensure we don't go out of bounds
                if x_offset < output_width and y_offset < output_height:
                    output_image.paste(tile_img, (x_offset, y_offset))
                    decoded_count += 1

                    if decoded_count % 10 == 0:
                        print(f"    Decoded {decoded_count}/{len(tiles_to_process)} tiles")

        print(f"  ✓ Stitched {decoded_count} tiles into {output_width}x{output_height} image")

        # Resize to target thumbnail size while maintaining aspect ratio
        output_image.thumbnail((thumbnail_size, thumbnail_size), Image.Resampling.LANCZOS)

        # Save thumbnail
        output_image.save(output_path, 'JPEG', quality=85)
        print(f"  ✓ Saved thumbnail: {output_path}")
        print(f"    Original size: {output_width}x{output_height}")
        print(f"    Thumbnail size: {output_image.size[0]}x{output_image.size[1]}")

        return True


def main():
    parser = argparse.ArgumentParser(
        description='Generate thumbnails for DICOM WSI in AWS HealthImaging',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--datastore-id',
        help='Specific datastore ID to process'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all datastores'
    )
    parser.add_argument(
        '--output', '-o',
        default='thumbnails',
        help='Output directory for thumbnails (default: thumbnails/)'
    )
    parser.add_argument(
        '--size', '-s',
        type=int,
        default=512,
        help='Thumbnail size (max dimension, default: 512)'
    )
    parser.add_argument(
        '--max-tiles',
        type=int,
        default=None,
        help='Maximum tiles to process per thumbnail (default: None = all tiles)'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )

    args = parser.parse_args()

    # Load environment if available
    load_dotenv()

    # Check if we have either datastore-id or --all
    if not args.datastore_id and not args.all:
        parser.print_help()
        print("\nError: Must specify either --datastore-id or --all")
        sys.exit(1)

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize generator
    generator = ThumbnailGenerator(region=args.region)

    if args.all:
        # Process all datastores
        print("Listing all datastores...")
        datastores = generator.list_datastores()

        if not datastores:
            print("No datastores found")
            sys.exit(1)

        print(f"Found {len(datastores)} datastore(s)")

        success_count = 0
        for datastore in datastores:
            datastore_id = datastore['datastoreId']
            datastore_name = datastore.get('datastoreName', 'unnamed')

            output_path = output_dir / f"{datastore_id}.jpg"

            if generator.generate_thumbnail(datastore_id, str(output_path), args.size, args.max_tiles):
                success_count += 1

        print(f"\n✓ Generated {success_count}/{len(datastores)} thumbnails")

    else:
        # Process single datastore
        output_path = output_dir / f"{args.datastore_id}.jpg"

        if generator.generate_thumbnail(args.datastore_id, str(output_path), args.size, args.max_tiles):
            print("\n✓ Thumbnail generation complete")
        else:
            print("\n✗ Thumbnail generation failed")
            sys.exit(1)


if __name__ == '__main__':
    main()
