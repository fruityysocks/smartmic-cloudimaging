#!/usr/bin/env python3
"""
Script to read DICOM metadata from a .dcm file stored in S3.

Usage:
    python read_dcm_from_s3.py s3://bucket-name/path/to/file.dcm
    python read_dcm_from_s3.py --bucket bucket-name --key path/to/file.dcm
"""

import boto3
import pydicom
from io import BytesIO
import argparse
import sys
import json


def read_dcm_metadata_from_s3(bucket_name, object_key, region='us-east-1'):
    """
    Read DICOM metadata from S3.

    Args:
        bucket_name: S3 bucket name
        object_key: S3 object key (path to .dcm file)
        region: AWS region (default: us-east-1)

    Returns:
        pydicom.Dataset object
    """
    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=region)

    print(f"Reading DICOM file from S3...")
    print(f"  Bucket: {bucket_name}")
    print(f"  Key: {object_key}")

    try:
        # Download file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_data = response['Body'].read()

        print(f"  Size: {len(file_data):,} bytes")

        # Read DICOM from bytes
        dcm = pydicom.dcmread(BytesIO(file_data))

        return dcm

    except s3_client.exceptions.NoSuchKey:
        print(f"Error: File not found in S3: s3://{bucket_name}/{object_key}")
        return None
    except s3_client.exceptions.NoSuchBucket:
        print(f"Error: Bucket not found: {bucket_name}")
        return None
    except Exception as e:
        print(f"Error reading DICOM file: {e}")
        return None


def print_dcm_metadata(dcm, verbose=False):
    """
    Print DICOM metadata in a readable format.

    Args:
        dcm: pydicom.Dataset object
        verbose: If True, print all tags. If False, print summary only.
    """
    print("\n" + "="*60)
    print("DICOM METADATA")
    print("="*60)

    # Key metadata fields for WSI
    key_fields = {
        'PatientName': 'Patient Name',
        'PatientID': 'Patient ID',
        'StudyDate': 'Study Date',
        'StudyTime': 'Study Time',
        'Modality': 'Modality',
        'Manufacturer': 'Manufacturer',
        'SeriesNumber': 'Series Number',
        'SeriesDescription': 'Series Description',
        'Rows': 'Tile Rows',
        'Columns': 'Tile Columns',
        'TotalPixelMatrixRows': 'Total Rows',
        'TotalPixelMatrixColumns': 'Total Columns',
        'NumberOfFrames': 'Number of Frames',
        'SamplesPerPixel': 'Samples Per Pixel',
        'PhotometricInterpretation': 'Photometric Interpretation',
        'BitsAllocated': 'Bits Allocated',
        'BitsStored': 'Bits Stored',
        'PixelSpacing': 'Pixel Spacing',
        'ImagedVolumeWidth': 'Imaged Volume Width',
        'ImagedVolumeHeight': 'Imaged Volume Height',
        'LossyImageCompression': 'Lossy Compression',
        'LossyImageCompressionRatio': 'Compression Ratio',
        'TransferSyntaxUID': 'Transfer Syntax UID',
    }

    print("\n--- Key Metadata ---")
    for tag, label in key_fields.items():
        if hasattr(dcm, tag):
            value = getattr(dcm, tag)
            print(f"{label:30s}: {value}")

    # Print tile information if available
    if hasattr(dcm, 'Rows') and hasattr(dcm, 'Columns'):
        print(f"\n--- Tile Information ---")
        print(f"Tile Size: {dcm.Columns} × {dcm.Rows} pixels")

        if hasattr(dcm, 'TotalPixelMatrixColumns') and hasattr(dcm, 'TotalPixelMatrixRows'):
            total_cols = dcm.TotalPixelMatrixColumns
            total_rows = dcm.TotalPixelMatrixRows
            tile_cols = dcm.Columns
            tile_rows = dcm.Rows

            num_tiles_wide = (total_cols + tile_cols - 1) // tile_cols
            num_tiles_tall = (total_rows + tile_rows - 1) // tile_rows
            total_tiles = num_tiles_wide * num_tiles_tall

            print(f"Total Image: {total_cols} × {total_rows} pixels")
            print(f"Tiles: {num_tiles_wide} × {num_tiles_tall} = {total_tiles:,} tiles")

    # Print all tags if verbose
    if verbose:
        print("\n--- All DICOM Tags ---")
        print(dcm)

    print("\n" + "="*60)


def export_to_json(dcm, output_file):
    """
    Export DICOM metadata to JSON file.

    Args:
        dcm: pydicom.Dataset object
        output_file: Output JSON file path
    """
    # Convert DICOM to dictionary
    metadata = {}

    for elem in dcm:
        try:
            # Skip binary data (pixel data, etc)
            if elem.VR in ['OB', 'OW', 'UN']:
                metadata[elem.keyword] = f"<Binary data, {len(elem.value)} bytes>"
            else:
                metadata[elem.keyword] = str(elem.value)
        except:
            metadata[elem.keyword] = "<Unable to convert>"

    # Write to JSON
    with open(output_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\n✓ Metadata exported to: {output_file}")


def parse_s3_uri(s3_uri):
    """
    Parse S3 URI into bucket and key.

    Args:
        s3_uri: S3 URI in format s3://bucket/key

    Returns:
        Tuple of (bucket, key)
    """
    if not s3_uri.startswith('s3://'):
        raise ValueError("S3 URI must start with s3://")

    # Remove s3:// prefix
    path = s3_uri[5:]

    # Split into bucket and key
    parts = path.split('/', 1)
    if len(parts) == 1:
        return parts[0], ''
    else:
        return parts[0], parts[1]


def main():
    parser = argparse.ArgumentParser(
        description='Read DICOM metadata from S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using S3 URI
  python read_dcm_from_s3.py s3://my-bucket/dicom/image.dcm

  # Using bucket and key
  python read_dcm_from_s3.py --bucket my-bucket --key dicom/image.dcm

  # Verbose output
  python read_dcm_from_s3.py s3://my-bucket/dicom/image.dcm --verbose

  # Export to JSON
  python read_dcm_from_s3.py s3://my-bucket/dicom/image.dcm --json output.json
        """
    )

    parser.add_argument(
        's3_uri',
        nargs='?',
        help='S3 URI (s3://bucket/key)'
    )
    parser.add_argument(
        '--bucket',
        help='S3 bucket name'
    )
    parser.add_argument(
        '--key',
        help='S3 object key (path to .dcm file)'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print all DICOM tags'
    )
    parser.add_argument(
        '--json',
        metavar='FILE',
        help='Export metadata to JSON file'
    )

    args = parser.parse_args()

    # Determine bucket and key
    if args.s3_uri:
        try:
            bucket, key = parse_s3_uri(args.s3_uri)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
    elif args.bucket and args.key:
        bucket = args.bucket
        key = args.key
    else:
        parser.print_help()
        sys.exit(1)

    # Read DICOM from S3
    dcm = read_dcm_metadata_from_s3(bucket, key, args.region)

    if dcm is None:
        sys.exit(1)

    # Print metadata
    print_dcm_metadata(dcm, verbose=args.verbose)

    # Export to JSON if requested
    if args.json:
        export_to_json(dcm, args.json)

    print("\n✓ Done!")


if __name__ == '__main__':
    main()
