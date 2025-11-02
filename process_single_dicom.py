#!/usr/bin/env python3
"""
Process a single DICOM file through the complete AWS HealthImaging pipeline.

Usage:
    python process_single_dicom.py s3://source-healthimaging-test/IMG-0002-00001.dcm

Or specify local file:
    python process_single_dicom.py /path/to/local/file.dcm
"""

import sys
import os
import boto3
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Import our modules
from HealthImaging import AWSHealthImaging

def process_single_dicom(dicom_path, datastore_id=None, region='us-east-1'):
    """
    Process a single DICOM file through the complete pipeline.

    Steps:
    1. Upload to S3 (if local file)
    2. Organize in S3 by Patient/Study/Series
    3. Import to AWS HealthImaging
    4. Wait for import to complete
    5. Retrieve and display image set info
    """

    print("="*60)
    print("Single DICOM File Processing Pipeline")
    print("="*60)

    # Get configuration
    datastore_id = datastore_id or os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID', 'cd35473aceae45a78d156048f1132089')
    region = os.getenv('AWS_REGION', region)

    # Bucket configuration
    SOURCE_BUCKET = os.getenv('AWS_SOURCE_BUCKET', 'source-healthimaging-test')
    DEST_BUCKET = os.getenv('AWS_DEST_BUCKET', 'target-healthimaging-test')
    OUTPUT_BUCKET = os.getenv('AWS_OUTPUT_BUCKET', 'output-healthimaging-test')
    ROLE_ARN = os.getenv('AWS_HEALTHIMAGING_ROLE_ARN')

    if not ROLE_ARN:
        print("\n⚠️  AWS_HEALTHIMAGING_ROLE_ARN not set!")
        print("Set it in .env file or export it:")
        print("  export AWS_HEALTHIMAGING_ROLE_ARN=arn:aws:iam::ACCOUNT:role/HealthImagingServiceRole")
        return

    print(f"\nConfiguration:")
    print(f"  DICOM: {dicom_path}")
    print(f"  Datastore: {datastore_id}")
    print(f"  Region: {region}")
    print(f"  Source Bucket: {SOURCE_BUCKET}")
    print(f"  Dest Bucket: {DEST_BUCKET}")

    s3_client = boto3.client('s3', region_name=region)

    # ===== STEP 1: Get DICOM file from S3 =====
    if dicom_path.startswith('s3://'):
        # Parse S3 path
        s3_path = dicom_path.replace('s3://', '')
        bucket = s3_path.split('/')[0]
        key = '/'.join(s3_path.split('/')[1:])

        print(f"\n" + "="*60)
        print("STEP 1: DICOM file already in S3")
        print("="*60)
        print(f"  Bucket: {bucket}")
        print(f"  Key: {key}")

        SOURCE_BUCKET = bucket
        source_key = key

    elif os.path.exists(dicom_path):
        # Upload local file
        print(f"\n" + "="*60)
        print("STEP 1: Upload DICOM file to S3")
        print("="*60)

        filename = Path(dicom_path).name
        source_key = f"uploads/{filename}"

        print(f"  Uploading: {dicom_path}")
        print(f"  To: s3://{SOURCE_BUCKET}/{source_key}")

        s3_client.upload_file(dicom_path, SOURCE_BUCKET, source_key)
        print(f"  ✓ Uploaded")

    else:
        print(f"\n✗ Error: File not found: {dicom_path}")
        return

    # ===== STEP 2: Read DICOM metadata =====
    print(f"\n" + "="*60)
    print("STEP 2: Read DICOM metadata")
    print("="*60)

    import pydicom
    from io import BytesIO

    try:
        response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=source_key)
        dicom_bytes = response['Body'].read()
        ds = pydicom.dcmread(BytesIO(dicom_bytes), stop_before_pixels=True)

        patient_id = str(ds.get('PatientID', 'Unknown'))
        study_id = str(ds.get('StudyInstanceUID', 'Unknown'))
        series_id = str(ds.get('SeriesInstanceUID', 'Unknown'))

        print(f"  Patient ID: {patient_id}")
        print(f"  Study UID: {study_id[:40]}...")
        print(f"  Series UID: {series_id[:40]}...")
        print(f"  Modality: {ds.get('Modality', 'Unknown')}")
    except Exception as e:
        print(f"  ✗ Error reading DICOM: {e}")
        return

    # ===== STEP 3: Organize in S3 =====
    print(f"\n" + "="*60)
    print("STEP 3: Organize DICOM in S3")
    print("="*60)

    # Create organized path
    def sanitize(text):
        for char in '<>:"|?*\\':
            text = text.replace(char, '_')
        return text

    organized_key = f"dicoms/{sanitize(patient_id)}/{sanitize(study_id)}/{sanitize(series_id)}/{Path(source_key).name}"

    print(f"  Copying to: s3://{DEST_BUCKET}/{organized_key}")

    try:
        s3_client.copy_object(
            CopySource={'Bucket': SOURCE_BUCKET, 'Key': source_key},
            Bucket=DEST_BUCKET,
            Key=organized_key
        )
        print(f"  ✓ Organized")
    except Exception as e:
        print(f"  ✗ Error organizing: {e}")
        return

    # ===== STEP 4: Import to HealthImaging =====
    print(f"\n" + "="*60)
    print("STEP 4: Import to AWS HealthImaging")
    print("="*60)

    healthimaging = AWSHealthImaging(datastore_id=datastore_id, region_name=region)

    input_s3_uri = f"s3://{DEST_BUCKET}/dicoms/{sanitize(patient_id)}/{sanitize(study_id)}/{sanitize(series_id)}/"
    output_s3_uri = f"s3://{OUTPUT_BUCKET}/import-output/{sanitize(study_id)}/"

    print(f"  Input: {input_s3_uri}")
    print(f"  Output: {output_s3_uri}")
    print(f"  Role: {ROLE_ARN}")

    try:
        job_id = healthimaging.import_dicom_from_s3(
            input_s3_uri=input_s3_uri,
            output_s3_uri=output_s3_uri,
            role_arn=ROLE_ARN
        )

        if not job_id:
            print("  ✗ Failed to start import job")
            return

        print(f"  ✓ Import job started: {job_id}")
    except Exception as e:
        print(f"  ✗ Error starting import: {e}")
        return

    # ===== STEP 5: Wait for import =====
    print(f"\n" + "="*60)
    print("STEP 5: Wait for import to complete")
    print("="*60)

    print("  Waiting (this may take a few minutes)...")
    try:
        success = healthimaging.wait_for_import_completion(
            job_id=job_id,
            check_interval=10,
            max_wait=600
        )

        if not success:
            print("  ✗ Import failed or timed out")
            print(f"\n  Check logs at: {output_s3_uri}")
            return

        print("  ✓ Import completed!")
    except Exception as e:
        print(f"  ✗ Error waiting for import: {e}")
        return

    # ===== STEP 6: Find the image set =====
    print(f"\n" + "="*60)
    print("STEP 6: Find imported image set")
    print("="*60)

    try:
        # Search for the image set
        image_sets = healthimaging.search_image_sets(max_results=50)

        # Find the one that matches our patient/study
        matching_sets = []
        for img_set in image_sets:
            tags = img_set.get('DICOMTags', {})
            if tags.get('DICOMPatientId') == patient_id and tags.get('DICOMStudyInstanceUID') == study_id:
                matching_sets.append(img_set)

        if not matching_sets:
            print("  ⚠️  Could not find matching image set (may take a moment to index)")
            print("  Try searching manually with:")
            print(f"    aws medical-imaging search-image-sets --datastore-id {datastore_id}")
            # Try to get the most recent image set
            if image_sets:
                image_set = image_sets[0]
                print(f"\n  Using most recent image set instead...")
            else:
                return
        else:
            image_set = matching_sets[0]

        image_set_id = image_set['imageSetId']

        print(f"  ✓ Found image set: {image_set_id}")
        print(f"    Created: {image_set.get('createdAt')}")
        print(f"    Version: {image_set.get('version')}")
    except Exception as e:
        print(f"  ✗ Error finding image set: {e}")
        return

    # ===== STEP 7: Get metadata =====
    print(f"\n" + "="*60)
    print("STEP 7: Retrieve image metadata")
    print("="*60)

    try:
        metadata = healthimaging.get_image_set_metadata(image_set_id)

        if metadata:
            # Count frames
            study = metadata.get('Study', {})
            series_dict = study.get('Series', {})

            total_frames = 0
            for series in series_dict.values():
                for instance in series.get('Instances', {}).values():
                    total_frames += len(instance.get('ImageFrames', []))

            print(f"  ✓ Metadata retrieved")
            print(f"    Series: {len(series_dict)}")
            print(f"    Total frames: {total_frames}")
    except Exception as e:
        print(f"  ⚠️  Error retrieving metadata: {e}")

    # ===== COMPLETE =====
    print(f"\n" + "="*60)
    print("✓ PROCESSING COMPLETE!")
    print("="*60)

    print(f"\nImage Set ID: {image_set_id}")
    print(f"Datastore ID: {datastore_id}")

    print(f"\nTo view this image:")
    print(f"  python wsi_app.py")
    print(f"\nThen open: http://localhost:8080")

    return image_set_id


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python process_single_dicom.py <dicom_path>")
        print("\nExamples:")
        print("  python process_single_dicom.py s3://source-healthimaging-test/IMG-0002-00001.dcm")
        print("  python process_single_dicom.py /path/to/local/file.dcm")
        sys.exit(1)

    dicom_path = sys.argv[1]
    process_single_dicom(dicom_path)
