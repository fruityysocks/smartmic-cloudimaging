#!/usr/bin/env python3
"""
Check what images are in the HealthImaging datastore.
"""

import boto3
import os
import json

print("="*60)
print("HealthImaging Datastore Image Check")
print("="*60)

# Get configuration
datastore_id = os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID', 'cd35473aceae45a78d156048f1132089')
region = os.getenv('AWS_REGION', 'us-east-1')

print(f"\nDatastore ID: {datastore_id}")
print(f"Region: {region}")

# Initialize client
try:
    client = boto3.client('medical-imaging', region_name=region)
    print("\n✓ AWS HealthImaging client initialized")
except Exception as e:
    print(f"\n❌ Failed to initialize client: {e}")
    exit(1)

# Check datastore exists
print("\n" + "="*60)
print("Step 1: Verify Datastore")
print("="*60)

try:
    datastore = client.get_datastore(datastoreId=datastore_id)
    print(f"\n✓ Datastore exists")
    print(f"  Name: {datastore['datastoreProperties'].get('datastoreName', 'Unnamed')}")
    print(f"  Status: {datastore['datastoreProperties']['datastoreStatus']}")
    print(f"  Created: {datastore['datastoreProperties'].get('createdAt', 'Unknown')}")
except Exception as e:
    print(f"\n❌ Cannot access datastore: {e}")
    print("\nPossible issues:")
    print("  1. Datastore ID is incorrect")
    print("  2. Missing IAM permissions")
    print("  3. Wrong region")
    exit(1)

# Search for image sets
print("\n" + "="*60)
print("Step 2: Search for Image Sets")
print("="*60)

try:
    print("\nSearching for all image sets...")
    response = client.search_image_sets(
        datastoreId=datastore_id
    )

    image_sets = response.get('imageSetsMetadataSummaries', [])

    if not image_sets:
        print("\n❌ NO IMAGE SETS FOUND in datastore!")
        print("\nThis means:")
        print("  - No DICOM files have been imported yet")
        print("  - OR all import jobs failed")
        print("\nTo import DICOM files:")
        print("  1. Ensure DICOM files are in S3")
        print("  2. Run: python main.py")
        print("  3. Wait for import job to complete")

        # Check for recent import jobs
        print("\n" + "="*60)
        print("Checking Import Jobs")
        print("="*60)

        try:
            jobs_response = client.list_dicom_import_jobs(
                datastoreId=datastore_id
            )
            jobs = jobs_response.get('jobSummaries', [])

            if jobs:
                print(f"\nFound {len(jobs)} import job(s):")
                for job in jobs[:5]:  # Show last 5
                    print(f"\n  Job ID: {job['jobId']}")
                    print(f"  Status: {job['jobStatus']}")
                    print(f"  Submitted: {job.get('submittedAt', 'Unknown')}")

                    if job['jobStatus'] == 'FAILED':
                        # Get detailed job info
                        job_detail = client.get_dicom_import_job(
                            datastoreId=datastore_id,
                            jobId=job['jobId']
                        )
                        print(f"  ❌ Error: {job_detail['jobProperties'].get('message', 'Unknown error')}")
            else:
                print("\nNo import jobs found. You need to import DICOM files first.")

        except Exception as e:
            print(f"\n⚠️  Cannot list import jobs: {e}")

        exit(1)

    print(f"\n✓ Found {len(image_sets)} image set(s)!")

    # Display details of each image set
    print("\n" + "="*60)
    print("Image Set Details")
    print("="*60)

    for idx, img_set in enumerate(image_sets, 1):
        print(f"\n{idx}. Image Set ID: {img_set['imageSetId']}")
        print(f"   Version: {img_set.get('version', 'N/A')}")
        print(f"   Created: {img_set.get('createdAt', 'Unknown')}")
        print(f"   Updated: {img_set.get('updatedAt', 'Unknown')}")

        # Show DICOM tags if available
        dicom_tags = img_set.get('DICOMTags', {})
        if dicom_tags:
            print(f"   DICOM Tags:")
            if 'DICOMPatientId' in dicom_tags:
                print(f"     Patient ID: {dicom_tags['DICOMPatientId']}")
            if 'DICOMStudyDate' in dicom_tags:
                print(f"     Study Date: {dicom_tags['DICOMStudyDate']}")
            if 'DICOMStudyDescription' in dicom_tags:
                print(f"     Description: {dicom_tags['DICOMStudyDescription']}")

        # Get detailed metadata for first image set
        if idx == 1:
            print(f"\n   Fetching detailed metadata...")
            try:
                metadata_response = client.get_image_set_metadata(
                    datastoreId=datastore_id,
                    imageSetId=img_set['imageSetId']
                )

                # Read and parse metadata
                metadata_blob = metadata_response['imageSetMetadataBlob'].read()

                # Check if gzipped
                if metadata_response.get('contentEncoding') == 'gzip':
                    import gzip
                    metadata_blob = gzip.decompress(metadata_blob)

                metadata = json.loads(metadata_blob)

                # Count frames
                study = metadata.get('Study', {})
                series_dict = study.get('Series', {})

                total_frames = 0
                for series_id, series_data in series_dict.items():
                    instances = series_data.get('Instances', {})
                    for instance_id, instance_data in instances.items():
                        frames = instance_data.get('ImageFrames', [])
                        total_frames += len(frames)

                print(f"   ✓ Total frames: {total_frames}")
                print(f"   ✓ Series count: {len(series_dict)}")

            except Exception as e:
                print(f"   ⚠️  Could not fetch metadata: {e}")

    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    print(f"\n✓ Datastore has {len(image_sets)} image set(s)")
    print(f"✓ Images are available for viewing")
    print("\nTo view these images:")
    print("  python wsi_app.py")
    print("  # Then open http://localhost:8080")

except Exception as e:
    print(f"\n❌ Error searching image sets: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
