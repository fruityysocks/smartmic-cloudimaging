from HealthImaging import AWSHealthImaging
from src.pipeline.pipeline import DICOMWSIPipeline
import time
import os
import boto3
# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

def main():
    """
    Complete workflow for AWS HealthImaging system.
    
    Steps:
    1. Organize raw DICOM files in S3
    2. Read and process DICOM images
    3. Import into AWS HealthImaging
    4. Search and retrieve images
    """
    
    # ===== CONFIGURATION =====
    SOURCE_BUCKET = os.getenv('AWS_S3_SOURCE_BUCKET', 'source-healthimaging-test')
    SOURCE_PREFIX = os.getenv('AWS_S3_SOURCE_PREFIX', 'dicom-case13')
    OUTPUT_BUCKET = os.getenv('AWS_S3_OUTPUT_BUCKET', 'output-healthimaging-test')
    OUTPUT_PREFIX = os.getenv('AWS_S3_OUTPUT_PREFIX', '')
    HEALTHIMAGING_ROLE_ARN = os.getenv('AWS_HEALTHIMAGING_ROLE_ARN', 'arn:aws:iam::207799300633:role/HealthImagingServiceRole')
    DATASTORE_ID = os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID')
    DATASTORE_NAME = os.getenv('AWS_HEALTHIMAGING_DATASTORE_NAME', 'dicom-wsi-datastore')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    print("\n" + "="*60)
    print("AWS HealthImaging Workflow")
    print("="*60)
    print(f"\nConfiguration:")
    print(f"  Source: s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}")
    print(f"  Output: s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}")
    print(f"  Region: {AWS_REGION}")
    print(f"  Datastore ID: {DATASTORE_ID or 'Not set - will create or find existing'}")
    
    # ===== STEP 1: SETUP HEALTHIMAGING DATASTORE =====
    print("\n" + "="*60)
    print("STEP 1: Setting up AWS HealthImaging")
    print("="*60)
    
    # Initialize boto3 client to list/create datastores
    client = boto3.client('medical-imaging', region_name=AWS_REGION)
    
    datastore_id = None
    
    if DATASTORE_ID:
        # Use configured datastore
        datastore_id = DATASTORE_ID
        print(f"\n✓ Using configured datastore: {datastore_id}")
    else:
        # List existing datastores first
        print("\nNo datastore configured. Listing existing datastores...")
        try:
            response = client.list_datastores()
            datastores = response.get('datastoreSummaries', [])
            
            if datastores:
                # Use first existing datastore
                datastore_id = datastores[0]['datastoreId']
                print(f"\n✓ Found existing datastore: {datastore_id}")
                print(f"  Name: {datastores[0].get('datastoreName', 'Unnamed')}")
                print(f"\n💡 TIP: To use this datastore in future, add to .env:")
                print(f"   AWS_HEALTHIMAGING_DATASTORE_ID={datastore_id}")
            else:
                # Create new datastore
                print(f"\nNo existing datastores found. Creating new datastore: {DATASTORE_NAME}")
                response = client.create_datastore(datastoreName=DATASTORE_NAME)
                datastore_id = response['datastoreId']
                
                if not datastore_id:
                    print("Failed to create datastore. Exiting.")
                    return
                
                print(f"\n✓ Created datastore: {datastore_id}")
                print(f"\n📝 IMPORTANT: Add this to your .env file:")
                print(f"   AWS_HEALTHIMAGING_DATASTORE_ID={datastore_id}")
        
        except Exception as e:
            print(f"Error accessing HealthImaging: {e}")
            return
    
    if not datastore_id:
        print("\n✗ Could not determine datastore ID. Exiting.")
        return
    
    # Now initialize HealthImaging client with datastore_id
    healthimaging = AWSHealthImaging(datastore_id=datastore_id, region_name=AWS_REGION)
    
    # ===== STEP 1.5: CHECK FOR RUNNING IMPORT JOBS =====
    print("\n" + "="*60)
    print("Checking for running import jobs...")
    print("="*60)
    
    try:
        response = client.list_dicom_import_jobs(datastoreId=datastore_id)
        running_jobs = [
            job for job in response.get('jobSummaries', [])
            if job['jobStatus'] in ['SUBMITTED', 'IN_PROGRESS']
        ]
        
        if running_jobs:
            print(f"\n⚠️  Found {len(running_jobs)} running import job(s):")
            for job in running_jobs:
                print(f"  - Job ID: {job['jobId']}")
                print(f"    Status: {job['jobStatus']}")
                print(f"    Started: {job.get('submittedAt', 'Unknown')}")
            
            print("\n❌ Cannot start new import job - AWS HealthImaging quota exceeded")
            print("\nOptions:")
            print("  1. Wait for current jobs to complete (check every few minutes)")
            print("  2. Check job status with: aws medical-imaging list-dicom-import-jobs")
            print("  3. If jobs are stuck, contact AWS Support")
            print("\nSkipping import step...")
            skip_import = True
        else:
            print("\n✓ No running import jobs. Ready to import.")
            skip_import = False
    
    except Exception as e:
        print(f"Warning: Could not check running jobs: {e}")
        skip_import = False
    
    # ===== STEP 2: IMPORT DICOM INTO HEALTHIMAGING =====
    if not skip_import:
        print("\n" + "="*60)
        print("STEP 2: Importing DICOM files into HealthImaging")
        print("="*60)
        
        # Build S3 URIs
        input_s3_uri = f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/"
        output_s3_uri = f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}" if OUTPUT_PREFIX else f"s3://{OUTPUT_BUCKET}/"
        
        print(f"\nInput location: {input_s3_uri}")
        print(f"Output location: {output_s3_uri}")
        print(f"IAM Role: {HEALTHIMAGING_ROLE_ARN}")
        
        try:
            # Start import job
            job_id = healthimaging.import_dicom_from_s3(
                input_s3_uri=input_s3_uri,
                output_s3_uri=output_s3_uri,
                role_arn=HEALTHIMAGING_ROLE_ARN
            )
            
            if not job_id:
                print("Failed to start import job. Continuing with existing data...")
            else:
                # Wait for import to complete
                print("\nWaiting for import to complete...")
                success = healthimaging.wait_for_import_completion(
                    job_id=job_id,
                    check_interval=30,  # Check every 30 seconds
                    max_wait=3600       # Wait up to 1 hour
                )
                
                if not success:
                    print("\nImport did not complete successfully.")
                    print(f"Check import logs at: {output_s3_uri}")
        
        except Exception as e:
            print(f"\n⚠️  Import error: {e}")
            print("Continuing with existing data in datastore...")
    
    # ===== STEP 3: SEARCH FOR IMAGE SETS =====
    print("\n" + "="*60)
    print("STEP 3: Searching for imported image sets")
    print("="*60)
    
    # Search for all image sets
    print("\nSearching for all image sets...")
    image_sets = healthimaging.search_image_sets(max_results=10)
    
    if not image_sets:
        print("No image sets found in datastore.")
        print("\nPossible reasons:")
        print("  - Import hasn't completed yet")
        print("  - Import failed")
        print("  - Datastore is empty")
        return
    
    print(f"\n✓ Found {len(image_sets)} image set(s)")
    for idx, img_set in enumerate(image_sets, 1):
        print(f"\n  {idx}. Image Set ID: {img_set['imageSetId']}")
        if 'DICOM' in img_set:
            dicom = img_set['DICOM']
            print(f"     Patient ID: {dicom.get('DICOMPatientId', 'N/A')}")
            print(f"     Study Date: {dicom.get('DICOMStudyDate', 'N/A')}")
    
    # Example: Search by patient ID (optional)
    print("\n--- Example: Searching by Patient ID ---")
    try:
        patient_filter = {
            'filters': [{
                'values': [{'DICOMPatientId': 'PATIENT001'}],
                'operator': 'EQUAL'
            }]
        }
        patient_image_sets = healthimaging.search_image_sets(filters=patient_filter)
        print(f"Found {len(patient_image_sets)} image sets for PATIENT001")
    except Exception as e:
        print(f"Patient search skipped: {e}")
    
    # ===== STEP 4: RETRIEVE IMAGE SET METADATA =====
    print("\n" + "="*60)
    print("STEP 4: Retrieving detailed metadata")
    print("="*60)
    
    metadata = None
    if image_sets:
        # Get metadata for first image set
        first_image_set_id = image_sets[0]['imageSetId']
        
        print(f"\nRetrieving metadata for image set: {first_image_set_id}")
        try:
            metadata = healthimaging.get_image_set_metadata(first_image_set_id)
            
            if metadata:
                print("\n✓ Metadata retrieved successfully!")
                print("You can now use this to:")
                print("  - Display study information in a UI")
                print("  - Find specific images to retrieve")
                print("  - Build a DICOM viewer")
        except Exception as e:
            print(f"Could not retrieve metadata: {e}")
    
    # ===== STEP 5: RETRIEVE IMAGE FRAME (Optional) =====
    print("\n" + "="*60)
    print("STEP 5: Retrieving image frame (pixel data)")
    print("="*60)
    
    if metadata:
        try:
            # Navigate metadata to find an image frame ID
            patient = metadata.get('Patient', {})
            studies = patient.get('Study', {})
            
            if studies:
                study = next(iter(studies.values()))
                series = next(iter(study.get('Series', {}).values()), None)
                
                if series:
                    instances = series.get('Instances', {})
                    if instances:
                        instance = next(iter(instances.values()))
                        
                        # Get frame ID from ImageFrames
                        frames = instance.get('ImageFrames', [])
                        if frames:
                            frame_id = frames[0]['ID']
                            
                            print(f"\nRetrieving image frame: {frame_id}")
                            image_data = healthimaging.get_image_frame(
                                image_set_id=first_image_set_id,
                                image_frame_id=frame_id
                            )
                            
                            if image_data:
                                print(f"✓ Successfully retrieved {len(image_data)} bytes of image data")
                                print("Note: Data is in HTJ2K format and needs decoding")
                        else:
                            print("No image frames found in metadata")
                    else:
                        print("No instances found in series")
                else:
                    print("No series found in study")
            else:
                print("No studies found in metadata")
        
        except Exception as e:
            print(f"Could not retrieve image frame: {e}")
    
    # ===== COMPLETE =====
    print("\n" + "="*60)
    print("WORKFLOW COMPLETE")
    print("="*60)
    print(f"\nDatastore ID: {datastore_id}")
    print(f"Region: {AWS_REGION}")
    print(f"Image Sets: {len(image_sets)}")
    
    # ===== STEP 6: VIEW IMAGES =====
    print("\n" + "="*60)
    print("STEP 6: View imported images with OpenSeadragon")
    print("="*60)
    
    if image_sets:
        print("\n✓ Images are ready to view!")
        print("\nTo start the WSI viewer, run:")
        print(f"  python wsi_app.py")
        print("\nThen open your browser to: http://localhost:8080")
        print("\nThe viewer will automatically use:")
        print(f"  Datastore ID: {datastore_id}")
        print(f"  Region: {AWS_REGION}")
    else:
        print("\n⚠️  No images available to view yet.")
        print("Complete the import workflow first.")

if __name__ == "__main__":
    # Run main workflow
    main()