from DicomOrganizer import S3DICOMOrganizer
from HealthImaging import AWSHealthImaging
import time

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
    # Replace these with actual values
    SOURCE_BUCKET = 'arn:aws:s3:::source-healthimaging-test'
    SOURCE_PREFIX = ''
    TARGET_BUCKET = 'arn:aws:s3:::target-healthimaging-test'
    TARGET_PREFIX = ''
    
    IMPORT_OUTPUT_BUCKET = 'arn:aws:s3:::output-healthimaging-test'
    IMPORT_OUTPUT_PREFIX = ''
    
    # IAM role that HealthImaging uses to access S3
    # Must have permissions for S3 read/write and HealthImaging
    HEALTHIMAGING_ROLE_ARN = 'arn:aws:iam::123456789012:role/HealthImagingImportRole'
    
    DATASTORE_NAME = 'test-healthimaging-datastore'
    AWS_REGION = 'us-east-1'
    
    
    # ===== STEP 1: ORGANIZE DICOM FILES =====
    print("\n" + "="*60)
    print("STEP 1: Organizing DICOM files in S3")
    print("="*60)
    
    organizer = S3DICOMOrganizer(
        source_bucket=SOURCE_BUCKET,
        source_prefix=SOURCE_PREFIX,
        target_bucket=TARGET_BUCKET,
        target_prefix=TARGET_PREFIX
    )
    
    # Organize files into Patient/Study/Series hierarchy
    organizer.organize_by_patient_study_series()
    
    # View the organized structure
    print("\n--- Organized Structure ---")
    structure = organizer.get_organized_structure()
    for patient_id, studies in structure.items():
        print(f"\nPatient: {patient_id}")
        for study_id, series_list in studies.items():
            print(f"  Study: {study_id}")
            for series_id in series_list:
                print(f"    Series: {series_id}")
    
    
    # ===== STEP 2: CREATE HEALTHIMAGING DATASTORE =====
    print("\n" + "="*60)
    print("STEP 3: Setting up AWS HealthImaging")
    print("="*60)
    
    healthimaging = AWSHealthImaging(region_name=AWS_REGION)
    
    # List existing datastores
    print("\nListing existing datastores...")
    datastores = healthimaging.list_datastores()
    
    # Create new datastore or use existing
    if datastores:
        # Use first existing datastore
        datastore_id = datastores[0]['datastoreId']
        healthimaging.datastore_id = datastore_id
        print(f"\nUsing existing datastore: {datastore_id}")
    else:
        # Create new datastore
        print(f"\nCreating new datastore: {DATASTORE_NAME}")
        datastore_id = healthimaging.create_datastore(DATASTORE_NAME)
        
        if not datastore_id:
            print("Failed to create datastore. Exiting.")
            return
    
    
    # ===== STEP 3: IMPORT DICOM INTO HEALTHIMAGING =====
    print("\n" + "="*60)
    print("STEP 4: Importing DICOM files into HealthImaging")
    print("="*60)
    
    # Build S3 URIs
    input_s3_uri = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}"
    output_s3_uri = f"s3://{IMPORT_OUTPUT_BUCKET}/{IMPORT_OUTPUT_PREFIX}"
    
    print(f"\nInput location: {input_s3_uri}")
    print(f"Output location: {output_s3_uri}")
    print(f"IAM Role: {HEALTHIMAGING_ROLE_ARN}")
    
    # Start import job
    job_id = healthimaging.import_dicom_from_s3(
        input_s3_uri=input_s3_uri,
        output_s3_uri=output_s3_uri,
        role_arn=HEALTHIMAGING_ROLE_ARN
    )
    
    if not job_id:
        print("Failed to start import job. Exiting.")
        return
    
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
        return
    
    
    # ===== STEP 4: SEARCH FOR IMAGE SETS =====
    print("\n" + "="*60)
    print("STEP 5: Searching for imported image sets")
    print("="*60)
    
    # Search for all image sets
    print("\nSearching for all image sets...")
    image_sets = healthimaging.search_image_sets(max_results=10)
    
    if not image_sets:
        print("No image sets found. Import may have failed.")
        return
    
    # Example: Search by patient ID
    print("\n--- Searching by Patient ID ---")
    patient_filter = {
        'filters': [{
            'values': [{'DICOMPatientId': 'PATIENT001'}],
            'operator': 'EQUAL'
        }]
    }
    patient_image_sets = healthimaging.search_image_sets(filters=patient_filter)
    
    # Example: Search by modality (CT scans)
    print("\n--- Searching for CT scans ---")
    modality_filter = {
        'filters': [{
            'values': [{'DICOMSeriesModality': 'CT'}],
            'operator': 'EQUAL'
        }]
    }
    ct_image_sets = healthimaging.search_image_sets(filters=modality_filter)
    
    
    # ===== STEP 5: RETRIEVE IMAGE SET METADATA =====
    print("\n" + "="*60)
    print("STEP 6: Retrieving detailed metadata")
    print("="*60)
    
    if image_sets:
        # Get metadata for first image set
        first_image_set_id = image_sets[0]['imageSetId']
        
        print(f"\nRetrieving metadata for image set: {first_image_set_id}")
        metadata = healthimaging.get_image_set_metadata(first_image_set_id)
        
        if metadata:
            print("\nMetadata retrieved successfully!")
            print("You can now use this to:")
            print("  - Display study information in a UI")
            print("  - Find specific images to retrieve")
            print("  - Build a DICOM viewer")
    
    
    # ===== STEP 6: RETRIEVE IMAGE FRAME (Optional) =====
    print("\n" + "="*60)
    print("STEP 7: Retrieving image frame (pixel data)")
    print("="*60)
    
    if metadata:
        # Navigate metadata to find an image frame ID
        # This is a simplified example - real implementation would parse the hierarchy
        try:
            patient = metadata.get('Patient', {})
            study = patient.get('Study', [{}])[0]
            series = study.get('Series', [{}])[0]
            instance = series.get('Instances', [{}])[0]
            
            # Construct image frame ID
            series_uid = series.get('DICOM', {}).get('SeriesInstanceUID')
            instance_uid = instance.get('DICOM', {}).get('SOPInstanceUID')
            
            if series_uid and instance_uid:
                image_frame_id = f"{series_uid}/{instance_uid}/1"
                
                print(f"\nRetrieving image frame: {image_frame_id}")
                image_data = healthimaging.get_image_frame(
                    image_set_id=first_image_set_id,
                    image_frame_id=image_frame_id
                )
                
                if image_data:
                    print(f"Successfully retrieved {len(image_data)} bytes of image data")
                    print("Note: Data is in HTJ2K format and needs decoding")
        
        except Exception as e:
            print(f"Could not retrieve image frame: {e}")
    
    
    # ===== COMPLETE =====
    print("\n" + "="*60)
    print("WORKFLOW COMPLETE")
    print("="*60)
    print(f"\nDatastore ID: {datastore_id}")
    print(f"Region: {AWS_REGION}")


def example_read_series():
    """
    Example: Read all images in a series (e.g., CT scan slices)
    """
    print("\n" + "="*60)
    print("EXAMPLE: Reading entire CT series")
    print("="*60)
    
    



if __name__ == "__main__":
    # Run main workflow
    main()
    
    # Uncomment to run examples:
    # example_read_series()
    # example_batch_convert_to_png()