import boto3
import pydicom
from io import BytesIO
from pathlib import Path

class S3DICOMOrganizer:
    def __init__(self, source_bucket, source_prefix='', target_bucket=None, target_prefix=''):
        """
        Args:
            source_bucket: S3 bucket containing raw DICOM files (e.g., 'my-dicom-bucket')
            source_prefix: Folder path in source bucket (e.g., 'uploads/2024/')
            target_bucket: Where to save organized files (defaults to same bucket)
            target_prefix: Root folder for organized structure (e.g., 'organized')
        """
        self.s3_client = boto3.client('s3')
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix
        self.target_bucket = target_bucket or source_bucket
        self.target_prefix = target_prefix
    
    def organize_by_patient_study_series(self):
        """
        MAIN FUNCTION: Reorganizes DICOM files into this structure:
        
        organized/
        ├── PatientA/
        │   ├── Study_20240101/
        │   │   ├── Series_CT_Chest/
        │   │   │   ├── instance_001.dcm
        │   │   │   └── instance_002.dcm
        │   │   └── Series_CT_Abdomen/
        │   │       └── instance_001.dcm
        │   └── Study_20240115/
        │       └── Series_MRI_Brain/
        │           └── instance_001.dcm
        └── PatientB/
            └── Study_20240110/
                └── Series_XRay_Chest/
                    └── instance_001.dcm
        
        This hierarchy follows DICOM standards:
        - Patient: Individual person
        - Study: Imaging session (e.g., hospital visit)
        - Series: Set of images from one scan (e.g., CT chest scan)
        - Instance: Individual image slice
        """
        dicom_files = self._list_dicom_files()
        print(f"Found {len(dicom_files)} DICOM files to organize")
        
        for s3_key in dicom_files:
            try:
                ds = self._read_dicom_metadata(s3_key)
                patient_id = self._sanitize(ds.get('PatientID', 'Unknown_Patient'))
                study_uid = self._sanitize(ds.get('StudyInstanceUID', 'Unknown_Study'))
                series_uid = self._sanitize(ds.get('SeriesInstanceUID', 'Unknown_Series'))
                sop_instance_uid = self._sanitize(ds.get('SOPInstanceUID', 'Unknown_Instance'))
                
                new_key = f"{self.target_prefix}/{patient_id}/{study_uid}/{series_uid}/{sop_instance_uid}.dcm"
                
                copy_source = {'Bucket': self.source_bucket, 'Key': s3_key}
                self.s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=self.target_bucket,
                    Key=new_key
                )
                
                print(f"Organized: {s3_key} -> {new_key}")
                
            except Exception as e:
                print(f"Error processing {s3_key}: {e}")
        
        print(f"\nOrganization complete in bucket: {self.target_bucket}/{self.target_prefix}")
    
    def _list_dicom_files(self):
        """
        Finds all DICOM files in the source S3 location.
        
        How it identifies DICOM files:
        1. File extension (.dcm, .dicom)
        2. DICOM magic number check (reads bytes 128-131, should be 'DICM')
        
        Uses pagination because S3 limits to 1000 objects per request.
        """
        dicom_files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.source_bucket, Prefix=self.source_prefix):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith(('.dcm', '.dicom', '.DCM', '.DICOM')) or self._is_dicom_file(key):
                    dicom_files.append(key)
        
        return dicom_files
    
    def _is_dicom_file(self, s3_key):
        """
        Verifies if a file is DICOM by checking the magic number.
        
        DICOM files have 'DICM' at bytes 128-131.
        This is the official DICOM standard identifier.
        
        Uses S3 Range request to only download 4 bytes (very efficient).
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.source_bucket,
                Key=s3_key,
                Range='bytes=128-131'  # Only download these 4 bytes
            )
            header = response['Body'].read()
            return header == b'DICM'
        except:
            return False
    
    def _read_dicom_metadata(self, s3_key):
        """
        Downloads and parses DICOM file, but stops before loading pixel data.
        
        Why stop before pixels?
        - Metadata is typically 1-10 KB
        - Pixel data can be 5-500 MB per file
        - We only need metadata for organization
        
        This makes the process 100-1000x faster and cheaper!
        """
        response = self.s3_client.get_object(Bucket=self.source_bucket, Key=s3_key)
        dicom_bytes = response['Body'].read()
        
        ds = pydicom.dcmread(BytesIO(dicom_bytes), stop_before_pixels=True)
        return ds
    
    def _sanitize(self, text):
        """
        Removes characters that are invalid in S3 keys or file paths.
        
        Example: "Patient: John/Doe" becomes "Patient_ John_Doe"
        """
        invalid_chars = '<>:"|?*\\'
        for char in invalid_chars:
            text = text.replace(char, '_')
        return text
    
    def get_organized_structure(self):
        """
        Returns a dictionary showing the organized structure.
        
        Useful for:
        - Viewing what patients/studies/series exist
        - Building a UI to browse medical images
        - Auditing the organization
        
        Returns:
        {
            'Patient123': {
                'Study_001': ['Series_CT', 'Series_MRI'],
                'Study_002': ['Series_XRay']
            },
            'Patient456': {
                'Study_003': ['Series_CT']
            }
        }
        """
        structure = {}
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.target_bucket, Prefix=self.target_prefix):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                parts = key.split('/')
                
                # Parse: organized/PatientID/StudyUID/SeriesUID/file.dcm
                if len(parts) >= 4:
                    patient = parts[-4]
                    study = parts[-3]
                    series = parts[-2]
                    
                    if patient not in structure:
                        structure[patient] = {}
                    if study not in structure[patient]:
                        structure[patient][study] = []
                    if series not in structure[patient][study]:
                        structure[patient][study].append(series)
        
        return structure