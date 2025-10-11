import boto3
import json
import time

class AWSHealthImaging:
    def __init__(self, datastore_id, region_name='us-east-1'):
        self.client = boto3.client('medical-imaging', region_name=region_name)
        self.datastore_id = datastore_id
        self.region_name = region_name
    
    def create_datastore(self, datastore_name):
        try:
            response = self.client.create_datastore(
                datastoreName=datastore_name
            )
            print(f"Created datastore: {response['datastoreId']}")
            return response['datastoreId']
        except Exception as e:
            print(f"Error creating datastore: {e}")
            return None
    
    def import_dicom_from_s3(self, input_s3_uri, output_s3_uri, role_arn):
        """    
        Args:
            input_s3_uri: s3://bucket/prefix/ where DICOM files are located
            output_s3_uri: s3://bucket/prefix/ for import job output
            role_arn: IAM role ARN with permissions to access S3 and HealthImaging
        """
        try:
            response = self.client.start_dicom_import_job(
                datastoreId=self.datastore_id,
                inputS3Uri=input_s3_uri,
                outputS3Uri=output_s3_uri,
                dataAccessRoleArn=role_arn
            )
            
            job_id = response['jobId']
            print(f"Started import job: {job_id}")
            return job_id
            
        except Exception as e:
            print(f"Error starting import job: {e}")
            return None
    
    def get_import_job_status(self, job_id):
        try:
            response = self.client.get_dicom_import_job(
                datastoreId=self.datastore_id,
                jobId=job_id
            )
            
            status = response['jobProperties']['jobStatus']
            print(f"Job {job_id} status: {status}")
            
            if 'message' in response['jobProperties']:
                print(f"Message: {response['jobProperties']['message']}")
            
            return response['jobProperties']
            
        except Exception as e:
            print(f"Error getting job status: {e}")
            return None
    
    def wait_for_import_completion(self, job_id, check_interval=30, max_wait=3600):
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            job_props = self.get_import_job_status(job_id)
            
            if not job_props:
                return False
            
            status = job_props['jobStatus']
            
            if status == 'COMPLETED':
                print("Import job completed successfully!")
                return True
            elif status in ['FAILED', 'COMPLETED_WITH_ERRORS']:
                print(f"Import job ended with status: {status}")
                return False
            
            print(f"Waiting... (status: {status})")
            time.sleep(check_interval)
        
        print("Max wait time exceeded")
        return False
    
    def search_studies(self, filters=None, max_results=50):
        """
        Searches for image sets (studies) in the datastore.
        
        This is how you find specific medical images after import.
        
        Filter examples:
        
        1. Find by patient ID:
        {
            'filters': [{
                'values': [{'DICOMPatientId': 'PATIENT123'}],
                'operator': 'EQUAL'
            }]
        }
        
        2. Find by date range:
        {
            'filters': [{
                'values': [
                    {'DICOMStudyDateAndTime': {'DICOMStudyDate': '20240101'}},
                    {'DICOMStudyDateAndTime': {'DICOMStudyDate': '20240131'}}
                ],
                'operator': 'BETWEEN'
            }]
        }
        
        3. Find by modality (CT, MRI, XR, etc.):
        {
            'filters': [{
                'values': [{'DICOMSeriesModality': 'CT'}],
                'operator': 'EQUAL'
            }]
        }
        
        Returns list of image sets with metadata.
        """
        try:
            params = {
                'datastoreId': self.datastore_id,
                'maxResults': max_results
            }
            
            if filters:
                params['searchCriteria'] = filters
            
            response = self.client.search_image_sets(**params)
            
            image_sets = response.get('imageSetsMetadataSummaries', [])
            
            print(f"\nFound {len(image_sets)} image set(s):")
            for img_set in image_sets:
                print(f"\n  Image Set ID: {img_set['imageSetId']}")
                print(f"  Version: {img_set.get('version', 'N/A')}")
                
                # Display DICOM metadata if available
                if 'DICOMTags' in img_set:
                    tags = img_set['DICOMTags']
                    print(f"  Patient ID: {tags.get('DICOMPatientId', 'N/A')}")
                    print(f"  Study Date: {tags.get('DICOMStudyDate', 'N/A')}")
                    print(f"  Modality: {tags.get('DICOMStudyDescription', 'N/A')}")
            
            return image_sets
            
        except Exception as e:
            print(f"Error searching image sets: {e}")
            return []
    
    def get_image_set_metadata(self, image_set_id, version_id=None):
        """
        Retrieves detailed metadata for a specific image set.
        """
        try:
            params = {
                'datastoreId': self.datastore_id,
                'imageSetId': image_set_id
            }
            
            if version_id:
                params['versionId'] = version_id
            
            response = self.client.get_image_set_metadata(**params)
            
            metadata_blob = response['imageSetMetadataBlob']
            metadata = json.loads(metadata_blob.read())
            
            print(f"\n=== Image Set Metadata ===")
            print(f"Image Set ID: {image_set_id}")
            
            if 'Patient' in metadata:
                patient = metadata['Patient']
                print(f"\nPatient ID: {patient.get('DICOM', {}).get('PatientID', 'N/A')}")
                
                for study in patient.get('Study', []):
                    study_dicom = study.get('DICOM', {})
                    print(f"\n  Study:")
                    print(f"    Date: {study_dicom.get('StudyDate', 'N/A')}")
                    print(f"    Description: {study_dicom.get('StudyDescription', 'N/A')}")
                    
                    for series in study.get('Series', []):
                        series_dicom = series.get('DICOM', {})
                        print(f"\n    Series:")
                        print(f"      Modality: {series_dicom.get('Modality', 'N/A')}")
                        print(f"      Description: {series_dicom.get('SeriesDescription', 'N/A')}")
                        
                        instances = series.get('Instances', [])
                        print(f"      Instances: {len(instances)} images")
            
            return metadata
            
        except Exception as e:
            print(f"Error getting image set metadata: {e}")
            return None
    
    def get_image_frame(self, image_set_id, image_frame_id):
        """
        Downloads actual pixel data for a specific image frame.
        
        This retrieves the medical image itself (not just metadata).
        
        Args:
            image_set_id: The image set containing the frame
            image_frame_id: Specific frame identifier from metadata
                           Format: series_uid/instance_uid/frame_number
        
        Returns:
            Binary image data (HTJ2K compressed)
            You'll need to decode this with an HTJ2K decoder
        """
        try:
            response = self.client.get_image_frame(
                datastoreId=self.datastore_id,
                imageSetId=image_set_id,
                imageFrameInformation={
                    'imageFrameId': image_frame_id
                }
            )
            
            image_blob = response['imageFrameBlob']
            image_data = image_blob.read()
            
            print(f"Retrieved image frame: {len(image_data)} bytes")
            print(f"Content type: {response.get('contentType', 'N/A')}")
            
            return image_data
            
        except Exception as e:
            print(f"Error getting image frame: {e}")
            return None
    
    def delete_image_set(self, image_set_id):
        """
            Deletes an image set from the datastore.
            
            WARNING: This permanently deletes the medical images.
            Use with caution in production systems.
        """
        try:
            response = self.client.delete_image_set(
                datastoreId=self.datastore_id,
                imageSetId=image_set_id
            )
            
            print(f"Deleted image set: {image_set_id}")
            print(f"Status: {response['imageSetState']}")
            return True
            
        except Exception as e:
            print(f"Error deleting image set: {e}")
            return False
    
    def delete_datastore(self, datastore_id=None):
        """
            Deletes an entire datastore.
                
            WARNING: This deletes ALL medical images in the datastore.
            The datastore must be empty first (delete all image sets).
        """
        ds_id = datastore_id or self.datastore_id
        
        try:
            response = self.client.delete_datastore(
                datastoreId=ds_id
            )
            
            print(f"Deleted datastore: {ds_id}")
            print(f"Status: {response['datastoreStatus']}")
            return True
            
        except Exception as e:
            print(f"Error deleting datastore: {e}")
            print("Note: Datastore must be empty before deletion")
            return False