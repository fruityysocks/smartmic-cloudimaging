import boto3
import os
import json
import logging
import gzip
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pydicom
from collections import defaultdict
from datetime import datetime
import numpy as np
from io import BytesIO

# Import AWS HealthImaging wrapper
from .healthimaging import AWSHealthImaging
from .config import AWSConfig

try:
    from pylibjpeg import decode as jpeg_decode
    HAS_PYLIBJPEG = True
except ImportError:
    HAS_PYLIBJPEG = False
    logger = logging.getLogger(__name__)
    logger.warning("pylibjpeg not available. HTJ2K decoding will not work.")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DICOMWSIPipeline:
    def __init__(
        self,
        source_bucket: Optional[str] = None,
        dest_bucket: Optional[str] = None,
        datastore_id: Optional[str] = None,
        region: str = 'us-east-1',
        config: Optional[AWSConfig] = None
    ):
        """
        Initialize DICOM WSI Pipeline with AWS HealthImaging integration.

        Args:
            source_bucket: S3 bucket for source DICOM files (optional if using config)
            dest_bucket: S3 bucket for processed files (optional if using config)
            datastore_id: AWS HealthImaging datastore ID (optional if using config)
            region: AWS region
            config: AWSConfig instance (if provided, overrides other parameters)
        """
        # Use config if provided, otherwise create from parameters
        if config:
            self.config = config
            self.source_bucket = config.source_bucket or source_bucket
            self.dest_bucket = config.dest_bucket or dest_bucket
            self.datastore_id = config.datastore_id or datastore_id
            self.region = config.region or region
        else:
            self.source_bucket = source_bucket
            self.dest_bucket = dest_bucket
            self.datastore_id = datastore_id
            self.region = region
            self.config = AWSConfig(
                datastore_id=datastore_id,
                region=region,
                source_bucket=source_bucket,
                dest_bucket=dest_bucket
            )

        # Initialize AWS clients
        self.s3_client = boto3.client('s3', region_name=self.region)

        # Initialize HealthImaging wrapper (accessible as 'accessor' for compatibility)
        if self.datastore_id:
            self.accessor = AWSHealthImaging(
                datastore_id=self.datastore_id,
                region_name=self.region
            )
        else:
            self.accessor = None
            logger.warning("No datastore_id provided. HealthImaging operations will not be available.")
        
    def sort_and_upload_dicoms(self, local_dicom_dir: str) -> Dict[str, List[str]]:
        """
        Sort DICOM files by StudyID/SeriesID and upload to S3.
        Returns mapping of series keys to uploaded file paths.
        """
        logger.info(f"Processing DICOM files from {local_dicom_dir}")
        
        # Group files by StudyID/SeriesID
        series_files = defaultdict(list)
        
        for dicom_file in Path(local_dicom_dir).rglob('*.dcm'):
            try:
                ds = pydicom.dcmread(str(dicom_file), stop_before_pixels=True)
                study_id = str(ds.StudyInstanceUID)
                series_id = str(ds.SeriesInstanceUID)
                series_key = f"{study_id}/{series_id}"
                series_files[series_key].append(str(dicom_file))
                logger.info(f"Found DICOM: {dicom_file} -> {series_key}")
            except Exception as e:
                logger.error(f"Failed to read {dicom_file}: {e}")
                continue
        
        # Upload files maintaining directory structure
        uploaded_paths = {}
        for series_key, files in series_files.items():
            uploaded_paths[series_key] = []
            for file_path in files:
                try:
                    s3_key = f"dicoms/{series_key}/{Path(file_path).name}"
                    self.s3_client.upload_file(file_path, self.dest_bucket, s3_key)
                    uploaded_paths[series_key].append(s3_key)
                    logger.info(f"Uploaded to s3://{self.dest_bucket}/{s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload {file_path}: {e}")
        
        logger.info(f"Successfully organized {sum(len(v) for v in uploaded_paths.values())} files")
        return uploaded_paths
    
    def import_to_healthimaging(self, series_mapping: Dict[str, List[str]], datastore_id: str, combine_series: bool = False) -> Dict[str, Dict]:
        """
        Import sorted DICOM series into AWS HealthImaging.

        Args:
            series_mapping: Dict mapping series keys to S3 paths
            datastore_id: AWS HealthImaging datastore ID
            combine_series: If True, combine all series from same study into single image set
        """
        import_results = {}

        if combine_series:
            # Group all series by study
            study_series = defaultdict(list)
            for series_key, s3_keys in series_mapping.items():
                study_id = series_key.split('/')[0]
                study_series[study_id].extend(s3_keys)

            # Import each study as a single image set with all series
            for study_id, all_s3_keys in study_series.items():
                try:
                    # Use the study directory as input location
                    response = self.client.start_dicom_import_job(
                        dataStoreId=datastore_id,
                        inputS3Uri=f's3://{self.dest_bucket}/dicoms/{study_id}/',
                        outputS3Uri=f's3://{self.dest_bucket}/healthimaging-output/{study_id}/',
                        jobName=f"import-combined-{study_id[:8]}"
                    )

                    import_results[study_id] = {
                        'jobId': response['jobId'],
                        'status': response['jobStatus'],
                        'studyId': study_id,
                        'fileCount': len(all_s3_keys),
                        'combined': True
                    }
                    logger.info(f"Started COMBINED import job {response['jobId']} for study {study_id} ({len(all_s3_keys)} files)")

                except Exception as e:
                    logger.error(f"Failed to import study {study_id}: {e}")
                    import_results[study_id] = {'error': str(e)}
        else:
            # ORIGINAL APPROACH: Import each series separately
            for series_key, s3_keys in series_mapping.items():
                study_id, series_id = series_key.split('/')

                try:
                    response = self.client.start_dicom_import_job(
                        dataStoreId=datastore_id,
                        inputS3Uri=f's3://{self.dest_bucket}/dicoms/{series_key}/',
                        outputS3Uri=f's3://{self.dest_bucket}/healthimaging-output/{series_key}/',
                        jobName=f"import-{study_id[:8]}-{series_id[:8]}"
                    )

                    import_results[series_key] = {
                        'jobId': response['jobId'],
                        'status': response['jobStatus'],
                        'studyId': study_id,
                        'seriesId': series_id,
                        'fileCount': len(s3_keys)
                    }
                    logger.info(f"Started import job {response['jobId']} for {series_key}")

                except Exception as e:
                    logger.error(f"Failed to import {series_key}: {e}")
                    import_results[series_key] = {'error': str(e)}

        return import_results
    
    def wait_for_import_jobs(self, import_results: Dict) -> Dict[str, Dict]:
        """
        Poll for completion of import jobs and get image set IDs.
        """
        completed_results = {}
        
        for series_key, job_info in import_results.items():
            if 'error' in job_info:
                completed_results[series_key] = job_info
                continue
            
            job_id = job_info['jobId']
            max_attempts = 120  # ~10 minutes with 5-second intervals
            attempt = 0
            
            while attempt < max_attempts:
                try:
                    response = self.client.get_dicom_import_job(jobId=job_id)
                    status = response['jobProperties']['jobStatus']
                    
                    if status == 'COMPLETED':
                        output_s3_uri = response['jobProperties']['outputS3Uri']
                        # Extract image set ID from output
                        completed_results[series_key] = {
                            **job_info,
                            'status': status,
                            'outputS3Uri': output_s3_uri,
                            'completedAt': datetime.now().isoformat()
                        }
                        logger.info(f"Import job {job_id} completed")
                        break
                    elif status == 'FAILED':
                        completed_results[series_key] = {
                            **job_info,
                            'status': status,
                            'error': response['jobProperties'].get('message', 'Unknown error')
                        }
                        logger.error(f"Import job {job_id} failed")
                        break
                    else:
                        logger.info(f"Job {job_id} still {status}...")
                        
                except Exception as e:
                    logger.error(f"Error checking job status: {e}")
                
                attempt += 1
                import time
                time.sleep(5)
            
            if attempt >= max_attempts:
                completed_results[series_key] = {
                    **job_info,
                    'status': 'TIMEOUT',
                    'error': 'Job did not complete within timeout period'
                }
        
        return completed_results
    
    def list_image_sets(self, datastore_id: str) -> List[Dict]:
        """
        List all image sets in the datastore.
        """
        image_sets = []
        
        try:
            paginator = self.client.get_paginator('search_image_sets')
            for page in paginator.paginate(datastoreId=datastore_id):
                image_sets.extend(page.get('imageSetsMetadata', []))
            
            logger.info(f"Found {len(image_sets)} image sets")
            return image_sets
            
        except Exception as e:
            logger.error(f"Failed to list image sets: {e}")
            return []
    
    def get_image_set_metadata(self, datastore_id: str, image_set_id: str) -> Dict:
        """
        Retrieve metadata for a specific image set.
        """
        try:
            response = self.client.get_image_set_metadata(
                datastoreId=datastore_id,
                imageSetId=image_set_id
            )
            return response
        except Exception as e:
            logger.error(f"Failed to get metadata for {image_set_id}: {e}")
            return {}
    
    def get_pixel_data_url(self, datastore_id: str, image_set_id: str, frame_num: int = 0) -> str:
        """
        Get a pre-signed URL for pixel data access.
        """
        if not self.accessor:
            logger.error("HealthImaging accessor not initialized")
            return None

        try:
            # Note: This method is not typically used with AWS HealthImaging
            # Use get_image_frame instead
            response = self.accessor.client.get_image_frame(
                datastoreId=datastore_id,
                imageSetId=image_set_id,
                imageFrameInformation={
                    'imageFrameId': f'frame-{frame_num}'
                }
            )
            return response.get('imageFrameBlob')
        except Exception as e:
            logger.error(f"Failed to get pixel data URL: {e}")
            return None

    def list_imported_images(self) -> List[Dict]:
        """
        List all imported image sets in the datastore.
        This is the method used by wsi_app.py to discover available images.

        Returns:
            List of image set metadata dictionaries with keys:
            - imageSetId: Unique identifier
            - version: Image set version
            - DICOMTags: Metadata tags
        """
        if not self.accessor:
            logger.error("HealthImaging accessor not initialized. Cannot list images.")
            return []

        try:
            image_sets = self.accessor.search_image_sets(max_results=50)
            logger.info(f"Found {len(image_sets)} image sets in datastore")
            return image_sets
        except Exception as e:
            logger.error(f"Failed to list imported images: {e}")
            return []

    def get_decoded_frame(self, image_set_id: str, frame_id: str) -> Optional[np.ndarray]:
        """
        Retrieve and decode an HTJ2K-compressed frame from AWS HealthImaging.

        Args:
            image_set_id: The image set containing the frame
            frame_id: The frame identifier from metadata

        Returns:
            Decoded image as numpy array (RGB uint8), or None on failure
        """
        if not self.accessor:
            logger.error("HealthImaging accessor not initialized")
            return None

        if not HAS_PYLIBJPEG:
            logger.error("pylibjpeg-openjpeg not installed. Cannot decode HTJ2K frames.")
            return None

        try:
            # Get compressed frame data
            image_data = self.accessor.get_image_frame(
                image_set_id=image_set_id,
                image_frame_id=frame_id
            )

            if not image_data:
                logger.error(f"No data returned for frame {frame_id}")
                return None

            # Decode HTJ2K using pylibjpeg-openjpeg (AWS recommended)
            img_array = jpeg_decode(image_data)

            # Ensure RGB format
            if len(img_array.shape) == 2:
                # Grayscale -> RGB
                img_array = np.stack([img_array] * 3, axis=-1)

            # Ensure uint8
            if img_array.dtype != np.uint8:
                img_array = img_array.astype(np.uint8)

            return img_array

        except Exception as e:
            logger.error(f"Failed to decode frame {frame_id}: {e}")
            return None


# HTML/JavaScript for OpenSeadragon viewer
OPENSEADRAGON_VIEWER_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>DICOM WSI Viewer</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/openseadragon/4.1.0/openseadragon.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; }
        #viewer { width: 100%; height: 100vh; background-color: #000; }
        #info { position: absolute; top: 10px; left: 10px; background: rgba(0,0,0,0.7); 
                color: white; padding: 10px; border-radius: 5px; z-index: 100; max-width: 300px; }
        .controls { position: absolute; bottom: 20px; left: 20px; background: rgba(0,0,0,0.7); 
                    padding: 15px; border-radius: 5px; z-index: 100; }
        button { padding: 8px 15px; margin: 5px; cursor: pointer; border: none; border-radius: 3px; }
        select { padding: 8px; margin: 5px; }
    </style>
</head>
<body>
    <div id="viewer"></div>
    <div id="info">
        <h3>DICOM WSI Viewer</h3>
        <p>Study ID: <span id="studyId">-</span></p>
        <p>Series ID: <span id="seriesId">-</span></p>
        <p>Frame: <span id="frameNum">-</span></p>
    </div>
    <div class="controls">
        <label for="imageSelect">Select Frame:</label>
        <select id="imageSelect"></select>
        <button onclick="viewer.viewport.zoomBy(1.2)">Zoom In</button>
        <button onclick="viewer.viewport.zoomBy(0.8)">Zoom Out</button>
        <button onclick="viewer.viewport.goHome()">Reset</button>
    </div>

    <script>
        const imageData = PLACEHOLDER_IMAGE_DATA;
        
        const viewer = OpenSeadragon({
            id: "viewer",
            prefixUrl: "https://cdnjs.cloudflare.com/ajax/libs/openseadragon/4.1.0/images/",
            tileSources: {
                type: "image",
                url: imageData.pixelDataUrl
            },
            showNavigator: true,
            minZoomLevel: 0.5,
            maxZoomLevel: 10
        });

        // Update display info
        document.getElementById('studyId').textContent = imageData.studyId;
        document.getElementById('seriesId').textContent = imageData.seriesId;
        
        // Populate frame selector
        const select = document.getElementById('imageSelect');
        for (let i = 0; i < imageData.frameCount; i++) {
            const option = document.createElement('option');
            option.value = i;
            option.textContent = `Frame ${i + 1}`;
            select.appendChild(option);
        }
        
        select.addEventListener('change', (e) => {
            document.getElementById('frameNum').textContent = e.target.value;
            // Update viewer with new frame URL
            const newUrl = imageData.pixelDataUrl.replace(/frame-\\d+/, `frame-${e.target.value}`);
            viewer.open({ type: "image", url: newUrl });
        });
    </script>
</body>
</html>
"""


def generate_viewer_html(study_id: str, series_id: str, pixel_data_url: str, frame_count: int = 1) -> str:
    """Generate HTML viewer with image data."""
    image_data = {
        'studyId': study_id,
        'seriesId': series_id,
        'pixelDataUrl': pixel_data_url,
        'frameCount': frame_count
    }
    return OPENSEADRAGON_VIEWER_HTML.replace('PLACEHOLDER_IMAGE_DATA', json.dumps(image_data))


# Example usage
if __name__ == '__main__':
    import time

    # Example 1: Using environment variables for configuration
    print("Example 1: Initialize pipeline with environment variables")
    print("Set AWS_HEALTHIMAGING_DATASTORE_ID and AWS_REGION environment variables")

    # Example 2: Using explicit configuration
    print("\nExample 2: Initialize pipeline with explicit parameters")
    pipeline = DICOMWSIPipeline(
        source_bucket='your-source-bucket',
        dest_bucket='your-dest-bucket',
        datastore_id='your-datastore-id',
        region='us-east-1'
    )

    # Step 1: Sort and upload DICOM files
    local_dicom_dir = './dicom_files'
    if os.path.exists(local_dicom_dir):
        series_mapping = pipeline.sort_and_upload_dicoms(local_dicom_dir)

        # Step 2: Import to HealthImaging
        import_results = pipeline.import_to_healthimaging(series_mapping, pipeline.datastore_id)

        # Step 3: Wait for jobs to complete
        completed = pipeline.wait_for_import_jobs(import_results)

    # Step 4: List all imported image sets
    print("\nListing imported image sets...")
    image_sets = pipeline.list_imported_images()

    if image_sets:
        print(f"Found {len(image_sets)} image sets")
        first_set = image_sets[0]
        image_set_id = first_set['imageSetId']

        # Get metadata
        metadata = pipeline.get_image_set_metadata(pipeline.datastore_id, image_set_id)

        print(f"\nImage Set ID: {image_set_id}")
        print("To view this image, run the WSI viewer:")
        print("  python wsi_app.py")
    else:
        print("No image sets found. Import DICOM files first.")
        print("\nTo import DICOM files:")
        print("1. Upload DICOM files to S3")
        print("2. Run the import workflow in main.py")