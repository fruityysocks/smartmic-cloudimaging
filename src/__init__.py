"""
SmartMic Cloud Imaging - AWS HealthImaging DICOM WSI Pipeline

Main package for processing and viewing whole slide imaging (WSI) DICOM files
using AWS HealthImaging service.
"""

__version__ = '2.0.0'

from .healthimaging import AWSHealthImaging
from .config import AWSConfig

__all__ = [
    'AWSHealthImaging',
    'AWSConfig',
]
