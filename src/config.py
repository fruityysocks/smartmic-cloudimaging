"""
Configuration module for AWS HealthImaging Pipeline.

This module centralizes all AWS configuration including credentials,
bucket names, datastore IDs, and regional settings.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class AWSConfig:
    """Centralized AWS configuration with environment variable support."""

    def __init__(
        self,
        datastore_id: Optional[str] = None,
        region: Optional[str] = None,
        source_bucket: Optional[str] = None,
        dest_bucket: Optional[str] = None,
        output_bucket: Optional[str] = None
    ):
        """
        Initialize AWS configuration.

        Args:
            datastore_id: AWS HealthImaging datastore ID
            region: AWS region (e.g., 'us-east-1')
            source_bucket: S3 bucket for source DICOM files
            dest_bucket: S3 bucket for organized DICOM files
            output_bucket: S3 bucket for HealthImaging import job outputs

        Environment variables (if parameters not provided):
            - AWS_HEALTHIMAGING_DATASTORE_ID
            - AWS_REGION or AWS_DEFAULT_REGION
            - AWS_SOURCE_BUCKET
            - AWS_DEST_BUCKET
            - AWS_OUTPUT_BUCKET
        """
        self.datastore_id = datastore_id or os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID')
        self.region = region or os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.source_bucket = source_bucket or os.getenv('AWS_SOURCE_BUCKET')
        self.dest_bucket = dest_bucket or os.getenv('AWS_DEST_BUCKET')
        self.output_bucket = output_bucket or os.getenv('AWS_OUTPUT_BUCKET')

        # Validate required fields
        self._validate()

    def _validate(self):
        """Validate that required configuration is present."""
        if not self.datastore_id:
            logger.warning(
                "No datastore_id configured. Set AWS_HEALTHIMAGING_DATASTORE_ID "
                "environment variable or pass datastore_id parameter."
            )

        if not self.region:
            logger.warning("No AWS region configured. Using default: us-east-1")
            self.region = 'us-east-1'

    @property
    def is_complete(self) -> bool:
        """Check if all required configuration is present."""
        return all([
            self.datastore_id,
            self.region,
            self.source_bucket,
            self.dest_bucket,
            self.output_bucket
        ])

    def get_datastore_id(self, raise_if_missing: bool = False) -> Optional[str]:
        """
        Get datastore ID with optional error raising.

        Args:
            raise_if_missing: If True, raise ValueError when datastore_id is missing

        Returns:
            Datastore ID or None

        Raises:
            ValueError: If datastore_id is missing and raise_if_missing is True
        """
        if not self.datastore_id and raise_if_missing:
            raise ValueError(
                "Datastore ID not configured. Please set AWS_HEALTHIMAGING_DATASTORE_ID "
                "environment variable or pass it to the constructor."
            )
        return self.datastore_id

    def __repr__(self):
        """String representation for debugging."""
        return (
            f"AWSConfig(datastore_id={'***' if self.datastore_id else None}, "
            f"region={self.region}, "
            f"source_bucket={self.source_bucket}, "
            f"dest_bucket={self.dest_bucket}, "
            f"output_bucket={self.output_bucket})"
        )


# Default global configuration instance
# Can be imported and used across modules
default_config = AWSConfig()


def get_config() -> AWSConfig:
    """Get the default global configuration instance."""
    return default_config


def set_config(config: AWSConfig):
    """Set a new global configuration instance."""
    global default_config
    default_config = config
    logger.info(f"Updated global config: {config}")
