"""
Production-ready S3 upload pipeline orchestrator.
"""

from pathlib import Path
from typing import Optional
from src.ingestion.s3_uploader import S3Uploader
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.exceptions import RedditPipelineException

logger = get_logger(__name__)


class S3Pipeline:
    """S3 upload pipeline orchestrator."""
    
    def __init__(self):
        """Initialize pipeline components."""
        self.config = get_config()
        self.uploader = S3Uploader()
        self.logger = logger
    
    def upload_file(
        self,
        local_file_path: str,
        s3_key: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Upload a file to S3.
        
        Args:
            local_file_path: Path to local file
            s3_key: S3 object key (optional, auto-generated if not provided)
            metadata: Optional metadata dictionary
            
        Returns:
            S3 URI of uploaded file
        """
        try:
            logger.info(f"Starting S3 upload for file: {local_file_path}")
            
            # Validate file exists
            file_path = Path(local_file_path)
            if not file_path.exists():
                raise RedditPipelineException(f"File not found: {local_file_path}")
            
            # Upload to S3
            s3_uri = self.uploader.upload_file(
                local_file_path=str(file_path),
                s3_key=s3_key,
                metadata=metadata
            )
            
            logger.info(f"S3 upload completed successfully: {s3_uri}")
            return s3_uri
            
        except Exception as e:
            logger.error(f"S3 upload failed: {str(e)}")
            raise RedditPipelineException(f"S3 upload failed: {str(e)}")
    
    def upload_dataframe(
        self,
        df,
        s3_key: str,
        format: str = 'csv',
        **kwargs
    ) -> str:
        """
        Upload a pandas DataFrame directly to S3.
        
        Args:
            df: pandas DataFrame
            s3_key: S3 object key
            format: File format ('csv', 'parquet', 'json')
            **kwargs: Additional arguments for pandas export methods
            
        Returns:
            S3 URI of uploaded file
        """
        try:
            logger.info(f"Uploading DataFrame to S3: {s3_key}")
            
            s3_uri = self.uploader.upload_dataframe(
                df=df,
                s3_key=s3_key,
                format=format,
                **kwargs
            )
            
            logger.info(f"DataFrame upload completed successfully: {s3_uri}")
            return s3_uri
            
        except Exception as e:
            logger.error(f"DataFrame upload failed: {str(e)}")
            raise RedditPipelineException(f"DataFrame upload failed: {str(e)}")


def upload_to_s3(local_file_path: str, s3_key: Optional[str] = None) -> str:
    """
    Convenience function to upload a file to S3.
    
    Args:
        local_file_path: Path to local file
        s3_key: S3 object key (optional)
        
    Returns:
        S3 URI of uploaded file
    """
    pipeline = S3Pipeline()
    return pipeline.upload_file(local_file_path, s3_key)
