"""
Production-ready AWS S3 uploader with error handling, retries, and progress tracking.
"""

import os
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config
from src.utils.exceptions import S3Exception
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)


class S3Uploader:
    """AWS S3 uploader with production-grade error handling."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize S3 uploader.
        
        Args:
            config: Optional config dict. If not provided, uses global config.
        """
        if config is None:
            app_config = get_config()
            self.aws_config = app_config.aws
        else:
            from src.utils.config import AWSConfig
            self.aws_config = AWSConfig(**config)
        
        self.s3_client = self._create_s3_client()
        self._ensure_bucket_exists()
    
    def _create_s3_client(self) -> boto3.client:
        """Create and configure S3 client."""
        try:
            config = Config(
                retries={'max_attempts': 3, 'mode': 'standard'},
                connect_timeout=60,
                read_timeout=60
            )
            
            client_kwargs = {
                'aws_access_key_id': self.aws_config.access_key_id,
                'aws_secret_access_key': self.aws_config.secret_access_key,
                'region_name': self.aws_config.region,
                'config': config
            }
            
            if self.aws_config.session_token:
                client_kwargs['aws_session_token'] = self.aws_config.session_token
            
            s3_client = boto3.client('s3', **client_kwargs)
            logger.info(f"S3 client initialized for region {self.aws_config.region}")
            return s3_client
            
        except Exception as e:
            logger.error(f"Failed to create S3 client: {str(e)}")
            raise S3Exception(f"S3 client creation failed: {str(e)}")
    
    def _ensure_bucket_exists(self) -> None:
        """Ensure S3 bucket exists, create if it doesn't."""
        if not self.aws_config.bucket_name:
            logger.warning("No bucket name configured, skipping bucket check")
            return
        
        try:
            self.s3_client.head_bucket(Bucket=self.aws_config.bucket_name)
            logger.info(f"Bucket {self.aws_config.bucket_name} exists")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.s3_client.create_bucket(
                        Bucket=self.aws_config.bucket_name,
                        CreateBucketConfiguration={
                            'LocationConstraint': self.aws_config.region
                        } if self.aws_config.region != 'us-east-1' else {}
                    )
                    logger.info(f"Created bucket {self.aws_config.bucket_name}")
                except Exception as create_error:
                    logger.error(f"Failed to create bucket: {str(create_error)}")
                    raise S3Exception(f"Bucket creation failed: {str(create_error)}")
            else:
                logger.error(f"Error checking bucket: {str(e)}")
                raise S3Exception(f"Bucket access failed: {str(e)}")
    
    def upload_file(
        self,
        local_file_path: str,
        s3_key: Optional[str] = None,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Upload a file to S3.
        
        Args:
            local_file_path: Path to local file
            s3_key: S3 object key (path). If None, uses filename with date prefix
            bucket: S3 bucket name. If None, uses configured bucket
            metadata: Optional metadata to attach to object
            
        Returns:
            S3 URI of uploaded file
        """
        if bucket is None:
            bucket = self.aws_config.bucket_name
        
        if not bucket:
            raise S3Exception("Bucket name is required")
        
        local_path = Path(local_file_path)
        if not local_path.exists():
            raise S3Exception(f"Local file not found: {local_file_path}")
        
        if s3_key is None:
            # Generate S3 key with date partitioning
            date_str = datetime.now().strftime("%Y-%m-%d")
            filename = local_path.name
            s3_key = f"raw/reddit/dt={date_str}/{filename}"
        
        try:
            upload_kwargs = {
                'Bucket': bucket,
                'Key': s3_key,
                'Filename': str(local_path)
            }
            
            if metadata:
                upload_kwargs['Metadata'] = metadata
            
            logger.info(f"Uploading {local_file_path} to s3://{bucket}/{s3_key}")
            self.s3_client.upload_file(**upload_kwargs)
            
            s3_uri = f"s3://{bucket}/{s3_key}"
            logger.info(f"Successfully uploaded to {s3_uri}")
            return s3_uri
            
        except FileNotFoundError:
            logger.error(f"File not found: {local_file_path}")
            raise S3Exception(f"File not found: {local_file_path}")
        except ClientError as e:
            logger.error(f"S3 upload failed: {str(e)}")
            raise S3Exception(f"S3 upload failed: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during upload: {str(e)}")
            raise S3Exception(f"Upload failed: {str(e)}")
    
    def upload_dataframe(
        self,
        df,
        s3_key: str,
        bucket: Optional[str] = None,
        format: str = 'csv',
        **kwargs
    ) -> str:
        """
        Upload a pandas DataFrame directly to S3.
        
        Args:
            df: pandas DataFrame
            s3_key: S3 object key
            bucket: S3 bucket name. If None, uses configured bucket
            format: File format ('csv', 'parquet', 'json')
            **kwargs: Additional arguments for pandas to_* methods
            
        Returns:
            S3 URI of uploaded file
        """
        import pandas as pd
        import io
        
        if bucket is None:
            bucket = self.aws_config.bucket_name
        
        if not bucket:
            raise S3Exception("Bucket name is required")
        
        try:
            buffer = io.BytesIO()
            
            if format.lower() == 'csv':
                df.to_csv(buffer, index=False, **kwargs)
                content_type = 'text/csv'
            elif format.lower() == 'parquet':
                df.to_parquet(buffer, index=False, **kwargs)
                content_type = 'application/parquet'
            elif format.lower() == 'json':
                df.to_json(buffer, orient='records', **kwargs)
                content_type = 'application/json'
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            buffer.seek(0)
            
            logger.info(f"Uploading DataFrame to s3://{bucket}/{s3_key}")
            self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType=content_type
            )
            
            s3_uri = f"s3://{bucket}/{s3_key}"
            logger.info(f"Successfully uploaded DataFrame to {s3_uri}")
            return s3_uri
            
        except Exception as e:
            logger.error(f"DataFrame upload failed: {str(e)}")
            raise S3Exception(f"DataFrame upload failed: {str(e)}")
    
    def list_objects(
        self,
        prefix: str = "",
        bucket: Optional[str] = None,
        max_keys: int = 1000
    ) -> list:
        """
        List objects in S3 bucket.
        
        Args:
            prefix: Object key prefix to filter
            bucket: S3 bucket name. If None, uses configured bucket
            max_keys: Maximum number of keys to return
            
        Returns:
            List of object keys
        """
        if bucket is None:
            bucket = self.aws_config.bucket_name
        
        if not bucket:
            raise S3Exception("Bucket name is required")
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
            
        except ClientError as e:
            logger.error(f"Failed to list objects: {str(e)}")
            raise S3Exception(f"List objects failed: {str(e)}")
    
    def delete_object(self, s3_key: str, bucket: Optional[str] = None) -> None:
        """
        Delete an object from S3.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name. If None, uses configured bucket
        """
        if bucket is None:
            bucket = self.aws_config.bucket_name
        
        if not bucket:
            raise S3Exception("Bucket name is required")
        
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=s3_key)
            logger.info(f"Deleted s3://{bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"Failed to delete object: {str(e)}")
            raise S3Exception(f"Delete object failed: {str(e)}")
