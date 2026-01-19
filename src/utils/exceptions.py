"""
Custom exceptions for the Reddit Data Pipeline.
"""


class RedditPipelineException(Exception):
    """Base exception for Reddit pipeline errors."""
    pass


class RedditAPIException(RedditPipelineException):
    """Exception raised when Reddit API calls fail."""
    pass


class S3Exception(RedditPipelineException):
    """Exception raised when S3 operations fail."""
    pass


class DataValidationException(RedditPipelineException):
    """Exception raised when data validation fails."""
    pass


class ConfigurationException(RedditPipelineException):
    """Exception raised when configuration is invalid."""
    pass


class ProcessingException(RedditPipelineException):
    """Exception raised when data processing fails."""
    pass
