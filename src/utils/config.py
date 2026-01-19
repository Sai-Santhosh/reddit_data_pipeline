"""
Production-ready configuration management for the Reddit Data Pipeline.
Supports environment variables, config files, and validation.
"""

import os
import configparser
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from src.utils.exceptions import ConfigurationException
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class RedditConfig:
    """Reddit API configuration."""
    client_id: str
    client_secret: str
    user_agent: str = "RedditDataPipeline/1.0"
    
    def __post_init__(self):
        if not self.client_id or not self.client_secret:
            raise ConfigurationException("Reddit client_id and client_secret are required")


@dataclass
class AWSConfig:
    """AWS configuration."""
    access_key_id: str
    secret_access_key: str
    region: str = "us-east-1"
    bucket_name: str = ""
    session_token: Optional[str] = None
    
    def __post_init__(self):
        if not self.access_key_id or not self.secret_access_key:
            raise ConfigurationException("AWS access_key_id and secret_access_key are required")


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    name: str = "airflow_reddit"
    username: str = "postgres"
    password: str = "postgres"
    
    @property
    def connection_string(self) -> str:
        """Get SQLAlchemy connection string."""
        return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class PipelineConfig:
    """Pipeline configuration."""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5
    timeout: int = 300
    log_level: str = "INFO"
    data_quality_checks: bool = True


@dataclass
class PathConfig:
    """File path configuration."""
    input_path: Path = Path("data/input")
    output_path: Path = Path("data/output")
    logs_path: Path = Path("logs")
    models_path: Path = Path("models")
    
    def __post_init__(self):
        """Create directories if they don't exist."""
        for path in [self.input_path, self.output_path, self.logs_path, self.models_path]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class AppConfig:
    """Main application configuration."""
    reddit: RedditConfig
    aws: AWSConfig
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    
    @classmethod
    def from_file(cls, config_path: Optional[Path] = None) -> "AppConfig":
        """
        Load configuration from file and environment variables.
        Environment variables take precedence over config file.
        """
        if config_path is None:
            config_path = Path("config/config.conf")
        
        parser = configparser.ConfigParser()
        if config_path.exists():
            parser.read(config_path)
            logger.info(f"Loaded configuration from {config_path}")
        else:
            logger.warning(f"Config file not found at {config_path}, using environment variables only")
        
        # Helper function to get config with env var override
        def get_config(section: str, key: str, default: str = "") -> str:
            env_key = f"{section.upper()}_{key.upper()}"
            return os.getenv(env_key, parser.get(section, key, fallback=default))
        
        # Reddit config
        reddit_config = RedditConfig(
            client_id=get_config("api_keys", "reddit_client_id") or os.getenv("REDDIT_CLIENT_ID", ""),
            client_secret=get_config("api_keys", "reddit_secret_key") or os.getenv("REDDIT_SECRET_KEY", ""),
            user_agent=get_config("api_keys", "reddit_user_agent", "RedditDataPipeline/1.0") 
                      or os.getenv("REDDIT_USER_AGENT", "RedditDataPipeline/1.0")
        )
        
        # AWS config
        aws_config = AWSConfig(
            access_key_id=get_config("aws", "aws_access_key_id") or os.getenv("AWS_ACCESS_KEY_ID", ""),
            secret_access_key=get_config("aws", "aws_secret_access_key") or os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            region=get_config("aws", "aws_region", "us-east-1") or os.getenv("AWS_REGION", "us-east-1"),
            bucket_name=get_config("aws", "aws_bucket_name") or os.getenv("AWS_BUCKET_NAME", ""),
            session_token=get_config("aws", "aws_session_token") or os.getenv("AWS_SESSION_TOKEN")
        )
        
        # Database config
        database_config = DatabaseConfig(
            host=get_config("database", "database_host", "localhost") or os.getenv("DB_HOST", "localhost"),
            port=int(get_config("database", "database_port", "5432") or os.getenv("DB_PORT", "5432")),
            name=get_config("database", "database_name", "airflow_reddit") or os.getenv("DB_NAME", "airflow_reddit"),
            username=get_config("database", "database_username", "postgres") or os.getenv("DB_USER", "postgres"),
            password=get_config("database", "database_password", "postgres") or os.getenv("DB_PASSWORD", "postgres")
        )
        
        # Pipeline config
        pipeline_config = PipelineConfig(
            batch_size=int(get_config("etl_settings", "batch_size", "1000") or os.getenv("BATCH_SIZE", "1000")),
            max_retries=int(get_config("etl_settings", "max_retries", "3") or os.getenv("MAX_RETRIES", "3")),
            retry_delay=int(get_config("etl_settings", "retry_delay", "5") or os.getenv("RETRY_DELAY", "5")),
            timeout=int(get_config("etl_settings", "timeout", "300") or os.getenv("TIMEOUT", "300")),
            log_level=get_config("etl_settings", "log_level", "INFO") or os.getenv("LOG_LEVEL", "INFO"),
            data_quality_checks=get_config("etl_settings", "data_quality_checks", "true").lower() == "true"
        )
        
        return cls(
            reddit=reddit_config,
            aws=aws_config,
            database=database_config,
            pipeline=pipeline_config
        )
    
    def validate(self) -> None:
        """Validate configuration."""
        errors = []
        
        if not self.reddit.client_id or not self.reddit.client_secret:
            errors.append("Reddit credentials are required")
        
        if not self.aws.access_key_id or not self.aws.secret_access_key:
            errors.append("AWS credentials are required")
        
        if errors:
            raise ConfigurationException(f"Configuration validation failed: {'; '.join(errors)}")


# Global config instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = AppConfig.from_file()
        _config.validate()
    return _config


def reload_config() -> AppConfig:
    """Reload configuration from file."""
    global _config
    _config = AppConfig.from_file()
    _config.validate()
    return _config
