"""
Production-ready logging configuration for the Reddit Data Pipeline.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class PipelineLogger:
    """Centralized logging utility for the pipeline."""
    
    _loggers: dict[str, logging.Logger] = {}
    _log_dir = Path("logs")
    
    @classmethod
    def setup_log_dir(cls) -> None:
        """Create logs directory if it doesn't exist."""
        cls._log_dir.mkdir(exist_ok=True)
    
    @classmethod
    def get_logger(
        cls,
        name: str,
        log_level: str = "INFO",
        log_to_file: bool = True,
        log_file_prefix: Optional[str] = None
    ) -> logging.Logger:
        """
        Get or create a logger instance.
        
        Args:
            name: Logger name (typically __name__)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_to_file: Whether to log to file
            log_file_prefix: Optional prefix for log file name
            
        Returns:
            Configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Prevent duplicate handlers
        if logger.handlers:
            return logger
        
        # Console handler with colored output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        logger.addHandler(console_handler)
        
        # File handler
        if log_to_file:
            cls.setup_log_dir()
            log_file_name = log_file_prefix or name.replace('.', '_')
            log_file_path = cls._log_dir / f"{log_file_name}_{datetime.now().strftime('%Y%m%d')}.log"
            
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_format)
            logger.addHandler(file_handler)
        
        cls._loggers[name] = logger
        return logger


def get_logger(name: str, **kwargs) -> logging.Logger:
    """Convenience function to get a logger."""
    return PipelineLogger.get_logger(name, **kwargs)
