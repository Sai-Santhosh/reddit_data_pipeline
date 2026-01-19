"""
Production-ready data validation module with comprehensive quality checks.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from src.utils.exceptions import DataValidationException
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    stats: Dict
    
    def __str__(self) -> str:
        """String representation of validation result."""
        status = "VALID" if self.is_valid else "INVALID"
        result = f"Validation Status: {status}\n"
        if self.errors:
            result += f"Errors ({len(self.errors)}):\n"
            for error in self.errors:
                result += f"  - {error}\n"
        if self.warnings:
            result += f"Warnings ({len(self.warnings)}):\n"
            for warning in self.warnings:
                result += f"  - {warning}\n"
        return result


class DataValidator:
    """Data validation utilities for Reddit posts."""
    
    REQUIRED_COLUMNS = ['id', 'title', 'subreddit']
    EXPECTED_COLUMNS = [
        'id', 'subreddit', 'title', 'selftext', 'score', 'num_comments',
        'author', 'created_utc', 'upvote_ratio', 'url', 'permalink'
    ]
    
    def __init__(
        self,
        min_rows: int = 1,
        max_null_percentage: float = 50.0,
        require_unique_ids: bool = True
    ):
        """
        Initialize data validator.
        
        Args:
            min_rows: Minimum number of rows required
            max_null_percentage: Maximum allowed null percentage per column
            require_unique_ids: Whether IDs must be unique
        """
        self.min_rows = min_rows
        self.max_null_percentage = max_null_percentage
        self.require_unique_ids = require_unique_ids
        self.logger = logger
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Perform comprehensive data validation.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult object
        """
        errors = []
        warnings = []
        stats = {}
        
        try:
            # Basic structure checks
            structure_errors, structure_warnings, structure_stats = self._validate_structure(df)
            errors.extend(structure_errors)
            warnings.extend(structure_warnings)
            stats.update(structure_stats)
            
            # Data quality checks
            quality_errors, quality_warnings, quality_stats = self._validate_quality(df)
            errors.extend(quality_errors)
            warnings.extend(quality_warnings)
            stats.update(quality_stats)
            
            # Business logic checks
            business_errors, business_warnings, business_stats = self._validate_business_rules(df)
            errors.extend(business_errors)
            warnings.extend(business_warnings)
            stats.update(business_stats)
            
            is_valid = len(errors) == 0
            
            result = ValidationResult(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                stats=stats
            )
            
            logger.info(f"Validation complete: {'VALID' if is_valid else 'INVALID'} "
                       f"({len(errors)} errors, {len(warnings)} warnings)")
            
            return result
            
        except Exception as e:
            logger.error(f"Validation failed with exception: {str(e)}")
            raise DataValidationException(f"Validation failed: {str(e)}")
    
    def _validate_structure(
        self, df: pd.DataFrame
    ) -> Tuple[List[str], List[str], Dict]:
        """Validate DataFrame structure."""
        errors = []
        warnings = []
        stats = {}
        
        # Check if DataFrame is empty
        if df.empty:
            errors.append("DataFrame is empty")
            return errors, warnings, stats
        
        stats['total_rows'] = len(df)
        stats['total_columns'] = len(df.columns)
        
        # Check required columns
        missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check for unexpected columns
        unexpected_columns = [col for col in df.columns if col not in self.EXPECTED_COLUMNS]
        if unexpected_columns:
            warnings.append(f"Unexpected columns found: {unexpected_columns}")
        
        # Check data types
        if 'id' in df.columns:
            if not pd.api.types.is_string_dtype(df['id']):
                warnings.append("Column 'id' should be string type")
        
        if 'score' in df.columns:
            if not pd.api.types.is_numeric_dtype(df['score']):
                errors.append("Column 'score' should be numeric")
        
        return errors, warnings, stats
    
    def _validate_quality(
        self, df: pd.DataFrame
    ) -> Tuple[List[str], List[str], Dict]:
        """Validate data quality."""
        errors = []
        warnings = []
        stats = {}
        
        # Check minimum rows
        if len(df) < self.min_rows:
            errors.append(f"DataFrame has {len(df)} rows, minimum required is {self.min_rows}")
        
        # Check for duplicates
        if 'id' in df.columns:
            duplicate_count = df['id'].duplicated().sum()
            stats['duplicate_ids'] = int(duplicate_count)
            if duplicate_count > 0 and self.require_unique_ids:
                errors.append(f"Found {duplicate_count} duplicate IDs")
            elif duplicate_count > 0:
                warnings.append(f"Found {duplicate_count} duplicate IDs")
        
        # Check null percentages
        null_percentages = (df.isnull().sum() / len(df) * 100).to_dict()
        stats['null_percentages'] = null_percentages
        
        for col, pct in null_percentages.items():
            if pct > self.max_null_percentage:
                errors.append(f"Column '{col}' has {pct:.2f}% null values (max allowed: {self.max_null_percentage}%)")
            elif pct > self.max_null_percentage / 2:
                warnings.append(f"Column '{col}' has {pct:.2f}% null values")
        
        # Check for empty strings in critical fields
        if 'title' in df.columns:
            empty_titles = (df['title'].astype(str).str.strip() == '').sum()
            if empty_titles > 0:
                errors.append(f"Found {empty_titles} posts with empty titles")
        
        return errors, warnings, stats
    
    def _validate_business_rules(
        self, df: pd.DataFrame
    ) -> Tuple[List[str], List[str], Dict]:
        """Validate business logic rules."""
        errors = []
        warnings = []
        stats = {}
        
        # Check score ranges
        if 'score' in df.columns:
            negative_scores = (df['score'] < 0).sum()
            stats['negative_scores'] = int(negative_scores)
            if negative_scores > len(df) * 0.1:  # More than 10% negative
                warnings.append(f"High percentage of negative scores: {negative_scores}")
        
        # Check comment counts
        if 'num_comments' in df.columns:
            negative_comments = (df['num_comments'] < 0).sum()
            if negative_comments > 0:
                errors.append(f"Found {negative_comments} posts with negative comment counts")
        
        # Check upvote ratio
        if 'upvote_ratio' in df.columns:
            invalid_ratios = ((df['upvote_ratio'] < 0) | (df['upvote_ratio'] > 1)).sum()
            if invalid_ratios > 0:
                errors.append(f"Found {invalid_ratios} posts with invalid upvote ratios")
        
        # Check date ranges
        if 'created_utc' in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df['created_utc']):
                future_dates = (df['created_utc'] > pd.Timestamp.now()).sum()
                if future_dates > 0:
                    errors.append(f"Found {future_dates} posts with future dates")
                
                stats['date_range'] = {
                    'min': str(df['created_utc'].min()),
                    'max': str(df['created_utc'].max())
                }
        
        # Check subreddit distribution
        if 'subreddit' in df.columns:
            subreddit_counts = df['subreddit'].value_counts().to_dict()
            stats['subreddit_distribution'] = subreddit_counts
            
            if len(subreddit_counts) == 0:
                errors.append("No subreddits found in data")
            elif len(subreddit_counts) == 1:
                warnings.append("Data contains posts from only one subreddit")
        
        return errors, warnings, stats
    
    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict) -> ValidationResult:
        """
        Validate DataFrame against expected schema.
        
        Args:
            df: DataFrame to validate
            expected_schema: Dictionary mapping column names to expected types
            
        Returns:
            ValidationResult object
        """
        errors = []
        warnings = []
        stats = {}
        
        for col, expected_type in expected_schema.items():
            if col not in df.columns:
                errors.append(f"Missing column: {col}")
                continue
            
            actual_type = str(df[col].dtype)
            if expected_type not in actual_type:
                warnings.append(f"Column '{col}' has type {actual_type}, expected {expected_type}")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            stats=stats
        )
