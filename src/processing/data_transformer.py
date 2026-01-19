"""
Production-ready data transformation module with type conversion and cleaning.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from datetime import datetime
from src.utils.exceptions import ProcessingException
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataTransformer:
    """Data transformation utilities for Reddit posts."""
    
    def __init__(self):
        """Initialize data transformer."""
        self.logger = logger
    
    def transform_reddit_posts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw Reddit post data into clean, typed DataFrame.
        
        Args:
            df: Raw DataFrame from Reddit extraction
            
        Returns:
            Transformed DataFrame with proper types and cleaned data
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return df
        
        logger.info(f"Transforming {len(df)} posts")
        df = df.copy()
        
        try:
            # Type conversions
            df = self._convert_types(df)
            
            # Data cleaning
            df = self._clean_data(df)
            
            # Feature engineering
            df = self._engineer_features(df)
            
            # Remove duplicates
            df = self._remove_duplicates(df)
            
            logger.info(f"Transformation complete: {len(df)} posts remaining")
            return df
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise ProcessingException(f"Data transformation failed: {str(e)}")
    
    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        # String columns
        string_columns = ['id', 'subreddit', 'title', 'selftext', 'author', 'url', 'permalink']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace('nan', np.nan)
        
        # Numeric columns
        numeric_columns = ['score', 'num_comments', 'upvote_ratio']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Boolean columns
        boolean_columns = ['is_self', 'over_18', 'stickied', 'distinguished']
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        
        # Datetime conversion
        if 'created_utc' in df.columns:
            df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s', errors='coerce')
            df['created_date'] = df['created_utc'].dt.date
            df['created_hour'] = df['created_utc'].dt.hour
            df['created_day_of_week'] = df['created_utc'].dt.day_name()
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean data: handle nulls, remove invalid rows."""
        # Remove rows with missing critical fields
        critical_fields = ['id', 'title']
        for field in critical_fields:
            if field in df.columns:
                before = len(df)
                df = df[df[field].notna()]
                removed = before - len(df)
                if removed > 0:
                    logger.debug(f"Removed {removed} rows with missing {field}")
        
        # Clean text fields
        text_fields = ['title', 'selftext']
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].str.strip()
                df[field] = df[field].replace('', np.nan)
        
        # Handle author field (deleted/removed users)
        if 'author' in df.columns:
            df['author'] = df['author'].replace(['[deleted]', '[removed]', 'nan'], np.nan)
        
        return df
    
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer additional features."""
        # Text length features
        if 'title' in df.columns:
            df['title_length'] = df['title'].str.len()
            df['title_word_count'] = df['title'].str.split().str.len()
        
        if 'selftext' in df.columns:
            df['selftext_length'] = df['selftext'].str.len()
            df['selftext_word_count'] = df['selftext'].str.split().str.len()
        
        # Engagement metrics
        if 'score' in df.columns and 'num_comments' in df.columns:
            df['engagement_score'] = df['score'] + (df['num_comments'] * 2)
        
        # Popularity categories
        if 'score' in df.columns:
            df['popularity_category'] = pd.cut(
                df['score'],
                bins=[-np.inf, 0, 10, 100, 1000, np.inf],
                labels=['Negative', 'Low', 'Medium', 'High', 'Viral']
            )
        
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate posts based on ID."""
        if 'id' in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=['id'], keep='first')
            removed = before - len(df)
            if removed > 0:
                logger.debug(f"Removed {removed} duplicate posts")
        return df
    
    def prepare_for_export(self, df: pd.DataFrame, format: str = 'csv') -> pd.DataFrame:
        """
        Prepare DataFrame for export by handling format-specific requirements.
        
        Args:
            df: DataFrame to prepare
            format: Export format ('csv', 'parquet', 'json')
            
        Returns:
            Prepared DataFrame
        """
        df = df.copy()
        
        # Convert datetime to string for CSV
        if format.lower() == 'csv':
            datetime_columns = df.select_dtypes(include=['datetime64']).columns
            for col in datetime_columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Handle NaN values
        df = df.replace([np.nan, None], '')
        
        return df
