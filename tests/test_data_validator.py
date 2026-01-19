"""
Unit tests for data validator.
"""

import unittest
import pandas as pd
import numpy as np
from src.processing.data_validator import DataValidator, ValidationResult


class TestDataValidator(unittest.TestCase):
    """Test cases for DataValidator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = DataValidator()
    
    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        df = pd.DataFrame()
        result = self.validator.validate(df)
        
        self.assertFalse(result.is_valid)
        self.assertIn("DataFrame is empty", result.errors)
    
    def test_validate_valid_data(self):
        """Test validation of valid data."""
        df = pd.DataFrame({
            'id': ['1', '2', '3'],
            'title': ['Title 1', 'Title 2', 'Title 3'],
            'subreddit': ['test', 'test', 'test'],
            'score': [10, 20, 30],
            'num_comments': [5, 10, 15]
        })
        
        result = self.validator.validate(df)
        
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)
    
    def test_validate_missing_required_columns(self):
        """Test validation with missing required columns."""
        df = pd.DataFrame({
            'title': ['Title 1', 'Title 2']
        })
        
        result = self.validator.validate(df)
        
        self.assertFalse(result.is_valid)
        self.assertTrue(any('Missing required columns' in error for error in result.errors))
    
    def test_validate_duplicates(self):
        """Test validation with duplicate IDs."""
        df = pd.DataFrame({
            'id': ['1', '1', '2'],
            'title': ['Title 1', 'Title 2', 'Title 3'],
            'subreddit': ['test', 'test', 'test']
        })
        
        validator = DataValidator(require_unique_ids=True)
        result = validator.validate(df)
        
        self.assertFalse(result.is_valid)
        self.assertTrue(any('duplicate' in error.lower() for error in result.errors))
    
    def test_validate_schema(self):
        """Test schema validation."""
        df = pd.DataFrame({
            'id': ['1', '2'],
            'score': [10, 20]
        })
        
        expected_schema = {
            'id': 'object',
            'score': 'int64'
        }
        
        result = self.validator.validate_schema(df, expected_schema)
        self.assertTrue(result.is_valid)


if __name__ == '__main__':
    unittest.main()
