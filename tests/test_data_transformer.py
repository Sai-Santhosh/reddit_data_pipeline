"""
Unit tests for data transformer.
"""

import unittest
import pandas as pd
import numpy as np
from src.processing.data_transformer import DataTransformer


class TestDataTransformer(unittest.TestCase):
    """Test cases for DataTransformer."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.transformer = DataTransformer()
    
    def test_transform_reddit_posts(self):
        """Test transformation of Reddit posts."""
        df = pd.DataFrame({
            'id': ['1', '2'],
            'title': ['Test Title', 'Another Title'],
            'subreddit': ['test', 'test'],
            'score': ['10', '20'],
            'created_utc': [1609459200, 1609545600]
        })
        
        result = self.transformer.transform_reddit_posts(df)
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertTrue(pd.api.types.is_numeric_dtype(result['score']))
    
    def test_remove_duplicates(self):
        """Test duplicate removal."""
        df = pd.DataFrame({
            'id': ['1', '1', '2'],
            'title': ['Title 1', 'Title 2', 'Title 3']
        })
        
        result = self.transformer._remove_duplicates(df)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result['id'].nunique(), 2)
    
    def test_engineer_features(self):
        """Test feature engineering."""
        df = pd.DataFrame({
            'title': ['Test Title', 'Another Title'],
            'score': [10, 20],
            'num_comments': [5, 10]
        })
        
        result = self.transformer._engineer_features(df)
        
        self.assertIn('title_length', result.columns)
        self.assertIn('title_word_count', result.columns)
        self.assertIn('engagement_score', result.columns)


if __name__ == '__main__':
    unittest.main()
