"""
Unit tests for Reddit extractor.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from src.ingestion.reddit_extractor import RedditExtractor
from src.utils.exceptions import RedditAPIException


class TestRedditExtractor(unittest.TestCase):
    """Test cases for RedditExtractor."""
    
    @patch('src.ingestion.reddit_extractor.praw.Reddit')
    def test_connect_success(self, mock_reddit_class):
        """Test successful Reddit connection."""
        mock_reddit_instance = MagicMock()
        mock_reddit_instance.user.me.return_value = None
        mock_reddit_class.return_value = mock_reddit_instance
        
        config = {
            'client_id': 'test_id',
            'client_secret': 'test_secret',
            'user_agent': 'test_agent'
        }
        
        extractor = RedditExtractor(config=config)
        self.assertIsNotNone(extractor.reddit)
    
    @patch('src.ingestion.reddit_extractor.praw.Reddit')
    def test_connect_failure(self, mock_reddit_class):
        """Test Reddit connection failure."""
        mock_reddit_class.side_effect = Exception("Connection failed")
        
        config = {
            'client_id': 'test_id',
            'client_secret': 'test_secret',
            'user_agent': 'test_agent'
        }
        
        with self.assertRaises(RedditAPIException):
            RedditExtractor(config=config)
    
    @patch('src.ingestion.reddit_extractor.praw.Reddit')
    def test_extract_post_fields(self, mock_reddit_class):
        """Test post field extraction."""
        mock_reddit_instance = MagicMock()
        mock_reddit_instance.user.me.return_value = None
        mock_reddit_class.return_value = mock_reddit_instance
        
        config = {
            'client_id': 'test_id',
            'client_secret': 'test_secret',
            'user_agent': 'test_agent'
        }
        
        extractor = RedditExtractor(config=config)
        
        # Mock post object
        mock_post = MagicMock()
        mock_post.id = 'test_id'
        mock_post.title = 'Test Title'
        mock_post.score = 100
        mock_post.created_utc = 1609459200
        mock_post.author = MagicMock()
        mock_post.author.__str__ = lambda x: 'test_author'
        
        result = extractor._extract_post_fields(mock_post)
        
        self.assertIn('id', result)
        self.assertEqual(result['id'], 'test_id')
        self.assertEqual(result['title'], 'Test Title')


if __name__ == '__main__':
    unittest.main()
