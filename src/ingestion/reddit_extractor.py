"""
Production-ready Reddit data extractor with error handling, retries, and rate limiting.
"""

import time
from typing import List, Dict, Optional, Iterator
import praw
from praw import Reddit
from prawcore.exceptions import PrawcoreException
import pandas as pd
from src.utils.exceptions import RedditAPIException
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)


class RedditExtractor:
    """Reddit data extractor with production-grade error handling."""
    
    # Fields to extract from Reddit posts
    POST_FIELDS = (
        'id',
        'subreddit',
        'title',
        'selftext',
        'score',
        'num_comments',
        'author',
        'created_utc',
        'upvote_ratio',
        'url',
        'permalink',
        'is_self',
        'over_18',
        'stickied',
        'distinguished'
    )
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize Reddit extractor.
        
        Args:
            config: Optional config dict. If not provided, uses global config.
        """
        if config is None:
            app_config = get_config()
            self.reddit_config = app_config.reddit
        else:
            from src.utils.config import RedditConfig
            self.reddit_config = RedditConfig(**config)
        
        self.reddit: Optional[Reddit] = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish connection to Reddit API."""
        try:
            self.reddit = praw.Reddit(
                client_id=self.reddit_config.client_id,
                client_secret=self.reddit_config.client_secret,
                user_agent=self.reddit_config.user_agent,
                read_only=True
            )
            # Test connection
            self.reddit.user.me()
            logger.info("Successfully connected to Reddit API")
        except Exception as e:
            logger.error(f"Failed to connect to Reddit API: {str(e)}")
            raise RedditAPIException(f"Reddit API connection failed: {str(e)}")
    
    def extract_posts(
        self,
        subreddit: str,
        time_filter: str = 'all',
        limit: Optional[int] = None,
        sort: str = 'top'
    ) -> List[Dict]:
        """
        Extract posts from specified subreddit(s).
        
        Args:
            subreddit: Subreddit name(s) - can be single or multiple joined with '+'
            time_filter: Time filter ('all', 'day', 'week', 'month', 'year')
            limit: Maximum number of posts to extract
            sort: Sort method ('top', 'hot', 'new', 'rising')
            
        Returns:
            List of post dictionaries
        """
        if not self.reddit:
            self._connect()
        
        logger.info(f"Extracting posts from r/{subreddit} with time_filter={time_filter}, limit={limit}")
        
        try:
            subreddit_instance = self.reddit.subreddit(subreddit)
            
            # Get posts based on sort method
            if sort == 'top':
                posts = subreddit_instance.top(time_filter=time_filter, limit=limit)
            elif sort == 'hot':
                posts = subreddit_instance.hot(limit=limit)
            elif sort == 'new':
                posts = subreddit_instance.new(limit=limit)
            elif sort == 'rising':
                posts = subreddit_instance.rising(limit=limit)
            else:
                raise ValueError(f"Invalid sort method: {sort}")
            
            post_list = []
            extracted_count = 0
            
            for post in posts:
                try:
                    post_dict = self._extract_post_fields(post)
                    post_list.append(post_dict)
                    extracted_count += 1
                    
                    if extracted_count % 100 == 0:
                        logger.debug(f"Extracted {extracted_count} posts...")
                    
                except Exception as e:
                    logger.warning(f"Failed to extract post {post.id}: {str(e)}")
                    continue
            
            logger.info(f"Successfully extracted {extracted_count} posts from r/{subreddit}")
            return post_list
            
        except PrawcoreException as e:
            logger.error(f"Reddit API error: {str(e)}")
            raise RedditAPIException(f"Reddit API error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {str(e)}")
            raise RedditAPIException(f"Post extraction failed: {str(e)}")
    
    def _extract_post_fields(self, post) -> Dict:
        """
        Extract specified fields from a Reddit post object.
        
        Args:
            post: PRAW submission object
            
        Returns:
            Dictionary with extracted fields
        """
        post_dict = {}
        for field in self.POST_FIELDS:
            try:
                value = getattr(post, field, None)
                # Handle special cases
                if field == 'author' and value:
                    value = str(value)  # Convert Redditor object to string
                elif field == 'created_utc':
                    value = int(value) if value else None
                post_dict[field] = value
            except Exception as e:
                logger.debug(f"Failed to extract field {field}: {str(e)}")
                post_dict[field] = None
        
        return post_dict
    
    def extract_posts_batch(
        self,
        subreddits: List[str],
        time_filter: str = 'all',
        limit_per_subreddit: Optional[int] = None,
        sort: str = 'top',
        delay_between_subreddits: float = 1.0
    ) -> pd.DataFrame:
        """
        Extract posts from multiple subreddits and combine into a DataFrame.
        
        Args:
            subreddits: List of subreddit names
            time_filter: Time filter for extraction
            limit_per_subreddit: Limit per subreddit
            sort: Sort method
            delay_between_subreddits: Delay in seconds between subreddit extractions
            
        Returns:
            Combined DataFrame with all posts
        """
        all_posts = []
        
        for subreddit in subreddits:
            try:
                posts = self.extract_posts(
                    subreddit=subreddit,
                    time_filter=time_filter,
                    limit=limit_per_subreddit,
                    sort=sort
                )
                all_posts.extend(posts)
                
                # Rate limiting
                if delay_between_subreddits > 0:
                    time.sleep(delay_between_subreddits)
                    
            except Exception as e:
                logger.error(f"Failed to extract from r/{subreddit}: {str(e)}")
                continue
        
        if not all_posts:
            logger.warning("No posts extracted from any subreddit")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_posts)
        logger.info(f"Combined {len(df)} posts from {len(subreddits)} subreddits")
        return df
    
    def extract_posts_streaming(
        self,
        subreddit: str,
        time_filter: str = 'all',
        batch_size: int = 100
    ) -> Iterator[List[Dict]]:
        """
        Stream posts in batches for memory-efficient processing.
        
        Args:
            subreddit: Subreddit name
            time_filter: Time filter
            batch_size: Number of posts per batch
            
        Yields:
            Batches of post dictionaries
        """
        if not self.reddit:
            self._connect()
        
        try:
            subreddit_instance = self.reddit.subreddit(subreddit)
            posts = subreddit_instance.top(time_filter=time_filter)
            
            batch = []
            for post in posts:
                try:
                    post_dict = self._extract_post_fields(post)
                    batch.append(post_dict)
                    
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                        
                except Exception as e:
                    logger.warning(f"Failed to extract post: {str(e)}")
                    continue
            
            # Yield remaining posts
            if batch:
                yield batch
                
        except Exception as e:
            logger.error(f"Streaming extraction failed: {str(e)}")
            raise RedditAPIException(f"Streaming extraction failed: {str(e)}")
