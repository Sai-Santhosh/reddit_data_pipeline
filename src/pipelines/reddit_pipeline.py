"""
Production-ready Reddit data pipeline orchestrator.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from src.ingestion.reddit_extractor import RedditExtractor
from src.processing.data_transformer import DataTransformer
from src.processing.data_validator import DataValidator
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.exceptions import RedditPipelineException

logger = get_logger(__name__)


class RedditPipeline:
    """Complete Reddit data extraction and processing pipeline."""
    
    def __init__(self):
        """Initialize pipeline components."""
        self.config = get_config()
        self.extractor = RedditExtractor()
        self.transformer = DataTransformer()
        self.validator = DataValidator()
        self.logger = logger
    
    def run(
        self,
        subreddits: List[str],
        time_filter: str = 'all',
        limit_per_subreddit: Optional[int] = None,
        sort: str = 'top',
        output_filename: Optional[str] = None,
        validate: bool = True,
        transform: bool = True
    ) -> str:
        """
        Run the complete Reddit data pipeline.
        
        Args:
            subreddits: List of subreddit names to extract from
            time_filter: Time filter ('all', 'day', 'week', 'month', 'year')
            limit_per_subreddit: Maximum posts per subreddit
            sort: Sort method ('top', 'hot', 'new', 'rising')
            output_filename: Output filename (without extension). If None, auto-generated
            validate: Whether to validate data
            transform: Whether to transform data
            
        Returns:
            Path to output file
        """
        try:
            logger.info(f"Starting Reddit pipeline for subreddits: {subreddits}")
            
            # Step 1: Extract data
            logger.info("Step 1: Extracting data from Reddit")
            df = self.extractor.extract_posts_batch(
                subreddits=subreddits,
                time_filter=time_filter,
                limit_per_subreddit=limit_per_subreddit,
                sort=sort
            )
            
            if df.empty:
                raise RedditPipelineException("No data extracted from Reddit")
            
            logger.info(f"Extracted {len(df)} posts")
            
            # Step 2: Transform data
            if transform:
                logger.info("Step 2: Transforming data")
                df = self.transformer.transform_reddit_posts(df)
            
            # Step 3: Validate data
            if validate:
                logger.info("Step 3: Validating data")
                validation_result = self.validator.validate(df)
                
                if not validation_result.is_valid:
                    logger.error(f"Data validation failed:\n{validation_result}")
                    if validation_result.errors:
                        raise RedditPipelineException(
                            f"Data validation failed: {', '.join(validation_result.errors)}"
                        )
                
                if validation_result.warnings:
                    logger.warning(f"Data validation warnings: {', '.join(validation_result.warnings)}")
            
            # Step 4: Save to file
            logger.info("Step 4: Saving data to file")
            if output_filename is None:
                date_str = datetime.now().strftime("%Y%m%d")
                output_filename = f"reddit_{date_str}"
            
            output_path = self._save_dataframe(df, output_filename)
            
            logger.info(f"Pipeline completed successfully. Output: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise RedditPipelineException(f"Pipeline execution failed: {str(e)}")
    
    def _save_dataframe(self, df: pd.DataFrame, filename: str) -> str:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Filename without extension
            
        Returns:
            Path to saved file
        """
        output_dir = self.config.paths.output_path
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / f"{filename}.csv"
        
        # Prepare for export
        df_export = self.transformer.prepare_for_export(df, format='csv')
        
        df_export.to_csv(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
        
        return str(output_path)


def run_reddit_pipeline(
    subreddits: List[str],
    time_filter: str = 'all',
    limit_per_subreddit: Optional[int] = None,
    sort: str = 'top',
    output_filename: Optional[str] = None
) -> str:
    """
    Convenience function to run the Reddit pipeline.
    
    Args:
        subreddits: List of subreddit names
        time_filter: Time filter
        limit_per_subreddit: Maximum posts per subreddit
        sort: Sort method
        output_filename: Output filename
        
    Returns:
        Path to output file
    """
    pipeline = RedditPipeline()
    return pipeline.run(
        subreddits=subreddits,
        time_filter=time_filter,
        limit_per_subreddit=limit_per_subreddit,
        sort=sort,
        output_filename=output_filename
    )
