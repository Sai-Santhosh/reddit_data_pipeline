"""
Production-ready sentiment analysis module using VADER and transformer models.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

try:
    from nltk.sentiment import SentimentIntensityAnalyzer
    import nltk
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

try:
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

from src.utils.logger import get_logger
from src.utils.exceptions import ProcessingException

logger = get_logger(__name__)


class SentimentAnalyzer:
    """Sentiment analysis using multiple methods."""
    
    def __init__(self, use_transformer: bool = True, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"):
        """
        Initialize sentiment analyzer.
        
        Args:
            use_transformer: Whether to use transformer model (requires transformers library)
            model_name: HuggingFace model name for transformer-based analysis
        """
        self.use_transformer = use_transformer and TRANSFORMERS_AVAILABLE
        self.model_name = model_name
        
        # Initialize VADER
        if NLTK_AVAILABLE:
            try:
                nltk.download('vader_lexicon', quiet=True)
                self.vader = SentimentIntensityAnalyzer()
                logger.info("VADER sentiment analyzer initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize VADER: {str(e)}")
                self.vader = None
        else:
            logger.warning("NLTK not available, VADER disabled")
            self.vader = None
        
        # Initialize transformer model
        self.transformer_pipeline = None
        if self.use_transformer:
            try:
                self.transformer_pipeline = pipeline(
                    "sentiment-analysis",
                    model=model_name,
                    tokenizer=model_name,
                    device=-1  # Use CPU, set to 0 for GPU
                )
                logger.info(f"Transformer model {model_name} initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize transformer model: {str(e)}")
                self.use_transformer = False
    
    def analyze_vader(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using VADER.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        if self.vader is None:
            raise ProcessingException("VADER analyzer not available")
        
        if not text or pd.isna(text):
            return {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}
        
        scores = self.vader.polarity_scores(str(text))
        return {
            'vader_neg': scores['neg'],
            'vader_neu': scores['neu'],
            'vader_pos': scores['pos'],
            'vader_compound': scores['compound']
        }
    
    def analyze_transformer(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using transformer model.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        if self.transformer_pipeline is None:
            raise ProcessingException("Transformer model not available")
        
        if not text or pd.isna(text):
            return {'transformer_label': 'NEUTRAL', 'transformer_score': 0.0}
        
        try:
            # Truncate text if too long (most models have token limits)
            max_length = 512
            text_str = str(text)[:max_length]
            
            result = self.transformer_pipeline(text_str)[0]
            
            # Normalize label to standard format
            label = result['label'].upper()
            if 'POSITIVE' in label or 'POS' in label:
                label = 'POSITIVE'
            elif 'NEGATIVE' in label or 'NEG' in label:
                label = 'NEGATIVE'
            else:
                label = 'NEUTRAL'
            
            return {
                'transformer_label': label,
                'transformer_score': result['score']
            }
        except Exception as e:
            logger.warning(f"Transformer analysis failed for text: {str(e)}")
            return {'transformer_label': 'NEUTRAL', 'transformer_score': 0.0}
    
    def analyze(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using all available methods.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with all sentiment scores
        """
        results = {}
        
        # VADER analysis
        if self.vader:
            try:
                vader_scores = self.analyze_vader(text)
                results.update(vader_scores)
            except Exception as e:
                logger.debug(f"VADER analysis failed: {str(e)}")
        
        # Transformer analysis
        if self.use_transformer and self.transformer_pipeline:
            try:
                transformer_scores = self.analyze_transformer(text)
                results.update(transformer_scores)
            except Exception as e:
                logger.debug(f"Transformer analysis failed: {str(e)}")
        
        # Combined sentiment label
        if 'vader_compound' in results:
            compound = results['vader_compound']
            if compound >= 0.05:
                results['sentiment_label'] = 'POSITIVE'
            elif compound <= -0.05:
                results['sentiment_label'] = 'NEGATIVE'
            else:
                results['sentiment_label'] = 'NEUTRAL'
        elif 'transformer_label' in results:
            results['sentiment_label'] = results['transformer_label']
        else:
            results['sentiment_label'] = 'NEUTRAL'
        
        return results
    
    def analyze_dataframe(
        self,
        df: pd.DataFrame,
        text_column: str = 'title',
        batch_size: int = 100,
        show_progress: bool = True
    ) -> pd.DataFrame:
        """
        Analyze sentiment for all rows in a DataFrame.
        
        Args:
            df: DataFrame with text to analyze
            text_column: Name of column containing text
            batch_size: Batch size for processing
            show_progress: Whether to show progress
            
        Returns:
            DataFrame with added sentiment columns
        """
        if text_column not in df.columns:
            raise ProcessingException(f"Column '{text_column}' not found in DataFrame")
        
        logger.info(f"Analyzing sentiment for {len(df)} rows")
        
        results_list = []
        
        iterator = df.iterrows()
        if show_progress:
            try:
                from tqdm import tqdm
                iterator = tqdm(df.iterrows(), total=len(df), desc="Analyzing sentiment")
            except ImportError:
                pass
        
        for idx, row in iterator:
            text = row[text_column]
            sentiment_scores = self.analyze(text)
            results_list.append(sentiment_scores)
        
        # Convert to DataFrame and merge
        sentiment_df = pd.DataFrame(results_list, index=df.index)
        result_df = pd.concat([df, sentiment_df], axis=1)
        
        logger.info(f"Sentiment analysis complete: {len(result_df)} rows processed")
        return result_df
    
    def get_sentiment_summary(self, df: pd.DataFrame) -> Dict:
        """
        Get summary statistics of sentiment analysis.
        
        Args:
            df: DataFrame with sentiment columns
            
        Returns:
            Dictionary with summary statistics
        """
        summary = {}
        
        if 'sentiment_label' in df.columns:
            label_counts = df['sentiment_label'].value_counts().to_dict()
            summary['label_distribution'] = label_counts
            
            total = len(df)
            summary['label_percentages'] = {
                label: (count / total * 100) 
                for label, count in label_counts.items()
            }
        
        if 'vader_compound' in df.columns:
            summary['vader_compound_stats'] = {
                'mean': float(df['vader_compound'].mean()),
                'std': float(df['vader_compound'].std()),
                'min': float(df['vader_compound'].min()),
                'max': float(df['vader_compound'].max())
            }
        
        if 'transformer_score' in df.columns:
            summary['transformer_score_stats'] = {
                'mean': float(df['transformer_score'].mean()),
                'std': float(df['transformer_score'].std()),
                'min': float(df['transformer_score'].min()),
                'max': float(df['transformer_score'].max())
            }
        
        return summary
