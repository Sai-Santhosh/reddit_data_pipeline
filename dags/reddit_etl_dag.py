"""
Production-ready Airflow DAG for Reddit ETL pipeline.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow.models import Variable

from src.pipelines.reddit_pipeline import run_reddit_pipeline
from src.pipelines.s3_pipeline import upload_to_s3
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

# DAG definition
dag = DAG(
    dag_id='reddit_etl_pipeline',
    default_args=default_args,
    description='Production Reddit ETL Pipeline - Extract, Transform, Load to S3',
    schedule_interval='@daily',  # Run daily
    max_active_runs=1,
    tags=['reddit', 'etl', 'data-pipeline', 'production'],
    catchup=False,
)


def extract_reddit_data(**context):
    """
    Extract data from Reddit.
    
    Returns:
        Path to output file
    """
    # Get configuration from Airflow Variables or use defaults
    subreddits = Variable.get(
        'reddit_subreddits',
        default_var='science+politics+technology+relationships'
    ).split('+')
    
    time_filter = Variable.get('reddit_time_filter', default_var='all')
    limit = int(Variable.get('reddit_limit', default_var='5000'))
    
    # Generate output filename with date
    execution_date = context.get('execution_date', datetime.now())
    date_str = execution_date.strftime("%Y%m%d")
    output_filename = f"reddit_{date_str}"
    
    logger.info(f"Extracting Reddit data for date: {date_str}")
    
    # Run extraction pipeline
    output_path = run_reddit_pipeline(
        subreddits=subreddits,
        time_filter=time_filter,
        limit_per_subreddit=limit,
        sort='top',
        output_filename=output_filename
    )
    
    logger.info(f"Extraction complete: {output_path}")
    return output_path


def upload_to_s3_task(**context):
    """
    Upload extracted data to S3.
    """
    # Get file path from previous task
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='extract_reddit_data')
    
    if not file_path:
        raise ValueError("No file path received from extraction task")
    
    logger.info(f"Uploading to S3: {file_path}")
    
    # Upload to S3
    s3_uri = upload_to_s3(local_file_path=file_path)
    
    logger.info(f"Upload complete: {s3_uri}")
    return s3_uri


def validate_data_quality(**context):
    """
    Validate data quality after extraction.
    """
    from src.processing.data_validator import DataValidator
    import pandas as pd
    
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='extract_reddit_data')
    
    if not file_path:
        raise ValueError("No file path received from extraction task")
    
    logger.info(f"Validating data quality: {file_path}")
    
    # Load and validate
    df = pd.read_csv(file_path)
    validator = DataValidator()
    result = validator.validate(df)
    
    if not result.is_valid:
        logger.error(f"Data validation failed: {result}")
        raise ValueError(f"Data quality check failed: {', '.join(result.errors)}")
    
    logger.info(f"Data validation passed: {len(df)} rows")
    return result.stats


# Task definitions
with TaskGroup("extraction_group", dag=dag) as extraction_group:
    extract_task = PythonOperator(
        task_id='extract_reddit_data',
        python_callable=extract_reddit_data,
        dag=dag,
    )
    
    validate_task = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        dag=dag,
    )

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3_task,
    dag=dag,
)

# Task dependencies
extract_task >> validate_task >> upload_task
