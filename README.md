# Reddit Data Pipeline - Production Ready

A comprehensive, production-grade data engineering pipeline for extracting, processing, and analyzing Reddit posts with sentiment analysis capabilities. Built with best practices for scalability, reliability, and maintainability.

## 🎯 Overview

This pipeline provides a complete end-to-end solution for:
- **Data Extraction**: Automated Reddit post extraction using PRAW
- **Data Processing**: Data transformation, validation, and quality checks
- **Data Storage**: Secure storage in AWS S3 with partitioning
- **Orchestration**: Apache Airflow for workflow management
- **Sentiment Analysis**: Multi-model sentiment analysis (VADER + RoBERTa)
- **Analytics**: Comprehensive notebooks for EDA, modeling, and analysis

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│   Reddit    │ --> │   Extract    │ --> │ Transform   │ --> │     S3      │
│     API     │     │   & Validate │     │  & Process  │     │   Storage   │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
                                                                    │
                                                                    ▼
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│  Notebooks  │ <-- │  Sentiment   │ <-- │   Load      │ <-- │   Analysis  │
│  (EDA/ML)   │     │   Analysis   │     │  from S3    │     │   Pipeline   │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
```

## 📁 Project Structure

```
reddit_data_pipeline/
├── src/                          # Source code
│   ├── ingestion/                # Data ingestion modules
│   │   ├── reddit_extractor.py   # Reddit API extractor
│   │   └── s3_uploader.py        # S3 upload handler
│   ├── processing/               # Data processing modules
│   │   ├── data_transformer.py   # Data transformation
│   │   └── data_validator.py     # Data quality validation
│   ├── ml/                       # Machine learning modules
│   │   └── sentiment_analyzer.py # Sentiment analysis
│   ├── pipelines/                # Pipeline orchestrators
│   │   ├── reddit_pipeline.py    # Reddit ETL pipeline
│   │   └── s3_pipeline.py        # S3 upload pipeline
│   └── utils/                    # Utilities
│       ├── config.py             # Configuration management
│       ├── logger.py             # Logging utilities
│       └── exceptions.py        # Custom exceptions
├── dags/                         # Airflow DAGs
│   └── reddit_etl_dag.py        # Main ETL DAG
├── notebooks/                    # Jupyter notebooks
│   ├── 01_data_ingestion.ipynb  # Data ingestion
│   ├── 02_eda.ipynb             # Exploratory data analysis
│   ├── 03_feature_engineering.ipynb  # Feature engineering
│   ├── 04_modeling.ipynb        # Sentiment modeling
│   ├── 05_model_tuning.ipynb   # Model optimization
│   └── 06_analysis.ipynb        # Final analysis
├── tests/                        # Unit tests
│   ├── test_reddit_extractor.py
│   ├── test_data_validator.py
│   └── test_data_transformer.py
├── config/                       # Configuration files
│   ├── config.example.conf      # Example config (safe to commit)
│   └── config.conf              # Actual config (gitignored)
├── data/                         # Data directories
│   ├── input/                    # Input data
│   ├── output/                   # Output data
│   └── processed/                # Processed data
├── logs/                          # Application logs
├── docker-compose.yml            # Docker Compose configuration
├── Dockerfile                    # Docker image definition
├── requirements.txt              # Python dependencies
├── .gitignore                   # Git ignore rules
└── README.md                     # This file
```

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (v20.10+) and Docker Compose
- **Python 3.11+** (for local development)
- **AWS Account** with S3 access (for cloud storage)
- **Reddit API Credentials** (client ID, secret, user agent)

### 1. Clone the Repository

```bash
git clone <repository-url>
cd reddit_data_pipeline
```

### 2. Configure Credentials

Copy the example configuration file and fill in your credentials:

```bash
cp config/config.example.conf config/config.conf
```

Edit `config/config.conf` with your credentials:

```ini
[api_keys]
reddit_client_id = YOUR_REDDIT_CLIENT_ID
reddit_secret_key = YOUR_REDDIT_SECRET_KEY
reddit_user_agent = YourApp/1.0

[aws]
aws_access_key_id = YOUR_AWS_ACCESS_KEY_ID
aws_secret_access_key = YOUR_AWS_SECRET_ACCESS_KEY
aws_region = us-east-1
aws_bucket_name = your-bucket-name
```

**⚠️ Important**: Never commit `config/config.conf` to version control!

### 3. Set Up Environment Variables

Create `airflow.env` file (or use environment variables):

```bash
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### 4. Start the Pipeline

```bash
# Build and start all services
docker compose up -d --build

# Check logs
docker compose logs -f

# Access Airflow UI
# http://localhost:8080
# Default credentials: airflow / airflow
```

### 5. Run the Pipeline

1. Open Airflow UI at http://localhost:8080
2. Find the `reddit_etl_pipeline` DAG
3. Toggle it ON and trigger a run

## 📊 Features

### 🔍 Data Extraction

- **Robust Reddit API Integration**: Handles rate limiting, retries, and errors gracefully
- **Batch Processing**: Extract from multiple subreddits efficiently
- **Streaming Support**: Memory-efficient streaming for large datasets
- **Comprehensive Field Extraction**: Extracts 15+ fields including metadata

### 🔄 Data Processing

- **Type Conversion**: Automatic type inference and conversion
- **Data Cleaning**: Handles missing values, duplicates, and invalid data
- **Feature Engineering**: Creates derived features (engagement scores, temporal features)
- **Data Validation**: Comprehensive quality checks with detailed reporting

### ☁️ Cloud Storage

- **S3 Integration**: Secure uploads with automatic bucket creation
- **Date Partitioning**: Organized storage with date-based partitioning
- **Multiple Formats**: Support for CSV, Parquet, and JSON
- **Metadata Support**: Attach custom metadata to uploaded files

### 🤖 Sentiment Analysis

- **VADER**: Rule-based sentiment analysis optimized for social media
- **RoBERTa**: Transformer-based sentiment analysis for high accuracy
- **Hybrid Approach**: Combines multiple models for robust predictions
- **Batch Processing**: Efficient processing of large datasets

### 📈 Analytics Notebooks

1. **01_data_ingestion.ipynb**: Load data from Reddit API, local files, or S3
2. **02_eda.ipynb**: Comprehensive exploratory data analysis
3. **03_feature_engineering.ipynb**: Create features for modeling
4. **04_modeling.ipynb**: Train and evaluate sentiment models
5. **05_model_tuning.ipynb**: Optimize model parameters
6. **06_analysis.ipynb**: Generate insights and visualizations

## 🛠️ Usage

### Running the Pipeline Programmatically

```python
from src.pipelines.reddit_pipeline import run_reddit_pipeline
from src.pipelines.s3_pipeline import upload_to_s3

# Extract and process Reddit data
output_path = run_reddit_pipeline(
    subreddits=['science', 'technology', 'politics'],
    time_filter='all',
    limit_per_subreddit=1000,
    sort='top'
)

# Upload to S3
s3_uri = upload_to_s3(output_path)
print(f"Uploaded to: {s3_uri}")
```

### Running Sentiment Analysis

```python
from src.ml.sentiment_analyzer import SentimentAnalyzer
import pandas as pd

# Load data
df = pd.read_csv('data/output/reddit_20240101.csv')

# Initialize analyzer
analyzer = SentimentAnalyzer(use_transformer=True)

# Analyze sentiment
df_sentiment = analyzer.analyze_dataframe(df, text_column='title')

# Save results
df_sentiment.to_csv('data/processed/sentiment_results.csv', index=False)
```

### Using Configuration

```python
from src.utils.config import get_config

config = get_config()

# Access configuration
reddit_client_id = config.reddit.client_id
aws_bucket = config.aws.bucket_name
batch_size = config.pipeline.batch_size
```

## 🧪 Testing

Run the test suite:

```bash
# Using pytest
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=html

# Specific test file
pytest tests/test_reddit_extractor.py -v
```

## 📝 Configuration

### Configuration Sources (Priority Order)

1. Environment variables (highest priority)
2. `config/config.conf` file
3. Default values

### Environment Variables

```bash
# Reddit
export REDDIT_CLIENT_ID=your_client_id
export REDDIT_SECRET_KEY=your_secret_key
export REDDIT_USER_AGENT=YourApp/1.0

# AWS
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
export AWS_BUCKET_NAME=your-bucket-name

# Pipeline Settings
export BATCH_SIZE=1000
export MAX_RETRIES=3
export LOG_LEVEL=INFO
```

### Airflow Variables

Set these in Airflow UI (Admin > Variables):

- `reddit_subreddits`: Comma-separated subreddit names (e.g., "science+politics+technology")
- `reddit_time_filter`: Time filter (all, day, week, month, year)
- `reddit_limit`: Maximum posts per subreddit

## 🔒 Security Best Practices

1. **Never commit secrets**: Use `.gitignore` to exclude config files
2. **Use environment variables**: Prefer env vars over config files in production
3. **IAM Roles**: Use IAM roles instead of access keys when running on AWS
4. **Secrets Management**: Use AWS Secrets Manager or similar in production
5. **Network Security**: Use VPC endpoints for S3 access in production

## 📊 Monitoring & Logging

### Logs

Logs are stored in the `logs/` directory with daily rotation:

```
logs/
├── src.ingestion.reddit_extractor_20240101.log
├── src.processing.data_transformer_20240101.log
└── ...
```

### Airflow Monitoring

- **UI**: http://localhost:8080
- **Logs**: Available in Airflow UI for each task
- **Metrics**: Use Airflow's built-in metrics or integrate with Prometheus

## 🐛 Troubleshooting

### Common Issues

**1. Airflow DAGs not appearing**
- Check DAG files are in `dags/` directory
- Verify scheduler container is running: `docker compose ps`
- Check scheduler logs: `docker compose logs airflow-scheduler`

**2. Reddit API errors**
- Verify credentials in `config/config.conf`
- Check rate limits (Reddit allows 60 requests/minute)
- Ensure user agent is set correctly

**3. S3 upload failures**
- Verify AWS credentials and permissions
- Check bucket name and region
- Ensure bucket exists or auto-create is enabled

**4. Import errors**
- Ensure `src/` is in Python path
- Check all dependencies are installed: `pip install -r requirements.txt`
- Verify Docker volumes are mounted correctly

**5. Memory issues**
- Reduce `limit_per_subreddit` in pipeline configuration
- Use streaming extraction for large datasets
- Increase Docker memory allocation

## 🚀 Production Deployment

### Recommended Setup

1. **Use managed services**:
   - AWS ECS/EKS for container orchestration
   - RDS for PostgreSQL
   - ElastiCache for Redis
   - S3 for data storage

2. **Implement monitoring**:
   - CloudWatch for AWS services
   - Prometheus + Grafana for metrics
   - ELK stack for log aggregation

3. **Set up CI/CD**:
   - GitHub Actions or GitLab CI
   - Automated testing
   - Docker image building and pushing

4. **Security hardening**:
   - Use IAM roles instead of access keys
   - Enable S3 bucket encryption
   - Use VPC for network isolation
   - Implement secrets management

### Scaling Considerations

- **Horizontal scaling**: Run multiple Airflow workers
- **Batch size**: Adjust based on available memory
- **Parallel processing**: Use Airflow's parallelism settings
- **Caching**: Implement Redis caching for frequently accessed data

## 📚 Documentation

- **Code Documentation**: All modules include docstrings
- **API Reference**: See docstrings in each module
- **Notebooks**: Step-by-step guides in `notebooks/` directory
- **Configuration**: See `config/config.example.conf` for options

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`pytest`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

See [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **PRAW**: Python Reddit API Wrapper
- **Apache Airflow**: Workflow orchestration
- **VADER**: Sentiment analysis
- **HuggingFace Transformers**: Pre-trained models

## 📞 Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check existing documentation
- Review troubleshooting section

## 🔄 Version History

- **v1.0.0** (Current): Production-ready release with comprehensive features
  - Complete ETL pipeline
  - Sentiment analysis (VADER + RoBERTa)
  - Comprehensive notebooks
  - Unit tests
  - Production-grade error handling and logging

---

**Built with ❤️ for data engineers and analysts**
