# Reddit Data Pipeline

A production-style data engineering project that extracts Reddit posts and metadata using the Reddit API, orchestrates the workflow with Apache Airflow, lands raw data in Amazon S3, prepares it with AWS Glue, validates it with Amazon Athena (SQL), and publishes curated datasets into Amazon Redshift for analytics.

---

## What this pipeline does

1. Extract subreddit posts and metadata from Reddit using **PRAW** (Python Reddit API Wrapper)  
2. Transform the raw response into a clean, tabular schema (CSV) in Python  
3. Store daily raw extracts in **Amazon S3**  
4. Prepare data using **AWS Glue** (catalog and transformations as needed)  
5. Query and validate data in S3 using **Amazon Athena**  
6. Load curated datasets into **Amazon Redshift** for warehouse analytics  
7. Optional: run sentiment scoring on post titles using **VADER**, **RoBERTa**, and aspect-based sentiment techniques

---

## Tech stack

- Orchestration: Apache Airflow (CeleryExecutor)
- Distributed execution: Celery and Redis
- Metadata store: PostgreSQL
- Ingestion: PRAW (Reddit API), Python
- Storage: Amazon S3
- ETL and catalog: AWS Glue
- Query engine: Amazon Athena (SQL)
- Warehouse: Amazon Redshift
- Containerization: Docker and Docker Compose
- Optional NLP: NLTK VADER, transformer-based RoBERTa, aspect-based sentiment workflow

---

## Repository structure

You can organize the repo however you like; this layout keeps things easy to follow:

```
.
├── dags/                      # Airflow DAGs
├── src/                       # ETL scripts (Reddit extract/transform, S3 upload helpers, config loader)
├── data/
│   └── output/                # Local outputs (CSV extracts) - keep this gitignored if large
├── notebooks/                 # Sentiment analysis notebook(s)
├── docker-compose.yml
├── Dockerfile
├── airflow.env
├── requirements.txt
├── config.conf                # local config (DO NOT commit secrets)
├── config.example.conf        # safe template
└── README.md
```

---

## Prerequisites

- AWS account with access to S3, Glue, Athena, and Redshift
- Reddit API credentials (client id, client secret, user agent)
- Docker Desktop installed and running
- Python 3.9+ (if you run scripts outside the container)
- Optional: Postman (useful for quickly validating Reddit API credentials)

---

## Setup

### 1) Create Reddit API credentials
Create a Reddit app and collect:
- client id
- client secret
- user agent

Keep these out of git. Store locally in `config.conf` or environment variables.

---

### 2) Configure AWS permissions
You need IAM permissions for:
- S3: create bucket (optional), put/get objects, list
- Glue: crawler/catalog, jobs (if using), read/write
- Athena: query execution and results bucket access
- Redshift: cluster access and load permissions

Use least-privilege policies where possible.

---

### 3) Configure the Airflow environment

#### airflow.env
This config enables CeleryExecutor with Redis and PostgreSQL.

Common keys:
- AIRFLOW__CORE__EXECUTOR=CeleryExecutor
- AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
- AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://postgres@postgres:5432/airflow_reddit
- AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres@postgres:5432/airflow_reddit
- AIRFLOW__CORE__LOAD_EXAMPLES=False

Do not commit secrets. Add `airflow.env` and `config.conf` to `.gitignore`.

---

### 4) Start Airflow (Docker Compose)

From the project root:

```bash
docker compose up -d --build
```

Then open the Airflow UI:
- http://localhost:8080

Once the scheduler is up, your DAGs should appear.

---

## Pipeline details

### Airflow DAG (daily workflow)
The pipeline is orchestrated as a two-step workflow:

1. Extract task  
   - Connects to Reddit using PRAW credentials  
   - Pulls posts and metadata for one or more configured subreddits  
   - Normalizes the response into a consistent table schema  
   - Writes a local CSV output (example: `reddit_YYYYMMDD.csv`)

2. Upload-to-S3 task  
   - Reads the output path from the extract task  
   - Ensures the S3 bucket exists (create if configured)  
   - Uploads the CSV to S3 as the raw layer

Task ordering is enforced by dependencies so the upload runs only after extraction succeeds.

---

## AWS data preparation and validation

### 1) S3 raw layer
Raw extracts are stored in S3. A common convention is:

- s3://<bucket>/raw/reddit/dt=YYYY-MM-DD/<file>.csv

Keeping date partitions makes exploration and backfills easier.

### 2) Glue Data Catalog
Use Glue Crawlers to infer schema and register the dataset in the Glue Data Catalog.

### 3) Athena SQL
Run ad-hoc SQL in Athena against the S3 raw layer for:
- schema sanity checks
- quick profiling (counts, distinct subreddit distribution, null checks)
- exploratory analysis without provisioning servers

### 4) Redshift publishing
Load transformed or curated data into Redshift to enable fast queries and downstream reporting.

If you use COPY from S3 into Redshift, document the COPY options you used (IAM role, delimiters, timeformat, and error handling) and keep credentials out of the repo.

---

## Optional: Sentiment analysis (titles)

This repo includes a sentiment scoring workflow for Reddit post titles using:
- VADER (rule-based baseline, good for short social text)
- RoBERTa (transformer-based sentiment scoring)
- Aspect-based sentiment analysis (for topic-specific interpretation)

Typical output columns produced by VADER include:
- neg, neu, pos, compound

You can store sentiment outputs as:
- a local CSV for exploration, or
- a curated table in Redshift for subreddit and time-based sentiment trends.

### Running the notebook
Place your extracted CSV in `data/output/` (or update the path in the notebook), then run:

- notebooks/reddit_sentiment_analysis.ipynb

---

## Configuration

### config.conf
Use this file to store:
- Reddit credentials
- AWS credentials (or prefer IAM roles if you run inside AWS)
- S3 bucket name and paths
- ETL settings (subreddits, output paths, etc.)

Create a safe template for sharing:
- config.example.conf (no secrets)

---

## Troubleshooting

- Airflow UI shows no DAGs: confirm your DAG files are in the mounted `dags/` folder and the scheduler container is healthy.
- Tasks stuck in queued state: confirm Redis and worker containers are up, and CeleryExecutor is configured correctly.
- S3 upload errors: verify IAM permissions and correct bucket region/name.
- Glue or Athena can’t see data: confirm your crawler ran successfully and Athena query results bucket permissions are set.
- Redshift load issues: validate COPY options, IAM role permissions, and CSV formatting (delimiters, quotes, nulls).

---

## Practical next upgrades


- Write curated outputs as Parquet and partition by date/subreddit for better query performance
- Add data-quality checks in Airflow (row counts, duplicate IDs, null thresholds)
- Add CI checks (linting, unit tests) and a simple GitHub Actions workflow
- Add a small warehouse model layer (views) for common analytics queries
