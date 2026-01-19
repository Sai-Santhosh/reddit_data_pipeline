#!/bin/bash

# Setup script for Reddit Data Pipeline

set -e

echo "🚀 Setting up Reddit Data Pipeline..."

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/input data/output data/processed logs models

# Copy example config if config doesn't exist
if [ ! -f config/config.conf ]; then
    echo "📝 Creating config file from example..."
    cp config/config.example.conf config/config.conf
    echo "⚠️  Please edit config/config.conf with your credentials!"
fi

# Create .env file if it doesn't exist
if [ ! -f airflow.env ]; then
    echo "📝 Creating airflow.env file..."
    cat > airflow.env << EOF
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__FERNET_KEY=''
EOF
    echo "⚠️  Please review airflow.env and update if needed!"
fi

# Install Python dependencies (if not using Docker)
if command -v python3 &> /dev/null; then
    echo "🐍 Installing Python dependencies..."
    python3 -m pip install --upgrade pip
    python3 -m pip install -r requirements.txt
fi

# Download NLTK data
if command -v python3 &> /dev/null; then
    echo "📚 Downloading NLTK data..."
    python3 -c "import nltk; nltk.download('vader_lexicon', quiet=True)" || echo "NLTK download skipped"
fi

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit config/config.conf with your credentials"
echo "2. Review airflow.env configuration"
echo "3. Run: docker compose up -d --build"
echo "4. Access Airflow UI at http://localhost:8080"
