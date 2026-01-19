# Setup script for Reddit Data Pipeline (PowerShell)

Write-Host "🚀 Setting up Reddit Data Pipeline..." -ForegroundColor Green

# Create necessary directories
Write-Host "📁 Creating directories..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path "data\input", "data\output", "data\processed", "logs", "models" | Out-Null

# Copy example config if config doesn't exist
if (-not (Test-Path "config\config.conf")) {
    Write-Host "📝 Creating config file from example..." -ForegroundColor Cyan
    Copy-Item "config\config.example.conf" "config\config.conf"
    Write-Host "⚠️  Please edit config\config.conf with your credentials!" -ForegroundColor Yellow
}

# Create .env file if it doesn't exist
if (-not (Test-Path "airflow.env")) {
    Write-Host "📝 Creating airflow.env file..." -ForegroundColor Cyan
    @"
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://postgres:postgres@postgres:5432/airflow_reddit
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__FERNET_KEY=''
"@ | Out-File -FilePath "airflow.env" -Encoding utf8
    Write-Host "⚠️  Please review airflow.env and update if needed!" -ForegroundColor Yellow
}

# Install Python dependencies (if Python is available)
if (Get-Command python -ErrorAction SilentlyContinue) {
    Write-Host "🐍 Installing Python dependencies..." -ForegroundColor Cyan
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
}

# Download NLTK data
if (Get-Command python -ErrorAction SilentlyContinue) {
    Write-Host "📚 Downloading NLTK data..." -ForegroundColor Cyan
    python -c "import nltk; nltk.download('vader_lexicon', quiet=True)" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "NLTK download skipped" -ForegroundColor Yellow
    }
}

Write-Host "✅ Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Edit config\config.conf with your credentials"
Write-Host "2. Review airflow.env configuration"
Write-Host "3. Run: docker compose up -d --build"
Write-Host "4. Access Airflow UI at http://localhost:8080"
