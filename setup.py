"""
Setup script for Reddit Data Pipeline.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="reddit-data-pipeline",
    version="1.0.0",
    author="Data Engineering Team",
    description="Production-ready Reddit data extraction and sentiment analysis pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/reddit-data-pipeline",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "pandas>=2.1.0",
        "numpy>=1.26.0",
        "praw>=7.7.0",
        "boto3>=1.31.0",
        "nltk>=3.8.0",
        "transformers>=4.35.0",
        "scikit-learn>=1.3.0",
        "matplotlib>=3.8.0",
        "seaborn>=0.13.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.11.0",
            "flake8>=6.1.0",
            "mypy>=1.7.0",
        ],
        "airflow": [
            "apache-airflow>=2.7.0",
        ],
        "notebooks": [
            "jupyter>=1.0.0",
            "ipykernel>=6.26.0",
        ],
    },
)
