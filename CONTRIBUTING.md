# Contributing to Reddit Data Pipeline

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone <your-fork-url>`
3. Create a virtual environment: `python -m venv venv`
4. Activate it: `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)
5. Install dependencies: `pip install -r requirements.txt`
6. Install dev dependencies: `pip install -r requirements.txt[dev]`

## Code Style

- Follow PEP 8 style guide
- Use type hints where appropriate
- Write docstrings for all functions and classes
- Keep functions focused and small
- Use meaningful variable names

## Testing

- Write tests for all new functionality
- Ensure all tests pass: `pytest tests/ -v`
- Aim for >80% code coverage
- Run linting: `flake8 src/` and `black --check src/`

## Commit Messages

Use clear, descriptive commit messages:
- `feat: Add new feature`
- `fix: Fix bug in X`
- `docs: Update README`
- `test: Add tests for Y`
- `refactor: Refactor Z module`

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes
3. Add/update tests
4. Update documentation if needed
5. Ensure all tests pass
6. Submit PR with clear description

## Questions?

Open an issue or reach out to the maintainers.
