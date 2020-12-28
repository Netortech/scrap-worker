#! /bin/bash
if [ ! -f "scrap_worker/default_config.py" ]; then
    cp scrap_worker/default_config.template scrap_worker/default_config.py
fi
echo "Running black..."
python -m black .
echo "Running mypy..."
python -m mypy --ignore-missing-imports scrap_worker
echo "Running tests..."
python -m pytest --cov=./scrap_worker --cov-report=html --log-cli-level=INFO