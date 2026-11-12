# Lightweight image for the collector, storage init and Streamlit dashboard.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --retries 10 --timeout 120 -r requirements.txt

# Source code is bind-mounted at /app by docker-compose during development.
