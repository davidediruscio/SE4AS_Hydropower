FROM python:3.11-slim

WORKDIR /app

# Install system deps (curl for healthchecks, if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY lib.txt /app/lib.txt
RUN pip install --no-cache-dir -r lib.txt

COPY main.py /app/main.py

# Ensure Python writes to stdout/stderr without buffering so prints/logs
# are visible in Docker logs immediately (useful for detached containers)
ENV PYTHONUNBUFFERED=1
# Avoid creating .pyc files in container
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "main.py"]
