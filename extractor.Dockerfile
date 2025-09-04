# extractor.Dockerfile
FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1

# Dependencias del extractor (requests, pandas, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY . /app


ENTRYPOINT ["python", "-m", "src.main"]

