FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /logs/artifacts /logs/verifier \
    && chmod 777 /logs/artifacts /logs/verifier

WORKDIR /workspace
