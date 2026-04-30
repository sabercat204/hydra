FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgdal-dev \
    libhdf5-dev \
    libmseed-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal34 \
    libhdf5-103-1 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home hydra
USER hydra
WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY src/ src/
COPY config/ config/

EXPOSE 8000
CMD ["uvicorn", "hydra.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
