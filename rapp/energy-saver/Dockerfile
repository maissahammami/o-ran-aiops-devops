FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    curl \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade \
    pip \
    "setuptools>=78.1.1" \
    wheel && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -f /usr/local/lib/python3.11/site-packages/pip/_vendor/bom.cdx.json

COPY src/ ./src/

RUN useradd -m rappuser

USER rappuser

CMD ["python3","src/rApp_Energy_Saver.py","-c","/app/src/config/config.yaml"]
