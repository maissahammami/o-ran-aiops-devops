FROM ghcr.io/maissahammami/cplex-python38:latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY src/ ./src/
COPY requirements.txt .

# Installer les dépendances sans reconstruire docplex
RUN pip install --no-cache-dir \
    certifi \
    charset-normalizer \
    confluent-kafka \
    requests \
    urllib3 \
    pandas \
    numpy

# Installer uniquement docplex
RUN pip install --no-cache-dir docplex==2.30.251

ENTRYPOINT ["sleep","999999"]
