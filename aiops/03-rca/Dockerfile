FROM bitnami/kubectl:1.29 AS kubectl-src
FROM python:3.11-slim
COPY --from=kubectl-src /opt/bitnami/kubectl/bin/kubectl /usr/local/bin/kubectl
WORKDIR /app
COPY rca_correlator.py .
CMD ["python", "rca_correlator.py"]
