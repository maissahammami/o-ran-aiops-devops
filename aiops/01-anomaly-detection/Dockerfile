FROM python:3.11-slim
WORKDIR /app
COPY anomaly_detector.py .
RUN pip install --no-cache-dir numpy scikit-learn influxdb kafka-python
CMD ["python", "anomaly_detector.py"]
