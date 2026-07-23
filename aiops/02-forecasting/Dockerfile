FROM python:3.11-slim
WORKDIR /app
COPY forecast.py .
RUN pip install --no-cache-dir numpy influxdb kafka-python
CMD ["python", "forecast.py"]
