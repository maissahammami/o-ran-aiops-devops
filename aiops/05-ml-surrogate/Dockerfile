FROM python:3.11-slim
WORKDIR /app
COPY train_surrogate.py serve_surrogate.py ./
RUN pip install --no-cache-dir numpy scikit-learn joblib flask
EXPOSE 8000
CMD ["python", "serve_surrogate.py"]
